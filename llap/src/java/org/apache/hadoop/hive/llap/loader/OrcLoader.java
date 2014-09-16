/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.loader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.api.Llap;
import org.apache.hadoop.hive.llap.api.Vector;
import org.apache.hadoop.hive.llap.api.impl.RequestImpl;
import org.apache.hadoop.hive.llap.api.impl.VectorImpl;
import org.apache.hadoop.hive.llap.cache.BufferPool;
import org.apache.hadoop.hive.llap.cache.BufferPool.WeakBuffer;
import org.apache.hadoop.hive.llap.cache.MetadataCache;
import org.apache.hadoop.hive.llap.chunk.ChunkWriterImpl;
import org.apache.hadoop.hive.llap.loader.ChunkPool.Chunk;
import org.apache.hadoop.hive.llap.processor.ChunkConsumer;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type.Kind;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.StripeInformation;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

public class OrcLoader extends Loader {
  private final ChunkPool<ChunkKey> chunkPool;
  private FileSystem cachedFs = null;
  private final MetadataCache metadataCache = new MetadataCache();

  public OrcLoader(BufferPool bufferPool, ChunkPool<ChunkKey> chunkPool,
      Configuration conf) throws IOException {
    super(bufferPool);
    this.chunkPool = chunkPool;
    // We assume all splits will come from the same FS.
    this.cachedFs = FileSystem.get(conf);
  }

  @Override
  protected void loadInternal(RequestImpl request, ChunkConsumer consumer, LoadContext context)
      throws IOException, InterruptedException {
    // TODO: decide on - LocalActorSystem.INSTANCE.enqueue(request, bufferPool, consumer);
    if (DebugUtils.isTraceMttEnabled()) {
      Llap.LOG.info("loadInternal called");
    }
    List<Integer> includedCols = request.getColumns();
    if (includedCols != null) {
      Collections.sort(includedCols);
    }
    FileSplit fileSplit = (FileSplit)request.getSplit();
    String internedFilePath = fileSplit.getPath().toString().intern();
    Llap.LOG.info("Processing split for " + internedFilePath);
    if (context.isStopped) return;
    List<StripeInformation> stripes = metadataCache.getStripes(internedFilePath);
    List<Type> types = metadataCache.getTypes(internedFilePath);
    Reader reader = null;
    if (stripes == null || types == null) {
      reader = createReader(fileSplit);
      if (stripes == null) {
        stripes = reader.getStripes();
        metadataCache.cacheStripes(internedFilePath, stripes);
      }
      if (types == null) {
        types = reader.getTypes();
        metadataCache.cacheTypes(internedFilePath, types);
      }
    }

    // Determine which stripes belong to this split and make keys to get chunks from cache.
    // This assumes all splits will have the same columns.
    if (includedCols == null) {
      includedCols = new ArrayList<Integer>(types.size());
      for (int i = 1; i < types.size(); ++i) {
        includedCols.add(i);
      }
    }
    List<List<ChunkKey>> keys = new ArrayList<List<ChunkKey>>();
    long stripeIxFromAndTo = determineStripesAndCacheKeys(
        fileSplit, includedCols, internedFilePath, stripes, keys);
    int stripeIxFrom = (int)(stripeIxFromAndTo >>> 32),
        stripeIxTo = (int)(stripeIxFromAndTo & (long)Integer.MAX_VALUE);

    // Prepare structures for tracking the results.
    int resultVectorCount = stripeIxTo - stripeIxFrom;
    Chunk[][] resultMatrix = new Chunk[resultVectorCount][];
    @SuppressWarnings("unchecked")
    // TODO: we store result buffers uniquely in a set, so we could lock/unlock them once. This may
    //       be more expensive than just making locking faster, and locking-unlocking as needed.
    HashSet<WeakBuffer>[] resultBuffers = new HashSet[resultVectorCount];
    for (int i = 0; i < resultVectorCount; ++i) {
      resultMatrix[i] = new Chunk[types.size()];
      resultBuffers[i] = new HashSet<WeakBuffer>();
    }
    if (context.isStopped) return;
    // TODO: after this moment, we must be careful when checking isStopped to avoid
    //       leaving some chunks locked and un-consumed. For now we just never check.

    // For now we will fetch missing results by stripe - this is how reader needs them.
    List<Integer> readyStripes = getChunksFromCache(
        keys, types.size(), stripeIxFrom, resultBuffers, resultMatrix);
    if (readyStripes != null) {
      Llap.LOG.info("Got " + readyStripes.size() + " full stripes from cache");
      for (Integer stripeIx : readyStripes) {
        int stripeIxMod = stripeIx - stripeIxFrom;
        VectorImpl vector = createVectorForStripe(
            resultMatrix[stripeIxMod], resultBuffers[stripeIxMod], types, includedCols);
        if (DebugUtils.isTraceMttEnabled()) {
          Llap.LOG.info("Returning stripe " + stripeIx + " from cache");
        }
        consumer.consumeVector(vector);
        resultMatrix[stripeIxMod] = null;
        resultBuffers[stripeIxMod] = null;
      }
    }

    // Now we have a set of keys for all the things that are missing. Fetch them...
    // TODO: this should happen on some sort of IO thread pool.
    for (List<ChunkKey> stripeKeys : keys) {
      if (stripeKeys.isEmpty()) continue;
      int stripeIx = stripeKeys.get(0).stripeIx;
      StripeInformation si = stripes.get(stripeIx);
      List<Integer> includeList = null;
      if (includedCols.size() == stripeKeys.size()) {
        includeList = includedCols;
      } else {
        includeList = new ArrayList<Integer>(stripeKeys.size());
        for (ChunkKey key : stripeKeys) {
          includeList.add(key.colIx);
        }
      }
      boolean[] includes = OrcInputFormat.genIncludedColumns(types, includeList, true);
      if (Llap.LOG.isDebugEnabled()) {
        Llap.LOG.debug("Reading stripe " + stripeIx + " {"
          + si.getOffset() + ", " + si.getLength() + "}, cols " + Arrays.toString(includes));
      }

      if (reader == null) {
        reader = createReader(fileSplit);
      }

      RecordReader stripeReader = reader.rows(si.getOffset(), si.getLength(), includes);
      int stripeIxMod = stripeIx - stripeIxFrom;
      Chunk[] result = resultMatrix[stripeIxMod];
      HashSet<WeakBuffer> buffers = resultBuffers[stripeIxMod];

      loadStripe(stripeReader, stripeKeys, result, buffers);
      stripeReader.close();
      VectorImpl vector = createVectorForStripe(result, buffers, types, includedCols);
      if (DebugUtils.isTraceMttEnabled()) {
        Llap.LOG.info("Returning stripe " + stripeIx + " from FS");
      }
      consumer.consumeVector(vector);
    }
    consumer.setDone();
    if (DebugUtils.isTraceMttEnabled()) {
      Llap.LOG.info("loadInternal is done");
    }
  }

  /**
   * Determines which stripe range belongs to a split, and generates cache keys
   *  for all these stripes and all the included columns.
   * @param fileSplit The split.
   * @param includedCols Included columns.
   * @param internedFilePath Interned file path from the split, for cache keys.
   * @param stripes Stripe information from the reader.
   * @param keys The keys for cache lookups are inserted here.
   * @return Combined int-s for stripe from (inc.) and to (exc.) indexes, because Java is a joke
   */
  private long determineStripesAndCacheKeys(FileSplit fileSplit, List<Integer> includedCols,
      String internedFilePath, List<StripeInformation> stripes, List<List<ChunkKey>> keys) {
    // The unit of caching for ORC is (stripe x column) (see ChunkKey). Note that we do not use
    // SARG anywhere, because file-level filtering on sarg is already performed during split
    // generation, and stripe-level filtering to get row groups is not very helpful right now.
    long offset = fileSplit.getStart(), maxOffset = offset + fileSplit.getLength();
    int stripeIxFrom = -1, stripeIxTo = -1, stripeIx = 0;
    if (Llap.LOG.isDebugEnabled()) {
      String tmp = "FileSplit {" + fileSplit.getStart()
          + ", " + fileSplit.getLength() + "}; stripes ";
      for (StripeInformation stripe : stripes) {
        tmp += "{" + stripe.getOffset() + ", " + stripe.getLength() + "}, ";
      }
      Llap.LOG.debug(tmp);
    }

    for (StripeInformation stripe : stripes) {
      long stripeStart = stripe.getOffset();
      if (offset > stripeStart) continue;
      if (stripeIxFrom == -1) {
        if (DebugUtils.isTraceEnabled()) {
          Llap.LOG.info("Including from " + stripeIx
              + " (" + stripeStart + " >= " + offset + ")");
        }
        stripeIxFrom = stripeIx;
      }
      if (stripeStart >= maxOffset) {
        if (DebugUtils.isTraceEnabled()) {
          Llap.LOG.info("Including until " + stripeIxTo
              + " (" + stripeStart + " >= " + maxOffset + ")");
        }
        stripeIxTo = stripeIx;
        break;
      }

      ArrayList<ChunkKey> stripeKeys = new ArrayList<ChunkKey>(includedCols.size());
      keys.add(stripeKeys);
      for (Integer colIx : includedCols) {
        stripeKeys.add(new ChunkKey(internedFilePath, stripeIx, colIx));
      }
      ++stripeIx;
    }
    if (stripeIxTo == -1) {
      if (DebugUtils.isTraceEnabled()) {
        Llap.LOG.info("Including until " + stripeIx + " (end of file)");
      }
      stripeIxTo = stripeIx;
    }
    return (((long)stripeIxFrom) << 32) + stripeIxTo;
  }

  /**
   * Gets chunks from cache and generates include arrays for things to be fetched.
   * @param keys Keys to get.
   * @param colCount Column count in the file.
   * @param stripeIxFrom Stripe index start in the split.
   * @param resultBuffers Resulting buffers are added here.
   * @param resultMatrix Results that are fetched from cache are added here.
   * @return Matrix of things are not cache.
   */
  private List<Integer> getChunksFromCache(List<List<ChunkKey>> keys, int colCount,
      int stripeIxFrom, HashSet<WeakBuffer>[] resultBuffers, Chunk[][] resultMatrix) {
    List<Integer> readyStripes = null;
    for (List<ChunkKey> stripeKeys : keys) {
      int stripeIx = stripeKeys.get(0).stripeIx;
      int stripeIxMod = stripeIx - stripeIxFrom;
      Chunk[] chunksForStripe = resultMatrix[stripeIxMod];
      HashSet<WeakBuffer> buffersForStripe = resultBuffers[stripeIxMod];
      Iterator<ChunkKey> iter = stripeKeys.iterator();
      while (iter.hasNext()) {
        ChunkKey key = iter.next();
        Chunk result = chunkPool.getChunk(key, buffersForStripe);
        if (result == null) continue;
        if (Llap.LOG.isDebugEnabled()) {
          Llap.LOG.debug("Found result in cache for " + key + ": " + result.toFullString());
        }
        chunksForStripe[key.colIx] = result;
        iter.remove();
      }
      if (stripeKeys.isEmpty()) {
        if (readyStripes == null) {
          readyStripes = new ArrayList<Integer>();
        }
        readyStripes.add(stripeIx);
      }
    }
    return readyStripes;
  }

  private Reader createReader(FileSplit fileSplit) throws IOException {
    FileSystem fs = cachedFs;
    Path path = fileSplit.getPath();
    Configuration conf = new Configuration();
    if ("pfile".equals(path.toUri().getScheme())) {
      fs = path.getFileSystem(conf); // Cannot use cached FS due to hive tests' proxy FS.
    }
    return OrcFile.createReader(path, OrcFile.readerOptions(conf).filesystem(fs));
  }

  private void loadStripe(RecordReader reader, List<ChunkKey> keys, Chunk[] results,
      HashSet<WeakBuffer> resultBuffers) throws IOException, InterruptedException {
    // Reader is reading a single stripe; read the entirety of each column.
    Object readCtx = reader.prepareColumnRead();
    for (int keyIx = 0; keyIx < keys.size(); ++keyIx) {
      ChunkKey key = keys.get(keyIx);
      BufferInProgress colBuffer = null;
      while (true) {
        colBuffer = prepareReusableBuffer(resultBuffers);
        writer.prepare(colBuffer);
        boolean hasMoreValues = reader.readNextColumnStripe(readCtx, writer);
        if (!hasMoreValues) break;
        if (DebugUtils.isTraceEnabled()) {
          Llap.LOG.info("Stripe doesn't fit into buffer");
        }
        // We couldn't write all rows to this buffer, so we'll close the chunk.
        results[key.colIx] = mergeResultChunks(colBuffer, results[key.colIx], false);
      }
      // Done with the reader:
      // 1) add final chunk to result;
      // 2) add reusable buffer back to list;
      // 3) add results to cache and resolve conflicts.
      Chunk val = results[key.colIx] =
          mergeResultChunks(colBuffer, results[key.colIx], true);
      if (Llap.LOG.isDebugEnabled()) {
        Llap.LOG.debug("Caching chunk " + key + " => " + val.toFullString());
      }
      Chunk cachedVal = chunkPool.addOrGetChunk(key, val, resultBuffers);
      if (cachedVal != val) {
        // Someone else has read and cached the same value while we were reading. Assumed to be
        // very rare (otherwise we'd need measures to prevent it), so we will not be efficient;
        // we will rebuild resultBuffers rather than removing buffers from them.
        results[key.colIx] = cachedVal;
        resultBuffers.clear();
        for (int i = 0; i < results.length; ++i) {
          Chunk chunk1 = results[i];
          while (chunk1 != null) {
            resultBuffers.add(chunk1.buffer);
            chunk1 = chunk1.nextChunk;
          }
        }
        Chunk chunk = cachedVal;
        while (chunk != null) {
          if (!resultBuffers.contains(chunk.buffer)) {
            chunk.buffer.unlock();
          }
          chunk = chunk.nextChunk;
        }
      }
      returnReusableBuffer(colBuffer);
    }
  }

  private VectorImpl createVectorForStripe(Chunk[] rowForStripe,
      Collection<WeakBuffer> resultBuffers, List<Type> types, List<Integer> includedCols) {
    VectorImpl vector = new VectorImpl(resultBuffers, types.size());
    for (Integer colIx : includedCols) {
      // TODO: this "+ 1" is a hack relying on knowledge of ORC. It might change, esp. w/ACID.
      Vector.Type type = vectorTypeFromOrcType(types.get(colIx + 1).getKind());
      vector.addChunk(colIx, rowForStripe[colIx], type);
    }
    return vector;
  }

  private static Vector.Type vectorTypeFromOrcType(Kind orcType) {
    switch (orcType) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case DATE:
      case TIMESTAMP:
        return Vector.Type.LONG;
      case FLOAT:
      case DOUBLE:
        return Vector.Type.DOUBLE;
      case STRING:
        return Vector.Type.BINARY;
      case DECIMAL:
        return Vector.Type.DECIMAL;
      default:
        throw new UnsupportedOperationException("Unsupported type " + orcType);
    }
  }

  public static class ChunkKey {
    /** @param file This MUST be interned by caller. */
    private ChunkKey(String file, int stripeIx, int colIx) {
      this.file = file;
      this.stripeIx = stripeIx;
      this.colIx = colIx;
    }
    private final String file;
    private final int stripeIx;
    private final int colIx;

    @Override
    public String toString() {
      return "[" + file + ", stripe " + stripeIx + ", colIx " + colIx + "]";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = prime + ((file == null) ? 0 : System.identityHashCode(file));
      return (prime * result + colIx) * prime + stripeIx;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof ChunkKey)) return false;
      ChunkKey other = (ChunkKey)obj;
      // Strings are interned and can thus be compared like this.
      return stripeIx == other.stripeIx && colIx == other.colIx && file == other.file;
    }
  }
}
