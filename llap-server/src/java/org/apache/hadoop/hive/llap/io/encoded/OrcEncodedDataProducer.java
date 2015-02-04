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

package org.apache.hadoop.hive.llap.io.encoded;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.cache.Cache;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch.StreamBuffer;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.api.orc.OrcBatchKey;
import org.apache.hadoop.hive.llap.io.api.orc.OrcCacheKey;
import org.apache.hadoop.hive.llap.io.metadata.OrcFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.OrcMetadataCache;
import org.apache.hadoop.hive.llap.io.metadata.OrcStripeMetadata;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.SargApplier;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.StripeInformation;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

public class OrcEncodedDataProducer implements EncodedDataProducer<OrcBatchKey> {
  private FileSystem cachedFs = null;
  private Configuration conf;
  private final OrcMetadataCache metadataCache;
  // TODO: it makes zero sense to have both at the same time and duplicate data. Add "cache mode".
  private final Cache<OrcCacheKey> cache;
  private final LowLevelCache lowLevelCache;

  private class OrcEncodedDataReader implements EncodedDataReader<OrcBatchKey>,
    Consumer<EncodedColumnBatch<OrcBatchKey>> {
    private final FileSplit split;
    private List<Integer> columnIds;
    private final SearchArgument sarg;
    private final String[] columnNames;
    private final Consumer<EncodedColumnBatch<OrcBatchKey>> consumer;


    // Read state.
    private int stripeIxFrom;
    private Reader orcReader;
    private final String internedFilePath;
    /**
     * readState[stripeIx'][colIx'] => boolean array (could be a bitmask) of rg-s that need to be
     * read. Contains only stripes that are read, and only columns included. null => read all RGs.
     */
    private boolean[][][] readState;
    private boolean isStopped = false, isPaused = false;

    public OrcEncodedDataReader(InputSplit split, List<Integer> columnIds,
        SearchArgument sarg, String[] columnNames, Consumer<EncodedColumnBatch<OrcBatchKey>> consumer) {
      this.split = (FileSplit)split;
      this.internedFilePath = this.split.getPath().toString().intern();
      this.columnIds = columnIds;
      if (this.columnIds != null) {
        Collections.sort(this.columnIds);
      }
      this.sarg = sarg;
      this.columnNames = columnNames;
      this.consumer = consumer;
    }

    @Override
    public void stop() {
      isStopped = true;
      // TODO: stop fetching if still in progress
    }

    @Override
    public void pause() {
      isPaused = true;
      // TODO: pause fetching
    }

    @Override
    public void unpause() {
      isPaused = false;
      // TODO: unpause fetching
    }

    @Override
    public Void call() throws IOException {
      LlapIoImpl.LOG.info("Processing split for " + internedFilePath);
      if (isStopped) return null;
      orcReader = null;
      // Get FILE metadata from cache, or create the reader and read it.
      OrcFileMetadata metadata = metadataCache.getFileMetadata(internedFilePath);
      if (metadata == null) {
        orcReader = createOrcReader(split);
        metadata = new OrcFileMetadata(orcReader);
        metadataCache.putFileMetadata(internedFilePath, metadata);
      }

      if (columnIds == null) {
        columnIds = new ArrayList<Integer>(metadata.getTypes().size());
        for (int i = 1; i < metadata.getTypes().size(); ++i) {
          columnIds.add(i);
        }
      }

      // Then, determine which stripes to read based on the split.
      determineStripesToRead(metadata.getStripes());
      if (readState.length == 0) {
        consumer.setDone();
        return null; // No data to read.
      }
      int stride = metadata.getRowIndexStride();
      ArrayList<OrcStripeMetadata> stripeMetadatas = null;
      boolean[] globalIncludes = OrcInputFormat.genIncludedColumns(
          metadata.getTypes(), columnIds, true);
      RecordReader[] stripeReaders = new RecordReader[readState.length];
      if (sarg != null && stride != 0) {
        // If SARG is present, get relevant stripe metadata from cache or readers.
        stripeMetadatas = readStripesMetadata(metadata, globalIncludes, stripeReaders);
      }

      // Now, apply SARG if any; w/o sarg, this will just initialize readState.
      determineRgsToRead(metadata.getStripes(), metadata.getTypes(),
          globalIncludes, stride, stripeMetadatas);
      if (isStopped) return null;
      // Get data from high-level cache; if some cols are fully in cache, this will also
      // give us the modified list of columns to read for every stripe (null means all).
      List<Integer>[] stripeColsToRead = produceDataFromCache(metadata.getStripes(), stride);
      // readState has been modified for column x rgs that were fetched from cache.

      // Then, create the readers for each stripe and prepare to read. We will create reader
      // with global column list and then separately pass stripe-specific includes below, to
      // allow it create the batches to return based on the former but only read the latter.
      for (int stripeIxMod = 0; stripeIxMod < stripeReaders.length; ++stripeIxMod) {
        RecordReader stripeReader = stripeReaders[stripeIxMod];
        if (stripeColsToRead != null && stripeColsToRead[stripeIxMod].isEmpty()) {
          if (stripeReader != null) {
            stripeReader.close();
            stripeReaders[stripeIxMod] = null;
          }
          continue; // All the data for this stripe was in cache.
        }

        if (stripeReader != null) continue; // We already have a reader.
        // Create RecordReader that will be used to read only this stripe.
        StripeInformation si = metadata.getStripes().get(stripeIxFrom + stripeIxMod);
        if (orcReader == null) {
          orcReader = createOrcReader(split);
        }
        stripeReader = orcReader.rows(si.getOffset(), si.getLength(), globalIncludes);
        stripeReader.prepareEncodedColumnRead();
        stripeReaders[stripeIxMod] = stripeReader;
      }

      // We now have one reader per stripe that needs to be read. Read.
      // TODO: I/O threadpool would be here - one thread per stripe; for now, linear.
      OrcBatchKey stripeKey = new OrcBatchKey(internedFilePath, -1, 0);
      for (int stripeIxMod = 0; stripeIxMod < stripeReaders.length; ++stripeIxMod) {
        RecordReader stripeReader = stripeReaders[stripeIxMod];
        if (stripeReader == null) continue; // No need to read this stripe, see above.
        List<Integer> colsToRead = stripeColsToRead == null ? null : stripeColsToRead[stripeIxMod];
        boolean[] stripeIncludes = null;
        boolean[][] colRgs = readState[stripeIxMod];
        if (colsToRead == null || colsToRead.size() == colRgs.length) {
          colsToRead = columnIds;
          stripeIncludes = globalIncludes;
        } else {
          // We are reading subset of the original columns, remove unnecessary bitmasks/etc.
          // This will never happen w/o high-level cache.
          stripeIncludes = OrcInputFormat.genIncludedColumns(metadata.getTypes(), colsToRead, true);
          boolean[][] colRgs2 = new boolean[colsToRead.size()][];
          for (int i = 0, i2 = -1; i < colRgs.length; ++i) {
            if (colRgs[i] == null) continue;
            colRgs2[i2] = colRgs[i];
            ++i2;
          }
          colRgs = colRgs2;
        }

        // Get stripe metadata from cache or reader. We might have read it before for RG filtering.
        OrcStripeMetadata stripeMetadata;
        int stripeIx = stripeIxMod + stripeIxFrom;
        if (stripeMetadatas != null) {
          stripeMetadata = stripeMetadatas.get(stripeIxMod);
        } else {
          stripeKey.stripeIx = stripeIx;
          stripeMetadata = metadataCache.getStripeMetadata(stripeKey);
          if (stripeMetadata == null) {
            stripeMetadata = new OrcStripeMetadata(
                stripeReader, stripeKey.stripeIx, stripeIncludes);
            metadataCache.putStripeMetadata(stripeKey, stripeMetadata);
            stripeKey = new OrcBatchKey(internedFilePath, -1, 0);
          }
        }
        // Set stripe metadata externally in the reader.
        stripeReader.setMetadata(stripeMetadata.getRowIndexes(),
            stripeMetadata.getEncodings(), stripeMetadata.getStreams());

        // In case if we have high-level cache, we will intercept the data and add it there;
        // otherwise just pass the data directly to the consumer.
        Consumer<EncodedColumnBatch<OrcBatchKey>> consumer = (cache == null) ? this.consumer : this;
        // This is where I/O happens. This is a sync call that will feed data to the consumer.
        try {
          stripeReader.readEncodedColumns(
              stripeIx, stripeIncludes, colRgs, lowLevelCache, consumer);
        } catch (Throwable t) {
          consumer.setError(t);
        }
        stripeReader.close();
      }

      // Done with all the things.
      consumer.setDone();
      if (DebugUtils.isTraceMttEnabled()) {
        LlapIoImpl.LOG.info("done processing " + split);
      }
      return null;
    }

    private ArrayList<OrcStripeMetadata> readStripesMetadata(OrcFileMetadata metadata,
        boolean[] globalInc, RecordReader[] stripeReaders) throws IOException {
      ArrayList<OrcStripeMetadata> result = new ArrayList<OrcStripeMetadata>(stripeReaders.length);
      OrcBatchKey stripeKey = new OrcBatchKey(internedFilePath, 0, 0);
      for (int stripeIxMod = 0; stripeIxMod < stripeReaders.length; ++stripeIxMod) {
        stripeKey.stripeIx = stripeIxMod + stripeIxFrom;
        OrcStripeMetadata value = metadataCache.getStripeMetadata(stripeKey);
        if (value == null) {
          // Metadata not present in cache - get it from the reader and put in cache.
          if (orcReader == null) {
            orcReader = createOrcReader(split);
          }
          StripeInformation si = metadata.getStripes().get(stripeKey.stripeIx);
          stripeReaders[stripeIxMod] = orcReader.rows(si.getOffset(), si.getLength(), globalInc);
          stripeReaders[stripeIxMod].prepareEncodedColumnRead();
          value = new OrcStripeMetadata(stripeReaders[stripeIxMod], stripeKey.stripeIx, globalInc);
          metadataCache.putStripeMetadata(stripeKey, value);
          // Create new key object to reuse for gets; we've used the old one to put in cache.
          stripeKey = new OrcBatchKey(internedFilePath, 0, 0);
        }
        result.add(value);
      }
      return result;
    }

    @Override
    public void returnData(StreamBuffer data) {
      lowLevelCache.releaseBuffers(data.cacheBuffers);
    }

    private void determineRgsToRead(List<StripeInformation> stripes, List<Type> types,
        boolean[] globalIncludes, int rowIndexStride, ArrayList<OrcStripeMetadata> metadata)
            throws IOException {
      SargApplier sargApp = null;
      if (sarg != null) {
        String[] colNamesForSarg = OrcInputFormat.getSargColumnNames(
            columnNames, types, globalIncludes, OrcInputFormat.isOriginal(orcReader));
        sargApp = new SargApplier(sarg, colNamesForSarg, rowIndexStride);
      }
      // readState should have been initialized by this time with an empty array.
      for (int stripeIxMod = 0; stripeIxMod < readState.length; ++stripeIxMod) {
        int originalStripeIx = stripeIxMod + stripeIxFrom;
        StripeInformation stripe = stripes.get(originalStripeIx);
        int rgCount = getRgCount(stripe, rowIndexStride);
        boolean[] rgsToRead = null;
        if (sargApp != null) {
          rgsToRead = sargApp.pickRowGroups(stripe, metadata.get(stripeIxMod).getRowIndexes());
        }
        assert rgsToRead == null || rgsToRead.length == rgCount;
        readState[stripeIxMod] = new boolean[columnIds.size()][];
        for (int j = 0; j < columnIds.size(); ++j) {
          readState[stripeIxMod][j] = (rgsToRead == null) ? null :
            Arrays.copyOf(rgsToRead, rgsToRead.length);
        }
      }
    }

    private int getRgCount(StripeInformation stripe, int rowIndexStride) {
      return (int)Math.ceil((double)stripe.getNumberOfRows() / rowIndexStride);
    }

    public void determineStripesToRead(List<StripeInformation> stripes) {
      // The unit of caching for ORC is (rg x column) (see OrcBatchKey).
      long offset = split.getStart(), maxOffset = offset + split.getLength();
      stripeIxFrom = -1;
      int stripeIxTo = -1;
      if (LlapIoImpl.LOG.isDebugEnabled()) {
        String tmp = "FileSplit {" + split.getStart() + ", " + split.getLength() + "}; stripes ";
        for (StripeInformation stripe : stripes) {
          tmp += "{" + stripe.getOffset() + ", " + stripe.getLength() + "}, ";
        }
        LlapIoImpl.LOG.debug(tmp);
      }

      int stripeIx = 0;
      for (StripeInformation stripe : stripes) {
        long stripeStart = stripe.getOffset();
        if (offset > stripeStart) continue;
        if (stripeIxFrom == -1) {
          if (DebugUtils.isTraceEnabled()) {
            LlapIoImpl.LOG.info("Including from " + stripeIx
                + " (" + stripeStart + " >= " + offset + ")");
          }
          stripeIxFrom = stripeIx;
        }
        if (stripeStart >= maxOffset) {
          if (DebugUtils.isTraceEnabled()) {
            LlapIoImpl.LOG.info("Including until " + stripeIxTo
                + " (" + stripeStart + " >= " + maxOffset + ")");
          }
          stripeIxTo = stripeIx;
          break;
        }
        ++stripeIx;
      }
      if (stripeIxTo == -1) {
        if (DebugUtils.isTraceEnabled()) {
          LlapIoImpl.LOG.info("Including until " + stripeIx + " (end of file)");
        }
        stripeIxTo = stripeIx;
      }
      readState = new boolean[stripeIxTo - stripeIxFrom][][];
    }

    // TODO: split by stripe? we do everything by stripe, and it might be faster
    private List<Integer>[] produceDataFromCache(
        List<StripeInformation> stripes, int rowIndexStride) {
      if (cache == null) return null;
      OrcCacheKey key = new OrcCacheKey(internedFilePath, -1, -1, -1);
      // For each stripe, keep a list of columns that are not fully in cache (null => all of them).
      @SuppressWarnings("unchecked") // No generics arrays - "J" in "Java" stands for "joke".
      List<Integer>[] stripeColsNotInCache = new List[readState.length];
      for (int stripeIxMod = 0; stripeIxMod < readState.length; ++stripeIxMod) {
        key.stripeIx = stripeIxFrom + stripeIxMod;
        boolean[][] cols = readState[stripeIxMod];
        boolean[] isMissingAnyRgs = new boolean[cols.length];
        int totalRgCount = getRgCount(stripes.get(key.stripeIx), rowIndexStride);
        for (int rgIx = 0; rgIx < totalRgCount; ++rgIx) {
          EncodedColumnBatch<OrcBatchKey> col = new EncodedColumnBatch<OrcBatchKey>(
              new OrcBatchKey(internedFilePath, key.stripeIx, rgIx), cols.length, cols.length);
          boolean hasAnyCached = false;
          key.rgIx = rgIx;
          for (int colIxMod = 0; colIxMod < cols.length; ++colIxMod) {
            boolean[] readMask = cols[colIxMod];
            // Check if RG is eliminated by SARG
            if (readMask != null && (readMask.length <= rgIx || !readMask[rgIx])) continue;
            key.colIx = columnIds.get(colIxMod);
            StreamBuffer[] cached = cache.get(key);
            if (cached == null) {
              isMissingAnyRgs[colIxMod] = true;
              continue;
            }
            col.setAllStreams(colIxMod, key.colIx, cached);
            hasAnyCached = true;
            if (readMask == null) {
              // We were going to read all RGs, but now that some were in cache, allocate the mask.
              cols[colIxMod] = readMask = new boolean[totalRgCount];
              Arrays.fill(readMask, true);
            }
            readMask[rgIx] = false; // Got from cache, don't read from disk.
          }
          if (hasAnyCached) {
            consumer.consumeData(col);
          }
        }
        boolean makeStripeColList = false; // By default assume we'll fetch all original columns.
        for (int colIxMod = 0; colIxMod < cols.length; ++colIxMod) {
          if (isMissingAnyRgs[colIxMod]) {
            if (makeStripeColList) {
              stripeColsNotInCache[stripeIxMod].add(columnIds.get(colIxMod));
            }
          } else if (!makeStripeColList) {
            // Some columns were fully in cache. Make a per-stripe col list, add previous columns.
            makeStripeColList = true;
            stripeColsNotInCache[stripeIxMod] = new ArrayList<Integer>(cols.length - 1);
            for (int i = 0; i < colIxMod; ++i) {
              stripeColsNotInCache[stripeIxMod].add(columnIds.get(i));
            }
          }
        }
      }
      return stripeColsNotInCache;
    }

    @Override
    public void setDone() {
      consumer.setDone();
    }

    @Override
    public void consumeData(EncodedColumnBatch<OrcBatchKey> data) {
      // Store object in cache; create new key object - cannot be reused.
      assert cache != null;
      for (int i = 0; i < data.columnData.length; ++i) {
        OrcCacheKey key = new OrcCacheKey(data.batchKey, data.columnIxs[i]);
        StreamBuffer[] toCache = data.columnData[i];
        StreamBuffer[] cached = cache.cacheOrGet(key, toCache);
        if (toCache != cached) {
          for (StreamBuffer sb : toCache) {
            if (sb.decRef() != 0) continue;
            lowLevelCache.releaseBuffers(sb.cacheBuffers);
          }
          data.columnData[i] = cached;
        }
      }
      consumer.consumeData(data);
    }

    @Override
    public void setError(Throwable t) {
      consumer.setError(t);
    }
  }

  private Reader createOrcReader(FileSplit fileSplit) throws IOException {
    FileSystem fs = cachedFs;
    Path path = fileSplit.getPath();
    if ("pfile".equals(path.toUri().getScheme())) {
      fs = path.getFileSystem(conf); // Cannot use cached FS due to hive tests' proxy FS.
    }
    return OrcFile.createReader(path, OrcFile.readerOptions(conf).filesystem(fs));
  }

  public OrcEncodedDataProducer(LowLevelCache lowLevelCache, Cache<OrcCacheKey> cache,
      Configuration conf) throws IOException {
    // We assume all splits will come from the same FS.
    this.cachedFs = FileSystem.get(conf);
    this.cache = cache;
    this.lowLevelCache = lowLevelCache;
    this.conf = conf;
    this.metadataCache = new OrcMetadataCache();
  }

  @Override
  public EncodedDataReader<OrcBatchKey> getReader(InputSplit split, List<Integer> columnIds,
      SearchArgument sarg, String[] columnNames, Consumer<EncodedColumnBatch<OrcBatchKey>> consumer) {
    return new OrcEncodedDataReader(split, columnIds, sarg, columnNames, consumer);
  }
}
