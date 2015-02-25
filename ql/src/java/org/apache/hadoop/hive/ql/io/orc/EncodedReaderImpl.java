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
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.DiskRangeList;
import org.apache.hadoop.hive.common.DiskRangeList.DiskRangeListCreateHelper;
import org.apache.hadoop.hive.common.DiskRangeList.DiskRangeListMutateHelper;
import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch.StreamBuffer;
import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.io.api.orc.OrcBatchKey;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndex;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndexEntry;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.CacheChunk;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderUtils.ByteBufferAllocatorPool;
import org.apache.hadoop.hive.shims.HadoopShims.ZeroCopyReaderShim;


public class EncodedReaderImpl implements EncodedReader {
  private static final Log LOG = LogFactory.getLog(EncodedReaderImpl.class);

  private final String fileName;
  private final FSDataInputStream file;
  private final CompressionCodec codec;
  private final int bufferSize;
  private final List<OrcProto.Type> types;
  private final ZeroCopyReaderShim zcr;
  private final long rowIndexStride;
  private final LowLevelCache cache;
  private final ByteBufferAllocatorPool pool;
  // For now, one consumer for all calls.
  private final Consumer<EncodedColumnBatch<OrcBatchKey>> consumer;

  public EncodedReaderImpl(FileSystem fileSystem, Path path, boolean useZeroCopy,
      List<OrcProto.Type> types, CompressionCodec codec, int bufferSize, long strideRate,
      LowLevelCache cache, Consumer<EncodedColumnBatch<OrcBatchKey>> consumer)
          throws IOException {
    this.fileName = path.toString().intern(); // should we normalize this, like DFS would?
    this.file = fileSystem.open(path);
    this.codec = codec;
    this.types = types;
    this.bufferSize = bufferSize;
    this.pool = useZeroCopy ? new ByteBufferAllocatorPool() : null;
    this.zcr = useZeroCopy ? RecordReaderUtils.createZeroCopyShim(file, codec, pool) : null;
    this.rowIndexStride = strideRate;
    this.cache = cache;
    this.consumer = consumer;
    if (zcr != null && !cache.isDirectAlloc()) {
      throw new UnsupportedOperationException("Cannot use zero-copy reader with non-direct cache "
          + "buffers; either disable zero-copy or enable direct cache allocation");
    }
  }


  /** Helper context for each column being read */
  private static final class ColumnReadContext {
    public ColumnReadContext(int colIx, ColumnEncoding encoding, RowIndex rowIndex) {
      this.encoding = encoding;
      this.rowIndex = rowIndex;
      this.colIx = colIx;
    }
    public static final int MAX_STREAMS = OrcProto.Stream.Kind.ROW_INDEX_VALUE;
    /** The number of streams that are part of this column. */
    int streamCount = 0;
    final StreamContext[] streams = new StreamContext[MAX_STREAMS];
    /** Column encoding. */
    final ColumnEncoding encoding;
    /** Column rowindex. */
    final OrcProto.RowIndex rowIndex;
    /** Column index in the file. */
    final int colIx;

    public void addStream(long offset, OrcProto.Stream stream, int indexIx) {
      streams[streamCount++] = new StreamContext(stream, offset, indexIx);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" column_index: ").append(colIx);
      sb.append(" encoding: ").append(encoding);
      sb.append(" stream_count: ").append(streamCount);
      int i = 0;
      for (StreamContext sc : streams) {
        if (sc != null) {
          sb.append(" stream_").append(i).append(":").append(sc.toString());
        }
        i++;
      }
      return sb.toString();
    }
  }

  private static final class StreamContext {
    public StreamContext(OrcProto.Stream stream, long streamOffset, int streamIndexOffset) {
      this.kind = stream.getKind();
      this.length = stream.getLength();
      this.offset = streamOffset;
      this.streamIndexOffset = streamIndexOffset;
    }
    /** Offsets of each stream in the column. */
    public final long offset, length;
    public final int streamIndexOffset;
    public final OrcProto.Stream.Kind kind;
    /** Iterators for the buffers; used to maintain position in per-rg reading. */
    DiskRangeList bufferIter;
    /** Saved stripe-level stream, to reuse for each RG (e.g. dictionaries). */
    StreamBuffer stripeLevelStream;

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" kind: ").append(kind);
      sb.append(" offset: ").append(offset);
      sb.append(" length: ").append(length);
      sb.append(" index_offset: ").append(streamIndexOffset);
      return sb.toString();
    }
  }

  @Override
  public void readEncodedColumns(int stripeIx, StripeInformation stripe,
      RowIndex[] indexes, List<ColumnEncoding> encodings, List<Stream> streamList,
      boolean[] included, boolean[][] colRgs) throws IOException {
    // Note: for now we don't have to setError here, caller will setError if we throw.
    // We are also not supposed to call setDone, since we are only part of the operation.
    long stripeOffset = stripe.getOffset();
    // 1. Figure out what we have to read.
    long offset = 0; // Stream offset in relation to the stripe.
    // 1.1. Figure out which columns have a present stream
    boolean[] hasNull = RecordReaderUtils.findPresentStreamsByColumn(streamList, types);
    if (DebugUtils.isTraceOrcEnabled()) {
      LOG.info("The following columns have PRESENT streams: " + DebugUtils.toString(hasNull));
    }

    // We assume stream list is sorted by column and that non-data
    // streams do not interleave data streams for the same column.
    // 1.2. With that in mind, determine disk ranges to read/get from cache (not by stream).
    int colRgIx = -1, lastColIx = -1;
    ColumnReadContext[] colCtxs = new ColumnReadContext[colRgs.length];
    boolean[] includedRgs = null;
    boolean isCompressed = (codec != null);

    DiskRangeListCreateHelper listToRead = new DiskRangeListCreateHelper();
    for (OrcProto.Stream stream : streamList) {
      long length = stream.getLength();
      int colIx = stream.getColumn();
      OrcProto.Stream.Kind streamKind = stream.getKind();
      if (!included[colIx] || StreamName.getArea(streamKind) != StreamName.Area.DATA) {
        if (DebugUtils.isTraceOrcEnabled()) {
          LOG.info("Skipping stream: " + streamKind + " at " + offset + ", " + length);
        }
        offset += length;
        continue;
      }
      ColumnReadContext ctx = null;
      if (lastColIx != colIx) {
        ++colRgIx;
        assert colCtxs[colRgIx] == null;
        lastColIx = colIx;
        includedRgs = colRgs[colRgIx];
        ctx = colCtxs[colRgIx] = new ColumnReadContext(
            colIx, encodings.get(colIx), indexes[colIx]);
        if (DebugUtils.isTraceOrcEnabled()) {
          LOG.info("Creating context " + colRgIx + " for column " + colIx + ":" + ctx.toString());
        }
      } else {
        ctx = colCtxs[colRgIx];
        assert ctx != null;
      }
      int indexIx = RecordReaderUtils.getIndexPosition(ctx.encoding.getKind(),
          types.get(colIx).getKind(), streamKind, isCompressed, hasNull[colIx]);
      ctx.addStream(offset, stream, indexIx);
      if (DebugUtils.isTraceOrcEnabled()) {
        LOG.info("Adding stream for column " + colIx + ": " + streamKind + " at " + offset
            + ", " + length + ", index position " + indexIx);
      }
      if (includedRgs == null || RecordReaderUtils.isDictionary(streamKind, encodings.get(colIx))) {
        RecordReaderUtils.addEntireStreamToRanges(offset, length, listToRead, true);
        if (DebugUtils.isTraceOrcEnabled()) {
          LOG.info("Will read whole stream " + streamKind + "; added to " + listToRead.getTail());
        }
      } else {
        RecordReaderUtils.addRgFilteredStreamToRanges(stream, includedRgs,
            codec != null, indexes[colIx], encodings.get(colIx), types.get(colIx),
            bufferSize, hasNull[colIx], offset, length, listToRead, true);
      }
      offset += length;
    }

    // 2. Now, read all of the ranges from cache or disk.
    DiskRangeListMutateHelper toRead = new DiskRangeListMutateHelper(listToRead.get());
    if (DebugUtils.isTraceOrcEnabled()) {
      LOG.info("Resulting disk ranges to read: " + RecordReaderUtils.stringifyDiskRanges(toRead));
    }
    if (cache != null) {
      cache.getFileData(fileName, toRead.next, stripeOffset);
      if (DebugUtils.isTraceOrcEnabled()) {
        LOG.info("Disk ranges after cache (base offset " + stripeOffset
            + "): " + RecordReaderUtils.stringifyDiskRanges(toRead));
      }
    }

    // Force direct buffers if we will be decompressing to direct cache.
    RecordReaderUtils.readDiskRanges(file, zcr, stripeOffset, toRead.next, cache.isDirectAlloc());

    if (DebugUtils.isTraceOrcEnabled()) {
      LOG.info("Disk ranges after disk read  (base offset " + stripeOffset
            + "): " + RecordReaderUtils.stringifyDiskRanges(toRead.next));
    }

    // 3. Finally, decompress data, map per RG, and return to caller.
    // We go by RG and not by column because that is how data is processed.
    int rgCount = (int)Math.ceil((double)stripe.getNumberOfRows() / rowIndexStride);
    DiskRangeList iter = toRead.next; // Keep "toRead" list for future use, don't extract().
    for (int rgIx = 0; rgIx < rgCount; ++rgIx) {
      boolean isLastRg = rgIx == rgCount - 1;
      // Create the batch we will use to return data for this RG.
      EncodedColumnBatch<OrcBatchKey> ecb = new EncodedColumnBatch<OrcBatchKey>(
          new OrcBatchKey(fileName, stripeIx, rgIx), colRgs.length, 0);
      boolean isRGSelected = true;
      for (int colIxMod = 0; colIxMod < colRgs.length; ++colIxMod) {
        if (colRgs[colIxMod] != null && !colRgs[colIxMod][rgIx]) {
          // RG x col filtered.
          isRGSelected = false;
          continue; // TODO#: this would be invalid with HL cache, where RG x col can be excluded.
        }
        ColumnReadContext ctx = colCtxs[colIxMod];
        RowIndexEntry index = ctx.rowIndex.getEntry(rgIx),
            nextIndex = isLastRg ? null : ctx.rowIndex.getEntry(rgIx + 1);
        ecb.initColumn(colIxMod, ctx.colIx, ctx.streamCount);
        for (int streamIx = 0; streamIx < ctx.streamCount; ++streamIx) {
          StreamContext sctx = ctx.streams[streamIx];
          StreamBuffer cb = null;
          if (RecordReaderUtils.isDictionary(sctx.kind, ctx.encoding)) {
            // This stream is for entire stripe and needed for every RG; uncompress once and reuse.
            if (DebugUtils.isTraceOrcEnabled()) {
              LOG.info("Getting stripe-level stream [" + sctx.kind + ", " + ctx.encoding + "] for"
                  + " column " + ctx.colIx + " RG " + rgIx + " at " + sctx.offset + ", " + sctx.length);
            }
            if (sctx.stripeLevelStream == null) {
              sctx.stripeLevelStream = new StreamBuffer(sctx.kind.getNumber());
              // We will be using this for each RG while also sending RGs to processing.
              // To avoid buffers being unlocked, run refcount one ahead; we will not increase
              // it when building the last RG, so each RG processing will decref once, and the
              // last one will unlock the buffers.
              sctx.stripeLevelStream.incRef();
              iter = InStream.uncompressStream(fileName, stripeOffset, iter, sctx.offset,
                  sctx.offset + sctx.length, zcr, codec, bufferSize, cache, sctx.stripeLevelStream);
            }
            if (!isLastRg) {
              sctx.stripeLevelStream.incRef();
            }
            cb = sctx.stripeLevelStream;
          } else {
            // This stream can be separated by RG using index. Let's do that.
            long cOffset = index.getPositions(sctx.streamIndexOffset) + sctx.offset,
                nextCOffset = isLastRg ? sctx.length : nextIndex.getPositions(sctx.streamIndexOffset),
                endCOffset = RecordReaderUtils.estimateRgEndOffset(
                    isCompressed, isLastRg, nextCOffset, sctx.length, bufferSize) + sctx.offset;
            cb = new StreamBuffer(sctx.kind.getNumber());
            cb.incRef();
            if (DebugUtils.isTraceOrcEnabled()) {
              LOG.info("Getting data for column "+ ctx.colIx + " " + (isLastRg ? "last " : "")
                  + "RG " + rgIx + " stream " + sctx.kind  + " at " + sctx.offset + ", "
                  + sctx.length + " index position " + sctx.streamIndexOffset + ": compressed ["
                  + cOffset + ", " + endCOffset + ")");
            }
            boolean isStartOfStream = sctx.bufferIter == null;
            DiskRangeList range = isStartOfStream ? iter : sctx.bufferIter;
            DiskRangeList next = InStream.uncompressStream(fileName, stripeOffset, range, cOffset,
                endCOffset, zcr, codec, bufferSize, cache, cb);
            sctx.bufferIter = iter = next; // Reset iter just to ensure it's valid
          }
          ecb.setStreamData(colIxMod, streamIx, cb);
        }
      }
      if (isRGSelected) {
        consumer.consumeData(ecb);
      }
    }

    // TODO: this is not good; we hold all the blocks until we send them all.
    //       Hard to avoid due to sharing by RGs... perhaps we can still do better.
    DiskRangeList toFree = toRead.next;
    while (toFree != null) {
      if (toFree instanceof CacheChunk) {
        LlapMemoryBuffer buffer = ((CacheChunk)toFree).buffer;
        if (DebugUtils.isTraceLockingEnabled()) {
          LOG.info("Unlocking " + buffer + " at the end of readEncodedColumns");
        }
        cache.releaseBuffer(buffer);
      }
      toFree = toFree.next;
    }
  }

  @Override
  public void close() throws IOException {
    file.close();
    if (pool != null) {
      pool.clear();
    }
  }
}
