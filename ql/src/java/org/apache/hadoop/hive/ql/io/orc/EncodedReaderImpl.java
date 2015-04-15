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
import java.util.Arrays;
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
import org.apache.hadoop.hive.ql.io.orc.InStream.TrackedCacheChunk;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndex;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndexEntry;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.CacheChunk;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderUtils.ByteBufferAllocatorPool;
import org.apache.hadoop.hive.shims.HadoopShims.ZeroCopyReaderShim;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.apache.hive.common.util.FixedSizedObjectPool.PoolObjectHelper;


/**
 * Encoded reader implementation.
 *
 * Note about refcounts on cache blocks.
 * When we get or put blocks into cache, they are "locked" (refcount++). After that, we send the
 * blocks out to processor as part of RG data; one block can be used for multiple RGs. In some
 * cases, one block is sent for ALL rgs (e.g. a dictionary for string column). This is how we deal
 * with this:
 * For non-dictionary case:
 * 1) At all times, every buffer has +1 refcount for each time we sent this block to processing.
 * 2) When processor is done with an RG, it decrefs for all the blocks involved.
 * 3) Additionally, we keep an extra +1 refcount "for the fetching thread". That way, if we send
 *    the block to processor, and the latter decrefs it, the block won't be evicted when we want
 *    to reuse it for some other RG, forcing us to do an extra disk read or cache lookup.
 * 4) As we read (we always read RGs in order, and assume they are stored in physical order in the
 *    file, plus that RGs are not shared between streams, AND that we read each stream from the
 *    beginning), we note which blocks cannot possibly be reused anymore (next RG starts in the
 *    next CB). We decref for the refcount from (3) in such case.
 * 5) Given that RG end boundary in ORC is an estimate, so we can request data from cache and then
 *    not use it, at the end we go thru all the blocks, and release those not released by (4).
 * For dictionary case:
 * 1) We have a separate refcount on the ColumnBuffer object we send to the processor. In the above
 *    case, it's always 1, so when processor is done it goes directly to decrefing cache buffers.
 * 2) In the dictionary case, it's increased per RG, and processors don't touch cache buffers if
 *    they do not happen to decref this counter to 0.
 * 3) This is done because dictionary can have many buffers; decrefing all of them for all RGs
 *    is more expensive; plus, decrefing in cache may be more expensive due to cache policy/etc.
 */
public class EncodedReaderImpl implements EncodedReader {
  public static final Log LOG = LogFactory.getLog(EncodedReaderImpl.class);
  private static final FixedSizedObjectPool<ColumnReadContext> COLCTX_POOL =
      new FixedSizedObjectPool<>(256, new FixedSizedObjectPool.PoolObjectHelper<ColumnReadContext>() {
        @Override
        public ColumnReadContext create() {
          return new ColumnReadContext();
        }
        @Override
        public void resetBeforeOffer(ColumnReadContext t) {
          t.reset();
        }
      });
  private static final FixedSizedObjectPool<StreamContext> STREAMCTX_POOL =
      new FixedSizedObjectPool<>(256, new FixedSizedObjectPool.PoolObjectHelper<StreamContext>() {
        @Override
        public StreamContext create() {
          return new StreamContext();
        }
        @Override
        public void resetBeforeOffer(StreamContext t) {
          t.reset();
        }
      });
  public static final FixedSizedObjectPool<OrcEncodedColumnBatch> ECB_POOL =
      new FixedSizedObjectPool<>(1024, new PoolObjectHelper<OrcEncodedColumnBatch>() {
        @Override
        protected OrcEncodedColumnBatch create() {
          return new OrcEncodedColumnBatch();
        }
        @Override
        protected void resetBeforeOffer(OrcEncodedColumnBatch t) {
          t.reset();
        }
      });
  public static final FixedSizedObjectPool<StreamBuffer> SB_POOL =
      new FixedSizedObjectPool<>(8192, new PoolObjectHelper<StreamBuffer>() {
        @Override
        protected StreamBuffer create() {
          return new StreamBuffer();
        }
        @Override
        protected void resetBeforeOffer(StreamBuffer t) {
          t.reset();
        }
      });
  private final long fileId;
  private final FSDataInputStream file;
  private final CompressionCodec codec;
  private final int bufferSize;
  private final List<OrcProto.Type> types;
  private final ZeroCopyReaderShim zcr;
  private final long rowIndexStride;
  private final LowLevelCache cache;
  private final ByteBufferAllocatorPool pool;
  // For now, one consumer for all calls.
  private final Consumer<OrcEncodedColumnBatch> consumer;


  public EncodedReaderImpl(FileSystem fileSystem, Path path, long fileId, boolean useZeroCopy,
      List<OrcProto.Type> types, CompressionCodec codec, int bufferSize, long strideRate,
      LowLevelCache cache, Consumer<OrcEncodedColumnBatch> consumer)
          throws IOException {
    this.fileId = fileId;
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
    public void init(int colIx, ColumnEncoding encoding, RowIndex rowIndex) {
      this.encoding = encoding;
      this.rowIndex = rowIndex;
      this.colIx = colIx;
      streamCount = 0;
    }
    public void reset() {
      encoding = null;
      rowIndex = null;
      streamCount = 0;
      Arrays.fill(streams, null);
    }
    public static final int MAX_STREAMS = OrcProto.Stream.Kind.ROW_INDEX_VALUE;
    /** The number of streams that are part of this column. */
    int streamCount = 0;
    final StreamContext[] streams = new StreamContext[MAX_STREAMS];
    /** Column encoding. */
    ColumnEncoding encoding;
    /** Column rowindex. */
    OrcProto.RowIndex rowIndex;
    /** Column index in the file. */
    int colIx;

    public void addStream(long offset, OrcProto.Stream stream, int indexIx) {
      StreamContext sctx = streams[streamCount++] = STREAMCTX_POOL.take();
      sctx.init(stream, offset, indexIx);
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
    public void init(OrcProto.Stream stream, long streamOffset, int streamIndexOffset) {
      this.kind = stream.getKind();
      this.length = stream.getLength();
      this.offset = streamOffset;
      this.streamIndexOffset = streamIndexOffset;
    }
    void reset() {
      bufferIter = null;
      stripeLevelStream = null;
      kind = null;
    }
    /** Offsets of each stream in the column. */
    public long offset, length;
    public int streamIndexOffset;
    public OrcProto.Stream.Kind kind;
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

  public static final class OrcEncodedColumnBatch extends EncodedColumnBatch<OrcBatchKey> {
    public static final int ALL_RGS = -1;
    public void init(long fileId, int stripeIx, int rgIx, int columnCount) {
      if (batchKey == null) {
        batchKey = new OrcBatchKey(fileId, stripeIx, rgIx);
      } else {
        batchKey.set(fileId, stripeIx, rgIx);
      }
      if (columnIxs == null || columnCount != columnIxs.length) {
        columnIxs = new int[columnCount];
        columnData = new StreamBuffer[columnCount][];
      }
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
    DiskRangeListMutateHelper toRead = null;
    DiskRangeListCreateHelper listToRead = new DiskRangeListCreateHelper();
    boolean hasIndexOnlyCols = false;
    for (OrcProto.Stream stream : streamList) {
      long length = stream.getLength();
      int colIx = stream.getColumn();
      OrcProto.Stream.Kind streamKind = stream.getKind();
      if (!included[colIx] || StreamName.getArea(streamKind) != StreamName.Area.DATA) {
        hasIndexOnlyCols = hasIndexOnlyCols | included[colIx];
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
        ctx = colCtxs[colRgIx] = COLCTX_POOL.take();
        ctx.init(colIx, encodings.get(colIx), indexes[colIx]);
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

    if (listToRead.get() == null) {
      // No data to read for this stripe. Check if we have some included index-only columns.
      if (hasIndexOnlyCols) {
        OrcEncodedColumnBatch ecb = ECB_POOL.take();
        ecb.init(fileId, stripeIx, OrcEncodedColumnBatch.ALL_RGS, colRgs.length);
        consumer.consumeData(ecb);
      } else {
        LOG.warn("Nothing to read for stripe [" + stripe + "]");
      }
      releaseContexts(colCtxs);
      return;
    }

    // 2. Now, read all of the ranges from cache or disk.
    toRead = new DiskRangeListMutateHelper(listToRead.get());
    if (DebugUtils.isTraceOrcEnabled()) {
      LOG.info("Resulting disk ranges to read (file " + fileId + "): "
          + RecordReaderUtils.stringifyDiskRanges(toRead.next));
    }
    if (cache != null) {
      cache.getFileData(fileId, toRead.next, stripeOffset, InStream.CC_FACTORY);
      if (DebugUtils.isTraceOrcEnabled()) {
        LOG.info("Disk ranges after cache (file " + fileId + ", base offset " + stripeOffset
            + "): " + RecordReaderUtils.stringifyDiskRanges(toRead.next));
      }
    }

    // Force direct buffers if we will be decompressing to direct cache.
    RecordReaderUtils.readDiskRanges(file, zcr, stripeOffset, toRead.next, cache.isDirectAlloc());

    if (DebugUtils.isTraceOrcEnabled()) {
      LOG.info("Disk ranges after disk read (file " + fileId + ", base offset " + stripeOffset
            + "): " + RecordReaderUtils.stringifyDiskRanges(toRead.next));
    }

    // 3. Finally, decompress data, map per RG, and return to caller.
    // We go by RG and not by column because that is how data is processed.
    int rgCount = (int)Math.ceil((double)stripe.getNumberOfRows() / rowIndexStride);
    DiskRangeList iter = toRead.next; // Keep "toRead" list for future use, don't extract().
    for (int rgIx = 0; rgIx < rgCount; ++rgIx) {
      boolean isLastRg = rgIx == rgCount - 1;
      // Create the batch we will use to return data for this RG.
      OrcEncodedColumnBatch ecb = ECB_POOL.take();
      ecb.init(fileId, stripeIx, rgIx, colRgs.length);
      boolean isRGSelected = true;
      for (int colIxMod = 0; colIxMod < colRgs.length; ++colIxMod) {
        if (colRgs[colIxMod] != null && !colRgs[colIxMod][rgIx]) {
          // RG x col filtered.
          isRGSelected = false;
          continue; // TODO: this would be invalid with HL cache, where RG x col can be excluded.
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
              sctx.stripeLevelStream = SB_POOL.take();
              sctx.stripeLevelStream.init(sctx.kind.getNumber());
              // We will be using this for each RG while also sending RGs to processing.
              // To avoid buffers being unlocked, run refcount one ahead; we will not increase
              // it when building the last RG, so each RG processing will decref once, and the
              // last one will unlock the buffers.
              sctx.stripeLevelStream.incRef();
              // For stripe-level streams we don't need the extra refcount on the block. See class comment about refcounts.
              long unlockUntilCOffset = sctx.offset + sctx.length;
              DiskRangeList lastCached = InStream.uncompressStream(fileId, stripeOffset, iter,
                  sctx.offset, sctx.offset + sctx.length, zcr, codec, bufferSize, cache,
                  sctx.stripeLevelStream, unlockUntilCOffset, sctx.offset);
              if (lastCached != null) {
                iter = lastCached;
              }
            }
            if (!isLastRg) {
              sctx.stripeLevelStream.incRef();
            }
            cb = sctx.stripeLevelStream;
          } else {
            // This stream can be separated by RG using index. Let's do that.
            long cOffset = sctx.offset + index.getPositions(sctx.streamIndexOffset);
            long nextCOffsetRel = isLastRg ? sctx.length
                : nextIndex.getPositions(sctx.streamIndexOffset);
            long endCOffset = sctx.offset + RecordReaderUtils.estimateRgEndOffset(
                    isCompressed, isLastRg, nextCOffsetRel, sctx.length, bufferSize);
            // See class comment about refcounts.
            long unlockUntilCOffset = sctx.offset + nextCOffsetRel;
            cb = SB_POOL.take();
            cb.init(sctx.kind.getNumber());
            cb.incRef();
            if (DebugUtils.isTraceOrcEnabled()) {
              LOG.info("Getting data for column "+ ctx.colIx + " " + (isLastRg ? "last " : "")
                  + "RG " + rgIx + " stream " + sctx.kind  + " at " + sctx.offset + ", "
                  + sctx.length + " index position " + sctx.streamIndexOffset + ": compressed ["
                  + cOffset + ", " + endCOffset + ")");
            }
            boolean isStartOfStream = sctx.bufferIter == null;
            DiskRangeList range = isStartOfStream ? iter : sctx.bufferIter;
            DiskRangeList lastCached = InStream.uncompressStream(fileId, stripeOffset, range,
                cOffset, endCOffset, zcr, codec, bufferSize, cache, cb, unlockUntilCOffset,
                sctx.offset);
            if (lastCached != null) {
              sctx.bufferIter = iter = lastCached; // Reset iter just to ensure it's valid
            }
          }
          ecb.setStreamData(colIxMod, streamIx, cb);
        }
      }
      if (isRGSelected) {
        consumer.consumeData(ecb);
      }
    }
    releaseContexts(colCtxs);

    if (DebugUtils.isTraceOrcEnabled()) {
      LOG.info("Disk ranges after processing all the data "
          + RecordReaderUtils.stringifyDiskRanges(toRead.next));
    }

    // Release the unreleased buffers. See class comment about refcounts.
    releaseInitialRefcounts(toRead.next);
    InStream.releaseCacheChunksIntoObjectPool(toRead.next);
  }


  private void releaseContexts(ColumnReadContext[] colCtxs) {
    // Return all contexts to the pools.
    for (ColumnReadContext ctx : colCtxs) {
      if (ctx == null) continue;
      for (int i = 0; i < ctx.streamCount; ++i) {
        StreamContext sctx = ctx.streams[i];
        if (sctx == null) continue;
        STREAMCTX_POOL.offer(sctx);
      }
      COLCTX_POOL.offer(ctx);
    }
  }


  private void releaseInitialRefcounts(DiskRangeList current) {
    while (current != null) {
      DiskRangeList toFree = current;
      current = current.next;
      if (!(toFree instanceof TrackedCacheChunk)) continue;
      TrackedCacheChunk cc = (TrackedCacheChunk)toFree;
      if (cc.isReleased) continue;
      LlapMemoryBuffer buffer = ((CacheChunk)toFree).buffer;
      if (DebugUtils.isTraceLockingEnabled()) {
        LOG.info("Unlocking " + buffer + " for the fetching thread at the end");
      }
      cache.releaseBuffer(buffer);
      cc.isReleased = true;
    }
  }


  @Override
  public void close() throws IOException {
    try {
      file.close();
    } catch (IOException ex) {
      // Tez might have closed our filesystem. Log and ignore error.
      LOG.info("Failed to close file; ignoring: " + ex.getMessage());
    }
    if (pool != null) {
      pool.clear();
    }
  }
}
