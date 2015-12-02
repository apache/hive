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
package org.apache.hadoop.hive.ql.io.orc.encoded;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.Pool;
import org.apache.hadoop.hive.common.Pool.PoolObjectHelper;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.DataCache.BooleanRef;
import org.apache.hadoop.hive.common.io.DataCache.DiskRangeListFactory;
import org.apache.hadoop.hive.common.io.DiskRangeList.CreateHelper;
import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch.ColumnStreamData;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.orc.CompressionCodec;
import org.apache.orc.DataReader;
import org.apache.orc.OrcConf;
import org.apache.orc.impl.OutStream;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderUtils;
import org.apache.orc.impl.StreamName;
import org.apache.orc.StripeInformation;
import org.apache.orc.impl.BufferChunk;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderUtils.ByteBufferAllocatorPool;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader.OrcEncodedColumnBatch;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader.PoolFactory;
import org.apache.orc.OrcProto;

/**
 * Encoded reader implementation.
 *
 * Note about refcounts on cache blocks.
 * When we get or put blocks into cache, they are "locked" (refcount++), so they cannot be evicted.
 * We send the MemoryBuffer-s to caller as part of RG data; one MemoryBuffer can be used for many
 * RGs (e.g. a dictionary, or multiple RGs per block). Also, we want to "unlock" MemoryBuffer-s in
 * cache as soon as possible. This is how we deal with this:
 *
 * For dictionary case:
 * 1) There's a separate refcount on the ColumnStreamData object we send to the caller. In the
 *    dictionary case, it's increased per RG, and callers don't release MBs if the containing
 *    ColumnStreamData is not ready to be released. This is done because dictionary can have many
 *    buffers; decrefing all of them for all RGs is more expensive; plus, decrefing in cache
 *    may be more expensive due to cache policy/etc.
 *
 * For non-dictionary case:
 * 1) All the ColumnStreamData-s for normal data always have refcount 1; we return them once.
 * 2) At all times, every MB in such cases has +1 refcount for each time we return it as part of CSD.
 * 3) When caller is done, it therefore decrefs SB to 0, and decrefs all the MBs involved.
 * 4) Additionally, we keep an extra +1 refcount "for the fetching thread". That way, if we return
 *    the MB to caller, and he decrefs it, the MB can't be evicted and will be there if we want to
 *    reuse it for some other RG.
 * 5) As we read (we always read RGs in order and forward in each stream; we assume they are stored
 *    physically in order in the file; AND that CBs are not shared between streams), we note which
 *    MBs cannot possibly be reused anymore (next RG starts in the next CB). We decref the refcount
 *    from (4) in such case.
 * 6) Given that RG end boundaries in ORC are estimates, we can request data from cache and then
 *    not use it; thus, at the end we go thru all the MBs, and release those not released by (5).
 */
class EncodedReaderImpl implements EncodedReader {
  public static final Logger LOG = LoggerFactory.getLogger(EncodedReaderImpl.class);
  private static final Object POOLS_CREATION_LOCK = new Object();
  private static Pools POOLS;
  private static class Pools {
    Pool<CacheChunk> tccPool;
    Pool<ProcCacheChunk> pccPool;
    Pool<OrcEncodedColumnBatch> ecbPool;
    Pool<ColumnStreamData> csdPool;
  }
  private final static DiskRangeListFactory CC_FACTORY = new DiskRangeListFactory() {
        @Override
        public DiskRangeList createCacheChunk(MemoryBuffer buffer, long offset, long end) {
          CacheChunk tcc = POOLS.tccPool.take();
          tcc.init(buffer, offset, end);
          return tcc;
        }
      };
  private final Long fileId;
  private final DataReader dataReader;
  private boolean isDataReaderOpen = false;
  private final CompressionCodec codec;
  private final int bufferSize;
  private final List<OrcProto.Type> types;
  private final long rowIndexStride;
  private final DataCache cache;
  private ByteBufferAllocatorPool pool;
  private boolean isDebugTracingEnabled;

  public EncodedReaderImpl(long fileId, List<OrcProto.Type> types, CompressionCodec codec,
      int bufferSize, long strideRate, DataCache cache, DataReader dataReader, PoolFactory pf)
          throws IOException {
    this.fileId = fileId;
    this.codec = codec;
    this.types = types;
    this.bufferSize = bufferSize;
    this.rowIndexStride = strideRate;
    this.cache = cache;
    this.dataReader = dataReader;
    if (POOLS != null) return;
    if (pf == null) {
      pf = new NoopPoolFactory();
    }
    Pools pools = createPools(pf);
    synchronized (POOLS_CREATION_LOCK) {
      if (POOLS != null) return;
      POOLS = pools;
    }
  }

  /** Helper context for each column being read */
  private static final class ColumnReadContext {
    public ColumnReadContext(int colIx, OrcProto.ColumnEncoding encoding,
                             OrcProto.RowIndex rowIndex) {
      this.encoding = encoding;
      this.rowIndex = rowIndex;
      this.colIx = colIx;
      streamCount = 0;
    }

    public static final int MAX_STREAMS = OrcProto.Stream.Kind.ROW_INDEX_VALUE;
    /** The number of streams that are part of this column. */
    int streamCount = 0;
    final StreamContext[] streams = new StreamContext[MAX_STREAMS];
    /** Column encoding. */
    OrcProto.ColumnEncoding encoding;
    /** Column rowindex. */
    OrcProto.RowIndex rowIndex;
    /** Column index in the file. */
    int colIx;

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
    public long offset, length;
    public int streamIndexOffset;
    public OrcProto.Stream.Kind kind;
    /** Iterators for the buffers; used to maintain position in per-rg reading. */
    DiskRangeList bufferIter;
    /** Saved stripe-level stream, to reuse for each RG (e.g. dictionaries). */
    ColumnStreamData stripeLevelStream;

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
      OrcProto.RowIndex[] indexes, List<OrcProto.ColumnEncoding> encodings, List<OrcProto.Stream> streamList,
      boolean[] included, boolean[][] colRgs,
      Consumer<OrcEncodedColumnBatch> consumer) throws IOException {
    // Note: for now we don't have to setError here, caller will setError if we throw.
    // We are also not supposed to call setDone, since we are only part of the operation.
    long stripeOffset = stripe.getOffset();
    // 1. Figure out what we have to read.
    long offset = 0; // Stream offset in relation to the stripe.
    // 1.1. Figure out which columns have a present stream
    boolean[] hasNull = RecordReaderUtils.findPresentStreamsByColumn(streamList, types);
    if (isDebugTracingEnabled) {
      LOG.info("The following columns have PRESENT streams: " + arrayToString(hasNull));
    }

    // We assume stream list is sorted by column and that non-data
    // streams do not interleave data streams for the same column.
    // 1.2. With that in mind, determine disk ranges to read/get from cache (not by stream).
    int colRgIx = -1, lastColIx = -1;
    ColumnReadContext[] colCtxs = new ColumnReadContext[colRgs.length];
    boolean[] includedRgs = null;
    boolean isCompressed = (codec != null);
    CreateHelper listToRead = new CreateHelper();
    boolean hasIndexOnlyCols = false;
    for (OrcProto.Stream stream : streamList) {
      long length = stream.getLength();
      int colIx = stream.getColumn();
      OrcProto.Stream.Kind streamKind = stream.getKind();
      if (!included[colIx] || StreamName.getArea(streamKind) != StreamName.Area.DATA) {
        // We have a stream for included column, but in future it might have no data streams.
        // It's more like "has at least one column included that has an index stream".
        hasIndexOnlyCols = hasIndexOnlyCols | included[colIx];
        if (isDebugTracingEnabled) {
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
        if (isDebugTracingEnabled) {
          LOG.info("Creating context " + colRgIx + " for column " + colIx + ":" + ctx.toString());
        }
      } else {
        ctx = colCtxs[colRgIx];
        assert ctx != null;
      }
      int indexIx = RecordReaderUtils.getIndexPosition(ctx.encoding.getKind(),
          types.get(colIx).getKind(), streamKind, isCompressed, hasNull[colIx]);
      ctx.addStream(offset, stream, indexIx);
      if (isDebugTracingEnabled) {
        LOG.info("Adding stream for column " + colIx + ": " + streamKind + " at " + offset
            + ", " + length + ", index position " + indexIx);
      }
      if (includedRgs == null || RecordReaderUtils.isDictionary(streamKind, encodings.get(colIx))) {
        RecordReaderUtils.addEntireStreamToRanges(offset, length, listToRead, true);
        if (isDebugTracingEnabled) {
          LOG.info("Will read whole stream " + streamKind + "; added to " + listToRead.getTail());
        }
      } else {
        RecordReaderUtils.addRgFilteredStreamToRanges(stream, includedRgs,
            codec != null, indexes[colIx], encodings.get(colIx), types.get(colIx),
            bufferSize, hasNull[colIx], offset, length, listToRead, true);
      }
      offset += length;
    }

    boolean hasFileId = this.fileId != null;
    long fileId = hasFileId ? this.fileId : 0;
    if (listToRead.get() == null) {
      // No data to read for this stripe. Check if we have some included index-only columns.
      // TODO: there may be a bug here. Could there be partial RG filtering on index-only column?
      if (hasIndexOnlyCols && (includedRgs == null)) {
        OrcEncodedColumnBatch ecb = POOLS.ecbPool.take();
        ecb.init(fileId, stripeIx, OrcEncodedColumnBatch.ALL_RGS, colRgs.length);
        consumer.consumeData(ecb);
      } else {
        LOG.warn("Nothing to read for stripe [" + stripe + "]");
      }
      return;
    }

    // 2. Now, read all of the ranges from cache or disk.
    DiskRangeList.MutateHelper toRead = new DiskRangeList.MutateHelper(listToRead.get());
    if (isDebugTracingEnabled && LOG.isInfoEnabled()) {
      LOG.info("Resulting disk ranges to read (file " + fileId + "): "
          + RecordReaderUtils.stringifyDiskRanges(toRead.next));
    }
    BooleanRef isAllInCache = new BooleanRef();
    if (hasFileId) {
      cache.getFileData(fileId, toRead.next, stripeOffset, CC_FACTORY, isAllInCache);
      if (isDebugTracingEnabled && LOG.isInfoEnabled()) {
        LOG.info("Disk ranges after cache (file " + fileId + ", base offset " + stripeOffset
            + "): " + RecordReaderUtils.stringifyDiskRanges(toRead.next));
      }
    }

    if (!isAllInCache.value) {
      if (!isDataReaderOpen) {
        this.dataReader.open();
        isDataReaderOpen = true;
      }
      dataReader.readFileData(toRead.next, stripeOffset, cache.getAllocator().isDirectAlloc());
    }

    // 3. For uncompressed case, we need some special processing before read.
    DiskRangeList iter = toRead.next;  // Keep "toRead" list for future use, don't extract().
    if (codec == null) {
      for (int colIxMod = 0; colIxMod < colRgs.length; ++colIxMod) {
        ColumnReadContext ctx = colCtxs[colIxMod];
        for (int streamIx = 0; streamIx < ctx.streamCount; ++streamIx) {
          StreamContext sctx = ctx.streams[streamIx];
          DiskRangeList newIter = preReadUncompressedStream(
              stripeOffset, iter, sctx.offset, sctx.offset + sctx.length);
          if (newIter != null) {
            iter = newIter;
          }
        }
      }
      if (isDebugTracingEnabled) {
        LOG.info("Disk ranges after pre-read (file " + fileId + ", base offset "
            + stripeOffset + "): " + RecordReaderUtils.stringifyDiskRanges(toRead.next));
      }
      iter = toRead.next; // Reset the iter to start.
    }

    // 4. Finally, decompress data, map per RG, and return to caller.
    // We go by RG and not by column because that is how data is processed.
    int rgCount = (int)Math.ceil((double)stripe.getNumberOfRows() / rowIndexStride);
    for (int rgIx = 0; rgIx < rgCount; ++rgIx) {
      boolean isLastRg = rgIx == rgCount - 1;
      // Create the batch we will use to return data for this RG.
      OrcEncodedColumnBatch ecb = POOLS.ecbPool.take();
      ecb.init(fileId, stripeIx, rgIx, colRgs.length);
      boolean isRGSelected = true;
      for (int colIxMod = 0; colIxMod < colRgs.length; ++colIxMod) {
        if (colRgs[colIxMod] != null && !colRgs[colIxMod][rgIx]) {
          // RG x col filtered.
          isRGSelected = false;
          continue; // TODO: this would be invalid with HL cache, where RG x col can be excluded.
        }
        ColumnReadContext ctx = colCtxs[colIxMod];
        OrcProto.RowIndexEntry index = ctx.rowIndex.getEntry(rgIx),
            nextIndex = isLastRg ? null : ctx.rowIndex.getEntry(rgIx + 1);
        ecb.initColumn(colIxMod, ctx.colIx, OrcEncodedColumnBatch.MAX_DATA_STREAMS);
        for (int streamIx = 0; streamIx < ctx.streamCount; ++streamIx) {
          StreamContext sctx = ctx.streams[streamIx];
          ColumnStreamData cb = null;
          if (RecordReaderUtils.isDictionary(sctx.kind, ctx.encoding)) {
            // This stream is for entire stripe and needed for every RG; uncompress once and reuse.
            if (isDebugTracingEnabled) {
              LOG.info("Getting stripe-level stream [" + sctx.kind + ", " + ctx.encoding + "] for"
                  + " column " + ctx.colIx + " RG " + rgIx + " at " + sctx.offset + ", " + sctx.length);
            }
            if (sctx.stripeLevelStream == null) {
              sctx.stripeLevelStream = POOLS.csdPool.take();
              // We will be using this for each RG while also sending RGs to processing.
              // To avoid buffers being unlocked, run refcount one ahead; we will not increase
              // it when building the last RG, so each RG processing will decref once, and the
              // last one will unlock the buffers.
              sctx.stripeLevelStream.incRef();
              // For stripe-level streams we don't need the extra refcount on the block.
              // See class comment about refcounts.
              long unlockUntilCOffset = sctx.offset + sctx.length;
              DiskRangeList lastCached = readEncodedStream(stripeOffset, iter,
                  sctx.offset, sctx.offset + sctx.length, sctx.stripeLevelStream,
                  unlockUntilCOffset, sctx.offset);
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
            // Offset to where this RG begins.
            long cOffset = sctx.offset + index.getPositions(sctx.streamIndexOffset);
            // Offset relative to the beginning of the stream of where this RG ends.
            long nextCOffsetRel = isLastRg ? sctx.length
                : nextIndex.getPositions(sctx.streamIndexOffset);
            // Offset before which this RG is guaranteed to end. Can only be estimated.
            // We estimate the same way for compressed and uncompressed for now.
            long endCOffset = sctx.offset + RecordReaderUtils.estimateRgEndOffset(
                isCompressed, isLastRg, nextCOffsetRel, sctx.length, bufferSize);
            // As we read, we can unlock initial refcounts for the buffers that end before
            // the data that we need for this RG.
            long unlockUntilCOffset = sctx.offset + nextCOffsetRel;
            cb = createRgColumnStreamData(
                rgIx, isLastRg, ctx.colIx, sctx, cOffset, endCOffset, isCompressed);
            boolean isStartOfStream = sctx.bufferIter == null;
            DiskRangeList lastCached = readEncodedStream(stripeOffset,
                (isStartOfStream ? iter : sctx.bufferIter), cOffset, endCOffset, cb,
                unlockUntilCOffset, sctx.offset);
            if (lastCached != null) {
              sctx.bufferIter = iter = lastCached;
            }
          }
          ecb.setStreamData(colIxMod, sctx.kind.getNumber(), cb);
        }
      }
      if (isRGSelected) {
        consumer.consumeData(ecb);
      }
    }

    if (isDebugTracingEnabled) {
      LOG.info("Disk ranges after preparing all the data "
          + RecordReaderUtils.stringifyDiskRanges(toRead.next));
    }

    // Release the unreleased buffers. See class comment about refcounts.
    releaseInitialRefcounts(toRead.next);
    releaseCacheChunksIntoObjectPool(toRead.next);
  }


  private static String arrayToString(boolean[] a) {
    StringBuilder b = new StringBuilder();
    b.append('[');
    for (int i = 0; i < a.length; ++i) {
      b.append(a[i] ? "1" : "0");
    }
    b.append(']');
    return b.toString();
  }


  private ColumnStreamData createRgColumnStreamData(int rgIx, boolean isLastRg,
      int colIx, StreamContext sctx, long cOffset, long endCOffset, boolean isCompressed) {
    ColumnStreamData cb = POOLS.csdPool.take();
    cb.incRef();
    if (isDebugTracingEnabled) {
      LOG.info("Getting data for column "+ colIx + " " + (isLastRg ? "last " : "")
          + "RG " + rgIx + " stream " + sctx.kind  + " at " + sctx.offset + ", "
          + sctx.length + " index position " + sctx.streamIndexOffset + ": " +
          (isCompressed ? "" : "un") + "compressed [" + cOffset + ", " + endCOffset + ")");
    }
    return cb;
  }

  private void releaseInitialRefcounts(DiskRangeList current) {
    while (current != null) {
      DiskRangeList toFree = current;
      current = current.next;
      if (!(toFree instanceof CacheChunk)) continue;
      CacheChunk cc = (CacheChunk)toFree;
      if (cc.getBuffer() == null) continue;
      MemoryBuffer buffer = cc.getBuffer();
      cache.releaseBuffer(buffer);
      cc.setBuffer(null);
    }
  }

  @Override
  public void setDebugTracing(boolean isEnabled) {
    this.isDebugTracingEnabled = isEnabled;
  }


  @Override
  public void close() throws IOException {
    dataReader.close();
    if (pool != null) {
      pool.clear();
    }
  }

  /**
   * Fake cache chunk used for uncompressed data. Used in preRead for uncompressed files.
   * Makes assumptions about preRead code; for example, we add chunks here when they are
   * already in the linked list, without unlinking. So, we record the start position in the
   * original list, and then, when someone adds the next element, we merely increase the number
   * of elements one has to traverse from that position to get the whole list.
   */
  private static class UncompressedCacheChunk extends CacheChunk {
    private BufferChunk chunk;
    private int count;

    public UncompressedCacheChunk(BufferChunk bc) {
      super();
      init(null, bc.getOffset(), bc.getEnd());
      chunk = bc;
      count = 1;
    }

    public void addChunk(BufferChunk bc) {
      assert bc.getOffset() == this.getEnd();
      this.end = bc.getEnd();
      ++count;
    }

    public BufferChunk getChunk() {
      return chunk;
    }

    public int getCount() {
      return count;
    }

    @Override
    public void handleCacheCollision(DataCache cache,
        MemoryBuffer replacementBuffer, List<MemoryBuffer> cacheBuffers) {
      assert cacheBuffers == null;
      // This is done at pre-read stage where there's nothing special w/refcounts. Just release.
      cache.getAllocator().deallocate(getBuffer());
      // Replace the buffer in our big range list, as well as in current results.
      this.setBuffer(replacementBuffer);
    }

    public void clear() {
      this.chunk = null;
      this.count = -1;
    }
  }

  /**
   * CacheChunk that is pre-created for new cache data; initially, it contains an original disk
   * buffer and an unallocated MemoryBuffer object. Before we expose it, the MB is allocated,
   * the data is decompressed, and original compressed data is discarded. The chunk lives on in
   * the DiskRange list created for the request, and everyone treats it like regular CacheChunk.
   */
  private static class ProcCacheChunk extends CacheChunk {
    public void init(long cbStartOffset, long cbEndOffset, boolean isCompressed,
        ByteBuffer originalData, MemoryBuffer targetBuffer, int originalCbIndex) {
      super.init(targetBuffer, cbStartOffset, cbEndOffset);
      this.isOriginalDataCompressed = isCompressed;
      this.originalData = originalData;
      this.originalCbIndex = originalCbIndex;
    }

    @Override
    public void reset() {
      super.reset();
      this.originalData = null;
    }

    @Override
    public String toString() {
      return super.toString() + ", original is set " + (this.originalData != null)
          + ", buffer was replaced " + (originalCbIndex == -1);
    }

    @Override
    public void handleCacheCollision(DataCache cache, MemoryBuffer replacementBuffer,
        List<MemoryBuffer> cacheBuffers) {
      assert originalCbIndex >= 0;
      // Had the put succeeded for our new buffer, it would have refcount of 2 - 1 from put,
      // and 1 from notifyReused call above. "Old" buffer now has the 1 from put; new buffer
      // is not in cache.
      cache.getAllocator().deallocate(getBuffer());
      cache.reuseBuffer(replacementBuffer);
      // Replace the buffer in our big range list, as well as in current results.
      this.buffer = replacementBuffer;
      cacheBuffers.set(originalCbIndex, replacementBuffer);
      originalCbIndex = -1; // This can only happen once at decompress time.
    }

    /** Original data that will be turned into encoded cache data in this.buffer and reset. */
    private ByteBuffer originalData = null;
    /** Whether originalData is compressed. */
    private boolean isOriginalDataCompressed;
    /** Index of the MemoryBuffer corresponding to this object inside the result list. If we
     * hit a cache collision, we will replace this memory buffer with the one from cache at
     * this index, without having to look for it. */
    private int originalCbIndex;
  }

  /**
   * Uncompresses part of the stream. RGs can overlap, so we cannot just go and decompress
   * and remove what we have returned. We will keep iterator as a "hint" point.
   * @param baseOffset Absolute offset of boundaries and ranges relative to file, for cache keys.
   * @param start Ordered ranges containing file data. Helpful if they point close to cOffset.
   * @param cOffset Start offset to decompress.
   * @param endCOffset End offset to decompress; estimate, partial CBs will be ignored.
   * @param csd Stream data, to add the results.
   * @param unlockUntilCOffset The offset until which the buffers can be unlocked in cache, as
   *                           they will not be used in future calls (see the class comment in
   *                           EncodedReaderImpl about refcounts).
   * @return Last buffer cached during decompression. Cache buffers are never removed from
   *         the master list, so they are safe to keep as iterators for various streams.
   */
  public DiskRangeList readEncodedStream(long baseOffset, DiskRangeList start, long cOffset,
      long endCOffset, ColumnStreamData csd, long unlockUntilCOffset, long streamOffset)
          throws IOException {
    if (csd.getCacheBuffers() == null) {
      csd.setCacheBuffers(new ArrayList<MemoryBuffer>());
    } else {
      csd.getCacheBuffers().clear();
    }
    if (cOffset == endCOffset) return null;
    boolean isCompressed = codec != null;
    List<ProcCacheChunk> toDecompress = null;
    List<ByteBuffer> toRelease = null;
    if (isCompressed) {
      toRelease = !dataReader.isTrackingDiskRanges() ? null : new ArrayList<ByteBuffer>();
      toDecompress = new ArrayList<ProcCacheChunk>();
    }

    // 1. Find our bearings in the stream. Normally, iter will already point either to where we
    // want to be, or just before. However, RGs can overlap due to encoding, so we may have
    // to return to a previous block.
    DiskRangeList current = findExactPosition(start, cOffset);
    if (isDebugTracingEnabled) {
      LOG.info("Starting read for [" + cOffset + "," + endCOffset + ") at " + current);
    }

    CacheChunk lastUncompressed = null;

    // 2. Go thru the blocks; add stuff to results and prepare the decompression work (see below).
    lastUncompressed = isCompressed ?
        prepareRangesForCompressedRead(cOffset, endCOffset, streamOffset,
            unlockUntilCOffset, current, csd, toRelease, toDecompress)
      : prepareRangesForUncompressedRead(
          cOffset, endCOffset, streamOffset, unlockUntilCOffset, current, csd);

    // 3. Allocate the buffers, prepare cache keys.
    // At this point, we have read all the CBs we need to read. cacheBuffers contains some cache
    // data and some unallocated membufs for decompression. toDecompress contains all the work we
    // need to do, and each item points to one of the membufs in cacheBuffers as target. The iter
    // has also been adjusted to point to these buffers instead of compressed data for the ranges.
    if (toDecompress == null) return lastUncompressed; // Nothing to decompress.

    MemoryBuffer[] targetBuffers = new MemoryBuffer[toDecompress.size()];
    DiskRange[] cacheKeys = new DiskRange[toDecompress.size()];
    int ix = 0;
    for (ProcCacheChunk chunk : toDecompress) {
      cacheKeys[ix] = chunk; // Relies on the fact that cache does not actually store these.
      targetBuffers[ix] = chunk.getBuffer();
      ++ix;
    }
    cache.getAllocator().allocateMultiple(targetBuffers, bufferSize);

    // 4. Now decompress (or copy) the data into cache buffers.
    for (ProcCacheChunk chunk : toDecompress) {
      ByteBuffer dest = chunk.getBuffer().getByteBufferRaw();
      if (chunk.isOriginalDataCompressed) {
        decompressChunk(chunk.originalData, codec, dest);
      } else {
        copyUncompressedChunk(chunk.originalData, dest);
      }

      chunk.originalData = null;
      if (isDebugTracingEnabled) {
        LOG.info("Locking " + chunk.getBuffer() + " due to reuse (after decompression)");
      }
      cache.reuseBuffer(chunk.getBuffer());
    }

    // 5. Release original compressed buffers to zero-copy reader if needed.
    if (toRelease != null) {
      assert dataReader.isTrackingDiskRanges();
      for (ByteBuffer buffer : toRelease) {
        dataReader.releaseBuffer(buffer);
      }
    }

    // 6. Finally, put uncompressed data to cache.
    if (fileId != null) {
      long[] collisionMask = cache.putFileData(fileId, cacheKeys, targetBuffers, baseOffset);
      processCacheCollisions(collisionMask, toDecompress, targetBuffers, csd.getCacheBuffers());
    }

    // 7. It may happen that we know we won't use some compression buffers anymore.
    //    Release initial refcounts.
    for (ProcCacheChunk chunk : toDecompress) {
      ponderReleaseInitialRefcount(unlockUntilCOffset, streamOffset, chunk);
    }

    return lastUncompressed;
  }

  private CacheChunk prepareRangesForCompressedRead(long cOffset, long endCOffset,
      long streamOffset, long unlockUntilCOffset, DiskRangeList current, ColumnStreamData columnStreamData,
      List<ByteBuffer> toRelease, List<ProcCacheChunk> toDecompress) throws IOException {
    if (cOffset > current.getOffset()) {
      // Target compression block is in the middle of the range; slice the range in two.
      current = current.split(cOffset).next;
    }
    long currentOffset = cOffset;
    CacheChunk lastUncompressed = null;
    while (true) {
      DiskRangeList next = null;
      if (current instanceof CacheChunk) {
        // 2a. This is a decoded compression buffer, add as is.
        CacheChunk cc = (CacheChunk)current;
        if (isDebugTracingEnabled) {
          LOG.info("Locking " + cc.getBuffer() + " due to reuse");
        }
        cache.reuseBuffer(cc.getBuffer());
        columnStreamData.getCacheBuffers().add(cc.getBuffer());
        currentOffset = cc.getEnd();
        if (isDebugTracingEnabled) {
          LOG.info("Adding an already-uncompressed buffer " + cc.getBuffer());
        }
        ponderReleaseInitialRefcount(unlockUntilCOffset, streamOffset, cc);
        lastUncompressed = cc;
        next = current.next;
      } else if (current instanceof IncompleteCb)  {
        // 2b. This is a known incomplete CB caused by ORC CB end boundaries being estimates.
        if (isDebugTracingEnabled) {
          LOG.info("Cannot read " + current);
        }
        next = null;
        currentOffset = -1;
      } else {
        // 2c. This is a compressed buffer. We need to uncompress it; the buffer can comprise
        // several disk ranges, so we might need to combine them.
        BufferChunk bc = (BufferChunk)current;
        ProcCacheChunk newCached = addOneCompressionBuffer(
            bc, columnStreamData.getCacheBuffers(), toDecompress, toRelease);
        lastUncompressed = (newCached == null) ? lastUncompressed : newCached;
        next = (newCached != null) ? newCached.next : null;
        currentOffset = (next != null) ? next.getOffset() : -1;
      }

      if (next == null || (endCOffset >= 0 && currentOffset >= endCOffset)) {
        break;
      }
      current = next;
    }
    return lastUncompressed;
  }

  private CacheChunk prepareRangesForUncompressedRead(long cOffset, long endCOffset,
      long streamOffset, long unlockUntilCOffset, DiskRangeList current, ColumnStreamData columnStreamData)
          throws IOException {
    long currentOffset = cOffset;
    CacheChunk lastUncompressed = null;
    boolean isFirst = true;
    while (true) {
      DiskRangeList next = null;
      assert current instanceof CacheChunk;
      lastUncompressed = (CacheChunk)current;
      if (isDebugTracingEnabled) {
        LOG.info("Locking " + lastUncompressed.getBuffer() + " due to reuse");
      }
      cache.reuseBuffer(lastUncompressed.getBuffer());
      if (isFirst) {
        columnStreamData.setIndexBaseOffset((int)(lastUncompressed.getOffset() - streamOffset));
        isFirst = false;
      }
      columnStreamData.getCacheBuffers().add(lastUncompressed.getBuffer());
      currentOffset = lastUncompressed.getEnd();
      if (isDebugTracingEnabled) {
        LOG.info("Adding an uncompressed buffer " + lastUncompressed.getBuffer());
      }
      ponderReleaseInitialRefcount(unlockUntilCOffset, streamOffset, lastUncompressed);
      next = current.next;
      if (next == null || (endCOffset >= 0 && currentOffset >= endCOffset)) {
        break;
      }
      current = next;
    }
    return lastUncompressed;
  }

  /**
   * To achieve some sort of consistent cache boundaries, we will cache streams deterministically;
   * in segments starting w/stream start, and going for either stream size or some fixed size.
   * If we are not reading the entire segment's worth of data, then we will not cache the partial
   * RGs; the breakage of cache assumptions (no interleaving blocks, etc.) is way too much PITA
   * to handle just for this case.
   * We could avoid copy in non-zcr case and manage the buffer that was not allocated by our
   * allocator. Uncompressed case is not mainline though so let's not complicate it.
   * @param qfCounters
   */
  private DiskRangeList preReadUncompressedStream(long baseOffset,
      DiskRangeList start, long streamOffset, long streamEnd) throws IOException {
    if (streamOffset == streamEnd) return null;
    List<UncompressedCacheChunk> toCache = null;
    List<ByteBuffer> toRelease = null;

    // 1. Find our bearings in the stream.
    DiskRangeList current = findIntersectingPosition(start, streamOffset, streamEnd);
    if (isDebugTracingEnabled) {
      LOG.info("Starting pre-read for [" + streamOffset + "," + streamEnd + ") at " + current);
    }

    if (streamOffset > current.getOffset()) {
      // Target compression block is in the middle of the range; slice the range in two.
      current = current.split(streamOffset).next;
    }
    // Account for maximum cache buffer size.
    long streamLen = streamEnd - streamOffset;
    int partSize = determineUncompressedPartSize(),
        partCount = (int)(streamLen / partSize) + (((streamLen % partSize) != 0) ? 1 : 0);

    CacheChunk lastUncompressed = null;
    MemoryBuffer[] singleAlloc = new MemoryBuffer[1];
    for (int i = 0; i < partCount; ++i) {
      long partOffset = streamOffset + (i * partSize),
           partEnd = Math.min(partOffset + partSize, streamEnd);
      long hasEntirePartTo = partOffset; // We have 0 bytes of data for this part, for now.
      if (current == null) {
        break; // We have no data from this point on (could be unneeded), skip.
      }
      assert partOffset <= current.getOffset();
      if (partOffset == current.getOffset() && current instanceof CacheChunk) {
        // We assume cache chunks would always match the way we read, so check and skip it.
        assert current.getOffset() == partOffset && current.getEnd() == partEnd;
        lastUncompressed = (CacheChunk)current;
        current = current.next;
        continue;
      }
      if (current.getOffset() >= partEnd) {
        continue; // We have no data at all for this part of the stream (could be unneeded), skip.
      }
      if (toRelease == null && dataReader.isTrackingDiskRanges()) {
        toRelease = new ArrayList<ByteBuffer>();
      }
      // We have some disk buffers... see if we have entire part, etc.
      UncompressedCacheChunk candidateCached = null; // We will cache if we have the entire part.
      DiskRangeList next = current;
      while (true) {
        boolean noMoreDataForPart = (next == null || next.getOffset() >= partEnd);
        if (noMoreDataForPart && hasEntirePartTo < partEnd && candidateCached != null) {
          // We are missing a section at the end of the part... copy the start to non-cached.
          lastUncompressed = copyAndReplaceCandidateToNonCached(
              candidateCached, partOffset, hasEntirePartTo, cache, singleAlloc);
          candidateCached = null;
        }
        current = next;
        if (noMoreDataForPart) break; // Done with this part.

        boolean wasSplit = false;
        if (current.getEnd() > partEnd) {
          // If the current buffer contains multiple parts, split it.
          current = current.split(partEnd);
          wasSplit = true;
        }
        if (isDebugTracingEnabled) {
          LOG.info("Processing uncompressed file data at ["
              + current.getOffset() + ", " + current.getEnd() + ")");
        }
        BufferChunk curBc = (BufferChunk)current;
        if (!wasSplit && toRelease != null) {
          toRelease.add(curBc.getChunk()); // TODO: is it valid to give zcr the modified 2nd part?
        }

        // Track if we still have the entire part.
        long hadEntirePartTo = hasEntirePartTo;
        // We have data until the end of current block if we had it until the beginning.
        hasEntirePartTo = (hasEntirePartTo == current.getOffset()) ? current.getEnd() : -1;
        if (hasEntirePartTo == -1) {
          // We don't have the entire part; copy both whatever we intended to cache, and the rest,
          // to an allocated buffer. We could try to optimize a bit if we have contiguous buffers
          // with gaps, but it's probably not needed.
          if (candidateCached != null) {
            assert hadEntirePartTo != -1;
            copyAndReplaceCandidateToNonCached(
                candidateCached, partOffset, hadEntirePartTo, cache, singleAlloc);
            candidateCached = null;
          }
          lastUncompressed = copyAndReplaceUncompressedToNonCached(curBc, cache, singleAlloc);
          next = lastUncompressed.next; // There may be more data after the gap.
        } else {
          // So far we have all the data from the beginning of the part.
          if (candidateCached == null) {
            candidateCached = new UncompressedCacheChunk(curBc);
          } else {
            candidateCached.addChunk(curBc);
          }
          next = current.next;
        }
      }
      if (candidateCached != null) {
        if (toCache == null) {
          toCache = new ArrayList<>(partCount - i);
        }
        toCache.add(candidateCached);
      }
    }

    // 3. Allocate the buffers, prepare cache keys.
    if (toCache == null) return lastUncompressed; // Nothing to copy and cache.

    MemoryBuffer[] targetBuffers =
        toCache.size() == 1 ? singleAlloc : new MemoryBuffer[toCache.size()];
    targetBuffers[0] = null;
    DiskRange[] cacheKeys = new DiskRange[toCache.size()];
    int ix = 0;
    for (UncompressedCacheChunk chunk : toCache) {
      cacheKeys[ix] = chunk; // Relies on the fact that cache does not actually store these.
      ++ix;
    }
    cache.getAllocator().allocateMultiple(
        targetBuffers, (int)(partCount == 1 ? streamLen : partSize));

    // 4. Now copy the data into cache buffers.
    ix = 0;
    for (UncompressedCacheChunk candidateCached : toCache) {
      candidateCached.setBuffer(targetBuffers[ix]);
      ByteBuffer dest = candidateCached.getBuffer().getByteBufferRaw();
      copyAndReplaceUncompressedChunks(candidateCached, dest, candidateCached);
      candidateCached.clear();
      lastUncompressed = candidateCached;
      ++ix;
    }

    // 5. Release original compressed buffers to zero-copy reader if needed.
    if (toRelease != null) {
      assert dataReader.isTrackingDiskRanges();
      for (ByteBuffer buf : toRelease) {
        dataReader.releaseBuffer(buf);
      }
    }

    // 6. Finally, put uncompressed data to cache.
    if (fileId != null) {
      long[] collisionMask = cache.putFileData(fileId, cacheKeys, targetBuffers, baseOffset);
      processCacheCollisions(collisionMask, toCache, targetBuffers, null);
    }

    return lastUncompressed;
  }


  private int determineUncompressedPartSize() {
    // We will break the uncompressed data in the cache in the chunks that are the size
    // of the prevalent ORC compression buffer (the default), or maximum allocation (since we
    // cannot allocate bigger chunks), whichever is less.
    long orcCbSizeDefault = ((Number)OrcConf.BUFFER_SIZE.getDefaultValue()).longValue();
    int maxAllocSize = cache.getAllocator().getMaxAllocation();
    return (int)Math.min(maxAllocSize, orcCbSizeDefault);
  }

  private static void copyUncompressedChunk(ByteBuffer src, ByteBuffer dest) {
    int startPos = dest.position(), startLim = dest.limit();
    dest.put(src); // Copy uncompressed data to cache.
    // Put moves position forward by the size of the data.
    int newPos = dest.position();
    if (newPos > startLim) {
      throw new AssertionError("After copying, buffer [" + startPos + ", " + startLim
          + ") became [" + newPos + ", " + dest.limit() + ")");
    }
    dest.position(startPos);
    dest.limit(newPos);
  }


  private static CacheChunk copyAndReplaceCandidateToNonCached(
      UncompressedCacheChunk candidateCached, long partOffset,
      long candidateEnd, DataCache cache, MemoryBuffer[] singleAlloc) {
    // We thought we had the entire part to cache, but we don't; convert start to
    // non-cached. Since we are at the first gap, the previous stuff must be contiguous.
    singleAlloc[0] = null;
    cache.getAllocator().allocateMultiple(singleAlloc, (int)(candidateEnd - partOffset));

    MemoryBuffer buffer = singleAlloc[0];
    cache.reuseBuffer(buffer);
    ByteBuffer dest = buffer.getByteBufferRaw();
    CacheChunk tcc = POOLS.tccPool.take();
    tcc.init(buffer, partOffset, candidateEnd);
    copyAndReplaceUncompressedChunks(candidateCached, dest, tcc);
    return tcc;
  }

  private static CacheChunk copyAndReplaceUncompressedToNonCached(
      BufferChunk bc, DataCache cache, MemoryBuffer[] singleAlloc) {
    singleAlloc[0] = null;
    cache.getAllocator().allocateMultiple(singleAlloc, bc.getLength());
    MemoryBuffer buffer = singleAlloc[0];
    cache.reuseBuffer(buffer);
    ByteBuffer dest = buffer.getByteBufferRaw();
    CacheChunk tcc = POOLS.tccPool.take();
    tcc.init(buffer, bc.getOffset(), bc.getEnd());
    copyUncompressedChunk(bc.getChunk(), dest);
    bc.replaceSelfWith(tcc);
    return tcc;
  }

  private static void copyAndReplaceUncompressedChunks(
      UncompressedCacheChunk candidateCached, ByteBuffer dest, CacheChunk tcc) {
    int startPos = dest.position(), startLim = dest.limit();
    DiskRangeList next = null;
    for (int i = 0; i < candidateCached.getCount(); ++i) {
      BufferChunk chunk = (i == 0) ? candidateCached.getChunk() : (BufferChunk)next;
      dest.put(chunk.getData());
      next = chunk.next;
      if (i == 0) {
        chunk.replaceSelfWith(tcc);
      } else {
        chunk.removeSelf();
      }
    }
    int newPos = dest.position();
    if (newPos > startLim) {
      throw new AssertionError("After copying, buffer [" + startPos + ", " + startLim
          + ") became [" + newPos + ", " + dest.limit() + ")");
    }
    dest.position(startPos);
    dest.limit(newPos);
  }

  private static void decompressChunk(
      ByteBuffer src, CompressionCodec codec, ByteBuffer dest) throws IOException {
    int startPos = dest.position(), startLim = dest.limit();
    codec.decompress(src, dest);
    // Codec resets the position to 0 and limit to correct limit.
    dest.position(startPos);
    int newLim = dest.limit();
    if (newLim > startLim) {
      throw new AssertionError("After codec, buffer [" + startPos + ", " + startLim
          + ") became [" + dest.position() + ", " + newLim + ")");
    }
  }

  public static void releaseCacheChunksIntoObjectPool(DiskRangeList current) {
    while (current != null) {
      if (current instanceof ProcCacheChunk) {
        POOLS.pccPool.offer((ProcCacheChunk)current);
      } else if (current instanceof CacheChunk) {
        POOLS.tccPool.offer((CacheChunk)current);
      }
      current = current.next;
    }
  }

  private void ponderReleaseInitialRefcount(
      long unlockUntilCOffset, long streamStartOffset, CacheChunk cc) {
    // Don't release if the buffer contains any data beyond the acceptable boundary.
    if (cc.getEnd() > unlockUntilCOffset) return;
    assert cc.getBuffer() != null;
    try {
      releaseInitialRefcount(cc, false);
    } catch (AssertionError e) {
      LOG.error("BUG: releasing initial refcount; stream start " + streamStartOffset + ", "
          + "unlocking until " + unlockUntilCOffset + " from [" + cc + "]: " + e.getMessage());
      throw e;
    }
    // Release all the previous buffers that we may not have been able to release due to reuse,
    // as long as they are still in the same stream and are not already released.
    DiskRangeList prev = cc.prev;
    while (true) {
      if ((prev == null) || (prev.getEnd() <= streamStartOffset)
          || !(prev instanceof CacheChunk)) break;
      CacheChunk prevCc = (CacheChunk)prev;
      if (prevCc.buffer == null) break;
      try {
        releaseInitialRefcount(prevCc, true);
      } catch (AssertionError e) {
        LOG.error("BUG: releasing initial refcount; stream start " + streamStartOffset + ", "
            + "unlocking until " + unlockUntilCOffset + " from [" + cc + "] and backtracked to ["
            + prevCc + "]: " + e.getMessage());
        throw e;
      }
      prev = prev.prev;
    }
  }

  private void releaseInitialRefcount(CacheChunk cc, boolean isBacktracking) {
    // This is the last RG for which this buffer will be used. Remove the initial refcount
    if (isDebugTracingEnabled) {
      LOG.info("Unlocking " + cc.getBuffer() + " for the fetching thread"
          + (isBacktracking ? "; backtracking" : ""));
    }
    cache.releaseBuffer(cc.getBuffer());
    cc.setBuffer(null);
  }

  private void processCacheCollisions(long[] collisionMask,
      List<? extends CacheChunk> toDecompress, MemoryBuffer[] targetBuffers,
      List<MemoryBuffer> cacheBuffers) {
    if (collisionMask == null) return;
    assert collisionMask.length >= (toDecompress.size() >>> 6);
    // There are some elements that were cached in parallel, take care of them.
    long maskVal = -1;
    for (int i = 0; i < toDecompress.size(); ++i) {
      if ((i & 63) == 0) {
        maskVal = collisionMask[i >>> 6];
      }
      if ((maskVal & 1) == 1) {
        // Cache has found an old buffer for the key and put it into array instead of our new one.
        CacheChunk replacedChunk = toDecompress.get(i);
        MemoryBuffer replacementBuffer = targetBuffers[i];
        if (isDebugTracingEnabled) {
          LOG.info("Discarding data due to cache collision: " + replacedChunk.getBuffer()
              + " replaced with " + replacementBuffer);
        }
        assert replacedChunk.getBuffer() != replacementBuffer : i + " was not replaced in the results "
            + "even though mask is [" + Long.toBinaryString(maskVal) + "]";
        replacedChunk.handleCacheCollision(cache, replacementBuffer, cacheBuffers);
      }
      maskVal >>= 1;
    }
  }


  /** Finds compressed offset in a stream and makes sure iter points to its position.
     This may be necessary for obscure combinations of compression and encoding boundaries. */
  private static DiskRangeList findExactPosition(DiskRangeList ranges, long offset) {
    if (offset < 0) return ranges;
    return findIntersectingPosition(ranges, offset, offset);
  }

  private static DiskRangeList findIntersectingPosition(DiskRangeList ranges, long offset, long end) {
    if (offset < 0) return ranges;
    // We expect the offset to be valid TODO: rather, validate
    while (ranges.getEnd() <= offset) {
      ranges = ranges.next;
    }
    while (ranges.getOffset() > end) {
      ranges = ranges.prev;
    }
    // We are now on some intersecting buffer, find the first intersecting buffer.
    while (ranges.prev != null && ranges.prev.getEnd() > offset) {
      ranges = ranges.prev;
    }
    return ranges;
  }

  private static class IncompleteCb extends DiskRangeList {
    public IncompleteCb(long offset, long end) {
      super(offset, end);
    }

    @Override
    public String toString() {
      return "incomplete CB start: " + offset + " end: " + end;
    }
  }

  /**
   * Reads one compression block from the source; handles compression blocks read from
   * multiple ranges (usually, that would only happen with zcr).
   * Adds stuff to cachedBuffers, toDecompress and toRelease (see below what each does).
   * @param current BufferChunk where compression block starts.
   * @param ranges Iterator of all chunks, pointing at current.
   * @param cacheBuffers The result buffer array to add pre-allocated target cache buffer.
   * @param toDecompress The list of work to decompress - pairs of compressed buffers and the
   *                     target buffers (same as the ones added to cacheBuffers).
   * @param toRelease The list of buffers to release to zcr because they are no longer in use.
   * @return The resulting cache chunk.
   */
  private ProcCacheChunk addOneCompressionBuffer(BufferChunk current,
      List<MemoryBuffer> cacheBuffers, List<ProcCacheChunk> toDecompress,
      List<ByteBuffer> toRelease) throws IOException {
    ByteBuffer slice = null;
    ByteBuffer compressed = current.getChunk();
    long cbStartOffset = current.getOffset();
    int b0 = compressed.get() & 0xff;
    int b1 = compressed.get() & 0xff;
    int b2 = compressed.get() & 0xff;
    int chunkLength = (b2 << 15) | (b1 << 7) | (b0 >> 1);
    if (chunkLength > bufferSize) {
      throw new IllegalArgumentException("Buffer size too small. size = " +
          bufferSize + " needed = " + chunkLength);
    }
    int consumedLength = chunkLength + OutStream.HEADER_SIZE;
    long cbEndOffset = cbStartOffset + consumedLength;
    boolean isUncompressed = ((b0 & 0x01) == 1);
    if (isDebugTracingEnabled) {
      LOG.info("Found CB at " + cbStartOffset + ", chunk length " + chunkLength + ", total "
          + consumedLength + ", " + (isUncompressed ? "not " : "") + "compressed");
    }
    if (compressed.remaining() >= chunkLength) {
      // Simple case - CB fits entirely in the disk range.
      slice = compressed.slice();
      slice.limit(chunkLength);
      ProcCacheChunk cc = addOneCompressionBlockByteBuffer(slice, isUncompressed,
          cbStartOffset, cbEndOffset, chunkLength, current, toDecompress, cacheBuffers);
      if (compressed.remaining() <= 0 && dataReader.isTrackingDiskRanges()) {
        toRelease.add(compressed);
      }
      return cc;
    }
    if (current.getEnd() < cbEndOffset && !current.hasContiguousNext()) {
      addIncompleteCompressionBuffer(cbStartOffset, current, 0);
      return null; // This is impossible to read from this chunk.
    }

    // TODO: we could remove extra copy for isUncompressed case by copying directly to cache.
    // We need to consolidate 2 or more buffers into one to decompress.
    ByteBuffer copy = allocateBuffer(chunkLength, compressed.isDirect());
    int remaining = chunkLength - compressed.remaining();
    int originalPos = compressed.position();
    copy.put(compressed);
    if (isDebugTracingEnabled) {
      LOG.info("Removing partial CB " + current + " from ranges after copying its contents");
    }
    DiskRangeList next = current.next;
    current.removeSelf();
    if (dataReader.isTrackingDiskRanges()) {
      if (originalPos == 0) {
        dataReader.releaseBuffer(compressed); // We copied the entire buffer.
      } else {
        toRelease.add(compressed); // There might be slices depending on this buffer.
      }
    }

    int extraChunkCount = 0;
    while (true) {
      if (!(next instanceof BufferChunk)) {
        throw new IOException("Trying to extend compressed block into uncompressed block " + next);
      }
      compressed = next.getData();
      ++extraChunkCount;
      if (compressed.remaining() >= remaining) {
        // This is the last range for this compression block. Yay!
        slice = compressed.slice();
        slice.limit(remaining);
        copy.put(slice);
        ProcCacheChunk cc = addOneCompressionBlockByteBuffer(copy, isUncompressed,
            cbStartOffset, cbEndOffset, remaining, (BufferChunk)next, toDecompress, cacheBuffers);
        if (compressed.remaining() <= 0 && dataReader.isTrackingDiskRanges()) {
          dataReader.releaseBuffer(compressed); // We copied the entire buffer.
        }
        return cc;
      }
      remaining -= compressed.remaining();
      copy.put(compressed);
      if (dataReader.isTrackingDiskRanges()) {
        dataReader.releaseBuffer(compressed); // We copied the entire buffer.
      }
      DiskRangeList tmp = next;
      next = next.hasContiguousNext() ? next.next : null;
      if (next != null) {
        if (isDebugTracingEnabled) {
          LOG.info("Removing partial CB " + tmp + " from ranges after copying its contents");
        }
        tmp.removeSelf();
      } else {
        addIncompleteCompressionBuffer(cbStartOffset, tmp, extraChunkCount);
        return null; // This is impossible to read from this chunk.
      }
    }
  }

  private void addIncompleteCompressionBuffer(
      long cbStartOffset, DiskRangeList target, int extraChunkCount) {
    IncompleteCb icb = new IncompleteCb(cbStartOffset, target.getEnd());
    if (isDebugTracingEnabled) {
      LOG.info("Replacing " + target + " (and " + extraChunkCount + " previous chunks) with "
          + icb + " in the buffers");
    }
    target.replaceSelfWith(icb);
  }

  /**
   * Add one buffer with compressed data the results for addOneCompressionBuffer (see javadoc).
   * @param fullCompressionBlock (fCB) Entire compression block, sliced or copied from disk data.
   * @param isUncompressed Whether the data in the block is uncompressed.
   * @param cbStartOffset Compressed start offset of the fCB.
   * @param cbEndOffset Compressed end offset of the fCB.
   * @param lastRange The buffer from which the last (or all) bytes of fCB come.
   * @param lastChunkLength The number of compressed bytes consumed from last *chunk* into fullCompressionBlock.
   * @param ranges The iterator of all compressed ranges for the stream, pointing at lastRange.
   * @param lastChunk
   * @param toDecompress See addOneCompressionBuffer.
   * @param cacheBuffers See addOneCompressionBuffer.
   * @return New cache buffer.
   */
  private ProcCacheChunk addOneCompressionBlockByteBuffer(ByteBuffer fullCompressionBlock,
      boolean isUncompressed, long cbStartOffset, long cbEndOffset, int lastChunkLength,
      BufferChunk lastChunk, List<ProcCacheChunk> toDecompress, List<MemoryBuffer> cacheBuffers) {
    // Prepare future cache buffer.
    MemoryBuffer futureAlloc = cache.getAllocator().createUnallocated();
    // Add it to result in order we are processing.
    cacheBuffers.add(futureAlloc);
    // Add it to the list of work to decompress.
    ProcCacheChunk cc = POOLS.pccPool.take();
    cc.init(cbStartOffset, cbEndOffset, !isUncompressed,
        fullCompressionBlock, futureAlloc, cacheBuffers.size() - 1);
    toDecompress.add(cc);
    // Adjust the compression block position.
    if (isDebugTracingEnabled) {
      LOG.info("Adjusting " + lastChunk + " to consume " + lastChunkLength + " compressed bytes");
    }
    lastChunk.getChunk().position(lastChunk.getChunk().position() + lastChunkLength);
    // Finally, put it in the ranges list for future use (if shared between RGs).
    // Before anyone else accesses it, it would have been allocated and decompressed locally.
    if (lastChunk.getChunk().remaining() <= 0) {
      if (isDebugTracingEnabled) {
        LOG.info("Replacing " + lastChunk + " with " + cc + " in the buffers");
      }
      lastChunk.replaceSelfWith(cc);
    } else {
      if (isDebugTracingEnabled) {
        LOG.info("Adding " + cc + " before " + lastChunk + " in the buffers");
      }
      lastChunk.insertPartBefore(cc);
    }
    return cc;
  }

  private static ByteBuffer allocateBuffer(int size, boolean isDirect) {
    return isDirect ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
  }


  private static Pools createPools(PoolFactory pf) {
    Pools pools = new Pools();
    pools.pccPool = pf.createPool(1024, new PoolObjectHelper<ProcCacheChunk>() {
      @Override
      public ProcCacheChunk create() {
        return new ProcCacheChunk();
      }
      @Override
      public void resetBeforeOffer(ProcCacheChunk t) {
        t.reset();
      }
    });
    pools.tccPool = pf.createPool(1024, new PoolObjectHelper<CacheChunk>() {
      @Override
      public CacheChunk create() {
        return new CacheChunk();
      }
      @Override
      public void resetBeforeOffer(CacheChunk t) {
        t.reset();
      }
    });
    pools.ecbPool = pf.createEncodedColumnBatchPool();
    pools.csdPool = pf.createColumnStreamDataPool();
    return pools;
  }

  /** Pool factory that is used if another one isn't specified - just creates the objects. */
  private static class NoopPoolFactory implements PoolFactory {
    @Override
    public <T> Pool<T> createPool(int size, final PoolObjectHelper<T> helper) {
      return new Pool<T>() {
        public void offer(T t) {
        }
        public T take() {
          return helper.create();
        }
      };
    }

    @Override
    public Pool<OrcEncodedColumnBatch> createEncodedColumnBatchPool() {
      return createPool(0, new PoolObjectHelper<OrcEncodedColumnBatch>() {
        @Override
        public OrcEncodedColumnBatch create() {
          return new OrcEncodedColumnBatch();
        }
        @Override
        public void resetBeforeOffer(OrcEncodedColumnBatch t) {
        }
      });
    }

    @Override
    public Pool<ColumnStreamData> createColumnStreamDataPool() {
      return createPool(0, new PoolObjectHelper<ColumnStreamData>() {
        @Override
        public ColumnStreamData create() {
          return new ColumnStreamData();
        }
        @Override
        public void resetBeforeOffer(ColumnStreamData t) {
        }
      });
    }
  }
}
