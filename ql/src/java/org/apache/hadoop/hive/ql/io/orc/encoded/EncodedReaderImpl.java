/*
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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.Pool;
import org.apache.hadoop.hive.common.Pool.PoolObjectHelper;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.DataCache.BooleanRef;
import org.apache.hadoop.hive.common.io.DataCache.DiskRangeListFactory;
import org.apache.hadoop.hive.common.io.DiskRangeList.CreateHelper;
import org.apache.hadoop.hive.common.io.DiskRangeList.MutateHelper;
import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch.ColumnStreamData;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.orc.CompressionCodec;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile.WriterVersion;
import org.apache.orc.OrcProto.ColumnEncoding;
import org.apache.orc.OrcProto.Stream;
import org.apache.orc.OrcProto.Stream.Kind;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcCodecPool;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.RecordReaderUtils;
import org.apache.orc.impl.StreamName;
import org.apache.orc.impl.RecordReaderImpl.SargApplier;
import org.apache.orc.impl.StreamName.Area;
import org.apache.orc.impl.WriterImpl;
import org.apache.orc.StripeInformation;
import org.apache.orc.impl.BufferChunk;
import org.apache.hadoop.hive.ql.io.orc.encoded.IoTrace.RangesSrc;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader.OrcEncodedColumnBatch;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader.PoolFactory;
import org.apache.hive.common.util.CleanerUtil;
import org.apache.orc.OrcProto;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.CodedInputStream;

import static org.apache.hadoop.hive.llap.LlapHiveUtils.throwIfCacheOnlyRead;


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
// Note: this thing should know nothing about ACID or schema. It reads physical columns by index;
//       schema evolution/ACID schema considerations should be on higher level.
class EncodedReaderImpl implements EncodedReader {
  public static final Logger LOG = LoggerFactory.getLogger(EncodedReaderImpl.class);
  private static final Object POOLS_CREATION_LOCK = new Object();
  private static Pools POOLS;
  private static class Pools {
    Pool<OrcEncodedColumnBatch> ecbPool;
    Pool<ColumnStreamData> csdPool;
  }
  private final static DiskRangeListFactory CC_FACTORY = new DiskRangeListFactory() {
    @Override
    public DiskRangeList createCacheChunk(MemoryBuffer buffer, long offset, long end) {
      return new CacheChunk(buffer, offset, end);
    }
  };
  private final Object fileKey;
  private final LlapDataReader dataReader;
  private boolean isDataReaderOpen = false;
  private CompressionCodec codec;
  private final boolean isCodecFromPool;
  private boolean isCodecFailure = false;
  private final boolean isCompressed;
  private final org.apache.orc.CompressionKind compressionKind;
  private final int bufferSize;
  private final List<OrcProto.Type> types;
  private final long rowIndexStride;
  private final DataCache cacheWrapper;
  private boolean isTracingEnabled;
  private final IoTrace trace;
  private final TypeDescription fileSchema;
  private final WriterVersion version;
  private final CacheTag tag;
  private AtomicBoolean isStopped;
  private StoppableAllocator allocator;
  private final boolean isReadCacheOnly;

  public EncodedReaderImpl(Object fileKey, List<OrcProto.Type> types,
      TypeDescription fileSchema, org.apache.orc.CompressionKind kind, WriterVersion version,
      int bufferSize, long strideRate, DataCache cacheWrapper, LlapDataReader dataReader,
      PoolFactory pf, IoTrace trace, boolean useCodecPool, CacheTag tag, boolean isReadCacheOnly) throws IOException {
    this.fileKey = fileKey;
    this.compressionKind = kind;
    this.isCompressed = kind != org.apache.orc.CompressionKind.NONE;
    this.isCodecFromPool = useCodecPool;
    this.codec = useCodecPool ? OrcCodecPool.getCodec(kind) : WriterImpl.createCodec(kind);
    this.types = types;
    this.fileSchema = fileSchema; // Note: this is redundant with types
    this.version = version;
    this.bufferSize = bufferSize;
    this.rowIndexStride = strideRate;
    this.cacheWrapper = cacheWrapper;
    Allocator alloc = cacheWrapper.getAllocator();
    this.allocator = alloc instanceof StoppableAllocator ? (StoppableAllocator) alloc : null;
    this.dataReader = dataReader;
    this.trace = trace;
    this.tag = tag;
    this.isReadCacheOnly = isReadCacheOnly;
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
  private static final class ColumnReadContext extends ReadContext {

    public ColumnReadContext(int colIx, OrcProto.ColumnEncoding encoding,
                             OrcProto.RowIndex rowIndex, int colRgIx) {
      super(colIx, colRgIx, MAX_STREAMS);
      this.encoding = encoding;
      this.rowIndex = rowIndex;
    }

    public static final int MAX_STREAMS = countMaxStreams(Area.DATA);
    /** Column encoding. */
    OrcProto.ColumnEncoding encoding;
    /** Column rowindex. */
    OrcProto.RowIndex rowIndex;

    public void addStream(long offset, OrcProto.Stream stream, int indexIx) {
      streams[streamCount++] = new StreamContext(stream, offset, indexIx);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" column_index: ").append(colIx);
      sb.append(" included_index: ").append(includedIx);
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

  /** Helper context for each column for which the index is being read */
  private static class ReadContext {
    protected ReadContext(int colIx, int colRgIx, int maxStreams) {
      this.colIx = colIx;
      this.includedIx = colRgIx;
      streamCount = 0;
      streams = new StreamContext[maxStreams];
    }

    public ReadContext(int colIx, int colRgIx) {
      this(colIx, colRgIx, MAX_STREAMS);
    }

    public static final int MAX_STREAMS = countMaxStreams(Area.INDEX);
    /** The number of streams that are part of this column. */
    int streamCount = 0;
    final StreamContext[] streams;
    /** Column index in the file. */
    int colIx;
    /** Column index in the included columns only (for RG masks). */
    int includedIx;

    public void addStream(long offset, OrcProto.Stream stream, int indexIx) {
      streams[streamCount++] = new StreamContext(stream, offset, indexIx);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" column_index: ").append(colIx);
      sb.append(" included_index: ").append(includedIx);
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

  /**
   * Given a list of Streams find out which of them have a present stream.
   * (Present stream means that there are Null values)
   * @param streamList a list of streams
   * @param types stream types
   * @return a boolean array indexed by streamIds (true when a stream has Nulls)
   */
  private static boolean[] findPresentStreamsByColumn(
      List<OrcProto.Stream> streamList, List<OrcProto.Type> types) {
    boolean[] hasNull = new boolean[types.size()];
    for(OrcProto.Stream stream: streamList) {
      if (stream.hasKind() && (stream.getKind() == OrcProto.Stream.Kind.PRESENT)) {
        hasNull[stream.getColumn()] = true;
      }
    }
    return hasNull;
  }

  /**
   * Add an entire Stream to the given DiskRange list.
   *
   * @param offset Stream offset in relation to the stripe
   * @param length Stream length
   * @param list DiskRangeList to add range to
   * @param doMergeBuffers whether we want to merge DiskRange offsets
   */
  private static void addEntireStreamToRanges(
      long offset, long length, DiskRangeList.CreateHelper list, boolean doMergeBuffers) {
    list.addOrMerge(offset, offset + length, doMergeBuffers, false);
  }

  /**
   * Add includedRowGroups from a Stream to the given DiskRange list.
   *
   * @param stream
   * @param includedRowGroups
   * @param isCompressed
   * @param index
   * @param encoding
   * @param colType
   * @param compressionSize
   * @param hasNull
   * @param offset
   * @param length
   * @param list
   * @param doMergeBuffers
   */
  private static void addRgFilteredStreamToRanges(OrcProto.Stream stream,
      boolean[] includedRowGroups, boolean isCompressed, OrcProto.RowIndex index,
      OrcProto.ColumnEncoding encoding, TypeDescription.Category colType, int compressionSize, boolean hasNull,
      long offset, long length, DiskRangeList.CreateHelper list, boolean doMergeBuffers) {
    for (int group = 0; group < includedRowGroups.length; ++group) {
      if (!includedRowGroups[group]) continue;
      int posn = RecordReaderUtils.getIndexPosition(
          encoding.getKind(), colType, stream.getKind(), isCompressed, hasNull);
      long start = index.getEntry(group).getPositions(posn);
      final long nextGroupOffset;
      boolean isLast = group == (includedRowGroups.length - 1);
      nextGroupOffset = isLast ? length : index.getEntry(group + 1).getPositions(posn);

      start += offset;
      long end = offset + estimateRgEndOffset(
          isCompressed, isLast, nextGroupOffset, length, compressionSize);
      list.addOrMerge(start, end, doMergeBuffers, true);
    }
  }

  // for uncompressed streams, what is the most overlap with the following set
  // of rows (long vint literal group).
  static final int WORST_UNCOMPRESSED_SLOP = 2 + 8 * 512;

  /**
   * Estimate the RowGroup offset given the next RG offset and isLast.
   *
   * @param isCompressed is the RowGroup compressed
   * @param isLast is this the last RowGroup
   * @param nextGroupOffset the offset of the next RowGroup
   * @param streamLength the length of the whole Stream
   * @param bufferSize the size of the buffer
   * @return
   */
  private static long estimateRgEndOffset(boolean isCompressed, boolean isLast,
      long nextGroupOffset, long streamLength, int bufferSize) {
    // figure out the worst case last location
    // if adjacent groups have the same compressed block offset then stretch the slop
    // by factor of 2 to safely accommodate the next compression block.
    // One for the current compression block and another for the next compression block.
    long slop = isCompressed ? 2 * (OutStream.HEADER_SIZE + bufferSize) : WORST_UNCOMPRESSED_SLOP;
    return isLast ? streamLength : Math.min(streamLength, nextGroupOffset + slop);
  }

  @Override
  public void readEncodedColumns(int stripeIx, StripeInformation stripe,
      OrcProto.RowIndex[] indexes, List<OrcProto.ColumnEncoding> encodings,
      List<OrcProto.Stream> streamList, boolean[] physicalFileIncludes, boolean[] rgs,
      Consumer<OrcEncodedColumnBatch> consumer) throws IOException {
    // Note: for now we don't have to setError here, caller will setError if we throw.
    // We are also not supposed to call setDone, since we are only part of the operation.
    long stripeOffset = stripe.getOffset();
    // 1. Figure out what we have to read.
    long offset = 0; // Stream offset in relation to the stripe.
    // 1.1. Figure out which columns have a present stream
    boolean[] hasNull = findPresentStreamsByColumn(streamList, types);
    if (isTracingEnabled) {
      LOG.trace("The following columns have PRESENT streams: " + arrayToString(hasNull));
    }

    // We assume stream list is sorted by column and that non-data
    // streams do not interleave data streams for the same column.
    // 1.2. With that in mind, determine disk ranges to read/get from cache (not by stream).
    ColumnReadContext[] colCtxs = new ColumnReadContext[physicalFileIncludes.length];
    int colRgIx = -1;
    // Don't create context for the 0-s column.
    for (int i = 1; i < physicalFileIncludes.length; ++i) {
      if (!physicalFileIncludes[i]) continue;
      ColumnEncoding enc = encodings.get(i);
      colCtxs[i] = new ColumnReadContext(i, enc, indexes[i], ++colRgIx);
      if (isTracingEnabled) {
        LOG.trace("Creating context: " + colCtxs[i].toString());
      }
      trace.logColumnRead(i, colRgIx, enc.getKind());
    }
    CreateHelper listToRead = new CreateHelper();
    boolean hasIndexOnlyCols = false, hasAnyNonData = false;
    for (OrcProto.Stream stream : streamList) {
      long length = stream.getLength();
      int colIx = stream.getColumn();
      OrcProto.Stream.Kind streamKind = stream.getKind();
      boolean isIndexCol = StreamName.getArea(streamKind) != StreamName.Area.DATA;
      hasAnyNonData = hasAnyNonData || isIndexCol;
      // We have a stream for included column, but in future it might have no data streams.
      // It's more like "has at least one column included that has an index stream".
      hasIndexOnlyCols = hasIndexOnlyCols || (isIndexCol && physicalFileIncludes[colIx]);
      if (!physicalFileIncludes[colIx] || isIndexCol) {
        if (isTracingEnabled) {
          LOG.trace("Skipping stream for column " + colIx + ": "
              + streamKind + " at " + offset + ", " + length);
        }
        trace.logSkipStream(colIx, streamKind, offset, length);
        offset += length;
        continue;
      }
      ColumnReadContext ctx = colCtxs[colIx];
      assert ctx != null;
      int indexIx = RecordReaderUtils.getIndexPosition(ctx.encoding.getKind(),
          fileSchema.findSubtype(colIx).getCategory(), streamKind, isCompressed, hasNull[colIx]);
      ctx.addStream(offset, stream, indexIx);
      if (isTracingEnabled) {
        LOG.trace("Adding stream for column " + colIx + ": " + streamKind + " at " + offset
            + ", " + length + ", index position " + indexIx);
      }
      if (rgs == null || RecordReaderUtils.isDictionary(streamKind, encodings.get(colIx))) {
        trace.logAddStream(colIx, streamKind, offset, length, indexIx, true);
        addEntireStreamToRanges(offset, length, listToRead, true);
        if (isTracingEnabled) {
          LOG.trace("Will read whole stream " + streamKind + "; added to " + listToRead.getTail());
        }
      } else {
        trace.logAddStream(colIx, streamKind, offset, length, indexIx, false);
        addRgFilteredStreamToRanges(stream, rgs,
            isCompressed, indexes[colIx], encodings.get(colIx), fileSchema.findSubtype(colIx).getCategory(),
            bufferSize, hasNull[colIx], offset, length, listToRead, true);
      }
      offset += length;
    }

    boolean hasFileId = this.fileKey != null;
    if (listToRead.get() == null) {
      // No data to read for this stripe. Check if we have some included index-only columns.
      // For example, count(1) would have the root column, that has no data stream, included.
      // It may also happen that we have a column included with no streams whatsoever. That
      // should only be possible if the file has no index streams.
      boolean hasAnyIncludes = false;
      if (!hasIndexOnlyCols) {
        for (int i = 0; i < physicalFileIncludes.length; ++i) {
          if (!physicalFileIncludes[i]) continue;
          hasAnyIncludes = true;
          break;
        }
      }
      boolean nonProjectionRead = hasIndexOnlyCols || (!hasAnyNonData && hasAnyIncludes);

      // TODO: Could there be partial RG filtering w/no projection?
      //       We should probably just disable filtering for such cases if they exist.
      if (nonProjectionRead && (rgs == SargApplier.READ_ALL_RGS)) {
        OrcEncodedColumnBatch ecb = POOLS.ecbPool.take();
        ecb.init(fileKey, stripeIx, OrcEncodedColumnBatch.ALL_RGS, physicalFileIncludes.length);
        try {
          consumer.consumeData(ecb);
        } catch (InterruptedException e) {
          LOG.error("IO thread interrupted while queueing data");
          throw new IOException(e);
        }
      } else {
        LOG.warn("Nothing to read for stripe [" + stripe + "]");
      }
      return;
    }

    // 2. Now, read all of the ranges from cache or disk.
    IdentityHashMap<ByteBuffer, Boolean> toRelease = new IdentityHashMap<>();
    MutateHelper toRead = getDataFromCacheAndDisk(
        listToRead.get(), stripeOffset, hasFileId, toRelease);


    // 3. For uncompressed case, we need some special processing before read.
    //    Basically, we are trying to create artificial, consistent ranges to cache, as there are
    //    no CBs in an uncompressed file. At the end of this processing, the list would contain
    //    either cache buffers, or buffers allocated by us and not cached (if we are only reading
    //    parts of the data for some ranges and don't want to cache it). Both are represented by
    //    CacheChunks, so the list is just CacheChunk-s from that point on.
    DiskRangeList iter = preReadUncompressedStreams(stripeOffset, colCtxs, toRead, toRelease);

    // 4. Finally, decompress data, map per RG, and return to caller.
    // We go by RG and not by column because that is how data is processed.
    boolean hasError = true;
    try {
      int rgCount = rowIndexStride == 0 ? 1 : (int)Math.ceil((double)stripe.getNumberOfRows() / rowIndexStride);
      for (int rgIx = 0; rgIx < rgCount; ++rgIx) {
        if (rgs != null && !rgs[rgIx]) {
          continue; // RG filtered.
        }
        boolean isLastRg = rgIx == rgCount - 1;
        // Create the batch we will use to return data for this RG.
        OrcEncodedColumnBatch ecb = POOLS.ecbPool.take();
        trace.logStartRg(rgIx);
        boolean hasErrorForEcb = true;
        try {
          ecb.init(fileKey, stripeIx, rgIx, physicalFileIncludes.length);
          for (int colIx = 0; colIx < colCtxs.length; ++colIx) {
            ColumnReadContext ctx = colCtxs[colIx];
            if (ctx == null) continue; // This column is not included

            OrcProto.RowIndexEntry index;
            OrcProto.RowIndexEntry nextIndex;
            // index is disabled
            if (ctx.rowIndex == null) {
              if (isTracingEnabled) {
                LOG.trace("Row index is null. Likely reading a file with indexes disabled.");
              }
              index = null;
              nextIndex = null;
            } else {
              index = ctx.rowIndex.getEntry(rgIx);
              nextIndex = isLastRg ? null : ctx.rowIndex.getEntry(rgIx + 1);
            }
            if (isTracingEnabled) {
              LOG.trace("ctx: {} rgIx: {} isLastRg: {} rgCount: {}", ctx, rgIx, isLastRg, rgCount);
            }
            ecb.initOrcColumn(ctx.colIx);
            trace.logStartCol(ctx.colIx);
            for (int streamIx = 0; streamIx < ctx.streamCount; ++streamIx) {
              StreamContext sctx = ctx.streams[streamIx];
              ColumnStreamData cb = null;
              try {
                if (RecordReaderUtils.isDictionary(sctx.kind, ctx.encoding) || index == null) {
                  // This stream is for entire stripe and needed for every RG; uncompress once and reuse.
                  if (sctx.stripeLevelStream == null) {
                    if (isTracingEnabled) {
                      LOG.trace("Getting stripe-level stream [" + sctx.kind + ", " + ctx.encoding + "] for"
                          + " column " + ctx.colIx + " RG " + rgIx + " at " + sctx.offset + ", " + sctx.length);
                    }
                    trace.logStartStripeStream(sctx.kind);
                    sctx.stripeLevelStream = POOLS.csdPool.take();
                    // We will be using this for each RG while also sending RGs to processing.
                    // To avoid buffers being unlocked, run refcount one ahead; so each RG
                    // processing will decref once, and the last one will unlock the buffers.
                    sctx.stripeLevelStream.incRef();
                    // For stripe-level streams we don't need the extra refcount on the block.
                    // See class comment about refcounts.
                    long unlockUntilCOffset = sctx.offset + sctx.length;
                    DiskRangeList lastCached = readEncodedStream(stripeOffset, iter,
                        sctx.offset, sctx.offset + sctx.length, sctx.stripeLevelStream,
                        unlockUntilCOffset, sctx.offset, toRelease);
                    if (lastCached != null) {
                      iter = lastCached;
                    }
                  }
                  sctx.stripeLevelStream.incRef();
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
                  long endCOffset = sctx.offset + estimateRgEndOffset(
                      isCompressed, isLastRg, nextCOffsetRel, sctx.length, bufferSize);
                  // As we read, we can unlock initial refcounts for the buffers that end before
                  // the data that we need for this RG.
                  long unlockUntilCOffset = sctx.offset + nextCOffsetRel;
                  cb = createRgColumnStreamData(rgIx, isLastRg, ctx.colIx, sctx,
                      cOffset, endCOffset, isCompressed, unlockUntilCOffset);
                  boolean isStartOfStream = sctx.bufferIter == null;
                  DiskRangeList lastCached = readEncodedStream(stripeOffset,
                      (isStartOfStream ? iter : sctx.bufferIter), cOffset, endCOffset, cb,
                      unlockUntilCOffset, sctx.offset, toRelease);
                  if (lastCached != null) {
                    sctx.bufferIter = iter = lastCached;
                  }
                }
              } catch (Exception ex) {
                DiskRangeList drl = toRead == null ? null : toRead.next;
                LOG.error("Error getting stream [" + sctx.kind + ", " + ctx.encoding + "] for"
                    + " column " + ctx.colIx + " RG " + rgIx + " at " + sctx.offset + ", "
                    + sctx.length + "; toRead " + RecordReaderUtils.stringifyDiskRanges(drl), ex);
                throw (ex instanceof IOException) ? (IOException)ex : new IOException(ex);
              } finally {
                // Always add stream data to ecb; releaseEcbRefCountsOnError relies on it.
                // Otherwise, we won't release consumer refcounts for a partially read stream.
                if (cb != null) {
                  ecb.setStreamData(ctx.colIx, sctx.kind.getNumber(), cb);
                }
              }
            }
          }
          hasErrorForEcb = false;
        } finally {
          if (hasErrorForEcb) {
            releaseEcbRefCountsOnError(ecb);
          }
        }
        try {
          consumer.consumeData(ecb);
          // After this, the non-initial refcounts are the responsibility of the consumer.
        } catch (InterruptedException e) {
          LOG.error("IO thread interrupted while queueing data");
          releaseEcbRefCountsOnError(ecb);
          throw new IOException(e);
        }
      }

      if (isTracingEnabled) {
        LOG.trace("Disk ranges after preparing all the data "
            + RecordReaderUtils.stringifyDiskRanges(toRead.next));
      }
      trace.logRanges(fileKey, stripeOffset, toRead.next, RangesSrc.PREREAD);
      hasError = false;
    } finally {
      try {
        // Release the unreleased stripe-level buffers. See class comment about refcounts.
        for (int colIx = 0; colIx < colCtxs.length; ++colIx) {
          ColumnReadContext ctx = colCtxs[colIx];
          if (ctx == null) continue; // This column is not included.
          for (int streamIx = 0; streamIx < ctx.streamCount; ++streamIx) {
            StreamContext sctx = ctx.streams[streamIx];
            if (sctx == null || sctx.stripeLevelStream == null) continue;
            if (0 != sctx.stripeLevelStream.decRef()) continue;
            // Note - this is a little bit confusing; the special treatment of stripe-level buffers
            // is because we run the ColumnStreamData refcount one ahead (as specified above). It
            // may look like this would release the buffers too many times (one release from the
            // consumer, one from releaseInitialRefcounts below, and one here); however, this is
            // merely handling a special case where all the batches that are sharing the stripe-
            // level stream have been processed before we got here; they have all decRef-ed the CSD,
            // but have not released the buffers because of that extra refCount. So, this is
            // essentially the "consumer" refcount being released here.
            for (MemoryBuffer buf : sctx.stripeLevelStream.getCacheBuffers()) {
              LOG.trace("Unlocking {} at the end of processing", buf);
              cacheWrapper.releaseBuffer(buf);
            }
          }
        }
        releaseInitialRefcounts(toRead.next);
        // Release buffers as we are done with all the streams... also see toRelease comment.
        releaseBuffers(toRelease.keySet(), true);
      } catch (Throwable t) {
        if (!hasError) throw new IOException(t);
        LOG.error("Error during the cleanup after another error; ignoring", t);
      }
    }
  }

  private static int countMaxStreams(Area area) {
    int count = 0;
    for (Stream.Kind sk : Stream.Kind.values()) {
      if (StreamName.getArea(sk) == area) {
        ++count;
      }
    }
    return count;
  }

  private DiskRangeList.MutateHelper getDataFromCacheAndDisk(DiskRangeList listToRead,
      long stripeOffset, boolean hasFileId, IdentityHashMap<ByteBuffer, Boolean> toRelease)
          throws IOException {
    DiskRangeList.MutateHelper toRead = new DiskRangeList.MutateHelper(listToRead);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Resulting disk ranges to read (file " + fileKey + "): "
          + RecordReaderUtils.stringifyDiskRanges(toRead.next));
    }
    BooleanRef isAllInCache = new BooleanRef();
    if (hasFileId) {
      cacheWrapper.getFileData(fileKey, toRead.next, stripeOffset, CC_FACTORY, isAllInCache);
      if (!isAllInCache.value) {
        throwIfCacheOnlyRead(isReadCacheOnly);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Disk ranges after cache (found everything " + isAllInCache.value + "; file "
            + fileKey + ", base offset " + stripeOffset  + "): "
            + RecordReaderUtils.stringifyDiskRanges(toRead.next));
      }
      trace.logRanges(fileKey, stripeOffset, toRead.next, RangesSrc.CACHE);
    }

    // TODO: the memory release could be optimized - we could release original buffers after we
    //       are fully done with each original buffer from disk. For now release all at the end;
    //       it doesn't increase the total amount of memory we hold, just the duration a bit.
    //       This is much simpler - we can just remember original ranges after reading them, and
    //       release them at the end. In a few cases where it's easy to determine that a buffer
    //       can be freed in advance, we remove it from the map.
    if (!isAllInCache.value) {
      boolean hasError = true;
      try {
        if (!isDataReaderOpen) {
          this.dataReader.open();
          isDataReaderOpen = true;
        }
        dataReader.readFileData(toRead.next, stripeOffset,
            cacheWrapper.getAllocator().isDirectAlloc());
        DiskRangeList drl = toRead.next;
        while (drl != null) {
          if (drl instanceof BufferChunk) {
            toRelease.put(drl.getData(), true);
          }
          drl = drl.next;
        }
        hasError = false;
      } finally {
        // The FS can be closed from under us if the task is interrupted. Release cache buffers.
        // We are assuming here that toRelease will not be present in such cases.
        if (hasError) {
          releaseInitialRefcounts(toRead.next);
        }
      }
    }
    return toRead;
  }

  private void releaseEcbRefCountsOnError(OrcEncodedColumnBatch ecb) {
    try {
      if (isTracingEnabled) {
        LOG.trace("Unlocking the batch not sent to consumer, on error");
      }
      // We cannot send the ecb to consumer. Discard whatever is already there.
      for (int colIx = 0; colIx < ecb.getTotalColCount(); ++colIx) {
        if (!ecb.hasData(colIx)) continue;
        ColumnStreamData[] datas = ecb.getColumnData(colIx);
        for (ColumnStreamData data : datas) {
          if (data == null || data.decRef() != 0) continue;
          for (MemoryBuffer buf : data.getCacheBuffers()) {
            if (buf == null) continue;
            cacheWrapper.releaseBuffer(buf);
          }
        }
      }
    } catch (Throwable t) {
      LOG.error("Error during the cleanup of an error; ignoring", t);
    }
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


  private ColumnStreamData createRgColumnStreamData(int rgIx, boolean isLastRg, int colIx,
      StreamContext sctx, long cOffset, long endCOffset, boolean isCompressed,
      long unlockUntilCOffset) {
    ColumnStreamData cb = POOLS.csdPool.take();
    cb.incRef();
    if (isTracingEnabled) {
      LOG.trace("Getting data for column "+ colIx + " " + (isLastRg ? "last " : "")
          + "RG " + rgIx + " stream " + sctx.kind  + " at " + sctx.offset + ", "
          + sctx.length + " index position " + sctx.streamIndexOffset + ": " +
          (isCompressed ? "" : "un") + "compressed [" + cOffset + ", " + endCOffset + ")");
    }
    trace.logStartStream(sctx.kind, cOffset, endCOffset, unlockUntilCOffset);
    return cb;
  }

  private void releaseInitialRefcounts(DiskRangeList current) {
    while (current != null) {
      DiskRangeList toFree = current;
      current = current.next;
      if (toFree instanceof ProcCacheChunk) {
        ProcCacheChunk pcc = (ProcCacheChunk)toFree;
        if (pcc.originalData != null) {
          // TODO: can this still happen? we now clean these up explicitly to avoid other issues.
          // This can only happen in case of failure - we read some data, but didn't decompress
          // it. Deallocate the buffer directly, do not decref.
          if (pcc.getBuffer() != null) {
            cacheWrapper.getAllocator().deallocate(pcc.getBuffer());
          }
          continue;
        }
      }
      if (!(toFree instanceof CacheChunk)) continue;
      CacheChunk cc = (CacheChunk)toFree;
      if (cc.getBuffer() == null) continue;
      MemoryBuffer buffer = cc.getBuffer();
      cacheWrapper.releaseBuffer(buffer);
      cc.setBuffer(null);
    }
  }

  @Override
  public void setTracing(boolean isEnabled) {
    this.isTracingEnabled = isEnabled;
  }


  @Override
  public void close() throws IOException {
    try {
      if (codec != null) {
        if (isCodecFromPool && !isCodecFailure) {
          OrcCodecPool.returnCodec(compressionKind, codec);
        } else {
          codec.close();
        }
        codec = null;
      }
    } catch (Exception ex) {
      LOG.error("Ignoring error from codec", ex);
    } finally {
      dataReader.close();
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
      super(null, bc.getOffset(), bc.getEnd());
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
    public void handleCacheCollision(DataCache cacheWrapper,
        MemoryBuffer replacementBuffer, List<MemoryBuffer> cacheBuffers) {
      assert cacheBuffers == null;
      // This is done at pre-read stage where there's nothing special w/refcounts. Just release.
      cacheWrapper.getAllocator().deallocate(getBuffer());
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
    public ProcCacheChunk(long cbStartOffset, long cbEndOffset, boolean isCompressed,
        ByteBuffer originalData, MemoryBuffer targetBuffer, int originalCbIndex) {
      super(targetBuffer, cbStartOffset, cbEndOffset);
      this.isOriginalDataCompressed = isCompressed;
      this.originalData = originalData;
      this.originalCbIndex = originalCbIndex;
    }

    @Override
    public String toString() {
      return super.toString() + ", original is set " + (this.originalData != null)
          + ", buffer was replaced " + (originalCbIndex == -1);
    }

    @Override
    public void handleCacheCollision(DataCache cacheWrapper, MemoryBuffer replacementBuffer,
        List<MemoryBuffer> cacheBuffers) {
      assert originalCbIndex >= 0;
      // Had the put succeeded for our new buffer, it would have refcount of 2 - 1 from put,
      // and 1 from notifyReused call above. "Old" buffer now has the 1 from put; new buffer
      // is not in cache. releaseBuffer will decref the buffer, and also deallocate.
      cacheWrapper.releaseBuffer(this.buffer);
      cacheWrapper.reuseBuffer(replacementBuffer);
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
      long endCOffset, ColumnStreamData csd, long unlockUntilCOffset, long streamOffset,
      IdentityHashMap<ByteBuffer, Boolean> toRelease) throws IOException {
    if (csd.getCacheBuffers() == null) {
      csd.setCacheBuffers(new ArrayList<MemoryBuffer>());
    } else {
      csd.getCacheBuffers().clear();
    }
    if (cOffset == endCOffset) return null;
    List<ProcCacheChunk> toDecompress = null;
    List<IncompleteCb> badEstimates = null;
    List<ByteBuffer> toReleaseCopies = null;
    if (isCompressed) {
      toReleaseCopies = new ArrayList<>();
      toDecompress = new ArrayList<>();
      badEstimates = new ArrayList<>();
    }

    // 1. Find our bearings in the stream. Normally, iter will already point either to where we
    // want to be, or just before. However, RGs can overlap due to encoding, so we may have
    // to return to a previous block.
    DiskRangeList current = findExactPosition(start, cOffset);
    if (isTracingEnabled) {
      LOG.trace("Starting read for [" + cOffset + "," + endCOffset + ") at " + current);
    }
    trace.logStartRead(current);

    CacheChunk lastUncompressed = null;

    // 2. Go thru the blocks; add stuff to results and prepare the decompression work (see below).
    try {
      lastUncompressed = isCompressed ?
          prepareRangesForCompressedRead(cOffset, endCOffset, streamOffset, unlockUntilCOffset,
              current, csd, toRelease, toReleaseCopies, toDecompress, badEstimates)
        : prepareRangesForUncompressedRead(
            cOffset, endCOffset, streamOffset, unlockUntilCOffset, current, csd);
    } catch (Exception ex) {
      LOG.error("Failed " + (isCompressed ? "" : "un") + "compressed read; cOffset " + cOffset
          + ", endCOffset " + endCOffset + ", streamOffset " + streamOffset
          + ", unlockUntilCOffset " + unlockUntilCOffset + "; ranges passed in "
          + RecordReaderUtils.stringifyDiskRanges(start) + "; ranges passed to prepare "
          + RecordReaderUtils.stringifyDiskRanges(current)); // Don't log exception here.
      throw (ex instanceof IOException) ? (IOException)ex : new IOException(ex);
    }

    // 2.5. Remember the bad estimates for future reference.
    if (badEstimates != null && !badEstimates.isEmpty()) {
      // Relies on the fact that cache does not actually store these.
      DiskRange[] cacheKeys = badEstimates.toArray(new DiskRange[badEstimates.size()]);
      long[] result = cacheWrapper.putFileData(fileKey, cacheKeys, null, baseOffset, tag);
      assert result == null; // We don't expect conflicts from bad estimates.
    }

    if (toDecompress == null || toDecompress.isEmpty()) {
      releaseBuffers(toReleaseCopies, false);
      return lastUncompressed; // Nothing to do.
    }

    // 3. Allocate the buffers, prepare cache keys.
    // At this point, we have read all the CBs we need to read. cacheBuffers contains some cache
    // data and some unallocated membufs for decompression. toDecompress contains all the work we
    // need to do, and each item points to one of the membufs in cacheBuffers as target. The iter
    // has also been adjusted to point to these buffers instead of compressed data for the ranges.
    MemoryBuffer[] targetBuffers = new MemoryBuffer[toDecompress.size()];
    DiskRange[] cacheKeys = new DiskRange[toDecompress.size()];
    int ix = 0;
    for (ProcCacheChunk chunk : toDecompress) {
      cacheKeys[ix] = chunk; // Relies on the fact that cache does not actually store these.
      targetBuffers[ix] = chunk.getBuffer();
      ++ix;
    }
    boolean isAllocated = false;
    try {
      allocateMultiple(targetBuffers, bufferSize);
      isAllocated = true;
    } finally {
      // toDecompress/targetBuffers contents are actually already added to some structures that
      // will be cleaned up on error. Remove the unallocated buffers; keep the cached buffers in.
      if (!isAllocated) {
        // Inefficient - this only happens during cleanup on errors.
        for (MemoryBuffer buf : targetBuffers) {
          csd.getCacheBuffers().remove(buf);
        }
        for (ProcCacheChunk chunk : toDecompress) {
          chunk.buffer = null;
        }
      }
    }

    // 4. Now decompress (or copy) the data into cache buffers.
    int decompressedIx = 0;
    try {
      while (decompressedIx < toDecompress.size()) {
        ProcCacheChunk chunk = toDecompress.get(decompressedIx);
        ByteBuffer dest = chunk.getBuffer().getByteBufferRaw();
        if (chunk.isOriginalDataCompressed) {
          boolean isOk = false;
          try {
            decompressChunk(chunk.originalData, codec, dest);
            isOk = true;
          } finally {
            if (!isOk) {
              isCodecFailure = true;
            }
          }
        } else {
          copyUncompressedChunk(chunk.originalData, dest);
        }

        if (isTracingEnabled) {
          LOG.trace("Locking " + chunk.getBuffer() + " due to reuse (after decompression)");
        }
        // After we set originalData to null, we incref the buffer and the cleanup would decref it.
        // Note that this assumes the failure during incref means incref didn't occur.
        try {
          cacheWrapper.reuseBuffer(chunk.getBuffer());
        } finally {
          chunk.originalData = null;
        }
        ++decompressedIx;
      }
    } finally {
      // This will only execute on error. Deallocate the remaining allocated buffers explicitly.
      // The ones that were already incref-ed will be cleaned up with the regular cache buffers.
      while (decompressedIx < toDecompress.size()) {
        ProcCacheChunk chunk = toDecompress.get(decompressedIx);
        csd.getCacheBuffers().remove(chunk.getBuffer());
        try {
          cacheWrapper.getAllocator().deallocate(chunk.getBuffer());
        } catch (Throwable t) {
          LOG.error("Ignoring the cleanup error after another error", t);
        }
        chunk.setBuffer(null);
        ++decompressedIx;
      }
    }

    // 5. Release the copies we made directly to the cleaner.
    releaseBuffers(toReleaseCopies, false);

    // 6. Finally, put uncompressed data to cache.
    if (fileKey != null) {
      long[] collisionMask = cacheWrapper.putFileData(
          fileKey, cacheKeys, targetBuffers, baseOffset, tag);
      processCacheCollisions(collisionMask, toDecompress, targetBuffers, csd.getCacheBuffers());
    }

    // 7. It may happen that we know we won't use some cache buffers anymore (the alternative
    //    is that we will use the same buffers for other streams in separate calls).
    //    Release initial refcounts.
    for (ProcCacheChunk chunk : toDecompress) {
      ponderReleaseInitialRefcount(unlockUntilCOffset, streamOffset, chunk);
    }

    return lastUncompressed;
  }

  @Override
  public void preReadDataRanges(DiskRangeList ranges) throws IOException {
    boolean hasFileId = this.fileKey != null;
    long baseOffset = 0L;

    // 2. Now, read all of the ranges from cache or disk.
    IdentityHashMap<ByteBuffer, Boolean> toRelease = new IdentityHashMap<>();
    MutateHelper toRead = getDataFromCacheAndDisk(ranges, 0, hasFileId, toRelease);

    // 3. For uncompressed case, we need some special processing before read.
    preReadUncompressedStreams(baseOffset, toRead, toRelease);

    // 4. Decompress the data.
    ColumnStreamData csd = POOLS.csdPool.take();
    try {
      csd.incRef();
      DiskRangeList drl = toRead.next;
      while (drl != null) {
        drl = readEncodedStream(baseOffset, drl, drl.getOffset(), drl.getEnd(), csd, drl.getOffset(), drl.getEnd(),
                toRelease);
        for (MemoryBuffer buf : csd.getCacheBuffers()) {
          cacheWrapper.releaseBuffer(buf);
        }
        if (drl != null)
          drl = drl.next;
        }
    } finally {
      if (toRead != null) {
        releaseInitialRefcounts(toRead.next);
      }
      if (toRelease != null) {
        releaseBuffers(toRelease.keySet(), true);
        toRelease.clear();
      }
      if (csd != null) {
        csd.decRef();
        POOLS.csdPool.offer(csd);
      }
    }
  }

  private void preReadUncompressedStreams(long baseOffset, MutateHelper toRead,
      IdentityHashMap<ByteBuffer, Boolean> toRelease) throws IOException {
    if (isCompressed)
      return;
    DiskRangeList iter = toRead.next;
    while (iter != null) {
      DiskRangeList newIter = preReadUncompressedStream(baseOffset, iter, iter.getOffset(), iter.getOffset() + iter.getLength(), Kind.DATA);
      iter = newIter != null ? newIter.next : null;
    }
    if (toRelease != null) {
      releaseBuffers(toRelease.keySet(), true);
      toRelease.clear();
    }
  }

  /** Subset of readEncodedStream specific to compressed streams, separate to avoid long methods. */
  private CacheChunk prepareRangesForCompressedRead(long cOffset, long endCOffset,
      long streamOffset, long unlockUntilCOffset, DiskRangeList current,
      ColumnStreamData columnStreamData, IdentityHashMap<ByteBuffer, Boolean> toRelease,
      List<ByteBuffer> toReleaseCopies, List<ProcCacheChunk> toDecompress,
      List<IncompleteCb> badEstimates) throws IOException {
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
        if (isTracingEnabled) {
          LOG.trace("Locking " + cc.getBuffer() + " due to reuse");
        }
        cacheWrapper.reuseBuffer(cc.getBuffer());
        columnStreamData.getCacheBuffers().add(cc.getBuffer());
        currentOffset = cc.getEnd();
        if (isTracingEnabled) {
          LOG.trace("Adding an already-uncompressed buffer " + cc.getBuffer());
        }
        ponderReleaseInitialRefcount(unlockUntilCOffset, streamOffset, cc);
        lastUncompressed = cc;
        next = current.next;
        if (next != null && (endCOffset >= 0 && currentOffset < endCOffset)
            && next.getOffset() >= endCOffset) {
          throw new IOException("Expected data at " + currentOffset + " (reading until "
            + endCOffset + "), but the next buffer starts at " + next.getOffset());
        }
      } else if (current instanceof IncompleteCb)  {
        // 2b. This is a known incomplete CB caused by ORC CB end boundaries being estimates.
        if (isTracingEnabled) {
          LOG.trace("Cannot read " + current);
        }
        next = null;
        currentOffset = -1;
      } else {
        // 2c. This is a compressed buffer. We need to uncompress it; the buffer can comprise
        // several disk ranges, so we might need to combine them.
        if (!(current instanceof BufferChunk)) {
          String msg = "Found an unexpected " + current.getClass().getSimpleName() + ": "
              + current + " while looking at " + currentOffset;
          LOG.error(msg);
          throw new RuntimeException(msg);
        }
        BufferChunk bc = (BufferChunk)current;
        ProcCacheChunk newCached = addOneCompressionBuffer(bc, columnStreamData.getCacheBuffers(),
            toDecompress, toRelease, toReleaseCopies, badEstimates);
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

  /** Subset of readEncodedStream specific to uncompressed streams, separate to avoid long methods. */
  private CacheChunk prepareRangesForUncompressedRead(long cOffset, long endCOffset,
      long streamOffset, long unlockUntilCOffset, DiskRangeList current,
      ColumnStreamData columnStreamData) throws IOException {
    // Note: we are called after preReadUncompressedStream, so it doesn't have to do nearly as much
    //       as prepareRangesForCompressedRead does; e.g. every buffer is already a CacheChunk.
    long currentOffset = cOffset;
    CacheChunk lastUncompressed = null;
    boolean isFirst = true;
    while (true) {
      DiskRangeList next = null;
      assert current instanceof CacheChunk;
      lastUncompressed = (CacheChunk)current;
      if (isTracingEnabled) {
        LOG.trace("Locking " + lastUncompressed.getBuffer() + " due to reuse");
      }
      cacheWrapper.reuseBuffer(lastUncompressed.getBuffer());
      if (isFirst) {
        columnStreamData.setIndexBaseOffset((int)(lastUncompressed.getOffset() - streamOffset));
        isFirst = false;
      }
      columnStreamData.getCacheBuffers().add(lastUncompressed.getBuffer());
      currentOffset = lastUncompressed.getEnd();
      if (isTracingEnabled) {
        LOG.trace("Adding an uncompressed buffer " + lastUncompressed.getBuffer());
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
   * @param kind
   */
  private DiskRangeList preReadUncompressedStream(long baseOffset, DiskRangeList start,
      long streamOffset, long streamEnd, Kind kind) throws IOException {
    if (streamOffset == streamEnd) return null;
    List<UncompressedCacheChunk> toCache = null;

    // 1. Find our bearings in the stream.
    DiskRangeList current = findIntersectingPosition(start, streamOffset, streamEnd);
    if (isTracingEnabled) {
      LOG.trace("Starting pre-read for [" + streamOffset + "," + streamEnd + ") at " + current);
    }
    trace.logStartStream(kind, streamOffset, streamEnd, streamOffset);
    trace.logStartRead(current);

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
      // We have some disk buffers... see if we have entire part, etc.
      UncompressedCacheChunk candidateCached = null; // We will cache if we have the entire part.
      DiskRangeList next = current;
      while (true) {
        boolean noMoreDataForPart = (next == null || next.getOffset() >= partEnd);
        if (noMoreDataForPart && hasEntirePartTo < partEnd && candidateCached != null) {
          // We are missing a section at the end of the part... copy the start to non-cached.
          lastUncompressed = copyAndReplaceCandidateToNonCached(
              candidateCached, partOffset, hasEntirePartTo, cacheWrapper, singleAlloc);
          candidateCached = null;
        }
        current = next;
        if (noMoreDataForPart) break; // Done with this part.

        if (current.getEnd() > partEnd) {
          // If the current buffer contains multiple parts, split it.
          current = current.split(partEnd);
        }
        if (isTracingEnabled) {
          LOG.trace("Processing uncompressed file data at ["
              + current.getOffset() + ", " + current.getEnd() + ")");
        }
        trace.logUncompressedData(current.getOffset(), current.getEnd());
        BufferChunk curBc = (BufferChunk)current;
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
                candidateCached, partOffset, hadEntirePartTo, cacheWrapper, singleAlloc);
            candidateCached = null;
          }
          lastUncompressed = copyAndReplaceUncompressedToNonCached(curBc, cacheWrapper, singleAlloc);
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
    allocateMultiple(targetBuffers, (int)(partCount == 1 ? streamLen : partSize));

    // 4. Now copy the data into cache buffers.
    ix = 0;
    for (UncompressedCacheChunk candidateCached : toCache) {
      candidateCached.setBuffer(targetBuffers[ix]);
      ByteBuffer dest = candidateCached.getBuffer().getByteBufferRaw();
      copyAndReplaceUncompressedChunks(candidateCached, dest, candidateCached, true);
      candidateCached.clear();
      lastUncompressed = candidateCached;
      ++ix;
    }

    // 5. Put uncompressed data to cache.
    if (fileKey != null) {
      long[] collisionMask = cacheWrapper.putFileData(
          fileKey, cacheKeys, targetBuffers, baseOffset, tag);
      processCacheCollisions(collisionMask, toCache, targetBuffers, null);
    }

    return lastUncompressed;
  }

  private int determineUncompressedPartSize() {
    // We will break the uncompressed data in the cache in the chunks that are the size
    // of the prevalent ORC compression buffer (the default), or maximum allocation (since we
    // cannot allocate bigger chunks), whichever is less.
    long orcCbSizeDefault = ((Number)OrcConf.BUFFER_SIZE.getDefaultValue()).longValue();
    int maxAllocSize = cacheWrapper.getAllocator().getMaxAllocation();
    return (int)Math.min(maxAllocSize, orcCbSizeDefault);
  }

  private static void copyUncompressedChunk(ByteBuffer src, ByteBuffer dest) {
    int startPos = dest.position(), startLim = dest.limit();
    dest.put(src); // Copy uncompressed data to cache.
    // Put call moves position forward by the size of the data.
    int newPos = dest.position();
    if (newPos > startLim) {
      throw new AssertionError("After copying, buffer [" + startPos + ", " + startLim
          + ") became [" + newPos + ", " + dest.limit() + ")");
    }
    dest.position(startPos);
    dest.limit(newPos);
  }


  private CacheChunk copyAndReplaceCandidateToNonCached(
      UncompressedCacheChunk candidateCached, long partOffset,
      long candidateEnd, DataCache cacheWrapper, MemoryBuffer[] singleAlloc) {
    // We thought we had the entire part to cache, but we don't; convert start to
    // non-cached. Since we are at the first gap, the previous stuff must be contiguous.
    singleAlloc[0] = null;
    trace.logPartialUncompressedData(partOffset, candidateEnd, true);
    allocateMultiple(singleAlloc, (int)(candidateEnd - partOffset));
    MemoryBuffer buffer = singleAlloc[0];
    cacheWrapper.reuseBuffer(buffer);
    ByteBuffer dest = buffer.getByteBufferRaw();
    CacheChunk tcc = new CacheChunk(buffer, partOffset, candidateEnd);
    copyAndReplaceUncompressedChunks(candidateCached, dest, tcc, false);
    return tcc;
  }

  private void allocateMultiple(MemoryBuffer[] dest, int size) {
    if (allocator != null) {
      allocator.allocateMultiple(dest, size, cacheWrapper.getDataBufferFactory(), isStopped);
    } else {
      cacheWrapper.getAllocator().allocateMultiple(dest, size, cacheWrapper.getDataBufferFactory());
    }
  }

  private CacheChunk copyAndReplaceUncompressedToNonCached(
      BufferChunk bc, DataCache cacheWrapper, MemoryBuffer[] singleAlloc) {
    singleAlloc[0] = null;
    trace.logPartialUncompressedData(bc.getOffset(), bc.getEnd(), false);
    allocateMultiple(singleAlloc, bc.getLength());
    MemoryBuffer buffer = singleAlloc[0];
    cacheWrapper.reuseBuffer(buffer);
    ByteBuffer dest = buffer.getByteBufferRaw();
    CacheChunk tcc = new CacheChunk(buffer, bc.getOffset(), bc.getEnd());
    copyUncompressedChunk(bc.getData(), dest);
    bc.replaceSelfWith(tcc);
    return tcc;
  }

  private void copyAndReplaceUncompressedChunks(UncompressedCacheChunk candidateCached,
      ByteBuffer dest, CacheChunk tcc, boolean isValid) {
    int startPos = dest.position(), startLim = dest.limit();
    DiskRangeList next = null;
    for (int i = 0; i < candidateCached.getCount(); ++i) {
      BufferChunk chunk = (i == 0) ? candidateCached.getChunk() : (BufferChunk)next;
      dest.put(chunk.getData());
      if (isValid) {
        trace.logValidUncompressedChunk(startLim - startPos, chunk);
      }
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
    int startSrcPos = src.position(), startSrcLim = src.limit();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Decompressing " + src.remaining() + " bytes to dest buffer pos "
          + dest.position() + ", limit " + dest.limit());
    }
    codec.reset(); // We always need to call reset on the codec.
    codec.decompress(src, dest);

    dest.position(startPos);
    int newLim = dest.limit();
    if (newLim > startLim) {
      throw new AssertionError("After codec, buffer [" + startPos + ", " + startLim
          + ") became [" + dest.position() + ", " + newLim + ")");
    }
    if (dest.remaining() == 0) {
      throw new IOException("The codec has produced 0 bytes for {" + src.isDirect() + ", "
        + src.position() + ", " + src.remaining() + "} into {" + dest.isDirect() + ", "
        + dest.position()  + ", " + dest.remaining() + "}");
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
      // Do not release beyond current stream (we don't know which RGs that buffer is for).
      if ((prev == null) || (prev.getEnd() <= streamStartOffset)) break;
      // Only release cache chunks; do not release ProcCacheChunks - they may not yet have data.
      if (prev.getClass() != CacheChunk.class) break;

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
    if (isTracingEnabled) {
      LOG.trace("Unlocking " + cc.getBuffer() + " for the fetching thread"
          + (isBacktracking ? "; backtracking" : ""));
    }
    cacheWrapper.releaseBuffer(cc.getBuffer());
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
        if (isTracingEnabled) {
          LOG.trace("Discarding data due to cache collision: " + replacedChunk.getBuffer()
              + " replaced with " + replacementBuffer);
        }
        trace.logCacheCollision(replacedChunk, replacementBuffer);
        assert replacedChunk.getBuffer() != replacementBuffer : i + " was not replaced in the results "
            + "even though mask is [" + Long.toBinaryString(maskVal) + "]";
        replacedChunk.handleCacheCollision(cacheWrapper, replacementBuffer, cacheBuffers);
      }
      maskVal >>= 1;
    }
  }


  /** Finds compressed offset in a stream and makes sure iter points to its position.
     This may be necessary for obscure combinations of compression and encoding boundaries. */
  private static DiskRangeList findExactPosition(DiskRangeList ranges, long offset) {
    if (offset < 0) return ranges;
    ranges = findUpperBound(ranges, offset);
    ranges = findLowerBound(ranges, offset);
    if (offset < ranges.getOffset() || offset >= ranges.getEnd()) {
      throwRangesError(ranges, offset, offset);
    }
    return ranges;
  }

  private static DiskRangeList findIntersectingPosition(
      DiskRangeList ranges, long offset, long end) {
    if (offset < 0) return ranges;
    ranges = findUpperBound(ranges, offset);
    ranges = findLowerBound(ranges, end);
    // We are now on some intersecting buffer, find the first intersecting buffer.
    while (ranges.prev != null && ranges.prev.getEnd() > offset) {
      if (ranges.prev.getEnd() > ranges.getOffset()) {
        throwRangesError(ranges, offset, end);
      }
      ranges = ranges.prev;
    }
    return ranges;
  }


  public static DiskRangeList findLowerBound(DiskRangeList ranges, long end) {
    while (ranges.getOffset() > end) {
      if (ranges.prev.getEnd() > ranges.getOffset()) {
        throwRangesError(ranges, end, end);
      }
      ranges = ranges.prev;
    }
    return ranges;
  }


  public static DiskRangeList findUpperBound(DiskRangeList ranges, long offset) {
    while (ranges.getEnd() <= offset) {
      if (ranges.next.getOffset() < ranges.getEnd()) {
        throwRangesError(ranges, offset, offset);
      }
      ranges = ranges.next;
    }
    return ranges;
  }

  private static void throwRangesError(DiskRangeList ranges, long offset, long end) {
    // We are going to fail, so it is ok to do expensive stuff. Ranges are broken, play it safe.
    IdentityHashMap<DiskRangeList, Boolean> seen = new IdentityHashMap<>();
    seen.put(ranges, true);
    StringBuilder errors = new StringBuilder();
    while (ranges.prev != null) {
      if (ranges.prev.next != ranges) {
        errors.append("inconsistent list going back: [").append(ranges).append("].prev = [")
          .append(ranges.prev).append("]; prev.next = [").append(ranges.prev.next).append("]; ");
        // Stop, as we won't be able to go forward.
        break;
      }
      if (seen.containsKey(ranges.prev)) {
        errors.append("loop: [").append(ranges).append(
            "].prev = [").append(ranges.prev).append("]; ");
        break;
      }
      ranges = ranges.prev;
      seen.put(ranges, true);
    }
    seen.clear();
    seen.put(ranges, true);
    StringBuilder sb = new StringBuilder("Incorrect ranges detected while looking for ");
    if (offset == end) {
      sb.append(offset);
    } else {
      sb.append("[").append(offset).append(", ").append(end).append(")");
    }
    sb.append(": [").append(ranges).append("], ");
    while (ranges.next != null) {
      if (ranges.next.prev != ranges) {
        errors.append("inconsistent list going forward: [").append(ranges).append(
            "].next.prev = [").append(ranges.next.prev).append("]; ");
      }
      if (seen.containsKey(ranges.next)) {
        errors.append("loop: [").append(ranges).append(
            "].next = [").append(ranges.next).append("]; ");
        break;
      }
      ranges = ranges.next;
      sb.append("[").append(ranges).append("], ");
      seen.put(ranges, true);
    }
    sb.append("; ").append(errors);
    String error = sb.toString();
    LOG.error(error);
    throw new RuntimeException(error);
  }


  /**
   * Reads one compression block from the source; handles compression blocks read from
   * multiple ranges (usually, that would only happen with zcr).
   * Adds stuff to cachedBuffers, toDecompress and toRelease (see below what each does).
   * @param current BufferChunk where compression block starts.
   * @param cacheBuffers The result buffer array to add pre-allocated target cache buffer.
   * @param toDecompress The list of work to decompress - pairs of compressed buffers and the
   *                     target buffers (same as the ones added to cacheBuffers).
   * @param toRelease The list of buffers to release to zcr because they are no longer in use.
   * @param badEstimates The list of bad estimates that cannot be decompressed.
   * @return The resulting cache chunk.
   */
  private ProcCacheChunk addOneCompressionBuffer(BufferChunk current,
      List<MemoryBuffer> cacheBuffers, List<ProcCacheChunk> toDecompress,
      IdentityHashMap<ByteBuffer, Boolean> toRelease, List<ByteBuffer> toReleaseCopies,
      List<IncompleteCb> badEstimates) throws IOException {
    ByteBuffer slice = null;
    ByteBuffer compressed = current.getData();
    long cbStartOffset = current.getOffset();
    int b0 = -1, b1 = -1, b2 = -1;
    // First, read the CB header. Due to ORC estimates, ZCR, etc. this can be complex.
    if (compressed.remaining() >= 3) {
      // The overwhelming majority of cases will go here. Read 3 bytes. Tada!
      b0 = compressed.get() & 0xff;
      b1 = compressed.get() & 0xff;
      b2 = compressed.get() & 0xff;
    } else {
      // Bad luck! Handle the corner cases where 3 bytes are in multiple blocks.
      int[] bytes = new int[3];
      current = readLengthBytesFromSmallBuffers(
          current, cbStartOffset, bytes, badEstimates, isTracingEnabled, trace);
      if (current == null) return null;
      compressed = current.getData();
      b0 = bytes[0];
      b1 = bytes[1];
      b2 = bytes[2];
    }
    int chunkLength = (b2 << 15) | (b1 << 7) | (b0 >> 1);

    if (chunkLength > bufferSize) {
      throw new IllegalArgumentException("Buffer size too small. size = " +
          bufferSize + " needed = " + chunkLength);
    }
    int consumedLength = chunkLength + OutStream.HEADER_SIZE;
    long cbEndOffset = cbStartOffset + consumedLength;
    boolean isUncompressed = ((b0 & 0x01) == 1);
    if (isTracingEnabled) {
      LOG.trace("Found CB at " + cbStartOffset + ", chunk length " + chunkLength + ", total "
          + consumedLength + ", " + (isUncompressed ? "not " : "") + "compressed");
    }
    trace.logOrcCb(cbStartOffset, chunkLength, isUncompressed);
    if (compressed.remaining() >= chunkLength) {
      // Simple case - CB fits entirely in the disk range.
      slice = compressed.slice();
      slice.limit(chunkLength);
      return addOneCompressionBlockByteBuffer(slice, isUncompressed,
          cbStartOffset, cbEndOffset, chunkLength, current, toDecompress, cacheBuffers, false);
    }
    if (current.getEnd() < cbEndOffset && !current.hasContiguousNext()) {
      badEstimates.add(addIncompleteCompressionBuffer(
          cbStartOffset, current, 0, isTracingEnabled, trace));
      return null; // This is impossible to read from this chunk.
    }

    // TODO: we could remove extra copy for isUncompressed case by copying directly to cache.
    // We need to consolidate 2 or more buffers into one to decompress.
    ByteBuffer copy = allocateBuffer(chunkLength, compressed.isDirect());
    toReleaseCopies.add(copy); // We will always release copies at the end.
    int remaining = chunkLength - compressed.remaining();
    int originalPos = compressed.position();
    copy.put(compressed);
    if (isTracingEnabled) {
      LOG.trace("Removing partial CB " + current + " from ranges after copying its contents");
    }
    trace.logPartialCb(current);
    DiskRangeList next = current.next;
    current.removeSelf();
    if (originalPos == 0 && toRelease.remove(compressed)) {
      releaseBuffer(compressed, true);
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
        ProcCacheChunk cc = addOneCompressionBlockByteBuffer(copy, isUncompressed, cbStartOffset,
            cbEndOffset, remaining, (BufferChunk)next, toDecompress, cacheBuffers, true);
        if (compressed.remaining() <= 0 && toRelease.remove(compressed)) {
          releaseBuffer(compressed, true); // We copied the entire buffer.
        } // else there's more data to process; will be handled in next call.
        return cc;
      }
      remaining -= compressed.remaining();
      copy.put(compressed); // TODO: move into the if below; account for release call
      if (toRelease.remove(compressed)) {
        releaseBuffer(compressed, true); // We copied the entire buffer.
      }
      DiskRangeList tmp = next;
      next = next.hasContiguousNext() ? next.next : null;
      if (next != null) {
        if (isTracingEnabled) {
          LOG.trace("Removing partial CB " + tmp + " from ranges after copying its contents");
        }
        trace.logPartialCb(tmp);
        tmp.removeSelf();
      } else {
        badEstimates.add(addIncompleteCompressionBuffer(
            cbStartOffset, tmp, extraChunkCount, isTracingEnabled, trace));
        return null; // This is impossible to read from this chunk.
      }
    }
  }


  @VisibleForTesting
  static BufferChunk readLengthBytesFromSmallBuffers(BufferChunk first, long cbStartOffset,
      int[] result, List<IncompleteCb> badEstimates, boolean isTracingEnabled, IoTrace trace)
          throws IOException {
    if (!first.hasContiguousNext()) {
      badEstimates.add(addIncompleteCompressionBuffer(
          cbStartOffset, first, 0, isTracingEnabled, trace));
      return null; // This is impossible to read from this chunk.
    }
    int ix = readLengthBytes(first.getData(), result, 0);
    assert ix < 3; // Otherwise we wouldn't be here.
    DiskRangeList current = first.next;
    first.removeSelf();
    while (true) {
      if (!(current instanceof BufferChunk)) {
        throw new IOException(
            "Trying to extend compressed block into uncompressed block " + current);
      }
      BufferChunk currentBc = (BufferChunk) current;
      ix = readLengthBytes(currentBc.getData(), result, ix);
      if (ix == 3) return currentBc; // Done, we have 3 bytes. Continue reading this buffer.
      DiskRangeList tmp = current;
      current = current.hasContiguousNext() ? current.next : null;
      if (current != null) {
        if (isTracingEnabled) {
          LOG.trace("Removing partial CB " + tmp + " from ranges after copying its contents");
        }
        trace.logPartialCb(tmp);
        tmp.removeSelf();
      } else {
        badEstimates.add(addIncompleteCompressionBuffer(
            cbStartOffset, tmp, -1, isTracingEnabled, trace));
        return null; // This is impossible to read from this chunk.
      }
    }
  }

  private static int readLengthBytes(ByteBuffer compressed, int[] bytes, int ix) {
    int byteCount = compressed.remaining();
    while (byteCount > 0 && ix < 3) {
      bytes[ix++] = compressed.get() & 0xff;
      --byteCount;
    }
    return ix;
  }

  private void releaseBuffers(Collection<ByteBuffer> toRelease, boolean isFromDataReader) {
    if (toRelease == null) return;
    for (ByteBuffer buf : toRelease) {
      releaseBuffer(buf, isFromDataReader);
    }
  }

  private void releaseBuffer(ByteBuffer bb, boolean isFromDataReader) {
    if (isTracingEnabled) {
      LOG.trace("Releasing the buffer " + System.identityHashCode(bb));
    }
    if (isFromDataReader && dataReader.isTrackingDiskRanges()) {
      dataReader.releaseBuffer(bb);
      return;
    }
    if (!bb.isDirect() || !CleanerUtil.UNMAP_SUPPORTED) {
      return;
    }
    try {
      CleanerUtil.getCleaner().freeBuffer(bb);
    } catch (Exception e) {
      // leave it for GC to clean up
      LOG.warn("Unable to clean direct buffers using Cleaner");
    }
  }


  private static IncompleteCb addIncompleteCompressionBuffer(long cbStartOffset,
      DiskRangeList target, int extraChunkCountToLog, boolean isTracingEnabled, IoTrace trace) {
    IncompleteCb icb = new IncompleteCb(cbStartOffset, target.getEnd());
    if (isTracingEnabled) {
      LOG.trace("Replacing " + target + " (and " + extraChunkCountToLog
          + " previous chunks) with " + icb + " in the buffers");
    }
    trace.logInvalidOrcCb(cbStartOffset, target.getEnd());
    target.replaceSelfWith(icb);
    return icb;
  }

  /**
   * Add one buffer with compressed data the results for addOneCompressionBuffer (see javadoc).
   * @param fullCompressionBlock (fCB) Entire compression block, sliced or copied from disk data.
   * @param isUncompressed Whether the data in the block is uncompressed.
   * @param cbStartOffset Compressed start offset of the fCB.
   * @param cbEndOffset Compressed end offset of the fCB.
   * @param lastChunkLength The number of compressed bytes consumed from last *chunk* into fullCompressionBlock.
   * @param lastChunk
   * @param toDecompress See addOneCompressionBuffer.
   * @param cacheBuffers See addOneCompressionBuffer.
   * @return New cache buffer.
   */
  private ProcCacheChunk addOneCompressionBlockByteBuffer(ByteBuffer fullCompressionBlock,
      boolean isUncompressed, long cbStartOffset, long cbEndOffset, int lastChunkLength,
      BufferChunk lastChunk, List<ProcCacheChunk> toDecompress, List<MemoryBuffer> cacheBuffers,
      boolean doTrace) {
    // Prepare future cache buffer.
    MemoryBuffer futureAlloc = cacheWrapper.getDataBufferFactory().create();
    // Add it to result in order we are processing.
    cacheBuffers.add(futureAlloc);
    // Add it to the list of work to decompress.
    ProcCacheChunk cc = new ProcCacheChunk(cbStartOffset, cbEndOffset, !isUncompressed,
        fullCompressionBlock, futureAlloc, cacheBuffers.size() - 1);
    toDecompress.add(cc);
    // Adjust the compression block position.
    if (isTracingEnabled) {
      LOG.trace("Adjusting " + lastChunk + " to consume " + lastChunkLength + " compressed bytes");
    }
    if (doTrace) {
      trace.logCompositeOrcCb(lastChunkLength, lastChunk.getData().remaining(), cc);
    }
    lastChunk.getData().position(lastChunk.getData().position() + lastChunkLength);
    // Finally, put it in the ranges list for future use (if shared between RGs).
    // Before anyone else accesses it, it would have been allocated and decompressed locally.
    if (lastChunk.getData().remaining() <= 0) {
      if (isTracingEnabled) {
        LOG.trace("Replacing " + lastChunk + " with " + cc + " in the buffers");
      }
      lastChunk.replaceSelfWith(cc);
    } else {
      if (isTracingEnabled) {
        LOG.trace("Adding " + cc + " before " + lastChunk + " in the buffers");
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
    pools.ecbPool = pf.createEncodedColumnBatchPool();
    pools.csdPool = pf.createColumnStreamDataPool();
    return pools;
  }

  /** Pool factory that is used if another one isn't specified - just creates the objects. */
  private static class NoopPoolFactory implements PoolFactory {
    private <T> Pool<T> createPool(final int size, final PoolObjectHelper<T> helper) {
      return new Pool<T>() {
        public void offer(T t) {
        }

        @Override
        public int size() {
          return size;
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

  // TODO: perhaps move to Orc InStream?
  private static class IndexStream extends InputStream {
    private List<MemoryBuffer> ranges;
    private long currentOffset = 0, length;
    private ByteBuffer range;
    private int rangeIx = -1;

    public IndexStream(List<MemoryBuffer> input, long length) {
      this.ranges = input;
      this.length = length;
    }

    @Override
    public int read() {
      if (!ensureRangeWithData()) {
        return -1;
      }
      currentOffset += 1;
      return 0xff & range.get();
    }

    private boolean ensureRangeWithData() {
      while (range == null || range.remaining() <= 0) {
        ++rangeIx;
        if (rangeIx == ranges.size()) return false;
        range = ranges.get(rangeIx).getByteBufferDup();
      }
      return true;
    }

    @Override
    public int read(byte[] data, int offset, int length) {
     if (!ensureRangeWithData()) {
        return -1;
      }
      int actualLength = Math.min(length, range.remaining());
      range.get(data, offset, actualLength);
      currentOffset += actualLength;
      return actualLength;
    }

    @Override
    public int available() {
      if (range != null && range.remaining() > 0) {
        return range.remaining();
      }
      return (int) (length - currentOffset);
    }

    @Override
    public void close() {
      rangeIx = ranges.size();
      currentOffset = length;
      ranges.clear();
    }

    @Override
    public String toString() {
      return "position: " + currentOffset + " length: " + length + " range: " + rangeIx +
          " offset: " + (range == null ? 0 : range.position())
          + " limit: " + (range == null ? 0 : range.limit());
    }
  }

  @Override
  public void readIndexStreams(OrcIndex index, StripeInformation stripe,
      List<OrcProto.Stream> streams, boolean[] physicalFileIncludes, boolean[] sargColumns)
          throws IOException {
    long stripeOffset = stripe.getOffset();
    DiskRangeList indexRanges = planIndexReading(fileSchema, streams, true, physicalFileIncludes,
        sargColumns, version, index.getBloomFilterKinds());
    if (indexRanges == null) {
      LOG.debug("Nothing to read for stripe [{}]", stripe);
      return;
    }
    ReadContext[] colCtxs = new ReadContext[physicalFileIncludes.length];
    int colRgIx = -1;
    for (int i = 0; i < physicalFileIncludes.length; ++i) {
      if (!physicalFileIncludes[i] && (sargColumns == null || !sargColumns[i])) continue;
      colCtxs[i] = new ReadContext(i, ++colRgIx);
      if (isTracingEnabled) {
        LOG.trace("Creating context: " + colCtxs[i].toString());
      }
      trace.logColumnRead(i, colRgIx, ColumnEncoding.Kind.DIRECT); // Bogus encoding.
    }
    long offset = 0;
    for (OrcProto.Stream stream : streams) {
      long length = stream.getLength();
      int colIx = stream.getColumn();
      OrcProto.Stream.Kind streamKind = stream.getKind();
      // See planIndexReading - only read non-row-index streams if involved in SARGs.
      if ((StreamName.getArea(streamKind) == StreamName.Area.INDEX)
          && ((sargColumns != null && sargColumns[colIx])
              || (physicalFileIncludes[colIx] && streamKind == Kind.ROW_INDEX))) {
        trace.logAddStream(colIx, streamKind, offset, length, -1, true);
        colCtxs[colIx].addStream(offset, stream, -1);
        if (isTracingEnabled) {
          LOG.trace("Adding stream for column " + colIx + ": "
              + streamKind + " at " + offset + ", " + length);
        }
      }
      offset += length;
    }

    boolean hasFileId = this.fileKey != null;

    // 2. Now, read all of the ranges from cache or disk.
    IdentityHashMap<ByteBuffer, Boolean> toRelease = new IdentityHashMap<>();
    MutateHelper toRead = getDataFromCacheAndDisk(indexRanges, stripeOffset, hasFileId, toRelease);

    // 3. For uncompressed case, we need some special processing before read.
    DiskRangeList iter = preReadUncompressedStreams(stripeOffset, colCtxs, toRead, toRelease);

    // 4. Decompress the data.
    boolean hasError = true;
    try {
      for (int colIx = 0; colIx < colCtxs.length; ++colIx) {
        ReadContext ctx = colCtxs[colIx];
        if (ctx == null) continue; // This column is not included.
        for (int streamIx = 0; streamIx < ctx.streamCount; ++streamIx) {
          StreamContext sctx = ctx.streams[streamIx];
          try {
            if (isTracingEnabled) {
              LOG.trace("Getting index stream " + sctx.kind + " for column " + ctx.colIx
                  + " at " + sctx.offset + ", " + sctx.length);
            }
            ColumnStreamData csd = POOLS.csdPool.take();
            long endCOffset = sctx.offset + sctx.length;
            DiskRangeList lastCached = readEncodedStream(stripeOffset, iter, sctx.offset,
                endCOffset, csd, endCOffset, sctx.offset, toRelease);
            if (lastCached != null) {
              iter = lastCached;
            }
            if (isTracingEnabled) {
              traceLogBuffersUsedToParse(csd);
            }
            CodedInputStream cis = CodedInputStream.newInstance(
                new IndexStream(csd.getCacheBuffers(), sctx.length));
            cis.setSizeLimit(InStream.PROTOBUF_MESSAGE_MAX_LIMIT);
            switch (sctx.kind) {
              case ROW_INDEX:
                OrcProto.RowIndex tmp = index.getRowGroupIndex()[colIx]
                    = OrcProto.RowIndex.parseFrom(cis);
                if (isTracingEnabled) {
                  LOG.trace("Index is " + tmp.toString().replace('\n', ' '));
                }
                break;
              case BLOOM_FILTER:
              case BLOOM_FILTER_UTF8:
                index.getBloomFilterIndex()[colIx] = OrcProto.BloomFilterIndex.parseFrom(cis);
                break;
              default:
                throw new AssertionError("Unexpected index stream type " + sctx.kind);
            }
            // We are done with the buffers; unlike data blocks, we are also the consumer. Release.
            for (MemoryBuffer buf : csd.getCacheBuffers()) {
              if (buf == null) continue;
              cacheWrapper.releaseBuffer(buf);
            }
          } catch (Exception ex) {
            DiskRangeList drl = toRead == null ? null : toRead.next;
            LOG.error("Error getting stream " + sctx.kind + " for column " + ctx.colIx
                + " at " + sctx.offset + ", " + sctx.length + "; toRead "
                + RecordReaderUtils.stringifyDiskRanges(drl), ex);
            throw (ex instanceof IOException) ? (IOException)ex : new IOException(ex);
          }
        }
      }

      if (isTracingEnabled) {
        LOG.trace("Disk ranges after preparing all the data "
            + RecordReaderUtils.stringifyDiskRanges(toRead.next));
      }
      hasError = false;
    } finally {
      // Release the unreleased buffers. See class comment about refcounts.
      try {
        if (toRead != null) {
          releaseInitialRefcounts(toRead.next);
        }
        releaseBuffers(toRelease.keySet(), true);
      } catch (Throwable t) {
        if (!hasError) throw new IOException(t);
        LOG.error("Error during the cleanup after another error; ignoring", t);
      }
    }
  }

  private void traceLogBuffersUsedToParse(ColumnStreamData csd) {
    String s = "Buffers ";
    if (csd.getCacheBuffers() != null) {
      for (MemoryBuffer buf : csd.getCacheBuffers()) {
        ByteBuffer bb = buf.getByteBufferDup();
        s += "{" + buf + ", " + bb.remaining() + /* " => " + bb.hashCode() + */"}, ";
      }
    }
    LOG.trace(s);
  }


  private DiskRangeList preReadUncompressedStreams(long stripeOffset, ReadContext[] colCtxs,
      MutateHelper toRead, IdentityHashMap<ByteBuffer, Boolean> toRelease) throws IOException {
    if (isCompressed) return toRead.next;
    DiskRangeList iter = toRead.next;  // Keep "toRead" list for future use, don't extract().
    boolean hasError = true;
    try {
      for (int colIx = 0; colIx < colCtxs.length; ++colIx) {
        ReadContext ctx = colCtxs[colIx];
        if (ctx == null) continue; // This column is not included.
        for (int streamIx = 0; streamIx < ctx.streamCount; ++streamIx) {
          StreamContext sctx = ctx.streams[streamIx];
          DiskRangeList newIter = preReadUncompressedStream(
              stripeOffset, iter, sctx.offset, sctx.offset + sctx.length, sctx.kind);
          if (newIter != null) {
            iter = newIter;
          }
        }
      }
      // Release buffers as we are done with all the streams... also see toRelease comment.\
      // With uncompressed streams, we know we are done earlier.
      if (toRelease != null) {
        releaseBuffers(toRelease.keySet(), true);
        toRelease.clear();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Disk ranges after pre-read (file " + fileKey + ", base offset "
            + stripeOffset + "): " + RecordReaderUtils.stringifyDiskRanges(toRead.next));
      }
      iter = toRead.next; // Reset the iter to start.
      hasError = false;
    } finally {
      // At this point, everything in the list is going to have a refcount of one. Unless it
      // failed between the allocation and the incref for a single item, we should be ok.
      if (hasError) {
        try {
          releaseInitialRefcounts(toRead.next);
          if (toRelease != null) {
            releaseBuffers(toRelease.keySet(), true);
            toRelease.clear();
          }
        } catch (Throwable t) {
          LOG.error("Error during the cleanup after another error; ignoring", t);
        }
      }
    }
    return toRead.next; // Reset the iter to start.
  }

  // TODO: temporary, need to expose from ORC utils (note the difference in null checks)
  static DiskRangeList planIndexReading(TypeDescription fileSchema,
                                        List<OrcProto.Stream> streams,
                                        boolean ignoreNonUtf8BloomFilter,
                                        boolean[] fileIncluded,
                                        boolean[] sargColumns,
                                        WriterVersion version,
                                        OrcProto.Stream.Kind[] bloomFilterKinds) {
    DiskRangeList.CreateHelper result = new DiskRangeList.CreateHelper();
    // figure out which kind of bloom filter we want for each column
    // picks bloom_filter_utf8 if its available, otherwise bloom_filter
    if (sargColumns != null) {
      for (OrcProto.Stream stream : streams) {
        if (stream.hasKind() && stream.hasColumn()) {
          int column = stream.getColumn();
          if (sargColumns[column]) {
            switch (stream.getKind()) {
              case BLOOM_FILTER:
                if (bloomFilterKinds[column] == null &&
                    !(ignoreNonUtf8BloomFilter &&
                        hadBadBloomFilters(fileSchema.findSubtype(column)
                            .getCategory(), version))) {
                  bloomFilterKinds[column] = OrcProto.Stream.Kind.BLOOM_FILTER;
                }
                break;
              case BLOOM_FILTER_UTF8:
                bloomFilterKinds[column] = OrcProto.Stream.Kind.BLOOM_FILTER_UTF8;
                break;
              default:
                break;
            }
          }
        }
      }
    }
    long offset = 0;
    for (OrcProto.Stream stream: streams) {
      if (stream.hasKind() && stream.hasColumn()) {
        int column = stream.getColumn();
        if (fileIncluded == null || fileIncluded[column]) {
          boolean needStream = false;
          switch (stream.getKind()) {
            case ROW_INDEX:
              needStream = true;
              break;
            case BLOOM_FILTER:
            case BLOOM_FILTER_UTF8:
              needStream = (sargColumns != null) && (bloomFilterKinds[column] == stream.getKind());
              break;
            default:
              // PASS
              break;
          }
          if (needStream) {
            result.addOrMerge(offset, offset + stream.getLength(), true, false);
          }
        }
      }
      offset += stream.getLength();
    }
    return result.get();
  }

  // TODO: see planIndexReading; this is not needed here.
  private static boolean hadBadBloomFilters(TypeDescription.Category category,
                                    WriterVersion version) {
    switch(category) {
      case STRING:
      case CHAR:
      case VARCHAR:
        return !version.includes(WriterVersion.HIVE_12055);
      case DECIMAL:
        return true;
      default:
        return false;
    }
  }

  @Override
  public void setStopped(AtomicBoolean isStopped) {
    this.isStopped = isStopped;
  }
}
