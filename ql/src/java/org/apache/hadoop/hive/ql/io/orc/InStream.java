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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.DiskRange;
import org.apache.hadoop.hive.common.DiskRangeList;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch.StreamBuffer;
import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache.CacheChunkFactory;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.BufferChunk;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.CacheChunk;
import org.apache.hadoop.hive.shims.HadoopShims.ZeroCopyReaderShim;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.apache.hive.common.util.FixedSizedObjectPool.PoolObjectHelper;

import com.google.common.annotations.VisibleForTesting;

public abstract class InStream extends InputStream {

  private static final Log LOG = LogFactory.getLog(InStream.class);
  private static final FixedSizedObjectPool<TrackedCacheChunk> TCC_POOL =
      new FixedSizedObjectPool<>(1024, new PoolObjectHelper<TrackedCacheChunk>() {
        @Override
        protected TrackedCacheChunk create() {
          return new TrackedCacheChunk();
        }
        @Override
        protected void resetBeforeOffer(TrackedCacheChunk t) {
          t.reset();
        }
      });
  private static final FixedSizedObjectPool<ProcCacheChunk> PCC_POOL =
      new FixedSizedObjectPool<>(1024, new PoolObjectHelper<ProcCacheChunk>() {
        @Override
        protected ProcCacheChunk create() {
          return new ProcCacheChunk();
        }
        @Override
        protected void resetBeforeOffer(ProcCacheChunk t) {
          t.reset();
        }
      });
  final static CacheChunkFactory CC_FACTORY = new CacheChunkFactory() {
        @Override
        public DiskRangeList createCacheChunk(LlapMemoryBuffer buffer, long offset, long end) {
          TrackedCacheChunk tcc = TCC_POOL.take();
          tcc.init(buffer, offset, end);
          return tcc;
        }
      };
  protected final Long fileId;
  protected final String name;
  protected long length;

  public InStream(Long fileId, String name, long length) {
    this.fileId = fileId;
    this.name = name;
    this.length = length;
  }

  public String getStreamName() {
    return name;
  }

  public long getStreamLength() {
    return length;
  }

  static class UncompressedStream extends InStream {
    private List<DiskRange> bytes;
    private long length;
    private long currentOffset;
    private ByteBuffer range;
    private int currentRange;

    public UncompressedStream(Long fileId, String name, List<DiskRange> input, long length) {
      super(fileId, name, length);
      reset(input, length);
    }

    protected void reset(List<DiskRange> input, long length) {
      this.bytes = input;
      this.length = length;
      currentRange = 0;
      currentOffset = 0;
      range = null;
    }

    @Override
    public int read() {
      if (range == null || range.remaining() == 0) {
        if (currentOffset == length) {
          return -1;
        }
        seek(currentOffset);
      }
      currentOffset += 1;
      return 0xff & range.get();
    }

    @Override
    public int read(byte[] data, int offset, int length) {
      if (range == null || range.remaining() == 0) {
        if (currentOffset == this.length) {
          return -1;
        }
        seek(currentOffset);
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
      currentRange = bytes.size();
      currentOffset = length;
      // explicit de-ref of bytes[]
      bytes.clear();
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      seek(index.getNext());
    }

    public void seek(long desired) {
      if (desired == 0 && bytes.isEmpty()) {
        logEmptySeek(name);
        return;
      }
      int i = 0;
      for (DiskRange curRange : bytes) {
        if (desired == 0 && curRange.getData().remaining() == 0) {
          logEmptySeek(name);
          return;
        }
        if (curRange.getOffset() <= desired &&
            (desired - curRange.getOffset()) < curRange.getLength()) {
          currentOffset = desired;
          currentRange = i;
          this.range = curRange.getData().duplicate();
          int pos = range.position();
          pos += (int)(desired - curRange.getOffset()); // this is why we duplicate
          this.range.position(pos);
          return;
        }
        ++i;
      }
      // if they are seeking to the precise end, go ahead and let them go there
      int segments = bytes.size();
      if (segments != 0 && desired == bytes.get(segments - 1).getEnd()) {
        currentOffset = desired;
        currentRange = segments - 1;
        DiskRange curRange = bytes.get(currentRange);
        this.range = curRange.getData().duplicate();
        int pos = range.position();
        pos += (int)(desired - curRange.getOffset()); // this is why we duplicate
        this.range.position(pos);
        return;
      }
      throw new IllegalArgumentException("Seek in " + name + " to " +
        desired + " is outside of the data");
    }

    @Override
    public String toString() {
      return "uncompressed stream " + name + " position: " + currentOffset +
          " length: " + length + " range: " + currentRange +
          " offset: " + (range == null ? 0 : range.position()) + " limit: " + (range == null ? 0 : range.limit());
    }
  }

  private static ByteBuffer allocateBuffer(int size, boolean isDirect) {
    // TODO: use the same pool as the ORC readers
    if (isDirect) {
      return ByteBuffer.allocateDirect(size);
    } else {
      return ByteBuffer.allocate(size);
    }
  }

  private static class CompressedStream extends InStream {
    private final List<DiskRange> bytes;
    private final int bufferSize;
    private LlapMemoryBuffer cacheBuffer;
    private ByteBuffer uncompressed;
    private final CompressionCodec codec;
    private ByteBuffer compressed;
    private long currentOffset;
    private int currentRange;
    private boolean isUncompressedOriginal;
    private final LowLevelCache cache;
    private final boolean doManageBuffers = true;

    public CompressedStream(Long fileId, String name, List<DiskRange> input, long length,
                            CompressionCodec codec, int bufferSize, LowLevelCache cache) {
      super(fileId, name, length);
      this.bytes = input;
      this.codec = codec;
      this.bufferSize = bufferSize;
      currentOffset = 0;
      currentRange = 0;
      this.cache = cache;
    }

    // TODO: This should not be used for main path.
    private final LlapMemoryBuffer[] singleAllocDest = new LlapMemoryBuffer[1];
    private void allocateForUncompressed(int size, boolean isDirect) {
      if (cache == null) {
        cacheBuffer = null;
        uncompressed = allocateBuffer(size, isDirect);
      } else {
        singleAllocDest[0] = null;
        cache.allocateMultiple(singleAllocDest, size);
        cacheBuffer = singleAllocDest[0];
        uncompressed = cacheBuffer.getByteBufferDup();
      }
    }

    private void readHeader() throws IOException {
      if (compressed == null || compressed.remaining() <= 0) {
        seek(currentOffset);
      }
      if (cacheBuffer != null) {
        assert compressed == null;
        return; // Next block is ready from cache.
      }
      if (compressed.remaining() > OutStream.HEADER_SIZE) {
        int b0 = compressed.get() & 0xff;
        int b1 = compressed.get() & 0xff;
        int b2 = compressed.get() & 0xff;
        boolean isOriginal = (b0 & 0x01) == 1;
        int chunkLength = (b2 << 15) | (b1 << 7) | (b0 >> 1);

        if (chunkLength > bufferSize) {
          throw new IllegalArgumentException("Buffer size too small. size = " +
              bufferSize + " needed = " + chunkLength);
        }
        // read 3 bytes, which should be equal to OutStream.HEADER_SIZE always
        assert OutStream.HEADER_SIZE == 3 : "The Orc HEADER_SIZE must be the same in OutStream and InStream";
        currentOffset += OutStream.HEADER_SIZE;

        ByteBuffer slice = this.slice(chunkLength);

        if (isOriginal) {
          uncompressed = slice;
          isUncompressedOriginal = true;
        } else {
          if (isUncompressedOriginal) {
            allocateForUncompressed(bufferSize, slice.isDirect());
            isUncompressedOriginal = false;
          } else if (uncompressed == null) {
            allocateForUncompressed(bufferSize, slice.isDirect());
          } else {
            uncompressed.clear();
          }
          codec.decompress(slice, uncompressed);
          if (cache != null) {
            // this is the inefficient path
            // TODO: this is invalid; base stripe offset should be passed, return value handled.
            //cache.putFileData(fileId, new DiskRange[] { new DiskRange(originalOffset,
            //  chunkLength + OutStream.HEADER_SIZE) }, new LlapMemoryBuffer[] { cacheBuffer }, 0);
          }
        }
      } else {
        throw new IllegalStateException("Can't read header at " + this);
      }
    }

    @Override
    public int read() throws IOException {
      if (uncompressed == null || uncompressed.remaining() == 0) {
        if (currentOffset == length) {
          return -1;
        }
        readHeader();
      }
      return 0xff & uncompressed.get();
    }

    @Override
    public int read(byte[] data, int offset, int length) throws IOException {
      if (uncompressed == null || uncompressed.remaining() == 0) {
        if (currentOffset == this.length) {
          return -1;
        }
        readHeader();
      }
      int actualLength = Math.min(length, uncompressed.remaining());
      uncompressed.get(data, offset, actualLength);
      return actualLength;
    }

    @Override
    public int available() throws IOException {
      if (uncompressed == null || uncompressed.remaining() == 0) {
        if (currentOffset == length) {
          return 0;
        }
        readHeader();
      }
      return uncompressed.remaining();
    }

    @Override
    public void close() {
      cacheBuffer = null;
      uncompressed = null;
      compressed = null;
      currentRange = bytes.size();
      currentOffset = length;
      if (doManageBuffers) {
        // TODO: this is the inefficient path for now. LLAP will used this differently.
        for (DiskRange range : bytes) {
          if (range instanceof CacheChunk) {
            cache.releaseBuffer(((CacheChunk)range).buffer);
          }
        }
      }
      bytes.clear();
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      seek(index.getNext());
      long uncompressedBytes = index.getNext();
      if (uncompressedBytes != 0) {
        readHeader();
        uncompressed.position(uncompressed.position() +
                              (int) uncompressedBytes);
      } else if (uncompressed != null) {
        // mark the uncompressed buffer as done
        uncompressed.position(uncompressed.limit());
      }
    }

    /* slices a read only contiguous buffer of chunkLength */
    private ByteBuffer slice(int chunkLength) throws IOException {
      int len = chunkLength;
      final long oldOffset = currentOffset;
      ByteBuffer slice;
      if (compressed.remaining() >= len) {
        slice = compressed.slice();
        // simple case
        slice.limit(len);
        currentOffset += len;
        compressed.position(compressed.position() + len);
        return slice;
      } else if (currentRange >= (bytes.size() - 1)) {
        // nothing has been modified yet
        throw new IOException("EOF in " + this + " while trying to read " +
            chunkLength + " bytes");
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format(
            "Crossing into next BufferChunk because compressed only has %d bytes (needs %d)",
            compressed.remaining(), len));
      }

      // we need to consolidate 2 or more buffers into 1
      // first copy out compressed buffers
      ByteBuffer copy = allocateBuffer(chunkLength, compressed.isDirect());
      currentOffset += compressed.remaining();
      len -= compressed.remaining();
      copy.put(compressed);
      ListIterator<DiskRange> iter = bytes.listIterator(currentRange);

      while (len > 0 && iter.hasNext()) {
        ++currentRange;
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Read slow-path, >1 cross block reads with %s", this.toString()));
        }
        DiskRange range = iter.next();
        compressed = range.getData().duplicate();
        if (compressed.remaining() >= len) {
          slice = compressed.slice();
          slice.limit(len);
          copy.put(slice);
          currentOffset += len;
          compressed.position(compressed.position() + len);
          return copy;
        }
        currentOffset += compressed.remaining();
        len -= compressed.remaining();
        copy.put(compressed);
      }

      // restore offsets for exception clarity
      seek(oldOffset);
      throw new IOException("EOF in " + this + " while trying to read " +
          chunkLength + " bytes");
    }

    private void seek(long desired) throws IOException {
      if (desired == 0 && bytes.isEmpty()) {
        logEmptySeek(name);
        return;
      }
      int i = 0;
      for (DiskRange range : bytes) {
        if (range.getOffset() <= desired && desired < range.getEnd()) {
          currentRange = i;
          if (range instanceof BufferChunk) {
            cacheBuffer = null;
            compressed = range.getData().duplicate();
            int pos = compressed.position();
            pos += (int)(desired - range.getOffset());
            compressed.position(pos);
          } else {
            compressed = null;
            cacheBuffer = ((CacheChunk)range).buffer;
            uncompressed = cacheBuffer.getByteBufferDup();
            if (desired != range.getOffset()) {
              throw new IOException("Cannot seek into the middle of uncompressed cached data");
            }
          }
          currentOffset = desired;
          return;
        }
        ++i;
      }
      // if they are seeking to the precise end, go ahead and let them go there
      int segments = bytes.size();
      if (segments != 0 && desired == bytes.get(segments - 1).getEnd()) {
        DiskRange range = bytes.get(segments - 1);
        currentRange = segments - 1;
        if (range instanceof BufferChunk) {
          cacheBuffer = null;
          compressed = range.getData().duplicate();
          compressed.position(compressed.limit());
        } else {
          compressed = null;
          cacheBuffer = ((CacheChunk)range).buffer;
          uncompressed = cacheBuffer.getByteBufferDup();
          uncompressed.position(uncompressed.limit());
          if (desired != range.getOffset()) {
            throw new IOException("Cannot seek into the middle of uncompressed cached data");
          }
        }
        currentOffset = desired;
        return;
      }
      throw new IOException("Seek outside of data in " + this + " to " + desired);
    }

    private String rangeString() {
      StringBuilder builder = new StringBuilder();
      int i = 0;
      for (DiskRange range : bytes) {
        if (i != 0) {
          builder.append("; ");
        }
        builder.append(" range " + i + " = " + range.getOffset()
            + " to " + (range.getEnd() - range.getOffset()));
        ++i;
      }
      return builder.toString();
    }

    @Override
    public String toString() {
      return "compressed stream " + name + " position: " + currentOffset +
          " length: " + length + " range: " + currentRange +
          " offset: " + (compressed == null ? 0 : compressed.position()) + " limit: " + (compressed == null ? 0 : compressed.limit()) +
          rangeString() +
          (uncompressed == null ? "" :
              " uncompressed: " + uncompressed.position() + " to " +
                  uncompressed.limit());
    }
  }

  public abstract void seek(PositionProvider index) throws IOException;

  private static void logEmptySeek(String name) {
    if (LOG.isWarnEnabled()) {
      LOG.warn("Attempting seek into empty stream (" + name + ") Skipping stream.");
    }
  }

  /**
   * Create an input stream from a list of buffers.
   * @param fileName name of the file
   * @param streamName the name of the stream
   * @param buffers the list of ranges of bytes for the stream
   * @param offsets a list of offsets (the same length as input) that must
   *                contain the first offset of the each set of bytes in input
   * @param length the length in bytes of the stream
   * @param codec the compression codec
   * @param bufferSize the compression buffer size
   * @return an input stream
   * @throws IOException
   */
  @VisibleForTesting
  @Deprecated
  public static InStream create(Long fileId,
                                String streamName,
                                ByteBuffer[] buffers,
                                long[] offsets,
                                long length,
                                CompressionCodec codec,
                                int bufferSize) throws IOException {
    List<DiskRange> input = new ArrayList<DiskRange>(buffers.length);
    for (int i = 0; i < buffers.length; ++i) {
      input.add(new BufferChunk(buffers[i], offsets[i]));
    }
    return create(fileId, streamName, input, length, codec, bufferSize, null);
  }

  /**
   * Create an input stream from a list of disk ranges with data.
   * @param name the name of the stream
   * @param input the list of ranges of bytes for the stream; from disk or cache
   * @param length the length in bytes of the stream
   * @param codec the compression codec
   * @param bufferSize the compression buffer size
   * @param cache Low-level cache to use to put data, if any. Only works with compressed streams.
   * @return an input stream
   * @throws IOException
   */
  public static InStream create(Long fileId,
                                String name,
                                List<DiskRange> input,
                                long length,
                                CompressionCodec codec,
                                int bufferSize,
                                LowLevelCache cache) throws IOException {
    if (codec == null) {
      return new UncompressedStream(fileId, name, input, length);
    } else {
      return new CompressedStream(fileId, name, input, length, codec, bufferSize, cache);
    }
  }

  /** Cache chunk which tracks whether it has been fully read. See
      EncodedReaderImpl class comment about refcounts. */
  public static class TrackedCacheChunk extends CacheChunk {
    public boolean isReleased = false;
    public TrackedCacheChunk() {
      super(null, -1, -1);
    }

    public void init(LlapMemoryBuffer buffer, long offset, long end) {
      this.buffer = buffer;
      this.offset = offset;
      this.end = end;
    }

    public void reset() {
      this.buffer = null;
      this.next = this.prev = null;
      this.isReleased = false;
    }
  }

  /**
   * CacheChunk that is pre-created for new cache data; initially, it contains an original disk
   * buffer and an unallocated LlapMemoryBuffer object. Before we expose it, the LMB is allocated,
   * the data is decompressed, and original compressed data is discarded. The chunk lives on in
   * the DiskRange list created for the request, and everyone treats it like regular CacheChunk.
   */
  private static class ProcCacheChunk extends TrackedCacheChunk {
    public void init(long cbStartOffset, long cbEndOffset, boolean isCompressed,
        ByteBuffer originalData, LlapMemoryBuffer targetBuffer, int originalCbIndex) {
      super.init(targetBuffer, cbStartOffset, cbEndOffset);
      this.isCompressed = isCompressed;
      this.originalData = originalData;
      this.originalCbIndex = originalCbIndex;
    }

    @Override
    public void reset() {
      super.reset();
      this.originalData = null;
    }

    boolean isCompressed;
    ByteBuffer originalData = null;
    int originalCbIndex;
  }

  /**
   * Uncompresses part of the stream. RGs can overlap, so we cannot just go and decompress
   * and remove what we have returned. We will keep iterator as a "hint" point.
   * @param fileName File name for cache keys.
   * @param baseOffset Absolute offset of boundaries and ranges relative to file, for cache keys.
   * @param start Ordered ranges containing file data. Helpful if they point close to cOffset.
   * @param cOffset Start offset to decompress.
   * @param endCOffset End offset to decompress; estimate, partial CBs will be ignored.
   * @param zcr Zero-copy reader, if any, to release discarded buffers.
   * @param codec Compression codec.
   * @param bufferSize Compressed buffer (CB) size.
   * @param cache Low-level cache to cache new data.
   * @param streamBuffer Stream buffer, to add the results.
   * @param unlockUntilCOffset The offset until which the buffers can be unlocked in cache, as
   *                           they will not be used in future calls (see the class comment in
   *                           EncodedReaderImpl about refcounts).
   * @return Last buffer cached during decomrpession. Cache buffers are never removed from
   *         the master list, so they are safe to keep as iterators for various streams.
   */
  // TODO: move to EncodedReaderImpl
  public static DiskRangeList uncompressStream(long fileId, long baseOffset, DiskRangeList start,
      long cOffset, long endCOffset, ZeroCopyReaderShim zcr, CompressionCodec codec,
      int bufferSize, LowLevelCache cache, StreamBuffer streamBuffer, long unlockUntilCOffset,
      long streamStartOffset) throws IOException {
    if (streamBuffer.cacheBuffers == null) {
      streamBuffer.cacheBuffers = new ArrayList<LlapMemoryBuffer>();
    } else {
      streamBuffer.cacheBuffers.clear();
    }
    if (cOffset == endCOffset) return null;
    List<ProcCacheChunk> toDecompress = null;
    List<ByteBuffer> toRelease = null;

    // 1. Find our bearings in the stream. Normally, iter will already point either to where we
    // want to be, or just before. However, RGs can overlap due to encoding, so we may have
    // to return to a previous block.
    DiskRangeList current = findCompressedPosition(start, cOffset);
    if (DebugUtils.isTraceOrcEnabled()) {
      LOG.info("Starting uncompressStream for [" + cOffset + "," + endCOffset + ") at " + current);
    }

    // 2. Go thru the blocks; add stuff to results and prepare the decompression work (see below).
    if (cOffset > current.getOffset()) {
      // Target compression block is in the middle of the range; slice the range in two.
      current = current.split(cOffset).next;
    }
    long currentCOffset = cOffset;
    TrackedCacheChunk lastCached = null;
    while (true) {
      DiskRangeList next = null;
      if (current instanceof TrackedCacheChunk) {
        // 2a. This is a cached compression buffer, add as is.
        TrackedCacheChunk cc = (TrackedCacheChunk)current;
        if (DebugUtils.isTraceLockingEnabled()) {
          LOG.info("Locking " + cc.buffer + " due to reuse");
        }
        boolean canReuse = cache.notifyReused(cc.buffer);
        assert canReuse;
        streamBuffer.cacheBuffers.add(cc.buffer);
        currentCOffset = cc.getEnd();
        if (DebugUtils.isTraceOrcEnabled()) {
          LOG.info("Adding an already-uncompressed buffer " + cc.buffer);
        }
        ponderReleaseInitialRefcount(cache, unlockUntilCOffset, streamStartOffset, cc);
        lastCached = cc;
        next = current.next;
      } else if (current instanceof IncompleteCb)  {
        // 2b. This is a known incomplete CB caused by ORC CB end boundaries being estimates.
        if (DebugUtils.isTraceOrcEnabled()) {
          LOG.info("Cannot read " + current);
        }
        next = null;
        currentCOffset = -1;
      } else {
        // 2c. This is a compressed buffer. We need to uncompress it; the buffer can comprise
        // several disk ranges, so we might need to combine them.
        BufferChunk bc = (BufferChunk)current;
        if (toDecompress == null) {
          toDecompress = new ArrayList<ProcCacheChunk>();
          toRelease = (zcr == null) ? null : new ArrayList<ByteBuffer>();
        }
        lastCached = addOneCompressionBuffer(bc, zcr, bufferSize,
            cache, streamBuffer.cacheBuffers, toDecompress, toRelease);
        next = (lastCached != null) ? lastCached.next : null;
        currentCOffset = (next != null) ? next.getOffset() : -1;
      }

      if (next == null || (endCOffset >= 0 && currentCOffset >= endCOffset)) {
        break;
      }
      current = next;
    }

    // 3. Allocate the buffers, prepare cache keys.
    // At this point, we have read all the CBs we need to read. cacheBuffers contains some cache
    // data and some unallocated membufs for decompression. toDecompress contains all the work we
    // need to do, and each item points to one of the membufs in cacheBuffers as target. The iter
    // has also been adjusted to point to these buffers instead of compressed data for the ranges.
    if (toDecompress == null) return lastCached; // Nothing to decompress.

    LlapMemoryBuffer[] targetBuffers = new LlapMemoryBuffer[toDecompress.size()];
    DiskRange[] cacheKeys = new DiskRange[toDecompress.size()];
    int ix = 0;
    for (ProcCacheChunk chunk : toDecompress) {
      cacheKeys[ix] = chunk; // Relies on the fact that cache does not actually store these.
      targetBuffers[ix] = chunk.buffer;
      ++ix;
    }
    cache.allocateMultiple(targetBuffers, bufferSize);

    // 4. Now decompress (or copy) the data into cache buffers.
    for (ProcCacheChunk chunk : toDecompress) {
      ByteBuffer dest = chunk.buffer.getByteBufferRaw();
      int startPos = dest.position(), startLim = dest.limit();
      if (chunk.isCompressed) {
        codec.decompress(chunk.originalData, dest);
        // Codec resets the position to 0 and limit to correct limit.
        dest.position(startPos);
        int newLim = dest.limit();
        if (newLim > startLim) {
          throw new AssertionError("After codec, buffer [" + startPos + ", " + startLim
              + ") became [" + dest.position() + ", " + newLim + ")");
        }
      } else {
        dest.put(chunk.originalData); // Copy uncompressed data to cache.
        // Put moves position forward by the size of the data.
        int newPos = dest.position();
        if (newPos > startLim) {
          throw new AssertionError("After copying, buffer [" + startPos + ", " + startLim
              + ") became [" + newPos + ", " + dest.limit() + ")");
        }
        dest.position(startPos);
        dest.limit(newPos);
      }

      chunk.originalData = null;
      if (DebugUtils.isTraceLockingEnabled()) {
        LOG.info("Locking " + chunk.buffer + " due to reuse (after decompression)");
      }
      cache.notifyReused(chunk.buffer);
    }

    // 5. Release original compressed buffers to zero-copy reader if needed.
    if (toRelease != null) {
      assert zcr != null;
      for (ByteBuffer buf : toRelease) {
        zcr.releaseBuffer(buf);
      }
    }

    // 6. Finally, put data to cache.
    long[] collisionMask = cache.putFileData(
        fileId, cacheKeys, targetBuffers, baseOffset, Priority.NORMAL);
    processCacheCollisions(
        cache, collisionMask, toDecompress, targetBuffers, streamBuffer.cacheBuffers);

    // 7. It may happen that we only use new compression buffers once. Release initial refcounts.
    for (ProcCacheChunk chunk : toDecompress) {
      ponderReleaseInitialRefcount(cache, unlockUntilCOffset, streamStartOffset, chunk);
    }

    return lastCached;
  }

  public static void releaseCacheChunksIntoObjectPool(DiskRangeList current) {
    while (current != null) {
      if (current instanceof ProcCacheChunk) {
        PCC_POOL.offer((ProcCacheChunk)current);
      } else if (current instanceof TrackedCacheChunk) {
        TCC_POOL.offer((TrackedCacheChunk)current);
      }
      current = current.next;
    }
  }

  private static void ponderReleaseInitialRefcount(LowLevelCache cache,
      long unlockUntilCOffset, long streamStartOffset, TrackedCacheChunk cc) {
    if (cc.getEnd() > unlockUntilCOffset) return;
    assert !cc.isReleased;
    releaseInitialRefcount(cache, cc, false);
    // Release all the previous buffers that we may not have been able to release due to reuse.
    DiskRangeList prev = cc.prev;
    while (true) {
      if ((prev == null) || (prev.getEnd() <= streamStartOffset)
          || !(prev instanceof TrackedCacheChunk)) break;
      TrackedCacheChunk prevCc = (TrackedCacheChunk)prev;
      if (prevCc.isReleased) break;
      releaseInitialRefcount(cache, prevCc, true);
      prev = prev.prev;
    }
  }

  private static void releaseInitialRefcount(
      LowLevelCache cache, TrackedCacheChunk cc, boolean isBacktracking) {
    // This is the last RG for which this buffer will be used. Remove the initial refcount
    if (DebugUtils.isTraceLockingEnabled()) {
      LOG.info("Unlocking " + cc.buffer + " for the fetching thread"
          + (isBacktracking ? "; backtracking" : ""));
    }
    cache.releaseBuffer(cc.buffer);
    cc.isReleased = true;
  }

  private static void processCacheCollisions(LowLevelCache cache, long[] collisionMask,
      List<ProcCacheChunk> toDecompress, LlapMemoryBuffer[] targetBuffers,
      List<LlapMemoryBuffer> cacheBuffers) {
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
        ProcCacheChunk replacedChunk = toDecompress.get(i);
        LlapMemoryBuffer replacementBuffer = targetBuffers[i];
        if (DebugUtils.isTraceOrcEnabled()) {
          LOG.info("Discarding data due to cache collision: " + replacedChunk.buffer
              + " replaced with " + replacementBuffer);
        }
        assert replacedChunk.buffer != replacementBuffer : i + " was not replaced in the results "
            + "even though mask is [" + Long.toBinaryString(maskVal) + "]";
        assert replacedChunk.originalCbIndex >= 0;
        // Had the put succeeded for our new buffer, it would have refcount of 2 - 1 from put,
        // and 1 from notifyReused call above. "Old" buffer now has the 1 from put; new buffer
        // is unchanged at 1 from notifyReused. Make it all consistent.
        cache.releaseBuffer(replacedChunk.buffer);
        cache.notifyReused(replacementBuffer);
        // Replace the buffer in our big range list, as well as in current results.
        replacedChunk.buffer = replacementBuffer;
        cacheBuffers.set(replacedChunk.originalCbIndex, replacementBuffer);
        replacedChunk.originalCbIndex = -1; // This can only happen once at decompress time.
      }
      maskVal >>= 1;
    }
  }


  /** Finds compressed offset in a stream and makes sure iter points to its position.
     This may be necessary for obscure combinations of compression and encoding boundaries. */
  private static DiskRangeList findCompressedPosition(
      DiskRangeList ranges, long cOffset) {
    if (cOffset < 0) return ranges;
    // We expect the offset to be valid TODO: rather, validate
    while (ranges.getEnd() <= cOffset) {
      ranges = ranges.next;
    }
    while (ranges.getOffset() > cOffset) {
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
  private static ProcCacheChunk addOneCompressionBuffer(BufferChunk current, ZeroCopyReaderShim zcr,
      int bufferSize, LowLevelCache cache, List<LlapMemoryBuffer> cacheBuffers,
      List<ProcCacheChunk> toDecompress, List<ByteBuffer> toRelease) throws IOException {
    ByteBuffer slice = null;
    ByteBuffer compressed = current.chunk;
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
    if (DebugUtils.isTraceOrcEnabled()) {
      LOG.info("Found CB at " + cbStartOffset + ", chunk length " + chunkLength + ", total "
          + consumedLength + ", " + (isUncompressed ? "not " : "") + "compressed");
    }
    if (compressed.remaining() >= chunkLength) {
      // Simple case - CB fits entirely in the disk range.
      slice = compressed.slice();
      slice.limit(chunkLength);
      ProcCacheChunk cc = addOneCompressionBlockByteBuffer(slice, isUncompressed, cbStartOffset,
          cbEndOffset, chunkLength, current, cache, toDecompress, cacheBuffers);
      if (compressed.remaining() <= 0 && zcr != null) {
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
    if (DebugUtils.isTraceOrcEnabled()) {
      LOG.info("Removing partial CB " + current + " from ranges after copying its contents");
    }
    DiskRangeList next = current.next;
    current.removeSelf();
    if (zcr != null) {
      if (originalPos == 0) {
        zcr.releaseBuffer(compressed); // We copied the entire buffer.
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
        ProcCacheChunk cc = addOneCompressionBlockByteBuffer(
            copy, isUncompressed, cbStartOffset, cbEndOffset, remaining,
            (BufferChunk)next, cache, toDecompress, cacheBuffers);
        if (compressed.remaining() <= 0 && zcr != null) {
          zcr.releaseBuffer(compressed); // We copied the entire buffer.
        }
        return cc;
      }
      remaining -= compressed.remaining();
      copy.put(compressed);
      if (zcr != null) {
        zcr.releaseBuffer(compressed); // We copied the entire buffer.
      }
      DiskRangeList tmp = next;
      next = next.hasContiguousNext() ? next.next : null;
      if (next != null) {
        if (DebugUtils.isTraceOrcEnabled()) {
          LOG.info("Removing partial CB " + tmp + " from ranges after copying its contents");
        }
        tmp.removeSelf();
      } else {
        addIncompleteCompressionBuffer(cbStartOffset, tmp, extraChunkCount);
        return null; // This is impossible to read from this chunk.
      }
    }
  }

  private static void addIncompleteCompressionBuffer(
      long cbStartOffset, DiskRangeList target, int extraChunkCount) {
    IncompleteCb icb = new IncompleteCb(cbStartOffset, target.getEnd());
    if (DebugUtils.isTraceOrcEnabled()) {
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
  private static ProcCacheChunk addOneCompressionBlockByteBuffer(ByteBuffer fullCompressionBlock,
      boolean isUncompressed, long cbStartOffset, long cbEndOffset, int lastChunkLength,
      BufferChunk lastChunk, LowLevelCache cache, List<ProcCacheChunk> toDecompress,
      List<LlapMemoryBuffer> cacheBuffers) {
    // Prepare future cache buffer.
    LlapMemoryBuffer futureAlloc = cache.createUnallocated();
    // Add it to result in order we are processing.
    cacheBuffers.add(futureAlloc);
    // Add it to the list of work to decompress.
    ProcCacheChunk cc = PCC_POOL.take();
    cc.init(cbStartOffset, cbEndOffset, !isUncompressed,
        fullCompressionBlock, futureAlloc, cacheBuffers.size() - 1);
    toDecompress.add(cc);
    // Adjust the compression block position.
    if (DebugUtils.isTraceOrcEnabled()) {
      LOG.info("Adjusting " + lastChunk + " to consume " + lastChunkLength + " compressed bytes");
    }
    lastChunk.chunk.position(lastChunk.chunk.position() + lastChunkLength);
    // Finally, put it in the ranges list for future use (if shared between RGs).
    // Before anyone else accesses it, it would have been allocated and decompressed locally.
    if (lastChunk.chunk.remaining() <= 0) {
      if (DebugUtils.isTraceOrcEnabled()) {
        LOG.info("Replacing " + lastChunk + " with " + cc + " in the buffers");
      }
      lastChunk.replaceSelfWith(cc);
    } else {
      if (DebugUtils.isTraceOrcEnabled()) {
        LOG.info("Adding " + cc + " before " + lastChunk + " in the buffers");
      }
      lastChunk.insertPartBefore(cc);
    }
    return cc;
  }
}
