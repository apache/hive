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
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch.StreamBuffer;
import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.BufferChunk;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.CacheChunk;
import org.apache.hadoop.hive.shims.HadoopShims.ZeroCopyReaderShim;

import com.google.common.annotations.VisibleForTesting;

public abstract class InStream extends InputStream {

  private static final Log LOG = LogFactory.getLog(InStream.class);
  protected final String fileName;
  protected final String name;
  protected final long length;

  public InStream(String fileName, String name, long length) {
    this.fileName = fileName;
    this.name = name;
    this.length = length;
  }

  public String getFileName() {
    return fileName;
  }

  public String getStreamName() {
    return name;
  }

  public long getStreamLength() {
    return length;
  }

  private static class UncompressedStream extends InStream {
    private final List<DiskRange> bytes;
    private final long length;
    private long currentOffset;
    private ByteBuffer range;
    private int currentRange;

    public UncompressedStream(String fileName, String name, List<DiskRange> input, long length) {
      super(fileName, name, length);
      this.bytes = input;
      this.length = length;
      currentRange = 0;
      currentOffset = 0;
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
      for(DiskRange curRange : bytes) {
        if (desired == 0 && curRange.getData().remaining() == 0) {
          logEmptySeek(name);
          return;
        }
        if (curRange.offset <= desired &&
            (desired - curRange.offset) < curRange.getLength()) {
          currentOffset = desired;
          currentRange = i;
          this.range = curRange.getData();
          int pos = range.position();
          pos += (int)(desired - curRange.offset); // this is why we duplicate
          this.range.position(pos);
          return;
        }
        ++i;
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

    public CompressedStream(String fileName, String name, List<DiskRange> input, long length,
                            CompressionCodec codec, int bufferSize, LowLevelCache cache) {
      super(fileName, name, length);
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
        uncompressed = cacheBuffer.byteBuffer;
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
      long originalOffset = currentOffset;
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
            // TODO: this is the inefficient path
            // TODO#: this is invalid; base stripe offset should be passed.
            cache.putFileData(fileName, new DiskRange[] { new DiskRange(originalOffset,
                chunkLength + OutStream.HEADER_SIZE) }, new LlapMemoryBuffer[] { cacheBuffer }, 0);
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
        if (range.offset <= desired && desired < range.end) {
          currentRange = i;
          if (range instanceof BufferChunk) {
            cacheBuffer = null;
            compressed = range.getData().duplicate();
            int pos = compressed.position();
            pos += (int)(desired - range.offset);
            compressed.position(pos);
          } else {
            compressed = null;
            cacheBuffer = ((CacheChunk)range).buffer;
            uncompressed = cacheBuffer.byteBuffer.duplicate();
            if (desired != range.offset) {
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
      if (segments != 0 && desired == bytes.get(segments - 1).end) {
        DiskRange range = bytes.get(segments - 1);
        currentRange = segments - 1;
        if (range instanceof BufferChunk) {
          cacheBuffer = null;
          compressed = range.getData().duplicate();
          compressed.position(compressed.limit());
        } else {
          compressed = null;
          cacheBuffer = ((CacheChunk)range).buffer;
          uncompressed = cacheBuffer.byteBuffer.duplicate();
          uncompressed.position(uncompressed.limit());
          if (desired != range.offset) {
            throw new IOException("Cannot seek into the middle of uncompressed cached data");
          }
          currentOffset = desired;
        }
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
        builder.append(" range " + i + " = " + range.offset
            + " to " + (range.end - range.offset));
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
   * @param name the name of the stream
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
  public static InStream create(String name,
                                ByteBuffer[] buffers,
                                long[] offsets,
                                long length,
                                CompressionCodec codec,
                                int bufferSize) throws IOException {
    List<DiskRange> input = new ArrayList<DiskRange>(buffers.length);
    for (int i = 0; i < buffers.length; ++i) {
      input.add(new BufferChunk(buffers[i], offsets[i]));
    }
    return create(null, name, input, length, codec, bufferSize, null);
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
  public static InStream create(String fileName,
                                String name,
                                List<DiskRange> input,
                                long length,
                                CompressionCodec codec,
                                int bufferSize,
                                LowLevelCache cache) throws IOException {
    if (codec == null) {
      return new UncompressedStream(fileName, name, input, length);
    } else {
      return new CompressedStream(fileName, name, input, length, codec, bufferSize, cache);
    }
  }

  private static class ProcCacheChunk extends CacheChunk {
    public ProcCacheChunk(long cbStartOffset, long cbEndOffset, boolean isCompressed,
        ByteBuffer originalData, LlapMemoryBuffer targetBuffer) {
      super(targetBuffer, cbStartOffset, cbEndOffset);
      this.isCompressed = isCompressed;
      this.originalData = originalData;
    }

    boolean isCompressed;
    ByteBuffer originalData = null;
  }

  /**
   * Uncompresses part of the stream. RGs can overlap, so we cannot just go and decompress
   * and remove what we have returned. We will keep iterator as a "hint" point.
   * TODO: Java LinkedList and iter have a really stupid interface. Replace with own simple one?
   * @param zcr
   */
  public static void uncompressStream(String fileName, long baseOffset,
      ZeroCopyReaderShim zcr, ListIterator<DiskRange> ranges,
      CompressionCodec codec, int bufferSize, LowLevelCache cache,
      long cOffset, long endCOffset, StreamBuffer streamBuffer)
          throws IOException {
    streamBuffer.cacheBuffers = new ArrayList<LlapMemoryBuffer>();
    List<ProcCacheChunk> toDecompress = null;
    List<ByteBuffer> toRelease = null;

    // 1. Find our bearings in the stream. Normally, iter will already point either to where we
    // want to be, or just before. However, RGs can overlap due to encoding, so we may have
    // to return to a previous block.
    DiskRange current = findCompressedPosition(ranges, cOffset);
    if (DebugUtils.isTraceOrcEnabled()) {
      LOG.info("Starting uncompressStream for [" + cOffset + "," + endCOffset + ") at " + current);
    }

    // 2. Go thru the blocks; add stuff to results and prepare the decompression work (see below).
    if (cOffset >= 0 && cOffset != current.offset) {
      // We adjust offsets as we decompress, we expect to decompress sequentially, and we cache and
      // decompress entire CBs (obviously). Therefore the offset in the next DiskRange should
      // always be the start offset of a CB. TODO: what about at start?
      throw new AssertionError("Unexpected offset - for " + cOffset + ", got " + current.offset);
    }
    long currentCOffset = cOffset;
    while (true) {
      if (current instanceof CacheChunk) {
        // 2a. This is a cached compression buffer, add as is.
        CacheChunk cc = (CacheChunk)current;
        cache.notifyReused(cc.buffer);
        streamBuffer.cacheBuffers.add(cc.buffer);
        currentCOffset = cc.end;
        if (DebugUtils.isTraceOrcEnabled()) {
          LOG.info("Adding an already-uncompressed buffer " + cc.buffer);
        }
      } else {
        // 2b. This is a compressed buffer. We need to uncompress it; the buffer can comprise
        // several disk ranges, so we might need to combine them.
        BufferChunk bc = (BufferChunk)current;
        if (toDecompress == null) {
          toDecompress = new ArrayList<ProcCacheChunk>();
          toRelease = (zcr == null) ? null : new ArrayList<ByteBuffer>();
        }
        long originalOffset = bc.offset;
        int compressedBytesConsumed = addOneCompressionBuffer(bc, ranges, zcr, bufferSize,
            cache, streamBuffer.cacheBuffers, toDecompress, toRelease);
        if (compressedBytesConsumed == -1) {
          // endCOffset is an estimate; we have a partially-read compression block, ignore it
          break;
        }
        currentCOffset = originalOffset + compressedBytesConsumed;
      }
      if ((endCOffset >= 0 && currentCOffset >= endCOffset) || !ranges.hasNext()) {
        break;
      }
      current = ranges.next();
    }

    // 3. Allocate the buffers, prepare cache keys.
    // At this point, we have read all the CBs we need to read. cacheBuffers contains some cache
    // data and some unallocated membufs for decompression. toDecompress contains all the work we
    // need to do, and each item points to one of the membufs in cacheBuffers as target. The iter
    // has also been adjusted to point to these buffers instead of compressed data for the ranges.
    if (toDecompress == null) return; // Nothing to decompress.

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
      int startPos = chunk.buffer.byteBuffer.position();
      if (chunk.isCompressed) {
        codec.decompress(chunk.originalData, chunk.buffer.byteBuffer);
      } else {
        chunk.buffer.byteBuffer.put(chunk.originalData); // Copy uncompressed data to cache.
      }
      chunk.buffer.byteBuffer.position(startPos);
      chunk.originalData = null;
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
    cache.putFileData(fileName, cacheKeys, targetBuffers, baseOffset);
  }


  /** Finds compressed offset in a stream and makes sure iter points to its position.
     This may be necessary for obscure combinations of compression and encoding boundaries. */
  private static DiskRange findCompressedPosition(
      ListIterator<DiskRange> ranges, long cOffset) {
    if (cOffset < 0) return ranges.next();
    DiskRange current = null;
    boolean doCallNext = false;
    if (ranges.hasNext()) {
      current = ranges.next();
    } else if (ranges.hasPrevious()) {
      current = ranges.previous();
      doCallNext = true;
    }
    // We expect the offset to be valid TODO: rather, validate
    while (current.end <= cOffset) {
      current = ranges.next();
      doCallNext = false;
    }
    while (current.offset > cOffset) {
      current = ranges.previous();
      doCallNext = true;
    }
    if (doCallNext) {
      // TODO: WTF?
      ranges.next(); // We called previous, make sure next is the real next and not current.
    }
    return current;
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
   * @return The total number of compressed bytes consumed.
   */
  private static int addOneCompressionBuffer(BufferChunk current,
      ListIterator<DiskRange> ranges, ZeroCopyReaderShim zcr, int bufferSize,
      LowLevelCache cache, List<LlapMemoryBuffer> cacheBuffers,
      List<ProcCacheChunk> toDecompress, List<ByteBuffer> toRelease) throws IOException {
    ByteBuffer slice = null;
    ByteBuffer compressed = current.chunk;
    long cbStartOffset = current.offset;
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
      addOneCompressionBlockByteBuffer(slice, isUncompressed, cbStartOffset, cbEndOffset,
          chunkLength, ranges, current, cache, toDecompress, cacheBuffers);
      if (DebugUtils.isTraceOrcEnabled()) {
        LOG.info("Adjusting " + current + " to consume " + consumedLength);
      }
      current.offset += consumedLength;
      if (compressed.remaining() <= 0 && zcr != null) {
        toRelease.add(compressed);
      }
      return consumedLength;
    }
    if (current.end < cbEndOffset && !ranges.hasNext()) {
      return -1; // This is impossible to read from this chunk.
    }

    // TODO: we could remove extra copy for isUncompressed case by copying directly to cache.
    // We need to consolidate 2 or more buffers into one to decompress.
    ByteBuffer copy = allocateBuffer(chunkLength, compressed.isDirect());
    int remaining = chunkLength - compressed.remaining();
    int originalPos = compressed.position();
    copy.put(compressed);
    ranges.remove();
    if (DebugUtils.isTraceOrcEnabled()) {
      LOG.info("Removing " + current + " from ranges");
    }
    if (zcr != null) {
      if (originalPos == 0) {
        zcr.releaseBuffer(compressed); // We copied the entire buffer.
      } else {
        toRelease.add(compressed); // There might be slices depending on this buffer.
      }
    }

    DiskRange nextRange = null;
    while (ranges.hasNext()) {
      nextRange = ranges.next();
      if (!(nextRange instanceof BufferChunk)) {
        throw new IOException("Trying to extend compressed block into uncompressed block");
      }
      compressed = nextRange.getData();
      if (compressed.remaining() >= remaining) {
        // This is the last range for this compression block. Yay!
        slice = compressed.slice();
        slice.limit(remaining);
        copy.put(slice);
        addOneCompressionBlockByteBuffer(copy, isUncompressed, cbStartOffset,
            cbEndOffset, remaining, ranges, current, cache, toDecompress, cacheBuffers);
        if (DebugUtils.isTraceOrcEnabled()) {
          LOG.info("Adjusting " + nextRange + " to consume " + remaining);
        }
        nextRange.offset += remaining;
        if (compressed.remaining() <= 0 && zcr != null) {
          zcr.releaseBuffer(compressed); // We copied the entire buffer.
        }
        return consumedLength;
      }
      remaining -= compressed.remaining();
      copy.put(compressed);
      if (zcr != null) {
        zcr.releaseBuffer(compressed); // We copied the entire buffer.
      }
      if (DebugUtils.isTraceOrcEnabled()) {
        LOG.info("Removing " + nextRange + " from ranges");
      }
      ranges.remove();
    }
    return -1; // This is impossible to read from this chunk.
  }

  /**
   * Add one buffer with compressed data the results for addOneCompressionBuffer (see javadoc).
   * @param fullCompressionBlock (fCB) Entire compression block, sliced or copied from disk data.
   * @param isUncompressed Whether the data in the block is uncompressed.
   * @param cbStartOffset Compressed start offset of the fCB.
   * @param cbEndOffset Compressed end offset of the fCB.
   * @param lastRange The buffer from which the last (or all) bytes of fCB come.
   * @param lastPartLength The number of bytes consumed from lastRange into fCB.
   * @param ranges The iterator of all compressed ranges for the stream, pointing at lastRange.
   * @param lastChunk 
   * @param toDecompress See addOneCompressionBuffer.
   * @param cacheBuffers See addOneCompressionBuffer.
   */
  private static void addOneCompressionBlockByteBuffer(ByteBuffer fullCompressionBlock,
      boolean isUncompressed, long cbStartOffset, long cbEndOffset, int lastPartLength,
      ListIterator<DiskRange> ranges, BufferChunk lastChunk,
      LowLevelCache cache, List<ProcCacheChunk> toDecompress,
      List<LlapMemoryBuffer> cacheBuffers) {
    // Prepare future cache buffer.
    LlapMemoryBuffer futureAlloc = cache.createUnallocated();
    // Add it to result in order we are processing.
    cacheBuffers.add(futureAlloc);
    // Add it to the list of work to decompress.
    ProcCacheChunk cc = new ProcCacheChunk(
        cbStartOffset, cbEndOffset, !isUncompressed, fullCompressionBlock, futureAlloc);
    toDecompress.add(cc);
    // Adjust the compression block position.
    lastChunk.chunk.position(lastChunk.chunk.position() + lastPartLength);
    // Finally, put it in the ranges list for future use (if shared between RGs).
    // Before anyone else accesses it, it would have been allocated and decompressed locally.
    if (lastChunk.chunk.remaining() <= 0) {
      if (DebugUtils.isTraceOrcEnabled()) {
        LOG.info("Replacing " + lastChunk + " with " + cc + " in the buffers");
      }
      ranges.set(cc);
    } else {
      DiskRange before = ranges.previous();
      if (DebugUtils.isTraceOrcEnabled()) {
        LOG.info("Adding " + cc + " before " + before + " in the buffers");
      }
      ranges.add(cc);
      // At this point, next() should return before, which is the 2nd part of the split buffer.
    }
  }
}
