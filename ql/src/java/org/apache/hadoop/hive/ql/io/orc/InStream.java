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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.DiskRange;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.BufferChunk;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.CacheChunk;
import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache;

import com.google.common.annotations.VisibleForTesting;

abstract class InStream extends InputStream {
  private static final Log LOG = LogFactory.getLog(InStream.class);

  private static class UncompressedStream extends InStream {
    private final String name;
    private final List<DiskRange> bytes;
    private final long length;
    private long currentOffset;
    private ByteBuffer range;
    private int currentRange;

    public UncompressedStream(String name, List<DiskRange> input, long length) {
      this.name = name;
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
      for(int i = 0; i < bytes.size(); ++i) {
        DiskRange curRange = bytes.get(i);
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

  private static class CompressedStream extends InStream {
    private final String fileName;
    private final String name;
    private final List<DiskRange> bytes;
    private final int bufferSize;
    private final long length;
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
      this.fileName = fileName;
      this.bytes = input;
      this.name = name;
      this.codec = codec;
      this.length = length;
      this.bufferSize = bufferSize;
      currentOffset = 0;
      currentRange = 0;
      this.cache = cache;
    }

    private ByteBuffer allocateBuffer(int size, boolean isDirect) {
      // TODO: use the same pool as the ORC readers
      if (isDirect) {
        return ByteBuffer.allocateDirect(size);
      } else {
        return ByteBuffer.allocate(size);
      }
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
            cache.putFileData(fileName, new DiskRange[] { new DiskRange(originalOffset,
                chunkLength + OutStream.HEADER_SIZE) }, new LlapMemoryBuffer[] { cacheBuffer });
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

      while (len > 0 && (++currentRange) < bytes.size()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Read slow-path, >1 cross block reads with %s", this.toString()));
        }
        DiskRange range = bytes.get(currentRange);
        if (!(range instanceof BufferChunk)) {
          throw new IOException("Trying to extend compressed block into uncompressed block");
        }
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
      for(int i = 0; i < bytes.size(); ++i) {
        DiskRange range = bytes.get(i);
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
      for(int i=0; i < bytes.size(); ++i) {
        if (i != 0) {
          builder.append("; ");
        }
        DiskRange range = bytes.get(i);
        builder.append(" range " + i + " = " + range.offset
            + " to " + (range.end - range.offset));
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

  /**
   * Create an input stream from a list of buffers.
   * @param name the name of the stream
   * @param input the list of ranges of bytes for the stream
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
      return new UncompressedStream(name, input, length);
    } else {
      return new CompressedStream(fileName, name, input, length, codec, bufferSize, cache);
    }
  }
}
