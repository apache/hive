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

abstract class InStream extends InputStream {

  private static class UncompressedStream extends InStream {
    private final String name;
    private final ByteBuffer[] bytes;
    private final long[] offsets;
    private final long length;
    private long currentOffset;
    private ByteBuffer range;
    private int currentRange;

    public UncompressedStream(String name, ByteBuffer[] input, long[] offsets,
                              long length) {
      this.name = name;
      this.bytes = input;
      this.offsets = offsets;
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
      currentRange = bytes.length;
      currentOffset = length;
      // explicit de-ref of bytes[]
      for(int i = 0; i < bytes.length; i++) {
        bytes[i] = null;
      }
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      seek(index.getNext());
    }

    public void seek(long desired) {
      for(int i = 0; i < bytes.length; ++i) {
        if (offsets[i] <= desired &&
            desired - offsets[i] < bytes[i].remaining()) {
          currentOffset = desired;
          currentRange = i;
          this.range = bytes[i].duplicate();
          int pos = range.position();
          pos += (int)(desired - offsets[i]); // this is why we duplicate
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
    private final String name;
    private final ByteBuffer[] bytes;
    private final long[] offsets;
    private final int bufferSize;
    private final long length;
    private ByteBuffer uncompressed;
    private final CompressionCodec codec;
    private ByteBuffer compressed;
    private long currentOffset;
    private int currentRange;
    private boolean isUncompressedOriginal;
    private boolean isDirect = false;

    public CompressedStream(String name, ByteBuffer[] input,
                            long[] offsets, long length,
                            CompressionCodec codec, int bufferSize
                           ) {
      this.bytes = input;
      this.name = name;
      this.codec = codec;
      this.length = length;
      if(this.length > 0) {
        isDirect = this.bytes[0].isDirect();
      }
      this.offsets = offsets;
      this.bufferSize = bufferSize;
      currentOffset = 0;
      currentRange = 0;
    }

    private ByteBuffer allocateBuffer(int size) {
      // TODO: use the same pool as the ORC readers
      if(isDirect == true) {
        return ByteBuffer.allocateDirect(size);
      } else {
        return ByteBuffer.allocate(size);
      }
    }

    private void readHeader() throws IOException {
      if (compressed == null || compressed.remaining() <= 0) {
        seek(currentOffset);
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
            uncompressed = allocateBuffer(bufferSize);
            isUncompressedOriginal = false;
          } else if (uncompressed == null) {
            uncompressed = allocateBuffer(bufferSize);
          } else {
            uncompressed.clear();
          }
          codec.decompress(slice, uncompressed);
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
      uncompressed = null;
      compressed = null;
      currentRange = bytes.length;
      currentOffset = length;
      for(int i = 0; i < bytes.length; i++) {
        bytes[i] = null;
      }
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

    /* slices a read only contigous buffer of chunkLength */
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
      } else if (currentRange >= (bytes.length - 1)) {
        // nothing has been modified yet
        throw new IOException("EOF in " + this + " while trying to read " +
            chunkLength + " bytes");
      }

      // we need to consolidate 2 or more buffers into 1
      // first clear out compressed buffers
      ByteBuffer copy = allocateBuffer(chunkLength);
      currentOffset += compressed.remaining();
      len -= compressed.remaining();
      copy.put(compressed);

      while (len > 0 && (++currentRange) < bytes.length) {
        compressed = bytes[currentRange].duplicate();
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
      for(int i = 0; i < bytes.length; ++i) {
        if (offsets[i] <= desired &&
            desired - offsets[i] < bytes[i].remaining()) {
          currentRange = i;
          compressed = bytes[i].duplicate();
          int pos = compressed.position();
          pos += (int)(desired - offsets[i]);
          compressed.position(pos);
          currentOffset = desired;
          return;
        }
      }
      // if they are seeking to the precise end, go ahead and let them go there
      int segments = bytes.length;
      if (segments != 0 &&
          desired == offsets[segments - 1] + bytes[segments - 1].remaining()) {
        currentRange = segments - 1;
        compressed = bytes[currentRange].duplicate();
        compressed.position(compressed.limit());
        currentOffset = desired;
        return;
      }
      throw new IOException("Seek outside of data in " + this + " to " +
        desired);
    }

    private String rangeString() {
      StringBuilder builder = new StringBuilder();
      for(int i=0; i < offsets.length; ++i) {
        if (i != 0) {
          builder.append("; ");
        }
        builder.append(" range " + i + " = " + offsets[i] + " to " +
            bytes[i].remaining());
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
  public static InStream create(String name,
                                ByteBuffer[] input,
                                long[] offsets,
                                long length,
                                CompressionCodec codec,
                                int bufferSize) throws IOException {
    if (codec == null) {
      return new UncompressedStream(name, input, offsets, length);
    } else {
      return new CompressedStream(name, input, offsets, length, codec,
          bufferSize);
    }
  }
}
