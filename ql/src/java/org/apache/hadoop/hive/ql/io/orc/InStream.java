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
    private byte[] range;
    private int currentRange;
    private int offsetInRange;
    private int limitInRange;

    public UncompressedStream(String name, ByteBuffer[] input, long[] offsets,
                              long length) {
      this.name = name;
      this.bytes = input;
      this.offsets = offsets;
      this.length = length;
      currentRange = 0;
      offsetInRange = 0;
      limitInRange = 0;
      currentOffset = 0;
    }

    @Override
    public int read() {
      if (offsetInRange >= limitInRange) {
        if (currentOffset == length) {
          return -1;
        }
        seek(currentOffset);
      }
      currentOffset += 1;
      return 0xff & range[offsetInRange++];
    }

    @Override
    public int read(byte[] data, int offset, int length) {
      if (offsetInRange >= limitInRange) {
        if (currentOffset == this.length) {
          return -1;
        }
        seek(currentOffset);
      }
      int actualLength = Math.min(length, limitInRange - offsetInRange);
      System.arraycopy(range, offsetInRange, data, offset, actualLength);
      offsetInRange += actualLength;
      currentOffset += actualLength;
      return actualLength;
    }

    @Override
    public int available() {
      if (offsetInRange < limitInRange) {
        return limitInRange - offsetInRange;
      }
      return (int) (length - currentOffset);
    }

    @Override
    public void close() {
      currentRange = bytes.length;
      currentOffset = length;
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
          this.range = bytes[i].array();
          offsetInRange = bytes[i].arrayOffset() + bytes[i].position();
          limitInRange = bytes[i].arrayOffset() + bytes[i].limit();
          offsetInRange += desired - offsets[i];
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
          " offset: " + offsetInRange + " limit: " + limitInRange;
    }
  }

  private static class CompressedStream extends InStream {
    private final String name;
    private final ByteBuffer[] bytes;
    private final long[] offsets;
    private final int bufferSize;
    private final long length;
    private ByteBuffer uncompressed = null;
    private final CompressionCodec codec;
    private byte[] compressed = null;
    private long currentOffset;
    private int currentRange;
    private int offsetInCompressed;
    private int limitInCompressed;
    private boolean isUncompressedOriginal;

    public CompressedStream(String name, ByteBuffer[] input,
                            long[] offsets, long length,
                            CompressionCodec codec, int bufferSize
                           ) {
      this.bytes = input;
      this.name = name;
      this.codec = codec;
      this.length = length;
      this.offsets = offsets;
      this.bufferSize = bufferSize;
      currentOffset = 0;
      currentRange = 0;
      offsetInCompressed = 0;
      limitInCompressed = 0;
    }

    private void readHeader() throws IOException {
      if (compressed == null || offsetInCompressed >= limitInCompressed) {
        seek(currentOffset);
      }
      if (limitInCompressed - offsetInCompressed > OutStream.HEADER_SIZE) {
        int chunkLength = ((0xff & compressed[offsetInCompressed + 2]) << 15) |
          ((0xff & compressed[offsetInCompressed + 1]) << 7) |
            ((0xff & compressed[offsetInCompressed]) >> 1);
        if (chunkLength > bufferSize) {
          throw new IllegalArgumentException("Buffer size too small. size = " +
              bufferSize + " needed = " + chunkLength);
        }
        boolean isOriginal = (compressed[offsetInCompressed] & 0x01) == 1;
        offsetInCompressed += OutStream.HEADER_SIZE;
        if (isOriginal) {
          isUncompressedOriginal = true;
          uncompressed = bytes[currentRange].duplicate();
          uncompressed.position(offsetInCompressed -
              bytes[currentRange].arrayOffset());
          uncompressed.limit(offsetInCompressed + chunkLength);
        } else {
          if (isUncompressedOriginal) {
            uncompressed = ByteBuffer.allocate(bufferSize);
            isUncompressedOriginal = false;
          } else if (uncompressed == null) {
            uncompressed = ByteBuffer.allocate(bufferSize);
          } else {
            uncompressed.clear();
          }
          codec.decompress(ByteBuffer.wrap(compressed, offsetInCompressed,
              chunkLength),
            uncompressed);
        }
        offsetInCompressed += chunkLength;
        currentOffset += chunkLength + OutStream.HEADER_SIZE;
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
      System.arraycopy(uncompressed.array(),
        uncompressed.arrayOffset() + uncompressed.position(), data,
        offset, actualLength);
      uncompressed.position(uncompressed.position() + actualLength);
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
      currentRange = bytes.length;
      offsetInCompressed = 0;
      limitInCompressed = 0;
      currentOffset = length;
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

    private void seek(long desired) throws IOException {
      for(int i = 0; i < bytes.length; ++i) {
        if (offsets[i] <= desired &&
            desired - offsets[i] < bytes[i].remaining()) {
          currentRange = i;
          compressed = bytes[i].array();
          offsetInCompressed = (int) (bytes[i].arrayOffset() +
              bytes[i].position() + (desired - offsets[i]));
          currentOffset = desired;
          limitInCompressed = bytes[i].arrayOffset() + bytes[i].limit();
          return;
        }
      }
      // if they are seeking to the precise end, go ahead and let them go there
      int segments = bytes.length;
      if (segments != 0 &&
          desired == offsets[segments - 1] + bytes[segments - 1].remaining()) {
        currentRange = segments - 1;
        compressed = bytes[currentRange].array();
        offsetInCompressed = bytes[currentRange].arrayOffset() +
          bytes[currentRange].limit();
        currentOffset = desired;
        limitInCompressed = offsetInCompressed;
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
          " offset: " + offsetInCompressed + " limit: " + limitInCompressed +
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
