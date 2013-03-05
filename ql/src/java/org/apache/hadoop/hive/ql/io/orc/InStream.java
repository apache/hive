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
    private byte[] array;
    private int offset;
    private final int base;
    private final int limit;

    public UncompressedStream(String name, ByteBuffer input) {
      this.name = name;
      this.array = input.array();
      base = input.arrayOffset() + input.position();
      offset = base;
      limit = input.arrayOffset() + input.limit();
    }

    @Override
    public int read() {
      if (offset == limit) {
        return -1;
      }
      return 0xff & array[offset++];
    }

    @Override
    public int read(byte[] data, int offset, int length) {
      if (this.offset == limit) {
        return -1;
      }
      int actualLength = Math.min(length, limit - this.offset);
      System.arraycopy(array, this.offset, data, offset, actualLength);
      this.offset += actualLength;
      return actualLength;
    }

    @Override
    public int available() {
      return limit - offset;
    }

    @Override
    public void close() {
      array = null;
      offset = 0;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      offset = base + (int) index.getNext();
    }

    @Override
    public String toString() {
      return "uncompressed stream " + name + " base: " + base +
         " offset: " + offset + " limit: " + limit;
    }
  }

  private static class CompressedStream extends InStream {
    private final String name;
    private byte[] array;
    private final int bufferSize;
    private ByteBuffer uncompressed = null;
    private final CompressionCodec codec;
    private int offset;
    private final int base;
    private final int limit;
    private boolean isUncompressedOriginal;

    public CompressedStream(String name, ByteBuffer input,
                            CompressionCodec codec, int bufferSize
                           ) {
      this.array = input.array();
      this.name = name;
      this.codec = codec;
      this.bufferSize = bufferSize;
      base = input.arrayOffset() + input.position();
      offset = base;
      limit = input.arrayOffset() + input.limit();
    }

    private void readHeader() throws IOException {
      if (limit - offset > OutStream.HEADER_SIZE) {
        int chunkLength = ((0xff & array[offset + 2]) << 15) |
          ((0xff & array[offset + 1]) << 7) | ((0xff & array[offset]) >> 1);
        if (chunkLength > bufferSize) {
          throw new IllegalArgumentException("Buffer size too small. size = " +
              bufferSize + " needed = " + chunkLength);
        }
        boolean isOriginal = (array[offset] & 0x01) == 1;
        offset += OutStream.HEADER_SIZE;
        if (isOriginal) {
          isUncompressedOriginal = true;
          uncompressed = ByteBuffer.wrap(array, offset, chunkLength);
        } else {
          if (isUncompressedOriginal) {
            uncompressed = ByteBuffer.allocate(bufferSize);
            isUncompressedOriginal = false;
          } else if (uncompressed == null) {
            uncompressed = ByteBuffer.allocate(bufferSize);
          } else {
            uncompressed.clear();
          }
          codec.decompress(ByteBuffer.wrap(array, offset, chunkLength),
            uncompressed);
        }
        offset += chunkLength;
      } else {
        throw new IllegalStateException("Can't read header");
      }
    }

    @Override
    public int read() throws IOException {
      if (uncompressed == null || uncompressed.remaining() == 0) {
        if (offset == limit) {
          return -1;
        }
        readHeader();
      }
      return 0xff & uncompressed.get();
    }

    @Override
    public int read(byte[] data, int offset, int length) throws IOException {
      if (uncompressed == null || uncompressed.remaining() == 0) {
        if (this.offset == this.limit) {
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
        if (offset == limit) {
          return 0;
        }
        readHeader();
      }
      return uncompressed.remaining();
    }

    @Override
    public void close() {
      array = null;
      uncompressed = null;
      offset = 0;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      offset = base + (int) index.getNext();
      int uncompBytes = (int) index.getNext();
      if (uncompBytes != 0) {
        readHeader();
        uncompressed.position(uncompressed.position() + uncompBytes);
      } else if (uncompressed != null) {
        uncompressed.position(uncompressed.limit());
      }
    }

    @Override
    public String toString() {
      return "compressed stream " + name + " base: " + base +
          " offset: " + offset + " limit: " + limit +
          (uncompressed == null ? "" :
              " uncompressed: " + uncompressed.position() + " to " +
                  uncompressed.limit());
    }
  }

  public abstract void seek(PositionProvider index) throws IOException;

  public static InStream create(String name,
                                ByteBuffer input,
                                CompressionCodec codec,
                                int bufferSize) throws IOException {
    if (codec == null) {
      return new UncompressedStream(name, input);
    } else {
      return new CompressedStream(name, input, codec, bufferSize);
    }
  }
}
