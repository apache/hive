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
import java.nio.ByteBuffer;

class OutStream extends PositionedOutputStream {

  interface OutputReceiver {
    /**
     * Output the given buffer to the final destination
     * @param buffer the buffer to output
     * @throws IOException
     */
    void output(ByteBuffer buffer) throws IOException;
  }

  static final int HEADER_SIZE = 3;
  private final String name;
  private final OutputReceiver receiver;
  // if enabled the stream will be suppressed when writing stripe
  private boolean suppress;

  /**
   * Stores the uncompressed bytes that have been serialized, but not
   * compressed yet. When this fills, we compress the entire buffer.
   */
  private ByteBuffer current = null;

  /**
   * Stores the compressed bytes until we have a full buffer and then outputs
   * them to the receiver. If no compression is being done, this (and overflow)
   * will always be null and the current buffer will be sent directly to the
   * receiver.
   */
  private ByteBuffer compressed = null;

  /**
   * Since the compressed buffer may start with contents from previous
   * compression blocks, we allocate an overflow buffer so that the
   * output of the codec can be split between the two buffers. After the
   * compressed buffer is sent to the receiver, the overflow buffer becomes
   * the new compressed buffer.
   */
  private ByteBuffer overflow = null;
  private final int bufferSize;
  private final CompressionCodec codec;
  private long compressedBytes = 0;
  private long uncompressedBytes = 0;

  OutStream(String name,
            int bufferSize,
            CompressionCodec codec,
            OutputReceiver receiver) throws IOException {
    this.name = name;
    this.bufferSize = bufferSize;
    this.codec = codec;
    this.receiver = receiver;
    this.suppress = false;
  }

  public void clear() throws IOException {
    flush();
    suppress = false;
  }

  /**
   * Write the length of the compressed bytes. Life is much easier if the
   * header is constant length, so just use 3 bytes. Considering most of the
   * codecs want between 32k (snappy) and 256k (lzo, zlib), 3 bytes should
   * be plenty. We also use the low bit for whether it is the original or
   * compressed bytes.
   * @param buffer the buffer to write the header to
   * @param position the position in the buffer to write at
   * @param val the size in the file
   * @param original is it uncompressed
   */
  private static void writeHeader(ByteBuffer buffer,
                                  int position,
                                  int val,
                                  boolean original) {
    buffer.put(position, (byte) ((val << 1) + (original ? 1 : 0)));
    buffer.put(position + 1, (byte) (val >> 7));
    buffer.put(position + 2, (byte) (val >> 15));
  }

  private void getNewInputBuffer() throws IOException {
    if (codec == null) {
      current = ByteBuffer.allocate(bufferSize);
    } else {
      current = ByteBuffer.allocate(bufferSize + HEADER_SIZE);
      writeHeader(current, 0, bufferSize, true);
      current.position(HEADER_SIZE);
    }
  }

  /**
   * Allocate a new output buffer if we are compressing.
   */
  private ByteBuffer getNewOutputBuffer() throws IOException {
    return ByteBuffer.allocate(bufferSize + HEADER_SIZE);
  }

  private void flip() throws IOException {
    current.limit(current.position());
    current.position(codec == null ? 0 : HEADER_SIZE);
  }

  @Override
  public void write(int i) throws IOException {
    if (current == null) {
      getNewInputBuffer();
    }
    if (current.remaining() < 1) {
      spill();
    }
    uncompressedBytes += 1;
    current.put((byte) i);
  }

  @Override
  public void write(byte[] bytes, int offset, int length) throws IOException {
    if (current == null) {
      getNewInputBuffer();
    }
    int remaining = Math.min(current.remaining(), length);
    current.put(bytes, offset, remaining);
    uncompressedBytes += remaining;
    length -= remaining;
    while (length != 0) {
      spill();
      offset += remaining;
      remaining = Math.min(current.remaining(), length);
      current.put(bytes, offset, remaining);
      uncompressedBytes += remaining;
      length -= remaining;
    }
  }

  private void spill() throws java.io.IOException {
    // if there isn't anything in the current buffer, don't spill
    if (current == null ||
        current.position() == (codec == null ? 0 : HEADER_SIZE)) {
      return;
    }
    flip();
    if (codec == null) {
      receiver.output(current);
      getNewInputBuffer();
    } else {
      if (compressed == null) {
        compressed = getNewOutputBuffer();
      } else if (overflow == null) {
        overflow = getNewOutputBuffer();
      }
      int sizePosn = compressed.position();
      compressed.position(compressed.position() + HEADER_SIZE);
      if (codec.compress(current, compressed, overflow)) {
        uncompressedBytes = 0;
        // move position back to after the header
        current.position(HEADER_SIZE);
        current.limit(current.capacity());
        // find the total bytes in the chunk
        int totalBytes = compressed.position() - sizePosn - HEADER_SIZE;
        if (overflow != null) {
          totalBytes += overflow.position();
        }
        compressedBytes += totalBytes + HEADER_SIZE;
        writeHeader(compressed, sizePosn, totalBytes, false);
        // if we have less than the next header left, spill it.
        if (compressed.remaining() < HEADER_SIZE) {
          compressed.flip();
          receiver.output(compressed);
          compressed = overflow;
          overflow = null;
        }
      } else {
        compressedBytes += uncompressedBytes + HEADER_SIZE;
        uncompressedBytes = 0;
        // we are using the original, but need to spill the current
        // compressed buffer first. So back up to where we started,
        // flip it and add it to done.
        if (sizePosn != 0) {
          compressed.position(sizePosn);
          compressed.flip();
          receiver.output(compressed);
          compressed = null;
          // if we have an overflow, clear it and make it the new compress
          // buffer
          if (overflow != null) {
            overflow.clear();
            compressed = overflow;
            overflow = null;
          }
        } else {
          compressed.clear();
          if (overflow != null) {
            overflow.clear();
          }
        }

        // now add the current buffer into the done list and get a new one.
        current.position(0);
        // update the header with the current length
        writeHeader(current, 0, current.limit() - HEADER_SIZE, true);
        receiver.output(current);
        getNewInputBuffer();
      }
    }
  }

  void getPosition(PositionRecorder recorder) throws IOException {
    if (codec == null) {
      recorder.addPosition(uncompressedBytes);
    } else {
      recorder.addPosition(compressedBytes);
      recorder.addPosition(uncompressedBytes);
    }
  }

  @Override
  public void flush() throws IOException {
    spill();
    if (compressed != null && compressed.position() != 0) {
      compressed.flip();
      receiver.output(compressed);
    }
    compressed = null;
    uncompressedBytes = 0;
    compressedBytes = 0;
    overflow = null;
    current = null;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public long getBufferSize() {
    long result = 0;
    if (current != null) {
      result += current.capacity();
    }
    if (compressed != null) {
      result += compressed.capacity();
    }
    if (overflow != null) {
      result += overflow.capacity();
    }
    return result;
  }

  /**
   * Set suppress flag
   */
  public void suppress() {
    suppress = true;
  }

  /**
   * Returns the state of suppress flag
   * @return value of suppress flag
   */
  public boolean isSuppressed() {
    return suppress;
  }
}

