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

package org.apache.hadoop.hive.serde2;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hadoop.hive.serde2.ByteStream.RandomAccessOutput;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.hash.MurmurHash;


/**
 * The structure storing arbitrary amount of data as a set of fixed-size byte buffers.
 * Maintains read and write pointers for convenient single-threaded writing/reading.
 */
public final class WriteBuffers implements RandomAccessOutput {
  private final ArrayList<byte[]> writeBuffers = new ArrayList<byte[]>(1);
  /** Buffer size in writeBuffers */
  private final int wbSize;
  private final long maxSize;

  private byte[] currentWriteBuffer;
  private int currentWriteBufferIndex;
  /** The offset in the last writeBuffer where the values are added */
  private int currentWriteOffset = 0;

  private byte[] currentReadBuffer = null;
  private int currentReadBufferIndex = 0;
  private int currentReadOffset = 0;

  public WriteBuffers(int wbSize, long maxSize) {
    this.wbSize = wbSize;
    this.maxSize = maxSize;
    currentWriteBufferIndex = -1;
    nextBufferToWrite();
  }

  public long readVLong() {
    ponderNextBufferToRead();
    byte firstByte = currentReadBuffer[currentReadOffset++];
    int length = (byte) WritableUtils.decodeVIntSize(firstByte) - 1;
    if (length == 0) {
      return firstByte;
    }
    long i = 0;
    if (isAllInOneReadBuffer(length)) {
      for (int idx = 0; idx < length; idx++) {
        i = (i << 8) | (currentReadBuffer[currentReadOffset + idx] & 0xFF);
      }
      currentReadOffset += length;
    } else {
      for (int idx = 0; idx < length; idx++) {
        i = (i << 8) | (readNextByte() & 0xFF);
      }
    }
    return (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }

  public void skipVLong() {
    ponderNextBufferToRead();
    byte firstByte = currentReadBuffer[currentReadOffset++];
    int length = (byte) WritableUtils.decodeVIntSize(firstByte);
    if (length > 1) {
      currentReadOffset += (length - 1);
    }
    int diff = currentReadOffset - wbSize;
    while (diff >= 0) {
      ++currentReadBufferIndex;
      currentReadBuffer = writeBuffers.get(currentReadBufferIndex);
      currentReadOffset = diff;
      diff = currentReadOffset - wbSize;
    }
  }

  public void setReadPoint(long offset) {
    currentReadBufferIndex = getBufferIndex(offset);
    currentReadBuffer = writeBuffers.get(currentReadBufferIndex);
    currentReadOffset = getOffset(offset);
  }

  public int hashCode(long offset, int length) {
    setReadPoint(offset);
    if (isAllInOneReadBuffer(length)) {
      int result = murmurHash(currentReadBuffer, currentReadOffset, length);
      currentReadOffset += length;
      return result;
    }

    // Rare case of buffer boundary. Unfortunately we'd have to copy some bytes.
    byte[] bytes = new byte[length];
    int destOffset = 0;
    while (destOffset < length) {
      ponderNextBufferToRead();
      int toRead = Math.min(length - destOffset, wbSize - currentReadOffset);
      System.arraycopy(currentReadBuffer, currentReadOffset, bytes, destOffset, toRead);
      currentReadOffset += toRead;
      destOffset += toRead;
    }
    return murmurHash(bytes, 0, bytes.length);
  }

  private byte readNextByte() {
    // This method is inefficient. It's only used when something crosses buffer boundaries.
    ponderNextBufferToRead();
    return currentReadBuffer[currentReadOffset++];
  }

  private void ponderNextBufferToRead() {
    if (currentReadOffset >= wbSize) {
      ++currentReadBufferIndex;
      currentReadBuffer = writeBuffers.get(currentReadBufferIndex);
      currentReadOffset = 0;
    }
  }

  public int hashCode(byte[] key, int offset, int length) {
    return murmurHash(key, offset, length);
  }

  private void setByte(long offset, byte value) {
    // No checks, the caller must ensure the offsets are correct.
    writeBuffers.get(getBufferIndex(offset))[getOffset(offset)] = value;
  }

  @Override
  public void reserve(int byteCount) {
    if (byteCount < 0) throw new AssertionError("byteCount must be positive");
    int currentWriteOffset = this.currentWriteOffset + byteCount;
    while (currentWriteOffset > wbSize) {
      nextBufferToWrite();
      currentWriteOffset -= wbSize;
    }
    this.currentWriteOffset = currentWriteOffset;
  }

  public void setWritePoint(long offset) {
    currentWriteBufferIndex = getBufferIndex(offset);
    currentWriteBuffer = writeBuffers.get(currentWriteBufferIndex);
    currentWriteOffset = getOffset(offset);
  }

  @Override
  public void write(int b) {
    if (currentWriteOffset == wbSize) {
      nextBufferToWrite();
    }
    currentWriteBuffer[currentWriteOffset++] = (byte)b;
  }

  @Override
  public void write(byte[] b) {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    int srcOffset = 0;
    while (srcOffset < len) {
      int toWrite = Math.min(len - srcOffset, wbSize - currentWriteOffset);
      System.arraycopy(b, srcOffset + off, currentWriteBuffer, currentWriteOffset, toWrite);
      currentWriteOffset += toWrite;
      srcOffset += toWrite;
      if (currentWriteOffset == wbSize) {
        nextBufferToWrite();
      }
    }
  }

  @Override
  public int getLength() {
    return (int)getWritePoint();
  }

  private int getOffset(long offset) {
    return (int)(offset % wbSize);
  }

  private int getBufferIndex(long offset) {
    return (int)(offset / wbSize);
  }

  private void nextBufferToWrite() {
    if (currentWriteBufferIndex == (writeBuffers.size() - 1)) {
      if ((1 + writeBuffers.size()) * ((long)wbSize) > maxSize) {
        // We could verify precisely at write time, but just do approximate at allocation time.
        throw new RuntimeException("Too much memory used by write buffers");
      }
      writeBuffers.add(new byte[wbSize]);
    }
    ++currentWriteBufferIndex;
    currentWriteBuffer = writeBuffers.get(currentWriteBufferIndex);
    currentWriteOffset = 0;
  }

  /** Compares two parts of the buffer with each other. Does not modify readPoint. */
  public boolean isEqual(long leftOffset, int leftLength, long rightOffset, int rightLength) {
    if (rightLength != leftLength) {
      return false;
    }
    int leftIndex = getBufferIndex(leftOffset), rightIndex = getBufferIndex(rightOffset),
        leftFrom = getOffset(leftOffset), rightFrom = getOffset(rightOffset);
    byte[] leftBuffer = writeBuffers.get(leftIndex), rightBuffer = writeBuffers.get(rightIndex);
    if (leftFrom + leftLength <= wbSize && rightFrom + rightLength <= wbSize) {
      for (int i = 0; i < leftLength; ++i) {
        if (leftBuffer[leftFrom + i] != rightBuffer[rightFrom + i]) {
          return false;
        }
      }
      return true;
    }
    for (int i = 0; i < leftLength; ++i) {
      if (leftFrom == wbSize) {
        ++leftIndex;
        leftBuffer = writeBuffers.get(leftIndex);
        leftFrom = 0;
      }
      if (rightFrom == wbSize) {
        ++rightIndex;
        rightBuffer = writeBuffers.get(rightIndex);
        rightFrom = 0;
      }
      if (leftBuffer[leftFrom++] != rightBuffer[rightFrom++]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Compares part of the buffer with a part of an external byte array.
   * Does not modify readPoint.
   */
  public boolean isEqual(byte[] left, int leftLength, long rightOffset, int rightLength) {
    if (rightLength != leftLength) {
      return false;
    }
    int rightIndex = getBufferIndex(rightOffset), rightFrom = getOffset(rightOffset);
    byte[] rightBuffer = writeBuffers.get(rightIndex);
    if (rightFrom + rightLength <= wbSize) {
      // TODO: allow using unsafe optionally.
      for (int i = 0; i < leftLength; ++i) {
        if (left[i] != rightBuffer[rightFrom + i]) {
          return false;
        }
      }
      return true;
    }
    for (int i = 0; i < rightLength; ++i) {
      if (rightFrom == wbSize) {
        ++rightIndex;
        rightBuffer = writeBuffers.get(rightIndex);
        rightFrom = 0;
      }
      if (left[i] != rightBuffer[rightFrom++]) {
        return false;
      }
    }
    return true;
  }

  public void clear() {
    writeBuffers.clear();
    currentWriteBuffer = currentReadBuffer = null;
    currentWriteOffset = currentReadOffset = currentWriteBufferIndex = currentReadBufferIndex = 0;
  }

  public long getWritePoint() {
    return (currentWriteBufferIndex * (long)wbSize) + currentWriteOffset;
  }

  public long getReadPoint() {
    return (currentReadBufferIndex * (long)wbSize) + currentReadOffset;
  }

  public void writeVLong(long value) {
    LazyBinaryUtils.writeVLong(this, value);
  }

  /** Reads some bytes from the buffer and writes them again at current write point. */
  public void writeBytes(long offset, int length) {
    int readBufIndex = getBufferIndex(offset);
    byte[] readBuffer = writeBuffers.get(readBufIndex);
    int readBufOffset = getOffset(offset);
    int srcOffset = 0;
    while (srcOffset < length) {
      if (readBufOffset == wbSize) {
        ++readBufIndex;
        readBuffer = writeBuffers.get(readBufIndex);
        readBufOffset = 0;
      }
      if (currentWriteOffset == wbSize) {
        nextBufferToWrite();
      }
      // How much we can read from current read buffer, out of what we need.
      int toRead = Math.min(length - srcOffset, wbSize - readBufOffset);
      // How much we can write to current write buffer, out of what we need.
      int toWrite = Math.min(toRead, wbSize - currentWriteOffset);
      System.arraycopy(readBuffer, readBufOffset, currentWriteBuffer, currentWriteOffset, toWrite);
      currentWriteOffset += toWrite;
      readBufOffset += toWrite;
      srcOffset += toWrite;
      if (toRead > toWrite) {
        nextBufferToWrite();
        toRead -= toWrite; // Remains to copy from current read buffer. Less than wbSize by def.
        System.arraycopy(readBuffer, readBufOffset, currentWriteBuffer, currentWriteOffset, toRead);
        currentWriteOffset += toRead;
        readBufOffset += toRead;
        srcOffset += toRead;
      }
    }
  }

  /**
   * The class representing a segment of bytes in the buffer. Can either be a reference
   * to a segment of the whole WriteBuffers (when bytes is not set), or to a segment of
   * some byte array (when bytes is set).
   */
  public static class ByteSegmentRef {
    public ByteSegmentRef(long offset, int length) {
      if (length < 0) {
        throw new AssertionError("Length is negative: " + length);
      }
      this.offset = offset;
      this.length = length;
    }
    public byte[] getBytes() {
      return bytes;
    }
    public long getOffset() {
      return offset;
    }
    public int getLength() {
      return length;
    }
    public ByteBuffer copy() {
      byte[] copy = new byte[length];
      System.arraycopy(bytes, (int)offset, copy, 0, length);
      return ByteBuffer.wrap(copy);
    }
    private byte[] bytes = null;
    private long offset;
    private int length;
  }

  /**
   * Changes the byte segment reference from being a reference to global buffer to
   * the one with a self-contained byte array. The byte array will either be one of
   * the internal ones, or a copy of data if the original reference pointed to a data
   * spanning multiple internal buffers.
   */
  public void populateValue(WriteBuffers.ByteSegmentRef value) {
    // At this point, we are going to make a copy if need to avoid array boundaries.
    int index = getBufferIndex(value.getOffset());
    byte[] buffer = writeBuffers.get(index);
    int bufferOffset = getOffset(value.getOffset());
    int length = value.getLength();
    if (bufferOffset + length <= wbSize) {
      value.bytes = buffer;
      value.offset = bufferOffset;
    } else {
      value.bytes = new byte[length];
      value.offset = 0;
      int destOffset = 0;
      while (destOffset < length) {
        if (destOffset > 0) {
          buffer = writeBuffers.get(++index);
          bufferOffset = 0;
        }
        int toCopy = Math.min(length - destOffset, wbSize - bufferOffset);
        System.arraycopy(buffer, bufferOffset, value.bytes, destOffset, toCopy);
        destOffset += toCopy;
      }
    }
  }

  private boolean isAllInOneReadBuffer(int length) {
    return currentReadOffset + length <= wbSize;
  }

  private boolean isAllInOneWriteBuffer(int length) {
    return currentWriteOffset + length <= wbSize;
  }

  public void seal() {
    if (currentWriteOffset < (wbSize * 0.8)) { // arbitrary
      byte[] smallerBuffer = new byte[currentWriteOffset];
      System.arraycopy(currentWriteBuffer, 0, smallerBuffer, 0, currentWriteOffset);
      writeBuffers.set(currentWriteBufferIndex, smallerBuffer);
    }
    if (currentWriteBufferIndex + 1 < writeBuffers.size()) {
      writeBuffers.subList(currentWriteBufferIndex + 1, writeBuffers.size()).clear();
    }
    currentWriteBuffer = currentReadBuffer = null; // Make sure we don't reference any old buffer.
    currentWriteBufferIndex = currentReadBufferIndex = -1;
    currentReadOffset = currentWriteOffset = -1;
  }

  public long readFiveByteULong(long offset) {
    return readNByteLong(offset, 5);
  }

  private long readNByteLong(long offset, int bytes) {
    setReadPoint(offset);
    long v = 0;
    if (isAllInOneReadBuffer(bytes)) {
      for (int i = 0; i < bytes; ++i) {
        v = (v << 8) + (currentReadBuffer[currentReadOffset + i] & 0xff);
      }
      currentReadOffset += bytes;
    } else {
      for (int i = 0; i < bytes; ++i) {
        v = (v << 8) + (readNextByte() & 0xff);
      }
    }
    return v;
  }

  public void writeFiveByteULong(long offset, long v) {
    int prevIndex = currentWriteBufferIndex, prevOffset = currentWriteOffset;
    setWritePoint(offset);
    if (isAllInOneWriteBuffer(5)) {
      currentWriteBuffer[currentWriteOffset++] = (byte)(v >>> 32);
      currentWriteBuffer[currentWriteOffset++] = (byte)(v >>> 24);
      currentWriteBuffer[currentWriteOffset++] = (byte)(v >>> 16);
      currentWriteBuffer[currentWriteOffset++] = (byte)(v >>> 8);
      currentWriteBuffer[currentWriteOffset] = (byte)(v);
    } else {
      setByte(offset++, (byte)(v >>> 32));
      setByte(offset++, (byte)(v >>> 24));
      setByte(offset++, (byte)(v >>> 16));
      setByte(offset++, (byte)(v >>> 8));
      setByte(offset, (byte)(v));
    }
    currentWriteBufferIndex = prevIndex;
    currentWriteBuffer = writeBuffers.get(currentWriteBufferIndex);
    currentWriteOffset = prevOffset;
  }

  public int readInt(long offset) {
    return (int)readNByteLong(offset, 4);
  }

  @Override
  public void writeInt(long offset, int v) {
    int prevIndex = currentWriteBufferIndex, prevOffset = currentWriteOffset;
    setWritePoint(offset);
    if (isAllInOneWriteBuffer(4)) {
      currentWriteBuffer[currentWriteOffset++] = (byte)(v >> 24);
      currentWriteBuffer[currentWriteOffset++] = (byte)(v >> 16);
      currentWriteBuffer[currentWriteOffset++] = (byte)(v >> 8);
      currentWriteBuffer[currentWriteOffset] = (byte)(v);
    } else {
      setByte(offset++, (byte)(v >>> 24));
      setByte(offset++, (byte)(v >>> 16));
      setByte(offset++, (byte)(v >>> 8));
      setByte(offset, (byte)(v));
    }
    currentWriteBufferIndex = prevIndex;
    currentWriteBuffer = writeBuffers.get(currentWriteBufferIndex);
    currentWriteOffset = prevOffset;
  }

  // Lifted from org.apache.hadoop.util.hash.MurmurHash... but supports offset.
  private static int murmurHash(byte[] data, int offset, int length) {
    int m = 0x5bd1e995;
    int r = 24;

    int h = length;

    int len_4 = length >> 2;

    for (int i = 0; i < len_4; i++) {
      int i_4 = offset + (i << 2);
      int k = data[i_4 + 3];
      k = k << 8;
      k = k | (data[i_4 + 2] & 0xff);
      k = k << 8;
      k = k | (data[i_4 + 1] & 0xff);
      k = k << 8;
      k = k | (data[i_4 + 0] & 0xff);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }

    // avoid calculating modulo
    int len_m = len_4 << 2;
    int left = length - len_m;

    if (left != 0) {
      length += offset;
      if (left >= 3) {
        h ^= (int) data[length - 3] << 16;
      }
      if (left >= 2) {
        h ^= (int) data[length - 2] << 8;
      }
      if (left >= 1) {
        h ^= (int) data[length - 1];
      }

      h *= m;
    }

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;

    return h;
  }
}