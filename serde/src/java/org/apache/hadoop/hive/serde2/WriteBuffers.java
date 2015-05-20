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

import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.ByteStream.RandomAccessOutput;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.io.WritableUtils;


/**
 * The structure storing arbitrary amount of data as a set of fixed-size byte buffers.
 * Maintains read and write pointers for convenient single-threaded writing/reading.
 */
public final class WriteBuffers implements RandomAccessOutput {
  private final ArrayList<byte[]> writeBuffers = new ArrayList<byte[]>(1);
  /** Buffer size in writeBuffers */
  private final int wbSize;
  private final int wbSizeLog2;
  private final long offsetMask;
  private final long maxSize;

  public static class Position {
    private byte[] buffer = null;
    private int bufferIndex = 0;
    private int offset = 0;
    public void clear() {
      buffer = null;
      bufferIndex = offset = -1;
    }
  }

  Position writePos = new Position(); // Position where we'd write
  Position defaultReadPos = new Position(); // Position where we'd read (by default).


  public WriteBuffers(int wbSize, long maxSize) {
    this.wbSize = Integer.bitCount(wbSize) == 1 ? wbSize : (Integer.highestOneBit(wbSize) << 1);
    this.wbSizeLog2 = 31 - Integer.numberOfLeadingZeros(this.wbSize);
    this.offsetMask = this.wbSize - 1;
    this.maxSize = maxSize;
    writePos.bufferIndex = -1;
    nextBufferToWrite();
  }

  public int readVInt() {
    return (int) readVLong(defaultReadPos);
  }

  public int readVInt(Position readPos) {
    return (int) readVLong(readPos);
  }

  public long readVLong() {
    return readVLong(defaultReadPos);
  }

  public long readVLong(Position readPos) {
    ponderNextBufferToRead(readPos);
    byte firstByte = readPos.buffer[readPos.offset++];
    int length = (byte) WritableUtils.decodeVIntSize(firstByte) - 1;
    if (length == 0) {
      return firstByte;
    }
    long i = 0;
    if (isAllInOneReadBuffer(length, readPos)) {
      for (int idx = 0; idx < length; idx++) {
        i = (i << 8) | (readPos.buffer[readPos.offset + idx] & 0xFF);
      }
      readPos.offset += length;
    } else {
      for (int idx = 0; idx < length; idx++) {
        i = (i << 8) | (readNextByte(readPos) & 0xFF);
      }
    }
    return (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }

  public void skipVLong() {
    skipVLong(defaultReadPos);
  }

  public void skipVLong(Position readPos) {
    ponderNextBufferToRead(readPos);
    byte firstByte = readPos.buffer[readPos.offset++];
    int length = (byte) WritableUtils.decodeVIntSize(firstByte);
    if (length > 1) {
      readPos.offset += (length - 1);
    }
    int diff = readPos.offset - wbSize;
    while (diff >= 0) {
      ++readPos.bufferIndex;
      readPos.buffer = writeBuffers.get(readPos.bufferIndex);
      readPos.offset = diff;
      diff = readPos.offset - wbSize;
    }
  }

  public void setReadPoint(long offset) {
    setReadPoint(offset, defaultReadPos);
  }

  public void setReadPoint(long offset, Position readPos) {
    readPos.bufferIndex = getBufferIndex(offset);
    readPos.buffer = writeBuffers.get(readPos.bufferIndex);
    readPos.offset = getOffset(offset);
  }

  public int hashCode(long offset, int length) {
    return hashCode(offset, length, defaultReadPos);
  }

  public int hashCode(long offset, int length, Position readPos) {
    setReadPoint(offset, readPos);
    if (isAllInOneReadBuffer(length, readPos)) {
      int result = murmurHash(readPos.buffer, readPos.offset, length);
      readPos.offset += length;
      return result;
    }

    // Rare case of buffer boundary. Unfortunately we'd have to copy some bytes.
    byte[] bytes = new byte[length];
    int destOffset = 0;
    while (destOffset < length) {
      ponderNextBufferToRead(readPos);
      int toRead = Math.min(length - destOffset, wbSize - readPos.offset);
      System.arraycopy(readPos.buffer, readPos.offset, bytes, destOffset, toRead);
      readPos.offset += toRead;
      destOffset += toRead;
    }
    return murmurHash(bytes, 0, bytes.length);
  }

  private byte readNextByte(Position readPos) {
    // This method is inefficient. It's only used when something crosses buffer boundaries.
    ponderNextBufferToRead(readPos);
    return readPos.buffer[readPos.offset++];
  }

  private void ponderNextBufferToRead(Position readPos) {
    if (readPos.offset >= wbSize) {
      ++readPos.bufferIndex;
      readPos.buffer = writeBuffers.get(readPos.bufferIndex);
      readPos.offset = 0;
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
    if (byteCount < 0) throw new AssertionError("byteCount must be non-negative");
    int currentWriteOffset = writePos.offset + byteCount;
    while (currentWriteOffset > wbSize) {
      nextBufferToWrite();
      currentWriteOffset -= wbSize;
    }
    writePos.offset = currentWriteOffset;
  }

  public void setWritePoint(long offset) {
    writePos.bufferIndex = getBufferIndex(offset);
    writePos.buffer = writeBuffers.get(writePos.bufferIndex);
    writePos.offset = getOffset(offset);
  }

  @Override
  public void write(int b) {
    if (writePos.offset == wbSize) {
      nextBufferToWrite();
    }
    writePos.buffer[writePos.offset++] = (byte)b;
  }

  @Override
  public void write(byte[] b) {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    int srcOffset = 0;
    while (srcOffset < len) {
      int toWrite = Math.min(len - srcOffset, wbSize - writePos.offset);
      System.arraycopy(b, srcOffset + off, writePos.buffer, writePos.offset, toWrite);
      writePos.offset += toWrite;
      srcOffset += toWrite;
      if (writePos.offset == wbSize) {
        nextBufferToWrite();
      }
    }
  }

  @Override
  public int getLength() {
    return (int)getWritePoint();
  }

  private int getOffset(long offset) {
    return (int)(offset & offsetMask);
  }

  private int getBufferIndex(long offset) {
    return (int)(offset >>> wbSizeLog2);
  }

  private void nextBufferToWrite() {
    if (writePos.bufferIndex == (writeBuffers.size() - 1)) {
      if ((1 + writeBuffers.size()) * ((long)wbSize) > maxSize) {
        // We could verify precisely at write time, but just do approximate at allocation time.
        throw new RuntimeException("Too much memory used by write buffers");
      }
      writeBuffers.add(new byte[wbSize]);
    }
    ++writePos.bufferIndex;
    writePos.buffer = writeBuffers.get(writePos.bufferIndex);
    writePos.offset = 0;
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

  /**
   * Compares part of the buffer with a part of an external byte array.
   * Does not modify readPoint.
   */
  public boolean isEqual(byte[] left, int leftOffset, int leftLength, long rightOffset, int rightLength) {
    if (rightLength != leftLength) {
      return false;
    }
    int rightIndex = getBufferIndex(rightOffset), rightFrom = getOffset(rightOffset);
    byte[] rightBuffer = writeBuffers.get(rightIndex);
    if (rightFrom + rightLength <= wbSize) {
      // TODO: allow using unsafe optionally.
      for (int i = 0; i < leftLength; ++i) {
        if (left[leftOffset + i] != rightBuffer[rightFrom + i]) {
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
      if (left[leftOffset + i] != rightBuffer[rightFrom++]) {
        return false;
      }
    }
    return true;
  }

  public void clear() {
    writeBuffers.clear();
    clearState();
  }

  private void clearState() {
    writePos.clear();
    defaultReadPos.clear();
  }


  public long getWritePoint() {
    return ((long)writePos.bufferIndex << wbSizeLog2) + writePos.offset;
  }

  public long getReadPoint() {
    return getReadPoint(defaultReadPos);
  }

  public long getReadPoint(Position readPos) {
    return (readPos.bufferIndex * (long)wbSize) + readPos.offset;
  }

  public void getByteSegmentRefToCurrent(ByteSegmentRef byteSegmentRef, int length,
       Position readPos) {

    byteSegmentRef.reset((readPos.bufferIndex * (long)wbSize) + readPos.offset, length);
    if (length > 0) {
      populateValue(byteSegmentRef);
    }
  }

  public void writeVInt(int value) {
    LazyBinaryUtils.writeVInt(this, value);
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
      if (writePos.offset == wbSize) {
        nextBufferToWrite();
      }
      // How much we can read from current read buffer, out of what we need.
      int toRead = Math.min(length - srcOffset, wbSize - readBufOffset);
      // How much we can write to current write buffer, out of what we need.
      int toWrite = Math.min(toRead, wbSize - writePos.offset);
      System.arraycopy(readBuffer, readBufOffset, writePos.buffer, writePos.offset, toWrite);
      writePos.offset += toWrite;
      readBufOffset += toWrite;
      srcOffset += toWrite;
      if (toRead > toWrite) {
        nextBufferToWrite();
        toRead -= toWrite; // Remains to copy from current read buffer. Less than wbSize by def.
        System.arraycopy(readBuffer, readBufOffset, writePos.buffer, writePos.offset, toRead);
        writePos.offset += toRead;
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
      reset(offset, length);
    }
    public void reset(long offset, int length) {
      if (length < 0) {
        throw new AssertionError("Length is negative: " + length);
      }
      this.offset = offset;
      this.length = length;
    }
    public ByteSegmentRef() {
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
      if (length > 0) {
        System.arraycopy(bytes, (int)offset, copy, 0, length);
      }
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
    // At this point, we are going to make a copy if needed to avoid array boundaries.
    int index = getBufferIndex(value.getOffset());
    byte[] buffer = writeBuffers.get(index);
    int bufferOffset = getOffset(value.getOffset());
    int length = value.getLength();
    if (bufferOffset + length <= wbSize) {
      // Common case - the segment is in one buffer.
      value.bytes = buffer;
      value.offset = bufferOffset;
      return;
    }
    // Special case (rare) - the segment is on buffer boundary.
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

  private boolean isAllInOneReadBuffer(int length, Position readPos) {
    return readPos.offset + length <= wbSize;
  }

  private boolean isAllInOneWriteBuffer(int length) {
    return writePos.offset + length <= wbSize;
  }

  public void seal() {
    if (writePos.offset < (wbSize * 0.8)) { // arbitrary
      byte[] smallerBuffer = new byte[writePos.offset];
      System.arraycopy(writePos.buffer, 0, smallerBuffer, 0, writePos.offset);
      writeBuffers.set(writePos.bufferIndex, smallerBuffer);
    }
    if (writePos.bufferIndex + 1 < writeBuffers.size()) {
      writeBuffers.subList(writePos.bufferIndex + 1, writeBuffers.size()).clear();
    }
    // Make sure we don't reference any old buffer.
    clearState();
  }

  public long readNByteLong(long offset, int bytes) {
    return readNByteLong(offset, bytes, defaultReadPos);
  }

  public long readNByteLong(long offset, int bytes, Position readPos) {
    setReadPoint(offset, readPos);
    long v = 0;
    if (isAllInOneReadBuffer(bytes, readPos)) {
      for (int i = 0; i < bytes; ++i) {
        v = (v << 8) + (readPos.buffer[readPos.offset + i] & 0xff);
      }
      readPos.offset += bytes;
    } else {
      for (int i = 0; i < bytes; ++i) {
        v = (v << 8) + (readNextByte(readPos) & 0xff);
      }
    }
    return v;
  }

  public void writeFiveByteULong(long offset, long v) {
    int prevIndex = writePos.bufferIndex, prevOffset = writePos.offset;
    setWritePoint(offset);
    if (isAllInOneWriteBuffer(5)) {
      writePos.buffer[writePos.offset] = (byte)(v >>> 32);
      writePos.buffer[writePos.offset + 1] = (byte)(v >>> 24);
      writePos.buffer[writePos.offset + 2] = (byte)(v >>> 16);
      writePos.buffer[writePos.offset + 3] = (byte)(v >>> 8);
      writePos.buffer[writePos.offset + 4] = (byte)(v);
      writePos.offset += 5;
    } else {
      setByte(offset++, (byte)(v >>> 32));
      setByte(offset++, (byte)(v >>> 24));
      setByte(offset++, (byte)(v >>> 16));
      setByte(offset++, (byte)(v >>> 8));
      setByte(offset, (byte)(v));
    }
    writePos.bufferIndex = prevIndex;
    writePos.buffer = writeBuffers.get(writePos.bufferIndex);
    writePos.offset = prevOffset;
  }

  public int readInt(long offset) {
    return (int)readNByteLong(offset, 4);
  }

  @Override
  public void writeInt(long offset, int v) {
    int prevIndex = writePos.bufferIndex, prevOffset = writePos.offset;
    setWritePoint(offset);
    if (isAllInOneWriteBuffer(4)) {
      writePos.buffer[writePos.offset] = (byte)(v >> 24);
      writePos.buffer[writePos.offset + 1] = (byte)(v >> 16);
      writePos.buffer[writePos.offset + 2] = (byte)(v >> 8);
      writePos.buffer[writePos.offset + 3] = (byte)(v);
      writePos.offset += 4;
    } else {
      setByte(offset++, (byte)(v >>> 24));
      setByte(offset++, (byte)(v >>> 16));
      setByte(offset++, (byte)(v >>> 8));
      setByte(offset, (byte)(v));
    }
    writePos.bufferIndex = prevIndex;
    writePos.buffer = writeBuffers.get(writePos.bufferIndex);
    writePos.offset = prevOffset;
  }


  @Override
  public void writeByte(long offset, byte value) {
    int prevIndex = writePos.bufferIndex, prevOffset = writePos.offset;
    setWritePoint(offset);
    // One byte is always available for writing.
    writePos.buffer[writePos.offset] = value;

    writePos.bufferIndex = prevIndex;
    writePos.buffer = writeBuffers.get(writePos.bufferIndex);
    writePos.offset = prevOffset;
  }

  // Lifted from org.apache.hadoop.util.hash.MurmurHash... but supports offset.
  public static int murmurHash(byte[] data, int offset, int length) {
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

  /**
   * Write buffer size
   * @return write buffer size
   */
  public long size() {
    return writeBuffers.size() * (long) wbSize;
  }

  public Position getReadPosition() {
    return defaultReadPos;
  }
}