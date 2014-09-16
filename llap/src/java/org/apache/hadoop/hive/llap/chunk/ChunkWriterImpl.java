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

package org.apache.hadoop.hive.llap.chunk;

import java.nio.ByteBuffer;

import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.api.Llap;
import org.apache.hadoop.hive.llap.api.Vector.Type;
import org.apache.hadoop.hive.llap.chunk.ChunkUtils;
import org.apache.hadoop.hive.llap.chunk.ChunkUtils.RleSegmentType;
import org.apache.hadoop.hive.llap.loader.BufferInProgress;
import org.apache.hadoop.hive.llap.loader.ChunkPool.Chunk;

/**
 * Chunk writer. Reusable, not thread safe. See ChunkReader for format details.
 */
public class ChunkWriterImpl implements ChunkWriter {
  private BufferInProgress colBuffer;

  public void prepare(BufferInProgress colBuffer) {
    this.colBuffer = colBuffer;
    Chunk chunk = this.colBuffer.ensureChunk();
    if (chunk.length == 0) {
      // This is a new chunk; reserve space for header.
      colBuffer.offset += 8;
      chunk.length = 8;
    }
    valuesOffset = colBuffer.offset;
  }

  /**
   * Completes the chunk by writing the header with externally-tracked row count.
   * Does not have to be prepare()-d - any chunk can be update via this method.
   * @param chunk Chunk to update.
   * @param rowCount Row count in the chunk.
   */
  public void finishChunk(Chunk chunk, int rowCount) {
    // The space for chunk start is reserved; no need to update offset or length.
    assert currentSegmentStart == -1;
    ByteBuffer buf = chunk.buffer.getContents();
    buf.put(chunk.offset, ChunkUtils.FORMAT_VERSION);
    buf.putInt(chunk.offset + 1, rowCount);
  }

  // State of current segment.
  private int currentSegmentStart = -1, currentSegmentValues = -1;
  private boolean currentSegmentIsRepeating = false, currentSegmentHasNulls = false;
  private long currentRepeatingLongValue = -1;
  private int currentSizeOf = -1;

  // State of the unique segment currently being written.
  private int currentBitmaskOffset = -1, currentBitmaskLimit = -1,
      valuesOffset = -1, valuesSinceBitmask = -1;

  @Override
  public int estimateValueCountThatFits(Type type, boolean hasNulls) {
    // Assume we'd only need to write unique values without nulls, we can always do that.
    // If we are in the middle of a bitmask segment, space for bitmask was already reserved
    // so values will take just as much space as without bitmask.
    // Caller is supposed to re-estimate after every write.
    return (colBuffer.getSpaceLeft(valuesOffset) - 8) / ChunkUtils.TYPE_SIZES[type.value()];
  }

  @Override
  public void writeLongs(long[] src, int srcOffset, int srcCount, NullsState nullsState) {
    writeLongsInternal(src, srcOffset, srcCount, nullsState, true);
  }

  @Override
  public void writeLongs(byte[] src, int srcOffset, int srcCount, NullsState nullsState) {
    writeLongsInternal(src, srcOffset, srcCount, nullsState, false);
  }

  private void writeLongsInternal(
      Object srcObj, int srcOffset, int srcCount, NullsState nullsState, boolean isLongSrc) {
    long[] srcL = isLongSrc ? (long[])srcObj : null;
    byte[] srcB = isLongSrc ? null : (byte[])srcObj;
    ByteBuffer buffer = colBuffer.buffer.getContents();
    currentSizeOf = ChunkUtils.TYPE_SIZES[Type.LONG.value()];
    ensureUniqueValueSegment(buffer, srcCount, nullsState);
    if (!currentSegmentHasNulls) {
      valuesOffset = isLongSrc
          ? writeLongs(buffer, valuesOffset, srcL, srcOffset, srcCount, currentSizeOf)
          : writeLongs(buffer, valuesOffset, srcB, srcOffset, srcCount, currentSizeOf);
      currentSegmentValues += srcCount;
    } else {
      if (valuesSinceBitmask == currentBitmaskLimit) {
        startNextBitmask(buffer);
      }
      // Write bitmasks followed by values, until we write all the values.
      while (srcCount > 0) {
        int valuesToWrite = Math.min(currentBitmaskLimit - valuesSinceBitmask, srcCount);
        assert valuesToWrite > 0 : valuesSinceBitmask + "/" + currentBitmaskLimit + " " + srcCount;
        writeZeroesIntoBytes(buffer, currentBitmaskOffset, valuesSinceBitmask, valuesToWrite);
        valuesOffset = isLongSrc
            ? writeLongs(buffer, valuesOffset, srcL, srcOffset, valuesToWrite, currentSizeOf)
            : writeLongs(buffer, valuesOffset, srcB, srcOffset, valuesToWrite, currentSizeOf);
        valuesSinceBitmask += valuesToWrite;
        currentSegmentValues += valuesToWrite;
        srcOffset += valuesToWrite;
        srcCount -= valuesToWrite;
        if (srcCount > 0) {
          assert valuesSinceBitmask == currentBitmaskLimit;
          startNextBitmask(buffer);
        }
      }
    }
  }

  @Override
  public void writeDoubles(double[] src, int srcOffset, int srcCount, NullsState nullsState) {
    ByteBuffer buffer = colBuffer.buffer.getContents();
    currentSizeOf = ChunkUtils.TYPE_SIZES[Type.DOUBLE.value()];
    ensureUniqueValueSegment(buffer, srcCount, nullsState);
    if (!currentSegmentHasNulls) {
      valuesOffset = writeDoubles(buffer, valuesOffset, src, srcOffset, srcCount, currentSizeOf);
      currentSegmentValues += srcCount;
    } else {
      if (valuesSinceBitmask == currentBitmaskLimit) {
        startNextBitmask(buffer);
      }
      // Write bitmasks followed by values, until we write all the values.
      while (srcCount > 0) {
        int valuesToWrite = Math.min(currentBitmaskLimit - valuesSinceBitmask, srcCount);
        assert valuesToWrite > 0;
        writeZeroesIntoBytes(buffer, currentBitmaskOffset, valuesSinceBitmask, valuesToWrite);
        valuesOffset = writeDoubles(
            buffer, valuesOffset, src, srcOffset, valuesToWrite, currentSizeOf);
        valuesSinceBitmask += valuesToWrite;
        currentSegmentValues += valuesToWrite;
        srcOffset += valuesToWrite;
        srcCount -= valuesToWrite;
        if (srcCount > 0) {
          assert valuesSinceBitmask == currentBitmaskLimit;
          startNextBitmask(buffer);
        }
      }
    }
  }

  @Override
  public void writeNulls(int count, boolean followedByNonNull) {
    if (!currentSegmentHasNulls || (!currentSegmentIsRepeating
        && valuesSinceBitmask == 0 && count > END_UNIQUE_SEGMENT_RUN_LEN)) {
      finishCurrentSegment();
    }
    if (currentSegmentStart != -1 && !currentSegmentIsRepeating) {
      assert currentSegmentHasNulls && valuesSinceBitmask > 0;
      assert currentSizeOf > 0;
      ByteBuffer buffer = colBuffer.buffer.getContents();
      // We are writing into an existing segment with bitmasks. Currently, we arbitrarily
      // choose to finish writing bitmask in such cases. Given that we write values despite
      // bitmask, this may be suboptimal, but otherwise we might write million tiny segments.
      int valuesToWrite = Math.min(currentBitmaskLimit - valuesSinceBitmask, count);
      if (valuesToWrite > 0) {
        writeOnesIntoBytes(buffer, currentBitmaskOffset, valuesSinceBitmask, valuesToWrite);
        valuesOffset += (currentSizeOf * valuesToWrite);
        valuesSinceBitmask += valuesToWrite;
        currentSegmentValues += valuesToWrite;
        count -= valuesToWrite;
      }
      if (count == 0) return;
      // Might not make sense if count remaining is low and is followed by non-nulls.
      finishCurrentSegment();
    }
    if (currentSegmentStart == -1) {
      // We have no segment. For small count, starting a bitmask might make sense, but for now
      // we always start repeated nulls segment, even if it doesn't make any sense.
      currentSizeOf = -1;
      startRepeatingSegment();
      currentSegmentHasNulls = true;
    }
    assert currentSegmentIsRepeating && currentSegmentHasNulls;
    currentSegmentValues += count;
  }

  /** Arbitrary; the tradeoff is between wasting space writing repeated values,
   *  and having many tiny segments that are more expensive to read. */
  private static final int END_UNIQUE_SEGMENT_RUN_LEN = 10;
  @Override
  public void writeRepeatedLongs(long value, int count, NullsState nullsState) {
    boolean isIncompatibleRepeating = currentSegmentIsRepeating
          && (currentSegmentHasNulls || currentRepeatingLongValue != value);
    boolean isAtEndOfBitmask = !currentSegmentIsRepeating
        && currentSegmentHasNulls && valuesSinceBitmask == 0;
    if (isIncompatibleRepeating || (isAtEndOfBitmask && count >= END_UNIQUE_SEGMENT_RUN_LEN)) {
      finishCurrentSegment();
    }
    if (currentSegmentStart != -1 && !currentSegmentIsRepeating) {
      assert currentSizeOf > 0;
      ByteBuffer buffer = colBuffer.buffer.getContents();
      if (currentSegmentHasNulls) {
        assert valuesSinceBitmask > 0;
        // See writeNulls - similar logic.
        int valuesToWrite = Math.min(currentBitmaskLimit - valuesSinceBitmask, count);
        if (valuesToWrite > 0) {
          writeZeroesIntoBytes(buffer, currentBitmaskOffset, valuesSinceBitmask, valuesToWrite);
          valuesOffset = writeLongs(buffer, valuesOffset, value, valuesToWrite, currentSizeOf);
          valuesSinceBitmask += valuesToWrite;
          currentSegmentValues += valuesToWrite;
          count -= valuesToWrite;
        }
        if (count > 0) {
          finishCurrentSegment();
        }
      } else if (count < END_UNIQUE_SEGMENT_RUN_LEN) {
        valuesOffset = writeLongs(buffer, valuesOffset, value, count, currentSizeOf);
        valuesSinceBitmask += count;
        currentSegmentValues += count;
        count = 0;
      } else {
        finishCurrentSegment();
      }
    }
    if (count == 0) return;
    if (currentSegmentStart == -1) {
      // We have no segment. For small count, starting a bitmask might make sense, but for now
      // we always start repeated segment, even if it doesn't make any sense.
      currentSizeOf = ChunkUtils.TYPE_SIZES[Type.LONG.value()];
      startRepeatingSegment();
      currentSegmentHasNulls = false;
      currentRepeatingLongValue = value;
      colBuffer.buffer.getContents().putLong(valuesOffset, value);
      valuesOffset += currentSizeOf;
    }
    assert currentSegmentIsRepeating && (currentRepeatingLongValue == value);
    currentSegmentValues += count;
  }

  @Override
  public void finishCurrentSegment() {
    if (currentSegmentStart == -1) return;
    ByteBuffer buffer = colBuffer.buffer.getContents();
    RleSegmentType segmentType = RleSegmentType.INVALID;
    if (currentSegmentIsRepeating || !currentSegmentHasNulls || valuesSinceBitmask == 0
        || valuesSinceBitmask == currentBitmaskLimit) {
      // Simple case - just write the type and count into current segment header.
      segmentType = currentSegmentIsRepeating ? (currentSegmentHasNulls
              ? RleSegmentType.REPEATING_NULL : RleSegmentType.REPEATING_VALUE)
          : (currentSegmentHasNulls
              ? RleSegmentType.UNIQUE_NULL_BITMASK : RleSegmentType.UNIQUE_NOT_NULL);
    } else {
      // Complicated case - bitmask is not finished, we may need to move values.
      segmentType = RleSegmentType.UNIQUE_NULL_BITMASK;
      int adjustedValues = ChunkUtils.align64(valuesSinceBitmask); // Rounded to 8 bytes.
      int bytesShift = (ChunkUtils.align64(currentBitmaskLimit) - adjustedValues) >>> 3;
      // Will never happen when bitmask is 8 bytes - minimum and maximum sizes are the same.
      if (bytesShift > 0) {
        if (DebugUtils.isTraceEnabled()) {
          Llap.LOG.info("Adjusting last bitmask by " + bytesShift + " bytes");
        }
        assert currentSizeOf > 0;
        int valuesSize = valuesSinceBitmask * currentSizeOf;
        int valuesStart = valuesOffset - valuesSize;
        assert buffer.hasArray();
        byte[] arr = buffer.array();
        System.arraycopy(arr, valuesStart, arr, valuesStart - bytesShift, valuesSize);
        valuesOffset -= bytesShift;
      }
    }
    if (DebugUtils.isTraceEnabled()) {
      Llap.LOG.info("Writing " + segmentType + " header w/ " + currentSegmentValues
          + " values at " + currentSegmentStart + " till " + valuesOffset);
    }
    writeSegmentHeader(buffer, currentSegmentStart, segmentType, currentSegmentValues);
    colBuffer.update(valuesOffset, currentSegmentValues);
    currentSegmentStart = -1;
  }

  private void startRepeatingSegment() {
    currentSegmentIsRepeating = true;
    currentSegmentStart = valuesOffset;
    currentSegmentValues = 0;
    currentBitmaskOffset = -1;
    valuesOffset += 8;
  }

  private void ensureUniqueValueSegment(ByteBuffer buffer, int valueCount, NullsState nullsState) {
    boolean forceNoNulls = false;
    if (currentSegmentStart != -1) {
      if (!currentSegmentIsRepeating) {
        if (!currentSegmentHasNulls
            || canValuesFitIntoCurrentSegment(buffer, valueCount, currentSizeOf)) {
          return; // We have an unique-value segment w/o bitmasks, or values fit w/bitmasks.
        }
        forceNoNulls = true;
      }
      finishCurrentSegment();
    }
    // There no unique-value segment (or we just finished one), start one.
    currentSegmentStart = valuesOffset;
    valuesOffset += 8;
    currentSegmentIsRepeating = false;
    valuesSinceBitmask = currentSegmentValues = 0;
    currentSegmentHasNulls = !forceNoNulls && shouldNewSegmentHaveBitmasks(
        valueCount, nullsState, buffer, valuesOffset, currentSizeOf);
    if (!currentSegmentHasNulls) {
      currentBitmaskOffset = -1;
    } else {
      startNextBitmask(buffer);
    }
  }

  private void startNextBitmask(ByteBuffer buffer) {
    currentBitmaskOffset = valuesOffset;
    int spaceLeft = buffer.limit() - currentBitmaskOffset;
    valuesSinceBitmask = 0;
    valuesOffset = currentBitmaskOffset;
    if (spaceLeft >= ChunkUtils.getFullBitmaskSize(currentSizeOf)) {
      // Most of the time, standard-sized bitmask will fit (we are not at the end of the buffer).
      currentBitmaskLimit = ChunkUtils.BITMASK_SIZE_BITS;
      valuesOffset += ChunkUtils.BITMASK_SIZE_BYTES;
      return;
    }
    // Only part of the bitmask will fit, so we will have a smaller one.
    int incrementSize = 8 + (currentSizeOf << 6); // 8 bytes, 64 values (minimum bitmask alignment)
    int incrementsThatFit = (spaceLeft / incrementSize);
    // Per each part, we will add 8 bytes to have space for bitmask, and space for 64 values.
    valuesOffset += (incrementsThatFit << 3);
    currentBitmaskLimit = incrementsThatFit >>> 6;
    // If there's more space, try to fit 8 more bytes of bitmask with less than 64 values.
    spaceLeft = (spaceLeft % incrementSize) - 8; // 8 bytes for that last bitmask
    if (spaceLeft >= currentSizeOf) {
      valuesOffset += 8;
      currentBitmaskLimit += (spaceLeft / currentSizeOf);
    }
    if (currentBitmaskLimit == 0) {
      throw new AssertionError("Bitmask won't fit; caller should have checked that");
    }
  }

  private static void writeZeroesIntoBytes(
      ByteBuffer buffer, int bitmaskOffset, int valuesSinceBitmask, int valuesToWrite) {
    // No need to write 0s into the tail of a partial byte - already set to 0s by
    // the previous call to writeZeroesIntoBytes or writeOnesIntoBytes.
    int bitsToSkip = 8 - (valuesSinceBitmask & 7);
    if (bitsToSkip < 8) {
      valuesToWrite -= bitsToSkip;
      valuesSinceBitmask += bitsToSkip;
      if (valuesToWrite <= 0) return;
    }
    int nextByteToModify = bitmaskOffset + (valuesSinceBitmask >>> 3);
    while (valuesToWrite > 0) {
      buffer.put(nextByteToModify, (byte)0);
      valuesToWrite -= 8;
      ++nextByteToModify;
    }
  }

  private static void writeOnesIntoBytes(
      ByteBuffer buffer, int bitmaskOffset, int valuesSinceBitmask, int valuesToWrite) {
    // We need to set the bits in the bitmask to one. We may have to do partial bits,
    // then whole bytes, then partial bits again for the last rows. I hate bits.
    int nextByteToModify = bitmaskOffset + (valuesSinceBitmask >>> 3);
    int bitsWritten = writeOneBitsFromPartialByte(
        buffer, nextByteToModify, valuesSinceBitmask, valuesToWrite);
    if (bitsWritten > 0) {
      valuesToWrite -= bitsWritten;
      ++nextByteToModify;
    }
    while (valuesToWrite > 8) {
      buffer.put(nextByteToModify, (byte)0xff);
      valuesToWrite -= 8;
      ++nextByteToModify;
    }
    if (valuesToWrite > 0) {
      int newBitsMask = ((1 << valuesToWrite) - 1) << (8 - valuesToWrite);
      buffer.put(nextByteToModify, (byte)newBitsMask);
    }
  }

  private static int writeOneBitsFromPartialByte(
      ByteBuffer buffer, int bufferOffset, int valuesInBitmask, int valuesToWrite) {
    int bitOffset = valuesInBitmask & 7;
    if (bitOffset == 0) return 0;
    assert bitOffset < 8;
    byte partialByte = buffer.get(bufferOffset);
    int unusedBits = 8 - bitOffset;
    int result = Math.min(unusedBits, valuesToWrite);
    // Make newBitCount ones, then shift them to create 0s on the right.
    int newBitsMask = ((1 << result) - 1) << (unusedBits - result);
    byte newByte = (byte)(partialByte | newBitsMask);
    buffer.put(bufferOffset, newByte);
    return result;
  }

  private static boolean shouldNewSegmentHaveBitmasks(
      int valueCount, NullsState nullsState, ByteBuffer buffer, int offset, int sizeOf) {
    // This is rather arbitrary. We'll write some values w/o bitmask if there are enough.
    // What is enough is an interesting question. We pay 8 bytes extra for segment header
    // if this is immediately followed by some nulls; so use this as a guideline. If we
    // don't know if this is followed by null or more values, use half?
    if (nullsState == NullsState.NO_NULLS || valueCount >= ChunkUtils.BITMASK_SIZE_BITS) {
      return false;
    }
    return canValuesFitWithBitmasks(buffer, offset, valueCount, sizeOf);
  }

  private boolean canValuesFitIntoCurrentSegment(ByteBuffer buffer, int valueCount, int sizeOf) {
    int valuesIntoCurrentBitmask = (currentBitmaskLimit - valuesSinceBitmask);
    valueCount -= valuesIntoCurrentBitmask;
    if (valueCount <= 0) return true;
    int nextBitmaskOffset = valuesOffset + valuesIntoCurrentBitmask * sizeOf;
    return canValuesFitWithBitmasks(buffer, nextBitmaskOffset, valueCount, sizeOf);
  }

  private static boolean canValuesFitWithBitmasks(
      ByteBuffer buffer, int offset, int valueCount, int elementSize) {
    return (determineSizeWithBitMask(valueCount, elementSize) < (buffer.limit() - offset));
  }

  private static int determineSizeWithBitMask(int count, int elementSize) {
    return count * elementSize + (ChunkUtils.align64(count) >>> 3);
  }

  private static void writeSegmentHeader(
      ByteBuffer buffer, int offset, RleSegmentType type, int rowCount) {
    buffer.put(offset++, type.getValue());
    buffer.putInt(offset, rowCount);
  }

  private static int writeLongs(
      ByteBuffer buffer, int offset, long[] cv, int cvOffset, int rowsToWrite, int sizeOf) {
    assert sizeOf > 0;
    for (int i = 0; i < rowsToWrite; ++i) {
      buffer.putLong(offset, cv[cvOffset + i]);
      offset += sizeOf;
    }
    return offset;
  }

  private static int writeDoubles(
      ByteBuffer buffer, int offset, double[] cv, int cvOffset, int rowsToWrite, int sizeOf) {
    assert sizeOf > 0;
    for (int i = 0; i < rowsToWrite; ++i) {
      buffer.putDouble(offset, cv[cvOffset + i]);
      offset += sizeOf;
    }
    return offset;
  }

  private static int writeLongs(
      ByteBuffer buffer, int offset, byte[] cv, int cvOffset, int rowsToWrite, int sizeOf) {
    assert sizeOf > 0;
    for (int i = 0; i < rowsToWrite; ++i) {
      buffer.putLong(offset, cv[cvOffset + i]);
      offset += sizeOf;
    }
    return offset;
  }

  private static int writeLongs(
      ByteBuffer buffer, int offset, long value, int rowsToWrite, int sizeOf) {
    for (int i = 0; i < rowsToWrite; ++i) {
      buffer.putLong(offset, value);
      offset += sizeOf;
    }
    return offset;
  }
}
