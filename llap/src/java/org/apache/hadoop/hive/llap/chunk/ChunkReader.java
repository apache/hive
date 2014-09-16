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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.common.type.Decimal128;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.api.Llap;
import org.apache.hadoop.hive.llap.api.Vector;
import org.apache.hadoop.hive.llap.api.Vector.Type;
import org.apache.hadoop.hive.llap.chunk.ChunkUtils;
import org.apache.hadoop.hive.llap.loader.ChunkPool.Chunk;
import org.apache.hadoop.hive.llap.chunk.ChunkUtils.RleSegmentType;

/**
 * Chunk reader; reads chained chunks (we might want to separate this later).
 *
 * Initial chunk format:
 * [version byte][int number of rows][padding to 8 bytes](segments)(if string, [dictionary])
 * Version is 0 for initial format. Segment format:
 * [type byte][int number of rows][padding to 8 bytes](values).
 * One value is stored for repeated segments, and none for null-repeated segments. For non-repeated
 * segments, values and bitmasks are interspersed; N-byte bitmask followed is by N*8 values,
 * repeated. Last bitmask may be smaller if there are less values, but it's still rounded to 8
 * bytes. Values for nulls are still stored (we could save space by not storing them, like ORC).
 *
 * Values are stored with fixed-length and are 8 bytes for long and double; 8 bytes for strings
 * (dictionary offset+length); TODO bytes for decimals. Values stored for NULLs are undefined.
 */
public class ChunkReader implements Vector.ColumnReader {
  private final Type type;
  private Chunk chunk;
  private List<ByteBuffer> chunkBuffers;
  private List<RleSegmentType> segmentTypes;
  private List<Integer> segmentOffsets, segmentRowCounts;
  private int rowOffsetInFirstSegment;

  private int remainingRowsInLastChunk = 0, rowOffsetInNextSegment = 0, nextSegmentOffset = 0;

  private int lastRowCountNeeded = 0;

  public ChunkReader(Type type, Chunk chunk) {
    this.type = type;
    this.chunk = chunk;
    // Verify chunk version.
    byte firstByte = chunk.buffer.getContents().get(chunk.offset);
    if (firstByte != ChunkUtils.FORMAT_VERSION) {
      throw new UnsupportedOperationException("Chunk doesn't start as expected: " + firstByte);
    }
    if (Integer.bitCount(ChunkUtils.BITMASK_SIZE_BYTES) != 1
        || (ChunkUtils.BITMASK_SIZE_BYTES < 8)) {
      throw new AssertionError("Must be a power of two >= 8: "+ ChunkUtils.BITMASK_SIZE_BYTES);
    }
  }

  public int getNumRowsRemaining() {
    if (this.chunk == null) return 0;
    int result = remainingRowsInLastChunk;
    Chunk chunk = this.chunk;
    if (remainingRowsInLastChunk > 0) {
      chunk = chunk.nextChunk; // remainingRowsInLastChunk accounts for current one
    }
    while (chunk != null) {
      ByteBuffer bb = chunk.buffer.getContents();
      result += bb.getInt(chunk.offset + 1);
      chunk = chunk.nextChunk;
    }
    return result;
  }

  public void next(int rowCountNeeded) {
    lastRowCountNeeded = rowCountNeeded;
    if (rowCountNeeded == 0) return;
    ByteBuffer chunkBuffer = null;
    if (chunkBuffers == null) {
      init();
    } else {
      if (remainingRowsInLastChunk > 0) {
        chunkBuffer = chunkBuffers.get(chunkBuffers.size() - 1);
      }
      reset();
    }

    while (chunk != null && rowCountNeeded > 0) {
      if (chunkBuffer == null) {
        chunkBuffer = createChunkBuffer(chunk);
        remainingRowsInLastChunk = chunkBuffer.getInt(1); // skip header byte
        nextSegmentOffset = 8;
      }
      while (rowCountNeeded > 0 && remainingRowsInLastChunk > 0) {
        int segmentOffset = nextSegmentOffset;
        assert (segmentOffset & 7) == 0; // must be 8-byte aligned
        RleSegmentType segmentType = RleSegmentType.fromInt(chunkBuffer.get(nextSegmentOffset));
        ++nextSegmentOffset;
        int segmentRowCount = chunkBuffer.getInt(nextSegmentOffset);
        int segmentRowOffset = rowOffsetInNextSegment; // Only non-zero for the first segment.
        nextSegmentOffset += 7;
        if (DebugUtils.isTraceEnabled()) {
          Llap.LOG.info("Segment type " + segmentType + " with " + segmentRowCount
              + " rows (skipping " + segmentRowOffset + "); at " + nextSegmentOffset + "; "
              + remainingRowsInLastChunk + " more in this chunk including this segment");
        }
        int dataLength = ChunkUtils.getSegmentDataSize(type, segmentType, segmentRowCount);
        int segmentRowCountAvail = segmentRowCount - segmentRowOffset;
        if (segmentRowCountAvail > rowCountNeeded) {
          // We have some rows remaining in the same segment.
          nextSegmentOffset = segmentOffset;
          rowOffsetInNextSegment = segmentRowOffset + rowCountNeeded;
          segmentRowCountAvail = rowCountNeeded;
        } else {
          nextSegmentOffset += dataLength;
          rowOffsetInNextSegment = 0;
        }
        remainingRowsInLastChunk -= segmentRowCountAvail;
        rowCountNeeded -= segmentRowCountAvail;

        // Finally, add segment to data.
        if (DebugUtils.isTraceEnabled()) {
          Llap.LOG.info("Adding segment with " + segmentRowCountAvail + " rows in "
              + chunk.buffer + "; segment row offset " + segmentRowOffset);
        }
        chunkBuffers.add(chunkBuffer);
        segmentTypes.add(segmentType);
        segmentOffsets.add(segmentOffset);
        segmentRowCounts.add(segmentRowCount);
        if (rowOffsetInFirstSegment == -1) {
          rowOffsetInFirstSegment = segmentRowOffset;
        }
      }
      if (remainingRowsInLastChunk == 0) {
        // We are done with current chunk.
        chunk = chunk.nextChunk;
        nextSegmentOffset = 0;
      } else {
        assert rowCountNeeded == 0;
      }
      chunkBuffer = null;
    }
    assert !chunkBuffers.isEmpty() : "No rows found, expected " + rowCountNeeded;
  }

  private void init() {
    chunkBuffers = new ArrayList<ByteBuffer>();
    segmentTypes = new ArrayList<RleSegmentType>();
    segmentOffsets = new ArrayList<Integer>();
    segmentRowCounts = new ArrayList<Integer>();
    remainingRowsInLastChunk = 0;
    rowOffsetInFirstSegment = -1;
  }

  private void reset() {
    chunkBuffers.clear();
    segmentRowCounts.clear();
    segmentTypes.clear();
    segmentOffsets.clear();
    rowOffsetInFirstSegment = -1;
  }

  private ByteBuffer createChunkBuffer(Chunk chunk) {
    ByteBuffer bb = chunk.buffer.getContents();
    if (DebugUtils.isTraceEnabled()) {
      Llap.LOG.info("Chunk in " + chunk.buffer
          + " at " + chunk.offset + ", length " + chunk.length);
    }
    bb.position(chunk.offset);
    ByteBuffer chunkBuffer = bb.slice();
    chunkBuffer.limit(chunk.length);
    return chunkBuffer;
  }

  @Override
  public boolean isSameValue() {
    // Assume repeated values wouldn't be written as separate segments
    if (segmentTypes.size() > 1) return false;
    RleSegmentType type = segmentTypes.get(0);
    return type == RleSegmentType.REPEATING_NULL || type == RleSegmentType.REPEATING_VALUE;
  }

  @Override
  public boolean hasNulls() {
    for (RleSegmentType type : segmentTypes) {
      if (type == RleSegmentType.REPEATING_NULL || type == RleSegmentType.UNIQUE_NULL_BITMASK) {
        return true;
      }
    }
    return false;
  }

  // TODO: Ideally we want to get rid of this and process segments as we go (nextLongs ...);
  //       however, this may preclude us from predicting whether there are nulls, etc.
  //       Plus code will be even more complex. But might be worth it.
  @Override
  public void copyLongs(long[] dest, boolean[] isNull, int destOffset) throws IOException {
    int segmentRowOffset = rowOffsetInFirstSegment;
    for (int segmentIx = 0; segmentIx < segmentTypes.size(); ++segmentIx) {
      int segmentRowCount = segmentRowCounts.get(segmentIx);
      RleSegmentType segmentType = segmentTypes.get(segmentIx);
      ByteBuffer chunkBuffer = chunkBuffers.get(segmentIx);
      int rowCountToRead = Math.min(segmentRowCount - segmentRowOffset, lastRowCountNeeded);
      lastRowCountNeeded -= rowCountToRead;
      if (DebugUtils.isTraceEnabled()) {
        Llap.LOG.info("Copying " + rowCountToRead + " rows from segment " + segmentIx
          + " of type " + segmentType + " segment at " + segmentOffsets.get(segmentIx)
          + " using row offset " + segmentRowOffset + " to result offset " + destOffset);
      }
      switch (segmentType) {
      case REPEATING_NULL: {
        assert isNull != null;
        Arrays.fill(isNull, destOffset, destOffset + rowCountToRead, true);
        destOffset += rowCountToRead;
        break;
      }
      case REPEATING_VALUE: {
        long value = chunkBuffer.getLong(segmentOffsets.get(segmentIx) + 8);
        Arrays.fill(dest, destOffset, destOffset + rowCountToRead, value);
        if (isNull != null) {
          Arrays.fill(isNull, destOffset, destOffset + rowCountToRead, false);
        }
        destOffset += rowCountToRead;
        break;
      }
      case UNIQUE_NOT_NULL: {
        int dataOffset = segmentOffsets.get(segmentIx) + 8;
        assert (dataOffset & 7) == 0; // Must be 8-byte aligned.
        copyLongValues(chunkBuffer, dataOffset, segmentRowOffset,
            dest, destOffset, rowCountToRead);
        if (isNull != null) {
          Arrays.fill(isNull, destOffset, destOffset + rowCountToRead, false);
        }
        destOffset += rowCountToRead;
        break;
      }
      case UNIQUE_NULL_BITMASK: {
        longCopier.initDest(dest);
        destOffset = copyValuesWithNulls(chunkBuffer, segmentOffsets.get(segmentIx),
            segmentRowOffset, segmentRowCount, longCopier, isNull, destOffset, rowCountToRead);
        break;
      }
      default: throw new IOException("Unsupported segment type " + segmentType);
      }
      segmentRowOffset = 0;
    }
  }

  private void copyLongValues(ByteBuffer chunkBuffer, int dataOffset,
      int segmentRowOffset, long[] dest, int destOffset, int rowCountToRead) {
    LongBuffer longBuffer = chunkBuffer.asLongBuffer();
    longBuffer.position((dataOffset >>> 3) + segmentRowOffset);
    longBuffer.get(dest, destOffset, rowCountToRead);
    if (DebugUtils.isTraceDataEnabled()) {
      Llap.LOG.info("Copied " + rowCountToRead + " rows from long offset "
        + ((dataOffset >>> 3) + segmentRowOffset) + " (" + dataOffset + ", "
          + segmentRowOffset + "): " + DebugUtils.toString(dest, destOffset, rowCountToRead));
      Llap.LOG.debug("VRB vector now looks like " + Arrays.toString(dest));
    }
  }

  @Override
  public void copyDoubles(double[] dest, boolean[] isNull, int destOffset) throws IOException {
    int segmentRowOffset = rowOffsetInFirstSegment;
    for (int segmentIx = 0; segmentIx < segmentTypes.size(); ++segmentIx) {
      int segmentRowCount = segmentRowCounts.get(segmentIx);
      RleSegmentType segmentType = segmentTypes.get(segmentIx);
      ByteBuffer chunkBuffer = chunkBuffers.get(segmentIx);
      int rowCountToRead = Math.min(segmentRowCount - segmentRowOffset, lastRowCountNeeded);
      lastRowCountNeeded -= rowCountToRead;
      switch (segmentType) {
      case REPEATING_NULL: {
        assert isNull != null;
        Arrays.fill(isNull, destOffset, destOffset + rowCountToRead, true);
        destOffset += rowCountToRead;
        break;
      }
      case REPEATING_VALUE: {
        long value = chunkBuffer.getLong(segmentOffsets.get(segmentIx) + 8);
        Arrays.fill(dest, destOffset, destOffset + rowCountToRead, value);
        if (isNull != null) {
          Arrays.fill(isNull, destOffset, destOffset + rowCountToRead, false);
        }
        destOffset += rowCountToRead;
        break;
      }
      case UNIQUE_NOT_NULL: {
        int dataOffset = segmentOffsets.get(segmentIx) + 8;
        assert (dataOffset & 7) == 0; // Must be 8-byte aligned.
        copyDoubleValues(chunkBuffer, dataOffset, segmentRowOffset,
            dest, destOffset, rowCountToRead);
        if (isNull != null) {
          Arrays.fill(isNull, destOffset, destOffset + rowCountToRead, false);
        }
        destOffset += rowCountToRead;
        break;
      }
      case UNIQUE_NULL_BITMASK: {
        doubleCopier.initDest(dest);
        destOffset = copyValuesWithNulls(chunkBuffer, segmentOffsets.get(segmentIx),
            segmentRowOffset, segmentRowCount, doubleCopier, isNull, destOffset, rowCountToRead);
        break;
      }
      default: throw new IOException("Unsupported segment type " + segmentType);
      }
      segmentRowOffset = 0;
    }
  }

  private void copyDoubleValues(ByteBuffer chunkBuffer, int dataOffset,
      int segmentRowOffset, double[] dest, int destOffset, int rowCountToRead) {
    DoubleBuffer doubleBuffer = chunkBuffer.asDoubleBuffer();
    doubleBuffer.position((dataOffset >>> 3) + segmentRowOffset);
    doubleBuffer.get(dest, destOffset, rowCountToRead);
  }

  private int copyValuesWithNulls(ByteBuffer chunkBuffer, int segmentDataOffset,
      int segmentRowOffset, int segmentRowCount, ValueCopier valueHelper, boolean[] isNull,
      int destOffset, int rowCountToRead) {
    if (rowCountToRead == 0) return destOffset;
    int valueSize = ChunkUtils.TYPE_SIZES[type.value()];
    // Prepare to read (or skip) the first bitmask.
    int bitmasksSkipped = 0;
    int currentBitmaskOffset = segmentDataOffset + 8;
    int currentBitmaskSize = determineBitmaskSizeBytes(bitmasksSkipped, segmentRowCount);
    // Size of bitmask and corresponding values in bytes, for a BITMASK_SIZE_BYTES-sized bitmask.
    int sizeOfBitmaskAndValues = ChunkUtils.getFullBitmaskSize(valueSize);
    valueHelper.initSrc(chunkBuffer);

    // For the first segment, we might have to skip some rows. This is sadly most of this method.
    if (segmentRowOffset > 0) {
      // First, see how many full bitmasks we need to skip.
      int bitmasksToSkip = (segmentRowOffset / ChunkUtils.BITMASK_SIZE_BITS);
      assert bitmasksToSkip == 0 || currentBitmaskSize == ChunkUtils.BITMASK_SIZE_BYTES;
      bitmasksSkipped += bitmasksToSkip;
      currentBitmaskOffset += (bitmasksToSkip * sizeOfBitmaskAndValues);
      currentBitmaskSize = determineBitmaskSizeBytes(bitmasksSkipped, segmentRowCount);
      segmentRowOffset = segmentRowOffset % ChunkUtils.BITMASK_SIZE_BITS;

      // Remember how many values we are skipping in the current bitmask, for value copying.
      int valuesToSkip = segmentRowOffset;
      // Then, in the bitmask we are in, skip however many full bytes we need to skip.
      int currentOffsetInBitmask = 0;
      if (segmentRowOffset >= 8) {
        int bytesToSkip = segmentRowOffset >>> 3;
        currentOffsetInBitmask = bytesToSkip;
        segmentRowOffset = segmentRowOffset & 7;
      }

      if (DebugUtils.isTraceEnabled()) {
        Llap.LOG.info("Skipping " + bitmasksToSkip + " bitmasks and "
            + currentOffsetInBitmask + " bytes; for a bitmask at " + currentBitmaskOffset
            + " will skip " + valuesToSkip + " values and " + segmentRowOffset + " bits");
      }
      // Finally, we may need to skip some bits in the first byte we are reading.
      // Read the partial byte of the bitmask (and corresponding long values).
      if (segmentRowOffset > 0) {
        int partialByteRowCount = Math.min(rowCountToRead, 8 - segmentRowOffset);
        copyBitsFromByte(chunkBuffer.get(currentBitmaskOffset + currentOffsetInBitmask),
            isNull, destOffset, segmentRowOffset, partialByteRowCount);
        valueHelper.copyValues(destOffset,
            currentBitmaskOffset + currentBitmaskSize, valuesToSkip, partialByteRowCount);
        if (DebugUtils.isTraceDataEnabled()) {
          Llap.LOG.info("After partial first byte w/" + partialByteRowCount
              + ", byte was " + chunkBuffer.get(currentBitmaskOffset));
          Llap.LOG.debug("After partial first byte w/" + partialByteRowCount
              + ", booleans are " + DebugUtils.toString(isNull));
        }

        rowCountToRead -= partialByteRowCount;
        destOffset += partialByteRowCount;
        ++currentOffsetInBitmask;
        if (currentOffsetInBitmask == ChunkUtils.BITMASK_SIZE_BYTES && rowCountToRead > 0) {
          // We only needed part of the last byte from this bitmask, go to the next one.
          ++bitmasksSkipped;
          currentBitmaskOffset += sizeOfBitmaskAndValues;
          currentBitmaskSize = determineBitmaskSizeBytes(bitmasksSkipped, segmentRowCount);
          currentOffsetInBitmask = 0;
        }
      }

      if (rowCountToRead == 0) return destOffset;

      // Then, if we have a partial bitmask, get to the boundary.
      if (currentOffsetInBitmask > 0) {
        // First, copy the bits, then the values at the same destOffset.
        for (int i = currentOffsetInBitmask, tmpToRead = rowCountToRead, tmpOffset = destOffset;
            (i < currentBitmaskSize) && (tmpToRead > 0); ++i) {
          int bitsToRead = Math.min(tmpToRead, 8);
          copyBitsFromByte(chunkBuffer.get(currentBitmaskOffset + i),
              isNull, tmpOffset, 0, bitsToRead);
          if (DebugUtils.isTraceDataEnabled()) {
            Llap.LOG.info("After copying " + bitsToRead +" bits from byte at "
                + (currentBitmaskOffset + i) + " to " + tmpOffset + ", booleans are "
                + DebugUtils.toString(isNull));
          }
          tmpOffset += bitsToRead;
          tmpToRead -= bitsToRead;
        }
        valuesToSkip = currentOffsetInBitmask << 3;
        int valuesToRead = Math.min((currentBitmaskSize << 3) - valuesToSkip, rowCountToRead);
        valueHelper.copyValues(destOffset, currentBitmaskOffset + currentBitmaskSize,
            valuesToSkip, valuesToRead);
        destOffset += valuesToRead;
        rowCountToRead -= valuesToRead;
        if (rowCountToRead == 0) return destOffset;
        // Go to next bitmask.
        currentBitmaskOffset += sizeOfBitmaskAndValues;
        ++bitmasksSkipped;
        currentBitmaskSize = determineBitmaskSizeBytes(bitmasksSkipped, segmentRowCount);
      }
    } // end of the epic "segmentRowOffset > 0" if

    // This is the main code path
    if (DebugUtils.isTraceEnabled()) {
      Llap.LOG.info("After segment offset, reading " + rowCountToRead + " rows from data at "
          + currentBitmaskOffset + " with " + segmentRowCount + " rows to offset " + destOffset
          + "; bitmask size " + currentBitmaskSize);
    }

    // Now we are finally done with all the crooked offsets (if any) so we can just read the data.
    while (true) {
      // Read one bitmask and corresponding values.
      for (int i = 0, tmpCountToRead = rowCountToRead, tmpOffset = destOffset;
          i < currentBitmaskSize && tmpCountToRead > 0; ++i) {
        byte b = chunkBuffer.get(currentBitmaskOffset + i);
        int bitsToRead = Math.min(tmpCountToRead, 8);
        copyBitsFromByte(b & 0xff, isNull, tmpOffset, 0, bitsToRead);
        if (DebugUtils.isTraceDataEnabled()) {
          Llap.LOG.info("Copied " + bitsToRead + " bits from " + b + " ("
              + Integer.toBinaryString(b & 0xff) + ") at " + (currentBitmaskOffset + i)
              + " to " + tmpOffset + "; current state is " + DebugUtils.toString(isNull));
        }
        tmpOffset += bitsToRead;
        tmpCountToRead -= bitsToRead;
      }
      int valuesToRead = Math.min(currentBitmaskSize << 3, rowCountToRead);
      int valuesOffset = currentBitmaskOffset + currentBitmaskSize;
      valueHelper.copyValues(destOffset, valuesOffset, 0, valuesToRead);
      destOffset += valuesToRead;
      rowCountToRead -= valuesToRead;
      if (rowCountToRead == 0) break;

      ++bitmasksSkipped;
      currentBitmaskOffset += sizeOfBitmaskAndValues;
      currentBitmaskSize = determineBitmaskSizeBytes(bitmasksSkipped, segmentRowCount);
    }
    return destOffset;
  }

  /** Helper interface to share the parts that deal with bitmasks, esp.
   * the insane offset logic, between method copying various datatypes. */
  private interface ValueCopier {
    void initSrc(ByteBuffer chunkBuffer);

    void copyValues(int destOffset,
        int valuesOffsetBytes, int valuesToSkip, int valuesToCopy);
  }

  private static class LongCopier implements ValueCopier {
    LongBuffer dataBuffer = null;
    long[] dest;
    public void initDest(long[] dest) {
      this.dest = dest;
    }
    public void initSrc(ByteBuffer chunkBuffer) {
      dataBuffer = chunkBuffer.asLongBuffer();
    }

    public void copyValues(int destOffset,
        int valuesOffsetBytes, int valuesToSkip, int valuesToCopy) {
      dataBuffer.position((valuesOffsetBytes >>> 3) + valuesToSkip);
      dataBuffer.get(dest, destOffset, valuesToCopy);
      if (DebugUtils.isTraceDataEnabled()) {
        Llap.LOG.info("After copying " + valuesToCopy + " from " + valuesOffsetBytes +
            " (skip " + valuesToSkip + ", long offset "+ ((valuesOffsetBytes >>> 3) + valuesToSkip)
             + ") to " + destOffset + ", values are " + DebugUtils.toString(dest, destOffset,
                 valuesToCopy) + " and dest is " + DebugUtils.toString(dest, 0, dest.length));
      }
    }
  }

  private static class DoubleCopier implements ValueCopier {
    DoubleBuffer dataBuffer = null;
    double[] dest;
    public void initDest(double[] dest) {
      this.dest = dest;
    }
    public void initSrc(ByteBuffer chunkBuffer) {
      dataBuffer = chunkBuffer.asDoubleBuffer();
    }

    public void copyValues(int destOffset,
        int valuesOffsetBytes, int valuesToSkip, int valuesToCopy) {
      valuesOffsetBytes += (valuesToSkip << 3);
      dataBuffer.position(valuesOffsetBytes >>> 3);
      dataBuffer.get(dest, destOffset, valuesToCopy);
    }
  }
  private LongCopier longCopier = new LongCopier();
  private DoubleCopier doubleCopier = new DoubleCopier();

  private int determineBitmaskSizeBytes(int skipped, int segmentRowCount) {
    int adjustedRowCount = segmentRowCount - ChunkUtils.BITMASK_SIZE_BITS * skipped;
    if (adjustedRowCount >= ChunkUtils.BITMASK_SIZE_BITS) return ChunkUtils.BITMASK_SIZE_BYTES;
    return ChunkUtils.align8((adjustedRowCount >>> 3) + (((adjustedRowCount & 7) != 0) ? 1 : 0));
  }

  @Override
  public long getLong() {
    return chunkBuffers.get(0).getLong(segmentOffsets.get(0) + 8);
  }

  @Override
  public double getDouble() {
    return chunkBuffers.get(0).getDouble(segmentOffsets.get(0) + 8);
  }

  private void copyBitsFromByte(int b, boolean[] dest, int offset, int skipBits, int bitCount) {
    // TODO: we could unroll the loop for full-byte copy.
    int shift = 7 - skipBits;
    for (int i = 0; i < bitCount; ++i, --shift) {
      dest[offset++] = (b & (1 << shift)) != 0;
    }
  }

  // TODO: add support for Decimal and Binary

  @Override
  public Decimal128 getDecimal() {
    throw new UnsupportedOperationException("Decimal not currently supported");
  }

  @Override
  public void copyDecimals(Decimal128[] dest, boolean[] isNull, int offset) {
    throw new UnsupportedOperationException("Decimal not currently supported");
  }

  @Override
  public byte[] getBytes() {
    throw new UnsupportedOperationException("Binary not currently supported");
  }

  @Override
  public void copyBytes(byte[][] dest, int[] destStarts, int[] destLengths,
      boolean[] isNull, int offset) {
    throw new UnsupportedOperationException("Binary not currently supported");
  }
}
