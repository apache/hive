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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.serde2.WriteBuffers;
import org.apache.hadoop.hive.serde2.WriteBuffers.ByteSegmentRef;
import org.apache.hadoop.hive.serde2.WriteBuffers.Position;;


// Supports random access.

public class VectorMapJoinFastValueStore {

  private static final Log LOG = LogFactory.getLog(VectorMapJoinFastValueStore.class.getName());

  private WriteBuffers writeBuffers;


  /**
   * A store for "lists" of arbitrary length values in memory.
   *
   * The memory is a "infinite" byte array or WriteBuffers object.
   *
   * We give the client a 64-bit (long) value reference to keep that has the offset within
   * the "infinite" byte array of the last value inserted in a "list".
   *
   * We optimize the common case when "list"s are 1 element and values are short and store the
   * value length in the value reference word.
   *
   * We also support keeping a count (up to a limit or cap) so help with join result generation
   * algorithms.
   *
   * If the last value is big, the big length will be encoded as an integer at the beginning
   * of the value followed by the big value bytes.
   *
   * Due to optimizing by keeping the last value's length in the value reference, when we have
   * more than one value, a new value will need to keep the small value length of the next
   * value.
   *
   * So, values after the first value have 4 parts: a relative offset word with flags, an
   * optional length if the current value is big, an optional next value length if it is small,
   * and the value bytes.
   *
   * Cases:
   *  1) One element, small:
   *
   *    Value Reference -------------
   *                                 |
   *                                 |
   *                                 v
   *                                 {Small Value Bytes}
   *
   *  2) One element, big:
   *
   *   Value Reference --------------
   *                                 |
   *                                 |
   *                                 v
   *                                 {Big Value Len} {Big Value Bytes}
   *
   *  1) Multiple elements:
   *
   *    Value Reference ----------------
   *                                    |
   *                                    |    //  Last value added.
   *                                    |
   *                                    v
   *                                    {Rel Offset Word} [Big Value Len] [Next Value Small Len] {Value Bytes}
   *                                             |            optional        optional
   *                                             |
   *                                             |
   *                                --- . . . ---
   *                               |
   *                               |      // 0 or more
   *                               |
   *                               v
   *                              {Rel Offset Word} [Big Value Len] [Next Value Small Len] {Value Bytes}
   *                                         |          optional        optional
   *                                         |
   *                                         |
   *                     --------------------
   *                    |
   *                    |
   *                    v
   *                   [Big Value Length] {Value Bytes}
   *                       optional
   *
   *                   // First value added without Relative Offset Word, etc.
   */

  public WriteBuffers writeBuffers() {
    return writeBuffers;
  }

  public static class HashMapResult extends VectorMapJoinHashMapResult {

    private VectorMapJoinFastValueStore valueStore;

    private boolean hasRows;
    private long valueRefWord;
    private boolean isSingleRow;
    private int cappedCount;

    private boolean haveReadCurrent;
    private int readIndex;
    private boolean isEof;

    private boolean isNextEof;
    private boolean isNextLast;
    long nextAbsoluteValueOffset;
    boolean isNextValueLengthSmall;
    int nextSmallValueLength;

    private ByteSegmentRef byteSegmentRef;
    private Position readPos;

    public HashMapResult() {
      super();
      valueRefWord = -1;
      hasRows = false;
      byteSegmentRef = new ByteSegmentRef();
      readPos = new Position();
    }

    public void set(VectorMapJoinFastValueStore valueStore, long valueRefWord) {
      // LOG.debug("VectorMapJoinFastValueStore set valueRefWord " + Long.toHexString(valueRefWord));

      this.valueStore = valueStore;
      this.valueRefWord = valueRefWord;

      hasRows = true;
      isSingleRow = ((valueRefWord & IsLastFlag.flagOnMask) != 0);
      cappedCount =
          (int) ((valueRefWord & CappedCount.bitMask) >> CappedCount.bitShift);
      // Position to beginning.
      haveReadCurrent = false;
      readIndex = 0;
      isEof = false;
    }

    @Override
    public boolean hasRows() {
      return hasRows;
    }

    @Override
    public boolean isSingleRow() {
      if (!hasRows) {
        return false;
      }

      return isSingleRow;
    }

    @Override
    public boolean isCappedCountAvailable() {
      return true;
    }

    @Override
    public int cappedCount() {
      if (!hasRows) {
        return 0;
      }

      return cappedCount;
    }

    @Override
    public ByteSegmentRef first() {
      if (!hasRows) {
        return null;
      }

      // Position to beginning.
      haveReadCurrent = false;
      readIndex = 0;
      isEof = false;

      return internalRead();
    }

    @Override
    public ByteSegmentRef next() {
      if (!hasRows) {
        return null;
      }

      return internalRead();
    }


    public ByteSegmentRef internalRead() {

      long absoluteValueOffset;

      int valueLength;

      if (readIndex == 0) {
        /*
         * Extract information from reference word from slot table.
         */
        absoluteValueOffset =
            (valueRefWord & AbsoluteValueOffset.bitMask);

        // Position before the last written value.
        valueStore.writeBuffers.setReadPoint(absoluteValueOffset, readPos);

        if (isSingleRow) {
          isNextEof = true;

          valueLength =
              (int) ((valueRefWord & SmallValueLength.bitMask) >> SmallValueLength.bitShift);
          boolean isValueLengthSmall = (valueLength != SmallValueLength.allBitsOn);
          if (!isValueLengthSmall) {
            // And, if current value is big we must read it.
            valueLength = valueStore.writeBuffers.readVInt(readPos);
          }
        } else {
          isNextEof = false;

          // 2nd and beyond records have a relative offset word at the beginning.
          long relativeOffsetWord = valueStore.writeBuffers.readVLong(readPos);

          long relativeOffset =
              (relativeOffsetWord & NextRelativeValueOffset.bitMask) >> NextRelativeValueOffset.bitShift;

          nextAbsoluteValueOffset = absoluteValueOffset - relativeOffset;

          isNextLast = ((relativeOffsetWord & IsNextValueLastFlag.flagOnMask) != 0);
          isNextValueLengthSmall =
              ((relativeOffsetWord & IsNextValueLengthSmallFlag.flagOnMask) != 0);
        }

        valueLength =
            (int) ((valueRefWord & SmallValueLength.bitMask) >> SmallValueLength.bitShift);
        boolean isValueLengthSmall = (valueLength != SmallValueLength.allBitsOn);
        if (!isValueLengthSmall) {
          // And, if current value is big we must read it.
          valueLength = valueStore.writeBuffers.readVInt(readPos);
        }

        // 2nd and beyond have the next value's small length in the current record.
        if (isNextValueLengthSmall) {
          nextSmallValueLength = valueStore.writeBuffers.readVInt(readPos);
        } else {
          nextSmallValueLength = -1;
        }

      } else {
        if (isNextEof) {
          return null;
        }

        absoluteValueOffset =  nextAbsoluteValueOffset;

        // Position before the last written value.
        valueStore.writeBuffers.setReadPoint(absoluteValueOffset, readPos);

        if (isNextLast) {
          isNextEof = true;

          if (isNextValueLengthSmall) {
            valueLength = nextSmallValueLength;
          } else {
            valueLength = (int) valueStore.writeBuffers.readVLong(readPos);
          }
        } else {
          isNextEof = false;

          // 2nd and beyond records have a relative offset word at the beginning.
          long relativeOffsetWord = valueStore.writeBuffers.readVLong(readPos);

          // Read current value's big length now, if necessary.
          if (isNextValueLengthSmall) {
            valueLength = nextSmallValueLength;
          } else {
            valueLength = (int) valueStore.writeBuffers.readVLong(readPos);
          }

          long relativeOffset =
              (relativeOffsetWord & NextRelativeValueOffset.bitMask) >> NextRelativeValueOffset.bitShift;

          nextAbsoluteValueOffset = absoluteValueOffset - relativeOffset;

          isNextLast = ((relativeOffsetWord & IsNextValueLastFlag.flagOnMask) != 0);
          isNextValueLengthSmall =
              ((relativeOffsetWord & IsNextValueLengthSmallFlag.flagOnMask) != 0);
          if (isNextValueLengthSmall) {
            // TODO: Write readVInt
            nextSmallValueLength = (int) valueStore.writeBuffers.readVLong(readPos);
          } else {
            nextSmallValueLength = -1;
          }
        }
      }

      // Our reading is positioned to the value.
      valueStore.writeBuffers.getByteSegmentRefToCurrent(byteSegmentRef, valueLength, readPos);

      readIndex++;
      return byteSegmentRef;
    }

    @Override
    public boolean isEof() {
      if (!hasRows) {
        return true;
      }
      return isEof;
    }

    @Override
    public void forget() {
    }


    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("(" + super.toString() + ", ");
      sb.append("cappedCount " + cappedCount() + ")");
      return sb.toString();
    }
  }

  /**
   * Bit-length fields within a 64-bit (long) value reference.
   *
   * Lowest field: An absolute byte offset the value in the WriteBuffers.
   *
   * 2nd field: For short values, the length of the value.  Otherwise, a special constant
   * indicating a big value whose length is stored with the value.
   *
   * 3rd field: A value count, up to a limit (a cap).  Have a count helps the join result
   * algorithms determine which optimization to use for M x N result cross products.
   * A special constant indicates if the value count is >= the cap.
   *
   * Last field: an bit indicating whether there is only one value.
   */

  // Lowest field.
  private final class AbsoluteValueOffset {
    private static final int bitLength = 40;
    private static final long allBitsOn = (1L << bitLength) - 1;
    private static final long bitMask = allBitsOn;

    // Make it a power of 2.
    private static final long maxSize = 1L << (bitLength - 2);
  }

  private final class SmallValueLength {
    private static final int bitLength = 10;
    private static final int allBitsOn = (1 << bitLength) - 1;
    private static final int threshold = allBitsOn;  // Lower this for big value testing.
    private static final int bitShift = AbsoluteValueOffset.bitLength;
    private static final long bitMask = ((long) allBitsOn) << bitShift;
    private static final long allBitsOnBitShifted = ((long) allBitsOn) << bitShift;
  }

  private final class CappedCount {
    private static final int bitLength = 10;
    private static final int allBitsOn = (1 << bitLength) - 1;
    private static final int limit = allBitsOn;
    private static final int bitShift =  SmallValueLength.bitShift + SmallValueLength.bitLength;
    private static final long bitMask = ((long) allBitsOn) << bitShift;
  }

  private final class IsLastFlag {
    private static final int bitShift = CappedCount.bitShift + CappedCount.bitLength;;
    private static final long flagOnMask = 1L << bitShift;
  }

  // This bit should not be on for valid value references.  We use -1 for a no value marker.
  private final class IsInvalidFlag {
    private static final int bitShift = 63;
    private static final long flagOnMask = 1L << bitShift;
  }

  /**
   * Relative Offset Word stored at the beginning of all but the last value that has a
   * relative offset and 2 flags.
   *
   * We put the flags at the low end of the word so the variable length integer will
   * encode smaller.
   *
   * First bit is a flag indicating if the next value (not the current value) has a small length.
   * When the first value is added and it has a small length, that length is stored in the
   * value reference and not with the value.  So, when we have multiple values, we need a way to
   * know to keep the next value's small length with the current value.
   *
   * Second bit is a flag indicating if the next value (not the current value) is the last value.
   *
   * The relative offset *backwards* to the next value.
   */

  private final class IsNextValueLengthSmallFlag {
    private static final int bitLength = 1;
    private static final long flagOnMask = 1L;
  }

  private final class IsNextValueLastFlag {
    private static final int bitLength = 1;
    private static final int bitShift = IsNextValueLengthSmallFlag.bitLength;
    private static final long flagOnMask = 1L << bitShift;
  }

  private final class NextRelativeValueOffset {
    private static final int bitLength = 40;
    private static final long allBitsOn = (1L << bitLength) - 1;
    private static final int bitShift = IsNextValueLastFlag.bitShift + IsNextValueLastFlag.bitLength;
    private static final long bitMask = allBitsOn << bitShift;
  }

  public long addFirst(byte[] valueBytes, int valueStart, int valueLength) {

    // First value is written without: next relative offset, next value length, is next value last
    // flag, is next value length small flag, etc.

    /*
     * We build up the Value Reference Word we will return that will be kept by the caller.
     */

    long valueRefWord = IsLastFlag.flagOnMask;

    valueRefWord |= ((long) 1 << CappedCount.bitShift);

    long newAbsoluteOffset;
    if (valueLength < SmallValueLength.threshold) {

      // Small case: Just write the value bytes only.

      if (valueLength == 0) {
        // We don't write a first empty value.
        // Get an offset to reduce the relative offset later if there are more than 1 value.
        newAbsoluteOffset = writeBuffers.getWritePoint();
      } else {
        newAbsoluteOffset = writeBuffers.getWritePoint();
        writeBuffers.write(valueBytes, valueStart, valueLength);
      }

      // The caller remembers the small value length.
      valueRefWord |= ((long) valueLength) << SmallValueLength.bitShift;
    } else {

      // Big case: write the length as a VInt and then the value bytes.

      newAbsoluteOffset = writeBuffers.getWritePoint();

      writeBuffers.writeVInt(valueLength);
      writeBuffers.write(valueBytes, valueStart, valueLength);

      // Use magic length value to indicate big.
      valueRefWord |= SmallValueLength.allBitsOnBitShifted;
    }

    // LOG.debug("VectorMapJoinFastValueStore addFirst valueLength " + valueLength + " newAbsoluteOffset " + newAbsoluteOffset + " valueRefWord " + Long.toHexString(valueRefWord));

    // The lower bits are the absolute value offset.
    valueRefWord |= newAbsoluteOffset;

    return valueRefWord;
  }

  public long addMore(long oldValueRef, byte[] valueBytes, int valueStart, int valueLength) {

    if ((oldValueRef & IsInvalidFlag.flagOnMask) != 0) {
      throw new RuntimeException("Invalid optimized hash table reference");
    }
    /*
     * Extract information about the old value.
     */
    long oldAbsoluteValueOffset =
        (oldValueRef & AbsoluteValueOffset.bitMask);
    int oldSmallValueLength =
        (int) ((oldValueRef & SmallValueLength.bitMask) >> SmallValueLength.bitShift);
    boolean isOldValueLengthSmall = (oldSmallValueLength != SmallValueLength.allBitsOn);
    int oldCappedCount =
        (int) ((oldValueRef & CappedCount.bitMask) >> CappedCount.bitShift);
    boolean isOldValueLast =
        ((oldValueRef & IsLastFlag.flagOnMask) != 0);

    // LOG.debug("VectorMapJoinFastValueStore addMore isOldValueLast " + isOldValueLast + " oldSmallValueLength " + oldSmallValueLength + " oldAbsoluteValueOffset " + oldAbsoluteValueOffset + " oldValueRef " + Long.toHexString(oldValueRef));

    /*
     * Write information about the old value (which becomes our next) at the beginning
     * of our new value.
     */
    long newAbsoluteOffset = writeBuffers.getWritePoint();

    long relativeOffsetWord = 0;
    if (isOldValueLengthSmall) {
      relativeOffsetWord |= IsNextValueLengthSmallFlag.flagOnMask;
    }
    if (isOldValueLast) {
      relativeOffsetWord |= IsNextValueLastFlag.flagOnMask;
    }
    int newCappedCount = oldCappedCount;
    if (newCappedCount < CappedCount.limit) {
      newCappedCount++;
    }
    long relativeOffset = newAbsoluteOffset - oldAbsoluteValueOffset;
    relativeOffsetWord |= (relativeOffset << NextRelativeValueOffset.bitShift);

    writeBuffers.writeVLong(relativeOffsetWord);

    // When the next value is small it was not recorded with the old (i.e. next) value and we
    // have to remember it.
    if (isOldValueLengthSmall) {
      writeBuffers.writeVInt(oldSmallValueLength);
    }

    // Now, we have written all information about the next value, work on the *new* value.

    long newValueRef = ((long) newCappedCount) << CappedCount.bitShift;
    boolean isNewValueSmall = (valueLength < SmallValueLength.threshold);
    if (!isNewValueSmall) {
      // Use magic value to indicating we are writing the big value length.
      newValueRef |= ((long) SmallValueLength.allBitsOn << SmallValueLength.bitShift);
      writeBuffers.writeVInt(valueLength);
    } else {
      // Caller must remember small value length.
      newValueRef |= ((long) valueLength) << SmallValueLength.bitShift;
    }
    writeBuffers.write(valueBytes, valueStart, valueLength);

    // The lower bits are the absolute value offset.
    newValueRef |=  newAbsoluteOffset;

    // LOG.debug("VectorMapJoinFastValueStore addMore valueLength " + valueLength + " newAbsoluteOffset " + newAbsoluteOffset + " newValueRef " + Long.toHexString(newValueRef));

    return newValueRef;
  }

  public VectorMapJoinFastValueStore(int writeBuffersSize) {
    writeBuffers = new WriteBuffers(writeBuffersSize, AbsoluteValueOffset.maxSize);
  }
}