/*
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

import org.apache.hadoop.hive.common.MemoryEstimate;
import org.apache.hadoop.hive.ql.exec.JoinUtil.JoinResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastBytesHashKeyRef.KeyRef;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.serde2.WriteBuffers;
import org.apache.hadoop.hive.serde2.WriteBuffers.ByteSegmentRef;
import org.apache.hadoop.hive.serde2.WriteBuffers.Position;

// import com.google.common.base.Preconditions;

/*
 * Used by VectorMapJoinFastBytesHashMap to store the key and values for a hash map with a bytes
 * key.
 */
public class VectorMapJoinFastBytesHashMapStore implements MemoryEstimate {

  private WriteBuffers writeBuffers;

  /**
   * A store for a key and a list of 1 or more arbitrary length values in memory.
   *
   * The memory is a "infinite" byte array as a WriteBuffers object.
   *
   * We give the client (e.g. hash map logic) a 64-bit key and value reference to keep that has
   * the offset within the "infinite" byte array of the key.  The 64 bits includes about half
   * of the upper hash code to help during matching.
   *
   * We optimize the common case when the key length is short and store that information in the
   * 64 bit reference.
   *
   * When there are more than 1 value, the zero padding is overwritten with a relative offset to
   * the next value.  The next value always includes the value length.
   *
   * Cases:
   *
   *  1) One element when key and is small (and stored in the reference word):
   *
   *    Key and Value Reference
   *      |
   *      | absoluteOffset
   *      |
   *      ---------------------------------
   *                                       |
   *                                       v
   *       &lt;5 0's for Next Relative Offset&gt; &lt;Key Bytes&gt; &lt;Value Length&gt; &lt;Value Bytes&gt;
   *                NEXT (NONE)                 KEY                        VALUE
   *
   * NOTE: AbsoluteOffset.byteLength = 5
   *
   *  2) One element, general: shows optional big key length.
   *
   *   Key and Value Reference
   *      |
   *      | absoluteOffset
   *      |
   *      ---------------------------------
   *                                       |
   *                                       v
   *      &lt;5 0's for Next Relative Offset&gt; [Big Key Length] &lt;Key Bytes&gt; &lt;Value Length&gt; &lt;Value Bytes&gt;
   *                NEXT (NONE)                optional        KEY                        VALUE
   *
   *  3) Two elements when key length is small and stored in reference word:
   *
   *    Key and Value Reference
   *      |
   *      | absoluteOffset
   *      |
   *      ------------------------------------
   *                                         |
   *                                         v
   *      &lt;Next Value Rel Offset as 5 bytes&gt; &lt;Key Bytes&gt; &lt;Value Bytes&gt;
   *         |     NEXT                         KEY         VALUE
   *         |
   *         | first record absolute offset + relative offset
   *         |
   *         --------
   *                 |
   *                 v
   *                &lt;5 0's Padding for Next Value Ref&gt; &lt;Value Length&gt; &lt;Value Bytes&gt;
   *                     NEXT (NONE)                                     VALUE
   *
   *  4) Three elements showing how first record updated to point to new value and
   *     new value points to most recent (additional) value:
   *
   *    Key and Value Reference
   *      |
   *      | absoluteOffset
   *      |
   *      ------------------------------------
   *                                         |
   *                                         v
   *      &lt;Next Value Rel Offset as 5 bytes&gt; &lt;Key Bytes&gt; &lt;Value Bytes&gt;
   *         |     NEXT                         KEY         VALUE
   *         |
   *         | first record absolute offset + relative offset
   *         |
   *         |
   *         |      &lt;5 0's Padding for Next Value Ref&gt; &lt;Value Length&gt; &lt;Value Bytes&gt;
   *         |      ^    NEXT (NONE)                                    VALUE
   *         |      |
   *         |      ------
   *         |            |
   *         |            | new record absolute offset - (minus) relative offset
   *         |            |
   *          -----&gt;&lt;Next Value Rel Offset as 5 bytes&gt; &lt;Value Length&gt; &lt;Value Bytes&gt;
   *                     NEXT                                            VALUE
   *
   *
   *   5) Four elements showing how first record is again updated to point to new value and
   *     new value points to most recent (additional) value:
   *
   *    Key and Value Reference
   *      |
   *      | absoluteOffset
   *      |
   *      ------------------------------------
   *                                         |
   *                                         v
   *      &lt;Next Value Rel Offset as 5 bytes&gt; &lt;Key Bytes&gt; &lt;Value Length&gt; &lt;Value Bytes&gt;
   *         |     NEXT                          KEY                      VALUE
   *         |
   *         | first record absolute offset + relative offset
   *         |
   *         |
   *         |      &lt;5 0's Padding for Next Value Ref&gt; &lt;Value Length&gt; &lt;Value Bytes&gt;
   *         |      ^    NEXT (NONE)                                     VALUE
   *         |      |
   *         |      ------
   *         |            | record absolute offset - (minus) relative offset
   *         |            |
   *         |      &lt;Next Value Rel Offset as 5 bytes&gt; &lt;Value Length&gt; &lt;Value Bytes&gt;
   *         |      ^       NEXT                                         VALUE
   *         |      |
   *         |      ------
   *         |            |
   *         |            | new record absolute offset - (minus) relative offset
   *         |            |
   *          -----&gt;&lt;Next Value Rel Offset as 5 bytes&gt; &lt;Value Length&gt; &lt;Value Bytes&gt;
   *                        NEXT                                         VALUE
   *
   *
   *  You get the idea.
   */

  public WriteBuffers getWriteBuffers() {
    return writeBuffers;
  }

  /**
   * A hash map result that can read values stored by the key and value store, one-by-one.
   * It also has support routines for checking the hash code and key equality.
   *
   * It implements the standard map join hash map result interface.
   *
   */
  public static class HashMapResult extends VectorMapJoinHashMapResult {

    private VectorMapJoinFastBytesHashMapStore hashMapStore;

    private int keyLength;

    private boolean hasRows;
    private long refWord;
    private boolean isSingleRow;
    private long absoluteOffset;
    private long keyAbsoluteOffset;
    private long firstValueAbsoluteOffset;

    private int readIndex;
    private boolean isNextEof;

    long nextAbsoluteValueOffset;

    private ByteSegmentRef byteSegmentRef;
    private Position readPos;

    public HashMapResult() {
      super();
      refWord = -1;
      hasRows = false;
      byteSegmentRef = new ByteSegmentRef();
      readPos = new Position();
    }

    /**
     * Setup for reading the key of an entry with the equalKey method.
     * @param hashMapStore
     * @param refWord
     */
    public void setKey(VectorMapJoinFastBytesHashMapStore hashMapStore, long refWord) {

      // Preconditions.checkState(!KeyRef.getIsInvalidFlag(refWord));

      this.hashMapStore = hashMapStore;

      this.refWord = refWord;

      absoluteOffset = KeyRef.getAbsoluteOffset(refWord);

      // Position after next relative offset (fixed length) to the key.
      hashMapStore.writeBuffers.setReadPoint(absoluteOffset, readPos);

      keyLength = KeyRef.getSmallKeyLength(refWord);
      boolean isKeyLengthSmall = (keyLength != KeyRef.SmallKeyLength.allBitsOn);
      if (isKeyLengthSmall) {

        keyAbsoluteOffset = absoluteOffset;
      } else {

        // And, if current value is big we must read it.
        keyLength = hashMapStore.writeBuffers.readVInt(readPos);
        keyAbsoluteOffset = hashMapStore.writeBuffers.getReadPoint(readPos);
      }

      // NOTE: Reading is now positioned before the key bytes.
    }

    /**
     * Compare a key with the key positioned with the setKey method.
     * @param keyBytes
     * @param keyStart
     * @param keyLength
     * @return
     */
    public boolean equalKey(byte[] keyBytes, int keyStart, int keyLength) {

      if (this.keyLength != keyLength) {
        return false;
      }

      // Our reading was positioned to the key.
      if (!hashMapStore.writeBuffers.isEqual(keyBytes, keyStart, readPos, keyLength)) {
        return false;
      }

      // NOTE: WriteBuffers.isEqual does not advance the read position...

      return true;
    }

    /**
     * Mark the key matched with equalKey as a match and set up for reading the values.
     * Afterward, methods isSingleRow, cappedCount, first, next, etc may be called.
     */
    public void setMatch() {
      hasRows = true;
      isSingleRow = KeyRef.getIsSingleFlag(refWord);

      // We must set the position since equalKey does not leave us positioned correctly.
      hashMapStore.writeBuffers.setReadPoint(
          keyAbsoluteOffset + keyLength, readPos);

      // Save first value absolute offset...
      firstValueAbsoluteOffset = hashMapStore.writeBuffers.getReadPoint(readPos);

      // Position to beginning.
      readIndex = 0;
      isNextEof = false;
      setJoinResult(JoinResult.MATCH);
    }

    /**
     * Setup for a match outright.
     * @param hashMapStore
     * @param refWord
     */
    public void set(VectorMapJoinFastBytesHashMapStore hashMapStore, long refWord) {
      setKey(hashMapStore, refWord);
      setMatch();
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

      // The return values are capped to return ==0, ==1 and >= 2.
      return hasRows ? (isSingleRow ? 1 : 2) : 0;
    }

    @Override
    public ByteSegmentRef first() {
      if (!hasRows) {
        return null;
      }

      // Position to beginning.
      readIndex = 0;
      isNextEof = false;

      return internalRead();
    }

    @Override
    public ByteSegmentRef next() {
      if (!hasRows || isNextEof) {
        return null;
      }

      return internalRead();
    }

    public ByteSegmentRef internalRead() {

      int nextValueLength;

      if (readIndex == 0) {
        if (isSingleRow) {
          isNextEof = true;
          nextAbsoluteValueOffset = -1;
        } else {

          // Read the next relative offset the last inserted value record.
          final long referenceAbsoluteOffset =
              absoluteOffset - KeyRef.AbsoluteOffset.byteLength;
          hashMapStore.writeBuffers.setReadPoint(
              referenceAbsoluteOffset, readPos);
          long relativeNextValueOffset =
              hashMapStore.writeBuffers.readNByteLong(
                  KeyRef.AbsoluteOffset.byteLength, readPos);
          // Preconditions.checkState(relativeNextValueOffset != 0);
          isNextEof = false;

          // Use positive relative offset from first record to last inserted value record.
          nextAbsoluteValueOffset = referenceAbsoluteOffset + relativeNextValueOffset;
        }

        // Position past the key to first value.
        hashMapStore.writeBuffers.setReadPoint(firstValueAbsoluteOffset, readPos);
        nextValueLength = hashMapStore.writeBuffers.readVInt(readPos);
      } else {

        // Position to the next value record.
        // Preconditions.checkState(nextAbsoluteValueOffset >= 0);
        hashMapStore.writeBuffers.setReadPoint(nextAbsoluteValueOffset, readPos);

        // Read the next relative offset.
        long relativeNextValueOffset =
            hashMapStore.writeBuffers.readNByteLong(
                RelativeOffset.byteLength, readPos);
        if (relativeNextValueOffset == 0) {
          isNextEof = true;
          nextAbsoluteValueOffset = -1;
        } else {
          isNextEof = false;

          // The way we insert causes our chain to backwards from the last inserted value record...
          nextAbsoluteValueOffset = nextAbsoluteValueOffset - relativeNextValueOffset;
        }
        nextValueLength = hashMapStore.writeBuffers.readVInt(readPos);

        // Now positioned to the value.
      }

      // Capture a ByteSegmentRef to the current value position and length.
      hashMapStore.writeBuffers.getByteSegmentRefToCurrent(
          byteSegmentRef, nextValueLength, readPos);

      readIndex++;
      return byteSegmentRef;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("(" + super.toString() + ", ");
      sb.append("cappedCount " + cappedCount() + ")");
      return sb.toString();
    }

    /**
     * Get detailed HashMap result position information to help diagnose exceptions.
     */
    @Override
    public String getDetailedHashMapResultPositionString() {
      StringBuilder sb = new StringBuilder();

      sb.append("Read index ");
      sb.append(readIndex);
      if (isSingleRow) {
        sb.append(" single row");
      } else {
        sb.append(" multiple rows ");
      }

      if (readIndex > 0) {
        sb.append(" byteSegmentRef is byte[] of length ");
        sb.append(byteSegmentRef.getBytes().length);
        sb.append(" at offset ");
        sb.append(byteSegmentRef.getOffset());
        sb.append(" for length ");
        sb.append(byteSegmentRef.getLength());
        if (!isSingleRow) {
          sb.append(" (isNextEof ");
          sb.append(isNextEof);
          sb.append(" nextAbsoluteValueOffset ");
          sb.append(nextAbsoluteValueOffset);
          sb.append(")");
        }
      }

      return sb.toString();
    }
  }

  private static final class RelativeOffset {
    private static final int byteLength = KeyRef.AbsoluteOffset.byteLength;

    // Relative offset zero padding.
    private static final byte[] zeroPadding = new byte[] { 0,0,0,0,0 };
  }

  /**
   * Two 64-bit long result is the key and value reference.
   * @param partialHashCode
   * @param keyBytes
   * @param keyStart
   * @param keyLength
   * @param valueBytes
   * @param valueStart
   * @param valueLength
   */
  public long addFirst(long partialHashCode, byte[] keyBytes, int keyStart, int keyLength,
      byte[] valueBytes, int valueStart, int valueLength) {

    // Zero pad out bytes for fixed size next relative offset if more values are added later.
    writeBuffers.write(RelativeOffset.zeroPadding);

    // We require the absolute offset to be non-zero so the 64 key and value reference is non-zero.
    // So, we make it the offset after the relative offset and to the key.
    final long absoluteOffset = writeBuffers.getWritePoint();
    // Preconditions.checkState(absoluteOffset > 0);

    boolean isKeyLengthBig = (keyLength >= KeyRef.SmallKeyLength.threshold);
    if (isKeyLengthBig) {
      writeBuffers.writeVInt(keyLength);
    }
    writeBuffers.write(keyBytes, keyStart, keyLength);

    writeBuffers.writeVInt(valueLength);
    writeBuffers.write(valueBytes, valueStart, valueLength);

    /*
     * Form 64 bit key and value reference.
     */
    long refWord = partialHashCode;

    refWord |= absoluteOffset << KeyRef.AbsoluteOffset.bitShift;

    if (isKeyLengthBig) {
      refWord |= KeyRef.SmallKeyLength.allBitsOnBitShifted;
    } else {
      refWord |= ((long) keyLength) << KeyRef.SmallKeyLength.bitShift;
    }

    refWord |= KeyRef.IsSingleFlag.flagOnMask;

    // Preconditions.checkState(!KeyRef.getIsInvalidFlag(refWord));

    return refWord;
  }

  /**
   * @param refWord
   * @param valueBytes
   * @param valueStart
   * @param valueLength
   */
  public long addMore(long refWord, byte[] valueBytes, int valueStart, int valueLength,
      WriteBuffers.Position unsafeReadPos) {

    // Preconditions.checkState(!KeyRef.getIsInvalidFlag(refWord));

    /*
     * Extract information from the reference word.
     */
    final long referenceAbsoluteOffset =
        KeyRef.getAbsoluteOffset(refWord) - KeyRef.AbsoluteOffset.byteLength;

    // Where the new value record will be written.
    long nextAbsoluteValueOffset = writeBuffers.getWritePoint();

    if (KeyRef.getIsSingleFlag(refWord)) {

      // Mark reference as having more than 1 value.
      refWord &= KeyRef.IsSingleFlag.flagOffMask;

      // Write zeros to indicate no 3rd record.
      writeBuffers.write(RelativeOffset.zeroPadding);
    } else {

      // To insert next value record above count 2:

      // 1) Read next relative offset in first record (this is a positive relative offset) to
      //    last inserted value record.
      long oldPrevRelativeValueOffset =
          writeBuffers.readNByteLong(
              referenceAbsoluteOffset, RelativeOffset.byteLength, unsafeReadPos);

      // 2) Relative offset is positive from first record to last inserted value record.
      long prevAbsoluteValueOffset = referenceAbsoluteOffset + oldPrevRelativeValueOffset;

      // 3) Since previous record is before the new one, subtract because we store relative offsets
      //    as unsigned.
      long newPrevRelativeValueOffset = nextAbsoluteValueOffset - prevAbsoluteValueOffset;
      // Preconditions.checkState(newPrevRelativeValueOffset >= 0);
      writeBuffers.writeFiveByteULong(newPrevRelativeValueOffset);
    }

    writeBuffers.writeVInt(valueLength);
    writeBuffers.write(valueBytes, valueStart, valueLength);

    // Overwrite relative offset in first record.
    long newRelativeOffset = nextAbsoluteValueOffset - referenceAbsoluteOffset;
    // Preconditions.checkState(newRelativeOffset >= 0);
    writeBuffers.writeFiveByteULong(referenceAbsoluteOffset, newRelativeOffset);

    return refWord;
  }

  public void getKey(long refWord, ByteSegmentRef keyByteSegmentRef,
      WriteBuffers.Position readPos) {

    final long absoluteOffset = KeyRef.getAbsoluteOffset(refWord);

    writeBuffers.setReadPoint(absoluteOffset, readPos);

    int keyLength = KeyRef.getSmallKeyLength(refWord);
    boolean isKeyLengthSmall = (keyLength != KeyRef.SmallKeyLength.allBitsOn);
    if (!isKeyLengthSmall) {

      // Read big key length we wrote with the key.
      keyLength = writeBuffers.readVInt(readPos);
    }
    writeBuffers.getByteSegmentRefToCurrent(keyByteSegmentRef, keyLength, readPos);
  }

  public VectorMapJoinFastBytesHashMapStore(int writeBuffersSize) {
    writeBuffers = new WriteBuffers(writeBuffersSize, KeyRef.AbsoluteOffset.maxSize);
  }

  @Override
  public long getEstimatedMemorySize() {
    long size = 0;
    size += writeBuffers == null ? 0 : writeBuffers.getEstimatedMemorySize();
    return size;
  }
}
