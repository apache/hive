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

import org.apache.hadoop.hive.serde2.WriteBuffers;
// import com.google.common.base.Preconditions;

public class VectorMapJoinFastBytesHashKeyRef {

  public static boolean equalKey(long refWord, byte[] keyBytes, int keyStart, int keyLength,
      WriteBuffers writeBuffers, WriteBuffers.Position readPos) {

    // Preconditions.checkState((refWord & KeyRef.IsInvalidFlag.flagOnMask) == 0);

    final long absoluteOffset = KeyRef.getAbsoluteOffset(refWord);

    writeBuffers.setReadPoint(absoluteOffset, readPos);

    int actualKeyLength = KeyRef.getSmallKeyLength(refWord);
    boolean isKeyLengthSmall = (actualKeyLength != KeyRef.SmallKeyLength.allBitsOn);
    if (!isKeyLengthSmall) {

      // And, if current value is big we must read it.
      actualKeyLength = writeBuffers.readVInt(readPos);
    }

    if (actualKeyLength != keyLength) {
      return false;
    }

    // Our reading was positioned to the key.
    if (!writeBuffers.isEqual(keyBytes, keyStart, readPos, keyLength)) {
      return false;
    }

    return true;
  }

  public static int calculateHashCode(long refWord, WriteBuffers writeBuffers,
      WriteBuffers.Position readPos) {

    // Preconditions.checkState((refWord & KeyRef.IsInvalidFlag.flagOnMask) == 0);

    final long absoluteOffset = KeyRef.getAbsoluteOffset(refWord);

    int actualKeyLength = KeyRef.getSmallKeyLength(refWord);
    boolean isKeyLengthSmall = (actualKeyLength != KeyRef.SmallKeyLength.allBitsOn);
    final long keyAbsoluteOffset;
    if (!isKeyLengthSmall) {

      // Position after next relative offset (fixed length) to the key.
      writeBuffers.setReadPoint(absoluteOffset, readPos);

      // And, if current value is big we must read it.
      actualKeyLength = writeBuffers.readVInt(readPos);
      keyAbsoluteOffset = absoluteOffset + actualKeyLength;
    } else {
      keyAbsoluteOffset = absoluteOffset;
    }

    return writeBuffers.unsafeHashCode(keyAbsoluteOffset, actualKeyLength);
  }

  public static final class KeyRef {

    // Lowest field.
    public static final class PartialHashCode {
      public static final int bitLength = 15;
      public static final long allBitsOn = (1L << bitLength) - 1;
      public static final long bitMask = allBitsOn;

      // Choose the high bits of the hash code KNOWING it was calculated as an int.
      //
      // We want the partial hash code to be different than the
      // lower bits used for our hash table slot calculations.
      public static final int intChooseBitShift = Integer.SIZE - bitLength;
    }

    public static long getPartialHashCode(long refWord) {
      // No shift needed since this is the lowest field.
      return refWord & PartialHashCode.bitMask;
    }

    // Can make the 64 bit reference non-zero if this is non-zero.  E.g. for hash map and
    // hash multi-set, the offset is to the first key which is always preceded by a 5 byte next
    // relative value offset or 4 byte count.
    public static final class AbsoluteOffset {
      public static final int bitLength = 39;
      public static final int byteLength = (bitLength + Byte.SIZE -1) / Byte.SIZE;
      public static final long allBitsOn = (1L << bitLength) - 1;
      public static final int bitShift = PartialHashCode.bitLength;
      public static final long bitMask = ((long) allBitsOn) << bitShift;

      // Make it a power of 2.
      public static final long maxSize = 1L << (bitLength - 2);
    }

    public static long getAbsoluteOffset(long refWord) {
      return (refWord & KeyRef.AbsoluteOffset.bitMask) >> AbsoluteOffset.bitShift;
    }

    // When this field equals SmallKeyLength.allBitsOn, the key length is serialized at the
    // beginning of the key.
    public static final class SmallKeyLength {
      public static final int bitLength = 8;
      public static final int allBitsOn = (1 << bitLength) - 1;
      public static final int threshold = allBitsOn;
      public static final int bitShift = AbsoluteOffset.bitShift + AbsoluteOffset.bitLength;
      public static final long bitMask = ((long) allBitsOn) << bitShift;
      public static final long allBitsOnBitShifted = ((long) allBitsOn) << bitShift;
    }

    public static int getSmallKeyLength(long refWord) {
      return (int) ((refWord & SmallKeyLength.bitMask) >> SmallKeyLength.bitShift);
    }

    public static final class IsSingleFlag {
      public static final int bitShift = SmallKeyLength.bitShift + SmallKeyLength.bitLength;
      public static final long flagOnMask = 1L << bitShift;
      public static final long flagOffMask = ~flagOnMask;
    }

    public static boolean getIsSingleFlag(long refWord) {
      return (refWord & IsSingleFlag.flagOnMask) != 0;
    }

    // This bit should not be on for valid value references.  We use -1 for a no value marker.
    public static final class IsInvalidFlag {
      public static final int bitShift = 63;
      public static final long flagOnMask = 1L << bitShift;
    }

    public static boolean getIsInvalidFlag(long refWord) {
      return (refWord & IsInvalidFlag.flagOnMask) != 0;
    }
  }


  /**
   * Extract partial hash code from the full hash code.
   *
   * Choose the high bits of the hash code KNOWING it was calculated as an int.
   *
   * We want the partial hash code to be different than the
   * lower bits used for our hash table slot calculations.
   *
   * @param hashCode
   * @return
   */
  public static long extractPartialHashCode(long hashCode) {
    return (hashCode >>> KeyRef.PartialHashCode.intChooseBitShift) & KeyRef.PartialHashCode.bitMask;
  }

  /**
   * Get partial hash code from the reference word.
   * @param hashCode
   * @return
   */
  public static long getPartialHashCodeFromRefWord(long refWord) {
    return KeyRef.getPartialHashCode(refWord);
  }
}
