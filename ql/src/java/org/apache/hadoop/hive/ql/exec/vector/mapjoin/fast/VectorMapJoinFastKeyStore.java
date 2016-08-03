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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde2.WriteBuffers;

// Optimized for sequential key lookup.

public class VectorMapJoinFastKeyStore {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastKeyStore.class.getName());

  private WriteBuffers writeBuffers;

  private WriteBuffers.Position unsafeReadPos; // Thread-unsafe position used at write time.

  /**
   * A store for arbitrary length keys in memory.
   *
   * The memory is a "infinite" byte array or WriteBuffers object.
   *
   * We give the client a 64-bit (long) key reference to keep that has the offset within
   * the "infinite" byte array of the key.
   *
   * We optimize the common case when keys are short and store the key length in the key reference
   * word.
   *
   * If the key is big, the big length will be encoded as an integer at the beginning of the key
   * followed by the big key bytes.
   */

  /**
   * Bit-length fields within a 64-bit (long) key reference.
   *
   * Lowest field: An absolute byte offset the the key in the WriteBuffers.
   *
   * Next field: For short keys, the length of the key.  Otherwise, a special constant
   * indicating a big key whose length is stored with the key.
   *
   * Last field: an always on bit to insure the key reference non-zero when the offset and
   * length are zero.
   */

  /*
   * The absolute offset to the beginning of the key within the WriteBuffers.
   */
  private final class AbsoluteKeyOffset {
    private static final int bitLength = 40;
    private static final long allBitsOn = (((long) 1) << bitLength) - 1;
    private static final long bitMask = allBitsOn;

    // Make it a power of 2 by backing down (i.e. the -2).
    private static final long maxSize = ((long) 1) << (bitLength - 2);
  }

  /*
   * The small key length.
   *
   * If the key is big (i.e. length >= allBitsOn), then the key length is stored in the
   * WriteBuffers.
   */
  private final class SmallKeyLength {
    private static final int bitLength = 20;
    private static final int allBitsOn = (1 << bitLength) - 1;
    private static final int threshold = allBitsOn;  // Lower this for big key testing.
    private static final int bitShift = AbsoluteKeyOffset.bitLength;
    private static final long bitMask = ((long) allBitsOn) << bitShift;
    private static final long allBitsOnBitShifted = ((long) allBitsOn) << bitShift;
  }

  /*
   * An always on bit to insure the key reference non-zero.
   */
  private final class IsNonZeroFlag {
    private static final int bitShift = SmallKeyLength.bitShift + SmallKeyLength.bitLength;;
    private static final long flagOnMask = ((long) 1) << bitShift;
  }

  public long add(byte[] keyBytes, int keyStart, int keyLength) {
    boolean isKeyLengthBig = (keyLength >= SmallKeyLength.threshold);

    long absoluteKeyOffset = writeBuffers.getWritePoint();
    if (isKeyLengthBig) {
      writeBuffers.writeVInt(keyLength);
    }
    writeBuffers.write(keyBytes, keyStart, keyLength);

    long keyRefWord = IsNonZeroFlag.flagOnMask;
    if (isKeyLengthBig) {
      keyRefWord |= SmallKeyLength.allBitsOnBitShifted;
    } else {
      keyRefWord |= ((long) keyLength) << SmallKeyLength.bitShift;
    }
    keyRefWord |= absoluteKeyOffset;

    // LOG.debug("VectorMapJoinFastKeyStore add keyLength " + keyLength + " absoluteKeyOffset " + absoluteKeyOffset + " keyRefWord " + Long.toHexString(keyRefWord));
    return keyRefWord;
  }

  /** THIS METHOD IS NOT THREAD-SAFE. Use only at load time (or be mindful of thread safety). */
  public boolean unsafeEqualKey(long keyRefWord, byte[] keyBytes, int keyStart, int keyLength) {
    return equalKey(keyRefWord, keyBytes, keyStart, keyLength, unsafeReadPos);
  }

  public boolean equalKey(long keyRefWord, byte[] keyBytes, int keyStart, int keyLength,
      WriteBuffers.Position readPos) {

    int storedKeyLengthLength =
        (int) ((keyRefWord & SmallKeyLength.bitMask) >> SmallKeyLength.bitShift);
    boolean isKeyLengthSmall = (storedKeyLengthLength != SmallKeyLength.allBitsOn);

    // LOG.debug("VectorMapJoinFastKeyStore equalKey keyLength " + keyLength + " isKeyLengthSmall " + isKeyLengthSmall + " storedKeyLengthLength " + storedKeyLengthLength + " keyRefWord " + Long.toHexString(keyRefWord));

    if (isKeyLengthSmall && storedKeyLengthLength != keyLength) {
      return false;
    }
    long absoluteKeyOffset =
        (keyRefWord & AbsoluteKeyOffset.bitMask);

    writeBuffers.setReadPoint(absoluteKeyOffset, readPos);
    if (!isKeyLengthSmall) {
      // Read big value length we wrote with the value.
      storedKeyLengthLength = writeBuffers.readVInt(readPos);
      if (storedKeyLengthLength != keyLength) {
        // LOG.debug("VectorMapJoinFastKeyStore equalKey no match big length");
        return false;
      }
    }

    // Our reading is positioned to the key.
    if (!writeBuffers.isEqual(keyBytes, keyStart, readPos, keyLength)) {
      // LOG.debug("VectorMapJoinFastKeyStore equalKey no match on bytes");
      return false;
    }

    // LOG.debug("VectorMapJoinFastKeyStore equalKey match on bytes");
    return true;
  }

  public VectorMapJoinFastKeyStore(int writeBuffersSize) {
    writeBuffers = new WriteBuffers(writeBuffersSize, AbsoluteKeyOffset.maxSize);
    unsafeReadPos = new WriteBuffers.Position();
  }

  public VectorMapJoinFastKeyStore(WriteBuffers writeBuffers) {
    // TODO: Check if maximum size compatible with AbsoluteKeyOffset.maxSize.
    this.writeBuffers = writeBuffers;
    unsafeReadPos = new WriteBuffers.Position();
  }
}
