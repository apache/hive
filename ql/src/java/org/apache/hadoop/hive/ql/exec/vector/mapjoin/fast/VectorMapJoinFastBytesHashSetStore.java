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
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashSetResult;
import org.apache.hadoop.hive.serde2.WriteBuffers;
import org.apache.hadoop.hive.serde2.WriteBuffers.Position;

// import com.google.common.base.Preconditions;

/*
 * Used by VectorMapJoinFastBytesHashSet to store the key and count for a hash set with
 * a bytes key.
 */
public class VectorMapJoinFastBytesHashSetStore implements MemoryEstimate {

  private WriteBuffers writeBuffers;

  /**
   * A store for a bytes key for a hash set in memory.
   *
   * The memory is a "infinite" byte array as a WriteBuffers object.
   *
   * We give the client (e.g. hash set logic) a 64-bit key and count reference to keep that
   * has the offset within the "infinite" byte array of the key.  The 64 bits includes about half
   * of the upper hash code to help during matching.
   *
   * We optimize the common case when the key length is short and store that information in the
   * 64 bit reference.
   *
   * Cases:
   *
   *  1) One element when key and is small (and stored in the reference word):
   *
   *    Key and Value Reference
   *      |
   *      | absoluteOffset
   *      |
   *      |
   *      v
   *      &lt;Key Bytes&gt;
   *        KEY
   *
   *  2) One element, general: shows optional big key length.
   *
   *   Key and Value Reference
   *      |
   *      | absoluteOffset
   *      |
   *      |
   *      v
   *      [Big Key Length] &lt;Key Bytes&gt;
   *        optional           KEY
   */

  public WriteBuffers getWriteBuffers() {
    return writeBuffers;
  }

  /**
   * A hash set result for the key.
   * It also has support routines for checking the hash code and key equality.
   *
   * It implements the standard map join hash set result interface.
   *
   */
  public static class HashSetResult extends VectorMapJoinHashSetResult {

    private VectorMapJoinFastBytesHashSetStore setStore;

    private int keyLength;

    private long absoluteOffset;

    private Position readPos;

    public HashSetResult() {
      super();
      readPos = new Position();
    }

    /**
     * Setup for reading the key of an entry with the equalKey method.
     * @param setStore
     * @param refWord
     */
    public void setKey(VectorMapJoinFastBytesHashSetStore setStore, long refWord) {

      // Preconditions.checkState(!KeyRef.getIsInvalidFlag(refWord));

      this.setStore = setStore;

      absoluteOffset = KeyRef.getAbsoluteOffset(refWord);

      // Position after next relative offset (fixed length) to the key.
      setStore.writeBuffers.setReadPoint(absoluteOffset, readPos);

      keyLength = KeyRef.getSmallKeyLength(refWord);
      boolean isKeyLengthSmall = (keyLength != KeyRef.SmallKeyLength.allBitsOn);
      if (!isKeyLengthSmall) {

        // And, if current value is big we must read it.
        keyLength = setStore.writeBuffers.readVInt(readPos);
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
      if (!setStore.writeBuffers.isEqual(keyBytes, keyStart, readPos, keyLength)) {
        return false;
      }

      // NOTE: WriteBuffers.isEqual does not advance the read position...

      return true;
    }

    /**
     * Mark the key matched with equalKey as a match and read the set membership count,
     * if necessary.
     */
    public void setContains() {
      setJoinResult(JoinResult.MATCH);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(super.toString());
      return sb.toString();
    }
  }

  /**
   * Two 64-bit long result is the key and value reference.
   * @param partialHashCode
   * @param keyBytes
   * @param keyStart
   * @param keyLength
   */
  public long add(long partialHashCode, byte[] keyBytes, int keyStart, int keyLength) {

    // We require the absolute offset to be non-zero so the 64 key and value reference is non-zero.
    // So, we make it the offset after the relative offset and to the key.
    final long absoluteOffset = writeBuffers.getWritePoint();

    // NOTE: In order to guarantee the reference word is non-zero, later we will set the
    // NOTE: single flag.

    boolean isKeyLengthBig = (keyLength >= KeyRef.SmallKeyLength.threshold);
    if (isKeyLengthBig) {
      writeBuffers.writeVInt(keyLength);
    }
    writeBuffers.write(keyBytes, keyStart, keyLength);

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

  public VectorMapJoinFastBytesHashSetStore(int writeBuffersSize) {
    writeBuffers = new WriteBuffers(writeBuffersSize, KeyRef.AbsoluteOffset.maxSize);
  }

  @Override
  public long getEstimatedMemorySize() {
    long size = 0;
    size += writeBuffers == null ? 0 : writeBuffers.getEstimatedMemorySize();
    return size;
  }
}
