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

package org.apache.hadoop.hive.ql.exec.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.MemoryEstimate;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.debug.Utils;
import org.apache.hadoop.hive.serde2.ByteStream.RandomAccessOutput;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.WriteBuffers;

import com.google.common.annotations.VisibleForTesting;


/**
 * HashMap that maps byte arrays to byte arrays with limited functionality necessary for
 * MapJoin hash tables, with small memory overhead. Supports multiple values for single key.
 * Values can be added for key (but cannot be removed); values can be gotten for the key.
 * Some things (like entrySet) are easy to add; some e.g. deletion are pretty hard to do well.
 * Additionally, for each key it contains a magic "state byte" which is not part of the key and
 * can be updated on every put for that key. That is really silly, we use it to store aliasFilter.
 * Magic byte could be removed for generality.
 * Initially inspired by HPPC LongLongOpenHashMap; however, the code is almost completely reworked
 * and there's very little in common left save for quadratic probing (and that with some changes).
 */
public final class BytesBytesMultiHashMap implements MemoryEstimate {
  public static final Logger LOG = LoggerFactory.getLogger(BytesBytesMultiHashMap.class);

  /*
   * This hashtable stores "references" in an array of longs;  index in the array is hash of
   * the key; these references point into infinite byte buffer (see below). This buffer contains
   * records written one after another. There are several simple record formats.
   * - single record for the key
   *    [key bytes][value bytes][vlong value length][vlong key length][padding]
   *    We leave padding to ensure we have at least 5 bytes after key and value.
   * - first of multiple records for the key (updated from "single value for the key")
   *    [key bytes][value bytes][5-byte long offset to a list start record]
   *  - list start record
   *    [vlong value length][vlong key length][5-byte long offset to the 2nd list record]
   *    Lengths are preserved from the first record. Offset is discussed above.
   *  - subsequent values in the list
   *    [value bytes][value length][vlong relative offset to next record].
   *
   * In summary, because we have separate list record, we have very little list overhead for
   * the typical case of primary key join, where there's no list for any key; large lists also
   * don't have a lot of relative overhead (also see the todo below).
   *
   * So the record looks as follows for one value per key (hash is fixed, 4 bytes, and is
   * stored to expand w/o rehashing, and to more efficiently deal with collision
   *
   *             i = key hash
   *           ._______.
   * REFS: ... |offset | ....
   *           `--\----'
   *               `-------------.
   *                            \|/
   *          .______._____._____'__.__._.
   * WBS: ... | hash | key | val |vl|kl| | ....
   *          `------'-----'-----'--'--'-'
   *
   * After that refs don't change so they are not pictured.
   * When we add the 2nd value, we rewrite lengths with relative offset to the list start record.
   * That way, the first record points to the "list record".
   *                         ref .---------.
   *                         \|/ |        \|/
   *       .______._____._____'__|___.     '__.__.______.
   * WBS:  | hash | key | val |offset| ... |vl|kl|      |
   *       `------'-----'-----'------'     '--'--'------'
   * After that refs don't change so they are not pictured. List record points to the 2nd value.
   *                         ref .---------.        .---------------.
   *                         \|/ |        \|/       |              \|/
   *       .______._____._____'__|___.     '__.__.__|___.     ._____'__._.
   * WBS:  | hash | key | val |offset| ... |vl|kl|offset| ... | val |vl|0|
   *       `------'-----'-----'------'     '--'--'------'     '-----'--'-'
   * If we add another value, we overwrite the list record.
   * We don't need to overwrite any vlongs and suffer because of that.
   *                         ref .---------.         .-------------------------------.
   *                         \|/ |        \|/        |                              \|/
   *       .______._____._____'__|___.     '__.__.___|__.     ._____.__._.     ._____'__.______.
   * WBS:  | hash | key | val |offset| ... |vl|kl|offset| ... | val |vl|0| ... | val |vl|offset|
   *       `------'-----'-----'------'     '--'--'------'     '-----:--'-'     '-----'--'--|---'
   *                                                               /|\                     |
   *                                                                `----------------------'
   * And another value (for example)
   * ... ---.         .-----------------------------------------------------.
   *       \|/        |                                                    \|/
   *        '__.__.___|__.     ._____.__._.     ._____.__.______.     ._____'__.______.
   * ...    |vl|kl|offset| ... | val |vl|0| ... | val |vl|offset| ... | val |vl|offset|
   *        '--'--'------'     '-----:--'-'     '-----'--:--|---'     '-----'--'--|---'
   *                                /|\                 /|\ |                     |
   *                                 `-------------------+--'                     |
   *                                                     `------------------------'
   */

  /**
   * Write buffers for keys and values. For the description of the structure above, think
   * of this as one infinite byte buffer.
   */
  private WriteBuffers writeBuffers;

  private final float loadFactor;

  private int resizeThreshold;
  private int keysAssigned;
  private int numValues;

  /**
   * Largest number of probe steps ever taken to find location for a key. When getting, we can
   * conclude that they key is not in hashtable when we make this many steps and don't find it.
   */
  private int largestNumberOfSteps = 0;

  /**
   * References to keys of the hashtable. The index is hash of the key; collisions are
   * resolved using open addressing with quadratic probing. Reference format
   * [40: offset into writeBuffers][8: state byte][1: has list flag]
   * [15: part of hash used to optimize probing]
   * Offset is tail offset of the first record for the key (the one containing the key).
   * It is not necessary to store 15 bits in particular to optimize probing; in fact when
   * we always store the hash it is not necessary. But we have nothing else to do with them.
   * TODO: actually we could also use few bits to store largestNumberOfSteps for each,
   *      so we'd stop earlier on read collision. Need to profile on real queries.
   */
  private long[] refs;
  private int startingHashBitCount, hashBitCount;

  private int metricPutConflict = 0, metricGetConflict = 0, metricExpands = 0, metricExpandsMs = 0;

  /** We have 39 bits to store list pointer from the first record; this is size limit */
  final static long MAX_WB_SIZE = ((long)1) << 38;
  /** 8 Gb of refs is the max capacity if memory limit is not specified. If someone has 100s of
   * Gbs of memory (this might happen pretty soon) we'd need to string together arrays anyway. */
  private final static int DEFAULT_MAX_CAPACITY = 1024 * 1024 * 1024;
  /** Make sure maxCapacity has a lower limit */
  private final static int DEFAULT_MIN_MAX_CAPACITY = 16 * 1024 * 1024;

  public BytesBytesMultiHashMap(int initialCapacity,
      float loadFactor, int wbSize, long maxProbeSize) {
    if (loadFactor < 0 || loadFactor > 1) {
      throw new AssertionError("Load factor must be between (0, 1].");
    }
    assert initialCapacity > 0;
    initialCapacity = (Long.bitCount(initialCapacity) == 1)
        ? initialCapacity : nextHighestPowerOfTwo(initialCapacity);
    // 8 bytes per long in the refs, assume data will be empty. This is just a sanity check.
    int maxCapacity =  (maxProbeSize <= 0) ? DEFAULT_MAX_CAPACITY
        : (int)Math.min((long)DEFAULT_MAX_CAPACITY, maxProbeSize / 8);
    if (maxCapacity < DEFAULT_MIN_MAX_CAPACITY) {
      maxCapacity = DEFAULT_MIN_MAX_CAPACITY;
    }
    if (maxCapacity < initialCapacity || initialCapacity <= 0) {
      // Either initialCapacity is too large, or nextHighestPowerOfTwo overflows
      initialCapacity = (Long.bitCount(maxCapacity) == 1)
          ? maxCapacity : nextLowestPowerOfTwo(maxCapacity);
    }

    validateCapacity(initialCapacity);
    startingHashBitCount = 63 - Long.numberOfLeadingZeros(initialCapacity);
    this.loadFactor = loadFactor;
    refs = new long[initialCapacity];
    writeBuffers = new WriteBuffers(wbSize, MAX_WB_SIZE);
    resizeThreshold = (int)(initialCapacity * this.loadFactor);
  }

  @VisibleForTesting
  BytesBytesMultiHashMap(int initialCapacity, float loadFactor, int wbSize) {
    this(initialCapacity, loadFactor, wbSize, -1);
  }

  /**
   * The result of looking up a key in the multi-hash map.
   *
   * This object can read through the 0, 1, or more values found for the key.
   */
  public static class Result {

    // Whether there are more than 0 rows.
    private boolean hasRows;

    // We need a pointer to the hash map since this class must be static to support having
    // multiple hash tables with Hybrid Grace partitioning.
    private BytesBytesMultiHashMap hashMap;

    // And, a mutable read position for thread safety when sharing a hash map.
    private WriteBuffers.Position readPos;

    // These values come from setValueResult when it finds a key.  These values allow this
    // class to read (and re-read) the values.
    private long firstOffset;
    private boolean hasList;
    private long offsetAfterListRecordKeyLen;

    // When we have multiple values, we save the next value record's offset here.
    private long nextTailOffset;

    // 0-based index of which row we are on.
    private long readIndex;

    // A reference to the current row.
    private WriteBuffers.ByteSegmentRef byteSegmentRef;

    public Result() {
      hasRows = false;
      byteSegmentRef = new WriteBuffers.ByteSegmentRef();
      readPos = new WriteBuffers.Position();
    }

    /**
     * Return the thread-safe read position.
     */
    public WriteBuffers.Position getReadPos() {
      return readPos;
    }

    /**
     * @return Whether there are 1 or more values.
     */
    public boolean hasRows() {
      // NOTE: Originally we named this isEmpty, but that name conflicted with another interface.
      return hasRows;
    }

    /**
     * @return Whether there is just 1 value row.
     */
    public boolean isSingleRow() {
      return !hasList;
    }

    /**
     * Set internal values for reading the values after finding a key.
     *
     * @param hashMap
     *          The hash map we found the key in.
     * @param firstOffset
     *          The absolute offset of the first record in the write buffers.
     * @param hasList
     *          Whether there are multiple values (true) or just a single value (false).
     * @param offsetAfterListRecordKeyLen
     *          The offset of just after the key length in the list record.  Or, 0 when single row.
     */
    public void set(BytesBytesMultiHashMap hashMap, long firstOffset, boolean hasList,
        long offsetAfterListRecordKeyLen) {

      this.hashMap = hashMap;

      this.firstOffset = firstOffset;
      this.hasList = hasList;
      this.offsetAfterListRecordKeyLen = offsetAfterListRecordKeyLen;

      // Position at first row.
      readIndex = 0;
      nextTailOffset = -1;

      hasRows = true;
    }

    public WriteBuffers.ByteSegmentRef first() {
      if (!hasRows) {
        return null;
      }

      // Position at first row.
      readIndex = 0;
      nextTailOffset = -1;

      return internalRead();
    }

    public WriteBuffers.ByteSegmentRef next() {
      if (!hasRows) {
        return null;
      }

      return internalRead();
    }

    /**
     * Read the current value.
     *
     * @return
     *           The ByteSegmentRef to the current value read.
     */
    private WriteBuffers.ByteSegmentRef internalRead() {

      if (!hasList) {

        /*
         * Single value.
         */

        if (readIndex > 0) {
          return null;
        }

        // For a non-list (i.e. single value), the offset is for the variable length long (VLong)
        // holding the value length (followed by the key length).
        hashMap.writeBuffers.setReadPoint(firstOffset, readPos);
        int valueLength = (int) hashMap.writeBuffers.readVLong(readPos);

        // The value is before the offset.  Make byte segment reference absolute.
        byteSegmentRef.reset(firstOffset - valueLength, valueLength);
        hashMap.writeBuffers.populateValue(byteSegmentRef);

        readIndex++;
        return byteSegmentRef;
      }

      /*
       * Multiple values.
       */

      if (readIndex == 0) {
        // For a list, the value and key lengths of 1st record were overwritten with the
        // relative offset to a new list record.
        long relativeOffset = hashMap.writeBuffers.readNByteLong(firstOffset, 5, readPos);

        // At the beginning of the list record will be the value length.
        hashMap.writeBuffers.setReadPoint(firstOffset + relativeOffset, readPos);
        int valueLength = (int) hashMap.writeBuffers.readVLong(readPos);

        // The value is before the list record offset.  Make byte segment reference absolute.
        byteSegmentRef.reset(firstOffset - valueLength, valueLength);
        hashMap.writeBuffers.populateValue(byteSegmentRef);

        readIndex++;
        return byteSegmentRef;
      }

      if (readIndex == 1) {
        // We remembered the offset of just after the key length in the list record.
        // Read the absolute offset to the 2nd value.
        nextTailOffset = hashMap.writeBuffers.readNByteLong(offsetAfterListRecordKeyLen, 5, readPos);
        if (nextTailOffset <= 0) {
          throw new Error("Expecting a second value");
        }
      } else if (nextTailOffset <= 0) {
        return null;
      }

      hashMap.writeBuffers.setReadPoint(nextTailOffset, readPos);

      // Get the value length.
      int valueLength = (int) hashMap.writeBuffers.readVLong(readPos);

      // Now read the relative offset to next record. Next record is always before the
      // previous record in the write buffers (see writeBuffers javadoc).
      long delta = hashMap.writeBuffers.readVLong(readPos);
      long newTailOffset = delta == 0 ? 0 : (nextTailOffset - delta);

      // The value is before the value record offset.  Make byte segment reference absolute.
      byteSegmentRef.reset(nextTailOffset - valueLength, valueLength);
      hashMap.writeBuffers.populateValue(byteSegmentRef);

      nextTailOffset = newTailOffset;
      readIndex++;
      return byteSegmentRef;
    }

    /**
     * Lets go of any references to a hash map.
     */
    public void forget() {
      hashMap = null;
      byteSegmentRef.reset(0, 0);
      hasRows = false;
      readIndex = 0;
      nextTailOffset = -1;
    }
  }

  /** The source of keys and values to put into hashtable; avoids byte copying. */
  public static interface KvSource {
    /** Write key into output. */
    public void writeKey(RandomAccessOutput dest) throws SerDeException;

    /** Write value into output. */
    public void writeValue(RandomAccessOutput dest) throws SerDeException;

    /**
     * Provide updated value for state byte for a key.
     * @param previousValue Previous value; null if this is the first call per key.
     * @return The updated value.
     */
    public byte updateStateByte(Byte previousValue);
  }

  /**
   * Adds new value to new or existing key in hashmap. Not thread-safe.
   * @param kv Keyvalue writer. Each method will be called at most once.
   */
  private static final byte[] FOUR_ZEROES = new byte[] { 0, 0, 0, 0 };
  public void put(KvSource kv, int keyHashCode) throws SerDeException {
    if (resizeThreshold <= keysAssigned) {
      expandAndRehash();
    }

    // Reserve 4 bytes for the hash (don't just reserve, there may be junk there)
    writeBuffers.write(FOUR_ZEROES);

    // Write key to buffer to compute hashcode and compare; if it's a new key, it will
    // become part of the record; otherwise, we will just write over it later.
    long keyOffset = writeBuffers.getWritePoint();

    kv.writeKey(writeBuffers);
    int keyLength = (int)(writeBuffers.getWritePoint() - keyOffset);
    int hashCode = (keyHashCode == -1) ? writeBuffers.unsafeHashCode(keyOffset, keyLength) : keyHashCode;

    int slot = findKeySlotToWrite(keyOffset, keyLength, hashCode);
    // LOG.info("Write hash code is " + Integer.toBinaryString(hashCode) + " - " + slot);

    long ref = refs[slot];
    if (ref == 0) {
      // This is a new key, keep writing the first record.
      long tailOffset = writeFirstValueRecord(kv, keyOffset, keyLength, hashCode);
      byte stateByte = kv.updateStateByte(null);
      refs[slot] = Ref.makeFirstRef(tailOffset, stateByte, hashCode, startingHashBitCount);
      ++keysAssigned;
    } else {
      // This is not a new key; we'll overwrite the key and hash bytes - not needed anymore.
      writeBuffers.setWritePoint(keyOffset - 4);
      long lrPtrOffset = createOrGetListRecord(ref);
      long tailOffset = writeValueAndLength(kv);
      addRecordToList(lrPtrOffset, tailOffset);
      byte oldStateByte = Ref.getStateByte(ref);
      byte stateByte = kv.updateStateByte(oldStateByte);
      if (oldStateByte != stateByte) {
        ref = Ref.setStateByte(ref, stateByte);
      }
      refs[slot] = Ref.setListFlag(ref);
    }
    ++numValues;
  }

  /**
   * Finds a key.  Values can be read with the supplied result object.
   *
   * Important Note: The caller is expected to pre-allocate the hashMapResult and not
   * share it among other threads.
   *
   * @param key Key buffer.
   * @param offset the offset to the key in the buffer
   * @param hashMapResult The object to fill in that can read the values.
   * @return The state byte.
   */
  public byte getValueResult(byte[] key, int offset, int length, Result hashMapResult) {

    hashMapResult.forget();

    WriteBuffers.Position readPos = hashMapResult.getReadPos();

    // First, find first record for the key.
    long ref = findKeyRefToRead(key, offset, length, readPos);
    if (ref == 0) {
      return 0;
    }

    boolean hasList = Ref.hasList(ref);

    // This relies on findKeyRefToRead doing key equality check and leaving read ptr where needed.
    long offsetAfterListRecordKeyLen = hasList ? writeBuffers.getReadPoint(readPos) : 0;

    hashMapResult.set(this, Ref.getOffset(ref), hasList, offsetAfterListRecordKeyLen);

    return Ref.getStateByte(ref);
  }

  /**
   * Take the segment reference from {@link #getValueRefs(byte[], int, List)}
   * result and makes it self-contained - adds byte array where the value is stored, and
   * updates the offset from "global" write buffers offset to offset within that array.
   */
  public void populateValue(WriteBuffers.ByteSegmentRef valueRef) {
    writeBuffers.populateValue(valueRef);
  }

  /**
   * Number of keys in the hashmap
   * @return number of keys
   */
  public int size() {
    return keysAssigned;
  }

  /**
   * Number of values in the hashmap
   * This is equal to or bigger than number of keys, since some values may share the same key
   * @return number of values
   */
  public int getNumValues() {
    return numValues;
  }

  /**
   * Number of bytes used by the hashmap
   * There are two main components that take most memory: writeBuffers and refs
   * Others include instance fields: 100
   * @return number of bytes
   */
  public long memorySize() {
    return getEstimatedMemorySize();
  }

  @Override
  public long getEstimatedMemorySize() {
    JavaDataModel jdm = JavaDataModel.get();
    long size = 0;
    size += writeBuffers.getEstimatedMemorySize();
    size += jdm.lengthForLongArrayOfSize(refs.length);
    // 11 primitive1 fields, 2 refs above with alignment
    size += JavaDataModel.alignUp(15 * jdm.primitive1(), jdm.memoryAlign());
    return size;
  }

  public void seal() {
    writeBuffers.seal();
  }

  public void clear() {
    // This will make the object completely unusable. Semantics of clear are not defined...
    this.writeBuffers.clear();
    this.refs = new long[1];
    this.keysAssigned = 0;
    this.numValues = 0;
  }

  public void expandAndRehashToTarget(int estimateNewRowCount) {
    int oldRefsCount = refs.length;
    int newRefsCount = oldRefsCount + estimateNewRowCount;
    if (resizeThreshold <= newRefsCount) {
      newRefsCount =
          (Long.bitCount(newRefsCount) == 1) ? estimateNewRowCount : nextHighestPowerOfTwo(newRefsCount);
      expandAndRehashImpl(newRefsCount);
      LOG.info("Expand and rehash to " + newRefsCount + " from " + oldRefsCount);
    }
  }

  private static void validateCapacity(long capacity) {
    if (Long.bitCount(capacity) != 1) {
      throw new AssertionError("Capacity must be a power of two");
    }
    if (capacity <= 0) {
      throw new AssertionError("Invalid capacity " + capacity);
    }
    if (capacity > Integer.MAX_VALUE) {
      throw new RuntimeException("Attempting to expand the hash table to " + capacity
          + " that overflows maximum array size. For this query, you may want to disable "
          + ConfVars.HIVEDYNAMICPARTITIONHASHJOIN.varname + " or reduce "
          + ConfVars.HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD.varname);
    }
  }

  /**
   * Finds the slot to use for writing, based on the key bytes already written to buffers.
   * @param keyOffset Offset to the key.
   * @param keyLength Length of the key.
   * @param hashCode Hash code of the key (passed in because java doesn't have ref/pointers).
   * @return The slot to use for writing; can be new, or matching existing key.
   */
  private int findKeySlotToWrite(long keyOffset, int keyLength, int hashCode) {
    final int bucketMask = (refs.length - 1);
    int slot = hashCode & bucketMask;
    long probeSlot = slot;
    int i = 0;
    while (true) {
      long ref = refs[slot];
      if (ref == 0 || isSameKey(keyOffset, keyLength, ref, hashCode)) {
        break;
      }
      ++metricPutConflict;
      // Some other key (collision) - keep probing.
      probeSlot += (++i);
      slot = (int)(probeSlot & bucketMask);
    }
    if (largestNumberOfSteps < i) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Probed " + i + " slots (the longest so far) to find space");
      }
      largestNumberOfSteps = i;
      // debugDumpKeyProbe(keyOffset, keyLength, hashCode, slot);
    }
    return slot;
  }

  /**
   * Finds the slot to use for reading.
   * @param key Read key array.
   * @param length Read key length.
   * @return The ref to use for reading.
   */
  private long findKeyRefToRead(byte[] key, int offset, int length,
          WriteBuffers.Position readPos) {
    final int bucketMask = (refs.length - 1);
    int hashCode = writeBuffers.hashCode(key, offset, length);
    int slot = hashCode & bucketMask;
    // LOG.info("Read hash code for " + Utils.toStringBinary(key, 0, length)
    //   + " is " + Integer.toBinaryString(hashCode) + " - " + slot);
    long probeSlot = slot;
    int i = 0;
    while (true) {
      long ref = refs[slot];
      // When we were inserting the key, we would have inserted here; so, there's no key.
      if (ref == 0) {
        return 0;
      }
      if (isSameKey(key, offset, length, ref, hashCode, readPos)) {
        return ref;
      }
      ++metricGetConflict;
      probeSlot += (++i);
      if (i > largestNumberOfSteps) {
        // We know we never went that far when we were inserting.
        return 0;
      }
      slot = (int)(probeSlot & bucketMask);
    }
  }

  /**
   * Puts ref into new refs array.
   * @param newRefs New refs array.
   * @param newRef New ref value.
   * @param hashCode Hash code to use.
   * @return The number of probe steps taken to find key position.
   */
  private int relocateKeyRef(long[] newRefs, long newRef, int hashCode) {
    final int bucketMask = newRefs.length - 1;
    int newSlot = hashCode & bucketMask;
    long probeSlot = newSlot;
    int i = 0;
    while (true) {
      long current = newRefs[newSlot];
      if (current == 0) {
        newRefs[newSlot] = newRef;
        break;
      }
      // New array cannot contain the records w/the same key, so just advance, don't check.
      probeSlot += (++i);
      newSlot = (int)(probeSlot & bucketMask);
    }
    return i;
  }

  /**
   * Verifies that the key matches a requisite key.
   * @param cmpOffset The offset to the key to compare with.
   * @param cmpLength The length of the key to compare with.
   * @param ref The ref that can be used to retrieve the candidate key.
   * @param hashCode
   * @return -1 if the key referenced by ref is different than the one referenced by cmp...
   *         0 if the keys match, and there's only one value for this key (no list).
   *         Offset if the keys match, and there are multiple values for this key (a list).
   */
  private boolean isSameKey(long cmpOffset, int cmpLength, long ref, int hashCode) {
    if (!compareHashBits(ref, hashCode)) {
      return false; // Hash bits in ref don't match.
    }
    writeBuffers.setUnsafeReadPoint(getFirstRecordLengthsOffset(ref, null));
    int valueLength = (int)writeBuffers.unsafeReadVLong(), keyLength = (int)writeBuffers.unsafeReadVLong();
    if (keyLength != cmpLength) {
      return false;
    }
    long keyOffset = Ref.getOffset(ref) - (valueLength + keyLength);
    // There's full hash code stored in front of the key. We could check that first. If keyLength
    // is <= 4 it obviously doesn't make sense, less bytes to check in a key. Then, if there's a
    // match, we check it in vain. But what is the proportion of matches? For writes it could be 0
    // if all keys are unique, for reads we hope it's really high. Then if there's a mismatch what
    // probability is there that key mismatches in <4 bytes (so just checking the key is faster)?
    // We assume the latter is pretty high, so we don't check for now.
    return writeBuffers.isEqual(cmpOffset, cmpLength, keyOffset, keyLength);
  }

  /**
   * Same as {@link #isSameKey(long, int, long, int)} but for externally stored key.
   */
  private boolean isSameKey(byte[] key, int offset, int length, long ref, int hashCode,
      WriteBuffers.Position readPos) {
    if (!compareHashBits(ref, hashCode)) {
      return false;  // Hash bits don't match.
    }
    writeBuffers.setReadPoint(getFirstRecordLengthsOffset(ref, readPos), readPos);
    int valueLength = (int)writeBuffers.readVLong(readPos),
        keyLength = (int)writeBuffers.readVLong(readPos);
    long keyOffset = Ref.getOffset(ref) - (valueLength + keyLength);
    // See the comment in the other isSameKey
    if (offset == 0) {
      return writeBuffers.isEqual(key, length, keyOffset, keyLength);
    } else {
      return writeBuffers.isEqual(key, offset, length, keyOffset, keyLength);
    }
  }

  private boolean compareHashBits(long ref, int hashCode) {
    long fakeRef = Ref.makeFirstRef(0, (byte)0, hashCode, startingHashBitCount);
    return (Ref.getHashBits(fakeRef) == Ref.getHashBits(ref));
  }

  /**
   * @param ref Reference.
   * @return The offset to value and key length vlongs of the first record referenced by ref.
   */
  private long getFirstRecordLengthsOffset(long ref, WriteBuffers.Position readPos) {
    long tailOffset = Ref.getOffset(ref);
    if (Ref.hasList(ref)) {
      long relativeOffset = (readPos == null) ? writeBuffers.unsafeReadNByteLong(tailOffset, 5)
          : writeBuffers.readNByteLong(tailOffset, 5, readPos);
      tailOffset += relativeOffset;
    }
    return tailOffset;
  }

  private void expandAndRehash() {
    expandAndRehashImpl(((long)refs.length) << 1);
  }

  private void expandAndRehashImpl(long capacity) {
    long expandTime = System.currentTimeMillis();
    final long[] oldRefs = refs;
    validateCapacity(capacity);
    long[] newRefs = new long[(int)capacity];

    // We store some hash bits in ref; for every expansion, we need to add one bit to hash.
    // If we have enough bits, we'll do that; if we don't, we'll rehash.
    // LOG.info("Expanding the hashtable to " + capacity + " capacity");
    int newHashBitCount = hashBitCount + 1;

    // Relocate all assigned slots from the old hash table.
    int maxSteps = 0;
    for (int oldSlot = 0; oldSlot < oldRefs.length; ++oldSlot) {
      long oldRef = oldRefs[oldSlot];
      if (oldRef == 0) {
        continue;
      }
      // TODO: we could actually store a bit flag in ref indicating whether this is a hash
      //       match or a probe, and in the former case use hash bits (for a first few resizes).
      // int hashCodeOrPart = oldSlot | Ref.getNthHashBit(oldRef, startingHashBitCount, newHashBitCount);
      writeBuffers.setUnsafeReadPoint(getFirstRecordLengthsOffset(oldRef, null));
      // Read the value and key length for the first record.
      int hashCode = (int)writeBuffers.unsafeReadNByteLong(Ref.getOffset(oldRef)
          - writeBuffers.unsafeReadVLong() - writeBuffers.unsafeReadVLong() - 4, 4);
      int probeSteps = relocateKeyRef(newRefs, oldRef, hashCode);
      maxSteps = Math.max(probeSteps, maxSteps);
    }
    this.refs = newRefs;
    this.largestNumberOfSteps = maxSteps;
    this.hashBitCount = newHashBitCount;
    this.resizeThreshold = (int)(capacity * loadFactor);
    metricExpandsMs += (System.currentTimeMillis() - expandTime);
    ++metricExpands;
  }

  /**
   * @param ref The ref.
   * @return The offset to list record pointer; list record is created if it doesn't exist.
   */
  private long createOrGetListRecord(long ref) {
    if (Ref.hasList(ref)) {
      // LOG.info("Found list record at " + writeBuffers.getReadPoint());
      return writeBuffers.getUnsafeReadPoint(); // Assumes we are here after key compare.
    }
    long firstTailOffset = Ref.getOffset(ref);
    // LOG.info("First tail offset to create list record is " + firstTailOffset);

    // Determine the length of storage for value and key lengths of the first record.
    writeBuffers.setUnsafeReadPoint(firstTailOffset);
    writeBuffers.unsafeSkipVLong();
    writeBuffers.unsafeSkipVLong();
    int lengthsLength = (int)(writeBuffers.getUnsafeReadPoint() - firstTailOffset);

    // Create the list record, copy first record value/key lengths there.
    writeBuffers.writeBytes(firstTailOffset, lengthsLength);
    long lrPtrOffset = writeBuffers.getWritePoint();
    // LOG.info("Creating list record: copying " + lengthsLength + ", lrPtrOffset " + lrPtrOffset);

    // Reserve 5 bytes for writeValueRecord to fill. There might be junk there so null them.
    writeBuffers.write(FIVE_ZEROES);
    // Link the list record to the first element.
    writeBuffers.writeFiveByteULong(firstTailOffset,
        lrPtrOffset - lengthsLength - firstTailOffset);
    return lrPtrOffset;
  }

  /**
   * Adds a newly-written record to existing list.
   * @param lrPtrOffset List record pointer offset.
   * @param tailOffset New record offset.
   */
  private void addRecordToList(long lrPtrOffset, long tailOffset) {
    // Now, insert this record into the list.
    long prevHeadOffset = writeBuffers.unsafeReadNByteLong(lrPtrOffset, 5);
    // LOG.info("Reading offset " + prevHeadOffset + " at " + lrPtrOffset);
    assert prevHeadOffset < tailOffset; // We replace an earlier element, must have lower offset.
    writeBuffers.writeFiveByteULong(lrPtrOffset, tailOffset);
    // LOG.info("Writing offset " + tailOffset + " at " + lrPtrOffset);
    writeBuffers.writeVLong(prevHeadOffset == 0 ? 0 : (tailOffset - prevHeadOffset));
  }


  /**
   * Writes first value and lengths to finish the first record after the key has been written.
   * @param kv Key-value writer.
   * @param keyOffset
   * @param keyLength Key length (already written).
   * @param hashCode
   * @return The offset of the new record.
   */
  private long writeFirstValueRecord(
      KvSource kv, long keyOffset, int keyLength, int hashCode) throws SerDeException {
    long valueOffset = writeBuffers.getWritePoint();
    kv.writeValue(writeBuffers);
    long tailOffset = writeBuffers.getWritePoint();
    int valueLength = (int)(tailOffset - valueOffset);
    // LOG.info("Writing value at " + valueOffset + " length " + valueLength);
    // In an unlikely case of 0-length key and value for the very first entry, we want to tell
    // this apart from an empty value. We'll just advance one byte; this byte will be lost.
    if (tailOffset == 0) {
      writeBuffers.reserve(1);
      ++tailOffset;
    }
    // LOG.info("First tail offset " + writeBuffers.getWritePoint());
    writeBuffers.writeVLong(valueLength);
    writeBuffers.writeVLong(keyLength);
    long lengthsLength = writeBuffers.getWritePoint() - tailOffset;
    if (lengthsLength < 5) { // Reserve space for potential future list
      writeBuffers.reserve(5 - (int)lengthsLength);
    }
    // Finally write the hash code.
    writeBuffers.writeInt(keyOffset - 4, hashCode);
    return tailOffset;
  }

  /**
   * Writes the value and value length for non-first record.
   * @param kv Key-value writer.
   * @return The offset of the new record.
   */
  private long writeValueAndLength(KvSource kv) throws SerDeException {
    long valueOffset = writeBuffers.getWritePoint();
    kv.writeValue(writeBuffers);
    long tailOffset = writeBuffers.getWritePoint();
    writeBuffers.writeVLong(tailOffset - valueOffset);
    // LOG.info("Writing value at " + valueOffset + " length " + (tailOffset - valueOffset));
    return tailOffset;
  }

  /** Writes the debug dump of the table into logs. Not thread-safe. */
  public void debugDumpTable() {
    StringBuilder dump = new StringBuilder(keysAssigned + " keys\n");
    TreeMap<Long, Integer> byteIntervals = new TreeMap<Long, Integer>();
    int examined = 0;
    for (int slot = 0; slot < refs.length; ++slot) {
      long ref = refs[slot];
      if (ref == 0) {
        continue;
      }
      ++examined;
      long recOffset = getFirstRecordLengthsOffset(ref, null);
      long tailOffset = Ref.getOffset(ref);
      writeBuffers.setUnsafeReadPoint(recOffset);
      int valueLength = (int)writeBuffers.unsafeReadVLong(),
          keyLength = (int)writeBuffers.unsafeReadVLong();
      long ptrOffset = writeBuffers.getUnsafeReadPoint();
      if (Ref.hasList(ref)) {
        byteIntervals.put(recOffset, (int)(ptrOffset + 5 - recOffset));
      }
      long keyOffset = tailOffset - valueLength - keyLength;
      byte[] key = new byte[keyLength];
      WriteBuffers.ByteSegmentRef fakeRef = new WriteBuffers.ByteSegmentRef(keyOffset, keyLength);
      byteIntervals.put(keyOffset - 4, keyLength + 4);
      writeBuffers.populateValue(fakeRef);
      System.arraycopy(fakeRef.getBytes(), (int)fakeRef.getOffset(), key, 0, keyLength);
      dump.append(Utils.toStringBinary(key, 0, key.length)).append(" ref [").append(dumpRef(ref))
        .append("]: ");
      Result hashMapResult = new Result();
      getValueResult(key, 0, key.length, hashMapResult);
      List<WriteBuffers.ByteSegmentRef> results = new ArrayList<WriteBuffers.ByteSegmentRef>();
      WriteBuffers.ByteSegmentRef byteSegmentRef = hashMapResult.first();
      while (byteSegmentRef != null) {
        results.add(hashMapResult.byteSegmentRef);
        byteSegmentRef = hashMapResult.next();
      }
      dump.append(results.size()).append(" rows\n");
      for (int i = 0; i < results.size(); ++i) {
        WriteBuffers.ByteSegmentRef segment = results.get(i);
        byteIntervals.put(segment.getOffset(),
            segment.getLength() + ((i == 0) ? 1 : 0)); // state byte in the first record
      }
    }
    if (examined != keysAssigned) {
      dump.append("Found " + examined + " keys!\n");
    }
    // Report suspicious gaps in writeBuffers
    long currentOffset = 0;
    for (Map.Entry<Long, Integer> e : byteIntervals.entrySet()) {
      long start = e.getKey(), len = e.getValue();
      if (start - currentOffset > 4) {
        dump.append("Gap! [" + currentOffset + ", " + start + ")\n");
      }
      currentOffset = start + len;
    }
    LOG.info("Hashtable dump:\n " + dump.toString());
  }

  private final static byte[] FIVE_ZEROES = new byte[] { 0,0,0,0,0 };

  private static int nextHighestPowerOfTwo(int v) {
    return Integer.highestOneBit(v) << 1;
  }

  private static int nextLowestPowerOfTwo(int v) {
    return Integer.highestOneBit(v);
  }

  @VisibleForTesting
  int getCapacity() {
    return refs.length;
  }

  /** Static helper for manipulating refs */
  private final static class Ref {
    private final static int OFFSET_SHIFT = 24;
    private final static int STATE_BYTE_SHIFT = 16;
    private final static long STATE_BYTE_MASK = ((long)1 << (OFFSET_SHIFT - STATE_BYTE_SHIFT)) - 1;
    public final static long HASH_BITS_COUNT = STATE_BYTE_SHIFT - 1;
    private final static long HAS_LIST_MASK = (long)1 << HASH_BITS_COUNT;
    private final static long HASH_BITS_MASK = HAS_LIST_MASK - 1;

    private final static long REMOVE_STATE_BYTE = ~(STATE_BYTE_MASK << STATE_BYTE_SHIFT);

    public static long getOffset(long ref) {
      return ref >>> OFFSET_SHIFT;
    }

    public static byte getStateByte(long ref) {
      return (byte)((ref >>> STATE_BYTE_SHIFT) & STATE_BYTE_MASK);
    }

    public static boolean hasList(long ref) {
      return (ref & HAS_LIST_MASK) == HAS_LIST_MASK;
    }

    public static long getHashBits(long ref) {
      return ref & HASH_BITS_MASK;
    }

    public static long makeFirstRef(long offset, byte stateByte, int hashCode, int skipHashBits) {
      long hashPart = (hashCode >>> skipHashBits) & HASH_BITS_MASK;
      return offset << OFFSET_SHIFT | hashPart | ((stateByte & 0xffl) << STATE_BYTE_SHIFT);
    }

    public static int getNthHashBit(long ref, int skippedBits, int position) {
      long hashPart = getHashBits(ref) << skippedBits; // Original hashcode, with 0-d low bits.
      return (int)(hashPart & (1 << (position - 1)));
    }


    public static long setStateByte(long ref, byte stateByte) {
      return (ref & REMOVE_STATE_BYTE) | ((stateByte & 0xffl) << STATE_BYTE_SHIFT);
    }

    public static long setListFlag(long ref) {
      return ref | HAS_LIST_MASK;
    }
  }

  private static String dumpRef(long ref) {
    return StringUtils.leftPad(Long.toBinaryString(ref), 64, "0") + " o="
        + Ref.getOffset(ref) + " s=" + Ref.getStateByte(ref) + " l=" + Ref.hasList(ref)
        + " h=" + Long.toBinaryString(Ref.getHashBits(ref));
  }

  public void debugDumpMetrics() {
    LOG.info("Map metrics: keys allocated " + this.refs.length +", keys assigned " + keysAssigned
        + ", write conflict " + metricPutConflict  + ", write max dist " + largestNumberOfSteps
        + ", read conflict " + metricGetConflict
        + ", expanded " + metricExpands + " times in " + metricExpandsMs + "ms");
  }

  private void debugDumpKeyProbe(long keyOffset, int keyLength, int hashCode, int finalSlot) {
    final int bucketMask = refs.length - 1;
    WriteBuffers.ByteSegmentRef fakeRef = new WriteBuffers.ByteSegmentRef(keyOffset, keyLength);
    writeBuffers.populateValue(fakeRef);
    int slot = hashCode & bucketMask;
    long probeSlot = slot;
    StringBuilder sb = new StringBuilder("Probe path debug for [");
    sb.append(Utils.toStringBinary(
        fakeRef.getBytes(), (int)fakeRef.getOffset(), fakeRef.getLength()));
    sb.append("] hashCode ").append(Integer.toBinaryString(hashCode)).append(" is: ");
    int i = 0;
    while (slot != finalSlot) {
      probeSlot += (++i);
      slot = (int)(probeSlot & bucketMask);
      sb.append(slot).append(" - ").append(probeSlot).append(" - ")
        .append(Long.toBinaryString(refs[slot])).append("\n");
    }
    LOG.info(sb.toString());
  }
}
