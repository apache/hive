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

package org.apache.hadoop.hive.ql.exec.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
public final class BytesBytesMultiHashMap {
  public static final Log LOG = LogFactory.getLog(BytesBytesMultiHashMap.class);

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

  private int metricPutConflict = 0, metricExpands = 0, metricExpandsUs = 0;

  /** We have 39 bits to store list pointer from the first record; this is size limit */
  final static long MAX_WB_SIZE = ((long)1) << 38;
  /** 8 Gb of refs is the max capacity if memory limit is not specified. If someone has 100s of
   * Gbs of memory (this might happen pretty soon) we'd need to string together arrays anyway. */
  private final static int DEFAULT_MAX_CAPACITY = 1024 * 1024 * 1024;

  public BytesBytesMultiHashMap(int initialCapacity,
      float loadFactor, int wbSize, long memUsage, int defaultCapacity) {
    if (loadFactor < 0 || loadFactor > 1) {
      throw new AssertionError("Load factor must be between (0, 1].");
    }
    assert initialCapacity > 0;
    initialCapacity = (Long.bitCount(initialCapacity) == 1)
        ? initialCapacity : nextHighestPowerOfTwo(initialCapacity);
    // 8 bytes per long in the refs, assume data will be empty. This is just a sanity check.
    int maxCapacity =  (memUsage <= 0) ? DEFAULT_MAX_CAPACITY
        : (int)Math.min((long)DEFAULT_MAX_CAPACITY, memUsage / 8);
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
    this(initialCapacity, loadFactor, wbSize, -1, 100000);
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
   * Adds new value to new or existing key in hashmap.
   * @param kv Keyvalue writer. Each method will be called at most once.
   */
  private static final byte[] FOUR_ZEROES = new byte[] { 0, 0, 0, 0 };
  public void put(KvSource kv) throws SerDeException {
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
    int hashCode = writeBuffers.hashCode(keyOffset, keyLength);

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
  }

  /**
   * Gets "lazy" values for a key (as a set of byte segments in underlying buffer).
   * @param key Key buffer.
   * @param length Length of the key in buffer.
   * @param result The list to use to store the results.
   * @return the state byte for the key (see class description).
   */
  public byte getValueRefs(byte[] key, int length, List<WriteBuffers.ByteSegmentRef> result) {
    // First, find first record for the key.
    result.clear();
    long ref = findKeyRefToRead(key, length);
    if (ref == 0) {
      return 0;
    }
    boolean hasList = Ref.hasList(ref);

    // This relies on findKeyRefToRead doing key equality check and leaving read ptr where needed.
    long lrPtrOffset = hasList ? writeBuffers.getReadPoint() : 0;

    writeBuffers.setReadPoint(getFirstRecordLengthsOffset(ref));
    int valueLength = (int)writeBuffers.readVLong();
    // LOG.info("Returning value at " + (Ref.getOffset(ref) - valueLength) +  " length " + valueLength);
    result.add(new WriteBuffers.ByteSegmentRef(Ref.getOffset(ref) - valueLength, valueLength));
    byte stateByte = Ref.getStateByte(ref);
    if (!hasList) {
      return stateByte;
    }

    // There're multiple records for the key; get the offset of the next one.
    long nextTailOffset = writeBuffers.readFiveByteULong(lrPtrOffset);
    // LOG.info("Next tail offset " + nextTailOffset);

    while (nextTailOffset > 0) {
      writeBuffers.setReadPoint(nextTailOffset);
      valueLength = (int)writeBuffers.readVLong();
      // LOG.info("Returning value at " + (nextTailOffset - valueLength) +  " length " + valueLength);
      result.add(new WriteBuffers.ByteSegmentRef(nextTailOffset - valueLength, valueLength));
      // Now read the relative offset to next record. Next record is always before the
      // previous record in the write buffers (see writeBuffers javadoc).
      long delta = writeBuffers.readVLong();
      nextTailOffset = delta == 0 ? 0 : (nextTailOffset - delta);
      // LOG.info("Delta " + delta +  ", next tail offset " + nextTailOffset);
    }
    return stateByte;
  }


  /**
   * Take the segment reference from {@link #getValueRefs(byte[], int, List)}
   * result and makes it self-contained - adds byte array where the value is stored, and
   * updates the offset from "global" write buffers offset to offset within that array.
   */
  public void populateValue(WriteBuffers.ByteSegmentRef valueRef) {
    writeBuffers.populateValue(valueRef);
  }

  public int size() {
    return keysAssigned;
  }

  public void seal() {
    writeBuffers.seal();
  }

  public void clear() {
    // This will make the object completely unusable. Semantics of clear are not defined...
    this.writeBuffers.clear();
    this.refs = new long[1];
    this.keysAssigned = 0;
  }

  private static void validateCapacity(long capacity) {
    if (Long.bitCount(capacity) != 1) {
      throw new AssertionError("Capacity must be a power of two");
    }
    if (capacity <= 0) {
      throw new AssertionError("Invalid capacity " + capacity);
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
  private long findKeyRefToRead(byte[] key, int length) {
    final int bucketMask = (refs.length - 1);
    int hashCode = writeBuffers.hashCode(key, 0, length);
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
      if (isSameKey(key, length, ref, hashCode)) {
        return ref;
      }
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
    writeBuffers.setReadPoint(getFirstRecordLengthsOffset(ref));
    int valueLength = (int)writeBuffers.readVLong(), keyLength = (int)writeBuffers.readVLong();
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
  private boolean isSameKey(byte[] key, int length, long ref, int hashCode) {
    if (!compareHashBits(ref, hashCode)) {
      return false;  // Hash bits don't match.
    }
    writeBuffers.setReadPoint(getFirstRecordLengthsOffset(ref));
    int valueLength = (int)writeBuffers.readVLong(), keyLength = (int)writeBuffers.readVLong();
    long keyOffset = Ref.getOffset(ref) - (valueLength + keyLength);
    // See the comment in the other isSameKey
    return writeBuffers.isEqual(key, length, keyOffset, keyLength);
  }

  private boolean compareHashBits(long ref, int hashCode) {
    long fakeRef = Ref.makeFirstRef(0, (byte)0, hashCode, startingHashBitCount);
    return (Ref.getHashBits(fakeRef) == Ref.getHashBits(ref));
  }

  /**
   * @param ref Reference.
   * @return The offset to value and key length vlongs of the first record referenced by ref.
   */
  private long getFirstRecordLengthsOffset(long ref) {
    long tailOffset = Ref.getOffset(ref);
    if (Ref.hasList(ref)) {
      long relativeOffset = writeBuffers.readFiveByteULong(tailOffset);
      tailOffset += relativeOffset;
    }
    return tailOffset;
  }

  private void expandAndRehash() {
    long expandTime = System.nanoTime();
    final long[] oldRefs = refs;
    long capacity = refs.length << 1;
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
      writeBuffers.setReadPoint(getFirstRecordLengthsOffset(oldRef));
      // Read the value and key length for the first record.
      int hashCode = writeBuffers.readInt(Ref.getOffset(oldRef)
          - writeBuffers.readVLong() - writeBuffers.readVLong() - 4);
      int probeSteps = relocateKeyRef(newRefs, oldRef, hashCode);
      maxSteps = Math.max(probeSteps, maxSteps);
    }
    this.refs = newRefs;
    this.largestNumberOfSteps = maxSteps;
    this.hashBitCount = newHashBitCount;
    this.resizeThreshold = (int)(capacity * loadFactor);
    metricExpandsUs += (System.nanoTime() - expandTime);
    ++metricExpands;

  }

  /**
   * @param ref The ref.
   * @return The offset to list record pointer; list record is created if it doesn't exist.
   */
  private long createOrGetListRecord(long ref) {
    if (Ref.hasList(ref)) {
      // LOG.info("Found list record at " + writeBuffers.getReadPoint());
      return writeBuffers.getReadPoint(); // Assumes we are here after key compare.
    }
    long firstTailOffset = Ref.getOffset(ref);
    // LOG.info("First tail offset to create list record is " + firstTailOffset);

    // Determine the length of storage for value and key lengths of the first record.
    writeBuffers.setReadPoint(firstTailOffset);
    writeBuffers.skipVLong();
    writeBuffers.skipVLong();
    int lengthsLength = (int)(writeBuffers.getReadPoint() - firstTailOffset);

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
    long prevHeadOffset = writeBuffers.readFiveByteULong(lrPtrOffset);
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

  /** Writes the debug dump of the table into logs. */
  public void debugDumpTable() {
    StringBuilder dump = new StringBuilder(keysAssigned + " keys\n");
    TreeMap<Long, Integer> byteIntervals = new TreeMap<Long, Integer>();
    List<WriteBuffers.ByteSegmentRef> results = new ArrayList<WriteBuffers.ByteSegmentRef>();
    int examined = 0;
    for (int slot = 0; slot < refs.length; ++slot) {
      long ref = refs[slot];
      if (ref == 0) {
        continue;
      }
      ++examined;
      long recOffset = getFirstRecordLengthsOffset(ref);
      long tailOffset = Ref.getOffset(ref);
      writeBuffers.setReadPoint(recOffset);
      int valueLength = (int)writeBuffers.readVLong(), keyLength = (int)writeBuffers.readVLong();
      long ptrOffset = writeBuffers.getReadPoint();
      if (Ref.hasList(ref)) {
        byteIntervals.put(recOffset, (int)(ptrOffset + 5 - recOffset));
      }
      long keyOffset = tailOffset - valueLength - keyLength;
      byte[] key = new byte[keyLength];
      WriteBuffers.ByteSegmentRef fakeRef = new WriteBuffers.ByteSegmentRef(keyOffset, keyLength);
      byteIntervals.put(keyOffset - 4, keyLength + 4);
      writeBuffers.populateValue(fakeRef);
      System.arraycopy(fakeRef.getBytes(), (int)fakeRef.getOffset(), key, 0, keyLength);
      getValueRefs(key, key.length, results);
      dump.append(Utils.toStringBinary(key, 0, key.length)).append(" ref [").append(dumpRef(ref))
        .append("]: ").append(results.size()).append(" rows\n");
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
        + ", expanded " + metricExpands + " times in " + metricExpandsUs + "us");
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
