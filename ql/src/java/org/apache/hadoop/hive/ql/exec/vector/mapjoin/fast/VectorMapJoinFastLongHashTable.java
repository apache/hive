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

import java.io.IOException;

import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashTable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;

/*
 * An single long value map optimized for vector map join.
 */
public abstract class VectorMapJoinFastLongHashTable
             extends VectorMapJoinFastHashTable
             implements VectorMapJoinLongHashTable {

  public static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastLongHashTable.class);

  private final HashTableKeyType hashTableKeyType;

  private final BinarySortableDeserializeRead keyBinarySortableDeserializeRead;

  private final boolean useMinMax;
  private long min;
  private long max;

  @Override
  public boolean useMinMax() {
    return useMinMax;
  }

  @Override
  public long min() {
    return min;
  }

  @Override
  public long max() {
    return max;
  }

  public boolean adaptPutRow(long hashCode, BytesWritable currentKey, BytesWritable currentValue)
      throws HiveException, IOException {
    byte[] keyBytes = currentKey.getBytes();
    int keyLength = currentKey.getLength();
    keyBinarySortableDeserializeRead.set(keyBytes, 0, keyLength);
    try {
      if (!keyBinarySortableDeserializeRead.readNextField()) {
        return false;
      }
    } catch (Exception e) {
      throw new HiveException("DeserializeRead details: " +
          keyBinarySortableDeserializeRead.getDetailedReadPositionString(), e);
    }
    long key = VectorMapJoinFastLongHashUtil.deserializeLongKey(keyBinarySortableDeserializeRead, hashTableKeyType);
    add(hashCode, key, currentValue);
    return true;
  }

  protected abstract void assignSlot(int slot, long key, boolean isNewKey, BytesWritable currentValue);

  public void add(long hashCode, long key, BytesWritable currentValue) {

    if (checkResize()) {
      expandAndRehash();
    }

    int slot = ((int) hashCode & logicalHashBucketMask);
    long probeSlot = slot;
    int i = 0;
    boolean isNewKey;
    while (true) {
      int pairIndex = 2 * slot;
      long valueRef = slotPairs[pairIndex];
      if (valueRef == 0) {
        // LOG.debug("VectorMapJoinFastLongHashTable add key " + key + " slot " + slot + " pairIndex " + pairIndex + " empty slot (i = " + i + ")");
        isNewKey = true;
        break;
      }
      long tableKey = slotPairs[pairIndex + 1];
      if (key == tableKey) {
        // LOG.debug("VectorMapJoinFastLongHashTable add key " + key + " slot " + slot + " pairIndex " + pairIndex + " found key (i = " + i + ")");
        isNewKey = false;
        break;
      }
      ++metricPutConflict;
      // Some other key (collision) - keep probing.
      probeSlot += (++i);
      slot = (int)(probeSlot & logicalHashBucketMask);
    }

    if (largestNumberOfSteps < i) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Probed " + i + " slots (the longest so far) to find space");
      }
      largestNumberOfSteps = i;
      // debugDumpKeyProbe(keyOffset, keyLength, hashCode, slot);
    }

    // LOG.debug("VectorMapJoinFastLongHashTable add slot " + slot + " hashCode " + Long.toHexString(hashCode));

    assignSlot(slot, key, isNewKey, currentValue);

    if (isNewKey) {
      keysAssigned++;
      if (useMinMax) {
        if (key < min) {
          min = key;
        }
        if (key > max) {
          max = key;
        }
      }
    }
  }

  private void expandAndRehash() {

    // We allocate pairs, so we cannot go above highest Integer power of 2 / 4.
    if (logicalHashBucketCount > ONE_QUARTER_LIMIT) {
      throwExpandError(ONE_QUARTER_LIMIT, "Long");
    }
    int newLogicalHashBucketCount = Math.max(FIRST_SIZE_UP, logicalHashBucketCount * 2);
    int newLogicalHashBucketMask = newLogicalHashBucketCount - 1;
    int newMetricPutConflict = 0;
    int newLargestNumberOfSteps = 0;

    int newSlotPairArraySize = newLogicalHashBucketCount * 2;
    long[] newSlotPairs = new long[newSlotPairArraySize];

    for (int slot = 0; slot < logicalHashBucketCount; slot++) {
      int pairIndex = slot * 2;
      long valueRef = slotPairs[pairIndex];
      if (valueRef != 0) {
        long tableKey = slotPairs[pairIndex + 1];

        // Copy to new slot table.
        long hashCode = HashCodeUtil.calculateLongHashCode(tableKey);
        int intHashCode = (int) hashCode;
        int newSlot = intHashCode & newLogicalHashBucketMask;
        long newProbeSlot = newSlot;
        int newPairIndex;
        int i = 0;
        while (true) {
          newPairIndex = newSlot * 2;
          long newValueRef = newSlotPairs[newPairIndex];
          if (newValueRef == 0) {
            break;
          }
          ++newMetricPutConflict;
          // Some other key (collision) - keep probing.
          newProbeSlot += (++i);
          newSlot = (int)(newProbeSlot & newLogicalHashBucketMask);
        }

        if (newLargestNumberOfSteps < i) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Probed " + i + " slots (the longest so far) to find space");
          }
          newLargestNumberOfSteps = i;
          // debugDumpKeyProbe(keyOffset, keyLength, hashCode, slot);
        }

        // Use old value reference word.
        // LOG.debug("VectorMapJoinFastLongHashTable expandAndRehash key " + tableKey + " slot " + newSlot + " newPairIndex " + newPairIndex + " empty slot (i = " + i + ")");

        newSlotPairs[newPairIndex] = valueRef;
        newSlotPairs[newPairIndex + 1] = tableKey;
      }
    }

    slotPairs = newSlotPairs;
    logicalHashBucketCount = newLogicalHashBucketCount;
    logicalHashBucketMask = newLogicalHashBucketMask;
    metricPutConflict = newMetricPutConflict;
    largestNumberOfSteps = newLargestNumberOfSteps;
    resizeThreshold = (int)(logicalHashBucketCount * loadFactor);
    metricExpands++;
  }

  protected boolean containsKey(long key) {
    long hashCode = HashCodeUtil.calculateLongHashCode(key);
    return findReadSlot(key, hashCode) != -1;
  }

  protected int findReadSlot(long key, long hashCode) {

    int intHashCode = (int) hashCode;
    int slot = intHashCode & logicalHashBucketMask;

    long probeSlot = slot;
    int i = 0;
    while (true) {
      int pairIndex = 2 * slot;
      long valueRef = slotPairs[pairIndex];
      if (valueRef == 0) {
        // Given that we do not delete, an empty slot means no match.
        return -1;
      }
      long tableKey = slotPairs[pairIndex + 1];
      if (key == tableKey) {
        return pairIndex;
      }
      // Some other key (collision) - keep probing.
      probeSlot += (++i);
      if (i > largestNumberOfSteps) {
        // We know we never went that far when we were inserting.
        return -1;
      }
      slot = (int)(probeSlot & logicalHashBucketMask);
    }
  }

  /*
   * The hash table slots.  For a long key hash table, each slot is 2 longs and the array is
   * 2X sized.
   *
   * The slot pair is 1) a non-zero reference word to the first value bytes and 2) the long value.
   */
  protected long[] slotPairs;

  private void allocateBucketArray() {
    // We allocate pairs, so we cannot go above highest Integer power of 2 / 4.
    if (logicalHashBucketCount > ONE_QUARTER_LIMIT) {
      throwExpandError(ONE_QUARTER_LIMIT, "Long");
    }
    int slotPairArraySize = 2 * logicalHashBucketCount;
    slotPairs = new long[slotPairArraySize];
  }

  public VectorMapJoinFastLongHashTable(
        boolean isFullOuter,
        boolean minMaxEnabled,
        HashTableKeyType hashTableKeyType,
        int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount, TableDesc tableDesc) {
    super(
        isFullOuter,
        initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount);
    this.hashTableKeyType = hashTableKeyType;
    PrimitiveTypeInfo[] primitiveTypeInfos = { hashTableKeyType.getPrimitiveTypeInfo() };
    keyBinarySortableDeserializeRead =
        BinarySortableDeserializeRead.with(primitiveTypeInfos, false, tableDesc.getProperties());
    allocateBucketArray();
    useMinMax = minMaxEnabled;
    min = Long.MAX_VALUE;
    max = Long.MIN_VALUE;
  }

  @Override
  public long getEstimatedMemorySize() {
    JavaDataModel jdm = JavaDataModel.get();
    long size = super.getEstimatedMemorySize();
    size += slotPairs == null ? 0 : jdm.lengthForLongArrayOfSize(slotPairs.length);
    size += (2 * jdm.primitive2());
    size += (2 * jdm.primitive1());
    size += jdm.object();
    // adding 16KB constant memory for keyBinarySortableDeserializeRead as the rabbit hole is deep to implement
    // MemoryEstimate interface, also it is constant overhead
    size += (16 * 1024L);
    return size;
  }
}
