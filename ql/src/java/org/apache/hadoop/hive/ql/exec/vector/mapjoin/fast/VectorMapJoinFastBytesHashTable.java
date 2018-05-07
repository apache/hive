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

import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashTable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.WriteBuffers;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;

import com.google.common.annotations.VisibleForTesting;

/*
 * An single byte array value hash map optimized for vector map join.
 */
public abstract class VectorMapJoinFastBytesHashTable
        extends VectorMapJoinFastHashTable
        implements VectorMapJoinBytesHashTable {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastBytesHashTable.class);

  protected VectorMapJoinFastKeyStore keyStore;

  protected BytesWritable testKeyBytesWritable;

  @Override
  public void putRow(BytesWritable currentKey, BytesWritable currentValue) throws HiveException, IOException {
    // No deserialization of key(s) here -- just get reference to bytes.
    byte[] keyBytes = currentKey.getBytes();
    int keyLength = currentKey.getLength();
    add(keyBytes, 0, keyLength, currentValue);
  }

  protected abstract void assignSlot(int slot, byte[] keyBytes, int keyStart, int keyLength,
          long hashCode, boolean isNewKey, BytesWritable currentValue);

  public void add(byte[] keyBytes, int keyStart, int keyLength, BytesWritable currentValue) {

    if (resizeThreshold <= keysAssigned) {
      expandAndRehash();
    }

    long hashCode = HashCodeUtil.murmurHash(keyBytes, keyStart, keyLength);
    int intHashCode = (int) hashCode;
    int slot = (intHashCode & logicalHashBucketMask);
    long probeSlot = slot;
    int i = 0;
    boolean isNewKey;
    while (true) {
      int tripleIndex = 3 * slot;
      if (slotTriples[tripleIndex] == 0) {
        // LOG.debug("VectorMapJoinFastBytesHashMap findWriteSlot slot " + slot + " tripleIndex " + tripleIndex + " empty");
        isNewKey = true;;
        break;
      }
      if (hashCode == slotTriples[tripleIndex + 1] &&
          keyStore.unsafeEqualKey(slotTriples[tripleIndex], keyBytes, keyStart, keyLength)) {
        // LOG.debug("VectorMapJoinFastBytesHashMap findWriteSlot slot " + slot + " tripleIndex " + tripleIndex + " existing");
        isNewKey = false;
        break;
      }
      // TODO
      ++metricPutConflict;
      // Some other key (collision) - keep probing.
      probeSlot += (++i);
      slot = (int) (probeSlot & logicalHashBucketMask);
    }

    if (largestNumberOfSteps < i) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Probed " + i + " slots (the longest so far) to find space");
      }
      largestNumberOfSteps = i;
      // debugDumpKeyProbe(keyOffset, keyLength, hashCode, slot);
    }

    assignSlot(slot, keyBytes, keyStart, keyLength, hashCode, isNewKey, currentValue);

    if (isNewKey) {
      keysAssigned++;
    }
  }

  private void expandAndRehash() {

    // We allocate triples, so we cannot go above highest Integer power of 2 / 6.
    if (logicalHashBucketCount > ONE_SIXTH_LIMIT) {
      throwExpandError(ONE_SIXTH_LIMIT, "Bytes");
    }
    int newLogicalHashBucketCount = logicalHashBucketCount * 2;
    int newLogicalHashBucketMask = newLogicalHashBucketCount - 1;
    int newMetricPutConflict = 0;
    int newLargestNumberOfSteps = 0;

    int newSlotTripleArraySize = newLogicalHashBucketCount * 3;
    long[] newSlotTriples = new long[newSlotTripleArraySize];

    for (int slot = 0; slot < logicalHashBucketCount; slot++) {
      int tripleIndex = slot * 3;
      long keyRef = slotTriples[tripleIndex];
      if (keyRef != 0) {
        long hashCode = slotTriples[tripleIndex + 1];
        long valueRef = slotTriples[tripleIndex + 2];

        // Copy to new slot table.
        int intHashCode = (int) hashCode;
        int newSlot = intHashCode & newLogicalHashBucketMask;
        long newProbeSlot = newSlot;
        int newTripleIndex;
        int i = 0;
        while (true) {
          newTripleIndex = newSlot * 3;
          long newKeyRef = newSlotTriples[newTripleIndex];
          if (newKeyRef == 0) {
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

        newSlotTriples[newTripleIndex] = keyRef;
        newSlotTriples[newTripleIndex + 1] = hashCode;
        newSlotTriples[newTripleIndex + 2] = valueRef;
      }
    }

    slotTriples = newSlotTriples;
    logicalHashBucketCount = newLogicalHashBucketCount;
    logicalHashBucketMask = newLogicalHashBucketMask;
    metricPutConflict = newMetricPutConflict;
    largestNumberOfSteps = newLargestNumberOfSteps;
    resizeThreshold = (int)(logicalHashBucketCount * loadFactor);
    metricExpands++;
    // LOG.debug("VectorMapJoinFastLongHashTable expandAndRehash new logicalHashBucketCount " + logicalHashBucketCount + " resizeThreshold " + resizeThreshold + " metricExpands " + metricExpands);
  }

  protected final long findReadSlot(
      byte[] keyBytes, int keyStart, int keyLength, long hashCode, WriteBuffers.Position readPos) {

    int intHashCode = (int) hashCode;
    int slot = (intHashCode & logicalHashBucketMask);
    long probeSlot = slot;
    int i = 0;
    while (true) {
      int tripleIndex = slot * 3;
      // LOG.debug("VectorMapJoinFastBytesHashMap findReadSlot slot keyRefWord " + Long.toHexString(slotTriples[tripleIndex]) + " hashCode " + Long.toHexString(hashCode) + " entry hashCode " + Long.toHexString(slotTriples[tripleIndex + 1]) + " valueRefWord " + Long.toHexString(slotTriples[tripleIndex + 2]));
      if (slotTriples[tripleIndex] == 0) {
        // Given that we do not delete, an empty slot means no match.
        return -1;
      } else if (hashCode == slotTriples[tripleIndex + 1]) {
        // Finally, verify the key bytes match.

        if (keyStore.equalKey(slotTriples[tripleIndex], keyBytes, keyStart, keyLength, readPos)) {
          return slotTriples[tripleIndex + 2];
        }
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
   * The hash table slots.  For a bytes key hash table, each slot is 3 longs and the array is
   * 3X sized.
   *
   * The slot triple is 1) a non-zero reference word to the key bytes, 2) the key hash code, and
   * 3) a non-zero reference word to the first value bytes.
   */
  protected long[] slotTriples;

  private void allocateBucketArray() {
    // We allocate triples, so we cannot go above highest Integer power of 2 / 6.
    if (logicalHashBucketCount > ONE_SIXTH_LIMIT) {
      throwExpandError(ONE_SIXTH_LIMIT, "Bytes");
    }
    int slotTripleArraySize = 3 * logicalHashBucketCount;
    slotTriples = new long[slotTripleArraySize];
  }

  public VectorMapJoinFastBytesHashTable(
        int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount) {
    super(initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount);
    allocateBucketArray();
  }

  @Override
  public long getEstimatedMemorySize() {
    return super.getEstimatedMemorySize() + JavaDataModel.get().lengthForLongArrayOfSize(slotTriples.length);
  }
}
