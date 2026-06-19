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

/*
 * An single byte array value hash map optimized for vector map join.
 */
public abstract class VectorMapJoinFastBytesHashTable
        extends VectorMapJoinFastHashTable
        implements VectorMapJoinBytesHashTable {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastBytesHashTable.class);

  protected WriteBuffers writeBuffers;

  protected WriteBuffers.Position unsafeReadPos; // Thread-unsafe position used at write time.

  protected BytesWritable testKeyBytesWritable;

  @Override
  public void putRow(long hashCode, BytesWritable currentKey, BytesWritable currentValue)
      throws HiveException, IOException {
    // No deserialization of key(s) here -- just get reference to bytes.
    byte[] keyBytes = currentKey.getBytes();
    int keyLength = currentKey.getLength();
    add(keyBytes, 0, keyLength, currentValue, hashCode);
  }

  @Override
  public boolean containsLongKey(long currentKey) {
    // Only supported for Long-Hash implementations
    throw new RuntimeException("Not supported yet!");
  }

  public abstract void add(byte[] keyBytes, int keyStart, int keyLength,
      BytesWritable currentValue, long hashCode);

  protected void expandAndRehash() {

    // We cannot go above highest Integer power of 2.
    if (logicalHashBucketCount > HIGHEST_INT_POWER_OF_2) {
      throwExpandError(HIGHEST_INT_POWER_OF_2, "Bytes");
    }
    final int newLogicalHashBucketCount = Math.max(FIRST_SIZE_UP, logicalHashBucketCount * 2);
    final int newLogicalHashBucketMask = newLogicalHashBucketCount - 1;
    int newMetricPutConflict = 0;
    int newLargestNumberOfSteps = 0;

    long[] newSlots = new long[newLogicalHashBucketCount];

    for (int slot = 0; slot < logicalHashBucketCount; slot++) {
      final long refWord = slots[slot];
      if (refWord != 0) {
        final long hashCode =
            VectorMapJoinFastBytesHashKeyRef.calculateHashCode(
                refWord, writeBuffers, unsafeReadPos);

        // Copy to new slot table.
        int intHashCode = (int) hashCode;
        int newSlot = intHashCode & newLogicalHashBucketMask;
        long newProbeSlot = newSlot;
        int i = 0;
        while (true) {
          if (newSlots[newSlot] == 0) {
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

        // Use old reference word.
        newSlots[newSlot] = refWord;
      }
    }

    slots = newSlots;
    logicalHashBucketCount = newLogicalHashBucketCount;
    logicalHashBucketMask = newLogicalHashBucketMask;
    metricPutConflict = newMetricPutConflict;
    largestNumberOfSteps = newLargestNumberOfSteps;
    resizeThreshold = (int)(logicalHashBucketCount * loadFactor);
    metricExpands++;
  }

  /*
   * The hash table slots for fast HashMap.
   */
  protected long[] slots;

  private void allocateBucketArray() {

    // We cannot go above highest Integer power of 2.
    if (logicalHashBucketCount > HIGHEST_INT_POWER_OF_2) {
      throwExpandError(HIGHEST_INT_POWER_OF_2, "Bytes");
    }
    slots = new long[logicalHashBucketCount];
  }

  public VectorMapJoinFastBytesHashTable(
      boolean isFullOuter,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount) {
    super(
        isFullOuter,
        initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount);
    unsafeReadPos = new WriteBuffers.Position();
    allocateBucketArray();
  }

  @Override
  public long getEstimatedMemorySize() {
    long size = 0;
    size += super.getEstimatedMemorySize();
    size += unsafeReadPos == null ? 0 : unsafeReadPos.getEstimatedMemorySize();
    size += JavaDataModel.get().lengthForLongArrayOfSize(slots.length);
    return size;
  }
}
