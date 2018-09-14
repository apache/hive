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

import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * An bytes key hash map optimized for vector map join.
 *
 * This is the abstract base for the multi-key and string bytes key hash map implementations.
 */
public abstract class VectorMapJoinFastBytesHashMap
        extends VectorMapJoinFastBytesHashTable
        implements VectorMapJoinBytesHashMap {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastBytesHashMap.class);

  private VectorMapJoinFastBytesHashMapStore hashMapStore;

  protected BytesWritable testValueBytesWritable;

  @Override
  public VectorMapJoinHashMapResult createHashMapResult() {
    return new VectorMapJoinFastBytesHashMapStore.HashMapResult();
  }

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
    long refWord;
    final long partialHashCode =
        VectorMapJoinFastBytesHashKeyRef.extractPartialHashCode(hashCode);
    while (true) {
      refWord = slots[slot];
      if (refWord == 0) {
        isNewKey = true;
        break;
      }
      if (VectorMapJoinFastBytesHashKeyRef.getPartialHashCodeFromRefWord(refWord) ==
              partialHashCode &&
          VectorMapJoinFastBytesHashKeyRef.equalKey(
                  refWord, keyBytes, keyStart, keyLength, writeBuffers, unsafeReadPos)) {
        isNewKey = false;
        break;
      }
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

    byte[] valueBytes = currentValue.getBytes();
    int valueLength = currentValue.getLength();

    if (isNewKey) {
      slots[slot] =
          hashMapStore.addFirst(
              partialHashCode, keyBytes, keyStart, keyLength, valueBytes, 0, valueLength);
      keysAssigned++;
    } else {
      final long newRefWord =
          hashMapStore.addMore(
              refWord, valueBytes, 0, valueLength, unsafeReadPos);
      if (newRefWord != refWord) {
        slots[slot] = newRefWord;
      }
    }
  }

  @Override
  public JoinUtil.JoinResult lookup(byte[] keyBytes, int keyStart, int keyLength,
      VectorMapJoinHashMapResult hashMapResult) {

    VectorMapJoinFastBytesHashMapStore.HashMapResult fastHashMapResult =
         (VectorMapJoinFastBytesHashMapStore.HashMapResult) hashMapResult;

    fastHashMapResult.forget();

    long hashCode = HashCodeUtil.murmurHash(keyBytes, keyStart, keyLength);

    doHashMapMatch(
        keyBytes, keyStart, keyLength, hashCode, fastHashMapResult);

    return fastHashMapResult.joinResult();
  }

  protected final void doHashMapMatch(
      byte[] keyBytes, int keyStart, int keyLength, long hashCode,
      VectorMapJoinFastBytesHashMapStore.HashMapResult fastHashMapResult) {

    int intHashCode = (int) hashCode;
    int slot = (intHashCode & logicalHashBucketMask);
    long probeSlot = slot;
    int i = 0;
    final long partialHashCode =
        VectorMapJoinFastBytesHashKeyRef.extractPartialHashCode(hashCode);
    while (true) {
      final long refWord = slots[slot];
      if (refWord == 0) {

        // Given that we do not delete, an empty slot means no match.
        return;
      } else if (
          VectorMapJoinFastBytesHashKeyRef.getPartialHashCodeFromRefWord(refWord) ==
              partialHashCode) {

        // Finally, verify the key bytes match and remember read positions, etc in
        // fastHashMapResult.
        fastHashMapResult.setKey(hashMapStore, refWord);
        if (fastHashMapResult.equalKey(keyBytes, keyStart, keyLength)) {
          fastHashMapResult.setMatch();
          return;
        }
      }
      // Some other key (collision) - keep probing.
      probeSlot += (++i);
      if (i > largestNumberOfSteps) {
        // We know we never went that far when we were inserting.
        return;
      }
      slot = (int) (probeSlot & logicalHashBucketMask);
    }
  }

  public VectorMapJoinFastBytesHashMap(
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount) {
    super(initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount);
    hashMapStore = new VectorMapJoinFastBytesHashMapStore(writeBuffersSize);
    writeBuffers = hashMapStore.getWriteBuffers();
  }

  @Override
  public long getEstimatedMemorySize() {
    long size = super.getEstimatedMemorySize();
    size += hashMapStore.getEstimatedMemorySize();
    return size;
  }
}
