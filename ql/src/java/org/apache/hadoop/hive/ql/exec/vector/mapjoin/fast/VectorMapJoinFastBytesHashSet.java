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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashSetResult;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;

/*
 * An bytes key hash set optimized for vector map join.
 *
 * This is the abstract base for the multi-key and string bytes key hash set implementations.
 */
public abstract class VectorMapJoinFastBytesHashSet
        extends VectorMapJoinFastBytesHashTable
        implements VectorMapJoinBytesHashSet {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastBytesHashSet.class);

  private VectorMapJoinFastBytesHashSetStore hashSetStore;

  @Override
  public VectorMapJoinHashSetResult createHashSetResult() {
    return new VectorMapJoinFastBytesHashSetStore.HashSetResult();
  }

  public void add(byte[] keyBytes, int keyStart, int keyLength, BytesWritable currentValue, long hashCode) {

    if (checkResize()) {
      expandAndRehash();
    }

    int slot = ((int) hashCode & logicalHashBucketMask);
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

    if (isNewKey) {
      slots[slot] =
          hashSetStore.add(
              partialHashCode, keyBytes, keyStart, keyLength);
      keysAssigned++;
    } else {

      // Key already exists -- do nothing.
    }
  }

  @Override
  public JoinUtil.JoinResult contains(byte[] keyBytes, int keyStart, int keyLength,
          VectorMapJoinHashSetResult hashSetResult) {

    VectorMapJoinFastBytesHashSetStore.HashSetResult fastHashSetResult =
        (VectorMapJoinFastBytesHashSetStore.HashSetResult) hashSetResult;

    fastHashSetResult.forget();

    long hashCode = HashCodeUtil.murmurHash(keyBytes, keyStart, keyLength);

    doHashSetContains(
        keyBytes, keyStart, keyLength, hashCode, fastHashSetResult);

    return fastHashSetResult.joinResult();
  }

  protected final void doHashSetContains(
      byte[] keyBytes, int keyStart, int keyLength, long hashCode,
      VectorMapJoinFastBytesHashSetStore.HashSetResult fastHashSetResult) {

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

        // Finally, verify the key bytes match and implicitly remember the set existence in
        // fastHashSetResult.
        fastHashSetResult.setKey(hashSetStore, refWord);
        if (fastHashSetResult.equalKey(keyBytes, keyStart, keyLength)) {
          fastHashSetResult.setContains();
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

  public VectorMapJoinFastBytesHashSet(
      boolean isFullOuter,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount) {
    super(
        isFullOuter,
        initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount);
    hashSetStore = new VectorMapJoinFastBytesHashSetStore(writeBuffersSize);
    writeBuffers = hashSetStore.getWriteBuffers();
  }

  @Override
  public long getEstimatedMemorySize() {
    long size = super.getEstimatedMemorySize();
    size += hashSetStore.getEstimatedMemorySize();
    return size;
  }
}
