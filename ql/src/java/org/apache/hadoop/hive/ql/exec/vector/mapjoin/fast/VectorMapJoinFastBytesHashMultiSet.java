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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMultiSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMultiSetResult;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;

import com.google.common.annotations.VisibleForTesting;

/*
 * An bytes key hash multi-set optimized for vector map join.
 *
 * This is the abstract base for the multi-key and string bytes key hash multi-set implementations.
 */
public abstract class VectorMapJoinFastBytesHashMultiSet
        extends VectorMapJoinFastBytesHashTable
        implements VectorMapJoinBytesHashMultiSet {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastBytesHashMultiSet.class);

  private VectorMapJoinFastBytesHashMultiSetStore hashMultiSetStore;

  @Override
  public VectorMapJoinHashMultiSetResult createHashMultiSetResult() {
    return new VectorMapJoinFastBytesHashMultiSetStore.HashMultiSetResult();
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
          hashMultiSetStore.addFirst(
              partialHashCode, keyBytes, keyStart, keyLength);
      keysAssigned++;
    } else {
      final long newRefWord =
          hashMultiSetStore.bumpCount(
              refWord, unsafeReadPos);
      if (newRefWord != refWord) {
        slots[slot] = newRefWord;
      }
    }
  }

  @Override
  public JoinUtil.JoinResult contains(byte[] keyBytes, int keyStart, int keyLength,
          VectorMapJoinHashMultiSetResult hashMultiSetResult) {

    VectorMapJoinFastBytesHashMultiSetStore.HashMultiSetResult fastHashMultiSetResult =
        (VectorMapJoinFastBytesHashMultiSetStore.HashMultiSetResult) hashMultiSetResult;

    fastHashMultiSetResult.forget();

    long hashCode = HashCodeUtil.murmurHash(keyBytes, keyStart, keyLength);

    doHashMultiSetContains(
        keyBytes, keyStart, keyLength, hashCode, fastHashMultiSetResult);

    return fastHashMultiSetResult.joinResult();
  }

  protected final void doHashMultiSetContains(
      byte[] keyBytes, int keyStart, int keyLength, long hashCode,
      VectorMapJoinFastBytesHashMultiSetStore.HashMultiSetResult fastHashMultiSetResult) {

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

        // Finally, verify the key bytes match and remember the set membership count in
        // fastHashMultiSetResult.
        fastHashMultiSetResult.setKey(hashMultiSetStore, refWord);
        if (fastHashMultiSetResult.equalKey(keyBytes, keyStart, keyLength)) {
          fastHashMultiSetResult.setContains();
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

  public VectorMapJoinFastBytesHashMultiSet(
      boolean isFullOuter,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount) {
    super(
        isFullOuter,
        initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount);
    hashMultiSetStore = new VectorMapJoinFastBytesHashMultiSetStore(writeBuffersSize);
    writeBuffers = hashMultiSetStore.getWriteBuffers();
  }

  @Override
  public long getEstimatedMemorySize() {
    long size = super.getEstimatedMemorySize();
    size += hashMultiSetStore.getEstimatedMemorySize();
    return size;
  }
}
