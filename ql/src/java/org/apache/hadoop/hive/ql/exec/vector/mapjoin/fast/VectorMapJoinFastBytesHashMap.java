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
import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinNonMatchedIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.WriteBuffers;
import org.apache.hadoop.hive.serde2.WriteBuffers.ByteSegmentRef;
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

  private long fullOuterNullKeyRefWord;

  public static class NonMatchedBytesHashMapIterator extends VectorMapJoinFastNonMatchedIterator {

    private VectorMapJoinFastBytesHashMap hashMap;

    private boolean noMore;
    private boolean keyIsNull;

    private WriteBuffers.Position nonMatchedReadPos;

    private ByteSegmentRef nonMatchedKeyByteSegmentRef;

    private VectorMapJoinFastBytesHashMapStore.HashMapResult nonMatchedHashMapResult;

    NonMatchedBytesHashMapIterator(MatchTracker matchTracker,
        VectorMapJoinFastBytesHashMap hashMap) {
      super(matchTracker);
      this.hashMap = hashMap;
    }

    @Override
    public void init() {
      super.init();
      noMore = false;
      keyIsNull = false;
      nonMatchedReadPos = new WriteBuffers.Position();
      nonMatchedKeyByteSegmentRef = new ByteSegmentRef();
      nonMatchedHashMapResult = new VectorMapJoinFastBytesHashMapStore.HashMapResult();
    }

    @Override
    public boolean findNextNonMatched() {
      if (noMore) {
        return false;
      }
      while (true) {
        nonMatchedLogicalSlotNum++;
        if (nonMatchedLogicalSlotNum >= hashMap.logicalHashBucketCount) {

          // Fall below and handle Small Table NULL key.
          break;
        }
        final long refWord = hashMap.slots[nonMatchedLogicalSlotNum];
        if (refWord != 0) {
          if (!matchTracker.wasMatched(nonMatchedLogicalSlotNum)) {
            nonMatchedHashMapResult.set(hashMap.hashMapStore, refWord);
            keyIsNull = false;
            return true;
          }
        }
      }

      // Do we have a Small Table NULL Key?
      if (hashMap.fullOuterNullKeyRefWord == 0) {
        return false;
      }
      nonMatchedHashMapResult.set(hashMap.hashMapStore, hashMap.fullOuterNullKeyRefWord);
      noMore = true;
      keyIsNull = true;
      return true;
    }

    @Override
    public boolean readNonMatchedBytesKey() throws HiveException {
      if (keyIsNull) {
        return false;
      }
      hashMap.hashMapStore.getKey(
          hashMap.slots[nonMatchedLogicalSlotNum],
          nonMatchedKeyByteSegmentRef,
          nonMatchedReadPos);
      return true;
    }

    @Override
    public byte[] getNonMatchedBytes() {
      return nonMatchedKeyByteSegmentRef.getBytes();
    }

    @Override
    public int getNonMatchedBytesOffset() {
      return (int) nonMatchedKeyByteSegmentRef.getOffset();
    }

    @Override
    public int getNonMatchedBytesLength() {
      return nonMatchedKeyByteSegmentRef.getLength();
    }

    @Override
    public VectorMapJoinHashMapResult getNonMatchedHashMapResult() {
      return nonMatchedHashMapResult;
    }
  }

  @Override
  public VectorMapJoinHashMapResult createHashMapResult() {
    return new VectorMapJoinFastBytesHashMapStore.HashMapResult();
  }

  @Override
  public VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker) {
    return new NonMatchedBytesHashMapIterator(matchTracker, this);
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

  @Override
  public JoinUtil.JoinResult lookup(byte[] keyBytes, int keyStart, int keyLength,
      VectorMapJoinHashMapResult hashMapResult, MatchTracker matchTracker) {

    VectorMapJoinFastBytesHashMapStore.HashMapResult fastHashMapResult =
         (VectorMapJoinFastBytesHashMapStore.HashMapResult) hashMapResult;

    fastHashMapResult.forget();

    long hashCode = HashCodeUtil.murmurHash(keyBytes, keyStart, keyLength);

    final int slot =
        doHashMapMatch(
            keyBytes, keyStart, keyLength, hashCode, fastHashMapResult);
    if (slot != -1 && matchTracker != null) {
      matchTracker.trackMatch(slot);
    }

    return fastHashMapResult.joinResult();
  }

  protected final int doHashMapMatch(
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
        return -1;
      } else if (
          VectorMapJoinFastBytesHashKeyRef.getPartialHashCodeFromRefWord(refWord) ==
              partialHashCode) {

        // Finally, verify the key bytes match and remember read positions, etc in
        // fastHashMapResult.
        fastHashMapResult.setKey(hashMapStore, refWord);
        if (fastHashMapResult.equalKey(keyBytes, keyStart, keyLength)) {
          fastHashMapResult.setMatch();
          return slot;
        }
      }
      // Some other key (collision) - keep probing.
      probeSlot += (++i);
      if (i > largestNumberOfSteps) {
        // We know we never went that far when we were inserting.
        return -1;
      }
      slot = (int) (probeSlot & logicalHashBucketMask);
    }
  }

  private static final byte[] EMPTY_BYTES = new byte[0];

  public void addFullOuterNullKeyValue(BytesWritable currentValue) {

    byte[] valueBytes = currentValue.getBytes();
    int valueLength = currentValue.getLength();

    if (fullOuterNullKeyRefWord == 0) {
      fullOuterNullKeyRefWord =
          hashMapStore.addFirst(
              /* partialHashCode */ 0, EMPTY_BYTES, 0, 0,
              valueBytes, 0, valueLength);
    } else {

      // Add another value.
      fullOuterNullKeyRefWord =
          hashMapStore.addMore(
              fullOuterNullKeyRefWord, valueBytes, 0, valueLength, unsafeReadPos);
    }
  }

  public VectorMapJoinFastBytesHashMap(
      boolean isFullOuter,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount) {
    super(
        isFullOuter,
        initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount);
    fullOuterNullKeyRefWord = 0;
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
