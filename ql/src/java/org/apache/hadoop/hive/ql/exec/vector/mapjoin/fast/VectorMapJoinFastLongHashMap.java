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

import org.apache.hadoop.hive.common.MemoryEstimate;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinNonMatchedIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.serde2.WriteBuffers;
import org.apache.hadoop.hive.serde2.WriteBuffers.ByteSegmentRef;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;

import com.google.common.annotations.VisibleForTesting;

/*
 * An single LONG key hash map optimized for vector map join.
 */
public class VectorMapJoinFastLongHashMap
             extends VectorMapJoinFastLongHashTable
             implements VectorMapJoinLongHashMap, MemoryEstimate {

  // public static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastLongHashMap.class);

  protected VectorMapJoinFastValueStore valueStore;

  private BytesWritable testValueBytesWritable;

  private long fullOuterNullKeyValueRef;

  public static class NonMatchedLongHashMapIterator extends VectorMapJoinFastNonMatchedIterator {

    private VectorMapJoinFastLongHashMap hashMap;

    private boolean noMore;
    private boolean keyIsNull;

    private WriteBuffers.Position nonMatchedReadPos;

    private ByteSegmentRef nonMatchedKeyByteSegmentRef;

    private VectorMapJoinFastValueStore.HashMapResult nonMatchedHashMapResult;

    public NonMatchedLongHashMapIterator(MatchTracker matchTracker,
        VectorMapJoinFastLongHashMap hashMap) {
      super(matchTracker);
      this.hashMap = hashMap;
    }

    @Override
    public void init() {
      super.init();
      noMore = false;
      keyIsNull = false;
      nonMatchedHashMapResult = new VectorMapJoinFastValueStore.HashMapResult();
    }

    @Override
    public boolean findNextNonMatched() {
      if (noMore) {
        return false;
      }
      while (true) {
        nonMatchedLogicalSlotNum++;
        if (nonMatchedLogicalSlotNum >= hashMap.logicalHashBucketCount){

          // Fall below and handle Small Table NULL key.
          break;
        }
        final int nonMatchedDoubleIndex = nonMatchedLogicalSlotNum * 2;
        if (hashMap.slotPairs[nonMatchedDoubleIndex] != 0) {
          if (!matchTracker.wasMatched(nonMatchedLogicalSlotNum)) {
            nonMatchedHashMapResult.set(
                hashMap.valueStore, hashMap.slotPairs[nonMatchedDoubleIndex]);
            keyIsNull = false;
            return true;
          }
        }
      }

      // Do we have a Small Table NULL Key?
      if (hashMap.fullOuterNullKeyValueRef == 0) {
        return false;
      }
      nonMatchedHashMapResult.set(
          hashMap.valueStore, hashMap.fullOuterNullKeyValueRef);
      noMore = true;
      keyIsNull = true;
      return true;
    }

    @Override
    public boolean readNonMatchedLongKey() {
      return !keyIsNull;
    }

    @Override
    public long getNonMatchedLongKey() {
      return hashMap.slotPairs[nonMatchedLogicalSlotNum * 2 + 1];
    }

    @Override
    public VectorMapJoinHashMapResult getNonMatchedHashMapResult() {
      return nonMatchedHashMapResult;
    }
  }

  @Override
  public VectorMapJoinHashMapResult createHashMapResult() {
    return new VectorMapJoinFastValueStore.HashMapResult();
  }

  @Override
  public VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker) {
    return new NonMatchedLongHashMapIterator(matchTracker, this);
  }

  @Override
  public void putRow(long hashCode, BytesWritable currentKey, BytesWritable currentValue)
      throws HiveException, IOException {

    if (!adaptPutRow(hashCode, currentKey, currentValue)) {

      // Ignore NULL keys, except for FULL OUTER.
      if (isFullOuter) {
        addFullOuterNullKeyValue(currentValue);
      }

    }
  }

  @Override
  public boolean containsLongKey(long currentKey) {
    return containsKey(currentKey);
  }

  /*
   * A Unit Test convenience method for putting key and value into the hash table using the
   * actual types.
   */
  @VisibleForTesting
  public void testPutRow(long currentKey, byte[] currentValue) throws HiveException, IOException {
    if (testValueBytesWritable == null) {
      testValueBytesWritable = new BytesWritable();
    }
    testValueBytesWritable.set(currentValue, 0, currentValue.length);
    long hashCode = HashCodeUtil.calculateLongHashCode(currentKey);
    add(hashCode, currentKey, testValueBytesWritable);
  }

  @Override
  public void assignSlot(int slot, long key, boolean isNewKey, BytesWritable currentValue) {

    byte[] valueBytes = currentValue.getBytes();
    int valueLength = currentValue.getLength();

    int pairIndex = 2 * slot;
    if (isNewKey) {
      // First entry.
      slotPairs[pairIndex] = valueStore.addFirst(valueBytes, 0, valueLength);
      slotPairs[pairIndex + 1] = key;
    } else {
      // Add another value.
      slotPairs[pairIndex] = valueStore.addMore(slotPairs[pairIndex], valueBytes, 0, valueLength);
    }
  }

  @Override
  public JoinUtil.JoinResult lookup(long key, VectorMapJoinHashMapResult hashMapResult) {

    VectorMapJoinFastValueStore.HashMapResult optimizedHashMapResult =
        (VectorMapJoinFastValueStore.HashMapResult) hashMapResult;

    optimizedHashMapResult.forget();

    long hashCode = HashCodeUtil.calculateLongHashCode(key);
    int pairIndex = findReadSlot(key, hashCode);
    JoinUtil.JoinResult joinResult;
    if (pairIndex == -1) {
      joinResult = JoinUtil.JoinResult.NOMATCH;
    } else {
      optimizedHashMapResult.set(valueStore, slotPairs[pairIndex]);

      joinResult = JoinUtil.JoinResult.MATCH;
    }

    optimizedHashMapResult.setJoinResult(joinResult);

    return joinResult;
  }

  @Override
  public JoinUtil.JoinResult lookup(long key, VectorMapJoinHashMapResult hashMapResult,
      MatchTracker matchTracker) {

    VectorMapJoinFastValueStore.HashMapResult optimizedHashMapResult =
        (VectorMapJoinFastValueStore.HashMapResult) hashMapResult;

    optimizedHashMapResult.forget();

    long hashCode = HashCodeUtil.calculateLongHashCode(key);
    int pairIndex = findReadSlot(key, hashCode);
    JoinUtil.JoinResult joinResult;
    if (pairIndex == -1) {
      joinResult = JoinUtil.JoinResult.NOMATCH;
    } else {
      if (matchTracker != null) {
        matchTracker.trackMatch(pairIndex / 2);
      }
      optimizedHashMapResult.set(valueStore, slotPairs[pairIndex]);

      joinResult = JoinUtil.JoinResult.MATCH;
    }

    optimizedHashMapResult.setJoinResult(joinResult);

    return joinResult;
  }

  public void addFullOuterNullKeyValue(BytesWritable currentValue) {

    byte[] valueBytes = currentValue.getBytes();
    int valueLength = currentValue.getLength();

    if (fullOuterNullKeyValueRef == 0) {
      fullOuterNullKeyValueRef = valueStore.addFirst(valueBytes, 0, valueLength);
    } else {

      // Add another value.
      fullOuterNullKeyValueRef =
          valueStore.addMore(fullOuterNullKeyValueRef, valueBytes, 0, valueLength);
    }
  }

  public VectorMapJoinFastLongHashMap(
      boolean isFullOuter,
      boolean minMaxEnabled,
      HashTableKeyType hashTableKeyType,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount, TableDesc tableDesc) {
    super(
        isFullOuter, minMaxEnabled, hashTableKeyType,
        initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount, tableDesc);
    valueStore = new VectorMapJoinFastValueStore(writeBuffersSize);
    fullOuterNullKeyValueRef = 0;
  }

  @Override
  public long getEstimatedMemorySize() {
    return super.getEstimatedMemorySize() + valueStore.getEstimatedMemorySize();
  }
}
