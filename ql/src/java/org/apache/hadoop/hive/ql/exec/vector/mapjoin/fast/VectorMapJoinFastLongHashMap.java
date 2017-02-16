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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashMap;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;

import com.google.common.annotations.VisibleForTesting;

/*
 * An single LONG key hash map optimized for vector map join.
 */
public class VectorMapJoinFastLongHashMap
             extends VectorMapJoinFastLongHashTable
             implements VectorMapJoinLongHashMap {

  public static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastLongHashMap.class);

  protected VectorMapJoinFastValueStore valueStore;

  private BytesWritable testValueBytesWritable;

  @Override
  public VectorMapJoinHashMapResult createHashMapResult() {
    return new VectorMapJoinFastValueStore.HashMapResult();
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
    add(currentKey, testValueBytesWritable);
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
    // LOG.debug("VectorMapJoinFastLongHashMap lookup " + key + " hashCode " + hashCode);
    long valueRef = findReadSlot(key, hashCode);
    JoinUtil.JoinResult joinResult;
    if (valueRef == -1) {
      joinResult = JoinUtil.JoinResult.NOMATCH;
    } else {
      optimizedHashMapResult.set(valueStore, valueRef);

      joinResult = JoinUtil.JoinResult.MATCH;
    }

    optimizedHashMapResult.setJoinResult(joinResult);

    return joinResult;
  }

  public VectorMapJoinFastLongHashMap(
      boolean minMaxEnabled, boolean isOuterJoin, HashTableKeyType hashTableKeyType,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount) {
    super(minMaxEnabled, isOuterJoin, hashTableKeyType,
        initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount);
    valueStore = new VectorMapJoinFastValueStore(writeBuffersSize);
  }
}
