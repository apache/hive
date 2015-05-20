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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashMap;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.io.BytesWritable;

/*
 * An single long value map optimized for vector map join.
 */
public class VectorMapJoinFastLongHashMap
             extends VectorMapJoinFastLongHashTable
             implements VectorMapJoinLongHashMap {

  public static final Log LOG = LogFactory.getLog(VectorMapJoinFastLongHashMap.class);

  protected VectorMapJoinFastValueStore valueStore;

  @Override
  public VectorMapJoinHashMapResult createHashMapResult() {
    return new VectorMapJoinFastValueStore.HashMapResult();
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

    long hashCode = VectorMapJoinFastLongHashUtil.hashKey(key);
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
      int initialCapacity, float loadFactor, int writeBuffersSize) {
    super(minMaxEnabled, isOuterJoin, hashTableKeyType,
        initialCapacity, loadFactor, writeBuffersSize);
    valueStore = new VectorMapJoinFastValueStore(writeBuffersSize);
  }
}