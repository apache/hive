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
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMultiSetResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashMultiSet;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;

import com.google.common.annotations.VisibleForTesting;

/*
 * An single LONG key hash multi-set optimized for vector map join.
 */
public class VectorMapJoinFastLongHashMultiSet
             extends VectorMapJoinFastLongHashTable
             implements VectorMapJoinLongHashMultiSet {

  public static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastLongHashMultiSet.class);

  @Override
  public VectorMapJoinHashMultiSetResult createHashMultiSetResult() {
    return new VectorMapJoinFastHashMultiSet.HashMultiSetResult();
  }

  /*
   * A Unit Test convenience method for putting the key into the hash table using the
   * actual type.
   */
  @VisibleForTesting
  public void testPutRow(long currentKey) throws HiveException, IOException {
    add(currentKey, null);
  }

  @Override
  public void assignSlot(int slot, long key, boolean isNewKey, BytesWritable currentValue) {

    int pairIndex = 2 * slot;
    if (isNewKey) {
      // First entry.
      slotPairs[pairIndex] = 1;    // Count.
      slotPairs[pairIndex + 1] = key;
    } else {
      // Add another value.
      slotPairs[pairIndex]++;
    }
  }


  @Override
  public JoinUtil.JoinResult contains(long key, VectorMapJoinHashMultiSetResult hashMultiSetResult) {

    VectorMapJoinFastHashMultiSet.HashMultiSetResult optimizedHashMultiSetResult =
        (VectorMapJoinFastHashMultiSet.HashMultiSetResult) hashMultiSetResult;

    optimizedHashMultiSetResult.forget();

    long hashCode = HashCodeUtil.calculateLongHashCode(key);
    long count = findReadSlot(key, hashCode);
    JoinUtil.JoinResult joinResult;
    if (count == -1) {
      joinResult = JoinUtil.JoinResult.NOMATCH;
    } else {
      optimizedHashMultiSetResult.set(count);
      joinResult = JoinUtil.JoinResult.MATCH;
    }

    optimizedHashMultiSetResult.setJoinResult(joinResult);

    return joinResult;
  }

  public VectorMapJoinFastLongHashMultiSet(
      boolean minMaxEnabled, boolean isOuterJoin, HashTableKeyType hashTableKeyType,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount) {
    super(minMaxEnabled, isOuterJoin, hashTableKeyType,
        initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount);
  }

  @Override
  public long getEstimatedMemorySize() {
    return super.getEstimatedMemorySize();
  }
}
