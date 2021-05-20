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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable;

import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKind;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/*
 * Interface for a vector map join hash table (which could be a hash map, hash multi-set, or
 * hash set) for a single long.
 */
public class VectorMapJoinFastHashTableParallel implements VectorMapJoinHashTable {

  public static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastHashTableParallel.class);

  private final VectorMapJoinFastHashTableWrapper vectorMapJoinFastHashTableWrapper;

  private int numThreads;

  public VectorMapJoinFastHashTableParallel(HashTableKeyType hashTableKeyType, HashTableKind hashTableKind,
      boolean isFullOuter, boolean minMaxEnabled, int newThreshold, float loadFactor, int writeBufferSize,
      long estimatedKeyCount, TableDesc tableDesc, int numThreads) {
    VectorMapJoinFastHashTableWrapper vectorMapJoinFastHashTableWrapperTemp;
    vectorMapJoinFastHashTableWrapperTemp = null;
    switch (hashTableKeyType) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case DATE:
    case LONG:
      switch (hashTableKind) {
      case HASH_MAP:
        vectorMapJoinFastHashTableWrapperTemp = new VectorMapJoinFastLongHashMapParallel(isFullOuter, minMaxEnabled,
            hashTableKeyType, newThreshold, loadFactor, writeBufferSize, estimatedKeyCount/numThreads,
            tableDesc, numThreads);
        break;
      case HASH_MULTISET:
        vectorMapJoinFastHashTableWrapperTemp = new VectorMapJoinFastLongHashMultiSetParallel(isFullOuter, minMaxEnabled,
            hashTableKeyType, newThreshold, loadFactor, writeBufferSize, estimatedKeyCount/numThreads,
            tableDesc, numThreads);
        break;
      case HASH_SET:
        vectorMapJoinFastHashTableWrapperTemp = new VectorMapJoinFastLongHashSetParallel(isFullOuter, minMaxEnabled,
            hashTableKeyType, newThreshold, loadFactor, writeBufferSize, estimatedKeyCount/numThreads,
            tableDesc, numThreads);
        break;
      }
      break;

    case STRING:
      switch (hashTableKind) {
      case HASH_MAP:
        vectorMapJoinFastHashTableWrapperTemp = new VectorMapJoinFastStringHashMapParallel(isFullOuter, newThreshold,
            loadFactor, writeBufferSize, estimatedKeyCount/numThreads, tableDesc, numThreads);
        break;
      case HASH_MULTISET:
        vectorMapJoinFastHashTableWrapperTemp = new VectorMapJoinFastStringHashMultiSetParallel(isFullOuter, newThreshold,
            loadFactor, writeBufferSize, estimatedKeyCount/numThreads, tableDesc, numThreads);
        break;
      case HASH_SET:
        vectorMapJoinFastHashTableWrapperTemp = new VectorMapJoinFastStringHashSetParallel(isFullOuter, newThreshold,
            loadFactor, writeBufferSize, estimatedKeyCount/numThreads, tableDesc, numThreads);
        break;
      }
      break;

    case MULTI_KEY:
      switch (hashTableKind) {
      case HASH_MAP:
        vectorMapJoinFastHashTableWrapperTemp = new VectorMapJoinFastMultiKeyHashMapParallel(isFullOuter, newThreshold,
            loadFactor, writeBufferSize, estimatedKeyCount/numThreads, numThreads);
        break;
      case HASH_MULTISET:
        vectorMapJoinFastHashTableWrapperTemp = new VectorMapJoinFastMultiKeyHashMultiSetParallel(isFullOuter, newThreshold,
            loadFactor, writeBufferSize, estimatedKeyCount/numThreads, numThreads);
        break;
      case HASH_SET:
        vectorMapJoinFastHashTableWrapperTemp = new VectorMapJoinFastMultiKeyHashSetParallel(isFullOuter, newThreshold,
            loadFactor, writeBufferSize, estimatedKeyCount/numThreads, numThreads);
        break;
      }
      break;
    }

    vectorMapJoinFastHashTableWrapper = vectorMapJoinFastHashTableWrapperTemp;
    this.numThreads = numThreads;
  }

  public void putRow(BytesWritable currentKey, BytesWritable currentValue)
      throws SerDeException, HiveException, IOException {
    long key = vectorMapJoinFastHashTableWrapper.deserializeToKey(currentKey);
    long hashCode = vectorMapJoinFastHashTableWrapper.calculateLongHashCode(key, currentKey);
    vectorMapJoinFastHashTableWrapper.putRow(currentKey, currentValue, hashCode, key);
  }

  public long calculateLongHashCode(long key, BytesWritable currentKey) throws HiveException, IOException {
    return vectorMapJoinFastHashTableWrapper.calculateLongHashCode(key, currentKey);
  }

  public long deserializeToKey(BytesWritable currentKey) throws HiveException, IOException {
    return vectorMapJoinFastHashTableWrapper.deserializeToKey(currentKey);
  }

  public void putRow(BytesWritable currentKey, BytesWritable currentValue, long hashCode, long key)
      throws SerDeException, HiveException, IOException {
    vectorMapJoinFastHashTableWrapper.putRow(currentKey, currentValue, hashCode, key);
  }

  @Override public boolean containsLongKey(long currentKey) {
    return vectorMapJoinFastHashTableWrapper.containsLongKey(currentKey);
  }

  @Override public int size() {
    return vectorMapJoinFastHashTableWrapper.size();
  }

  @Override public MatchTracker createMatchTracker() {
    return null;
  }

  @Override
  public VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public int spillPartitionId() {
    throw new RuntimeException("Not implemented");
  }

  @Override public long getEstimatedMemorySize() {
    return vectorMapJoinFastHashTableWrapper.getEstimatedMemorySize();
  }

  public VectorMapJoinFastHashTableWrapper getVectorMapJoinFastHashTableWrapper() {
    return vectorMapJoinFastHashTableWrapper;
  }
}