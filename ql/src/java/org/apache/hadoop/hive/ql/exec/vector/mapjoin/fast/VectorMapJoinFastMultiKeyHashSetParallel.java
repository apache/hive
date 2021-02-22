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

import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashSetResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinNonMatchedIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hive.common.util.HashCodeUtil;

/*
 * An multi-key hash set optimized for vector map join.
 *
 * The key is stored as the provided bytes (uninterpreted).
 */
public class VectorMapJoinFastMultiKeyHashSetParallel
    extends VectorMapJoinFastHashTableWrapper implements VectorMapJoinBytesHashSet {

  private VectorMapJoinFastMultiKeyHashSet[] vectorMapJoinFastMultiKeyHashSets;

  protected BytesWritable testKeyBytesWritable;

  private int numThreads;

  /*
   * A Unit Test convenience method for putting the key into the hash table using the
   * actual type.
   */
  @VisibleForTesting
  public void testPutRow(byte[] currentKey) throws HiveException, IOException {
    if (testKeyBytesWritable == null) {
      testKeyBytesWritable = new BytesWritable();
    }
    testKeyBytesWritable.set(currentKey, 0, currentKey.length);
    long hashCode = calculateLongHashCode(0, testKeyBytesWritable);
    vectorMapJoinFastMultiKeyHashSets[(int) ((numThreads - 1) & hashCode)].testPutRow(currentKey);
  }

  @Override
  public long calculateLongHashCode(long key, BytesWritable currentKey) throws HiveException, IOException {
    byte[] keyBytes = currentKey.getBytes();
    int keyLength = currentKey.getLength();
    return HashCodeUtil.murmurHash(keyBytes, 0, keyLength);
  }

  @Override public long deserializeToKey(BytesWritable currentKey) throws HiveException, IOException {
    return 0;
  }

  @Override
  public boolean containsLongKey(long currentKey) {
    // Only supported for Long-Hash implementations
    throw new RuntimeException("Not supported yet!");
  }

  @Override
  public void putRow(BytesWritable currentKey, BytesWritable currentValue, long hashCode, long key)
      throws HiveException, IOException {
    vectorMapJoinFastMultiKeyHashSets[(int) ((numThreads - 1) & hashCode)].putRow(currentKey, currentValue,
        hashCode, key);
  }

  public VectorMapJoinFastMultiKeyHashSetParallel(
      boolean isFullOuter,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount, int numThreads) {
    vectorMapJoinFastMultiKeyHashSets = new VectorMapJoinFastMultiKeyHashSet[numThreads];
    for (int i=0; i<numThreads; ++i) {
      vectorMapJoinFastMultiKeyHashSets[i] =
          new VectorMapJoinFastMultiKeyHashSet(isFullOuter, initialCapacity, loadFactor, writeBuffersSize,
              estimatedKeyCount);
    }
    this.numThreads = numThreads;
  }

  @Override
  public long getEstimatedMemorySize() {
    long estimatedMemorySize = 0;
    for (int i=0; i<numThreads; ++i) {
      estimatedMemorySize += vectorMapJoinFastMultiKeyHashSets[i].getEstimatedMemorySize();
    }
    return estimatedMemorySize;
  }

  @Override
  public int size() {
    int size = 0;
    for (int i=0; i<numThreads; ++i) {
      size += vectorMapJoinFastMultiKeyHashSets[i].size();
    }
    return size;
  }

  @Override public MatchTracker createMatchTracker() {
    int count = 0;
    for (int i=0; i < numThreads; ++i) {
      count += vectorMapJoinFastMultiKeyHashSets[i].logicalHashBucketCount;
    }
    return MatchTracker.create(count);
  }

  @Override public VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker) {
    throw new RuntimeException("Not implemented");
  }

  @Override public int spillPartitionId() {
    throw new RuntimeException("Not implemented");
  }

  @Override public JoinUtil.JoinResult contains(byte[] keyBytes, int keyStart, int keyLength,
      VectorMapJoinHashSetResult hashSetResult) throws IOException {
    long hashCode = HashCodeUtil.murmurHash(keyBytes, keyStart, keyLength);
    return vectorMapJoinFastMultiKeyHashSets[(int) ((numThreads - 1) & hashCode)].contains(keyBytes, keyStart, keyLength, hashSetResult);
  }

  @Override public VectorMapJoinHashSetResult createHashSetResult() {
    return new VectorMapJoinFastBytesHashSetStore.HashSetResult();
  }
}