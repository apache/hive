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
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinNonMatchedIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.io.BytesWritable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hive.common.util.HashCodeUtil;

/*
 * An multi-key value hash map optimized for vector map join.
 *
 * The key is stored as the provided bytes (uninterpreted).
 */
public class VectorMapJoinFastMultiKeyHashMapParallel
    extends VectorMapJoinFastHashTableWrapper implements VectorMapJoinBytesHashMap {

  private VectorMapJoinFastMultiKeyHashMap[] vectorMapJoinFastMultiKeyHashMaps;

  protected BytesWritable testKeyBytesWritable;

  /*
   * A Unit Test convenience method for putting key and value into the hash table using the
   * actual types.
   */
  @VisibleForTesting
  public void testPutRow(byte[] currentKey, byte[] currentValue) throws HiveException, IOException {
    if (testKeyBytesWritable == null) {
      testKeyBytesWritable = new BytesWritable();
    }
    testKeyBytesWritable.set(currentKey, 0, currentKey.length);
    long hashCode = calculateLongHashCode(0, testKeyBytesWritable);
    vectorMapJoinFastMultiKeyHashMaps[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].testPutRow(currentKey, currentValue);
  }

  public VectorMapJoinFastMultiKeyHashMapParallel(
      boolean isFullOuter,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount) {
    vectorMapJoinFastMultiKeyHashMaps = new VectorMapJoinFastMultiKeyHashMap[4];
    for (int i=0; i<4; ++i) {
      vectorMapJoinFastMultiKeyHashMaps[i] =
          new VectorMapJoinFastMultiKeyHashMap(isFullOuter, initialCapacity, loadFactor, writeBuffersSize,
              estimatedKeyCount);
    }
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
  public void putRow(BytesWritable currentKey, BytesWritable currentValue)
      throws HiveException, IOException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public synchronized void putRow(BytesWritable currentKey, BytesWritable currentValue, long hashCode, long key)
      throws HiveException, IOException {
    vectorMapJoinFastMultiKeyHashMaps[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].putRow(currentKey, currentValue,
        hashCode, key);
  }

  @Override
  public boolean containsLongKey(long currentKey) {
    // Only supported for Long-Hash implementations
    throw new RuntimeException("Not supported yet!");
  }

  @Override
  public long getEstimatedMemorySize() {
    long estimatedMemorySize = 0;
    for (int i=0; i<4; ++i) {
      estimatedMemorySize += vectorMapJoinFastMultiKeyHashMaps[i].getEstimatedMemorySize();
    }
    return estimatedMemorySize;
  }

  @Override
  public int size() {
    int size = 0;
    for (int i=0; i<4; ++i) {
      size += vectorMapJoinFastMultiKeyHashMaps[i].size();
    }
    return size;
  }

  @Override
  public MatchTracker createMatchTracker() {
    int count = 0;
    for (int i=0; i < 4; ++i) {
      count += vectorMapJoinFastMultiKeyHashMaps[i].logicalHashBucketCount;
    }
    return MatchTracker.create(count);
  }

  @Override public VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker) {
    throw new RuntimeException("Not implemented");
  }

  @Override public int spillPartitionId() {
    throw new RuntimeException("Not implemented");
  }

  @Override public JoinUtil.JoinResult lookup(byte[] keyBytes, int keyStart, int keyLength,
      VectorMapJoinHashMapResult hashMapResult) throws IOException {
    long hashCode = HashCodeUtil.murmurHash(keyBytes, keyStart, keyLength);
    return vectorMapJoinFastMultiKeyHashMaps[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].lookup(keyBytes, keyStart, keyLength, hashMapResult);
  }

  @Override public JoinUtil.JoinResult lookup(byte[] keyBytes, int keyStart, int keyLength,
      VectorMapJoinHashMapResult hashMapResult, MatchTracker matchTracker) throws IOException {
    long hashCode = HashCodeUtil.murmurHash(keyBytes, keyStart, keyLength);
    return vectorMapJoinFastMultiKeyHashMaps[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].lookup(keyBytes, keyStart, keyLength, hashMapResult,
        matchTracker);
  }

  @Override public VectorMapJoinHashMapResult createHashMapResult() {
    return new VectorMapJoinFastBytesHashMapStore.HashMapResult();
  }
}