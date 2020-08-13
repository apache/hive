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

import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMultiSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinNonMatchedIterator;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
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
public class VectorMapJoinFastLongHashMultiSetParallel extends VectorMapJoinFastHashTableWrapper implements
    VectorMapJoinLongHashMultiSet {

  public static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastLongHashMultiSetParallel.class);

  private VectorMapJoinFastLongHashMultiSet[] vectorMapJoinFastLongHashMultiSets;

  private final BinarySortableDeserializeRead keyBinarySortableDeserializeRead;

  HashTableKeyType hashTableKeyType;

  public VectorMapJoinHashMultiSetResult createHashMultiSetResult() {
    return new VectorMapJoinFastHashMultiSet.HashMultiSetResult();
  }

  @Override
  public long calculateLongHashCode(long key, BytesWritable currentKey) throws HiveException, IOException {
    return HashCodeUtil.calculateLongHashCode(key);
  }

  @Override public long deserializeToKey(BytesWritable currentKey) throws HiveException, IOException {
    byte[] keyBytes = currentKey.getBytes();
    int keyLength = currentKey.getLength();
    keyBinarySortableDeserializeRead.set(keyBytes, 0, keyLength);
    try {
      if (!keyBinarySortableDeserializeRead.readNextField()) {
        return 0;
      }
    } catch (Exception e) {
      throw new HiveException(
          "\nDeserializeRead details: " +
              keyBinarySortableDeserializeRead.getDetailedReadPositionString() +
              "\nException: " + e.toString());
    }

    return VectorMapJoinFastLongHashUtil.deserializeLongKey(keyBinarySortableDeserializeRead, hashTableKeyType);
  }

  @Override
  public void putRow(BytesWritable currentKey, BytesWritable currentValue)
      throws HiveException, IOException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void putRow(BytesWritable currentKey, BytesWritable currentValue, long hashCode, long key)
      throws HiveException, IOException {
    vectorMapJoinFastLongHashMultiSets[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].putRow(currentKey, currentValue,
            hashCode, key);
  }

  public boolean containsLongKey(long currentKey) {
    long hashCode = HashCodeUtil.calculateLongHashCode(currentKey);
    return vectorMapJoinFastLongHashMultiSets[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].containsKey(currentKey);
  }

  /*
   * A Unit Test convenience method for putting the key into the hash table using the
   * actual type.
   */
  @VisibleForTesting
  public void testPutRow(long currentKey) throws HiveException, IOException {
    long hashCode = HashCodeUtil.calculateLongHashCode(currentKey);
    vectorMapJoinFastLongHashMultiSets[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].testPutRow(currentKey);
  }

  public JoinUtil.JoinResult contains(long key, VectorMapJoinHashMultiSetResult hashMultiSetResult) {
    long hashCode = HashCodeUtil.calculateLongHashCode(key);
    return vectorMapJoinFastLongHashMultiSets[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].contains(key, hashMultiSetResult);
  }

  public VectorMapJoinFastLongHashMultiSetParallel(
      boolean isFullOuter,
      boolean minMaxEnabled,
      HashTableKeyType hashTableKeyType,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount, TableDesc tableDesc) {
    this.hashTableKeyType = hashTableKeyType;
    vectorMapJoinFastLongHashMultiSets = new VectorMapJoinFastLongHashMultiSet[4];
    for (int i=0; i<4; ++i) {
      vectorMapJoinFastLongHashMultiSets[i] = new VectorMapJoinFastLongHashMultiSet(isFullOuter,
          minMaxEnabled, hashTableKeyType,
          initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount, tableDesc);
    }
    PrimitiveTypeInfo[] primitiveTypeInfos = { hashTableKeyType.getPrimitiveTypeInfo() };
    keyBinarySortableDeserializeRead = BinarySortableDeserializeRead.with(
            primitiveTypeInfos,
            /* useExternalBuffer */ false, tableDesc.getProperties());
  }

  @Override
  public long getEstimatedMemorySize() {
    long estimatedMemorySize = 0;
    for (int i=0; i<4; ++i) {
      estimatedMemorySize += vectorMapJoinFastLongHashMultiSets[i].getEstimatedMemorySize();
    }
    return estimatedMemorySize;
  }

  @Override
  public int size() {
    int size = 0;
    for (int i=0; i<4; ++i) {
      size += vectorMapJoinFastLongHashMultiSets[i].size();
    }
    return size;
  }

  @Override public MatchTracker createMatchTracker() {
    int count = 0;
    for (int i=0; i < 4; ++i) {
      count += vectorMapJoinFastLongHashMultiSets[i].logicalHashBucketCount;
    }
    return MatchTracker.create(count);
  }

  @Override public VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker) {
    throw new RuntimeException("Not implemented");
  }

  @Override public int spillPartitionId() {
    throw new RuntimeException("Not implemented");
  }

  @Override public boolean useMinMax() {
    boolean useMinMax = false;
    /*for (int i=0; i<4; ++i) {
      useMinMax = vectorMapJoinFastLongHashMultiSets[i].useMinMax();
      if (!useMinMax) {
        break;
      }
    }*/
    return useMinMax;
  }

  @Override public long min() {
    long min = Long.MAX_VALUE;
    for (int i=0; i<4; ++i) {
      long currentMin = vectorMapJoinFastLongHashMultiSets[i].min();
      if (min > currentMin) {
        min = currentMin;
      }
    }
    return min;
  }

  @Override public long max() {
    long max = Long.MIN_VALUE;
    for (int i=0; i<4; ++i) {
      long currentMax = vectorMapJoinFastLongHashMultiSets[i].max();
      if (max > currentMax) {
        max = currentMax;
      }
    }
    return max;
  }
}
