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
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinNonMatchedIterator;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.JoinUtil.JoinResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashSetResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashSet;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;

/*
 * Single LONG key hash set optimized for vector map join.
 */
public class VectorMapJoinFastLongHashSetContainer extends VectorMapJoinFastHashTableContainerBase implements VectorMapJoinLongHashSet{

  public static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastLongHashSetContainer.class);

  private final VectorMapJoinFastLongHashSet[] vectorMapJoinFastLongHashSets;
  private final BinarySortableDeserializeRead keyBinarySortableDeserializeRead;
  private final HashTableKeyType hashTableKeyType;
  private final int numThreads;
  private final boolean minMaxEnabled;

  public VectorMapJoinFastLongHashSetContainer(
      boolean isFullOuter,
      boolean minMaxEnabled,
      HashTableKeyType hashTableKeyType,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount, TableDesc tableDesc,
      int numHTs) {
    this.hashTableKeyType = hashTableKeyType;
    this.vectorMapJoinFastLongHashSets = new VectorMapJoinFastLongHashSet[numHTs];
    LOG.info("Initializing {} HT Containers ", numHTs);
    for (int i = 0; i < numHTs; ++i) {
      vectorMapJoinFastLongHashSets[i] =
          new VectorMapJoinFastLongHashSet(isFullOuter, minMaxEnabled, hashTableKeyType, initialCapacity,
              loadFactor, writeBuffersSize, estimatedKeyCount, tableDesc);
    }
    PrimitiveTypeInfo[] primitiveTypeInfos = { hashTableKeyType.getPrimitiveTypeInfo() };
    this.keyBinarySortableDeserializeRead =
        BinarySortableDeserializeRead.with(primitiveTypeInfos, false, tableDesc.getProperties());
    this.numThreads = numHTs;
    this.minMaxEnabled = minMaxEnabled;
  }

  public VectorMapJoinHashSetResult createHashSetResult() {
    return new VectorMapJoinFastHashSet.HashSetResult();
  }

  @Override
  public long getHashCode(BytesWritable currentKey) throws HiveException, IOException {
    byte[] keyBytes = currentKey.getBytes();
    int keyLength = currentKey.getLength();
    keyBinarySortableDeserializeRead.set(keyBytes, 0, keyLength);
    try {
      if (!keyBinarySortableDeserializeRead.readNextField()) {
        return 0;
      }
    } catch (Exception e) {
      throw new HiveException("DeserializeRead details: " +
          keyBinarySortableDeserializeRead.getDetailedReadPositionString(), e);
    }
    long key = VectorMapJoinFastLongHashUtil.deserializeLongKey(keyBinarySortableDeserializeRead, hashTableKeyType);
    return HashCodeUtil.calculateLongHashCode(key);
  }

  @Override
  public void putRow(long hashCode, BytesWritable currentKey, BytesWritable currentValue)
      throws HiveException, IOException {
    vectorMapJoinFastLongHashSets[(int) ((numThreads - 1) & hashCode)].putRow(hashCode, currentKey, currentValue);
  }

  public JoinResult contains(long key, VectorMapJoinHashSetResult hashSetResult) {
    long hashCode = HashCodeUtil.calculateLongHashCode(key);
    return vectorMapJoinFastLongHashSets[(int) ((numThreads - 1) & hashCode)].contains(key, hashSetResult);
  }

  @Override
  public long getEstimatedMemorySize() {
    long estimatedMemorySize = 0;
    for (int i = 0; i < numThreads; ++i) {
      estimatedMemorySize += vectorMapJoinFastLongHashSets[i].getEstimatedMemorySize();
    }
    return estimatedMemorySize;
  }

  @Override
  public int size() {
    int size = 0;
    for (int i = 0; i < numThreads; ++i) {
      size += vectorMapJoinFastLongHashSets[i].size();
    }
    return size;
  }

  @Override
  public MatchTracker createMatchTracker() {
    int count = 0;
    for (int i = 0; i < numThreads; ++i) {
      count += vectorMapJoinFastLongHashSets[i].logicalHashBucketCount;
    }
    return MatchTracker.create(count);
  }

  @Override
  public VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int spillPartitionId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean useMinMax() {
    return minMaxEnabled;
  }

  @Override
  public long min() {
    long min = Long.MAX_VALUE;
    for (int i = 0; i < numThreads; ++i) {
      long currentMin = vectorMapJoinFastLongHashSets[i].min();
      if (currentMin < min) {
        min = currentMin;
      }
    }
    return min;
  }

  @Override
  public long max() {
    long max = Long.MIN_VALUE;
    for (int i = 0; i < numThreads; ++i) {
      long currentMax = vectorMapJoinFastLongHashSets[i].max();
      if (currentMax > max) {
        max = currentMax;
      }
    }
    return max;
  }
}
