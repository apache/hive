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

import org.apache.hadoop.hive.common.MemoryEstimate;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinNonMatchedIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/*
 * Single LONG key hash map optimized for vector map join.
 */
public class VectorMapJoinFastLongHashMapContainer extends VectorMapJoinFastHashTableContainerBase implements
    VectorMapJoinLongHashMap, MemoryEstimate {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastLongHashMapContainer.class);

  private final VectorMapJoinFastLongHashMap[] vectorMapJoinFastLongHashMaps;
  private final BinarySortableDeserializeRead keyBinarySortableDeserializeRead;
  private final HashTableKeyType hashTableKeyType;
  private final int numThreads;
  private final boolean minMaxEnabled;

  public VectorMapJoinFastLongHashMapContainer(
      boolean isFullOuter,
      boolean minMaxEnabled,
      HashTableKeyType hashTableKeyType,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount, TableDesc tableDesc,
      int numHTs) {
    this.hashTableKeyType = hashTableKeyType;
    this.vectorMapJoinFastLongHashMaps = new VectorMapJoinFastLongHashMap[numHTs];
    LOG.info("Initializing {} HT Containers ", numHTs);
    for (int i = 0; i < numHTs; ++i) {
      vectorMapJoinFastLongHashMaps[i] =
          new VectorMapJoinFastLongHashMap(isFullOuter, minMaxEnabled, hashTableKeyType, initialCapacity, loadFactor,
              writeBuffersSize, estimatedKeyCount, tableDesc);
    }
    PrimitiveTypeInfo[] primitiveTypeInfos = { hashTableKeyType.getPrimitiveTypeInfo() };
    this.keyBinarySortableDeserializeRead =
        BinarySortableDeserializeRead.with(primitiveTypeInfos, false, tableDesc.getProperties());
    this.numThreads = numHTs;
    this.minMaxEnabled = minMaxEnabled;
  }

  @Override
  public boolean useMinMax() {
    return minMaxEnabled;
  }

  @Override
  public long min() {
    long min = Long.MAX_VALUE;
    for (int i = 0; i < numThreads; ++i) {
      long currentMin = vectorMapJoinFastLongHashMaps[i].min();
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
      long currentMax = vectorMapJoinFastLongHashMaps[i].max();
      if (currentMax > max) {
        max = currentMax;
      }
    }
    return max;
  }

  public static class NonMatchedLongHashMapIterator extends VectorMapJoinFastNonMatchedIterator {

    private VectorMapJoinFastLongHashMap.NonMatchedLongHashMapIterator[] hashMapIterators;
    private int index;
    private int numThreads;

    private NonMatchedLongHashMapIterator(MatchTracker matchTracker,
        VectorMapJoinFastLongHashMap[] vectorMapJoinFastLongHashMaps, int numThreads) {
      super(matchTracker);

      hashMapIterators = new VectorMapJoinFastLongHashMap.NonMatchedLongHashMapIterator[numThreads];
      for (int i = 0; i < numThreads; ++i) {
        hashMapIterators[i] = new VectorMapJoinFastLongHashMap.NonMatchedLongHashMapIterator(
            matchTracker.getPartition(i), vectorMapJoinFastLongHashMaps[i]);
      }
      index = 0;
      this.numThreads = numThreads;
    }

    public void init() {
      for (int i = 0; i < numThreads; ++i) {
        hashMapIterators[i].init();
      }
      index = 0;
    }

    public boolean findNextNonMatched() {
      for (; index < numThreads; ++index) {
        if (hashMapIterators[index].findNextNonMatched()) {
          return true;
        }
      }
      return false;
    }

    public boolean readNonMatchedLongKey() {
      return hashMapIterators[index].readNonMatchedLongKey();
    }

    public long getNonMatchedLongKey() {
      return hashMapIterators[index].getNonMatchedLongKey();
    }

    public VectorMapJoinHashMapResult getNonMatchedHashMapResult() {
      return hashMapIterators[index].getNonMatchedHashMapResult();
    }
  }

  public VectorMapJoinHashMapResult createHashMapResult() {
    return new VectorMapJoinFastValueStore.HashMapResult();
  }

  @Override
  public int spillPartitionId() {
    throw new UnsupportedOperationException();
  }

  public long getHashCode(BytesWritable currentKey) throws HiveException, IOException {
    byte[] keyBytes = currentKey.getBytes();
    int keyLength = currentKey.getLength();
    keyBinarySortableDeserializeRead.set(keyBytes, 0, keyLength);
    try {
      if (!keyBinarySortableDeserializeRead.readNextField()) {
        return 0;
      }
    } catch(Exception e) {
      throw new HiveException("DeserializeRead details: " +
          keyBinarySortableDeserializeRead.getDetailedReadPositionString(), e);
    }
    long key = VectorMapJoinFastLongHashUtil.deserializeLongKey(keyBinarySortableDeserializeRead, hashTableKeyType);
    return HashCodeUtil.calculateLongHashCode(key);
  }

  @Override
  public void putRow(long hashCode, BytesWritable currentKey, BytesWritable currentValue)
      throws HiveException, IOException {
    vectorMapJoinFastLongHashMaps[(int) ((numThreads - 1) & hashCode)].putRow(hashCode, currentKey, currentValue);
  }

  public JoinUtil.JoinResult lookup(long key, VectorMapJoinHashMapResult hashMapResult) {
    long hashCode = HashCodeUtil.calculateLongHashCode(key);
    return vectorMapJoinFastLongHashMaps[(int) ((numThreads - 1) & hashCode)].lookup(key, hashMapResult);
  }

  public JoinUtil.JoinResult lookup(long key, VectorMapJoinHashMapResult hashMapResult,
      MatchTracker matchTracker) {
    long hashCode = HashCodeUtil.calculateLongHashCode(key);
    int partition = (int) ((numThreads - 1) & hashCode);
    MatchTracker childMatchTracker = matchTracker != null ? matchTracker.getPartition(partition) : null;

    return vectorMapJoinFastLongHashMaps[partition].lookup(key, hashMapResult,
        childMatchTracker);
  }

  public long getEstimatedMemorySize() {
    long estimatedMemorySize = 0;
    for (int i = 0; i < numThreads; ++i) {
      estimatedMemorySize += vectorMapJoinFastLongHashMaps[i].getEstimatedMemorySize();
    }
    return estimatedMemorySize;
  }

  @Override
  public int size() {
    int size = 0;
    for (int i = 0; i < numThreads; ++i) {
      size += vectorMapJoinFastLongHashMaps[i].size();
    }
    return size;
  }

  @Override
  public MatchTracker createMatchTracker() {
    MatchTracker parentMatchTracker = MatchTracker.createPartitioned(numThreads);
    for (int i = 0; i < numThreads; i++) {
      int childSize = vectorMapJoinFastLongHashMaps[i].logicalHashBucketCount;
      parentMatchTracker.addPartition(i, childSize);
    }

    return parentMatchTracker;
  }

  @Override
  public VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker) {
    return new NonMatchedLongHashMapIterator(matchTracker, vectorMapJoinFastLongHashMaps, numThreads);
  }
}
