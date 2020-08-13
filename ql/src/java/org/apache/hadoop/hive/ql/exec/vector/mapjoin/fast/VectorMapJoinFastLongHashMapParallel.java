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

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.MemoryEstimate;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinNonMatchedIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.serde2.WriteBuffers;
import org.apache.hadoop.hive.serde2.WriteBuffers.ByteSegmentRef;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.jgss.HttpCaller;

/*
 * An single LONG key hash map optimized for vector map join.
 */
public class VectorMapJoinFastLongHashMapParallel extends VectorMapJoinFastHashTableWrapper implements
    VectorMapJoinLongHashMap, MemoryEstimate {

  public static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastLongHashMapParallel.class);
  private VectorMapJoinFastLongHashMap[] vectorMapJoinFastLongHashMaps;

  private final BinarySortableDeserializeRead keyBinarySortableDeserializeRead;

  HashTableKeyType hashTableKeyType;

  @Override public boolean useMinMax() {
    boolean useMinMax = false;
    /*for (int i=0; i<4; ++i) {
      useMinMax = vectorMapJoinFastLongHashMaps[i].useMinMax();
      if (!useMinMax) {
        break;
      }
    }*/
    return useMinMax;
  }

  @Override public long min() {
    long min = Long.MAX_VALUE;
    for (int i = 0; i < 4; ++i) {
      long currentMin = vectorMapJoinFastLongHashMaps[i].min();
      if (min > currentMin) {
        min = currentMin;
      }
    }
    return min;
  }

  @Override public long max() {
    long max = Long.MIN_VALUE;
    for (int i = 0; i < 4; ++i) {
      long currentMax = vectorMapJoinFastLongHashMaps[i].max();
      if (max > currentMax) {
        max = currentMax;
      }
    }
    return max;
  }

  public static class NonMatchedLongHashMapIterator extends VectorMapJoinFastNonMatchedIterator {

    private VectorMapJoinFastLongHashMap.NonMatchedLongHashMapIterator[] hashMapIterators;

    int index;

    public NonMatchedLongHashMapIterator(MatchTracker matchTracker,
        VectorMapJoinFastLongHashMap[] vectorMapJoinFastLongHashMaps) {
      super(matchTracker);
      hashMapIterators = new VectorMapJoinFastLongHashMap.NonMatchedLongHashMapIterator[4];
      for (int i = 0; i < 4; ++i) {
        hashMapIterators[i] = new VectorMapJoinFastLongHashMap.NonMatchedLongHashMapIterator(matchTracker,
            vectorMapJoinFastLongHashMaps[i]);
      }
      index = 0;
    }

    public void init() {
      for (int i = 0; i < 4; ++i) {
        hashMapIterators[i].init();
      }
      index = 0;
    }

    public boolean findNextNonMatched() {
      for (; index < 4; ++index) {
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

  @Override public int spillPartitionId() {
    throw new RuntimeException("Not implemented");
  }

  public long deserializeToKey(BytesWritable currentKey) throws HiveException, IOException {
    byte[] keyBytes = currentKey.getBytes();
    int keyLength = currentKey.getLength();
    keyBinarySortableDeserializeRead.set(keyBytes,0,keyLength);
    try
    {
      if (!keyBinarySortableDeserializeRead.readNextField()) {
        return 0;
      }
    } catch(Exception e) {
      throw new HiveException(
          "\nDeserializeRead details: " + keyBinarySortableDeserializeRead.getDetailedReadPositionString()
              + "\nException: " + e.toString());
    }
    return VectorMapJoinFastLongHashUtil.deserializeLongKey(keyBinarySortableDeserializeRead, hashTableKeyType);
  }

  public long calculateLongHashCode(long key, BytesWritable currentKey) throws HiveException, IOException {
    return HashCodeUtil.calculateLongHashCode(key);
  }

  @Override
  public void putRow(BytesWritable currentKey, BytesWritable currentValue)
      throws HiveException, IOException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void putRow(BytesWritable currentKey, BytesWritable currentValue, long hashCode, long key)
      throws HiveException, IOException {
    vectorMapJoinFastLongHashMaps[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].putRow(currentKey, currentValue,
        hashCode, key);
  }

  public boolean containsLongKey(long currentKey) {
    long hashCode = HashCodeUtil.calculateLongHashCode(currentKey);
    return vectorMapJoinFastLongHashMaps[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].containsKey(currentKey);
  }

  /*
   * A Unit Test convenience method for putting key and value into the hash table using the
   * actual types.
   */
  @VisibleForTesting
  public void testPutRow(long currentKey, byte[] currentValue) throws HiveException, IOException {
    long hashCode = HashCodeUtil.calculateLongHashCode(currentKey);
    vectorMapJoinFastLongHashMaps[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].testPutRow(currentKey, currentValue);
  }

  public JoinUtil.JoinResult lookup(long key, VectorMapJoinHashMapResult hashMapResult) {
    long hashCode = HashCodeUtil.calculateLongHashCode(key);
    return vectorMapJoinFastLongHashMaps[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].lookup(key, hashMapResult);
  }

  public JoinUtil.JoinResult lookup(long key, VectorMapJoinHashMapResult hashMapResult,
      MatchTracker matchTracker) {
    long hashCode = HashCodeUtil.calculateLongHashCode(key);
    return vectorMapJoinFastLongHashMaps[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].lookup(key, hashMapResult, matchTracker);
  }

  public VectorMapJoinFastLongHashMapParallel(
      boolean isFullOuter,
      boolean minMaxEnabled,
      HashTableKeyType hashTableKeyType,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount, TableDesc tableDesc) {
    this.hashTableKeyType = hashTableKeyType;
    vectorMapJoinFastLongHashMaps = new VectorMapJoinFastLongHashMap[4];
    for (int i=0; i<4; ++i) {
      LOG.info("Initial Capacity is: " + initialCapacity);
      vectorMapJoinFastLongHashMaps[i] =
          new VectorMapJoinFastLongHashMap(isFullOuter, minMaxEnabled, hashTableKeyType, initialCapacity, loadFactor,
              writeBuffersSize, estimatedKeyCount, tableDesc);
    }
    PrimitiveTypeInfo[] primitiveTypeInfos = { hashTableKeyType.getPrimitiveTypeInfo() };
    keyBinarySortableDeserializeRead =
        BinarySortableDeserializeRead.with(
            primitiveTypeInfos,
            /* useExternalBuffer */ false, tableDesc.getProperties());
  }

  public long getEstimatedMemorySize() {
    long estimatedMemorySize = 0;
    for (int i=0; i<4; ++i) {
      estimatedMemorySize += vectorMapJoinFastLongHashMaps[i].getEstimatedMemorySize()
          + vectorMapJoinFastLongHashMaps[i].valueStore.getEstimatedMemorySize();
    }
    return estimatedMemorySize;
  }

  @Override
  public int size() {
    int size = 0;
    for (int i=0; i<4; ++i) {
      size += vectorMapJoinFastLongHashMaps[i].size();
    }
    return size;
  }

  @Override
  public MatchTracker createMatchTracker() {
    int count = 0;
    for (int i=0; i < 4; ++i) {
      count += vectorMapJoinFastLongHashMaps[i].logicalHashBucketCount;
    }
    return MatchTracker.create(count);
  }

  @Override
  public VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker) {
    return new NonMatchedLongHashMapIterator(matchTracker, vectorMapJoinFastLongHashMaps);
  }
}
