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
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Single STRING key hash map optimized for vector map join.
 *
 * The key will be deserialized and just the bytes will be stored.
 */
public class VectorMapJoinFastStringHashMapContainer extends VectorMapJoinFastHashTableContainerBase implements
    VectorMapJoinBytesHashMap {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastStringHashMapContainer.class);

  private final VectorMapJoinFastStringHashMap[] vectorMapJoinFastStringHashMaps;
  private final BinarySortableDeserializeRead keyBinarySortableDeserializeRead;
  private final int numThreads;

  public VectorMapJoinFastStringHashMapContainer(
      boolean isFullOuter,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount, TableDesc tableDesc,
      int numHTs) {
    this.vectorMapJoinFastStringHashMaps = new VectorMapJoinFastStringHashMap[numHTs];
    LOG.info("Initializing {} HT Containers ", numHTs);
    for (int i = 0; i < numHTs; ++i) {
      vectorMapJoinFastStringHashMaps[i] = new VectorMapJoinFastStringHashMap(isFullOuter,
          initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount, tableDesc);
    }
    PrimitiveTypeInfo[] primitiveTypeInfos = { TypeInfoFactory.stringTypeInfo };
    this.keyBinarySortableDeserializeRead =
        BinarySortableDeserializeRead.with(primitiveTypeInfos, false, tableDesc.getProperties());
    this.numThreads = numHTs;
  }

  private static class NonMatchedBytesHashMapIterator extends VectorMapJoinFastNonMatchedIterator {

    private VectorMapJoinFastBytesHashMap.NonMatchedBytesHashMapIterator[] hashMapIterators;
    private int index;
    private int numThreads;

    NonMatchedBytesHashMapIterator(MatchTracker matchTracker,
        VectorMapJoinFastStringHashMap[] hashMaps, int numThreads) {
      super(matchTracker);

      hashMapIterators = new VectorMapJoinFastBytesHashMap.NonMatchedBytesHashMapIterator[numThreads];
      for (int i = 0; i < numThreads; ++i) {
        hashMapIterators[i] = new VectorMapJoinFastBytesHashMap.NonMatchedBytesHashMapIterator(
            matchTracker.getPartition(i), hashMaps[i]);
      }
      index = 0;
      this.numThreads = numThreads;
    }

    @Override
    public void init() {
      for (int i = 0; i < numThreads; ++i) {
        hashMapIterators[i].init();
      }
      index = 0;
    }

    @Override
    public boolean findNextNonMatched() {
      for (; index < numThreads; ++index) {
        if (hashMapIterators[index].findNextNonMatched()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean readNonMatchedBytesKey() throws HiveException {
      return hashMapIterators[index].readNonMatchedBytesKey();
    }

    @Override
    public byte[] getNonMatchedBytes() {
      return hashMapIterators[index].getNonMatchedBytes();
    }

    @Override
    public int getNonMatchedBytesOffset() {
      return hashMapIterators[index].getNonMatchedBytesOffset();
    }

    @Override
    public int getNonMatchedBytesLength() {
      return hashMapIterators[index].getNonMatchedBytesLength();
    }

    @Override
    public VectorMapJoinHashMapResult getNonMatchedHashMapResult() {
      return hashMapIterators[index].getNonMatchedHashMapResult();
    }
  }

  @Override
  public void putRow(long hashCode, BytesWritable currentKey, BytesWritable currentValue)
      throws HiveException, IOException {
    vectorMapJoinFastStringHashMaps[(int) ((numThreads - 1) & hashCode)].putRow(hashCode, currentKey, currentValue);
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
    return HashCodeUtil.murmurHash(
        keyBinarySortableDeserializeRead.currentBytes,
        keyBinarySortableDeserializeRead.currentBytesStart,
        keyBinarySortableDeserializeRead.currentBytesLength);
  }

  @Override
  public long getEstimatedMemorySize() {
    long estimatedMemorySize = 0;
    for (int i = 0; i < numThreads; ++i) {
      estimatedMemorySize += vectorMapJoinFastStringHashMaps[i].getEstimatedMemorySize();
    }
    return estimatedMemorySize;
  }

  @Override
  public int size() {
    int size = 0;
    for (int i = 0; i < numThreads; ++i) {
      size += vectorMapJoinFastStringHashMaps[i].size();
    }
    return size;
  }

  @Override
  public MatchTracker createMatchTracker() {
    MatchTracker parentMatchTracker = MatchTracker.createPartitioned(numThreads);
    for (int i = 0; i < numThreads; i++) {
      int childSize = vectorMapJoinFastStringHashMaps[i].logicalHashBucketCount;
      parentMatchTracker.addPartition(i, childSize);
    }

    return parentMatchTracker;
  }

  @Override
  public VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker) {
    return new NonMatchedBytesHashMapIterator(matchTracker, vectorMapJoinFastStringHashMaps, numThreads);
  }

  @Override
  public int spillPartitionId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public JoinUtil.JoinResult lookup(byte[] keyBytes, int keyStart, int keyLength,
      VectorMapJoinHashMapResult hashMapResult) throws IOException {
    long hashCode = HashCodeUtil.murmurHash(keyBytes, keyStart, keyLength);
    return vectorMapJoinFastStringHashMaps[(int) ((numThreads - 1) & hashCode)].lookup(keyBytes, keyStart, keyLength, hashMapResult);
  }

  @Override
  public JoinUtil.JoinResult lookup(byte[] keyBytes, int keyStart, int keyLength,
      VectorMapJoinHashMapResult hashMapResult, MatchTracker matchTracker) throws IOException {
    long hashCode = HashCodeUtil.murmurHash(keyBytes, keyStart, keyLength);
    int partition = (int) ((numThreads - 1) & hashCode);
    MatchTracker childMatchTracker = matchTracker != null ? matchTracker.getPartition(partition) : null;

    return vectorMapJoinFastStringHashMaps[partition].lookup(keyBytes, keyStart, keyLength, hashMapResult,
        childMatchTracker);
  }

  @Override
  public VectorMapJoinHashMapResult createHashMapResult() {
    return new VectorMapJoinFastBytesHashMapStore.HashMapResult();
  }
}
