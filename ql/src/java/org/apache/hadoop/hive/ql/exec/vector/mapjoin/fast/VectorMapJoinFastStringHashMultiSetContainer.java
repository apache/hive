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
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMultiSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMultiSetResult;
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
 * Single STRING key hash multi-set optimized for vector map join.
 *
 * The key will be deserialized and just the bytes will be stored.
 */
public class VectorMapJoinFastStringHashMultiSetContainer extends VectorMapJoinFastHashTableContainerBase implements
    VectorMapJoinBytesHashMultiSet {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastStringHashMultiSetContainer.class);

  private final VectorMapJoinFastStringHashMultiSet[] vectorMapJoinFastStringHashMultiSets;
  private final BinarySortableDeserializeRead keyBinarySortableDeserializeRead;
  private final int numThreads;

  public VectorMapJoinFastStringHashMultiSetContainer(
      boolean isFullOuter,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount, TableDesc tableDesc,
      int numHTs) {
    vectorMapJoinFastStringHashMultiSets = new VectorMapJoinFastStringHashMultiSet[numHTs];
    LOG.info("Initializing {} HT Containers ", numHTs);
    for (int i = 0; i < numHTs; ++i) {
      vectorMapJoinFastStringHashMultiSets[i] = new VectorMapJoinFastStringHashMultiSet(
          isFullOuter,
          initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount, tableDesc);
    }
    PrimitiveTypeInfo[] primitiveTypeInfos = { TypeInfoFactory.stringTypeInfo };
    keyBinarySortableDeserializeRead =
        BinarySortableDeserializeRead.with(primitiveTypeInfos, false, tableDesc.getProperties());
    this.numThreads = numHTs;
  }

  @Override
  public void putRow(long hashCode, BytesWritable currentKey, BytesWritable currentValue)
      throws HiveException, IOException {
    vectorMapJoinFastStringHashMultiSets[(int) ((numThreads - 1) & hashCode)].putRow(hashCode, currentKey, currentValue);
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
      estimatedMemorySize += vectorMapJoinFastStringHashMultiSets[i].getEstimatedMemorySize();
    }
    return estimatedMemorySize;
  }

  @Override
  public int size() {
    int size = 0;
    for (int i = 0; i < numThreads; ++i) {
      size += vectorMapJoinFastStringHashMultiSets[i].size();
    }
    return size;
  }

  @Override
  public MatchTracker createMatchTracker() {
    int count = 0;
    for (int i = 0; i < numThreads; ++i) {
      count += vectorMapJoinFastStringHashMultiSets[i].logicalHashBucketCount;
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
  public JoinUtil.JoinResult contains(byte[] keyBytes, int keyStart, int keyLength,
      VectorMapJoinHashMultiSetResult hashMultiSetResult) throws IOException {
    long hashCode = HashCodeUtil.murmurHash(keyBytes, keyStart, keyLength);
    return vectorMapJoinFastStringHashMultiSets[(int) ((numThreads - 1) & hashCode)].contains(keyBytes, keyStart, keyLength,
        hashMultiSetResult);
  }

  @Override
  public VectorMapJoinHashMultiSetResult createHashMultiSetResult() {
    return new VectorMapJoinFastBytesHashMultiSetStore.HashMultiSetResult();
  }
}
