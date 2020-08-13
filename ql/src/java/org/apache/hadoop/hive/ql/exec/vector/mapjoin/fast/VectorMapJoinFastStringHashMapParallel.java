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
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.serde2.WriteBuffers;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;

/*
 * An single STRING key hash map optimized for vector map join.
 *
 * The key will be deserialized and just the bytes will be stored.
 */
public class VectorMapJoinFastStringHashMapParallel extends VectorMapJoinFastHashTableWrapper implements
    VectorMapJoinBytesHashMap {

  private VectorMapJoinFastStringHashMap[] vectorMapJoinFastStringHashMaps;

  private final BinarySortableDeserializeRead keyBinarySortableDeserializeRead;

  private static class NonMatchedBytesHashMapIterator extends VectorMapJoinFastNonMatchedIterator {

    private VectorMapJoinFastBytesHashMap.NonMatchedBytesHashMapIterator[] hashMapIterators;

    int index;

    NonMatchedBytesHashMapIterator(MatchTracker matchTracker,
        VectorMapJoinFastStringHashMap[] hashMaps) {
      super(matchTracker);
      hashMapIterators = new VectorMapJoinFastBytesHashMap.NonMatchedBytesHashMapIterator[4];
      for(int i=0; i<4; ++i) {
        hashMapIterators[i] = new VectorMapJoinFastBytesHashMap.NonMatchedBytesHashMapIterator(matchTracker,
            hashMaps[i]);
      }
      index = 0;
    }

    @Override
    public void init() {
      for(int i=0; i<4; ++i) {
        hashMapIterators[i].init();
      }
      index = 0;
    }

    @Override
    public boolean findNextNonMatched() {
      for(; index < 4; ++index) {
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
  public void putRow(BytesWritable currentKey, BytesWritable currentValue)
      throws HiveException, IOException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void putRow(BytesWritable currentKey, BytesWritable currentValue, long hashCode, long key)
      throws HiveException, IOException {
    vectorMapJoinFastStringHashMaps[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].putRow(currentKey, currentValue,
        hashCode, key);
  }

  @Override
  public long calculateLongHashCode(long key, BytesWritable currentKey) throws HiveException, IOException {
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
    return HashCodeUtil.murmurHash(
        keyBinarySortableDeserializeRead.currentBytes,
        keyBinarySortableDeserializeRead.currentBytesStart,
        keyBinarySortableDeserializeRead.currentBytesLength);
  }

  @Override public long deserializeToKey(BytesWritable currentKey) throws HiveException, IOException {
    return 0;
  }

  public VectorMapJoinFastStringHashMapParallel(
      boolean isFullOuter,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount, TableDesc tableDesc) {
    vectorMapJoinFastStringHashMaps = new VectorMapJoinFastStringHashMap[4];
    for (int i=0; i<4; ++i) {
      vectorMapJoinFastStringHashMaps[i] = new VectorMapJoinFastStringHashMap(
          isFullOuter,
          initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount, tableDesc);
    }
    PrimitiveTypeInfo[] primitiveTypeInfos = { TypeInfoFactory.stringTypeInfo };
    keyBinarySortableDeserializeRead =
        BinarySortableDeserializeRead.with(
            primitiveTypeInfos,
            /* useExternalBuffer */ false, tableDesc.getProperties());
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
      estimatedMemorySize += vectorMapJoinFastStringHashMaps[i].getEstimatedMemorySize();
    }
    return estimatedMemorySize;
  }

  @Override
  public int size() {
    int size = 0;
    for (int i=0; i<4; ++i) {
      size += vectorMapJoinFastStringHashMaps[i].size();
    }
    return size;
  }

  @Override public MatchTracker createMatchTracker() {
    throw new RuntimeException("Not implemented");
  }

  @Override public VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker) {
    return new NonMatchedBytesHashMapIterator(matchTracker, vectorMapJoinFastStringHashMaps);
  }

  @Override public int spillPartitionId() {
    throw new RuntimeException("Not implemented");
  }

  @Override public JoinUtil.JoinResult lookup(byte[] keyBytes, int keyStart, int keyLength,
      VectorMapJoinHashMapResult hashMapResult) throws IOException {
    long hashCode = HashCodeUtil.murmurHash(keyBytes, keyStart, keyLength);
    return vectorMapJoinFastStringHashMaps[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].lookup(keyBytes, keyStart, keyLength, hashMapResult);
  }

  @Override public JoinUtil.JoinResult lookup(byte[] keyBytes, int keyStart, int keyLength,
      VectorMapJoinHashMapResult hashMapResult, MatchTracker matchTracker) throws IOException {
    long hashCode = HashCodeUtil.murmurHash(keyBytes, keyStart, keyLength);
    return vectorMapJoinFastStringHashMaps[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].lookup(keyBytes, keyStart, keyLength, hashMapResult,
        matchTracker);
  }

  @Override
  public VectorMapJoinHashMapResult createHashMapResult() {
    return new VectorMapJoinFastBytesHashMapStore.HashMapResult();
  }
}