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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.optimized;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.JoinUtil.JoinResult;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer.ReusableGetAdaptor;
import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinNonMatchedIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.serde2.WriteBuffers.ByteSegmentRef;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/*
 * An single long value hash map based on the BytesBytesMultiHashMap.
 *
 * We serialize the long key into BinarySortable format into an output buffer accepted by
 * BytesBytesMultiHashMap.
 */
public class VectorMapJoinOptimizedLongHashMap
              extends VectorMapJoinOptimizedHashMap
              implements VectorMapJoinLongHashMap {

  private HashTableKeyType hashTableKeyType;

  private VectorMapJoinOptimizedLongCommon longCommon;

  private static class NonMatchedLongHashMapIterator
      extends VectorMapJoinOptimizedNonMatchedIterator {

    private VectorMapJoinOptimizedLongHashMap hashMap;

    // Extract long with non-shared deserializer object.
    private BinarySortableDeserializeRead keyBinarySortableDeserializeRead;

    private long longValue;

    NonMatchedLongHashMapIterator(MatchTracker matchTracker,
        VectorMapJoinOptimizedLongHashMap hashMap) {
      super(matchTracker);
      this.hashMap = hashMap;
    }

    @Override
    public void init() {
      super.init();
      nonMatchedIterator =
          ((MapJoinTableContainer) hashMap.originalTableContainer).
              createNonMatchedSmallTableIterator(matchTracker);

      TypeInfo integerTypeInfo;
      switch (hashMap.hashTableKeyType) {
      case BOOLEAN:
        integerTypeInfo = TypeInfoFactory.booleanTypeInfo;
        break;
      case BYTE:
        integerTypeInfo = TypeInfoFactory.byteTypeInfo;
        break;
      case SHORT:
        integerTypeInfo = TypeInfoFactory.shortTypeInfo;
        break;
      case INT:
        integerTypeInfo = TypeInfoFactory.intTypeInfo;
        break;
      case LONG:
        integerTypeInfo = TypeInfoFactory.longTypeInfo;
        break;
      default:
        throw new RuntimeException("Unexpected key type " + hashMap.hashTableKeyType);
      }
      keyBinarySortableDeserializeRead =
          BinarySortableDeserializeRead.with(
              new TypeInfo[] {integerTypeInfo}, false, hashMap.longCommon.getTableDesc().getProperties());
    }

    private boolean readNonMatchedLongKey(ByteSegmentRef keyRef) throws HiveException {

      try {
        byte[] keyBytes = keyRef.getBytes();
        int keyOffset = (int) keyRef.getOffset();
        int keyLength = keyRef.getLength();
        keyBinarySortableDeserializeRead.set(keyBytes, keyOffset, keyLength);
        if (!keyBinarySortableDeserializeRead.readNextField()) {
          return false;
        }
        switch (hashMap.hashTableKeyType) {
        case BOOLEAN:
          longValue = keyBinarySortableDeserializeRead.currentBoolean ? 1 : 0;
          break;
        case BYTE:
          longValue = keyBinarySortableDeserializeRead.currentByte;
          break;
        case SHORT:
          longValue = keyBinarySortableDeserializeRead.currentShort;
          break;
        case INT:
          longValue = keyBinarySortableDeserializeRead.currentInt;
          break;
        case LONG:
          longValue = keyBinarySortableDeserializeRead.currentLong;
          break;
        default:
          throw new RuntimeException("Unexpected key type " + hashMap.hashTableKeyType);
        }
      } catch (IOException e) {
        throw new HiveException(e);
      }
      return true;
    }

    @Override
    public boolean readNonMatchedLongKey() throws HiveException {
      return readNonMatchedLongKey(nonMatchedIterator.getCurrentKeyAsRef());
    }

    @Override
    public long getNonMatchedLongKey() throws HiveException {
      return longValue;
    }
  }

  @Override
  public boolean useMinMax() {
    return longCommon.useMinMax();
  }

  @Override
  public long min() {
    return longCommon.min();
  }

  @Override
  public long max() {
    return longCommon.max();
  }

  @Override
  public VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker) {
    return new NonMatchedLongHashMapIterator(matchTracker, this);
  }

  @Override
  public JoinResult lookup(long key,
      VectorMapJoinHashMapResult hashMapResult) throws IOException {

    SerializedBytes serializedBytes = longCommon.serialize(key);

    return super.lookup(serializedBytes.bytes, serializedBytes.offset, serializedBytes.length,
            hashMapResult);
  }

  @Override
  public JoinResult lookup(long key,
      VectorMapJoinHashMapResult hashMapResult, MatchTracker matchTracker) throws IOException {

    SerializedBytes serializedBytes = longCommon.serialize(key);

    return super.lookup(serializedBytes.bytes, serializedBytes.offset, serializedBytes.length,
            hashMapResult, matchTracker);
  }

  public VectorMapJoinOptimizedLongHashMap(
        boolean minMaxEnabled, boolean isOuterJoin, HashTableKeyType hashTableKeyType,
        MapJoinTableContainer originalTableContainer, ReusableGetAdaptor hashMapRowGetter, TableDesc tableDesc) {
    super(originalTableContainer, hashMapRowGetter);
    this.hashTableKeyType = hashTableKeyType;
    longCommon =  new VectorMapJoinOptimizedLongCommon(minMaxEnabled, isOuterJoin, hashTableKeyType, tableDesc);
  }
}