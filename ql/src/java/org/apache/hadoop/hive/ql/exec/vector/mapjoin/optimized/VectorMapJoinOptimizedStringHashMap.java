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
import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer.ReusableGetAdaptor;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinNonMatchedIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/*
 * An string hash map based on the BytesBytesMultiHashMap.
 */
public class VectorMapJoinOptimizedStringHashMap
             extends VectorMapJoinOptimizedHashMap
             implements VectorMapJoinBytesHashMap {

  private VectorMapJoinOptimizedStringCommon stringCommon;

  private static class NonMatchedStringHashMapIterator extends NonMatchedBytesHashMapIterator {

    private BinarySortableDeserializeRead keyBinarySortableDeserializeRead;
    private final VectorMapJoinOptimizedStringHashMap hashMap;

    NonMatchedStringHashMapIterator(MatchTracker matchTracker,
        VectorMapJoinOptimizedStringHashMap hashMap) {
      super(matchTracker, hashMap);
      this.hashMap = hashMap;
    }

    @Override
    public void init() {
      super.init();

      TypeInfo[] typeInfos = new TypeInfo[] {TypeInfoFactory.stringTypeInfo};
      keyBinarySortableDeserializeRead = BinarySortableDeserializeRead.with(
              typeInfos, false, this.hashMap.stringCommon.getTableDesc().getProperties());
    }

    @Override
    public boolean readNonMatchedBytesKey() throws HiveException {
      super.doReadNonMatchedBytesKey();

      byte[] bytes = keyRef.getBytes();
      final int keyOffset = (int) keyRef.getOffset();
      final int keyLength = keyRef.getLength();
      try {
        keyBinarySortableDeserializeRead.set(bytes, keyOffset, keyLength);
        return keyBinarySortableDeserializeRead.readNextField();
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }

    @Override
    public byte[] getNonMatchedBytes() {
      return keyBinarySortableDeserializeRead.currentBytes;
    }

    @Override
    public int getNonMatchedBytesOffset() {
      return keyBinarySortableDeserializeRead.currentBytesStart;
    }

    @Override
    public int getNonMatchedBytesLength() {
      return keyBinarySortableDeserializeRead.currentBytesLength;
    }
  }

  @Override
  public VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker) {
    return new NonMatchedStringHashMapIterator(matchTracker, this);
  }

  @Override
  public JoinResult lookup(byte[] keyBytes, int keyStart, int keyLength,
      VectorMapJoinHashMapResult hashMapResult) throws IOException {

    SerializedBytes serializedBytes = stringCommon.serialize(keyBytes, keyStart, keyLength);

    return super.lookup(serializedBytes.bytes, serializedBytes.offset, serializedBytes.length,
            hashMapResult);

  }

  @Override
  public JoinResult lookup(byte[] keyBytes, int keyStart, int keyLength,
      VectorMapJoinHashMapResult hashMapResult, MatchTracker matchTracker) throws IOException {

    SerializedBytes serializedBytes = stringCommon.serialize(keyBytes, keyStart, keyLength);

    return super.lookup(serializedBytes.bytes, serializedBytes.offset, serializedBytes.length,
            hashMapResult, matchTracker);

  }

  public VectorMapJoinOptimizedStringHashMap(boolean isOuterJoin, MapJoinTableContainer originalTableContainer,
                                             ReusableGetAdaptor hashMapRowGetter, TableDesc tableDesc) {
    super(originalTableContainer, hashMapRowGetter);
    stringCommon =  new VectorMapJoinOptimizedStringCommon(isOuterJoin, tableDesc);
  }
}