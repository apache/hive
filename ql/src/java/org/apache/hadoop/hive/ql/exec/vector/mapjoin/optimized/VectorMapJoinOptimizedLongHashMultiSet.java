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
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMultiSetResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashMultiSet;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;

/*
 * An single long value hash map based on the BytesBytesMultiHashMultiSet.
 *
 * We serialize the long key into BinarySortable format into an output buffer accepted by
 * BytesBytesMultiHashMultiSet.
 */
public class VectorMapJoinOptimizedLongHashMultiSet
              extends VectorMapJoinOptimizedHashMultiSet
              implements VectorMapJoinLongHashMultiSet  {

  private VectorMapJoinOptimizedLongCommon longCommon;

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

  /*
  @Override
  public void putRow(BytesWritable currentKey, BytesWritable currentValue)
      throws SerDeException, HiveException, IOException {

    longCommon.adaptPutRow((VectorMapJoinOptimizedHashTable) this, currentKey, currentValue);
  }
  */

  @Override
  public JoinResult contains(long key,
      VectorMapJoinHashMultiSetResult hashMultiSetResult) throws IOException {

    SerializedBytes serializedBytes = longCommon.serialize(key);

    return super.contains(serializedBytes.bytes, serializedBytes.offset, serializedBytes.length,
        hashMultiSetResult);

  }

  public VectorMapJoinOptimizedLongHashMultiSet(
        boolean minMaxEnabled, boolean isOuterJoin, HashTableKeyType hashTableKeyType,
        MapJoinTableContainer originalTableContainer, ReusableGetAdaptor hashMapRowGetter) {
    super(originalTableContainer, hashMapRowGetter);
    longCommon =  new VectorMapJoinOptimizedLongCommon(minMaxEnabled, isOuterJoin, hashTableKeyType);
  }
}