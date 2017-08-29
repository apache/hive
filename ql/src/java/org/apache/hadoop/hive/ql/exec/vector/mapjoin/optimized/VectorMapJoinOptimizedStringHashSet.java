/**
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
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashSetResult;

/*
 * An multi-key hash map based on the BytesBytesMultiHashSet.
 */
public class VectorMapJoinOptimizedStringHashSet
              extends VectorMapJoinOptimizedHashSet
              implements VectorMapJoinBytesHashSet {

  private VectorMapJoinOptimizedStringCommon stringCommon;

  /*
  @Override
  public void putRow(BytesWritable currentKey, BytesWritable currentValue)
      throws SerDeException, HiveException, IOException {

    stringCommon.adaptPutRow((VectorMapJoinOptimizedHashTable) this, currentKey, currentValue);
  }
  */

  @Override
  public JoinResult contains(byte[] keyBytes, int keyStart, int keyLength,
      VectorMapJoinHashSetResult hashSetResult) throws IOException {

    SerializedBytes serializedBytes = stringCommon.serialize(keyBytes, keyStart, keyLength);

    return super.contains(serializedBytes.bytes, serializedBytes.offset, serializedBytes.length,
        hashSetResult);

  }

  public VectorMapJoinOptimizedStringHashSet(boolean isOuterJoin,
      MapJoinTableContainer originalTableContainer, ReusableGetAdaptor hashMapRowGetter) {
    super(originalTableContainer, hashMapRowGetter);
    stringCommon =  new VectorMapJoinOptimizedStringCommon(isOuterJoin);
  }

  @Override
  public long getEstimatedMemorySize() {
    // adding 16KB constant memory for stringCommon as the rabit hole is deep to implement
    // MemoryEstimate interface, also it is constant overhead
    long size = (16 * 1024L);
    return super.getEstimatedMemorySize() + size;
  }
}