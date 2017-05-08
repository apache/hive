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

import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.persistence.BytesBytesMultiHashMap;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer.ReusableGetAdaptor;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashSetResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTableResult;

public class VectorMapJoinOptimizedHashSet
        extends VectorMapJoinOptimizedHashTable
        implements VectorMapJoinBytesHashSet {

  @Override
  public VectorMapJoinHashSetResult createHashSetResult() {
    return new HashSetResult();
  }

  public static class HashSetResult extends VectorMapJoinHashSetResult {

    private BytesBytesMultiHashMap.Result bytesBytesMultiHashMapResult;

    public HashSetResult() {
      super();
      bytesBytesMultiHashMapResult = new BytesBytesMultiHashMap.Result();
    }

    public BytesBytesMultiHashMap.Result bytesBytesMultiHashMapResult() {
      return bytesBytesMultiHashMapResult;
    }

    @Override
    public void forget() {
      bytesBytesMultiHashMapResult.forget();
      super.forget();
    }
  }

  @Override
  public JoinUtil.JoinResult contains(byte[] keyBytes, int keyOffset, int keyLength,
          VectorMapJoinHashSetResult hashSetResult) throws IOException {

    HashSetResult implementationHashSetResult = (HashSetResult) hashSetResult;

    JoinUtil.JoinResult joinResult =
        doLookup(keyBytes, keyOffset, keyLength,
            implementationHashSetResult.bytesBytesMultiHashMapResult(),
            (VectorMapJoinHashTableResult) hashSetResult);

    return joinResult;
  }

  public VectorMapJoinOptimizedHashSet(
      MapJoinTableContainer originalTableContainer, ReusableGetAdaptor hashMapRowGetter) {
    super(originalTableContainer, hashMapRowGetter);
  }
}