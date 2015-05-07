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
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMultiSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMultiSetResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTableResult;
import org.apache.hadoop.hive.serde2.WriteBuffers.ByteSegmentRef;

public class VectorMapJoinOptimizedHashMultiSet
        extends VectorMapJoinOptimizedHashTable
        implements VectorMapJoinBytesHashMultiSet {

  @Override
  public VectorMapJoinHashMultiSetResult createHashMultiSetResult() {
    return new HashMultiSetResult();
  }

  public static class HashMultiSetResult extends VectorMapJoinHashMultiSetResult {

    private BytesBytesMultiHashMap.Result bytesBytesMultiHashMapResult;

    private boolean haveCount;

    public HashMultiSetResult() {
      super();
      bytesBytesMultiHashMapResult = new BytesBytesMultiHashMap.Result();
    }

    public BytesBytesMultiHashMap.Result bytesBytesMultiHashMapResult() {
      return bytesBytesMultiHashMapResult;
    }

    /*
     * @return The multi-set count for the lookup key.
     */
    @Override
    public long count() {
      if (!haveCount) {
        if (bytesBytesMultiHashMapResult.isSingleRow()) {
          count = 1;
        } else {
          count = 0;
          ByteSegmentRef byteSegmentRef = bytesBytesMultiHashMapResult.first();
          while (byteSegmentRef != null) {
            count++;
            byteSegmentRef = bytesBytesMultiHashMapResult.next();
          }
        }
        haveCount = true;
      }
      return count;
    }

    @Override
    public void forget() {
      haveCount = false;
      bytesBytesMultiHashMapResult.forget();
      super.forget();
    }
  }

  @Override
  public JoinUtil.JoinResult contains(byte[] keyBytes, int keyOffset, int keyLength,
          VectorMapJoinHashMultiSetResult hashMultiSetResult) throws IOException {

    HashMultiSetResult implementationHashMultiSetResult = (HashMultiSetResult) hashMultiSetResult;

    JoinUtil.JoinResult joinResult =
        doLookup(keyBytes, keyOffset, keyLength,
            implementationHashMultiSetResult.bytesBytesMultiHashMapResult(),
            (VectorMapJoinHashTableResult) hashMultiSetResult);

    return joinResult;
  }

  public VectorMapJoinOptimizedHashMultiSet(
      MapJoinTableContainer originalTableContainer, ReusableGetAdaptor hashMapRowGetter) {
    super(originalTableContainer, hashMapRowGetter);
  }
}