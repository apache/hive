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
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTableResult;
import org.apache.hadoop.hive.serde2.WriteBuffers.ByteSegmentRef;

public class VectorMapJoinOptimizedHashMap
          extends VectorMapJoinOptimizedHashTable
          implements VectorMapJoinBytesHashMap {

  @Override
  public VectorMapJoinHashMapResult createHashMapResult() {
    return new HashMapResult();
  }

  public static class HashMapResult extends VectorMapJoinHashMapResult {

    private BytesBytesMultiHashMap.Result bytesBytesMultiHashMapResult;

    public HashMapResult() {
      super();
      bytesBytesMultiHashMapResult = new BytesBytesMultiHashMap.Result();
    }

    public BytesBytesMultiHashMap.Result bytesBytesMultiHashMapResult() {
      return bytesBytesMultiHashMapResult;
    }

    @Override
    public boolean hasRows() {
      return (joinResult() == JoinUtil.JoinResult.MATCH);
    }

    @Override
    public boolean isSingleRow() {
      if (joinResult() != JoinUtil.JoinResult.MATCH) {
        throw new RuntimeException("HashMapResult is not a match");
      }
      return bytesBytesMultiHashMapResult.isSingleRow();
    }

    @Override
    public boolean isCappedCountAvailable() {
      return true;
    }

    @Override
    public int cappedCount() {
      // the return values are capped to return ==0, ==1 and >= 2
      return hasRows() ? (isSingleRow() ? 1 : 2) : 0;
    }

    @Override
    public ByteSegmentRef first() {
      if (joinResult() != JoinUtil.JoinResult.MATCH) {
        throw new RuntimeException("HashMapResult is not a match");
      }
      return bytesBytesMultiHashMapResult.first();
    }

    @Override
    public ByteSegmentRef next() {
      return bytesBytesMultiHashMapResult.next();
    }

    @Override
    public void forget() {
      bytesBytesMultiHashMapResult.forget();
      super.forget();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("(" + super.toString() + ", ");
      sb.append("isSingleRow " + (joinResult() == JoinUtil.JoinResult.MATCH ? isSingleRow() : "<none>") + ")");
      return sb.toString();
    }

    @Override
    public String getDetailedHashMapResultPositionString() {
      return "(Not supported yet)";
    }
 }

  @Override
  public JoinUtil.JoinResult lookup(byte[] keyBytes, int keyOffset, int keyLength,
     VectorMapJoinHashMapResult hashMapResult) throws IOException {

    HashMapResult implementationHashMapResult = (HashMapResult) hashMapResult;

    JoinUtil.JoinResult joinResult =
        doLookup(keyBytes, keyOffset, keyLength,
            implementationHashMapResult.bytesBytesMultiHashMapResult(),
            (VectorMapJoinHashTableResult) hashMapResult);

    return joinResult;
  }

  public VectorMapJoinOptimizedHashMap(
      MapJoinTableContainer originalTableContainer, ReusableGetAdaptor hashMapRowGetter) {
    super(originalTableContainer, hashMapRowGetter);
  }
}