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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer.ReusableGetAdaptor;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKind;

/**
 */
public class VectorMapJoinOptimizedCreateHashTable {

  private static final Log LOG = LogFactory.getLog(VectorMapJoinOptimizedCreateHashTable.class.getName());

  public static VectorMapJoinOptimizedHashTable createHashTable(MapJoinDesc desc,
          MapJoinTableContainer mapJoinTableContainer) {

    MapJoinKey refKey = mapJoinTableContainer.getAnyKey();
    ReusableGetAdaptor hashMapRowGetter = mapJoinTableContainer.createGetter(refKey);

    boolean isOuterJoin = !desc.isNoOuterJoin();
    VectorMapJoinDesc vectorDesc = desc.getVectorDesc();
    HashTableKind hashTableKind = vectorDesc.hashTableKind();
    HashTableKeyType hashTableKeyType = vectorDesc.hashTableKeyType();
    boolean minMaxEnabled = vectorDesc.minMaxEnabled();

    VectorMapJoinOptimizedHashTable hashTable = null;

    switch (hashTableKeyType) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      switch (hashTableKind) {
      case HASH_MAP:
        hashTable = new VectorMapJoinOptimizedLongHashMap(
                  minMaxEnabled, isOuterJoin, hashTableKeyType,
                  mapJoinTableContainer, hashMapRowGetter);
        break;
      case HASH_MULTISET:
        hashTable = new VectorMapJoinOptimizedLongHashMultiSet(
                  minMaxEnabled, isOuterJoin, hashTableKeyType,
                  mapJoinTableContainer, hashMapRowGetter);
        break;
      case HASH_SET:
        hashTable = new VectorMapJoinOptimizedLongHashSet(
                  minMaxEnabled, isOuterJoin, hashTableKeyType,
                  mapJoinTableContainer, hashMapRowGetter);
        break;
      }
      break;

    case STRING:
      switch (hashTableKind) {
      case HASH_MAP:
        hashTable = new VectorMapJoinOptimizedStringHashMap(
                  isOuterJoin,
                  mapJoinTableContainer, hashMapRowGetter);
        break;
      case HASH_MULTISET:
        hashTable = new VectorMapJoinOptimizedStringHashMultiSet(
                  isOuterJoin,
                  mapJoinTableContainer, hashMapRowGetter);
        break;
      case HASH_SET:
        hashTable = new VectorMapJoinOptimizedStringHashSet(
                  isOuterJoin,
                  mapJoinTableContainer, hashMapRowGetter);
        break;
      }
      break;

    case MULTI_KEY:
      switch (hashTableKind) {
      case HASH_MAP:
        hashTable = new VectorMapJoinOptimizedMultiKeyHashMap(
                  isOuterJoin,
                  mapJoinTableContainer, hashMapRowGetter);
        break;
      case HASH_MULTISET:
        hashTable = new VectorMapJoinOptimizedMultiKeyHashMultiSet(
                  isOuterJoin,
                  mapJoinTableContainer, hashMapRowGetter);
        break;
      case HASH_SET:
        hashTable = new VectorMapJoinOptimizedMultiKeyHashSet(
                  isOuterJoin,
                  mapJoinTableContainer, hashMapRowGetter);
        break;
      }
      break;
    }
    return hashTable;
  }

  /*
  @Override
  public com.esotericsoftware.kryo.io.Output getHybridBigTableSpillOutput(int partitionId) {

    HybridHashTableContainer ht = (HybridHashTableContainer) mapJoinTableContainer;

    HashPartition hp = ht.getHashPartitions()[partitionId];

    return hp.getMatchfileOutput();
  }
  */
}