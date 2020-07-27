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

package org.apache.hadoop.hive.llap.io.probe;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinTableContainer;
import org.apache.hadoop.hive.ql.io.filter.MutableFilterContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OrcProbeHashTable {

  protected static final Logger LOG = LoggerFactory.getLogger(OrcProbeHashTable.class.getName());

  protected VectorMapJoinHashTable probeDecodeMapJoinTable;
  protected VectorMapJoinInfo probeDecodeMapJoinInfo;

  protected OrcProbeHashTable(VectorMapJoinHashTable vTable, VectorMapJoinInfo vInfo) {
    this.probeDecodeMapJoinTable = vTable;
    this.probeDecodeMapJoinInfo = vInfo;
  }

  public abstract void init() throws HiveException;

  /**
   *  Method that filters out rows that are do match the propagated MJ HashTable.
   *  Filtering done as part of the selected array updated as part of MutableFilterContext
   * @param cv ColumnVector
   * @param context MutableFilterContext
   * @param batchSize Original ColumnVectorBatch size
   */
  public abstract void filterColumnVector(ColumnVector cv, MutableFilterContext context, int batchSize);

  public static OrcProbeHashTable getOrcProbeHashTable(VectorMapJoinTableContainer vc) throws HiveException {
    OrcProbeHashTable currProbeHashTable = null;
    // MapJoin Keys are either MultiKey, String or Long (see Vectorizer specializeMapJoinOperator)
    if (vc.mapJoinDesc().getHashTableKeyType() == VectorMapJoinDesc.HashTableKeyType.MULTI_KEY) {
      switch (vc.mapJoinDesc().getHashTableKind()) {
      case HASH_MAP:
        currProbeHashTable =
            new OrcProbeMultiKeyHashMap(vc.vectorMapJoinHashTable(), vc.mapJoinDesc().getVectorMapJoinInfo());
        break;
      case HASH_MULTISET:
        currProbeHashTable =
            new OrcProbeMultiKeyHashMultiSet(vc.vectorMapJoinHashTable(), vc.mapJoinDesc().getVectorMapJoinInfo());
        break;
      case HASH_SET:
        currProbeHashTable =
            new OrcProbeMultiKeyHashSet(vc.vectorMapJoinHashTable(), vc.mapJoinDesc().getVectorMapJoinInfo());
        break;
      }
    } else if (vc.mapJoinDesc().getHashTableKeyType() == VectorMapJoinDesc.HashTableKeyType.STRING) {
      switch (vc.mapJoinDesc().getHashTableKind()) {
      case HASH_MAP:
        currProbeHashTable =
            new OrcProbeStringHashMap(vc.vectorMapJoinHashTable(), vc.mapJoinDesc().getVectorMapJoinInfo());
        break;
      case HASH_MULTISET:
        currProbeHashTable =
            new OrcProbeStringHashMultiSet(vc.vectorMapJoinHashTable(), vc.mapJoinDesc().getVectorMapJoinInfo());
        break;
      case HASH_SET:
        currProbeHashTable =
            new OrcProbeStringHashSet(vc.vectorMapJoinHashTable(), vc.mapJoinDesc().getVectorMapJoinInfo());
        break;
      }
    } else {
      switch (vc.mapJoinDesc().getHashTableKind()) {
      case HASH_MAP:
        currProbeHashTable =
            new OrcProbeLongHashMap(vc.vectorMapJoinHashTable(), vc.mapJoinDesc().getVectorMapJoinInfo());
        break;
      case HASH_MULTISET:
        currProbeHashTable =
            new OrcProbeLongHashMultiSet(vc.vectorMapJoinHashTable(), vc.mapJoinDesc().getVectorMapJoinInfo());
        break;
      case HASH_SET:
        currProbeHashTable =
            new OrcProbeLongHashSet(vc.vectorMapJoinHashTable(), vc.mapJoinDesc().getVectorMapJoinInfo());
        break;
      }
    }
    // OrcProbeHashTable should NOT be Null at this point!
    currProbeHashTable.init();
    return currProbeHashTable;
  }
}
