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

package org.apache.hive.benchmark.probe;

import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.probe.OrcProbeHashTable;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.io.filter.MutableFilterContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinInfo;

import java.io.IOException;

public class OrcDummyProbeStringHashMap extends OrcProbeHashTable {

  private VectorMapJoinBytesHashMap probeStringHashMap;
  private VectorMapJoinHashMapResult hashMapResult;

  public OrcDummyProbeStringHashMap(VectorMapJoinHashTable vTable, VectorMapJoinInfo vInfo) {
    super(vTable, vInfo);
    this.probeStringHashMap = (VectorMapJoinBytesHashMap) probeDecodeMapJoinTable;
    this.hashMapResult = probeStringHashMap.createHashMapResult();
  }

  @Override
  public void init() throws HiveException {

  }

  @Override
  public void filterColumnVector(ColumnVector cv, MutableFilterContext cntx, int batchSize) {
    int newSize = 0;
    boolean selectedInUse = false;
    BytesColumnVector probeCol = (BytesColumnVector) cv;

    try {
      if (probeCol.isRepeating) {
        newSize = probeStringHashMap.
            lookup(probeCol.vector[0], probeCol.start[0], probeCol.length[0], hashMapResult)
            == JoinUtil.JoinResult.MATCH ? batchSize : 0;
      } else {
        for (int row = 0; row < batchSize; ++row) {
          newSize += probeStringHashMap.
              lookup(probeCol.vector[row], probeCol.start[row], probeCol.length[row], hashMapResult)
              == JoinUtil.JoinResult.MATCH ? 1 : 0;
        }
        selectedInUse = true;
      }
      LOG.debug("DummyProbeDecode Str Matched: {} selectedInUse {} batchSize {}", newSize, selectedInUse, batchSize);
    } catch (IOException e) {
      LlapIoImpl.LOG.error("ProbeDecode MultiKey Filter failed: {}", e);
      e.printStackTrace();
    }
  }
}
