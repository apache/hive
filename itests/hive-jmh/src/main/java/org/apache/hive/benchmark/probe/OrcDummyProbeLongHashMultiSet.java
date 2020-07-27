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
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMultiSetResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashMultiSet;
import org.apache.hadoop.hive.ql.io.filter.MutableFilterContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinInfo;

import java.io.IOException;

public class OrcDummyProbeLongHashMultiSet extends OrcProbeHashTable {

  private VectorMapJoinLongHashMultiSet probeLongHashMultiSet;
  private VectorMapJoinHashMultiSetResult hashMultiSetResult;
  // For Longs, we have optional min/max filtering on HT bounds
  private boolean useMinMax = false;
  private long min = 0L;
  private long max = 0L;

  public OrcDummyProbeLongHashMultiSet(VectorMapJoinHashTable vTable, VectorMapJoinInfo vInfo) {
    super(vTable, vInfo);
    this.probeLongHashMultiSet = (VectorMapJoinLongHashMultiSet) probeDecodeMapJoinTable;
    this.hashMultiSetResult = probeLongHashMultiSet.createHashMultiSetResult();
    this.useMinMax = probeLongHashMultiSet.useMinMax();
    if (useMinMax) {
      this.min = probeLongHashMultiSet.min();
      this.max = probeLongHashMultiSet.max();
    }
  }

  @Override
  public void init() throws HiveException {

  }

  @Override
  public void filterColumnVector(ColumnVector cv, MutableFilterContext cntx, int batchSize) {
    int[] selected = null;
    int newSize = 0;
    boolean selectedInUse = false;
    LongColumnVector probeCol = (LongColumnVector) cv;

    try {
      if (probeCol.isRepeating) {
        newSize = probeLongHashMultiSet.contains(probeCol.vector[0], hashMultiSetResult) == JoinUtil.JoinResult.MATCH ? batchSize : 0;
      } else {
        for (int row = 0; row < batchSize; ++row) {
          newSize += probeLongHashMultiSet.contains(probeCol.vector[row], hashMultiSetResult) == JoinUtil.JoinResult.MATCH ? 1 : 0;
        }
        selectedInUse = true;
      }
      cntx.setFilterContext(selectedInUse, selected, newSize);
      LOG.info("DummyProbeDecode Long Matched: {} selectedInUse {} batchSize {}", newSize, selectedInUse, batchSize);
    } catch (IOException e) {
      LlapIoImpl.LOG.error("ProbeDecode MultiKey Filter failed: {}", e);
      e.printStackTrace();
    }
  }
}
