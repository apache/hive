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

package org.apache.hadoop.hive.ql.exec.vector.ptf;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;

/**
 * This class evaluates rank() for a PTF group.
 *
 * Rank starts at 1; the same rank is streamed to the output column as repeated; after the last
 * group row, the rank is increased by the number of group rows.
 */
public class VectorPTFEvaluatorRank extends VectorPTFEvaluatorBase {

  private int rank;
  private int groupCount;

  public VectorPTFEvaluatorRank(WindowFrameDef windowFrameDef, int outputColumnNum) {
    super(windowFrameDef, outputColumnNum);
    resetEvaluator();
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch)
      throws HiveException {

    // We don't evaluate input columns...

    /*
     * Do careful maintenance of the outputColVector.noNulls flag.
     */

    LongColumnVector longColVector = (LongColumnVector) batch.cols[outputColumnNum];
    longColVector.isRepeating = true;
    longColVector.isNull[0] = false;
    longColVector.vector[0] = rank;
    groupCount += batch.size;
  }

  @Override
  public void doLastBatchWork() {
    rank += groupCount;
    groupCount = 0;
  }

  @Override
  public boolean streamsResult() {
    // No group value.
    return true;
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.LONG;
  }

  @Override
  public void resetEvaluator() {
    rank = 1;
    groupCount = 0;
  }
}