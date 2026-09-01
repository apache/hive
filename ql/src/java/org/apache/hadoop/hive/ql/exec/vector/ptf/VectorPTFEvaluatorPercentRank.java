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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector.ptf;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;

/**
 * Evaluates {@code percent_rank()} as a <b>group-aggregated streaming</b>
 * evaluator.
 *
 * <p>
 * The partition is buffered so {@link #setPartitionSize(int)} is known before
 * output is written.
 * Peer-group rank is tracked incrementally (like
 * {@link VectorPTFEvaluatorRank}) during batch
 * forward; {@link #addStreamingGroupResults} is a no-op because no pre-pass is
 * required.
 */
public class VectorPTFEvaluatorPercentRank extends VectorPTFEvaluatorBase {

  private int rank;
  private int groupCount;

  public VectorPTFEvaluatorPercentRank(WindowFrameDef windowFrameDef, int outputColumnNum) {
    super(windowFrameDef, outputColumnNum);
    resetEvaluator();
  }

  @Override
  public boolean isGroupAggregatedStreamingEvaluator() {
    return true;
  }

  @Override
  public void addStreamingGroupResults(List<Integer> groupRowCounts) {
    // Rank is advanced during batch forward; partition size alone is needed up
    // front.
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch) throws HiveException {
    if (partitionSize <= 0) {
      throw new HiveException("Partition size must be set before computing percent_rank");
    }
    final double divisor = partitionSize > 1 ? partitionSize - 1 : 1;
    DoubleColumnVector outputColVector = (DoubleColumnVector) batch.cols[outputColumnNum];
    outputColVector.isRepeating = true;
    outputColVector.noNulls = true;
    outputColVector.isNull[0] = false;
    outputColVector.vector[0] = (rank - 1) / divisor;
    groupCount += batch.size;
  }

  @Override
  public void doLastBatchWork() {
    rank += groupCount;
    groupCount = 0;
  }

  @Override
  public boolean streamsResult() {
    return true;
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.DOUBLE;
  }

  @Override
  public void resetEvaluator() {
    rank = 1;
    partitionSize = -1;
    groupCount = 0;
  }
}
