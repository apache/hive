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

import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;

/**
 * Evaluates {@code cume_dist()} as a <b>group-aggregated streaming</b> evaluator — the third
 * of three evaluator categories (see {@link VectorPTFEvaluatorBase}).
 *
 * <p>It is both <b>buffered</b> and <b>streaming</b>: the operator buffers the full partition
 * before any output ({@link #isGroupAggregatedStreamingEvaluator()} {@code == true}), then
 * writes results per batch into column vectors on forward ({@link #streamsResult()}
 * {@code == true}). Peer-group values are precomputed in {@link #addStreamingGroupResult(int)}
 * after {@link #setPartitionSize(int)}, then {@link #evaluateGroupBatch} fills the output column
 * as buffered batches are replayed.
 */
public class VectorPTFEvaluatorCumeDist extends VectorPTFEvaluatorBase {

  /**
   * Per-peer-group cume_dist values from the first pass, popped in order during
   * streaming
   * (one per group at its last batch).
   */
  private final Deque<Double> groupResults = new ArrayDeque<>();
  private int rowPosition;

  public VectorPTFEvaluatorCumeDist(WindowFrameDef windowFrameDef, int outputColumnNum) {
    super(windowFrameDef, outputColumnNum);
    resetEvaluator();
  }

  @Override
  public boolean isGroupAggregatedStreamingEvaluator() {
    return true;
  }

  @Override
  public void addStreamingGroupResult(int groupRowCount) throws HiveException {
    if (partitionSize <= 0) {
      throw new HiveException("Partition size must be set before precomputing cume_dist");
    }
    rowPosition += groupRowCount;
    groupResults.addLast(((double) rowPosition) / partitionSize);
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch) throws HiveException {
    Double result = groupResults.peekFirst();
    if (result == null) {
      throw new HiveException("cume_dist streaming result is not available for the current group");
    }
    DoubleColumnVector outputColVector = (DoubleColumnVector) batch.cols[outputColumnNum];
    outputColVector.isRepeating = true;
    outputColVector.noNulls = true;
    outputColVector.isNull[0] = false;
    outputColVector.vector[0] = result;
  }

  @Override
  public void doLastBatchWork() {
    groupResults.pollFirst();
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
    rowPosition = 0;
    partitionSize = -1;
    groupResults.clear();
  }
}
