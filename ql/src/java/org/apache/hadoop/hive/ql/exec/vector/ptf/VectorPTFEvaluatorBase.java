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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.ptf.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the vector PTF evaluator base class.  An evaluator does the group batch aggregation work
 * on an aggregation's 0 or 1 argument(s) and at some point will fill in an output column with the
 * aggregation result.  The aggregation argument is an input column or expression, or no argument.
 *
 * When the aggregation is streaming (e.g. row_number, rank, first_value, etc), the output column
 * can be filled in immediately by the implementation of evaluateGroupBatch.
 *
 * For non-streaming aggregations, the aggregation result is not known until the last group batch
 * is processed.  After the last group batch has been processed, the VectorPTFGroupBatches class
 * will call the isGroupResultNull, getResultColumnVectorType, getLongGroupResult |
 * getDoubleGroupResult | getDecimalGroupResult, and getOutputColumnNum methods to get aggregation
 * result information necessary to write it into the output column (as a repeated column) of all
 * the group batches.
 */
public abstract class VectorPTFEvaluatorBase {

  final WindowFrameDef windowFrameDef;
  final VectorExpression inputVecExpr;
  protected int inputColumnNum;
  protected final int outputColumnNum;
  private boolean nullsLast;

  protected final Logger LOG = LoggerFactory.getLogger(getClass());

  public VectorPTFEvaluatorBase(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum) {
    this.windowFrameDef = windowFrameDef;
    if (inputVecExpr == null) {
      inputColumnNum = -1;
      this.inputVecExpr = null;
    } else {
      inputColumnNum = inputVecExpr.getOutputColumnNum();
      if (inputVecExpr instanceof IdentityExpression) {
        this.inputVecExpr = null;
      } else {
        this.inputVecExpr = inputVecExpr;
      }
    }
    this.outputColumnNum = outputColumnNum;
  }

  public VectorPTFEvaluatorBase(WindowFrameDef windowFrameDef, int outputColumnNum) {
    this.windowFrameDef = windowFrameDef;
    inputVecExpr = null;
    inputColumnNum = -1;
    this.outputColumnNum = outputColumnNum;
  }

  public boolean getNullsLast() {
    return nullsLast;
  }

  public void setNullsLast(boolean nullsLast) {
    this.nullsLast = nullsLast;
  }

  // Evaluate the aggregation input argument expression.
  public void evaluateInputExpr(VectorizedRowBatch batch) throws HiveException {
    if (inputVecExpr != null) {
      inputVecExpr.evaluate(batch);
    }
  }

  /*
   * Evaluate the aggregation over one of the group's batches. In this mode, evaluator is not
   * allowed to touch output columns, otherwise it can mess up buffered batches. This can happen,
   * when a streaming evaluator is used together with non-streaming ones. In this case, the
   * streaming evaluator should only calculate the evaluator results, which then can be collected by
   * VectorPTFGroupBatches.
   */
  public abstract void evaluateGroupBatch(VectorizedRowBatch batch)
      throws HiveException;

  // Do any work necessary after the last batch for a group has been processed.  Necessary
  // for both streaming and non-streaming evaluators..
  public void doLastBatchWork() {
    // By default, do nothing.
  }

  // Returns true if the aggregation result will be streamed.
  // Otherwise, we must evaluate whole group before producing a result.
  public abstract boolean streamsResult();

  public int getOutputColumnNum() {
    return outputColumnNum;
  }

  // After processing all the group's batches with evaluateGroupBatch, is the non-streaming
  // aggregation result null?
  public boolean isGroupResultNull() {
    throw new RuntimeException("Not implemented isGroupResultNull for " + this.getClass().getName());
  }

  // What is the ColumnVector type of the aggregation result?
  public abstract Type getResultColumnVectorType();

  /*
   * After processing all the non-streaming group's batches with evaluateGroupBatch and
   * isGroupResultNull is false, the aggregation result value (based on getResultColumnVectorType).
   */

  public Object getGroupResult(){
    throw new RuntimeException("No generic group result evaluator implementation " + this.getClass().getName());
  }

  // Resets the aggregation calculation variable(s).
  public abstract void resetEvaluator();

  /**
   * Whether the evaluator can calculate the result for a given range. The optimization is supposed
   * to save CPU/IO cost in case of evaluators that can provide the result without iterating through
   * the batches of a range. This method is called before every single calculation, so it lets the
   * evaluator store state whether it's able to run an optimized calculation in a given point of
   * time.
   */
  public boolean canRunOptimizedCalculation(int rowNum, Range range) {
    return false;
  }

  public Object runOnRange(int rowNum, Range range, VectorPTFGroupBatches batches)
      throws HiveException {
    if (canRunOptimizedCalculation(rowNum, range)) {
      throw new RuntimeException(
          "No optimized runOnRange is implemented for " + this.getClass().getName());
    } else {
      return null;
    }
  }

  /**
   * This method is called by VectorPTFGroupBatches once the calculation is finished for a given
   * row/range, but before the next calculation is invoked.
   *
   * @param result
   * @param range
   */
  public void onResultCalculated(Object result, Range range) {
  }

  /**
   * There might be some cleanup operations for evaluators that should not be taken care of after
   * every row (resetEvaluator), but rather on partition-level.
   */
  public void onPartitionEnd() {
  }

  /**
   * Whether the results calculated by this evaluator are eligible for caching by PTFValueCache. A
   * result of an evaluator is cacheable, if it returns the same value for the same range within the
   * a PTF partition, regardless of which row the current result is calculated for. That's typically
   * true for most of the evaluators (e.g. sum), but not true for e.g.
   * lead/lag/first_value/last_value, as their results only depend on a relation between the current
   * and some reference row (instead of ranges).
   */
  public boolean isCacheableForRange() {
    return true;
  }

  /**
   * VectorPTFGroupBatches class works on a subset of columns, which are mapped by an array of
   * changed indices. Some evaluators might refer to column indices, so they can adapt to the
   * changed column layout by this call.
   */
  public void mapCustomColumns(int[] bufferedColumnMap) {
  }
}