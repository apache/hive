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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

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

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorPTFEvaluatorBase.class.getName();
  private static final Log LOG = LogFactory.getLog(CLASS_NAME);

  protected final WindowFrameDef windowFrameDef;
  private final VectorExpression inputVecExpr;
  protected final int inputColumnNum;
  protected final int outputColumnNum;

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

  // Evaluate the aggregation input argument expression.
  public void evaluateInputExpr(VectorizedRowBatch batch) {
    if (inputVecExpr != null) {
      inputVecExpr.evaluate(batch);
    }
  }

  // Evaluate the aggregation over one of the group's batches.
  public abstract void evaluateGroupBatch(VectorizedRowBatch batch, boolean isLastGroupBatch);

  // Returns true if the aggregation result will be streamed.
  public boolean streamsResult() {
    // Assume it is not streamjng by default.
    return false;
  }

  public int getOutputColumnNum() {
    return outputColumnNum;
  }

  // After processing all the group's batches with evaluateGroupBatch, is the non-streaming
  // aggregation result null?
  public boolean isGroupResultNull() {
    return false;
  }

  // What is the ColumnVector type of the aggregation result?
  public abstract Type getResultColumnVectorType();

  /*
   * After processing all the non-streaming group's batches with evaluateGroupBatch and
   * isGroupResultNull is false, the aggregation result value (based on getResultColumnVectorType).
   */

  public long getLongGroupResult() {
    throw new RuntimeException("No long group result evaluator implementation " + this.getClass().getName());
  }

  public double getDoubleGroupResult() {
    throw new RuntimeException("No double group result evaluator implementation " + this.getClass().getName());
  }

  public HiveDecimalWritable getDecimalGroupResult() {
    throw new RuntimeException("No decimal group result evaluator implementation " + this.getClass().getName());
  }

  // Resets the aggregation calculation variable(s).
  public abstract void resetEvaluator();
}