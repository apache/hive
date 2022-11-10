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

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.ptf.Range;

public abstract class VectorPTFEvaluatorAbstractAvg<T> extends VectorPTFEvaluatorBase {

  protected boolean isGroupResultNull;
  protected T sum;
  protected int nonNullGroupCount;

  protected Range previousRange = null;
  protected T previousSum = null;
  protected int previousCount;

  public VectorPTFEvaluatorAbstractAvg(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
  }

  @Override
  public boolean canRunOptimizedCalculation(int rowNum, Range range) {
    return previousRange != null && !(range.getSize() <= range.getDiff(previousRange));
  }

  /**
   * This optimization is the vectorized counterpart of HIVE-15617.
   */
  @Override
  public Object runOnRange(int rowNum, Range range, VectorPTFGroupBatches batches)
      throws HiveException {
    Range r1 = new Range(previousRange.getStart(), range.getStart(), batches);
    Range r2 = new Range(previousRange.getEnd(), range.getEnd(), batches);

    // don't reset this evaluator after calculation
    batches.runEvaluatorOnRange(this, r1, false);
    T sum1 = computeValue(sum);
    int count1 = nonNullGroupCount;
    resetEvaluator();

    batches.runEvaluatorOnRange(this, r2, false);
    T sum2 = computeValue(sum);
    int count2 = nonNullGroupCount;
    resetEvaluator();

    sum = plus(minus(computeValue(previousSum), sum1), sum2);
    nonNullGroupCount = previousCount - count1 + count2;

    return divide(sum, nonNullGroupCount);
  }

  @Override
  public void onResultCalculated(Object result, Range range) {
    this.previousSum = computeValue(sum);
    this.previousCount = nonNullGroupCount;
    this.previousRange = range;
  }

  @Override
  public void onPartitionEnd() {
    previousSum = null;
    previousCount = 0;
    previousRange = null;
  }

  @Override
  public boolean streamsResult() {
    // We must evaluate whole group before producing a result.
    return false;
  }

  protected abstract T computeValue(T number);

  protected abstract T plus(T number1, T number2);

  protected abstract T minus(T number1, T number2);

  protected abstract Object divide(T number, long divisor);

  @Override
  public boolean isGroupResultNull() {
    return isGroupResultNull;
  }
}
