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
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;

/**
 * This class evaluates count(*) for a PTF group.
 *
 * Count all rows of the group.  No input column/expression.
 */
public class VectorPTFEvaluatorCountStar extends VectorPTFEvaluatorBase {

  protected long count;

  public VectorPTFEvaluatorCountStar(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    resetEvaluator();
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch) {
    // No input expression for COUNT(*).
    // evaluateInputExpr(batch);

    // Count all rows.

    count += batch.size;
  }

  @Override
  public boolean streamsResult() {
    // We must evaluate whole group before producing a result.
    return false;
  }

  @Override
  public boolean isGroupResultNull() {
    return false;
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.LONG;
  }

  @Override
  public long getLongGroupResult() {
    return count;
  }

  @Override
  public void resetEvaluator() {
    count = 0;
  }
}