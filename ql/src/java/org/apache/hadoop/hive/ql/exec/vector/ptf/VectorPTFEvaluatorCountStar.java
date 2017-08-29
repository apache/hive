/**
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
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;

import com.google.common.base.Preconditions;

/**
 * This class evaluates count(*) for a PTF group.
 *
 * Count all rows of the group.  No input column/expression.
 */
public class VectorPTFEvaluatorCountStar extends VectorPTFEvaluatorBase {

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorPTFEvaluatorCountStar.class.getName();
  private static final Log LOG = LogFactory.getLog(CLASS_NAME);

  protected long count;

  public VectorPTFEvaluatorCountStar(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    resetEvaluator();
  }

  public void evaluateGroupBatch(VectorizedRowBatch batch, boolean isLastGroupBatch) {
    // No input expression for COUNT(*).
    // evaluateInputExpr(batch);

    // Count all rows.

    count += batch.size;
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