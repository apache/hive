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
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import com.google.common.base.Preconditions;

/**
 * This class evaluates HiveDecimal sum() for a PTF group.
 */
public class VectorPTFEvaluatorDecimalSum extends VectorPTFEvaluatorBase {

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorPTFEvaluatorDecimalSum.class.getName();
  private static final Log LOG = LogFactory.getLog(CLASS_NAME);

  protected boolean isGroupResultNull;
  protected HiveDecimalWritable sum;
  protected HiveDecimalWritable temp;

  public VectorPTFEvaluatorDecimalSum(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    sum = new HiveDecimalWritable();
    temp = new HiveDecimalWritable();
    resetEvaluator();
  }

  public void evaluateGroupBatch(VectorizedRowBatch batch, boolean isLastGroupBatch) {
    evaluateInputExpr(batch);

    // Sum all non-null decimal column values; maintain isGroupResultNull.

    // We do not filter when PTF is in reducer.
    Preconditions.checkState(!batch.selectedInUse);

    final int size = batch.size;
    if (size == 0) {
      return;
    }
    DecimalColumnVector decimalColVector = ((DecimalColumnVector) batch.cols[inputColumnNum]);
    if (decimalColVector.isRepeating) {

      if (decimalColVector.noNulls || !decimalColVector.isNull[0]) {
        temp.setFromLong(batch.size);
        if (isGroupResultNull) {

          // First aggregation calculation for group.
          sum.set(decimalColVector.vector[0]);
          sum.mutateMultiply(temp);
          isGroupResultNull = false;
        } else {
          temp.mutateMultiply(decimalColVector.vector[0]);
          sum.mutateAdd(temp);
        }
      }
    } else if (decimalColVector.noNulls) {
      HiveDecimalWritable[] vector = decimalColVector.vector;
      if (isGroupResultNull) {

        // First aggregation calculation for group.
        sum.set(vector[0]);
        isGroupResultNull = false;
      } else {
        sum.mutateAdd(vector[0]);
      }
      for (int i = 1; i < size; i++) {
        sum.mutateAdd(vector[i]);
      }
    } else {
      boolean[] batchIsNull = decimalColVector.isNull;
      int i = 0;
      while (batchIsNull[i]) {
        if (++i >= size) {
          return;
        }
      }
      HiveDecimalWritable[] vector = decimalColVector.vector;
      if (isGroupResultNull) {

        // First aggregation calculation for group.
        sum.set(vector[i++]);
        isGroupResultNull = false;
      } else {
        sum.mutateAdd(vector[i++]);
      }
      for (; i < size; i++) {
        if (!batchIsNull[i]) {
          sum.mutateAdd(vector[i]);
        }
      }
    }
  }

  @Override
  public boolean isGroupResultNull() {
    return isGroupResultNull;
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.DECIMAL;
  }

  @Override
  public HiveDecimalWritable getDecimalGroupResult() {
    return sum;
  }

  @Override
  public void resetEvaluator() {
    isGroupResultNull = true;
    sum.set(HiveDecimal.ZERO);;
  }
}