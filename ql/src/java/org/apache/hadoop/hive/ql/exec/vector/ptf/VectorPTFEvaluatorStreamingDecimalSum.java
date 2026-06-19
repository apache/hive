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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import com.google.common.base.Preconditions;

/**
 * This class evaluates HiveDecimal sum() for a PTF group.
 */
public class VectorPTFEvaluatorStreamingDecimalSum extends VectorPTFEvaluatorBase {

  protected boolean isNull;
  protected HiveDecimalWritable sum;

  public VectorPTFEvaluatorStreamingDecimalSum(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    sum = new HiveDecimalWritable();
    resetEvaluator();
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch)
      throws HiveException {

    evaluateInputExpr(batch);

    // Sum all non-null decimal column values; maintain isGroupResultNull.

    // We do not filter when PTF is in reducer.
    Preconditions.checkState(!batch.selectedInUse);

    final int size = batch.size;
    if (size == 0) {
      return;
    }
    DecimalColumnVector decimalColVector = ((DecimalColumnVector) batch.cols[inputColumnNum]);

    DecimalColumnVector outputColVector = (DecimalColumnVector) batch.cols[outputColumnNum];

    if (decimalColVector.isRepeating) {

      if (decimalColVector.noNulls || !decimalColVector.isNull[0]) {

        // We have a repeated value.
        isNull = false;
        HiveDecimalWritable repeatedValue = decimalColVector.vector[0];

        for (int i = 0; i < size; i++) {
          sum.mutateAdd(repeatedValue);

          // Output row i SUM.
          outputColVector.set(i, sum);
        }
      } else {
        if (isNull) {
          outputColVector.isNull[0] = true;
          outputColVector.noNulls = false;
        } else {

          // Continue previous SUM.
          outputColVector.set(0, sum);
        }
        outputColVector.isRepeating = true;
      }
    } else if (decimalColVector.noNulls) {
      isNull = false;
      HiveDecimalWritable[] vector = decimalColVector.vector;
      for (int i = 0; i < size; i++) {
        sum.mutateAdd(vector[i]);

        // Output row i sum.
        outputColVector.set(i, sum);
      }
    } else {
      boolean[] batchIsNull = decimalColVector.isNull;
      int i = 0;
      while (batchIsNull[i]) {
        if (isNull) {
          outputColVector.isNull[i] = true;
          outputColVector.noNulls = false;
        } else {

          // Continue previous SUM.
          outputColVector.set(i, sum);
        }
        if (++i >= size) {
          return;
        }
      }

      isNull = false;
      HiveDecimalWritable[] vector = decimalColVector.vector;

      sum.mutateAdd(vector[i++]);

      // Output row i sum.
      outputColVector.set(i, sum);

      for (; i < size; i++) {
        if (!batchIsNull[i]) {
          sum.mutateAdd(vector[i]);
          outputColVector.set(i, sum);
        } else {

          // Continue previous SUM.
          outputColVector.set(i, sum);
        }
      }
    }
  }

  @Override
  public boolean streamsResult() {
    // No group value.
    return true;
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.DECIMAL;
  }

  @Override
  public void resetEvaluator() {
    isNull = true;
    sum.set(HiveDecimal.ZERO);;
  }
}