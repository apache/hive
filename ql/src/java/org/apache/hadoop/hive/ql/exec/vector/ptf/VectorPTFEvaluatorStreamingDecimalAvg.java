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
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import com.google.common.base.Preconditions;

/**
 * This class evaluates streaming HiveDecimal avg() for a PTF group.
 *
 * Stream average non-null column values and output sum / non-null count as we go along.
 */
public class VectorPTFEvaluatorStreamingDecimalAvg extends VectorPTFEvaluatorBase {

  protected boolean isNull;
  protected HiveDecimalWritable sum;
  private int nonNullGroupCount;
  private HiveDecimalWritable temp;
  private HiveDecimalWritable avg;

  public VectorPTFEvaluatorStreamingDecimalAvg(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    sum = new HiveDecimalWritable();
    temp = new HiveDecimalWritable();
    avg = new HiveDecimalWritable();
    resetEvaluator();
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch)
      throws HiveException {

    evaluateInputExpr(batch);

    // Sum all non-null decimal column values for avg; maintain isGroupResultNull; after last row of
    // last group batch compute the group avg when sum is non-null.

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
          nonNullGroupCount++;

          // Output row i AVG.
          avg.set(sum);
          temp.setFromLong(nonNullGroupCount);
          avg.mutateDivide(temp);
          outputColVector.set(i, avg);
        }
      } else {
        if (isNull) {
          outputColVector.isNull[0] = true;
          outputColVector.noNulls = false;
        } else {

          // Continue previous AVG.
          outputColVector.set(0, avg);
        }
        outputColVector.isRepeating = true;
      }
    } else if (decimalColVector.noNulls) {
      isNull = false;
      HiveDecimalWritable[] vector = decimalColVector.vector;
      for (int i = 0; i < size; i++) {
        sum.mutateAdd(vector[i]);
        nonNullGroupCount++;

        // Output row i AVG.
        avg.set(sum);
        temp.setFromLong(nonNullGroupCount);
        avg.mutateDivide(temp);
        outputColVector.set(i, avg);
      }
    } else {
      boolean[] batchIsNull = decimalColVector.isNull;
      int i = 0;
      while (batchIsNull[i]) {
        if (isNull) {
          outputColVector.isNull[i] = true;
          outputColVector.noNulls = false;
        } else {

          // Continue previous AVG.
          outputColVector.set(i, avg);
        }
        if (++i >= size) {
          return;
        }
      }

      isNull = false;
      HiveDecimalWritable[] vector = decimalColVector.vector;

      sum.mutateAdd(vector[i]);
      nonNullGroupCount++;

      // Output row i AVG.
      avg.set(sum);
      temp.setFromLong(nonNullGroupCount);
      avg.mutateDivide(temp);

      outputColVector.set(i++, avg);

      for (; i < size; i++) {
        if (!batchIsNull[i]) {
          sum.mutateAdd(vector[i]);
          nonNullGroupCount++;

          avg.set(sum);
          temp.setFromLong(nonNullGroupCount);
          avg.mutateDivide(temp);

          // Output row i AVG.
          outputColVector.set(i, avg);
        } else {

          // Continue previous AVG.
          outputColVector.set(i, avg);
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
    sum.set(HiveDecimal.ZERO);
    nonNullGroupCount = 0;
    avg.set(HiveDecimal.ZERO);
  }
}