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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This operation is handled as a special case because Hive
 * long/long division returns double. This file is thus not generated
 * from a template like the other arithmetic operations are.
 */
public class LongScalarDivideLongColumn extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private final int colNum;
  private final double value;

  public LongScalarDivideLongColumn(long value, int colNum, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum = colNum;
    this.value = (double) value;
  }

  public LongScalarDivideLongColumn() {
    super();

    // Dummy final assignments.
    colNum = -1;
    value = 0;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputColVector = (LongColumnVector) batch.cols[colNum];
    DoubleColumnVector outputColVector = (DoubleColumnVector) batch.cols[outputColumnNum];
    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    int n = batch.size;
    long[] vector = inputColVector.vector;
    double[] outputVector = outputColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    boolean hasDivBy0 = false;
    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        outputIsNull[0] = false;
        long denom = vector[0];
        outputVector[0] = value / denom;
        hasDivBy0 = hasDivBy0 || (denom == 0);
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
    } else if (inputColVector.noNulls) {
      if (batch.selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outputColVector.noNulls) {
          for(int j = 0; j != n; j++) {
           final int i = sel[j];
           // Set isNull before call in case it changes it mind.
           outputIsNull[i] = false;
           long denom = vector[i];
           outputVector[i] = value / denom;
           hasDivBy0 = hasDivBy0 || (denom == 0);
         }
        } else {
          for(int j = 0; j != n; j++) {
            final int i = sel[j];
            long denom = vector[i];
            outputVector[i] = value / denom;
            hasDivBy0 = hasDivBy0 || (denom == 0);
          }
        }
      } else {
        if (!outputColVector.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        for(int i = 0; i != n; i++) {
          long denom = vector[i];
          outputVector[i] = value / denom;
          hasDivBy0 = hasDivBy0 || (denom == 0);
        }
      }
    } else /* there are nulls */ {

      // Carefully handle NULLs...
      outputColVector.noNulls = false;

      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          long denom = vector[i];
          outputVector[i] = value / denom;
          hasDivBy0 = hasDivBy0 || (denom == 0);
          outputIsNull[i] = inputIsNull[i];
        }
      } else {
        System.arraycopy(inputIsNull, 0, outputIsNull, 0, n);
        for(int i = 0; i != n; i++) {
          long denom = vector[i];
          outputVector[i] = value / denom;
          hasDivBy0 = hasDivBy0 || (denom == 0);
        }
      }
    }

    /* Set double data vector array entries for NULL elements to the correct value.
     * Unlike other col-scalar operations, this one doesn't benefit from carrying
     * over NaN values from the input array.
     */
    if (!hasDivBy0) {
      NullUtil.setNullDataEntriesDouble(outputColVector, batch.selectedInUse, sel, n);
    } else {
      NullUtil.setNullAndDivBy0DataEntriesDouble(
          outputColVector, batch.selectedInUse, sel, n, inputColVector);
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return "val " + value + ", " + getColumnParamString(1, colNum);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.SCALAR,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
