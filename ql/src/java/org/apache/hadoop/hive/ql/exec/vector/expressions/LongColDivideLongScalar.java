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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This operation is handled as a special case because Hive
 * long/long division returns double. This file is thus not generated
 * from a template like the other arithmetic operations are.
 */
public class LongColDivideLongScalar extends VectorExpression {
  private static final long serialVersionUID = 1L;
  private int colNum;
  private long value;
  private int outputColumn;

  public LongColDivideLongScalar(int colNum, long value, int outputColumn) {
    this();
    this.colNum = colNum;
    this.value = value;
    this.outputColumn = outputColumn;
  }

  public LongColDivideLongScalar() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputColVector = (LongColumnVector) batch.cols[colNum];
    DoubleColumnVector outputColVector = (DoubleColumnVector) batch.cols[outputColumn];
    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    outputColVector.noNulls = inputColVector.noNulls;
    outputColVector.isRepeating = inputColVector.isRepeating;
    int n = batch.size;
    long[] vector = inputColVector.vector;
    double[] outputVector = outputColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    if (inputColVector.isRepeating) {
      outputVector[0] = vector[0] / (double) value;

      // Even if there are no nulls, we always copy over entry 0. Simplifies code.
      outputIsNull[0] = inputIsNull[0];
    } else if (inputColVector.noNulls) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = vector[i] / (double) value;
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputVector[i] = vector[i] / (double) value;
        }
      }
    } else /* there are nulls */ {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = vector[i] / (double) value;
          outputIsNull[i] = inputIsNull[i];
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputVector[i] = vector[i] / (double) value;
        }
        System.arraycopy(inputIsNull, 0, outputIsNull, 0, n);
      }
    }

    /* Set double data vector array entries for NULL elements to the correct value.
     * Unlike other col-scalar operations, this one doesn't benefit from carrying
     * over NaN values from the input array.
     */
    NullUtil.setNullDataEntriesDouble(outputColVector, batch.selectedInUse, sel, n);
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  @Override
  public String getOutputType() {
    return "double";
  }

  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  public long getValue() {
    return value;
  }

  public void setValue(long value) {
    this.value = value;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.LONG,
            VectorExpressionDescriptor.ArgumentType.LONG)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
