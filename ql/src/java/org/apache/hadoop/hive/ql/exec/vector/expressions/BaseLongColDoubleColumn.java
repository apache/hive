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

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public abstract class BaseLongColDoubleColumn extends VectorExpression {
  private static final long serialVersionUID = 1L;

  public BaseLongColDoubleColumn(int colNum1, int colNum2, int outputColumnNum) {
    super(colNum1, colNum2, outputColumnNum);
  }

  public BaseLongColDoubleColumn() {
    super();
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumnNum[0]) + ", " + getColumnParamString(1, inputColumnNum[1]);
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    // return immediately if batch is empty
    final int n = batch.size;
    if (n == 0) {
      return;
    }

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputColVector1 = (LongColumnVector) batch.cols[inputColumnNum[0]];
    DoubleColumnVector inputColVector2 = (DoubleColumnVector) batch.cols[inputColumnNum[1]];
    DoubleColumnVector outputColVector = (DoubleColumnVector) batch.cols[outputColumnNum];
    int[] sel = batch.selected;

    long[] vector1 = inputColVector1.vector;
    double[] vector2 = inputColVector2.vector;
    double[] outputVector = outputColVector.vector;

    /*
     * Propagate null values for a two-input operator and set isRepeating and noNulls appropriately.
     */
    NullUtil.propagateNullsColCol(
        inputColVector1, inputColVector2, outputColVector, sel, n, batch.selectedInUse);

    /* Disregard nulls for processing. In other words,
     * the arithmetic operation is performed even if one or
     * more inputs are null. This is to improve speed by avoiding
     * conditional checks in the inner loop.
     */
    if (inputColVector1.isRepeating && inputColVector2.isRepeating) {
      outputVector[0] = func(vector1[0], vector2[0]);
    } else if (inputColVector1.isRepeating) {
      final long vector1Value = vector1[0];
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = func(vector1Value, vector2[i]);
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputVector[i] = func(vector1Value, vector2[i]);
        }
      }
    } else if (inputColVector2.isRepeating) {
      final double vector2Value = vector2[0];
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = func(vector1[i], vector2Value);
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputVector[i] = func(vector1[i], vector2Value);
        }
      }
    } else {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = func(vector1[i], vector2[i]);
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputVector[i] = func(vector1[i],  vector2[i]);
        }
      }
    }

    if (supportsCheckedExecution()) {
      OverflowUtils.accountForOverflowDouble(getOutputTypeInfo(), outputColVector,
          batch.selectedInUse, sel, n);
    }

    /* For the case when the output can have null values, follow
     * the convention that the data values must be 1 for long and
     * NaN for double. This is to prevent possible later zero-divide errors
     * in complex arithmetic expressions like col2 / (col1 - 1)
     * in the case when some col1 entries are null.
     */
    NullUtil.setNullDataEntriesDouble(outputColVector, batch.selectedInUse, sel, n);
  }

  protected abstract double func(long a, double b);

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY,
            VectorExpressionDescriptor.ArgumentType.FLOAT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }

  public static class Add extends BaseLongColDoubleColumn {
    public Add(int colNum1, int colNum2, int outputColumnNum) {
      super(colNum1, colNum2, outputColumnNum);
    }

    public Add() {
      super();
    }

    @Override
    protected double func(long a, double b) {
      return a + b;
    }
  }

  public static class Multiply extends BaseLongColDoubleColumn {
    public Multiply(int colNum1, int colNum2, int outputColumnNum) {
      super(colNum1, colNum2, outputColumnNum);
    }

    public Multiply() {
      super();
    }

    @Override
    protected double func(long a, double b) {
      return a * b;
    }
  }

  public static class Subtract extends BaseLongColDoubleColumn {
    public Subtract(int colNum1, int colNum2, int outputColumnNum) {
      super(colNum1, colNum2, outputColumnNum);
    }

    public Subtract() {
      super();
    }

    @Override
    protected double func(long a, double b) {
      return a - b;
    }
  }

  public static class CheckedAdd extends BaseLongColLongColumn.Add {
    public CheckedAdd(int colNum1, int colNum2, int outputColumnNum) {
      super(colNum1, colNum2, outputColumnNum);
    }

    public CheckedAdd() {
      super();
    }

    @Override
    public boolean supportsCheckedExecution() {
      return true;
    }
  }

  public static class CheckedMultiply extends BaseLongColLongColumn.Multiply {
    public CheckedMultiply(int colNum1, int colNum2, int outputColumnNum) {
      super(colNum1, colNum2, outputColumnNum);
    }

    public CheckedMultiply() {
      super();
    }

    @Override
    public boolean supportsCheckedExecution() {
      return true;
    }
  }

  public static class CheckedSubtract extends BaseLongColLongColumn.Subtract {
    public CheckedSubtract(int colNum1, int colNum2, int outputColumnNum) {
      super(colNum1, colNum2, outputColumnNum);
    }

    public CheckedSubtract() {
      super();
    }

    @Override
    public boolean supportsCheckedExecution() {
      return true;
    }
  }
}
