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

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.IntervalDayTimeColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Compute IF(expr1, expr2, expr3) for 3 input column expressions.
 * The first is always a boolean (LongColumnVector).
 * The second is a column or non-constant expression result.
 * The third is a constant value.
 */
public class IfExprIntervalDayTimeColumnScalar extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private final int arg1Column;
  private final int arg2Column;
  private final HiveIntervalDayTime arg3Scalar;

  public IfExprIntervalDayTimeColumnScalar(int arg1Column, int arg2Column, HiveIntervalDayTime arg3Scalar,
      int outputColumnNum) {
    super(outputColumnNum);
    this.arg1Column = arg1Column;
    this.arg2Column = arg2Column;
    this.arg3Scalar = arg3Scalar;
  }

  public IfExprIntervalDayTimeColumnScalar() {
    super();

    // Dummy final assignments.
    arg1Column = -1;
    arg2Column = -1;
    arg3Scalar = null;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector arg1ColVector = (LongColumnVector) batch.cols[arg1Column];
    IntervalDayTimeColumnVector arg2ColVector = (IntervalDayTimeColumnVector) batch.cols[arg2Column];
    IntervalDayTimeColumnVector outputColVector = (IntervalDayTimeColumnVector) batch.cols[outputColumnNum];
    int[] sel = batch.selected;
    boolean[] outputIsNull = outputColVector.isNull;

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    int n = batch.size;
    long[] vector1 = arg1ColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    if (arg1ColVector.isRepeating) {
      if ((arg1ColVector.noNulls || !arg1ColVector.isNull[0]) && vector1[0] == 1) {
        arg2ColVector.copySelected(batch.selectedInUse, sel, n, outputColVector);
      } else {
        outputColVector.fill(arg3Scalar);
      }
      return;
    }

    // Extend any repeating values and noNulls indicator in the inputs to
    // reduce the number of code paths needed below.
    arg2ColVector.flatten(batch.selectedInUse, sel, n);

    if (arg1ColVector.noNulls) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = false;
          outputColVector.set(i, vector1[i] == 1 ? arg2ColVector.asScratchIntervalDayTime(i) : arg3Scalar);
        }
      } else {
        Arrays.fill(outputIsNull, 0, n, false);
        for(int i = 0; i != n; i++) {
          outputColVector.set(i, vector1[i] == 1 ? arg2ColVector.asScratchIntervalDayTime(i) : arg3Scalar);
        }
      }
    } else /* there are nulls */ {

      // Carefully handle NULLs...

      /*
       * For better performance on LONG/DOUBLE we don't want the conditional
       * statements inside the for loop.
       */
      outputColVector.noNulls = false;

      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputColVector.set(i, !arg1ColVector.isNull[i] && vector1[i] == 1 ?
              arg2ColVector.asScratchIntervalDayTime(i) : arg3Scalar);
          outputIsNull[i] = (!arg1ColVector.isNull[i] && vector1[i] == 1 ?
              arg2ColVector.isNull[i] : false);
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputColVector.set(i, !arg1ColVector.isNull[i] && vector1[i] == 1 ?
              arg2ColVector.asScratchIntervalDayTime(i) : arg3Scalar);
          outputIsNull[i] = (!arg1ColVector.isNull[i] && vector1[i] == 1 ?
              arg2ColVector.isNull[i] : false);
        }
      }
    }

    // restore repeating and no nulls indicators
    arg2ColVector.unFlatten();
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, arg1Column) + ", " + getColumnParamString(1, arg2Column) +
        ", val "+ arg3Scalar.toString();
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(3)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.getType("int_family"),
            VectorExpressionDescriptor.ArgumentType.getType("interval_day_time"),
            VectorExpressionDescriptor.ArgumentType.getType("interval_day_time"))
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}