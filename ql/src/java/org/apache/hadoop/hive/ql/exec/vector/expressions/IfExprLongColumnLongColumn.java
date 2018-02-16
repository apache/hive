/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Compute IF(expr1, expr2, expr3) for 3 input column expressions.
 * The first is always a boolean (LongColumnVector).
 * The second and third are long columns or long expression results.
 */
public class IfExprLongColumnLongColumn extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private final int arg1Column;
  private final int arg2Column;
  private final int arg3Column;

  public IfExprLongColumnLongColumn(int arg1Column, int arg2Column, int arg3Column,
      int outputColumnNum) {
    super(outputColumnNum);
    this.arg1Column = arg1Column;
    this.arg2Column = arg2Column;
    this.arg3Column = arg3Column;
  }

  public IfExprLongColumnLongColumn() {
    super();

    // Dummy final assignments.
    arg1Column = -1;
    arg2Column = -1;
    arg3Column = -1;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector arg1ColVector = (LongColumnVector) batch.cols[arg1Column];
    LongColumnVector arg2ColVector = (LongColumnVector) batch.cols[arg2Column];
    LongColumnVector arg3ColVector = (LongColumnVector) batch.cols[arg3Column];
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];
    int[] sel = batch.selected;
    boolean[] outputIsNull = outputColVector.isNull;

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    int n = batch.size;
    long[] vector1 = arg1ColVector.vector;
    long[] vector2 = arg2ColVector.vector;
    long[] vector3 = arg3ColVector.vector;
    long[] outputVector = outputColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    /* All the code paths below propagate nulls even if neither arg2 nor arg3
     * have nulls. This is to reduce the number of code paths and shorten the
     * code, at the expense of maybe doing unnecessary work if neither input
     * has nulls. This could be improved in the future by expanding the number
     * of code paths.
     */
    if (arg1ColVector.isRepeating) {
      if (vector1[0] == 1) {
        arg2ColVector.copySelected(batch.selectedInUse, sel, n, outputColVector);
      } else {
        arg3ColVector.copySelected(batch.selectedInUse, sel, n, outputColVector);
      }
      return;
    }

    // extend any repeating values and noNulls indicator in the inputs
    arg2ColVector.flatten(batch.selectedInUse, sel, n);
    arg3ColVector.flatten(batch.selectedInUse, sel, n);

    // Carefully handle NULLs...
    outputColVector.noNulls = false;

    if (arg1ColVector.noNulls) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = (~(vector1[i] - 1) & vector2[i]) | ((vector1[i] - 1) & vector3[i]);
          outputIsNull[i] = (vector1[i] == 1 ?
              arg2ColVector.isNull[i] : arg3ColVector.isNull[i]);
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputVector[i] = (~(vector1[i] - 1) & vector2[i]) | ((vector1[i] - 1) & vector3[i]);
          outputIsNull[i] = (vector1[i] == 1 ?
              arg2ColVector.isNull[i] : arg3ColVector.isNull[i]);
        }
      }
    } else /* there are nulls */ {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = (!arg1ColVector.isNull[i] && vector1[i] == 1 ?
              vector2[i] : vector3[i]);
          outputIsNull[i] = (!arg1ColVector.isNull[i] && vector1[i] == 1 ?
              arg2ColVector.isNull[i] : arg3ColVector.isNull[i]);
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputVector[i] = (!arg1ColVector.isNull[i] && vector1[i] == 1 ?
              vector2[i] : vector3[i]);
          outputIsNull[i] = (!arg1ColVector.isNull[i] && vector1[i] == 1 ?
              arg2ColVector.isNull[i] : arg3ColVector.isNull[i]);
        }
      }
    }

    // restore repeating and no nulls indicators
    arg2ColVector.unFlatten();
    arg3ColVector.unFlatten();
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, arg1Column) + ", " + getColumnParamString(1, arg2Column) +
        ", " + getColumnParamString(1, arg3Column);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(3)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.getType("long"),
            VectorExpressionDescriptor.ArgumentType.getType("int_date_interval_year_month"),
            VectorExpressionDescriptor.ArgumentType.getType("int_date_interval_year_month"))
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}