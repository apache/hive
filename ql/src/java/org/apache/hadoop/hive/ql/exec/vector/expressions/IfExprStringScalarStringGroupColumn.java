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

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;

/**
 * Compute IF(expr1, expr2, expr3) for 3 input column expressions.
 * The first is always a boolean (LongColumnVector).
 * The second is a string scalar.
 * The third is a string column or non-constant expression result.
 */
public class IfExprStringScalarStringGroupColumn extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private final int arg1Column;
  private final byte[] arg2Scalar;
  private final int arg3Column;


  public IfExprStringScalarStringGroupColumn(int arg1Column, byte[] arg2Scalar, int arg3Column,
      int outputColumnNum) {
    super(outputColumnNum);
    this.arg1Column = arg1Column;
    this.arg2Scalar = arg2Scalar;
    this.arg3Column = arg3Column;
  }

  public IfExprStringScalarStringGroupColumn() {
    super();

    // Dummy final assignments.
    arg1Column = -1;
    arg2Scalar = null;
    arg3Column = -1;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector arg1ColVector = (LongColumnVector) batch.cols[arg1Column];
    BytesColumnVector arg3ColVector = (BytesColumnVector) batch.cols[arg3Column];
    BytesColumnVector outputColVector = (BytesColumnVector) batch.cols[outputColumnNum];
    int[] sel = batch.selected;
    boolean[] outputIsNull = outputColVector.isNull;

    if (!outputColVector.noNulls) {
      // TEMPORARILY:
      outputColVector.reset();
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    int n = batch.size;
    long[] vector1 = arg1ColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    outputColVector.initBuffer();

    /* All the code paths below propagate nulls even arg3 has no
     * nulls. This is to reduce the number of code paths and shorten the
     * code, at the expense of maybe doing unnecessary work if neither input
     * has nulls. This could be improved in the future by expanding the number
     * of code paths.
     */
    if (arg1ColVector.isRepeating) {
      if ((arg1ColVector.noNulls || !arg1ColVector.isNull[0]) && vector1[0] == 1) {
        outputColVector.fill(arg2Scalar);
      } else {
        arg3ColVector.copySelected(batch.selectedInUse, sel, n, outputColVector);
      }
      return;
    }

    // extend any repeating values and noNulls indicator in the input
    arg3ColVector.flatten(batch.selectedInUse, sel, n);

    /*
     * Do careful maintenance of NULLs.
     */
    outputColVector.noNulls = false;

    if (arg1ColVector.noNulls) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          if (vector1[i] == 1) {
            outputColVector.setRef(i, arg2Scalar, 0, arg2Scalar.length);
          } else {
            if (!arg3ColVector.isNull[i]) {
              outputColVector.setVal(
                  i, arg3ColVector.vector[i], arg3ColVector.start[i], arg3ColVector.length[i]);
            }
          }
          outputIsNull[i] = (vector1[i] == 1 ? false : arg3ColVector.isNull[i]);
        }
      } else {
        for(int i = 0; i != n; i++) {
          if (vector1[i] == 1) {
            outputColVector.setRef(i, arg2Scalar, 0, arg2Scalar.length);
          } else {
            if (!arg3ColVector.isNull[i]) {
              outputColVector.setVal(
                  i, arg3ColVector.vector[i], arg3ColVector.start[i], arg3ColVector.length[i]);
            }
          }
          outputIsNull[i] = (vector1[i] == 1 ? false : arg3ColVector.isNull[i]);
        }
      }
    } else /* there are nulls */ {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          if (!arg1ColVector.isNull[i] && vector1[i] == 1) {
            outputColVector.setRef(i, arg2Scalar, 0, arg2Scalar.length);
          } else {
            if (!arg3ColVector.isNull[i]) {
              outputColVector.setVal(
                  i, arg3ColVector.vector[i], arg3ColVector.start[i], arg3ColVector.length[i]);
            }
          }
          outputIsNull[i] = (!arg1ColVector.isNull[i] && vector1[i] == 1 ?
              false : arg3ColVector.isNull[i]);
        }
      } else {
        for(int i = 0; i != n; i++) {
          if (!arg1ColVector.isNull[i] && vector1[i] == 1) {
            outputColVector.setRef(i, arg2Scalar, 0, arg2Scalar.length);
          } else {
            if (!arg3ColVector.isNull[i]) {
              outputColVector.setVal(
                  i, arg3ColVector.vector[i], arg3ColVector.start[i], arg3ColVector.length[i]);
            }
          }
          outputIsNull[i] = (!arg1ColVector.isNull[i] && vector1[i] == 1 ?
              false : arg3ColVector.isNull[i]);
        }
      }
    }

    // restore state of repeating and non nulls indicators
    arg3ColVector.unFlatten();
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, arg1Column) + ", val "+ displayUtf8Bytes(arg2Scalar) + getColumnParamString(2, arg3Column);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(3)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY,
            VectorExpressionDescriptor.ArgumentType.STRING,
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
