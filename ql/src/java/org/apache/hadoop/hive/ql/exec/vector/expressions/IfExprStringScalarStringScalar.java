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
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;

/**
 * Compute IF(expr1, expr2, expr3) for 3 input column expressions.
 * The first is always a boolean (LongColumnVector).
 * The second is a string scalar.
 * The third is a string scalar.
 */
public class IfExprStringScalarStringScalar extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private final int arg1Column;
  private final byte[] arg2Scalar;
  private final byte[] arg3Scalar;

  public IfExprStringScalarStringScalar(
      int arg1Column, byte[] arg2Scalar, byte[] arg3Scalar, int outputColumnNum) {
    super(outputColumnNum);
    this.arg1Column = arg1Column;
    this.arg2Scalar = arg2Scalar;
    this.arg3Scalar = arg3Scalar;
  }

  public IfExprStringScalarStringScalar() {
    super();

    // Dummy final assignments.
    arg1Column = -1;
    arg2Scalar = null;
    arg3Scalar = null;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector arg1ColVector = (LongColumnVector) batch.cols[arg1Column];
    BytesColumnVector outputColVector = (BytesColumnVector) batch.cols[outputColumnNum];
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

    outputColVector.initBuffer();

    if (arg1ColVector.isRepeating) {
      if ((arg1ColVector.noNulls || !arg1ColVector.isNull[0]) && vector1[0] == 1) {
        outputColVector.setRef(0, arg2Scalar, 0, arg2Scalar.length);
      } else {
        outputColVector.setRef(0, arg3Scalar, 0, arg3Scalar.length);
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (arg1ColVector.noNulls) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = false;
          if (vector1[i] == 1) {
            outputColVector.setRef(i, arg2Scalar, 0, arg2Scalar.length);
          } else {
            outputColVector.setRef(i, arg3Scalar, 0, arg3Scalar.length);
          }
        }
      } else {
        Arrays.fill(outputIsNull, 0, n, false);
        for(int i = 0; i != n; i++) {
          if (vector1[i] == 1) {
            outputColVector.setRef(i, arg2Scalar, 0, arg2Scalar.length);
          } else {
            outputColVector.setRef(i, arg3Scalar, 0, arg3Scalar.length);
          }
        }
      }
    } else /* there are nulls */ {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = false;
          if (!arg1ColVector.isNull[i] && vector1[i] == 1) {
            outputColVector.setRef(i, arg2Scalar, 0, arg2Scalar.length);
          } else {
            outputColVector.setRef(i, arg3Scalar, 0, arg3Scalar.length);
          }
        }
      } else {
        Arrays.fill(outputIsNull, 0, n, false);
        for(int i = 0; i != n; i++) {
          if (!arg1ColVector.isNull[i] && vector1[i] == 1) {
            outputColVector.setRef(i, arg2Scalar, 0, arg2Scalar.length);
          } else {
            outputColVector.setRef(i, arg3Scalar, 0, arg3Scalar.length);
          }
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, arg1Column) + ", val "+ displayUtf8Bytes(arg2Scalar) + ", val "+ displayUtf8Bytes(arg3Scalar);
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
            VectorExpressionDescriptor.ArgumentType.STRING)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
