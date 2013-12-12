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

  private int arg1Column;
  private byte[] arg2Scalar;
  private byte[] arg3Scalar;
  private int outputColumn;

  public IfExprStringScalarStringScalar(
      int arg1Column, byte[] arg2Scalar, byte[] arg3Scalar, int outputColumn) {
    this.arg1Column = arg1Column;
    this.arg2Scalar = arg2Scalar;
    this.arg3Scalar = arg3Scalar;
    this.outputColumn = outputColumn;
  }

  public IfExprStringScalarStringScalar() {
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector arg1ColVector = (LongColumnVector) batch.cols[arg1Column];
    BytesColumnVector outputColVector = (BytesColumnVector) batch.cols[outputColumn];
    int[] sel = batch.selected;
    outputColVector.noNulls = true; // output must be a scalar and neither one is null
    outputColVector.isRepeating = false; // may override later
    int n = batch.size;
    long[] vector1 = arg1ColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    outputColVector.initBuffer();

    if (arg1ColVector.isRepeating) {
      if (vector1[0] == 1) {
        outputColVector.fill(arg2Scalar);
      } else {
        outputColVector.fill(arg3Scalar);
      }
      return;
    }

    if (arg1ColVector.noNulls) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          if (vector1[i] == 1) {
            outputColVector.setRef(i, arg2Scalar, 0, arg2Scalar.length);
          } else {
            outputColVector.setRef(i, arg3Scalar, 0, arg2Scalar.length);
          }
        }
      } else {
        for(int i = 0; i != n; i++) {
          if (vector1[i] == 1) {
            outputColVector.setRef(i, arg2Scalar, 0, arg2Scalar.length);
          } else {
            outputColVector.setRef(i, arg3Scalar, 0, arg2Scalar.length);
          }
        }
      }
    } else /* there are nulls */ {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          if (!arg1ColVector.isNull[i] && vector1[i] == 1) {
            outputColVector.setRef(i, arg2Scalar, 0, arg2Scalar.length);
          } else {
            outputColVector.setRef(i, arg3Scalar, 0, arg2Scalar.length);
          }
        }
      } else {
        for(int i = 0; i != n; i++) {
          if (!arg1ColVector.isNull[i] && vector1[i] == 1) {
            outputColVector.setRef(i, arg2Scalar, 0, arg2Scalar.length);
          } else {
            outputColVector.setRef(i, arg3Scalar, 0, arg2Scalar.length);
          }
        }
      }
    }
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  @Override
  public String getOutputType() {
    return "String";
  }

  public int getArg1Column() {
    return arg1Column;
  }

  public void setArg1Column(int colNum) {
    this.arg1Column = colNum;
  }

  public byte[] getArg2Scalar() {
    return arg2Scalar;
  }

  public void setArg2Scalar(byte[] value) {
    this.arg2Scalar = value;
  }

  public byte[] getArg3Scalar() {
    return arg3Scalar;
  }

  public void setArg3Scalar(byte[] value) {
    this.arg3Scalar = value;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(3)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.getType("long"),
            VectorExpressionDescriptor.ArgumentType.getType("string"),
            VectorExpressionDescriptor.ArgumentType.getType("string"))
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
