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
 * The second and third are string columns or string expression results.
 */
public class IfExprStringGroupColumnStringGroupColumn extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private int arg1Column, arg2Column, arg3Column;
  private int outputColumn;

  public IfExprStringGroupColumnStringGroupColumn(int arg1Column, int arg2Column, int arg3Column, int outputColumn) {
    this.arg1Column = arg1Column;
    this.arg2Column = arg2Column;
    this.arg3Column = arg3Column;
    this.outputColumn = outputColumn;
  }

  public IfExprStringGroupColumnStringGroupColumn() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector arg1ColVector = (LongColumnVector) batch.cols[arg1Column];
    BytesColumnVector arg2ColVector = (BytesColumnVector) batch.cols[arg2Column];
    BytesColumnVector arg3ColVector = (BytesColumnVector) batch.cols[arg3Column];
    BytesColumnVector outputColVector = (BytesColumnVector) batch.cols[outputColumn];
    int[] sel = batch.selected;
    boolean[] outputIsNull = outputColVector.isNull;
    outputColVector.noNulls = arg2ColVector.noNulls && arg3ColVector.noNulls;
    outputColVector.isRepeating = false; // may override later
    int n = batch.size;
    long[] vector1 = arg1ColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    outputColVector.initBuffer();

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

    if (arg1ColVector.noNulls) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          if (vector1[i] == 1) {
            if (!arg2ColVector.isNull[i]) {
              outputColVector.setVal(
                i, arg2ColVector.vector[i], arg2ColVector.start[i], arg2ColVector.length[i]);
            }
          } else {
            if (!arg3ColVector.isNull[i]) {
              outputColVector.setVal(
                i, arg3ColVector.vector[i], arg3ColVector.start[i], arg3ColVector.length[i]);
            }
          }
          outputIsNull[i] = (vector1[i] == 1 ?
              arg2ColVector.isNull[i] : arg3ColVector.isNull[i]);
        }
      } else {
        for(int i = 0; i != n; i++) {
          if (vector1[i] == 1) {
            if (!arg2ColVector.isNull[i]) {
              outputColVector.setVal(
                  i, arg2ColVector.vector[i], arg2ColVector.start[i], arg2ColVector.length[i]);
            }
          } else {
            if (!arg3ColVector.isNull[i]) {
              outputColVector.setVal(
                  i, arg3ColVector.vector[i], arg3ColVector.start[i], arg3ColVector.length[i]);
            }
          }
          outputIsNull[i] = (vector1[i] == 1 ?
              arg2ColVector.isNull[i] : arg3ColVector.isNull[i]);
        }
      }
    } else /* there are nulls */ {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          if (!arg1ColVector.isNull[i] && vector1[i] == 1) {
            if (!arg2ColVector.isNull[i]) {
              outputColVector.setVal(
                  i, arg2ColVector.vector[i], arg2ColVector.start[i], arg2ColVector.length[i]);
            }
          } else {
            if (!arg3ColVector.isNull[i]) {
              outputColVector.setVal(
                  i, arg3ColVector.vector[i], arg3ColVector.start[i], arg3ColVector.length[i]);
            }
          }
          outputIsNull[i] = (!arg1ColVector.isNull[i] && vector1[i] == 1 ?
              arg2ColVector.isNull[i] : arg3ColVector.isNull[i]);
        }
      } else {
        for(int i = 0; i != n; i++) {
          if (!arg1ColVector.isNull[i] && vector1[i] == 1) {
            if (!arg2ColVector.isNull[i]) {
              outputColVector.setVal(
                  i, arg2ColVector.vector[i], arg2ColVector.start[i], arg2ColVector.length[i]);
            }
          } else {
            if (!arg3ColVector.isNull[i]) {
              outputColVector.setVal(
                  i, arg3ColVector.vector[i], arg3ColVector.start[i], arg3ColVector.length[i]);
            }
          }
          outputIsNull[i] = (!arg1ColVector.isNull[i] && vector1[i] == 1 ?
              arg2ColVector.isNull[i] : arg3ColVector.isNull[i]);
        }
      }
    }
    arg2ColVector.unFlatten();
    arg3ColVector.unFlatten();
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  @Override
  public String getOutputType() {
    return "String";
  }	

  @Override
  public String vectorExpressionParameters() {
    return "col " + arg1Column + ", col "+ arg2Column + ", col "+ arg3Column;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(3)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY,
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY,
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
