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
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.sql.Timestamp;
import java.util.Arrays;

/**
 * Compute IF(expr1, expr2, expr3) for 3 input  expressions.
 * The first is always a boolean (LongColumnVector).
 * The second is a constant value.
 * The third is a constant value.
 */
public abstract class IfExprTimestampScalarScalarBase extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private int arg1Column;
  private Timestamp arg2Scalar;
  private Timestamp arg3Scalar;
  private int outputColumn;

  public IfExprTimestampScalarScalarBase(int arg1Column, Timestamp arg2Scalar, Timestamp arg3Scalar,
      int outputColumn) {
    this.arg1Column = arg1Column;
    this.arg2Scalar = arg2Scalar;
    this.arg3Scalar = arg3Scalar;
    this.outputColumn = outputColumn;
  }

  public IfExprTimestampScalarScalarBase() {
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector arg1ColVector = (LongColumnVector) batch.cols[arg1Column];
    TimestampColumnVector outputColVector = (TimestampColumnVector) batch.cols[outputColumn];
    int[] sel = batch.selected;
    boolean[] outputIsNull = outputColVector.isNull;
    outputColVector.noNulls = false; // output is a scalar which we know is non null
    outputColVector.isRepeating = false; // may override later
    int n = batch.size;
    long[] vector1 = arg1ColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    if (arg1ColVector.isRepeating) {
      if (vector1[0] == 1) {
        outputColVector.fill(arg2Scalar);
      } else {
        outputColVector.fill(arg3Scalar);
      }
    } else if (arg1ColVector.noNulls) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputColVector.set(i, vector1[i] == 1 ? arg2Scalar : arg3Scalar);
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputColVector.set(i, vector1[i] == 1 ? arg2Scalar : arg3Scalar);
        }
      }
    } else /* there are nulls */ {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputColVector.set(i, !arg1ColVector.isNull[i] && vector1[i] == 1 ?
              arg2Scalar : arg3Scalar);
          outputIsNull[i] = false;
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputColVector.set(i, !arg1ColVector.isNull[i] && vector1[i] == 1 ?
              arg2Scalar : arg3Scalar);
        }
        Arrays.fill(outputIsNull, 0, n, false);
      }
    }
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  @Override
  public String getOutputType() {
    return "timestamp";
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + arg1Column + ", val "+ arg2Scalar + ", val "+ arg3Scalar;
  }

}
