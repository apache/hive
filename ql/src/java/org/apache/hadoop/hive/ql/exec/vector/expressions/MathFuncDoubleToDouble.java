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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Implement vectorized math function that takes a double (and optionally additional
 * constant argument(s)) and returns long.
 * May be used for functions like ROUND(d, N), Pow(a, p) etc.
 *
 * Do NOT use this for simple math functions like sin/cos/exp etc. that just take
 * a single argument. For those, modify the template ColumnUnaryFunc.txt
 * and expand the template to generate needed classes.
 */
public abstract class MathFuncDoubleToDouble extends VectorExpression {

  private static final long serialVersionUID = 1L;

  protected final int colNum;

  // Subclasses must override this with a function that implements the desired logic.
  protected abstract double func(double d);

  public MathFuncDoubleToDouble(int colNum, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum = colNum;
  }

  public MathFuncDoubleToDouble() {
    super();

    // Dummy final assignments.
    colNum = -1;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      this.evaluateChildren(batch);
    }

    DoubleColumnVector inputColVector = (DoubleColumnVector) batch.cols[colNum];
    DoubleColumnVector outputColVector = (DoubleColumnVector) batch.cols[outputColumnNum];
    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    int n = batch.size;
    double[] vector = inputColVector.vector;
    double[] outputVector = outputColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        outputIsNull[0] = false;
        outputVector[0] = func(vector[0]);
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      cleanup(outputColVector, sel, batch.selectedInUse, n);
      return;
    }

    if (inputColVector.noNulls) {
      if (batch.selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outputColVector.noNulls) {
          for(int j = 0; j != n; j++) {
           final int i = sel[j];
           // Set isNull before call in case it changes it mind.
           outputIsNull[i] = false;
           outputVector[i] = func(vector[i]);
         }
        } else {
          for(int j = 0; j != n; j++) {
            final int i = sel[j];
            outputVector[i] = func(vector[i]);
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
          outputVector[i] = func(vector[i]);
        }
      }
    } else /* there are nulls in the inputColVector */ {

      // Carefully handle NULLs...
      outputColVector.noNulls = false;

      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = inputIsNull[i];
          outputVector[i] = func(vector[i]);
        }
      } else {
        System.arraycopy(inputIsNull, 0, outputIsNull, 0, n);
        for(int i = 0; i != n; i++) {
          outputVector[i] = func(vector[i]);
        }
      }
    }
    cleanup(outputColVector, sel, batch.selectedInUse, n);
  }

  // override this with a no-op if subclass doesn't need to treat NaN as null
  protected void cleanup(DoubleColumnVector outputColVector, int[] sel,
      boolean selectedInUse, int n) {
    MathExpr.NaNToNull(outputColVector, sel, selectedInUse, n);
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, colNum);
  }
}
