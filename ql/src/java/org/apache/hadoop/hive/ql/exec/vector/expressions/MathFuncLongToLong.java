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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Implement vectorized math function that takes a long (and optionally additional
 * constant argument(s)) and returns long. Use this for PMOD() etc.
 *
 * Do NOT use this for simple math functions like sin/cos/exp etc. that just take
 * a single argument. For those, modify the template ColumnUnaryFunc.txt
 * and expand the template to generate needed classes.
 */
public abstract class MathFuncLongToLong extends VectorExpression {
  private static final long serialVersionUID = 1L;

  protected int colNum;
  private int outputColumn;

  // Subclasses must override this with a function that implements the desired logic.
  protected abstract long func(long d);

  public MathFuncLongToLong(int colNum, int outputColumn) {
    this.colNum = colNum;
    this.outputColumn = outputColumn;
  }

  public MathFuncLongToLong() {
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      this.evaluateChildren(batch);
    }

    LongColumnVector inputColVector = (LongColumnVector) batch.cols[colNum];
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumn];
    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    outputColVector.noNulls = inputColVector.noNulls;
    int n = batch.size;
    long[] vector = inputColVector.vector;
    long[] outputVector = outputColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    if (inputColVector.isRepeating) {
      outputVector[0] = func(vector[0]);

      // Even if there are no nulls, we always copy over entry 0. Simplifies code.
      outputIsNull[0] = inputIsNull[0];
      outputColVector.isRepeating = true;
    } else if (inputColVector.noNulls) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = func(vector[i]);
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputVector[i] = func(vector[i]);
        }
      }
      outputColVector.isRepeating = false;
    } else /* there are nulls */ {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = func(vector[i]);
          outputIsNull[i] = inputIsNull[i];
      }
      } else {
        for(int i = 0; i != n; i++) {
          outputVector[i] = func(vector[i]);
        }
        System.arraycopy(inputIsNull, 0, outputIsNull, 0, n);
      }
      outputColVector.isRepeating = false;
    }
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  @Override
  public String getOutputType() {
    return "long";
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + colNum;
  }
}
