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
 
package org.apache.hadoop.hive.ql.exec.vector.expressions.gen;

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Generated from template ColumnUnaryMinus.txt, which covers unary negation operator. 
 */
public class DoubleColUnaryMinus extends VectorExpression {
  private int colNum;
  private int outputColumn;

  public DoubleColUnaryMinus(int colNum, int outputColumn) {
    this.colNum = colNum;
    this.outputColumn = outputColumn;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      this.evaluateChildren(batch);
    }

    DoubleColumnVector inputColVector = (DoubleColumnVector) batch.cols[colNum];
    DoubleColumnVector outputColVector = (DoubleColumnVector) batch.cols[outputColumn];
    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    outputColVector.noNulls = inputColVector.noNulls;
    int n = batch.size;
    double[] vector = inputColVector.vector;
    double[] outputVector = outputColVector.vector;
    
    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    if (inputColVector.isRepeating) {
      //All must be selected otherwise size would be zero
      //Repeating property will not change.
      outputVector[0] = - vector[0];
      // Even if there are no nulls, we always copy over entry 0. Simplifies code.
      outputIsNull[0] = inputIsNull[0]; 
      outputColVector.isRepeating = true;
    } else if (inputColVector.noNulls) {
      if (batch.selectedInUse) {
        for(int j=0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = -vector[i];
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputVector[i] = -vector[i];
        }
      }
      outputColVector.isRepeating = false;
    } else /* there are nulls */ {
      if (batch.selectedInUse) {
        for(int j=0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = -vector[i];
          outputIsNull[i] = inputIsNull[i];
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputVector[i] = -vector[i];
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
  
  @Override
  public String getOutputType() {
    return "double";
  }
}
