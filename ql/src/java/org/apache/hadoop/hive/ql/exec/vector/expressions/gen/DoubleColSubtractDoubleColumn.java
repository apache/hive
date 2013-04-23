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

public class DoubleColSubtractDoubleColumn extends VectorExpression {
  int colNum1;
  int colNum2;
  int outputColumn;

  public DoubleColSubtractDoubleColumn(int colNum1, int colNum2, int outputColumn) {
    this.colNum1 = colNum1;
    this.colNum2 = colNum2;
    this.outputColumn = outputColumn;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    DoubleColumnVector inputColVector1 = (DoubleColumnVector) batch.cols[colNum1];
    DoubleColumnVector inputColVector2 = (DoubleColumnVector) batch.cols[colNum2];
    DoubleColumnVector outputColVector = (DoubleColumnVector) batch.cols[outputColumn];
    int[] sel = batch.selected;
    int n = batch.size;
    double[] vector1 = inputColVector1.vector;
    double[] vector2 = inputColVector2.vector;

    double[] outputVector = outputColVector.vector;
    
    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    //Handle nulls first
    if (inputColVector1.noNulls && !inputColVector2.noNulls) {
      outputColVector.noNulls = false;
      if (inputColVector2.isRepeating) {
        //Output will also be repeating and null
        outputColVector.isNull[0] = true;
        outputColVector.isRepeating = true;
        //return as no further processing is needed
        return;
      } else {
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputColVector.isNull[i] = inputColVector2.isNull[i];
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputColVector.isNull[i] = inputColVector2.isNull[i];
          }
        }
      }
    } else if (!inputColVector1.noNulls && inputColVector2.noNulls) {
      outputColVector.noNulls = false;
      if (inputColVector1.isRepeating) {
        //Output will also be repeating and null
        outputColVector.isRepeating = true;
        outputColVector.isNull[0] = true;
        //return as no further processing is needed
        return;
      } else {
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputColVector.isNull[i] = inputColVector1.isNull[i];
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputColVector.isNull[i] = inputColVector1.isNull[i];
          }
        }
      }
    } else if (!inputColVector1.noNulls && !inputColVector2.noNulls) {
      outputColVector.noNulls = false;
      if (inputColVector1.isRepeating || inputColVector2.isRepeating) {
        //Output will also be repeating and null
        outputColVector.isRepeating = true;
        outputColVector.isNull[0] = true;
        //return as no further processing is needed
        return;
      } else {
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputColVector.isNull[i] = inputColVector1.isNull[i] || inputColVector2.isNull[i];
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputColVector.isNull[i] = inputColVector1.isNull[i] || inputColVector2.isNull[i];
          }
        }
      }
    }


    //Disregard nulls for processing
    if (inputColVector1.isRepeating && inputColVector2.isRepeating) { 
      //All must be selected otherwise size would be zero
      //Repeating property will not change.
      outputVector[0] = vector1[0] - vector2[0];
      outputColVector.isRepeating = true;
    } else if (inputColVector1.isRepeating) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = vector1[0] - vector2[i];
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputVector[i] = vector1[0] - vector2[i];
        }
      }
    } else if (inputColVector2.isRepeating) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = vector1[i] - vector2[0];
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputVector[i] = vector1[i] - vector2[0];
        }
      }
    } else {
      if (batch.selectedInUse) {
        for(int j=0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = vector1[i] - vector2[i];
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputVector[i] = vector1[i] -  vector2[i];
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
    return "double";
  }
}
