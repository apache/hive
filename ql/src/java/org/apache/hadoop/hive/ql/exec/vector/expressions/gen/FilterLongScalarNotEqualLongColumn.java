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
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Generated from template FilterScalarCompareColumn.txt, which covers binary comparison 
 * expressions between a scalar and a column, however output is not produced in a separate column. 
 * The selected vector of the input {@link VectorizedRowBatch} is updated for in-place filtering.
 */
public class FilterLongScalarNotEqualLongColumn extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private int colNum;
  private long value;

  public FilterLongScalarNotEqualLongColumn(int colNum, long value) { 
    this.colNum = colNum;
    this.value = value;
  }

  public FilterLongScalarNotEqualLongColumn() {
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {
  
    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }
  
    LongColumnVector inputColVector = (LongColumnVector) batch.cols[colNum];
    int[] sel = batch.selected;
    boolean[] nullPos = inputColVector.isNull;
    int n = batch.size;
    long[] vector = inputColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }
    
    if (inputColVector.noNulls) {
      if (inputColVector.isRepeating) {
        //All must be selected otherwise size would be zero
        //Repeating property will not change.
        if (!(value != vector[0])) {
          //Entire batch is filtered out.
          batch.size = 0;
        }
      } else if (batch.selectedInUse) {
        int newSize = 0;
        for(int j=0; j != n; j++) {
          int i = sel[j];
          if (value != vector[i]) {
            sel[newSize++] = i;
          }
        }
        batch.size = newSize;
      } else {
        int newSize = 0;
        for(int i = 0; i != n; i++) {
          if (value != vector[i]) {
            sel[newSize++] = i;
          }
        }
        if (newSize < n) {
          batch.size = newSize;
          batch.selectedInUse = true;
        }
      }
    } else {
      if (inputColVector.isRepeating) {
        //All must be selected otherwise size would be zero
        //Repeating property will not change.
        if (!nullPos[0]) {
          if (!(value != vector[0])) {
            //Entire batch is filtered out.
            batch.size = 0;
          }
        } else {
          batch.size = 0;
        }
      } else if (batch.selectedInUse) {
        int newSize = 0;
        for(int j=0; j != n; j++) {
          int i = sel[j];
          if (!nullPos[i]) {
           if (value != vector[i]) {
             sel[newSize++] = i;
           }
          }
        }
        //Change the selected vector
        batch.size = newSize;
      } else {
        int newSize = 0;
        for(int i = 0; i != n; i++) {
          if (!nullPos[i]) {
            if (value != vector[i]) {
              sel[newSize++] = i;
            }
          }
        }
        if (newSize < n) {
          batch.size = newSize;
          batch.selectedInUse = true;
        }
      }
    }
  }

  @Override
  public int getOutputColumn() {
    return -1;
  }

  @Override
  public String getOutputType() {
    return "boolean";
  }
  
  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  public long getValue() {
    return value;
  }

  public void setValue(long value) {
    this.value = value;
  }
}
