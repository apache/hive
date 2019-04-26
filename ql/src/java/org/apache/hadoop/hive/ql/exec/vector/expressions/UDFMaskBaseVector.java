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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public abstract class UDFMaskBaseVector extends VectorExpression {

  private static final long serialVersionUID = 1L;

  static final int MASKED_UPPERCASE           = 'X';
  static final int MASKED_LOWERCASE           = 'x';
  static final int MASKED_DIGIT               = 'n';
  static final int MASKED_OTHER_CHAR          = -1;
  static final int MASKED_NUMBER              = 1;
  static final int MASKED_DAY_COMPONENT_VAL   = 1;
  static final int MASKED_MONTH_COMPONENT_VAL = 0;
  static final int MASKED_YEAR_COMPONENT_VAL  = 0;
  static final int UNMASKED_VAL               = -1;

  int maskedUpperChar  = MASKED_UPPERCASE;
  int maskedLowerChar  = MASKED_LOWERCASE;
  int maskedDigitChar  = MASKED_DIGIT;
  int maskedOtherChar  = MASKED_OTHER_CHAR;
  int maskedNumber     = MASKED_NUMBER;
  int maskedDayValue   = MASKED_DAY_COMPONENT_VAL;
  int maskedMonthValue = MASKED_MONTH_COMPONENT_VAL;
  int maskedYearValue  = MASKED_YEAR_COMPONENT_VAL;

  private final int colNum;

  public UDFMaskBaseVector(int colNum, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum = colNum;
  }

  public UDFMaskBaseVector() {
    super();

    // Dummy final assignments.
    colNum = -1;
  }

  public int getColNum() {
    return colNum;
  }

  public abstract void transform(ColumnVector outputColVector, ColumnVector inputVector, int idx);

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    int batchSize = batch.size;

    if(batchSize <=0) {
      return;
    }

    ColumnVector inputColVector = batch.cols[colNum];
    ColumnVector outputColVector = batch.cols[outputColumnNum];

    outputColVector.noNulls = inputColVector.noNulls;

    if(inputColVector.isRepeating) {
      outputColVector.isRepeating = true;
      if(inputColVector.noNulls || !inputColVector.isNull[0]) {
        // no NULLs
        transform(outputColVector, inputColVector, 0);
        outputColVector.noNulls = true;
      } else {
        // all NULLs
        outputColVector.isNull[0] = true;
        outputColVector.noNulls = false;
      }
    }
    if(batch.selectedInUse) {
      int[] selectedRows = batch.selected;
      for(int i=0; i != batchSize; i++) {
        final int offset = selectedRows[i];
        if(inputColVector.noNulls || !inputColVector.isNull[offset]) {
          // non-null
          transform(outputColVector, inputColVector, offset);
        } else {
          outputColVector.isNull[i] = true;
        }
      }
    } else {
      // all rows are to be processed
      for(int i=0; i != batchSize; i++) {
        if(inputColVector.noNulls || !inputColVector.isNull[i]) {
          // non-null
          transform(outputColVector, inputColVector, i);
        } else {
            outputColVector.isNull[i] = true;
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, colNum);
  }
}
