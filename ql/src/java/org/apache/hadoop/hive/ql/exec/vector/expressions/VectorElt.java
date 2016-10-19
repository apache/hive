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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public class VectorElt extends VectorExpression {

  private static final long serialVersionUID = 1L;
  private int [] inputColumns;
  private int outputColumn;

  public VectorElt(int [] inputColumns, int outputColumn) {
    this();
    this.inputColumns = inputColumns;
    this.outputColumn = outputColumn;
  }

  public VectorElt() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    int[] sel = batch.selected;
    int n = batch.size;
    BytesColumnVector outputVector = (BytesColumnVector) batch.cols[outputColumn];
    if (n <= 0) {
      return;
    }

    outputVector.init();

    outputVector.noNulls = false;
    outputVector.isRepeating = false;

    LongColumnVector inputIndexVector = (LongColumnVector) batch.cols[inputColumns[0]];
    long[] indexVector = inputIndexVector.vector;
    if (inputIndexVector.isRepeating) {
      int index = (int)indexVector[0];
      if (index > 0 && index < inputColumns.length) {
        BytesColumnVector cv = (BytesColumnVector) batch.cols[inputColumns[index]];
        if (cv.isRepeating) {
          outputVector.setElement(0, 0, cv);
          outputVector.isRepeating = true;
        } else if (batch.selectedInUse) {
          for (int j = 0; j != n; j++) {
            int i = sel[j];
            outputVector.setVal(i, cv.vector[0], cv.start[0], cv.length[0]);
          }
        } else {
          for (int i = 0; i != n; i++) {
            outputVector.setVal(i, cv.vector[0], cv.start[0], cv.length[0]);
          }
        }
      } else {
        outputVector.isNull[0] = true;
        outputVector.isRepeating = true;
      }
    } else if (batch.selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        int index = (int)indexVector[i];
        if (index > 0 && index < inputColumns.length) {
          BytesColumnVector cv = (BytesColumnVector) batch.cols[inputColumns[index]];
          int cvi = cv.isRepeating ? 0 : i;
          outputVector.setVal(i, cv.vector[cvi], cv.start[cvi], cv.length[cvi]);
        } else {
          outputVector.isNull[i] = true;
        }
      }
    } else {
      for (int i = 0; i != n; i++) {
        int index = (int)indexVector[i];
        if (index > 0 && index < inputColumns.length) {
          BytesColumnVector cv = (BytesColumnVector) batch.cols[inputColumns[index]];
          int cvi = cv.isRepeating ? 0 : i;
          outputVector.setVal(i, cv.vector[cvi], cv.start[cvi], cv.length[cvi]);
        } else {
          outputVector.isNull[i] = true;
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
    return outputType;
  }

  public int [] getInputColumns() {
    return inputColumns;
  }

  public void setInputColumns(int [] inputColumns) {
    this.inputColumns = inputColumns;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    // Descriptor is not defined because it takes variable number of arguments with different
    // data types.
    throw new UnsupportedOperationException("Undefined descriptor");
  }
}
