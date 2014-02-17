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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.util.Arrays;

/**
 * This expression returns the value of the first non-null expression
 * in the given set of inputs expressions.
 */
public class VectorCoalesce extends VectorExpression {

  private static final long serialVersionUID = 1L;
  private int [] inputColumns;
  private int outputColumn;

  public VectorCoalesce(int [] inputColumns, int outputColumn) {
    this();
    this.inputColumns = inputColumns;
    this.outputColumn = outputColumn;
  }

  public VectorCoalesce() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    int[] sel = batch.selected;
    int n = batch.size;
    ColumnVector outputVector = batch.cols[outputColumn];
    if (n <= 0) {
      // Nothing to do
      return;
    }

    outputVector.init();

    outputVector.noNulls = false;
    outputVector.isRepeating = false;
    if (batch.selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        outputVector.isNull[i] = true;
        for (int k = 0; k < inputColumns.length; k++) {
          ColumnVector cv = batch.cols[inputColumns[k]];
          if ( (cv.isRepeating) && (cv.noNulls || !cv.isNull[0])) {
            outputVector.isNull[i] = false;
            outputVector.setElement(i, 0, cv);
            break;
          } else if ((!cv.isRepeating) && (cv.noNulls || !cv.isNull[i])) {
            outputVector.isNull[i] = false;
            outputVector.setElement(i, i, cv);
            break;
          }
        }
      }
    } else {
      for (int i = 0; i != n; i++) {
        outputVector.isNull[i] = true;
        for (int k = 0; k < inputColumns.length; k++) {
          ColumnVector cv = batch.cols[inputColumns[k]];
          if ((cv.isRepeating) && (cv.noNulls || !cv.isNull[0])) {
            outputVector.isNull[i] = false;
            outputVector.setElement(i, 0, cv);
            break;
          } else if ((!cv.isRepeating) && (cv.noNulls || !cv.isNull[i])) {
            outputVector.isNull[i] = false;
            outputVector.setElement(i, i, cv);
            break;
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
