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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/*
 * ELT(index, string, ....) returns the string column/expression value at the specified
 * index expression.
 *
 * The first argument expression indicates the index of the string to be retrieved from
 * remaining arguments.  We return NULL when the index number is less than 1 or
 * index number is greater than the number of the string arguments.
 */
public class VectorElt extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private final int[] inputColumns;

  public VectorElt(int [] inputColumns, int outputColumnNum) {
    super(outputColumnNum);
    this.inputColumns = inputColumns;
  }

  public VectorElt() {
    super();

    // Dummy final assignments.
    inputColumns = null;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    int[] sel = batch.selected;
    int n = batch.size;
    BytesColumnVector outputVector = (BytesColumnVector) batch.cols[outputColumnNum];
    if (n <= 0) {
      return;
    }

    outputVector.init();

    outputVector.isRepeating = false;

    final int limit = inputColumns.length;
    LongColumnVector inputIndexVector = (LongColumnVector) batch.cols[inputColumns[0]];
    boolean[] inputIndexIsNull = inputIndexVector.isNull;
    long[] indexVector = inputIndexVector.vector;
    if (inputIndexVector.isRepeating) {
      if (inputIndexVector.noNulls || !inputIndexIsNull[0]) {
        int repeatedIndex = (int) indexVector[0];
        if (repeatedIndex > 0 && repeatedIndex < limit) {
          BytesColumnVector cv = (BytesColumnVector) batch.cols[inputColumns[repeatedIndex]];
          if (cv.isRepeating) {
            outputVector.isNull[0] = false;
            outputVector.setElement(0, 0, cv);
            outputVector.isRepeating = true;
          } else if (cv.noNulls) {
            if (batch.selectedInUse) {
              for (int j = 0; j != n; j++) {
                int i = sel[j];
                outputVector.isNull[i] = false;
                outputVector.setVal(i, cv.vector[i], cv.start[i], cv.length[i]);
              }
            } else {
              for (int i = 0; i != n; i++) {
                outputVector.isNull[i] = false;
                outputVector.setVal(i, cv.vector[i], cv.start[i], cv.length[i]);
              }
            }
          } else {
            if (batch.selectedInUse) {
              for (int j = 0; j != n; j++) {
                int i = sel[j];
                if (!cv.isNull[i]) {
                  outputVector.isNull[i] = false;
                  outputVector.setVal(i, cv.vector[i], cv.start[i], cv.length[i]);
                } else {
                  outputVector.isNull[i] = true;
                  outputVector.noNulls = false;
                }
              }
            } else {
              for (int i = 0; i != n; i++) {
                if (!cv.isNull[i]) {
                  outputVector.isNull[i] = false;
                  outputVector.setVal(i, cv.vector[i], cv.start[i], cv.length[i]);
                } else {
                  outputVector.isNull[i] = true;
                  outputVector.noNulls = false;
                }
              }
            }
          }
        } else {
          outputVector.isNull[0] = true;
          outputVector.noNulls = false;
          outputVector.isRepeating = true;
        }
      } else {
        outputVector.isNull[0] = true;
        outputVector.noNulls = false;
        outputVector.isRepeating = true;
      }
      return;
    }

    if (inputIndexVector.noNulls) {
      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          int index = (int) indexVector[i];
          if (index > 0 && index < limit) {
            BytesColumnVector cv = (BytesColumnVector) batch.cols[inputColumns[index]];
            int adjusted = cv.isRepeating ? 0 : i;
            if (!cv.isNull[adjusted]) {
              outputVector.isNull[i] = false;
              outputVector.setVal(i, cv.vector[adjusted], cv.start[adjusted], cv.length[adjusted]);
            } else {
              outputVector.isNull[i] = true;
              outputVector.noNulls = false;
            }
          } else {
            outputVector.isNull[i] = true;
            outputVector.noNulls = false;
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          int index = (int) indexVector[i];
          if (index > 0 && index < limit) {
            BytesColumnVector cv = (BytesColumnVector) batch.cols[inputColumns[index]];
            int adjusted = cv.isRepeating ? 0 : i;
            if (!cv.isNull[adjusted]) {
              outputVector.isNull[i] = false;
              outputVector.setVal(i, cv.vector[adjusted], cv.start[adjusted], cv.length[adjusted]);
            } else {
              outputVector.isNull[i] = true;
              outputVector.noNulls = false;
            }
          } else {
            outputVector.isNull[i] = true;
            outputVector.noNulls = false;
          }
        }
      }
    } else {
      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (!inputIndexVector.isNull[i]) {
            int index = (int) indexVector[i];
            if (index > 0 && index < limit) {
              BytesColumnVector cv = (BytesColumnVector) batch.cols[inputColumns[index]];
              int adjusted = cv.isRepeating ? 0 : i;
              if (cv.noNulls || !cv.isNull[adjusted]) {
                outputVector.isNull[i] = false;
                outputVector.setVal(i, cv.vector[adjusted], cv.start[adjusted], cv.length[adjusted]);
              } else {
                outputVector.isNull[i] = true;
                outputVector.noNulls = false;
              }
            } else {
              outputVector.isNull[i] = true;
              outputVector.noNulls = false;
            }
          } else {
            outputVector.isNull[i] = true;
            outputVector.noNulls = false;
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          if (!inputIndexVector.isNull[i]) {
            int index = (int) indexVector[i];
            if (index > 0 && index < limit) {
              BytesColumnVector cv = (BytesColumnVector) batch.cols[inputColumns[index]];
              int adjusted = cv.isRepeating ? 0 : i;
              if (cv.noNulls || !cv.isNull[adjusted]) {
                outputVector.isNull[i] = false;
                outputVector.setVal(i, cv.vector[adjusted], cv.start[adjusted], cv.length[adjusted]);
              } else {
                outputVector.isNull[i] = true;
                outputVector.noNulls = false;
              }
            } else {
              outputVector.isNull[i] = true;
              outputVector.noNulls = false;
            }
          } else {
            outputVector.isNull[i] = true;
            outputVector.noNulls = false;
          }
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return "columns " + Arrays.toString(inputColumns);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    // Descriptor is not defined because it takes variable number of arguments with different
    // data types.
    throw new UnsupportedOperationException("Undefined descriptor");
  }
}
