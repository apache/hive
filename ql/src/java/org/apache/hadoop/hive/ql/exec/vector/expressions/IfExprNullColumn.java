/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class IfExprNullColumn extends VectorExpression {

  private static final long serialVersionUID = 1L;

  public IfExprNullColumn(int arg1Column, int arg2Column, int outputColumnNum) {
	  super(arg1Column, arg2Column, outputColumnNum);
  }

  public IfExprNullColumn() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    final LongColumnVector arg1ColVector = (LongColumnVector) batch.cols[inputColumnNum[0]];
    final ColumnVector arg2ColVector = batch.cols[inputColumnNum[1]];
    final ColumnVector outputColVector = batch.cols[outputColumnNum];

    final int[] sel = batch.selected;
    final int n = batch.size;
    final boolean[] null1 = arg1ColVector.isNull;
    final long[] vector1 = arg1ColVector.vector;
    final boolean[] isNull = outputColVector.isNull;

    if (n == 0) {
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    /*
     * Repeating IF expression?
     */
    if (arg1ColVector.isRepeating) {
      if ((arg1ColVector.noNulls || !null1[0]) && vector1[0] == 1) {
        outputColVector.isRepeating = true;
        outputColVector.noNulls = false;
        isNull[0] = true;
      } else {
        arg2ColVector.copySelected(batch.selectedInUse, sel, n, outputColVector);
      }
      return;
    }

    /*
     * Do careful maintenance of the outputColVector.noNulls flag.
     */

    if (arg1ColVector.noNulls) {

      /*
       * Repeating ELSE expression?
       */
      if (arg2ColVector.isRepeating) {
        if (batch.selectedInUse) {
          for (int j = 0; j < n; j++) {
            int i = sel[j];
            if (vector1[i] == 1) {
              isNull[i] = true;
              outputColVector.noNulls = false;
            } else {
              isNull[i] = false;
              outputColVector.setElement(i, 0, arg2ColVector);
            }
          }
        } else {
          for (int i = 0; i < n; i++) {
            if (vector1[i] == 1) {
              isNull[i] = true;
              outputColVector.noNulls = false;
            } else {
              isNull[i] = false;
              outputColVector.setElement(i, 0, arg2ColVector);
            }
          }
        }
      } else {
        if (batch.selectedInUse) {
          for (int j = 0; j < n; j++) {
            int i = sel[j];
            if (vector1[i] == 1) {
              isNull[i] = true;
              outputColVector.noNulls = false;
            } else {
              isNull[i] = false;
              outputColVector.setElement(i, i, arg2ColVector);
            }
          }
        } else {
          for (int i = 0; i < n; i++) {
            if (vector1[i] == 1) {
              isNull[i] = true;
              outputColVector.noNulls = false;
            } else {
              isNull[i] = false;
              outputColVector.setElement(i, i, arg2ColVector);
            }
          }
        }
      }
    } else {

      /*
       * Repeating ELSE expression?
       */
      if (arg2ColVector.isRepeating) {
        if (batch.selectedInUse) {
          for (int j = 0; j < n; j++) {
            int i = sel[j];
            if (!null1[i] && vector1[i] == 1) {
              isNull[i] = true;
              outputColVector.noNulls = false;
            } else {
              isNull[i] = false;
              outputColVector.setElement(i, 0, arg2ColVector);
            }
          }
        } else {
          for (int i = 0; i < n; i++) {
            if (!null1[i] && vector1[i] == 1) {
              isNull[i] = true;
              outputColVector.noNulls = false;
            } else {
              isNull[i] = false;
              outputColVector.setElement(i, 0, arg2ColVector);
            }
          }
        }
      } else {
        if (batch.selectedInUse) {
          for (int j = 0; j < n; j++) {
            int i = sel[j];
            if (!null1[i] && vector1[i] == 1) {
              isNull[i] = true;
              outputColVector.noNulls = false;
            } else {
              isNull[i] = false;
              outputColVector.setElement(i, i, arg2ColVector);
            }
          }
        } else {
          for (int i = 0; i < n; i++) {
            if (!null1[i] && vector1[i] == 1) {
              isNull[i] = true;
              outputColVector.noNulls = false;
            } else {
              isNull[i] = false;
              outputColVector.setElement(i, i, arg2ColVector);
            }
          }
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumnNum[0]) + ", null, col "+ inputColumnNum[1];
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    throw new UnsupportedOperationException("Undefined descriptor");
  }
}
