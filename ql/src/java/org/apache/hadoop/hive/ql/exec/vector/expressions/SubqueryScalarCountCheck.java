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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class SubqueryScalarCountCheck extends VectorExpression {

  private final int inputColumnNum;

  public SubqueryScalarCountCheck(int inputColumnNum, int outputColumn) {
    super(outputColumn);
    this.inputColumnNum = inputColumnNum;
  }

  public SubqueryScalarCountCheck() {
    super();
    this.inputColumnNum = -1;
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumnNum);
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {
    final int n = batch.size;
    if (n == 0) {
      return;
    }

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputColVector = (LongColumnVector) batch.cols[inputColumnNum];
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];
    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;

    long[] inVector = inputColVector.vector;
    long[] outVector = outputColVector.vector;

    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        if (inVector[0] > 1) {
          throw new UDFArgumentException("Scalar subquery expression returned more than one row.");
        }
      }
      outputColVector.isRepeating = true;
      outputIsNull[0] = inputIsNull[0];
      outputColVector.noNulls = inputColVector.noNulls;
    } else {
      if (inputColVector.noNulls) {
        if (batch.selectedInUse) {
          for(int j=0; j < n; j++) {
            int i = sel[j];
            if (inVector[i] > 1) {
              throw new UDFArgumentException("Scalar subquery expression returned more than one row.");
            }
          }
        } else {
          for(int i = 0; i < n; i++) {
            if (inVector[i] > 1) {
              throw new UDFArgumentException("Scalar subquery expression returned more than one row.");
            }
          }
          System.arraycopy(inputIsNull, 0, outputIsNull, 0, n);
        }
      } else {
        if (batch.selectedInUse) {
          for(int j=0; j != n; j++) {
            int i = sel[j];
            if (!inputIsNull[i]) {
              if (inVector[i] > 1) {
                throw new UDFArgumentException("Scalar subquery expression returned more than one row.");
              }
            }
          }
        } else {
          for(int i = 0; i != n; i++) {
            if (!inputIsNull[i]) {
              if (inVector[i] > 1) {
                throw new UDFArgumentException("Scalar subquery expression returned more than one row.");
              }
            }
          }
          System.arraycopy(inputIsNull, 0, outputIsNull, 0, n);
        }
      }
      System.arraycopy(inVector, 0, outVector, 0, n);
      outputColVector.isRepeating = false;
    }
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.getType("long"))
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
