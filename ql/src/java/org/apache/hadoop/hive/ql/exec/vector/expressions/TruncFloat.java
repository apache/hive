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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.ArgumentType;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.TruncUtils;

/**
 * Vectorized implementation of trunc(number, scale) function for float/double input
 */
public class TruncFloat extends VectorExpression {
  private static final long serialVersionUID = 1L;
  protected int scale;

  public TruncFloat() {
    super();
  }

  public TruncFloat(int colNum, int scale, int outputColumnNum) {
    super(colNum, outputColumnNum);
    this.scale = scale;
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + inputColumnNum[0] + ", scale " + scale;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {
    if (childExpressions != null) {
      this.evaluateChildren(batch);
    }

    ColumnVector inputColVector = batch.cols[inputColumnNum[0]];
    ColumnVector outputColVector = batch.cols[outputColumnNum];

    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    int n = batch.size;

    if (n == 0) {
      return;
    }

    outputColVector.isRepeating = false;

    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        outputIsNull[0] = false;
        trunc(inputColVector, outputColVector, 0);
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (inputColVector.noNulls) {
      if (batch.selectedInUse) {
        if (!outputColVector.noNulls) {
          for (int j = 0; j != n; j++) {
            final int i = sel[j];
            outputIsNull[i] = false;
            trunc(inputColVector, outputColVector, i);
          }
        } else {
          for (int j = 0; j != n; j++) {
            final int i = sel[j];
            trunc(inputColVector, outputColVector, i);
          }
        }
      } else {
        if (!outputColVector.noNulls) {
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        for (int i = 0; i != n; i++) {
          trunc(inputColVector, outputColVector, i);
        }
      }
    } else {
      outputColVector.noNulls = false;

      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = inputIsNull[i];
          trunc(inputColVector, outputColVector, i);
        }
      } else {
        System.arraycopy(inputIsNull, 0, outputIsNull, 0, n);
        for (int i = 0; i != n; i++) {
          if (!inputColVector.isNull[i]) {
            trunc(inputColVector, outputColVector, i);
          }
        }
      }
    }
  }

  protected void trunc(ColumnVector inputColVector, ColumnVector outputColVector, int i) {
    ((DoubleColumnVector) outputColVector).vector[i] =
        TruncUtils.trunc(((DoubleColumnVector) inputColVector).vector[i], scale);
  }

  @Override
  public Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION).setNumArguments(2)
        .setArgumentTypes(getInputColumnType(), VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }

  protected ArgumentType getInputColumnType() {
    return VectorExpressionDescriptor.ArgumentType.FLOAT_FAMILY;
  }
}
