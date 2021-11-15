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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Evaluates the boolean complement of the input.
 */
public class NotCol extends VectorExpression {
  private static final long serialVersionUID = 1L;

  public NotCol(int colNum, int outputColumnNum) {
    super(colNum, outputColumnNum);
  }

  public NotCol() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputColVector = (LongColumnVector) batch.cols[inputColumnNum[0]];
    int[] sel = batch.selected;
    int n = batch.size;
    long[] vector = inputColVector.vector;
    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumnNum];
    long[] outputVector = outV.vector;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outV.isNull;

    if (n <= 0) {
      // Nothing to do, this is EOF
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outV.isRepeating = false;

    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        // Set isNull before call in case it changes it mind.
        outputIsNull[0] = false;
        // 0 XOR 1 yields 1, 1 XOR 1 yields 0
        outputVector[0] = vector[0] ^ 1;
      } else {
        outputIsNull[0] = true;
        outV.noNulls = false;
      }
      outV.isRepeating = true;
      return;
    }

    if (inputColVector.noNulls) {
      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          outV.isNull[i] = false;
          outputVector[i] = vector[i] ^ 1;
        }
      } else {
        Arrays.fill(outV.isNull, 0, n, false);
        for (int i = 0; i != n; i++) {
          outputVector[i] = vector[i] ^ 1;
        }
      }
    } else /* there are nulls in the inputColVector */ {

      // Carefully handle NULLs...

      /*
       * For better performance on LONG/DOUBLE we don't want the conditional
       * statements inside the for loop.
       */
      outV.noNulls = false;

      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = vector[i] ^ 1;
          outV.isNull[i] = inputColVector.isNull[i];
        }
      } else {
        for (int i = 0; i != n; i++) {
          outputVector[i] = vector[i] ^ 1;
          outV.isNull[i] = inputColVector.isNull[i];
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumnNum[0]);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
