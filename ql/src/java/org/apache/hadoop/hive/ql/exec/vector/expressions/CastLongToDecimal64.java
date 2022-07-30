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

import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressionsSupportDecimal64;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

import java.util.Arrays;

/**
 * To be used to cast long and boolean to decimal.
 * This works for boolean too because boolean is encoded as 0
 * for false and 1 for true.
 */
@VectorizedExpressionsSupportDecimal64()
public class CastLongToDecimal64 extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private static final long[] powerOfTenTable = {
      1L,                   // 0
      10L,
      100L,
      1_000L,
      10_000L,
      100_000L,
      1_000_000L,
      10_000_000L,
      100_000_000L,           // 8
      1_000_000_000L,
      10_000_000_000L,
      100_000_000_000L,
      1_000_000_000_000L,
      10_000_000_000_000L,
      100_000_000_000_000L,
      1_000_000_000_000_000L,
      10_000_000_000_000_000L,   // 16
      100_000_000_000_000_000L,
      1_000_000_000_000_000_000L, // 18
  };

  public CastLongToDecimal64(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  public CastLongToDecimal64() {
    super();
  }

  protected void scaleUp(Decimal64ColumnVector outV, LongColumnVector inV, int i, long scaleFactor) {
    outV.vector[i] = inV.vector[i] * scaleFactor;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputColVector = (LongColumnVector) batch.cols[inputColumnNum[0]];
    int[] sel = batch.selected;
    int n = batch.size;
    Decimal64ColumnVector outputColVector = (Decimal64ColumnVector) batch.cols[outputColumnNum];
    int outputScale = ((DecimalTypeInfo) outputTypeInfo).scale();
    final long scaleFactor = powerOfTenTable[outputScale];

    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;

    if (n == 0) {

      // Nothing to do
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        // Set isNull before call in case it changes it mind.
        outputIsNull[0] = false;
        scaleUp(outputColVector, inputColVector, 0, scaleFactor);
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (inputColVector.noNulls) {
      if (batch.selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outputColVector.noNulls) {
          for(int j = 0; j != n; j++) {
            final int i = sel[j];
            // Set isNull before call in case it changes it mind.
            outputIsNull[i] = false;
            scaleUp(outputColVector, inputColVector, i, scaleFactor);
          }
        } else {
          for(int j = 0; j != n; j++) {
            final int i = sel[j];
            scaleUp(outputColVector, inputColVector, i, scaleFactor);
          }
        }
      } else {
        if (!outputColVector.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        for(int i = 0; i != n; i++) {
          scaleUp(outputColVector, inputColVector, i, scaleFactor);
        }
      }
    } else /* there are nulls in the inputColVector */ {

      // Carefully handle NULLs...
      outputColVector.noNulls = false;

      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputColVector.isNull[i] = inputColVector.isNull[i];
          if (!inputColVector.isNull[i]) {
            scaleUp(outputColVector, inputColVector, i, scaleFactor);
          }
        }
      } else {
        System.arraycopy(inputColVector.isNull, 0, outputColVector.isNull, 0, n);
        for(int i = 0; i != n; i++) {
          if (!inputColVector.isNull[i]) {
            scaleUp(outputColVector, inputColVector, i, scaleFactor);
          }
        }
      }
    }
  }

  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumnNum[0]);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }

  @Override
  public boolean shouldConvertDecimal64ToDecimal() {
    return false;
  }
}
