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

import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Cast a decimal to a decimal, accounting for precision and scale changes.
 *
 * If other functions besides cast need to take a decimal in and produce a decimal,
 * you can subclass this class or convert it to a superclass, and
 * implement different methods for each operation. If that's done, the
 * convert() method should be renamed to func() for consistency with other
 * similar super classes such as FuncLongToDecimal.
 */
public class CastDecimalToDecimal extends VectorExpression {
  private static final long serialVersionUID = 1L;

  public CastDecimalToDecimal(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  public CastDecimalToDecimal() {
    super();
  }

  /**
   * Convert input decimal value to a decimal with a possibly different precision and scale,
   * at position i in the respective vectors.
   */
  protected void convert(DecimalColumnVector outputColVector, DecimalColumnVector inputColVector, int i) {
    // The set routine enforces precision and scale.
    outputColVector.set(i, inputColVector.vector[i]);
  }

  /**
   * Cast decimal(p1, s1) to decimal(p2, s2).
   *
   * The precision and scale are recorded in the input and output vectors,
   * respectively.
   */
  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    DecimalColumnVector inputColVector = (DecimalColumnVector) batch.cols[inputColumnNum[0]];
    int[] sel = batch.selected;
    int n = batch.size;
    DecimalColumnVector outputColVector = (DecimalColumnVector) batch.cols[outputColumnNum];

    boolean[] outputIsNull = outputColVector.isNull;

    if (n == 0) {

      // Nothing to do
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    if (inputColVector.isRepeating) {
      outputColVector.isRepeating = true;
      if (inputColVector.noNulls || !inputColVector.isNull[0]) {
        // Set isNull before call in case it changes it mind.
        outputColVector.isNull[0] = false;
        convert(outputColVector, inputColVector, 0);
      } else {
        outputColVector.isNull[0] = true;
        outputColVector.noNulls = false;
      }
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
           convert(outputColVector, inputColVector, i);
         }
        } else {
          for(int j = 0; j != n; j++) {
            final int i = sel[j];
            convert(outputColVector, inputColVector, i);
          }
        }
      } else {
        if (!outputColVector.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull, so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        for(int i = 0; i != n; i++) {
          convert(outputColVector, inputColVector, i);
        }
      }
    } else /* there are NULLs in the inputColVector */ {

      /*
       * Do careful maintenance of the outputColVector.noNulls flag.
       */

      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          if (!inputColVector.isNull[i]) {
            // Set isNull before call in case it changes it mind.
            outputColVector.isNull[i] = false;
            convert(outputColVector, inputColVector, i);
          } else {
            outputColVector.isNull[i] = true;
            outputColVector.noNulls = false;
          }
        }
      } else {
        for(int i = 0; i != n; i++) {
          if (!inputColVector.isNull[i]) {
            // Set isNull before call in case it changes it mind.
            outputColVector.isNull[i] = false;
            convert(outputColVector, inputColVector, i);
          } else {
            outputColVector.isNull[i] = true;
            outputColVector.noNulls = false;
          }
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
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.DECIMAL)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}