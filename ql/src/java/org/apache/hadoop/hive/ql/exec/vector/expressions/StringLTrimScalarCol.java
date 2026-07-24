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
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Vectorized LTRIM with a scalar string and a trim-characters column.
 */
public class StringLTrimScalarCol extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private final byte[] stringToTrim;

  public StringLTrimScalarCol(byte[] stringToTrim, int trimCharsCol, int outputColumnNum) {
    super(trimCharsCol, outputColumnNum);
    this.stringToTrim = stringToTrim;
  }

  public StringLTrimScalarCol() {
    super();
    stringToTrim = null;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {
    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    BytesColumnVector trimV = (BytesColumnVector) batch.cols[inputColumnNum[0]];
    BytesColumnVector outV = (BytesColumnVector) batch.cols[outputColumnNum];
    int[] sel = batch.selected;
    int n = batch.size;
    boolean[] trimIsNull = trimV.isNull;
    boolean[] outputIsNull = outV.isNull;

    if (n == 0) {
      return;
    }

    outV.initBuffer();
    outV.isRepeating = false;

    if (trimV.isRepeating) {
      if (trimV.noNulls || !trimIsNull[0]) {
        outputIsNull[0] = false;
        func(outV, trimV, 0, 0);
      } else {
        outputIsNull[0] = true;
        outV.noNulls = false;
      }
      outV.isRepeating = true;
      return;
    }

    if (trimV.noNulls) {
      if (batch.selectedInUse) {
        if (!outV.noNulls) {
          for (int j = 0; j != n; j++) {
            final int i = sel[j];
            outputIsNull[i] = false;
            func(outV, trimV, i, i);
          }
        } else {
          for (int j = 0; j != n; j++) {
            final int i = sel[j];
            func(outV, trimV, i, i);
          }
        }
      } else {
        if (!outV.noNulls) {
          Arrays.fill(outputIsNull, false);
          outV.noNulls = true;
        }
        for (int i = 0; i != n; i++) {
          func(outV, trimV, i, i);
        }
      }
    } else {
      outV.noNulls = false;
      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = trimIsNull[i];
          if (!trimIsNull[i]) {
            func(outV, trimV, i, i);
          }
        }
      } else {
        System.arraycopy(trimIsNull, 0, outputIsNull, 0, n);
        for (int i = 0; i != n; i++) {
          if (!trimIsNull[i]) {
            func(outV, trimV, i, i);
          }
        }
      }
    }
  }

  private void func(BytesColumnVector outV, BytesColumnVector trimV, int trimIndex,
      int outIndex) {
    StringTrimColScalarBase.trimLeft(outV, stringToTrim, 0, stringToTrim.length,
        trimV.vector[trimIndex], trimV.start[trimIndex], trimV.length[trimIndex], outIndex);
  }

  @Override
  public String vectorExpressionParameters() {
    return "val " + displayUtf8Bytes(stringToTrim) + ", "
        + getColumnParamString(0, inputColumnNum[0]);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY,
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.SCALAR,
            VectorExpressionDescriptor.InputExpressionType.COLUMN)
        .build();
  }
}
