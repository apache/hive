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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Base class for vectorized two-parameter trim functions with a string column and a
 * trim-characters column. Null/repeating/selected handling is delegated to
 * {@link NullUtil#propagateNullsColCol}.
 */
abstract public class StringTrimColColBase extends VectorExpression {
  private static final long serialVersionUID = 1L;

  public StringTrimColColBase(int strCol, int trimCharsCol, int outputColumnNum) {
    super(strCol, trimCharsCol, outputColumnNum);
  }

  public StringTrimColColBase() {
    super();
  }

  abstract protected void func(BytesColumnVector outV, BytesColumnVector strV, int strIndex,
      BytesColumnVector trimV, int trimIndex, int outIndex);

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {
    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    BytesColumnVector strV = (BytesColumnVector) batch.cols[inputColumnNum[0]];
    BytesColumnVector trimV = (BytesColumnVector) batch.cols[inputColumnNum[1]];
    BytesColumnVector outV = (BytesColumnVector) batch.cols[outputColumnNum];
    int[] sel = batch.selected;
    int n = batch.size;
    if (n == 0) {
      return;
    }

    outV.initBuffer();
    NullUtil.propagateNullsColCol(strV, trimV, outV, sel, n, batch.selectedInUse);

    if (strV.isRepeating && trimV.isRepeating) {
      if (!outV.isNull[0]) {
        func(outV, strV, 0, trimV, 0, 0);
      }
      outV.isRepeating = true;
      return;
    }

    if (batch.selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        if (!outV.isNull[i]) {
          func(outV, strV, i, trimV, trimV.isRepeating ? 0 : i, i);
        }
      }
    } else {
      for (int i = 0; i != n; i++) {
        if (!outV.isNull[i]) {
          func(outV, strV, i, trimV, trimV.isRepeating ? 0 : i, i);
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumnNum[0]) + ", "
        + getColumnParamString(1, inputColumnNum[1]);
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
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN)
        .build();
  }
}
