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
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.lazy.fast.StringToDouble;

/**
 * Cast a string to a double.
 *
 * If other functions besides cast need to take a string in and produce a long,
 * you can subclass this class or convert it to a superclass, and
 * implement different "func()" methods for each operation.
 */
public class CastStringToDouble extends VectorExpression {
  private static final long serialVersionUID = 1L;
  int inputColumn;

  public CastStringToDouble(int inputColumn, int outputColumnNum) {
    super(outputColumnNum);
    this.inputColumn = inputColumn;
  }

  public CastStringToDouble() {
    super();

    // Dummy final assignments.
    inputColumn = -1;
  }

  /**
   * Convert input string to a double, at position i in the respective vectors.
   */
  protected void func(DoubleColumnVector outV, BytesColumnVector inV, int batchIndex) {

    byte[] bytes = inV.vector[batchIndex];
    final int start = inV.start[batchIndex];
    final int length = inV.length[batchIndex];
    try {
      if (!LazyUtils.isNumberMaybe(bytes, start, length)) {
        outV.noNulls = false;
        outV.isNull[batchIndex] = true;
        outV.vector[batchIndex] = DoubleColumnVector.NULL_VALUE;
        return;
      }
      outV.vector[batchIndex] = StringToDouble.strtod(bytes, start, length);
    } catch (Exception e) {

      // for any exception in conversion to integer, produce NULL
      outV.noNulls = false;
      outV.isNull[batchIndex] = true;
      outV.vector[batchIndex] = DoubleColumnVector.NULL_VALUE;
    }
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    BytesColumnVector inV = (BytesColumnVector) batch.cols[inputColumn];
    int[] sel = batch.selected;
    int n = batch.size;
    DoubleColumnVector outV = (DoubleColumnVector) batch.cols[outputColumnNum];

    if (n == 0) {

      // Nothing to do
      return;
    }

    if (inV.noNulls) {
      outV.noNulls = true;
      if (inV.isRepeating) {
        outV.isRepeating = true;
        func(outV, inV, 0);
      } else if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          func(outV, inV, i);
        }
        outV.isRepeating = false;
      } else {
        for(int i = 0; i != n; i++) {
          func(outV, inV, i);
        }
        outV.isRepeating = false;
      }
    } else {

      // Handle case with nulls. Don't do function if the value is null,
      // because the data may be undefined for a null value.
      outV.noNulls = false;
      if (inV.isRepeating) {
        outV.isRepeating = true;
        outV.isNull[0] = inV.isNull[0];
        if (!inV.isNull[0]) {
          func(outV, inV, 0);
        }
      } else if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outV.isNull[i] = inV.isNull[i];
          if (!inV.isNull[i]) {
            func(outV, inV, i);
          }
        }
        outV.isRepeating = false;
      } else {
        System.arraycopy(inV.isNull, 0, outV.isNull, 0, n);
        for(int i = 0; i != n; i++) {
          if (!inV.isNull[i]) {
            func(outV, inV, i);
          }
        }
        outV.isRepeating = false;
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumn);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}