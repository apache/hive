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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Vectorized instruction to get the field of Struct type with field name and put
 * the result in an output column.
 */
public class VectorUDFStructField extends VectorExpression {

  private int structColumnNum;
  private int fieldIndex;

  public VectorUDFStructField() {
    super();
  }

  public VectorUDFStructField(int structColumnNum, int fieldIndex, int outputColumnNum) {
    super(outputColumnNum);
    this.structColumnNum = structColumnNum;
    this.fieldIndex = fieldIndex;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {
    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    ColumnVector outV = batch.cols[outputColumnNum];
    StructColumnVector structColumnVector = (StructColumnVector) batch.cols[structColumnNum];
    ColumnVector fieldColumnVector = structColumnVector.fields[fieldIndex];

    outV.noNulls = true;
    if (structColumnVector.isRepeating) {
      if (structColumnVector.isNull[0]) {
        outV.isNull[0] = true;
        outV.noNulls = false;
      } else {
        outV.setElement(0, 0, fieldColumnVector);
        outV.isNull[0] = false;
      }
      outV.isRepeating = true;
    } else {
      for (int i = 0; i < batch.size; i++) {
        int j = (batch.selectedInUse) ? batch.selected[i] : i;
        if (structColumnVector.isNull[j]) {
          outV.isNull[j] = true;
          outV.noNulls = false;
        } else {
          outV.setElement(j, j, fieldColumnVector);
          outV.isNull[j] = false;
        }
      }
      outV.isRepeating = false;
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, structColumnNum) + ", " + getColumnParamString(1, fieldIndex);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRUCT,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
