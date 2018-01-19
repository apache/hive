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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import java.sql.Date;

/**
 * Casts a timestamp and date vector to a date vector.
 */
public class CastLongToDate extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int inputColumn;
  private transient Date date = new Date(0);

  public CastLongToDate() {
    super();
  }

  public CastLongToDate(int inputColumn, int outputColumnNum) {
    super(outputColumnNum);
    this.inputColumn = inputColumn;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inV = (LongColumnVector) batch.cols[inputColumn];
    int[] sel = batch.selected;
    int n = batch.size;
    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumnNum];

    if (n == 0) {

      // Nothing to do
      return;
    }

    PrimitiveCategory primitiveCategory =
        ((PrimitiveTypeInfo) inputTypeInfos[0]).getPrimitiveCategory();
    switch (primitiveCategory) {
      case DATE:
        inV.copySelected(batch.selectedInUse, batch.selected, batch.size, outV);
        break;
      default:
        throw new Error("Unsupported input type " + primitiveCategory.name());
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
            VectorExpressionDescriptor.ArgumentType.DATE)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
