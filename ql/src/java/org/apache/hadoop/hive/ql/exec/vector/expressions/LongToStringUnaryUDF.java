/**
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
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This is a superclass for unary long functions returning strings that operate directly on the
 * input and set the output.
 */
abstract public class LongToStringUnaryUDF extends VectorExpression {
  private static final long serialVersionUID = 1L;
  int inputColumn;
  int outputColumn;

  public LongToStringUnaryUDF(int inputColumn, int outputColumn) {
    this.inputColumn = inputColumn;
    this.outputColumn = outputColumn;
  }

  public LongToStringUnaryUDF() {
    super();
  }

  abstract protected void func(BytesColumnVector outV, long[] vector, int i);

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputColVector = (LongColumnVector) batch.cols[inputColumn];
    int[] sel = batch.selected;
    int n = batch.size;
    long[] vector = inputColVector.vector;
    BytesColumnVector outV = (BytesColumnVector) batch.cols[outputColumn];
    outV.initBuffer();

    if (n == 0) {
      //Nothing to do
      return;
    }

    if (inputColVector.noNulls) {
      outV.noNulls = true;
      if (inputColVector.isRepeating) {
        outV.isRepeating = true;
        func(outV, vector, 0);
      } else if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          func(outV, vector, i);
        }
        outV.isRepeating = false;
      } else {
        for(int i = 0; i != n; i++) {
          func(outV, vector, i);
        }
        outV.isRepeating = false;
      }
    } else {

      // Handle case with nulls. Don't do function if the value is null,
      // because the data may be undefined for a null value.
      outV.noNulls = false;
      if (inputColVector.isRepeating) {
        outV.isRepeating = true;
        outV.isNull[0] = inputColVector.isNull[0];
        if (!inputColVector.isNull[0]) {
          func(outV, vector, 0);
        }
      } else if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outV.isNull[i] = inputColVector.isNull[i];
          if (!inputColVector.isNull[i]) {
            func(outV, vector, i);
          }
        }
        outV.isRepeating = false;
      } else {
        System.arraycopy(inputColVector.isNull, 0, outV.isNull, 0, n);
        for(int i = 0; i != n; i++) {
          if (!inputColVector.isNull[i]) {
            func(outV, vector, i);
          }
        }
        outV.isRepeating = false;
      }
    }
  }


  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  public int getInputColumn() {
    return inputColumn;
  }

  public void setInputColumn(int inputColumn) {
    this.inputColumn = inputColumn;
  }

  @Override
  public String getOutputType() {
    return "String";
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + inputColumn;
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
}