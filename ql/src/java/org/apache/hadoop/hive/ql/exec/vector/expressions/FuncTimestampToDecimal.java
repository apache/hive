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

import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This is a superclass for unary timestamp functions and expressions returning decimals that
 * operate directly on the input and set the output.
 */
public abstract class FuncTimestampToDecimal extends VectorExpression {
  private static final long serialVersionUID = 1L;
  int inputColumn;
  int outputColumn;

  public FuncTimestampToDecimal(int inputColumn, int outputColumn) {
    this.inputColumn = inputColumn;
    this.outputColumn = outputColumn;
    this.outputType = "decimal";
  }

  public FuncTimestampToDecimal() {
    super();
    this.outputType = "decimal";
  }

  abstract protected void func(DecimalColumnVector outV, TimestampColumnVector inV, int i);

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    TimestampColumnVector inV = (TimestampColumnVector) batch.cols[inputColumn];
    int[] sel = batch.selected;
    int n = batch.size;
    DecimalColumnVector outV = (DecimalColumnVector) batch.cols[outputColumn];

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
  public String vectorExpressionParameters() {
    return "col " + inputColumn;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.TIMESTAMP)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}