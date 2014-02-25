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

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.common.type.Decimal128;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.expressions.DecimalUtil;

import java.util.Arrays;


public class FuncRoundWithNumDigitsDecimalToDecimal extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int colNum;
  private int outputColumn;
  private int decimalPlaces;

  public FuncRoundWithNumDigitsDecimalToDecimal(int colNum, int scalarValue, int outputColumn) {
    this();
    this.colNum = colNum;
    this.outputColumn = outputColumn;
    this.decimalPlaces = scalarValue;
    this.outputType = "decimal";
  }
  
  public FuncRoundWithNumDigitsDecimalToDecimal() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      this.evaluateChildren(batch);
    }

    DecimalColumnVector inputColVector = (DecimalColumnVector) batch.cols[colNum];
    DecimalColumnVector outputColVector = (DecimalColumnVector) batch.cols[outputColumn];
    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    outputColVector.noNulls = inputColVector.noNulls;
    int n = batch.size;
    Decimal128[] vector = inputColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    if (inputColVector.isRepeating) {

      // All must be selected otherwise size would be zero
      // Repeating property will not change.
      outputIsNull[0] = inputIsNull[0];
      DecimalUtil.round(0, vector[0], outputColVector);
      outputColVector.isRepeating = true;
    } else if (inputColVector.noNulls) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];

          // Set isNull because decimal operation can yield a null.
          outputIsNull[i] = false;
          DecimalUtil.round(i, vector[i], outputColVector);
        }
      } else {

        // Set isNull because decimal operation can yield a null.
        Arrays.fill(outputIsNull, 0, n, false);
        for(int i = 0; i != n; i++) {
          DecimalUtil.round(i, vector[i], outputColVector);
        }
      }
      outputColVector.isRepeating = false;
    } else /* there are nulls */ {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = inputIsNull[i];
          DecimalUtil.round(i, vector[i], outputColVector);
        }
      } else {
        System.arraycopy(inputIsNull, 0, outputIsNull, 0, n);
        for(int i = 0; i != n; i++) {
          DecimalUtil.round(i, vector[i], outputColVector);
        }
      }
      outputColVector.isRepeating = false;
    }
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }
  
  @Override
  public String getOutputType() {
    return outputType;
  }
  
  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  public int getDecimalPlaces() {
    return decimalPlaces;
  }

  public void setDecimalPlaces(int decimalPlaces) {
    this.decimalPlaces = decimalPlaces;
  }


  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.DECIMAL,
            VectorExpressionDescriptor.ArgumentType.LONG)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
