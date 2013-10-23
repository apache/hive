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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Evaluate AND of two boolean columns and store result in the output boolean column.
 */
public class ColAndCol extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private int colNum1;
  private int colNum2;
  private int outputColumn;

  public ColAndCol(int colNum1, int colNum2, int outputColumn) {
    this();
    this.colNum1 = colNum1;
    this.colNum2 = colNum2;
    this.outputColumn = outputColumn;
  }

  public ColAndCol() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputColVector1 = (LongColumnVector) batch.cols[colNum1];
    LongColumnVector inputColVector2 = (LongColumnVector) batch.cols[colNum2];
    int[] sel = batch.selected;
    int n = batch.size;
    long[] vector1 = inputColVector1.vector;
    long[] vector2 = inputColVector2.vector;

    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumn];
    long[] outputVector = outV.vector;
    if (n <= 0) {
      // Nothing to do
      return;
    }

    // Handle null
    if (inputColVector1.noNulls && !inputColVector2.noNulls) {
      outV.noNulls = false;
      if (inputColVector2.isRepeating) {
        outV.isRepeating = true;
        outV.isNull[0] = true;
      } else {
        if (batch.selectedInUse) {
          for (int j = 0; j != n; j++) {
            int i = sel[j];
            outV.isNull[i] = inputColVector2.isNull[i];
          }
        } else {
          for (int i = 0; i != n; i++) {
            outV.isNull[i] = inputColVector2.isNull[i];
          }
        }
      }
    } else if (!inputColVector1.noNulls && inputColVector2.noNulls) {
      outV.noNulls = false;
      if (inputColVector1.isRepeating) {
        outV.isRepeating = true;
        outV.isNull[0] = true;
      } else {
        if (batch.selectedInUse) {
          for (int j = 0; j != n; j++) {
            int i = sel[j];
            outV.isNull[i] = inputColVector1.isNull[i];
          }
        } else {
          for (int i = 0; i != n; i++) {
            outV.isNull[i] = inputColVector1.isNull[i];
          }
        }
      }
    } else if (!inputColVector1.noNulls && !inputColVector2.noNulls) {
      outV.noNulls = false;
      if (inputColVector1.isRepeating || inputColVector2.isRepeating) {
        outV.isRepeating = true;
        outV.isNull[0] = true;
      } else {
        if (batch.selectedInUse) {
          for (int j = 0; j != n; j++) {
            int i = sel[j];
            outV.isNull[i] = inputColVector1.isNull[i] && inputColVector2.isNull[i];
          }
        } else {
          for (int i = 0; i != n; i++) {
            outV.isNull[i] = inputColVector1.isNull[i] && inputColVector2.isNull[i];
          }
        }
      }
    }

    // Now disregard null in second pass.
    if ((inputColVector1.isRepeating) && (inputColVector2.isRepeating)) {
      // All must be selected otherwise size would be zero
      // Repeating property will not change.
      outV.isRepeating = true;
      outputVector[0] = vector1[0] & vector2[0];
    } else if (batch.selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        outputVector[i] = vector1[i] & vector2[i];
      }
    } else {
      for (int i = 0; i != n; i++) {
        outputVector[i] = vector1[i] & vector2[i];
      }
    }
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  @Override
  public String getOutputType() {
    return "boolean";
  }

  public int getColNum1() {
    return colNum1;
  }

  public void setColNum1(int colNum1) {
    this.colNum1 = colNum1;
  }

  public int getColNum2() {
    return colNum2;
  }

  public void setColNum2(int colNum2) {
    this.colNum2 = colNum2;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.getType("long"),
            VectorExpressionDescriptor.ArgumentType.getType("long"))
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
