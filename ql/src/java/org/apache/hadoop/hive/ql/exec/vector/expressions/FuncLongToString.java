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
 * Superclass to support vectorized functions that take a long
 * and return a string, optionally with additional configuraiton arguments.
 * Used for bin(long), hex(long) etc.
 */
public abstract class FuncLongToString extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int inputCol;
  private int outputCol;
  protected transient byte[] bytes;

  FuncLongToString(int inputCol, int outputCol) {
    this.inputCol = inputCol;
    this.outputCol = outputCol;
    bytes = new byte[64];    // staging area for results, to avoid new() calls
  }

  FuncLongToString() {
    bytes = new byte[64];
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputColVector = (LongColumnVector) batch.cols[inputCol];
    int[] sel = batch.selected;
    int n = batch.size;
    long[] vector = inputColVector.vector;
    BytesColumnVector outV = (BytesColumnVector) batch.cols[outputCol];
    outV.initBuffer();

    if (n == 0) {
      //Nothing to do
      return;
    }

    if (inputColVector.noNulls) {
      outV.noNulls = true;
      if (inputColVector.isRepeating) {
        outV.isRepeating = true;
        prepareResult(0, vector, outV);
      } else if (batch.selectedInUse) {
        for(int j=0; j != n; j++) {
          int i = sel[j];
          prepareResult(i, vector, outV);
        }
        outV.isRepeating = false;
      } else {
        for(int i = 0; i != n; i++) {
          prepareResult(i, vector, outV);
        }
        outV.isRepeating = false;
      }
    } else {
      // Handle case with nulls. Don't do function if the value is null, to save time,
      // because calling the function can be expensive.
      outV.noNulls = false;
      if (inputColVector.isRepeating) {
        outV.isRepeating = true;
        outV.isNull[0] = inputColVector.isNull[0];
        if (!inputColVector.isNull[0]) {
          prepareResult(0, vector, outV);
        }
      } else if (batch.selectedInUse) {
        for(int j=0; j != n; j++) {
          int i = sel[j];
          if (!inputColVector.isNull[i]) {
            prepareResult(i, vector, outV);
          }
          outV.isNull[i] = inputColVector.isNull[i];
        }
        outV.isRepeating = false;
      } else {
        for(int i = 0; i != n; i++) {
          if (!inputColVector.isNull[i]) {
            prepareResult(i, vector, outV);
          }
          outV.isNull[i] = inputColVector.isNull[i];
        }
        outV.isRepeating = false;
      }
    }
  }

  /* Evaluate result for position i (using bytes[] to avoid storage allocation costs)
   * and set position i of the output vector to the result.
   */
  abstract void prepareResult(int i, long[] vector, BytesColumnVector outV);

  @Override
  public int getOutputColumn() {
    return outputCol;
  }

  public int getOutputCol() {
    return outputCol;
  }

  public void setOutputCol(int outputCol) {
    this.outputCol = outputCol;
  }

  public int getInputCol() {
    return inputCol;
  }

  public void setInputCol(int inputCol) {
    this.inputCol = inputCol;
  }

  @Override
  public String getOutputType() {
    return "String";
  }


  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder()).setMode(
        VectorExpressionDescriptor.Mode.PROJECTION).setNumArguments(1).setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN).setArgumentTypes(
                VectorExpressionDescriptor.ArgumentType.LONG).build();
  }
}
