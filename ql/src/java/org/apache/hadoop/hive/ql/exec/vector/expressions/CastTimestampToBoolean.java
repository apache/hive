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

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.MathExpr;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;

public class CastTimestampToBoolean extends VectorExpression {
  private static final long serialVersionUID = 1L;

  public CastTimestampToBoolean(int colNum, int outputColumnNum) {
    super(colNum, outputColumnNum);
  }

  public CastTimestampToBoolean() {
    super();
  }

  private int toBool(TimestampColumnVector timestampColVector, int index) {
    return (timestampColVector.getTime(index) != 0 ||
            timestampColVector.getNanos(index) != 0) ? 1 : 0;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      this.evaluateChildren(batch);
    }

    TimestampColumnVector inputColVector = (TimestampColumnVector) batch.cols[inputColumnNum[0]];
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];
    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    int n = batch.size;
    long[] outputVector = outputColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        outputIsNull[0] = false;
        outputVector[0] = toBool(inputColVector, 0);
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (inputColVector.noNulls) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = false;
          outputVector[i] = toBool(inputColVector, i);
        }
      } else {
        Arrays.fill(outputIsNull, 0, n, false);
        for(int i = 0; i != n; i++) {
          outputVector[i] =  toBool(inputColVector, i);
        }
      }
    } else /* there are NULLs in the inputColVector */ {

      // Carefully handle NULLs...
      outputColVector.noNulls = false;

      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = inputIsNull[i];
          outputVector[i] =  toBool(inputColVector, i);
        }
      } else {
        System.arraycopy(inputIsNull, 0, outputIsNull, 0, n);
        for(int i = 0; i != n; i++) {
          outputVector[i] =  toBool(inputColVector, i);
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumnNum[0]);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.getType(serdeConstants.TIMESTAMP_TYPE_NAME))
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
