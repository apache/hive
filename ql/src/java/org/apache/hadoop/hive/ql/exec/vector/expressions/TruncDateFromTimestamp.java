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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.ArgumentType;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Vectorized implementation of trunc(date, fmt) function for timestamp input
 */
public class TruncDateFromTimestamp extends VectorExpression {
  private static final long serialVersionUID = 1L;
  private final String fmt;

  public TruncDateFromTimestamp(int colNum, byte[] fmt, int outputColumnNum) {
    super(colNum, outputColumnNum);
    this.fmt = new String(fmt, StandardCharsets.UTF_8);
  }

  public TruncDateFromTimestamp() {
    super();
    this.fmt = "";
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + inputColumnNum[0] + ", format " + fmt;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      this.evaluateChildren(batch);
    }

    ColumnVector inputColVector = batch.cols[inputColumnNum[0]];
    BytesColumnVector outputColVector = (BytesColumnVector) batch.cols[outputColumnNum];

    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    int n = batch.size;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        outputIsNull[0] = false;
        truncDate(inputColVector, outputColVector, 0);
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (inputColVector.noNulls) {
      if (batch.selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.
        if (!outputColVector.noNulls) {
          for (int j = 0; j != n; j++) {
            final int i = sel[j];
            // Set isNull before call in case it changes it mind.
            outputIsNull[i] = false;
            truncDate(inputColVector, outputColVector, i);
          }
        } else {
          for (int j = 0; j != n; j++) {
            final int i = sel[j];
            truncDate(inputColVector, outputColVector, i);
          }
        }
      } else {
        if (!outputColVector.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        for (int i = 0; i != n; i++) {
          truncDate(inputColVector, outputColVector, i);
        }
      }
    } else /* there are nulls in the inputColVector */ {

      // Carefully handle NULLs...
      outputColVector.noNulls = false;

      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = inputIsNull[i];
          if (!inputIsNull[i]) {
            truncDate(inputColVector, outputColVector, i);
          }
        }
      } else {
        System.arraycopy(inputIsNull, 0, outputIsNull, 0, n);
        for (int i = 0; i != n; i++) {
          if (!inputColVector.isNull[i]) {
            truncDate(inputColVector, outputColVector, i);
          }
        }
      }
    }
  }

  protected void truncDate(ColumnVector inV, BytesColumnVector outV, int i) {
    Date date = Date.ofEpochMilli(((TimestampColumnVector) inV).getTime(i));
    processDate(outV, i, date);
  }

  protected void processDate(BytesColumnVector outV, int i, Date date) {
    switch (fmt) {
    case "YEAR":
    case "YYYY":
    case "YY":
      date.setMonth(1);
      /* fall through */
    case "MONTH":
    case "MON":
    case "MM":
      date.setDayOfMonth(1);
      break;
    case "QUARTER":
    case "Q":
      int month = date.getMonth() - 1;
      int quarter = month / 3;
      int monthToSet = quarter * 3 + 1;
      date.setMonth(monthToSet);
      date.setDayOfMonth(1);
      break;
    default:
      break;
    }

    byte[] bytes = date.toString().getBytes(StandardCharsets.UTF_8);
    outV.setVal(i, bytes, 0, bytes.length);
  }

  @Override
  public Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION).setNumArguments(2)
        .setArgumentTypes(getInputColumnType(),
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
        .setInputExpressionTypes(VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }

  protected ArgumentType getInputColumnType() {
    return VectorExpressionDescriptor.ArgumentType.TIMESTAMP;
  }
}
