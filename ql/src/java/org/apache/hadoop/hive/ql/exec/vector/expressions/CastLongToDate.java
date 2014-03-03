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
import org.apache.hadoop.hive.serde2.io.DateWritable;

import java.sql.Date;

/**
 * Casts a timestamp and date vector to a date vector.
 */
public class CastLongToDate extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int inputColumn;
  private int outputColumn;
  private transient Date date = new Date(0);

  public CastLongToDate() {
    super();
  }

  public CastLongToDate(int inputColumn, int outputColumn) {
    this.inputColumn = inputColumn;
    this.outputColumn = outputColumn;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inV = (LongColumnVector) batch.cols[inputColumn];
    int[] sel = batch.selected;
    int n = batch.size;
    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumn];

    if (n == 0) {

      // Nothing to do
      return;
    }

    switch (inputTypes[0]) {
      case TIMESTAMP:
        if (inV.noNulls) {
          outV.noNulls = true;
          if (inV.isRepeating) {
            outV.isRepeating = true;
            date.setTime(inV.vector[0] / 1000000);
            outV.vector[0] = DateWritable.dateToDays(date);
          } else if (batch.selectedInUse) {
            for(int j = 0; j != n; j++) {
              int i = sel[j];
              date.setTime(inV.vector[i] / 1000000);
              outV.vector[i] = DateWritable.dateToDays(date);
            }
            outV.isRepeating = false;
          } else {
            for(int i = 0; i != n; i++) {
              date.setTime(inV.vector[i] / 1000000);
              outV.vector[i] = DateWritable.dateToDays(date);
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
              date.setTime(inV.vector[0] / 1000000);
              outV.vector[0] = DateWritable.dateToDays(date);
            }
          } else if (batch.selectedInUse) {
            for(int j = 0; j != n; j++) {
              int i = sel[j];
              outV.isNull[i] = inV.isNull[i];
              if (!inV.isNull[i]) {
                date.setTime(inV.vector[i] / 1000000);
                outV.vector[i] = DateWritable.dateToDays(date);
              }
            }
            outV.isRepeating = false;
          } else {
            System.arraycopy(inV.isNull, 0, outV.isNull, 0, n);
            for(int i = 0; i != n; i++) {
              if (!inV.isNull[i]) {
                date.setTime(inV.vector[i] / 1000000);
                outV.vector[i] = DateWritable.dateToDays(date);
              }
            }
            outV.isRepeating = false;
          }
        }
        break;

      case DATE:
        inV.copySelected(batch.selectedInUse, batch.selected, batch.size, outV);
        break;
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
    return "date";
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.LONG)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
