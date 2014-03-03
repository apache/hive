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
import org.apache.hadoop.hive.serde2.io.DateWritable;

import java.util.Date;
import java.text.SimpleDateFormat;

/**
 * Casts a string vector to a date vector.
 */
public class CastStringToDate extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int inputColumn;
  private int outputColumn;
  private transient java.sql.Date sqlDate = new java.sql.Date(0);
  private transient SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

  public CastStringToDate() {

  }

  public CastStringToDate(int inputColumn, int outputColumn) {
    this.inputColumn = inputColumn;
    this.outputColumn = outputColumn;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    BytesColumnVector inV = (BytesColumnVector) batch.cols[inputColumn];
    int[] sel = batch.selected;
    int n = batch.size;
    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumn];

    if (n == 0) {

      // Nothing to do
      return;
    }

    if (inV.noNulls) {
      outV.noNulls = true;
      if (inV.isRepeating) {
        outV.isRepeating = true;
        evaluate(outV, inV, 0);
      } else if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          evaluate(outV, inV, i);
        }
        outV.isRepeating = false;
      } else {
        for(int i = 0; i != n; i++) {
          evaluate(outV, inV, i);
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
          evaluate(outV, inV, 0);
        }
      } else if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outV.isNull[i] = inV.isNull[i];
          if (!inV.isNull[i]) {
            evaluate(outV, inV, i);
          }
        }
        outV.isRepeating = false;
      } else {
        System.arraycopy(inV.isNull, 0, outV.isNull, 0, n);
        for(int i = 0; i != n; i++) {
          if (!inV.isNull[i]) {
            evaluate(outV, inV, i);
          }
        }
        outV.isRepeating = false;
      }
    }
  }

  private void evaluate(LongColumnVector outV, BytesColumnVector inV, int i) {
    try {
      Date utilDate = formatter.parse(new String(inV.vector[i], inV.start[i], inV.length[i], "UTF-8"));
      sqlDate.setTime(utilDate.getTime());
      outV.vector[i] = DateWritable.dateToDays(sqlDate);
    } catch (Exception e) {
      outV.vector[i] = 1;
      outV.isNull[i] = true;
      outV.noNulls = false;
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
            VectorExpressionDescriptor.ArgumentType.STRING)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
