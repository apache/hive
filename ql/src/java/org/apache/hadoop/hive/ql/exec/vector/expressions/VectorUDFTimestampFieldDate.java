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

import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;

import com.google.common.base.Preconditions;


/**
 * Abstract class to return various fields from a Timestamp or Date.
 */
public abstract class VectorUDFTimestampFieldDate extends VectorExpression {

  private static final long serialVersionUID = 1L;

  protected int colNum;
  protected int outputColumn;
  protected int field;
  protected transient final Calendar calendar = Calendar.getInstance();

  public VectorUDFTimestampFieldDate(int field, int colNum, int outputColumn) {
    this();
    this.colNum = colNum;
    this.outputColumn = outputColumn;
    this.field = field;
  }

  public VectorUDFTimestampFieldDate() {
    super();
  }

  protected long getDateField(long days) {
    calendar.setTimeInMillis(DateWritable.daysToMillis((int) days));
    return calendar.get(field);
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    Preconditions.checkState(inputTypes[0] == VectorExpression.Type.DATE);

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumn];
    ColumnVector inputColVec = batch.cols[this.colNum];

    /* every line below this is identical for evaluateLong & evaluateString */
    final int n = inputColVec.isRepeating ? 1 : batch.size;
    int[] sel = batch.selected;
    final boolean selectedInUse = (inputColVec.isRepeating == false) && batch.selectedInUse;

    if(batch.size == 0) {
      /* n != batch.size when isRepeating */
      return;
    }

    /* true for all algebraic UDFs with no state */
    outV.isRepeating = inputColVec.isRepeating;

    LongColumnVector longColVector = (LongColumnVector) inputColVec;

    if (inputColVec.noNulls) {
      outV.noNulls = true;
      if (selectedInUse) {
        for(int j=0; j < n; j++) {
          int i = sel[j];
          outV.vector[i] = getDateField(longColVector.vector[i]);
        }
      } else {
        for(int i = 0; i < n; i++) {
          outV.vector[i] = getDateField(longColVector.vector[i]);
        }
      }
    } else {
      // Handle case with nulls. Don't do function if the value is null, to save time,
      // because calling the function can be expensive.
      outV.noNulls = false;
      if (selectedInUse) {
        for(int j=0; j < n; j++) {
          int i = sel[j];
          outV.isNull[i] = inputColVec.isNull[i];
          if (!inputColVec.isNull[i]) {
            outV.vector[i] = getDateField(longColVector.vector[i]);
          }
        }
      } else {
        for(int i = 0; i < n; i++) {
          outV.isNull[i] = inputColVec.isNull[i];
          if (!inputColVec.isNull[i]) {
            outV.vector[i] = getDateField(longColVector.vector[i]);
          }
        }
      }
    }
  }

  @Override
  public int getOutputColumn() {
    return this.outputColumn;
  }

  @Override
  public String getOutputType() {
    return "long";
  }

  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  public int getField() {
    return field;
  }

  public void setField(int field) {
    this.field = field;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
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
