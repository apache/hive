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
import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.common.util.DateUtils;

import com.google.common.base.Preconditions;

/**
 * Abstract class to return various fields from a Timestamp.
 */
public abstract class VectorUDFTimestampFieldTimestamp extends VectorExpression {

  private static final long serialVersionUID = 1L;

  protected final int colNum;
  protected final int field;

  protected transient final Calendar calendar = Calendar.getInstance();

  public VectorUDFTimestampFieldTimestamp(int field, int colNum, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum = colNum;
    this.field = field;
  }

  public VectorUDFTimestampFieldTimestamp() {
    super();

    // Dummy final assignments.
    colNum = -1;
    field = -1;
  }

  public void initCalendar() {
  }

  @Override
  public void transientInit() throws HiveException {
    super.transientInit();
    initCalendar();
  }

  protected long getTimestampField(TimestampColumnVector timestampColVector, int elementNum) {
    calendar.setTime(timestampColVector.asScratchTimestamp(elementNum));
    return calendar.get(field);
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    Preconditions.checkState(
        ((PrimitiveTypeInfo) inputTypeInfos[0]).getPrimitiveCategory() == PrimitiveCategory.TIMESTAMP);

    if (childExpressions != null) {
        super.evaluateChildren(batch);
      }

    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumnNum];
    ColumnVector inputColVec = batch.cols[this.colNum];

    /* every line below this is identical for evaluateLong & evaluateString */
    final int n = inputColVec.isRepeating ? 1 : batch.size;
    int[] sel = batch.selected;
    final boolean selectedInUse = (inputColVec.isRepeating == false) && batch.selectedInUse;

    if(batch.size == 0) {
      /* n != batch.size when isRepeating */
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outV.isRepeating = false;

    TimestampColumnVector timestampColVector = (TimestampColumnVector) inputColVec;

    if (inputColVec.isRepeating) {
      if (inputColVec.noNulls || !inputColVec.isNull[0]) {
        outV.isNull[0] = false;
        outV.vector[0] = getTimestampField(timestampColVector, 0);
      } else {
        outV.isNull[0] = true;
        outV.noNulls = false;
      }
      outV.isRepeating = true;
      return;
    }

    if (inputColVec.noNulls) {
      if (selectedInUse) {
        for(int j=0; j < n; j++) {
          int i = sel[j];
          outV.isNull[i] = false;
          outV.vector[i] = getTimestampField(timestampColVector, i);
        }
      } else {
        Arrays.fill(outV.isNull, 0, n, false);
        for(int i = 0; i < n; i++) {
          outV.vector[i] = getTimestampField(timestampColVector, i);
        }
      }
    } else /* there are nulls in the inputColVector */ {

      // Carefully handle NULLs...
      outV.noNulls = false;

      if (selectedInUse) {
        for(int j=0; j < n; j++) {
          int i = sel[j];
          outV.isNull[i] = inputColVec.isNull[i];
          if (!inputColVec.isNull[i]) {
            outV.vector[i] = getTimestampField(timestampColVector, i);
          }
        }
      } else {
        for(int i = 0; i < n; i++) {
          outV.isNull[i] = inputColVec.isNull[i];
          if (!inputColVec.isNull[i]) {
            outV.vector[i] = getTimestampField(timestampColVector, i);
          }
        }
      }
    }
  }

  public String vectorExpressionParameters() {
    if (field == -1) {
      return getColumnParamString(0, colNum);
    } else {
      return getColumnParamString(0, colNum) + ", field " + DateUtils.getFieldName(field);
    }
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
