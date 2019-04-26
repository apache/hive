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

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;

public class UDFMaskVectorDate extends UDFMaskBaseVector {

  public UDFMaskVectorDate(int colNum, int outputColumnNum) {
    super(colNum, outputColumnNum);
  }

  public UDFMaskVectorDate() {
    super();
  }

  public void transform(ColumnVector outputColVector, ColumnVector inputVector, int idx) {
    outputColVector.isNull[idx] = false;

    long inputVal = ((LongColumnVector)inputVector).vector[idx];
    Date inputDate = new Date();
    inputDate.setTimeInMillis(DateWritableV2.daysToMillis((int) inputVal));

    int actualMonthValue = maskedMonthValue + 1;
    int year  = maskedYearValue  == UNMASKED_VAL ? inputDate.getYear()  : maskedYearValue;
    int month = maskedMonthValue == UNMASKED_VAL ? inputDate.getMonth() : actualMonthValue;
    int day   = maskedDayValue   == UNMASKED_VAL ? inputDate.getDay()  : maskedDayValue;

    ((LongColumnVector) outputColVector).vector[idx] =
        DateWritableV2.dateToDays(Date.of(year, month, day));
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, getColNum());
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
