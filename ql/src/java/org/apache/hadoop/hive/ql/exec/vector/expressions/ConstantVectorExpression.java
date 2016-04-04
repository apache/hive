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

import java.sql.Timestamp;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Constant is represented as a vector with repeating values.
 */
public class ConstantVectorExpression extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private int outputColumn;
  protected long longValue = 0;
  private double doubleValue = 0;
  private byte[] bytesValue = null;
  private HiveDecimal decimalValue = null;
  private Timestamp timestampValue = null;
  private HiveIntervalDayTime intervalDayTimeValue = null;
  private boolean isNullValue = false;

  private ColumnVector.Type type;
  private int bytesValueLength = 0;

  public ConstantVectorExpression() {
    super();
  }

  ConstantVectorExpression(int outputColumn, String typeString) {
    this();
    this.outputColumn = outputColumn;
    setTypeString(typeString);
  }

  public ConstantVectorExpression(int outputColumn, long value) {
    this(outputColumn, "long");
    this.longValue = value;
  }

  public ConstantVectorExpression(int outputColumn, double value) {
    this(outputColumn, "double");
    this.doubleValue = value;
  }

  public ConstantVectorExpression(int outputColumn, byte[] value) {
    this(outputColumn, "string");
    setBytesValue(value);
  }

  public ConstantVectorExpression(int outputColumn, HiveChar value) {
    this(outputColumn, "char");
    setBytesValue(value.getStrippedValue().getBytes());
  }

  public ConstantVectorExpression(int outputColumn, HiveVarchar value) {
    this(outputColumn, "varchar");
    setBytesValue(value.getValue().getBytes());
  }

  // Include type name for precision/scale.
  public ConstantVectorExpression(int outputColumn, HiveDecimal value, String typeName) {
    this(outputColumn, typeName);
    setDecimalValue(value);
  }

  public ConstantVectorExpression(int outputColumn, Timestamp value) {
    this(outputColumn, "timestamp");
    setTimestampValue(value);
  }

  public ConstantVectorExpression(int outputColumn, HiveIntervalDayTime value) {
    this(outputColumn, "interval_day_time");
    setIntervalDayTimeValue(value);
  }

  /*
   * Support for null constant object
   */
  public ConstantVectorExpression(int outputColumn, String typeString, boolean isNull) {
    this(outputColumn, typeString);
    isNullValue = isNull;
  }

  private void evaluateLong(VectorizedRowBatch vrg) {
    LongColumnVector cv = (LongColumnVector) vrg.cols[outputColumn];
    cv.isRepeating = true;
    cv.noNulls = !isNullValue;
    if (!isNullValue) {
      cv.vector[0] = longValue;
    } else {
      cv.isNull[0] = true;
    }
  }

  private void evaluateDouble(VectorizedRowBatch vrg) {
    DoubleColumnVector cv = (DoubleColumnVector) vrg.cols[outputColumn];
    cv.isRepeating = true;
    cv.noNulls = !isNullValue;
    if (!isNullValue) {
      cv.vector[0] = doubleValue;
    } else {
      cv.isNull[0] = true;
    }
  }

  private void evaluateBytes(VectorizedRowBatch vrg) {
    BytesColumnVector cv = (BytesColumnVector) vrg.cols[outputColumn];
    cv.isRepeating = true;
    cv.noNulls = !isNullValue;
    cv.initBuffer();
    if (!isNullValue) {
      cv.setVal(0, bytesValue, 0, bytesValueLength);
    } else {
      cv.isNull[0] = true;
    }
  }

  private void evaluateDecimal(VectorizedRowBatch vrg) {
    DecimalColumnVector dcv = (DecimalColumnVector) vrg.cols[outputColumn];
    dcv.isRepeating = true;
    dcv.noNulls = !isNullValue;
    if (!isNullValue) {
      dcv.vector[0].set(decimalValue);
    } else {
      dcv.isNull[0] = true;
    }
  }

  private void evaluateTimestamp(VectorizedRowBatch vrg) {
    TimestampColumnVector dcv = (TimestampColumnVector) vrg.cols[outputColumn];
    dcv.isRepeating = true;
    dcv.noNulls = !isNullValue;
    if (!isNullValue) {
      dcv.set(0, timestampValue);
    } else {
      dcv.isNull[0] = true;
    }
  }

  private void evaluateIntervalDayTime(VectorizedRowBatch vrg) {
    IntervalDayTimeColumnVector dcv = (IntervalDayTimeColumnVector) vrg.cols[outputColumn];
    dcv.isRepeating = true;
    dcv.noNulls = !isNullValue;
    if (!isNullValue) {
      dcv.set(0, intervalDayTimeValue);
    } else {
      dcv.isNull[0] = true;
    }
  }

  @Override
  public void evaluate(VectorizedRowBatch vrg) {
    switch (type) {
    case LONG:
      evaluateLong(vrg);
      break;
    case DOUBLE:
      evaluateDouble(vrg);
      break;
    case BYTES:
      evaluateBytes(vrg);
      break;
    case DECIMAL:
      evaluateDecimal(vrg);
      break;
    case TIMESTAMP:
      evaluateTimestamp(vrg);
      break;
    case INTERVAL_DAY_TIME:
      evaluateIntervalDayTime(vrg);
      break;
    }
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  public long getLongValue() {
    return longValue;
  }

  public void setLongValue(long longValue) {
    this.longValue = longValue;
  }

  public double getDoubleValue() {
    return doubleValue;
  }

  public void setDoubleValue(double doubleValue) {
    this.doubleValue = doubleValue;
  }

  public byte[] getBytesValue() {
    return bytesValue;
  }

  public void setBytesValue(byte[] bytesValue) {
    this.bytesValue = bytesValue.clone();
    this.bytesValueLength = bytesValue.length;
  }

  public void setDecimalValue(HiveDecimal decimalValue) {
    this.decimalValue = decimalValue;
  }

  public HiveDecimal getDecimalValue() {
    return decimalValue;
  }

  public void setTimestampValue(Timestamp timestampValue) {
    this.timestampValue = timestampValue;
  }

  public Timestamp getTimestampValue() {
    return timestampValue;
  }

  public void setIntervalDayTimeValue(HiveIntervalDayTime intervalDayTimeValue) {
    this.intervalDayTimeValue = intervalDayTimeValue;
  }

  public HiveIntervalDayTime getIntervalDayTimeValue() {
    return intervalDayTimeValue;
  }

  public String getTypeString() {
    return getOutputType();
  }

  private void setTypeString(String typeString) {
    this.outputType = typeString;

    String typeName = VectorizationContext.mapTypeNameSynonyms(outputType);
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);
    this.type = VectorizationContext.getColumnVectorTypeFromTypeInfo(typeInfo);
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder()).build();
  }
}
