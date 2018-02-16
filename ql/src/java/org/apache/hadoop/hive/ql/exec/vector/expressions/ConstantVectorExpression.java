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
import java.sql.Timestamp;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Constant is represented as a vector with repeating values.
 */
public class ConstantVectorExpression extends VectorExpression {

  private static final long serialVersionUID = 1L;

  protected long longValue = 0;
  private double doubleValue = 0;
  private byte[] bytesValue = null;
  private HiveDecimal decimalValue = null;
  private Timestamp timestampValue = null;
  private HiveIntervalDayTime intervalDayTimeValue = null;
  private boolean isNullValue = false;

  private final ColumnVector.Type type;
  private int bytesValueLength = 0;

  public ConstantVectorExpression() {
    super();

    // Dummy final assignments.
    type = null;
  }

  ConstantVectorExpression(int outputColumnNum, TypeInfo outputTypeInfo) throws HiveException {
    super(outputColumnNum);

    this.outputTypeInfo = outputTypeInfo;
    outputDataTypePhysicalVariation = DataTypePhysicalVariation.NONE;

    type = VectorizationContext.getColumnVectorTypeFromTypeInfo(outputTypeInfo);
  }

  public ConstantVectorExpression(int outputColumnNum, long value, TypeInfo outputTypeInfo) throws HiveException {
    this(outputColumnNum, outputTypeInfo);
    this.longValue = value;
  }

  public ConstantVectorExpression(int outputColumnNum, double value, TypeInfo outputTypeInfo) throws HiveException {
    this(outputColumnNum, outputTypeInfo);
    this.doubleValue = value;
  }

  public ConstantVectorExpression(int outputColumnNum, byte[] value, TypeInfo outputTypeInfo) throws HiveException {
    this(outputColumnNum, outputTypeInfo);
    setBytesValue(value);
  }

  public ConstantVectorExpression(int outputColumnNum, HiveChar value, TypeInfo outputTypeInfo)
      throws HiveException {
    this(outputColumnNum, outputTypeInfo);
    setBytesValue(value.getStrippedValue().getBytes());
  }

  public ConstantVectorExpression(int outputColumnNum, HiveVarchar value, TypeInfo outputTypeInfo)
      throws HiveException {
    this(outputColumnNum, outputTypeInfo);
    setBytesValue(value.getValue().getBytes());
  }

  // Include type name for precision/scale.
  public ConstantVectorExpression(int outputColumnNum, HiveDecimal value, TypeInfo outputTypeInfo)
      throws HiveException {
    this(outputColumnNum, outputTypeInfo);
    setDecimalValue(value);
  }

  public ConstantVectorExpression(int outputColumnNum, Timestamp value, TypeInfo outputTypeInfo)
      throws HiveException {
    this(outputColumnNum, outputTypeInfo);
    setTimestampValue(value);
  }

  public ConstantVectorExpression(int outputColumnNum, HiveIntervalDayTime value, TypeInfo outputTypeInfo)
      throws HiveException {
    this(outputColumnNum, outputTypeInfo);
    setIntervalDayTimeValue(value);
  }

  /*
   * Support for null constant object
   */
  public ConstantVectorExpression(int outputColumnNum, TypeInfo outputTypeInfo, boolean isNull)
      throws HiveException {
    this(outputColumnNum, outputTypeInfo);
    isNullValue = isNull;
  }

  /*
   * In the following evaluate* methods, since we are supporting scratch column reuse, we must
   * assume the column may have noNulls of false and some isNull entries true.
   *
   * So, do a proper assignments.
   */

  private void evaluateLong(VectorizedRowBatch vrg) {

    LongColumnVector cv = (LongColumnVector) vrg.cols[outputColumnNum];
    cv.isRepeating = true;
    if (!isNullValue) {
      cv.isNull[0] = false;
      cv.vector[0] = longValue;
    } else {
      cv.isNull[0] = true;
      cv.noNulls = false;
    }
  }

  private void evaluateDouble(VectorizedRowBatch vrg) {
    DoubleColumnVector cv = (DoubleColumnVector) vrg.cols[outputColumnNum];
    cv.isRepeating = true;
    if (!isNullValue) {
      cv.isNull[0] = false;
      cv.vector[0] = doubleValue;
    } else {
      cv.isNull[0] = true;
      cv.noNulls = false;
    }
  }

  private void evaluateBytes(VectorizedRowBatch vrg) {
    BytesColumnVector cv = (BytesColumnVector) vrg.cols[outputColumnNum];
    cv.isRepeating = true;
    cv.initBuffer();
    if (!isNullValue) {
      cv.isNull[0] = false;
      cv.setVal(0, bytesValue, 0, bytesValueLength);
    } else {
      cv.isNull[0] = true;
      cv.noNulls = false;
    }
  }

  private void evaluateDecimal(VectorizedRowBatch vrg) {
    DecimalColumnVector dcv = (DecimalColumnVector) vrg.cols[outputColumnNum];
    dcv.isRepeating = true;
    if (!isNullValue) {
      dcv.isNull[0] = false;
      dcv.set(0, decimalValue);
    } else {
      dcv.isNull[0] = true;
      dcv.noNulls = false;
    }
  }

  private void evaluateTimestamp(VectorizedRowBatch vrg) {
    TimestampColumnVector tcv = (TimestampColumnVector) vrg.cols[outputColumnNum];
    tcv.isRepeating = true;
    if (!isNullValue) {
      tcv.isNull[0] = false;
      tcv.set(0, timestampValue);
    } else {
      tcv.isNull[0] = true;
      tcv.noNulls = false;
    }
  }

  private void evaluateIntervalDayTime(VectorizedRowBatch vrg) {
    IntervalDayTimeColumnVector dcv = (IntervalDayTimeColumnVector) vrg.cols[outputColumnNum];
    dcv.isRepeating = true;
    if (!isNullValue) {
      dcv.isNull[0] = false;
      dcv.set(0, intervalDayTimeValue);
    } else {
      dcv.isNull[0] = true;
      dcv.noNulls = false;
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

  @Override
  public String vectorExpressionParameters() {
    String value;
    if (isNullValue) {
      value = "null";
    } else {
      switch (type) {
      case LONG:
        value = Long.toString(longValue);
        break;
      case DOUBLE:
        value = Double.toString(doubleValue);
        break;
      case BYTES:
        value = new String(bytesValue, StandardCharsets.UTF_8);
        break;
      case DECIMAL:
        value = decimalValue.toString();
        break;
      case TIMESTAMP:
        value = timestampValue.toString();
        break;
      case INTERVAL_DAY_TIME:
        value = intervalDayTimeValue.toString();
        break;
      default:
        throw new RuntimeException("Unknown vector column type " + type);
      }
    }
    return "val " + value;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder()).build();
  }
}
