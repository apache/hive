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

import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.DynamicValue;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Constant is represented as a vector with repeating values.
 */
public class DynamicValueVectorExpression extends VectorExpression {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicValueVectorExpression.class);

  private static final long serialVersionUID = 1L;

  private final DynamicValue dynamicValue;
  private final TypeInfo typeInfo;
  private final ColumnVector.Type type;

  transient private boolean initialized = false;

  protected long longValue = 0;
  private double doubleValue = 0;
  private byte[] bytesValue = null;
  private HiveDecimal decimalValue = null;
  private Timestamp timestampValue = null;
  private HiveIntervalDayTime intervalDayTimeValue = null;
  private boolean isNullValue = false;

  private int bytesValueLength = 0;

  public DynamicValueVectorExpression() {
    super();

    // Dummy final assignments.
    type = null;
    dynamicValue = null;
    typeInfo = null;
  }

  public DynamicValueVectorExpression(int outputColumnNum, TypeInfo typeInfo,
      DynamicValue dynamicValue) throws HiveException {
    super(outputColumnNum);
    this.type = VectorizationContext.getColumnVectorTypeFromTypeInfo(typeInfo);
    this.dynamicValue = dynamicValue;
    this.typeInfo = typeInfo;
  }

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
    TimestampColumnVector dcv = (TimestampColumnVector) vrg.cols[outputColumnNum];
    dcv.isRepeating = true;
    if (!isNullValue) {
      dcv.isNull[0] = false;
      dcv.set(0, timestampValue);
    } else {
      dcv.isNull[0] = true;
      dcv.noNulls = false;
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

  private void initValue() {
    Object val = dynamicValue.getValue();

    if (val == null) {
      isNullValue = true;
    } else {
      PrimitiveObjectInspector poi = dynamicValue.getObjectInspector();
      byte[] bytesVal;
      switch (poi.getPrimitiveCategory()) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        longValue = PrimitiveObjectInspectorUtils.getLong(val, poi);
        break;
      case FLOAT:
      case DOUBLE:
        doubleValue = PrimitiveObjectInspectorUtils.getDouble(val, poi);
        break;
      case STRING:
      case CHAR:
      case VARCHAR:
        bytesVal = PrimitiveObjectInspectorUtils.getString(val, poi).getBytes();
        setBytesValue(bytesVal);
        break;
      case BINARY:
        bytesVal = PrimitiveObjectInspectorUtils.getBinary(val, poi).copyBytes();
        setBytesValue(bytesVal);
        break;
      case DECIMAL:
        decimalValue = PrimitiveObjectInspectorUtils.getHiveDecimal(val, poi);
        break;
      case DATE:
        longValue = DateWritableV2.dateToDays(PrimitiveObjectInspectorUtils.getDate(val, poi));
      case TIMESTAMP:
        timestampValue = PrimitiveObjectInspectorUtils.getTimestamp(val, poi).toSqlTimestamp();
        break;
      case INTERVAL_YEAR_MONTH:
        longValue = PrimitiveObjectInspectorUtils.getHiveIntervalYearMonth(val, poi).getTotalMonths();
        break;
      case INTERVAL_DAY_TIME:
        intervalDayTimeValue = PrimitiveObjectInspectorUtils.getHiveIntervalDayTime(val, poi);
        break;
      default:
        throw new IllegalStateException("Unsupported type " + poi.getPrimitiveCategory());
      }
    }

    initialized = true;
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    dynamicValue.setConf(conf);
  }

  @Override
  public void evaluate(VectorizedRowBatch vrg) {
    if (!initialized) {
      initValue();
    }

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
    default:
      throw new IllegalStateException("Unsupported type " + type);
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

  public String getTypeString() {
    return outputTypeInfo.toString();
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder()).build();
  }

  @Override
  public String vectorExpressionParameters() {
    return null;
  }
}
