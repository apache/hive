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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

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
  private ConstantVectorExpression[] structValue;
  private boolean isNullValue = false;

  private final ColumnVector.Type type;
  private int bytesValueLength = 0;

  public ConstantVectorExpression() {
    super();

    // Dummy final assignments.
    type = null;
  }

  ConstantVectorExpression(int outputColumnNum, TypeInfo outputTypeInfo) throws HiveException {
    super(-1, outputColumnNum);

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
  public static VectorExpression createList(int outputColumnNum, Object value, TypeInfo outputTypeInfo)
      throws HiveException {
    ConstantVectorExpression result = new ConstantVectorExpression(outputColumnNum, outputTypeInfo);
    result.setListValue(value);
    return result;
  }

  public static VectorExpression createMap(int outputColumnNum, Object value, TypeInfo outputTypeInfo)
      throws HiveException {
    ConstantVectorExpression result = new ConstantVectorExpression(outputColumnNum, outputTypeInfo);
    result.setMapValue(value);
    return result;
  }
  */

  public static ConstantVectorExpression createStruct(int outputColumnNum, Object value,
      TypeInfo outputTypeInfo)
          throws HiveException {
    ConstantVectorExpression result = new ConstantVectorExpression(outputColumnNum, outputTypeInfo);
    result.setStructValue(value);
    return result;
  }

  /*
  public static VectorExpression createUnion(int outputColumnNum, Object value, TypeInfo outputTypeInfo)
      throws HiveException {
    ConstantVectorExpression result = new ConstantVectorExpression(outputColumnNum, outputTypeInfo);
    result.setUnionValue(value);
    return result;
  }
  */

  public static ConstantVectorExpression create(int outputColumnNum, Object constantValue, TypeInfo outputTypeInfo)
      throws HiveException {

    if (constantValue == null) {
      return new ConstantVectorExpression(outputColumnNum, outputTypeInfo, true);
    }

    Category category = outputTypeInfo.getCategory();
    switch (category) {
    case PRIMITIVE:
      {
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) outputTypeInfo;
        PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
        switch (primitiveCategory) {
        case BOOLEAN:
          if (((Boolean) constantValue).booleanValue()) {
            return new ConstantVectorExpression(outputColumnNum, 1, outputTypeInfo);
          } else {
            return new ConstantVectorExpression(outputColumnNum, 0, outputTypeInfo);
          }
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
          return new ConstantVectorExpression(
              outputColumnNum, ((Number) constantValue).longValue(), outputTypeInfo);
        case FLOAT:
        case DOUBLE:
          return new ConstantVectorExpression(
              outputColumnNum, ((Number) constantValue).doubleValue(), outputTypeInfo);
        case DATE:
          return new ConstantVectorExpression(
              outputColumnNum, DateWritableV2.dateToDays((Date) constantValue), outputTypeInfo);
        case TIMESTAMP:
          return new ConstantVectorExpression(
              outputColumnNum,
              ((org.apache.hadoop.hive.common.type.Timestamp) constantValue).toSqlTimestamp(),
              outputTypeInfo);
        case DECIMAL:
          return new ConstantVectorExpression(
              outputColumnNum, (HiveDecimal) constantValue, outputTypeInfo);
        case STRING:
          return new ConstantVectorExpression(
              outputColumnNum, ((String) constantValue).getBytes(StandardCharsets.UTF_8), outputTypeInfo);
        case VARCHAR:
          return new ConstantVectorExpression(
              outputColumnNum, ((HiveVarchar) constantValue), outputTypeInfo);
        case CHAR:
          return new ConstantVectorExpression(
              outputColumnNum, ((HiveChar) constantValue), outputTypeInfo);
        case BINARY:
          return new ConstantVectorExpression(
              outputColumnNum, ((byte[]) constantValue), outputTypeInfo);
        case INTERVAL_YEAR_MONTH:
          return new ConstantVectorExpression(
              outputColumnNum,
              ((HiveIntervalYearMonth) constantValue).getTotalMonths(),
              outputTypeInfo);
        case INTERVAL_DAY_TIME:
          return new ConstantVectorExpression(
              outputColumnNum,
              (HiveIntervalDayTime) constantValue,
              outputTypeInfo);
        case VOID:
        case TIMESTAMPLOCALTZ:
        case UNKNOWN:
        default:
          throw new RuntimeException("Unexpected primitive category " + primitiveCategory);
        }
      }
    // case LIST:
    //   return ConstantVectorExpression.createList(
    //       outputColumnNum, constantValue, outputTypeInfo);
    // case MAP:
    //   return ConstantVectorExpression.createMap(
    //       outputColumnNum, constantValue, outputTypeInfo);
    case STRUCT:
      return ConstantVectorExpression.createStruct(
          outputColumnNum, constantValue, outputTypeInfo);
    // case UNION:
    //   return ConstantVectorExpression.createUnion(
    //       outputColumnNum, constantValue, outputTypeInfo);
    default:
      throw new RuntimeException("Unexpected category " + category);
    }
  }

  /*
   * In the following evaluate* methods, since we are supporting scratch column reuse, we must
   * assume the column may have noNulls of false and some isNull entries true.
   *
   * So, do a proper assignments.
   */

  private void evaluateLong(ColumnVector colVector) {

    LongColumnVector cv = (LongColumnVector) colVector;
    cv.isRepeating = true;
    if (!isNullValue) {
      cv.isNull[0] = false;
      cv.vector[0] = longValue;
    } else {
      cv.isNull[0] = true;
      cv.noNulls = false;
    }
  }

  private void evaluateDouble(ColumnVector colVector) {
    DoubleColumnVector cv = (DoubleColumnVector) colVector;
    cv.isRepeating = true;
    if (!isNullValue) {
      cv.isNull[0] = false;
      cv.vector[0] = doubleValue;
    } else {
      cv.isNull[0] = true;
      cv.noNulls = false;
    }
  }

  private void evaluateBytes(ColumnVector colVector) {
    BytesColumnVector cv = (BytesColumnVector) colVector;
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

  private void evaluateDecimal(ColumnVector colVector) {
    DecimalColumnVector dcv = (DecimalColumnVector) colVector;
    dcv.isRepeating = true;
    if (!isNullValue) {
      dcv.isNull[0] = false;
      dcv.set(0, decimalValue);
    } else {
      dcv.isNull[0] = true;
      dcv.noNulls = false;
    }
  }

  private void evaluateDecimal64(ColumnVector colVector) {
    Decimal64ColumnVector dcv = (Decimal64ColumnVector) colVector;
    dcv.isRepeating = true;
    if (!isNullValue) {
      dcv.isNull[0] = false;
      dcv.vector[0] = longValue;
    } else {
      dcv.isNull[0] = true;
      dcv.noNulls = false;
    }
  }

  private void evaluateTimestamp(ColumnVector colVector) {
    TimestampColumnVector tcv = (TimestampColumnVector) colVector;
    tcv.isRepeating = true;
    if (!isNullValue) {
      tcv.isNull[0] = false;
      tcv.set(0, timestampValue);
    } else {
      tcv.isNull[0] = true;
      tcv.noNulls = false;
    }
  }

  private void evaluateIntervalDayTime(ColumnVector colVector) {
    IntervalDayTimeColumnVector dcv = (IntervalDayTimeColumnVector) colVector;
    dcv.isRepeating = true;
    if (!isNullValue) {
      dcv.isNull[0] = false;
      dcv.set(0, intervalDayTimeValue);
    } else {
      dcv.isNull[0] = true;
      dcv.noNulls = false;
    }
  }

  private void evaluateStruct(ColumnVector colVector) {
    StructColumnVector scv = (StructColumnVector) colVector;
    scv.isRepeating = true;
    if (!isNullValue) {
      scv.isNull[0] = false;
      final int size = structValue.length;
      for (int i = 0; i < size; i++) {
        structValue[i].evaluateColumn(scv.fields[i]);
      }
    } else {
      scv.isNull[0] = true;
      scv.noNulls = false;
    }
  }

  private void evaluateVoid(ColumnVector colVector) {
    VoidColumnVector voidColVector = (VoidColumnVector) colVector;
    voidColVector.isRepeating = true;
    voidColVector.isNull[0] = true;
    voidColVector.noNulls = false;
  }

  @Override
  public void evaluate(VectorizedRowBatch vrg) {
    evaluateColumn(vrg.cols[outputColumnNum]);
  }

  private void evaluateColumn(ColumnVector colVector) {
    switch (type) {
    case LONG:
      evaluateLong(colVector);
      break;
    case DOUBLE:
      evaluateDouble(colVector);
      break;
    case BYTES:
      evaluateBytes(colVector);
      break;
    case DECIMAL:
      if (outputDataTypePhysicalVariation == DataTypePhysicalVariation.DECIMAL_64) {
        evaluateDecimal64(colVector);
      } else {
        evaluateDecimal(colVector);
      }
      break;
    case TIMESTAMP:
      evaluateTimestamp(colVector);
      break;
    case INTERVAL_DAY_TIME:
      evaluateIntervalDayTime(colVector);
      break;
    case STRUCT:
      evaluateStruct(colVector);
      break;
    case VOID:
      evaluateVoid(colVector);
      break;
    default:
      throw new RuntimeException("Unexpected column vector type " + type);
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

  public Object getValue() {
    switch (type) {
    case LONG:
      return getLongValue();
    case DOUBLE:
      return getDoubleValue();
    case BYTES:
      return getBytesValue();
    case DECIMAL:
      return getDecimalValue();
    case TIMESTAMP:
      return getTimestampValue();
    case INTERVAL_DAY_TIME:
      return getIntervalDayTimeValue();
    default:
      throw new RuntimeException("Unexpected column vector type " + type);
    }
  }

  public void setStructValue(Object structValue) throws HiveException {
    StructTypeInfo structTypeInfo = (StructTypeInfo) outputTypeInfo;
    List<TypeInfo> fieldTypeInfoList = structTypeInfo.getAllStructFieldTypeInfos();
    final int size = fieldTypeInfoList.size();
    this.structValue = new ConstantVectorExpression[size];
    List<Object> fieldValueList = (List<Object>) structValue;
    for (int i = 0; i < size; i++) {
      this.structValue[i] = create(i, fieldValueList.get(i), fieldTypeInfoList.get(i));
    }
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
        value = org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(
            timestampValue.getTime(), timestampValue.getNanos()).toString();
        break;
      case INTERVAL_DAY_TIME:
        value = intervalDayTimeValue.toString();
        break;
      case STRUCT:
        {
          StringBuilder sb = new StringBuilder();
          sb.append("STRUCT {");
          boolean isFirst = true;
          final int size = structValue.length;
          for (int i = 0; i < size; i++) {
            if (isFirst) {
              isFirst = false;
            } else {
              sb.append(", ");
            }
            sb.append(structValue[i].toString());
          }
          sb.append("}");
          value = sb.toString();
        }
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

  public boolean getIsNullValue() {
    return isNullValue;
  }
}
