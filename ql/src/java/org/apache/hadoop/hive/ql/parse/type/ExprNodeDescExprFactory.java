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
package org.apache.hadoop.hive.ql.parse.type;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.optimizer.ConstantPropagateProcFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSubquerySemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.QBSubQueryParseInfo;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprDynamicParamDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeSubQueryDesc;
import org.apache.hadoop.hive.ql.plan.SubqueryType;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualNS;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqualNS;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStruct;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Expression factory for Hive {@link ExprNodeDesc}.
 */
public class ExprNodeDescExprFactory extends ExprFactory<ExprNodeDesc> {

  private static final Logger LOG = LoggerFactory.getLogger(ExprNodeDescExprFactory.class);

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isExprInstance(Object o) {
    return o instanceof ExprNodeDesc;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeDesc toExpr(ColumnInfo colInfo, RowResolver rowResolver, int offset)
      throws SemanticException {
    ObjectInspector inspector = colInfo.getObjectInspector();
    if (inspector instanceof ConstantObjectInspector) {
      if (inspector instanceof PrimitiveObjectInspector) {
        return toPrimitiveConstDesc(colInfo, inspector);
      }

      Object inputConstantValue = ((ConstantObjectInspector) inspector).getWritableConstantValue();
      if (inputConstantValue == null) {
        return createExprNodeConstantDesc(colInfo, null);
      }

      if (inspector instanceof ListObjectInspector) {
        ObjectInspector listElementOI = ((ListObjectInspector) inspector).getListElementObjectInspector();
        if (listElementOI instanceof PrimitiveObjectInspector) {
          PrimitiveObjectInspector poi = (PrimitiveObjectInspector) listElementOI;
          return createExprNodeConstantDesc(colInfo, toListConstant((List<?>) inputConstantValue, poi));
        }
      }
      if (inspector instanceof MapObjectInspector) {
        ObjectInspector keyOI = ((MapObjectInspector)inspector).getMapKeyObjectInspector();
        ObjectInspector valueOI = ((MapObjectInspector)inspector).getMapValueObjectInspector();
        if (keyOI instanceof PrimitiveObjectInspector && valueOI instanceof PrimitiveObjectInspector) {
          return createExprNodeConstantDesc(colInfo, toMapConstant((Map<?, ?>) inputConstantValue, keyOI, valueOI));
        }
      }
      if (inspector instanceof StructObjectInspector) {
        boolean allPrimitive = true;
        List<? extends StructField> fields = ((StructObjectInspector)inspector).getAllStructFieldRefs();
        for (StructField field : fields) {
          allPrimitive &= field.getFieldObjectInspector() instanceof PrimitiveObjectInspector;
        }
        if (allPrimitive) {
          return createExprNodeConstantDesc(colInfo, toStructConstDesc(
              (List<?>) ((ConstantObjectInspector) inspector).getWritableConstantValue(), fields));
        }
      }
    }
    // non-constant or non-primitive constants
    ExprNodeColumnDesc column = new ExprNodeColumnDesc(colInfo);
    column.setSkewedCol(colInfo.isSkewedCol());
    return column;
  }

  private static ExprNodeConstantDesc createExprNodeConstantDesc(ColumnInfo colInfo, Object constantValue) {
    ExprNodeConstantDesc constantExpr = new ExprNodeConstantDesc(colInfo.getType(), constantValue);
    constantExpr.setFoldedFromCol(colInfo.getInternalName());
    constantExpr.setFoldedFromTab(colInfo.getTabAlias());
    return constantExpr;
  }

  private static ExprNodeConstantDesc toPrimitiveConstDesc(ColumnInfo colInfo, ObjectInspector inspector) {
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) inspector;
    Object constant = ((ConstantObjectInspector) inspector).getWritableConstantValue();
    ExprNodeConstantDesc constantExpr =
        new ExprNodeConstantDesc(colInfo.getType(), poi.getPrimitiveJavaObject(constant));
    constantExpr.setFoldedFromCol(colInfo.getInternalName());
    constantExpr.setFoldedFromTab(colInfo.getTabAlias());
    return constantExpr;
  }

  private static List<Object> toListConstant(List<?> constantValue, PrimitiveObjectInspector poi) {
    List<Object> constant = new ArrayList<>(constantValue.size());
    for (Object o : constantValue) {
      constant.add(poi.getPrimitiveJavaObject(o));
    }
    return constant;
  }

  private static Map<Object, Object> toMapConstant(
      Map<?, ?> constantValue, ObjectInspector keyOI, ObjectInspector valueOI) {
    PrimitiveObjectInspector keyPoi = (PrimitiveObjectInspector) keyOI;
    PrimitiveObjectInspector valuePoi = (PrimitiveObjectInspector) valueOI;
    Map<Object, Object> constant = new LinkedHashMap<>(constantValue.size());
    for (Map.Entry<?, ?> e : constantValue.entrySet()) {
      constant.put(keyPoi.getPrimitiveJavaObject(e.getKey()), valuePoi.getPrimitiveJavaObject(e.getValue()));
    }
    return constant;
  }

  private static List<Object> toStructConstDesc(List<?> constantValue, List<? extends StructField> fields) {
    List<Object> constant = new ArrayList<>(constantValue.size());
    for (int i = 0; i < constantValue.size(); i++) {
      Object value = constantValue.get(i);
      PrimitiveObjectInspector fieldPoi = (PrimitiveObjectInspector) fields.get(i).getFieldObjectInspector();
      constant.add(fieldPoi.getPrimitiveJavaObject(value));
    }
    return constant;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeColumnDesc createColumnRefExpr(ColumnInfo colInfo, RowResolver rowResolver, int offset) {
    return new ExprNodeColumnDesc(colInfo);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeColumnDesc createColumnRefExpr(ColumnInfo colInfo, List<RowResolver> rowResolverList) {
    return new ExprNodeColumnDesc(colInfo);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createNullConstantExpr() {
    return new ExprNodeConstantDesc(TypeInfoFactory.
        getPrimitiveTypeInfoFromPrimitiveWritable(NullWritable.class), null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprDynamicParamDesc createDynamicParamExpr(int index) {
    return new ExprDynamicParamDesc(TypeInfoFactory.
        getPrimitiveTypeInfoFromPrimitiveWritable(NullWritable.class), index,null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createBooleanConstantExpr(String value) {
    Boolean b = value != null ? Boolean.valueOf(value) : null;
    return new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, b);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createBigintConstantExpr(String value) {
    Long l = Long.valueOf(value);
    return new ExprNodeConstantDesc(l);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createIntConstantExpr(String value) {
    Integer i = Integer.valueOf(value);
    return new ExprNodeConstantDesc(i);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createSmallintConstantExpr(String value) {
    Short s = Short.valueOf(value);
    return new ExprNodeConstantDesc(s);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createTinyintConstantExpr(String value) {
    Byte b = Byte.valueOf(value);
    return new ExprNodeConstantDesc(b);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createFloatConstantExpr(String value) {
    Float f = Float.valueOf(value);
    return new ExprNodeConstantDesc(f);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createDoubleConstantExpr(String value) {
    Double d = Double.valueOf(value);
    return new ExprNodeConstantDesc(d);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createDecimalConstantExpr(String value, boolean allowNullValueConstantExpr) {
    HiveDecimal hd = HiveDecimal.create(value);
    if (!allowNullValueConstantExpr && hd == null) {
      return null;
    }
    return new ExprNodeConstantDesc(adjustType(hd), hd);
  }

  @Override
  protected TypeInfo adjustConstantType(PrimitiveTypeInfo targetType, Object constantValue) {
    if (constantValue instanceof HiveDecimal) {
      return adjustType((HiveDecimal) constantValue);
    }
    return targetType;
  }

  private DecimalTypeInfo adjustType(HiveDecimal hd) {
    // Note: the normalize() call with rounding in HiveDecimal will currently reduce the
    //       precision and scale of the value by throwing away trailing zeroes. This may or may
    //       not be desirable for the literals; however, this used to be the default behavior
    //       for explicit decimal literals (e.g. 1.0BD), so we keep this behavior for now.
    int prec = 1;
    int scale = 0;
    if (hd != null) {
      prec = hd.precision();
      scale = hd.scale();
    }
    DecimalTypeInfo typeInfo = TypeInfoFactory.getDecimalTypeInfo(prec, scale);
    return typeInfo;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Object interpretConstantAsPrimitive(PrimitiveTypeInfo targetType, Object constantValue,
      PrimitiveTypeInfo sourceType, boolean isEqual) {
    if (constantValue instanceof Number || constantValue instanceof String) {
      try {
        PrimitiveTypeEntry primitiveTypeEntry = targetType.getPrimitiveTypeEntry();
        if (PrimitiveObjectInspectorUtils.intTypeEntry.equals(primitiveTypeEntry)) {
          return toBigDecimal(constantValue.toString()).intValueExact();
        } else if (PrimitiveObjectInspectorUtils.longTypeEntry.equals(primitiveTypeEntry)) {
          return toBigDecimal(constantValue.toString()).longValueExact();
        } else if (PrimitiveObjectInspectorUtils.doubleTypeEntry.equals(primitiveTypeEntry)) {
          return Double.valueOf(constantValue.toString());
        } else if (PrimitiveObjectInspectorUtils.floatTypeEntry.equals(primitiveTypeEntry)) {
          return Float.valueOf(constantValue.toString());
        } else if (PrimitiveObjectInspectorUtils.byteTypeEntry.equals(primitiveTypeEntry)) {
          return toBigDecimal(constantValue.toString()).byteValueExact();
        } else if (PrimitiveObjectInspectorUtils.shortTypeEntry.equals(primitiveTypeEntry)) {
          return toBigDecimal(constantValue.toString()).shortValueExact();
        } else if (PrimitiveObjectInspectorUtils.decimalTypeEntry.equals(primitiveTypeEntry)) {
          return HiveDecimal.create(constantValue.toString());
        }
      } catch (NumberFormatException | ArithmeticException nfe) {
        if (!isEqual && (constantValue instanceof Number ||
            NumberUtils.isNumber(constantValue.toString()))) {
          // The target is a number, if constantToInterpret can be interpreted as a number,
          // return the constantToInterpret directly, GenericUDFBaseCompare will do
          // type conversion for us.
          return constantValue;
        }
        LOG.trace("Failed to narrow type of constant", nfe);
        return null;
      }
    }

    // Comparision of decimal and float/double happens in float/double.
    if (constantValue instanceof HiveDecimal) {
      HiveDecimal hiveDecimal = (HiveDecimal) constantValue;

      PrimitiveTypeEntry primitiveTypeEntry = targetType.getPrimitiveTypeEntry();
      if (PrimitiveObjectInspectorUtils.doubleTypeEntry.equals(primitiveTypeEntry)) {
        return hiveDecimal.doubleValue();
      } else if (PrimitiveObjectInspectorUtils.floatTypeEntry.equals(primitiveTypeEntry)) {
        return hiveDecimal.floatValue();
      }
      return hiveDecimal;
    }

    String constTypeInfoName = sourceType.getTypeName();
    if (constTypeInfoName.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME)) {
      // because a comparison against a "string" will happen in "string" type.
      // to avoid unintentional comparisons in "string"
      // constants which are representing char/varchar values must be converted to the
      // appropriate type.
      if (targetType instanceof CharTypeInfo) {
        final String constValue = constantValue.toString();
        final int length = TypeInfoUtils.getCharacterLengthForType(targetType);
        HiveChar newValue = new HiveChar(constValue, length);
        HiveChar maxCharConst = new HiveChar(constValue, HiveChar.MAX_CHAR_LENGTH);
        if (maxCharConst.equals(newValue)) {
          return newValue;
        } else {
          return null;
        }
      }
      if (targetType instanceof VarcharTypeInfo) {
        final String constValue = constantValue.toString();
        final int length = TypeInfoUtils.getCharacterLengthForType(targetType);
        HiveVarchar newValue = new HiveVarchar(constValue, length);
        HiveVarchar maxCharConst = new HiveVarchar(constValue, HiveVarchar.MAX_VARCHAR_LENGTH);
        if (maxCharConst.equals(newValue)) {
          return newValue;
        } else {
          return null;
        }
      }
    }

    return constantValue;
  }

  private BigDecimal toBigDecimal(String val) {
    if (!NumberUtils.isNumber(val)) {
      throw new NumberFormatException("The given string is not a valid number: " + val);
    }
    return new BigDecimal(val.replaceAll("[dDfFlL]$", ""));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createStringConstantExpr(String value) {
    return new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createDateConstantExpr(String value) {
    Date d = Date.valueOf(value);
    return new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo, d);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createTimestampConstantExpr(String value) {
    Timestamp t = Timestamp.valueOf(value);
    return new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, t);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createTimestampLocalTimeZoneConstantExpr(String value, ZoneId zoneId) {
    TimestampTZ t = TimestampTZUtil.parse(value);
    return new ExprNodeConstantDesc(TypeInfoFactory.getTimestampTZTypeInfo(zoneId), t);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createIntervalYearMonthConstantExpr(String value) {
    return new ExprNodeConstantDesc(TypeInfoFactory.intervalYearMonthTypeInfo,
        HiveIntervalYearMonth.valueOf(value));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createIntervalDayTimeConstantExpr(String value) {
    return new ExprNodeConstantDesc(TypeInfoFactory.intervalDayTimeTypeInfo,
        HiveIntervalDayTime.valueOf(value));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createIntervalYearConstantExpr(String value) {
    return new ExprNodeConstantDesc(TypeInfoFactory.intervalYearMonthTypeInfo,
        new HiveIntervalYearMonth(Integer.parseInt(value), 0));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createIntervalMonthConstantExpr(String value) {
    return new ExprNodeConstantDesc(TypeInfoFactory.intervalYearMonthTypeInfo,
        new HiveIntervalYearMonth(0, Integer.parseInt(value)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createIntervalDayConstantExpr(String value) {
    return new ExprNodeConstantDesc(TypeInfoFactory.intervalDayTimeTypeInfo,
        new HiveIntervalDayTime(Integer.parseInt(value), 0, 0, 0, 0));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createIntervalHourConstantExpr(String value) {
    return new ExprNodeConstantDesc(TypeInfoFactory.intervalDayTimeTypeInfo,
        new HiveIntervalDayTime(0, Integer.parseInt(value), 0, 0, 0));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createIntervalMinuteConstantExpr(String value) {
    return new ExprNodeConstantDesc(TypeInfoFactory.intervalDayTimeTypeInfo,
        new HiveIntervalDayTime(0, 0, Integer.parseInt(value), 0, 0));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createIntervalSecondConstantExpr(String value) {
    BigDecimal bd = new BigDecimal(value);
    BigDecimal bdSeconds = new BigDecimal(bd.toBigInteger());
    BigDecimal bdNanos = bd.subtract(bdSeconds);
    return new ExprNodeConstantDesc(TypeInfoFactory.intervalDayTimeTypeInfo,
        new HiveIntervalDayTime(0, 0, 0, bdSeconds.intValueExact(),
            bdNanos.multiply(NANOS_PER_SEC_BD).intValue()));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeDesc createStructExpr(TypeInfo typeInfo, List<ExprNodeDesc> operands)
      throws SemanticException {
    assert typeInfo instanceof StructTypeInfo;
    if (isAllConstants(operands)) {
      return createConstantExpr(typeInfo,
          operands.stream()
              .map(this::getConstantValue)
              .collect(Collectors.toList()));
    }
    return ExprNodeGenericFuncDesc.newInstance(
        new GenericUDFStruct(),
        GenericUDFStruct.class.getAnnotation(Description.class).name(),
        operands);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeConstantDesc createConstantExpr(TypeInfo typeInfo, Object constantValue) {
    return new ExprNodeConstantDesc(typeInfo, constantValue);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeFieldDesc createNestedColumnRefExpr(
      TypeInfo typeInfo, ExprNodeDesc expr, String fieldName, Boolean isList) {
    return new ExprNodeFieldDesc(typeInfo, expr, fieldName, isList);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeGenericFuncDesc createFuncCallExpr(TypeInfo typeInfo, FunctionInfo fi,
      String funcText, List<ExprNodeDesc> inputs) throws UDFArgumentException {
    GenericUDF genericUDF = fi.getGenericUDF();
    if (genericUDF instanceof SettableUDF) {
      ((SettableUDF) genericUDF).setTypeInfo(typeInfo);
    }

    return ExprNodeGenericFuncDesc.newInstance(genericUDF, funcText, inputs);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeColumnListDesc createExprsListExpr() {
    return new ExprNodeColumnListDesc();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void addExprToExprsList(ExprNodeDesc columnList, ExprNodeDesc expr) {
    ExprNodeColumnListDesc l = (ExprNodeColumnListDesc) columnList;
    l.addColumn(expr);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isConstantExpr(Object o) {
    return o instanceof ExprNodeConstantDesc;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isFuncCallExpr(Object o) {
    return o instanceof ExprNodeGenericFuncDesc;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Object getConstantValue(ExprNodeDesc expr) {
    return ((ExprNodeConstantDesc) expr).getValue();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getConstantValueAsString(ExprNodeDesc expr) {
    return ((ExprNodeConstantDesc) expr).getValue().toString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isColumnRefExpr(Object o) {
    return o instanceof ExprNodeColumnDesc;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getColumnName(ExprNodeDesc expr, RowResolver rowResolver) {
    return ((ExprNodeColumnDesc) expr).getColumn();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isExprsListExpr(Object o) {
    return o instanceof ExprNodeColumnListDesc;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected List<ExprNodeDesc> getExprChildren(ExprNodeDesc expr) {
    return expr.getChildren();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected TypeInfo getTypeInfo(ExprNodeDesc expr) {
    return expr.getTypeInfo();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected List<TypeInfo> getStructTypeInfoList(ExprNodeDesc expr) {
    StructTypeInfo structTypeInfo = (StructTypeInfo) expr.getTypeInfo();
    return structTypeInfo.getAllStructFieldTypeInfos();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected List<String> getStructNameList(ExprNodeDesc expr) {
    StructTypeInfo structTypeInfo = (StructTypeInfo) expr.getTypeInfo();
    return structTypeInfo.getAllStructFieldNames();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isORFuncCallExpr(ExprNodeDesc expr) {
    return FunctionRegistry.isOpOr(expr);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isANDFuncCallExpr(ExprNodeDesc expr) {
    return FunctionRegistry.isOpAnd(expr);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isPOSITIVEFuncCallExpr(ExprNodeDesc expr) {
    return FunctionRegistry.isOpPositive(expr);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isNEGATIVEFuncCallExpr(ExprNodeDesc expr) {
    return FunctionRegistry.isOpNegative(expr);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isAndFunction(FunctionInfo fi) {
    return fi.getGenericUDF() instanceof GenericUDFOPAnd;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isOrFunction(FunctionInfo fi) {
    return fi.getGenericUDF() instanceof GenericUDFOPOr;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isInFunction(FunctionInfo fi) {
    return fi.getGenericUDF() instanceof GenericUDFIn;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isCompareFunction(FunctionInfo fi) {
    return fi.getGenericUDF() instanceof GenericUDFBaseCompare;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isEqualFunction(FunctionInfo fi) {
    return fi.getGenericUDF() instanceof GenericUDFOPEqual
        && !(fi.getGenericUDF() instanceof GenericUDFOPEqualNS);
  }

  @Override
  protected boolean isNSCompareFunction(FunctionInfo fi) {
    return fi.getGenericUDF() instanceof GenericUDFOPEqualNS ||
        fi.getGenericUDF() instanceof GenericUDFOPNotEqualNS;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isConsistentWithinQuery(FunctionInfo fi) {
    return FunctionRegistry.isConsistentWithinQuery(fi.getGenericUDF());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isStateful(FunctionInfo fi) {
    return FunctionRegistry.isStateful(fi.getGenericUDF());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeDesc setTypeInfo(ExprNodeDesc expr, TypeInfo type) {
    expr.setTypeInfo(type);
    return expr;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean convertCASEIntoCOALESCEFuncCallExpr(FunctionInfo fi, List<ExprNodeDesc> inputs) {
    GenericUDF genericUDF = fi.getGenericUDF();
    if (genericUDF instanceof GenericUDFWhen && inputs.size() == 3 &&
        inputs.get(1) instanceof ExprNodeConstantDesc &&
        inputs.get(2) instanceof ExprNodeConstantDesc) {
      ExprNodeConstantDesc constThen = (ExprNodeConstantDesc) inputs.get(1);
      ExprNodeConstantDesc constElse = (ExprNodeConstantDesc) inputs.get(2);
      Object thenVal = constThen.getValue();
      Object elseVal = constElse.getValue();
      if (thenVal instanceof Boolean && elseVal instanceof Boolean) {
        //only convert to COALESCE when both branches are valid
        return !thenVal.equals(elseVal);
      }
    }
    return false;
  }

  @Override
  protected boolean convertCASEIntoIFFuncCallExpr(FunctionInfo fi, List<ExprNodeDesc> inputs) {
    GenericUDF genericUDF = fi.getGenericUDF();
    return genericUDF instanceof GenericUDFWhen && inputs.size() == 3
        && TypeInfoFactory.booleanTypeInfo.equals(inputs.get(0).getTypeInfo());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeDesc foldExpr(ExprNodeDesc expr) {
    if (expr instanceof ExprNodeGenericFuncDesc) {
      return ConstantPropagateProcFactory.foldExpr((ExprNodeGenericFuncDesc) expr);
    }
    return expr;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isSTRUCTFuncCallExpr(ExprNodeDesc expr) {
    return ExprNodeDescUtils.isStructUDF(expr);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isConstantStruct(ExprNodeDesc expr) {
    return ExprNodeDescUtils.isConstantStruct(expr);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ExprNodeDesc createSubqueryExpr(TypeCheckCtx ctx, ASTNode expr, SubqueryType subqueryType,
      Object[] inputs) throws CalciteSubquerySemanticException {
    // subqueryToRelNode might be null if subquery expression anywhere other than
    //  as expected in filter (where/having). We should throw an appropriate error
    // message
    Map<ASTNode, QBSubQueryParseInfo> subqueryToRelNode = ctx.getSubqueryToRelNode();
    if (subqueryToRelNode == null) {
      throw new CalciteSubquerySemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
          " Currently SubQuery expressions are only allowed as " +
              "Where and Having Clause predicates"));
    }

    ASTNode subqueryOp = (ASTNode) expr.getChild(0);
    RelNode subqueryRel = subqueryToRelNode.get(expr).getSubQueryRelNode();
    // For now because subquery is only supported in filter
    // we will create subquery expression of boolean type
    switch (subqueryType) {
      case EXISTS: {
        if (subqueryToRelNode.get(expr).hasFullAggregate()) {
          return createConstantExpr(TypeInfoFactory.booleanTypeInfo, true);
        }
        return new ExprNodeSubQueryDesc(TypeInfoFactory.booleanTypeInfo, subqueryRel,
            SubqueryType.EXISTS);
      }
      case IN: {
        assert (inputs[2] != null);
        ExprNodeDesc lhs = (ExprNodeDesc) inputs[2];
        return new ExprNodeSubQueryDesc(TypeInfoFactory.booleanTypeInfo, subqueryRel,
            SubqueryType.IN, lhs);
      }
      case SCALAR: {
        // only single subquery expr is supported
        if (subqueryRel.getRowType().getFieldCount() != 1) {
          throw new CalciteSubquerySemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
              "More than one column expression in subquery"));
        }
        // figure out subquery expression column's type
        TypeInfo subExprType = TypeConverter.convert(subqueryRel.getRowType().getFieldList().get(0).getType());
        return new ExprNodeSubQueryDesc(subExprType, subqueryRel,
            SubqueryType.SCALAR);
      }
      case SOME: {
        assert (inputs[2] != null);
        ExprNodeDesc lhs = (ExprNodeDesc) inputs[2];
        return new ExprNodeSubQueryDesc(TypeInfoFactory.booleanTypeInfo, subqueryRel,
            SubqueryType.SOME, lhs, (ASTNode) subqueryOp.getChild(1));
      }
      case ALL: {
        assert (inputs[2] != null);
        ExprNodeDesc lhs = (ExprNodeDesc) inputs[2];
        return new ExprNodeSubQueryDesc(TypeInfoFactory.booleanTypeInfo, subqueryRel,
            SubqueryType.ALL, lhs, (ASTNode) subqueryOp.getChild(1));
      }
      default:
        return null;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected FunctionInfo getFunctionInfo(String funcName) throws SemanticException {
    return FunctionRegistry.getFunctionInfo(funcName);
  }

  @Override
  protected ExprNodeDesc replaceFieldNamesInStruct(ExprNodeDesc expr, List<String> newFieldNames) {
    if (newFieldNames.isEmpty()) {
      return expr;
    }

    ExprNodeGenericFuncDesc structCall = (ExprNodeGenericFuncDesc) expr;
    List<TypeInfo> newTypes = structCall.getChildren().stream().map(ExprNodeDesc::getTypeInfo).collect(Collectors.toList());
    TypeInfo newType = TypeInfoFactory.getStructTypeInfo(newFieldNames, newTypes);

    return new ExprNodeGenericFuncDesc(newType, structCall.getGenericUDF(), structCall.getChildren());
  }
}
