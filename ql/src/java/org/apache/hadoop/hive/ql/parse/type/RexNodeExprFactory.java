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

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimestampString;
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
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException.UnsupportedFeature;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSubquerySemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRexExprList;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.QBSubQueryParseInfo;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.SubqueryType;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Expression factory for Calcite {@link RexNode}.
 */
public class RexNodeExprFactory extends ExprFactory<RexNode> {

  private static final Logger LOG = LoggerFactory.getLogger(RexNodeExprFactory.class);

  private final RexBuilder rexBuilder;
  private final FunctionHelper functionHelper;

  public RexNodeExprFactory(RexBuilder rexBuilder) {
    this.rexBuilder = rexBuilder;
    this.functionHelper = new HiveFunctionHelper(rexBuilder);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isExprInstance(Object o) {
    return o instanceof RexNode;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode toExpr(ColumnInfo colInfo, RowResolver rowResolver, int offset)
      throws CalciteSemanticException {
    ObjectInspector inspector = colInfo.getObjectInspector();
    if (inspector instanceof ConstantObjectInspector && inspector instanceof PrimitiveObjectInspector) {
      return toPrimitiveConstDesc(colInfo, inspector, rexBuilder);
    }
    int index = rowResolver.getPosition(colInfo.getInternalName());
    if (index < 0) {
      throw new CalciteSemanticException("Unexpected error: Cannot find column");
    }
    return rexBuilder.makeInputRef(
        TypeConverter.convert(colInfo.getType(), colInfo.isNullable(), rexBuilder.getTypeFactory()), index + offset);
  }

  private static RexNode toPrimitiveConstDesc(
      ColumnInfo colInfo, ObjectInspector inspector, RexBuilder rexBuilder)
      throws CalciteSemanticException {
    Object constant = ((ConstantObjectInspector) inspector).getWritableConstantValue();
    return rexBuilder.makeLiteral(constant,
        TypeConverter.convert(colInfo.getType(), rexBuilder.getTypeFactory()),
        false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createColumnRefExpr(ColumnInfo colInfo, RowResolver rowResolver, int offset)
      throws CalciteSemanticException {
    int index = rowResolver.getPosition(colInfo.getInternalName());
    return rexBuilder.makeInputRef(
        TypeConverter.convert(colInfo.getType(), rexBuilder.getTypeFactory()), index + offset);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createColumnRefExpr(ColumnInfo colInfo, List<RowResolver> rowResolverList)
      throws SemanticException {
    int index = getPosition(colInfo, rowResolverList);
    return rexBuilder.makeInputRef(
        TypeConverter.convert(colInfo.getType(), rexBuilder.getTypeFactory()), index);
  }

  private int getPosition(ColumnInfo colInfo, List<RowResolver> rowResolverList)
      throws SemanticException {
    ColumnInfo tmp;
    ColumnInfo cInfoToRet = null;
    int position = 0;
    for (RowResolver rr : rowResolverList) {
      tmp = rr.get(colInfo.getTabAlias(), colInfo.getAlias());
      if (tmp != null) {
        if (cInfoToRet != null) {
          throw new CalciteSemanticException("Could not resolve column name");
        }
        cInfoToRet = tmp;
        position += rr.getPosition(cInfoToRet.getInternalName());
      } else if (cInfoToRet == null) {
        position += rr.getColumnInfos().size();
      }
    }
    return position;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createNullConstantExpr() {
    return rexBuilder.makeNullLiteral(
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.NULL));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createDynamicParamExpr(int index) {
    return rexBuilder.makeDynamicParam(
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.NULL), index);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createBooleanConstantExpr(String value) {
    Boolean b = value != null ? Boolean.valueOf(value) : null;
    return rexBuilder.makeLiteral(b,
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN),
        false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createBigintConstantExpr(String value) {
    return rexBuilder.makeLiteral(
        new BigDecimal(Long.valueOf(value)),
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
        false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createIntConstantExpr(String value) {
    return rexBuilder.makeLiteral(
        new BigDecimal(Integer.valueOf(value)),
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER),
        false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createSmallintConstantExpr(String value) {
    return rexBuilder.makeLiteral(
        new BigDecimal(Short.valueOf(value)),
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.SMALLINT),
        false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createTinyintConstantExpr(String value) {
    return rexBuilder.makeLiteral(
        new BigDecimal(Byte.valueOf(value)),
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TINYINT),
        false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createFloatConstantExpr(String value) {
    Float f = Float.valueOf(value);
    return rexBuilder.makeApproxLiteral(
        new BigDecimal(Float.toString(f)),
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.FLOAT));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createDoubleConstantExpr(String value) throws SemanticException {
    Double d = Double.valueOf(value);
    // TODO: The best solution is to support NaN in expression reduction.
    if (Double.isNaN(d)) {
      throw new CalciteSemanticException("NaN", UnsupportedFeature.Invalid_decimal);
    }
    return rexBuilder.makeApproxLiteral(
        new BigDecimal(Double.toString(d)),
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createDecimalConstantExpr(String value, boolean allowNullValueConstantExpr) {
    HiveDecimal hd = HiveDecimal.create(value);
    if (!allowNullValueConstantExpr && hd == null) {
      return null;
    }
    BigDecimal bd = hd != null ? hd.bigDecimalValue() : null;
    DecimalTypeInfo type = adjustType(bd);
    return rexBuilder.makeExactLiteral(
        bd, TypeConverter.convert(type, rexBuilder.getTypeFactory()));
  }

  @Override
  protected TypeInfo adjustConstantType(PrimitiveTypeInfo targetType, Object constantValue) {
    if (PrimitiveObjectInspectorUtils.decimalTypeEntry.equals(targetType.getPrimitiveTypeEntry())) {
      return adjustType((BigDecimal) constantValue);
    }
    return targetType;
  }

  private DecimalTypeInfo adjustType(BigDecimal bd) {
    int prec = 1;
    int scale = 0;
    if (bd != null) {
      prec = bd.precision();
      scale = bd.scale();
      if (prec < scale) {
        // This can happen for numbers less than 0.1
        // For 0.001234: prec=4, scale=6
        // In this case, we'll set the type to have the same precision as the scale.
        prec = scale;
      }
    }
    return TypeInfoFactory.getDecimalTypeInfo(prec, scale);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Object interpretConstantAsPrimitive(PrimitiveTypeInfo targetType, Object constantValue,
      PrimitiveTypeInfo sourceType, boolean isEqual) {
    // Extract string value if necessary
    Object constantToInterpret = constantValue;
    if (constantValue instanceof NlsString) {
      constantToInterpret = ((NlsString) constantValue).getValue();
    }

    if (constantToInterpret instanceof Number || constantToInterpret instanceof String) {
      try {
        PrimitiveTypeEntry primitiveTypeEntry = targetType.getPrimitiveTypeEntry();
        if (PrimitiveObjectInspectorUtils.intTypeEntry.equals(primitiveTypeEntry)) {
          return toBigDecimal(constantToInterpret.toString()).intValueExact();
        } else if (PrimitiveObjectInspectorUtils.longTypeEntry.equals(primitiveTypeEntry)) {
          return toBigDecimal(constantToInterpret.toString()).longValueExact();
        } else if (PrimitiveObjectInspectorUtils.doubleTypeEntry.equals(primitiveTypeEntry)) {
          return toBigDecimal(constantToInterpret.toString());
        } else if (PrimitiveObjectInspectorUtils.floatTypeEntry.equals(primitiveTypeEntry)) {
          return toBigDecimal(constantToInterpret.toString());
        } else if (PrimitiveObjectInspectorUtils.byteTypeEntry.equals(primitiveTypeEntry)) {
          return toBigDecimal(constantToInterpret.toString()).byteValueExact();
        } else if (PrimitiveObjectInspectorUtils.shortTypeEntry.equals(primitiveTypeEntry)) {
          return toBigDecimal(constantToInterpret.toString()).shortValueExact();
        } else if (PrimitiveObjectInspectorUtils.decimalTypeEntry.equals(primitiveTypeEntry)) {
          HiveDecimal decimal = HiveDecimal.create(constantToInterpret.toString());
          return decimal != null ? decimal.bigDecimalValue() : null;
        }
      } catch (NumberFormatException | ArithmeticException nfe) {
        if (!isEqual && (constantToInterpret instanceof Number ||
            NumberUtils.isNumber(constantToInterpret.toString()))) {
          // The target is a number, if constantToInterpret can be interpreted as a number,
          // return the constantToInterpret directly, GenericUDFBaseCompare will do
          // type conversion for us.
          return constantToInterpret;
        }
        LOG.trace("Failed to narrow type of constant", nfe);
        return null;
      }
    }

    if (constantToInterpret instanceof BigDecimal) {
      return constantToInterpret;
    }

    String constTypeInfoName = sourceType.getTypeName();
    if (constTypeInfoName.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME)) {
      // because a comparison against a "string" will happen in "string" type.
      // to avoid unintentional comparisons in "string"
      // constants which are representing char/varchar values must be converted to the
      // appropriate type.
      if (targetType instanceof CharTypeInfo) {
        final String constValue = constantToInterpret.toString();
        final int length = TypeInfoUtils.getCharacterLengthForType(targetType);
        HiveChar newValue = new HiveChar(constValue, length);
        HiveChar maxCharConst = new HiveChar(constValue, HiveChar.MAX_CHAR_LENGTH);
        if (maxCharConst.equals(newValue)) {
          return makeHiveUnicodeString(newValue.getValue());
        } else {
          return null;
        }
      }
      if (targetType instanceof VarcharTypeInfo) {
        final String constValue = constantToInterpret.toString();
        final int length = TypeInfoUtils.getCharacterLengthForType(targetType);
        HiveVarchar newValue = new HiveVarchar(constValue, length);
        HiveVarchar maxCharConst = new HiveVarchar(constValue, HiveVarchar.MAX_VARCHAR_LENGTH);
        if (maxCharConst.equals(newValue)) {
          return makeHiveUnicodeString(newValue.getValue());
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
  protected RexLiteral createStringConstantExpr(String value) {
    RelDataType stringType = rexBuilder.getTypeFactory().createTypeWithCharsetAndCollation(
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE),
        Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
    // Note. Though we pass allowCast=true as parameter, this method will return a
    // VARCHAR literal without a CAST.
    return (RexLiteral) rexBuilder.makeLiteral(
        makeHiveUnicodeString(value), stringType, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexLiteral createDateConstantExpr(String value) {
    Date d = Date.valueOf(value);
    return rexBuilder.makeDateLiteral(
        DateString.fromDaysSinceEpoch(d.toEpochDay()));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexLiteral createTimestampConstantExpr(String value) {
    Timestamp t = Timestamp.valueOf(value);
    return (RexLiteral) rexBuilder.makeLiteral(
        TimestampString.fromMillisSinceEpoch(t.toEpochMilli()).withNanos(t.getNanos()),
        rexBuilder.getTypeFactory().createSqlType(
            SqlTypeName.TIMESTAMP,
            rexBuilder.getTypeFactory().getTypeSystem().getDefaultPrecision(SqlTypeName.TIMESTAMP)),
        false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexLiteral createTimestampLocalTimeZoneConstantExpr(String value, ZoneId zoneId) {
    TimestampTZ t = TimestampTZUtil.parse(value);

    final TimestampString tsLocalTZString;
    if (value == null) {
      tsLocalTZString = null;
    } else {
      Instant i = t.getZonedDateTime().toInstant();
      tsLocalTZString = TimestampString
          .fromMillisSinceEpoch(i.toEpochMilli())
          .withNanos(i.getNano());
    }
    return rexBuilder.makeTimestampWithLocalTimeZoneLiteral(
        tsLocalTZString,
        rexBuilder.getTypeFactory().getTypeSystem().getDefaultPrecision(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexLiteral createIntervalYearMonthConstantExpr(String value) {
    BigDecimal totalMonths = BigDecimal.valueOf(HiveIntervalYearMonth.valueOf(value).getTotalMonths());
    return rexBuilder.makeIntervalLiteral(totalMonths,
        new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, new SqlParserPos(1, 1)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexLiteral createIntervalDayTimeConstantExpr(String value) {
    HiveIntervalDayTime v = HiveIntervalDayTime.valueOf(value);
    BigDecimal secsValueBd = BigDecimal
        .valueOf(v.getTotalSeconds() * 1000);
    BigDecimal nanosValueBd = BigDecimal.valueOf((v).getNanos(), 6);
    return rexBuilder.makeIntervalLiteral(secsValueBd.add(nanosValueBd),
        new SqlIntervalQualifier(TimeUnit.MILLISECOND, null, new
            SqlParserPos(1, 1)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexLiteral createIntervalYearConstantExpr(String value) {
    HiveIntervalYearMonth v = new HiveIntervalYearMonth(Integer.parseInt(value), 0);
    BigDecimal totalMonths = BigDecimal.valueOf(v.getTotalMonths());
    return rexBuilder.makeIntervalLiteral(totalMonths,
        new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, new SqlParserPos(1, 1)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexLiteral createIntervalMonthConstantExpr(String value) {
    BigDecimal totalMonths = BigDecimal.valueOf(Integer.parseInt(value));
    return rexBuilder.makeIntervalLiteral(totalMonths,
        new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, new SqlParserPos(1, 1)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexLiteral createIntervalDayConstantExpr(String value) {
    HiveIntervalDayTime v = new HiveIntervalDayTime(Integer.parseInt(value), 0, 0, 0, 0);
    BigDecimal secsValueBd = BigDecimal
        .valueOf(v.getTotalSeconds() * 1000);
    BigDecimal nanosValueBd = BigDecimal.valueOf((v).getNanos(), 6);
    return rexBuilder.makeIntervalLiteral(secsValueBd.add(nanosValueBd),
        new SqlIntervalQualifier(TimeUnit.MILLISECOND, null, new
            SqlParserPos(1, 1)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexLiteral createIntervalHourConstantExpr(String value) {
    HiveIntervalDayTime v = new HiveIntervalDayTime(0, Integer.parseInt(value), 0, 0, 0);
    BigDecimal secsValueBd = BigDecimal
        .valueOf(v.getTotalSeconds() * 1000);
    BigDecimal nanosValueBd = BigDecimal.valueOf((v).getNanos(), 6);
    return rexBuilder.makeIntervalLiteral(secsValueBd.add(nanosValueBd),
        new SqlIntervalQualifier(TimeUnit.MILLISECOND, null, new
            SqlParserPos(1, 1)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexLiteral createIntervalMinuteConstantExpr(String value) {
    HiveIntervalDayTime v = new HiveIntervalDayTime(0, 0, Integer.parseInt(value), 0, 0);
    BigDecimal secsValueBd = BigDecimal
        .valueOf(v.getTotalSeconds() * 1000);
    BigDecimal nanosValueBd = BigDecimal.valueOf((v).getNanos(), 6);
    return rexBuilder.makeIntervalLiteral(secsValueBd.add(nanosValueBd),
        new SqlIntervalQualifier(TimeUnit.MILLISECOND, null, new
            SqlParserPos(1, 1)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexLiteral createIntervalSecondConstantExpr(String value) {
    BigDecimal bd = new BigDecimal(value);
    BigDecimal bdSeconds = new BigDecimal(bd.toBigInteger());
    BigDecimal bdNanos = bd.subtract(bdSeconds);
    HiveIntervalDayTime v = new HiveIntervalDayTime(0, 0, 0, bdSeconds.intValueExact(),
        bdNanos.multiply(NANOS_PER_SEC_BD).intValue());
    BigDecimal secsValueBd = BigDecimal
        .valueOf(v.getTotalSeconds() * 1000);
    BigDecimal nanosValueBd = BigDecimal.valueOf((v).getNanos(), 6);
    return rexBuilder.makeIntervalLiteral(secsValueBd.add(nanosValueBd),
        new SqlIntervalQualifier(TimeUnit.MILLISECOND, null, new
            SqlParserPos(1, 1)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createStructExpr(TypeInfo typeInfo, List<RexNode> operands)
      throws CalciteSemanticException {
    assert typeInfo instanceof StructTypeInfo;
    return rexBuilder.makeCall(
        TypeConverter.convert(typeInfo, rexBuilder.getTypeFactory()),
        SqlStdOperatorTable.ROW,
        operands);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createConstantExpr(TypeInfo typeInfo, Object constantValue)
      throws CalciteSemanticException {
    if (typeInfo instanceof StructTypeInfo) {
      List<TypeInfo> typeList = ((StructTypeInfo) typeInfo).getAllStructFieldTypeInfos();
      List<Object> objectList = (List<Object>) constantValue;
      List<RexNode> operands = new ArrayList<>();
      for (int i = 0; i < typeList.size(); i++) {
        operands.add(
            rexBuilder.makeLiteral(
                objectList.get(i),
                TypeConverter.convert(typeList.get(i), rexBuilder.getTypeFactory()),
                false));
      }
      return rexBuilder.makeCall(
          TypeConverter.convert(typeInfo, rexBuilder.getTypeFactory()),
          SqlStdOperatorTable.ROW,
          operands);
    }
    return rexBuilder.makeLiteral(constantValue,
        TypeConverter.convert(typeInfo, rexBuilder.getTypeFactory()), false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createNestedColumnRefExpr(
      TypeInfo typeInfo, RexNode expr, String fieldName, Boolean isList) throws CalciteSemanticException {
    if (expr.getType().isStruct()) {
      // regular case of accessing nested field in a column
      return rexBuilder.makeFieldAccess(expr, fieldName, true);
    } else {
      // This may happen for schema-less tables, where columns are dynamically
      // supplied by serdes.
      throw new CalciteSemanticException("Unexpected rexnode : "
          + expr.getClass().getCanonicalName(), UnsupportedFeature.Schema_less_table);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createFuncCallExpr(TypeInfo typeInfo, FunctionInfo functionInfo, String funcText,
      List<RexNode> inputs) throws SemanticException {
    // 2) Compute return type
    RelDataType returnType;
    if (typeInfo != null) {
      returnType = TypeConverter.convert(typeInfo, rexBuilder.getTypeFactory());
    } else {
      returnType = functionHelper.getReturnType(functionInfo, inputs);
    }
    // 3) Convert inputs (if necessary)
    List<RexNode> newInputs = functionHelper.convertInputs(
        functionInfo, inputs, returnType);
    // 4) Return Calcite function
    return functionHelper.getExpression(
        funcText, functionInfo, newInputs, returnType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createExprsListExpr() {
    return new HiveRexExprList();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void addExprToExprsList(RexNode columnList, RexNode expr) {
    HiveRexExprList l = (HiveRexExprList) columnList;
    l.addExpression(expr);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isConstantExpr(Object o) {
    return o instanceof RexLiteral;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isFuncCallExpr(Object o) {
    return o instanceof RexCall;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Object getConstantValue(RexNode expr) {
    if (expr.getType().getSqlTypeName() == SqlTypeName.ROW) {
      List<Object> res = new ArrayList<>();
      for (RexNode node : ((RexCall) expr).getOperands()) {
        res.add(((RexLiteral) node).getValue4());
      }
      return res;
    }
    return ((RexLiteral) expr).getValue4();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getConstantValueAsString(RexNode expr) {
    return ((RexLiteral) expr).getValueAs(String.class);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isColumnRefExpr(Object o) {
    return o instanceof RexNode && RexUtil.isReferenceOrAccess((RexNode) o, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getColumnName(RexNode expr, RowResolver rowResolver) {
    int index = ((RexInputRef) expr).getIndex();
    return rowResolver.getColumnInfos().get(index).getInternalName();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isExprsListExpr(Object o) {
    return o instanceof HiveRexExprList;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected List<RexNode> getExprChildren(RexNode expr) {
    if (expr instanceof RexCall) {
      return ((RexCall) expr).getOperands();
    } else if (expr instanceof HiveRexExprList) {
      return ((HiveRexExprList) expr).getExpressions();
    }
    return new ArrayList<>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected TypeInfo getTypeInfo(RexNode expr) {
    return expr.isA(SqlKind.LITERAL) ?
        TypeConverter.convertLiteralType((RexLiteral) expr) :
        TypeConverter.convert(expr.getType());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected List<TypeInfo> getStructTypeInfoList(RexNode expr) {
    StructTypeInfo structTypeInfo = (StructTypeInfo) TypeConverter.convert(expr.getType());
    return structTypeInfo.getAllStructFieldTypeInfos();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected List<String> getStructNameList(RexNode expr) {
    StructTypeInfo structTypeInfo = (StructTypeInfo) TypeConverter.convert(expr.getType());
    return structTypeInfo.getAllStructFieldNames();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isORFuncCallExpr(RexNode expr) {
    return expr.isA(SqlKind.OR);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isANDFuncCallExpr(RexNode expr) {
    return expr.isA(SqlKind.AND);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isConsistentWithinQuery(FunctionInfo fi) {
    return functionHelper.isConsistentWithinQuery(fi);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isStateful(FunctionInfo fi) {
    return functionHelper.isStateful(fi);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isPOSITIVEFuncCallExpr(RexNode expr) {
    return expr.isA(SqlKind.PLUS_PREFIX);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isNEGATIVEFuncCallExpr(RexNode expr) {
    return expr.isA(SqlKind.MINUS_PREFIX);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode setTypeInfo(RexNode expr, TypeInfo type) throws CalciteSemanticException {
    RelDataType t = TypeConverter.convert(type, rexBuilder.getTypeFactory());
    if (expr instanceof RexCall) {
      RexCall call = (RexCall) expr;
      return rexBuilder.makeCall(t,
          call.getOperator(), call.getOperands());
    } else if (expr instanceof RexInputRef) {
      RexInputRef inputRef = (RexInputRef) expr;
      return rexBuilder.makeInputRef(t, inputRef.getIndex());
    } else if (expr instanceof RexLiteral) {
      RexLiteral literal = (RexLiteral) expr;
      return rexBuilder.makeLiteral(RexLiteral.value(literal), t, false);
    }
    throw new RuntimeException("Unsupported expression type: " + expr.getClass().getCanonicalName());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean convertCASEIntoCOALESCEFuncCallExpr(FunctionInfo fi, List<RexNode> inputs) {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode foldExpr(RexNode expr) {
    return functionHelper.foldExpression(expr);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isSTRUCTFuncCallExpr(RexNode expr) {
    return expr instanceof RexCall &&
        ((RexCall) expr).getOperator() == SqlStdOperatorTable.ROW;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isAndFunction(FunctionInfo fi) {
    return functionHelper.isAndFunction(fi);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isOrFunction(FunctionInfo fi) {
    return functionHelper.isOrFunction(fi);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isInFunction(FunctionInfo fi) {
    return functionHelper.isInFunction(fi);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isCompareFunction(FunctionInfo fi) {
    return functionHelper.isCompareFunction(fi);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isEqualFunction(FunctionInfo fi) {
    return functionHelper.isEqualFunction(fi);
  }

  @Override
  protected boolean isNSCompareFunction(FunctionInfo fi) {
    return functionHelper.isNSCompareFunction(fi);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isConstantStruct(RexNode expr) {
    return expr.getType().getSqlTypeName() == SqlTypeName.ROW &&
        HiveCalciteUtil.isLiteral(expr);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RexNode createSubqueryExpr(TypeCheckCtx ctx, ASTNode expr, SubqueryType subqueryType,
      Object[] inputs) throws SemanticException {
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
        return RexSubQuery.exists(subqueryRel);
      }
      case IN: {
        assert (inputs[2] != null);
        /*
         * Check.5.h :: For In and Not In the SubQuery must implicitly or
         * explicitly only contain one select item.
         */
        if(subqueryRel.getRowType().getFieldCount() > 1) {
          throw new CalciteSubquerySemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
              "SubQuery can contain only 1 item in Select List."));
        }
        //create RexNode for LHS
        RexNode lhs = (RexNode) inputs[2];
        //create RexSubQuery node
        return RexSubQuery.in(subqueryRel, ImmutableList.<RexNode>of(lhs));
      }
      case SCALAR: {
        // only single subquery expr is supported
        if (subqueryRel.getRowType().getFieldCount() != 1) {
          throw new CalciteSubquerySemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
              "More than one column expression in subquery"));
        }
        if(subqueryRel.getRowType().getFieldCount() > 1) {
          throw new CalciteSubquerySemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
              "SubQuery can contain only 1 item in Select List."));
        }
        //create RexSubQuery node
        return RexSubQuery.scalar(subqueryRel);
      }
      case SOME:
      case ALL: {
        assert (inputs[2] != null);
        //create RexNode for LHS
        RexNode lhs = (RexNode) inputs[2];
        return convertSubquerySomeAll(subqueryRel.getCluster(),
            (ASTNode) subqueryOp.getChild(1), subqueryType, subqueryRel, lhs);
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
    return functionHelper.getFunctionInfo(funcName);
  }

  @Override
  protected RexNode replaceFieldNamesInStruct(RexNode expr, List<String> newFieldNames) {
    if (newFieldNames.isEmpty()) {
      return expr;
    }

    RexCall structCall = (RexCall) expr;
    List<RelDataType> newTypes = structCall.operands.stream().map(RexNode::getType).collect(Collectors.toList());
    RelDataType newType = rexBuilder.getTypeFactory().createStructType(newTypes, newFieldNames);
    return rexBuilder.makeCall(newType, structCall.op, structCall.operands);
  }

  private static void throwInvalidSubqueryError(final ASTNode comparisonOp) throws SemanticException {
    throw new CalciteSubquerySemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
        "Invalid operator:" + comparisonOp.toString()));
  }

  public static RexNode convertSubquerySomeAll(final RelOptCluster cluster, final ASTNode comparisonOp,
      final SubqueryType subqueryType, final RelNode subqueryRel, final RexNode rexNodeLhs)
      throws SemanticException {
    SqlQuantifyOperator quantifyOperator = null;
    switch (comparisonOp.getType()) {
      case HiveParser.EQUAL:
        if(subqueryType == SubqueryType.ALL) {
          throwInvalidSubqueryError(comparisonOp);
        }
        quantifyOperator = SqlStdOperatorTable.SOME_EQ;
        break;
      case HiveParser.LESSTHAN:
        quantifyOperator = SqlStdOperatorTable.SOME_LT;
        break;
      case HiveParser.LESSTHANOREQUALTO:
        quantifyOperator = SqlStdOperatorTable.SOME_LE;
        break;
      case HiveParser.GREATERTHAN:
        quantifyOperator = SqlStdOperatorTable.SOME_GT;
        break;
      case HiveParser.GREATERTHANOREQUALTO:
        quantifyOperator = SqlStdOperatorTable.SOME_GE;
        break;
      case HiveParser.NOTEQUAL:
        if(subqueryType == SubqueryType.SOME) {
          throwInvalidSubqueryError(comparisonOp);
        }
        quantifyOperator = SqlStdOperatorTable.SOME_NE;
        break;
      default:
        throw new CalciteSubquerySemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
            "Invalid operator:" + comparisonOp.toString()));
    }
    if(subqueryType == SubqueryType.ALL) {
      quantifyOperator = SqlStdOperatorTable.some(quantifyOperator.comparisonKind.negateNullSafe());
    }

    RexNode someQuery = getSomeSubquery(cluster, subqueryRel, rexNodeLhs, quantifyOperator);
    if(subqueryType == SubqueryType.ALL) {
      return cluster.getRexBuilder().makeCall(SqlStdOperatorTable.NOT, someQuery);
    }
    return someQuery;
  }

  private static RexNode getSomeSubquery(final RelOptCluster cluster,
      final RelNode subqueryRel, final RexNode lhs,
      final SqlQuantifyOperator quantifyOperator) {
    if(quantifyOperator == SqlStdOperatorTable.SOME_EQ) {
      return RexSubQuery.in(subqueryRel, ImmutableList.<RexNode>of(lhs));
    } else if (quantifyOperator == SqlStdOperatorTable.SOME_NE) {
      RexSubQuery subQuery = RexSubQuery.in(subqueryRel, ImmutableList.<RexNode>of(lhs));
      return cluster.getRexBuilder().makeCall(SqlStdOperatorTable.NOT, subQuery);
    } else {
      return RexSubQuery.some(subqueryRel, ImmutableList.of(lhs), quantifyOperator);
    }
  }

  public static NlsString makeHiveUnicodeString(String text) {
    return new NlsString(text, ConversionUtil.NATIVE_UTF16_CHARSET_NAME, SqlCollation.IMPLICIT);
  }
}
