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
package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.DataSketchesFunctions;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException.UnsupportedFeature;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.CanAggregateDistinct;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlAverageAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlCountAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlMinMaxAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlSumAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlVarianceAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveBetween;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveConcat;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveDateAddSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveDateSubSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExtractDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFloorDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFromUnixTimeSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIn;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSqlFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveToDateSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTruncSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveToUnixTimestampSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnixTimestampSqlOperator;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNegative;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPositive;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hive.plugin.api.HiveUDFPlugin;
import org.apache.hive.plugin.api.HiveUDFPlugin.UDFDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class SqlFunctionConverter {
  private static final Logger LOG = LoggerFactory.getLogger(SqlFunctionConverter.class);

  static final Map<String, SqlOperator>    hiveToCalcite;
  static final Map<SqlOperator, HiveToken> calciteToHiveToken;
  static final Map<SqlOperator, String>    reverseOperatorMap;

  static {
    StaticBlockBuilder builder = new StaticBlockBuilder();
    hiveToCalcite = ImmutableMap.copyOf(builder.hiveToCalcite);
    calciteToHiveToken = ImmutableMap.copyOf(builder.calciteToHiveToken);
    reverseOperatorMap = ImmutableMap.copyOf(builder.reverseOperatorMap);
  }

  public static SqlOperator getCalciteOperator(String funcTextName, GenericUDF hiveUDF,
      ImmutableList<RelDataType> calciteArgTypes, RelDataType retType)
      throws SemanticException {
    // handle overloaded methods first
    if (hiveUDF instanceof GenericUDFOPNegative) {
      return SqlStdOperatorTable.UNARY_MINUS;
    } else if (hiveUDF instanceof GenericUDFOPPositive) {
      return SqlStdOperatorTable.UNARY_PLUS;
    } // do generic lookup
    String name = null;
    if (StringUtils.isEmpty(funcTextName)) {
      name = getName(hiveUDF); // this should probably never happen, see
      // getName
      // comment
      LOG.warn("The function text was empty, name from annotation is " + name);
    } else {
      // We could just do toLowerCase here and let SA qualify it, but
      // let's be proper...
      name = FunctionRegistry.getNormalizedFunctionName(funcTextName);
    }

    // For calcite, isDeterministic just matters for within the query.
    // runtimeConstant used to indicate the function is not deterministic between queries.
    boolean isDeterministic = FunctionRegistry.isConsistentWithinQuery(hiveUDF);
    boolean runtimeConstant = FunctionRegistry.isRuntimeConstant(hiveUDF);
    return getCalciteFn(name, calciteArgTypes, retType, isDeterministic, runtimeConstant);
  }

  public static SqlOperator getCalciteOperator(String funcTextName, GenericUDTF hiveUDTF,
      ImmutableList<RelDataType> calciteArgTypes, RelDataType retType) throws SemanticException {
    // We could just do toLowerCase here and let SA qualify it, but
    // let's be proper...
    String name = FunctionRegistry.getNormalizedFunctionName(funcTextName);
    return getCalciteFn(name, calciteArgTypes, retType, false, false);
  }

  public static GenericUDF getHiveUDF(SqlOperator op, RelDataType dt, int argsLength) {
    String name = reverseOperatorMap.get(op);
    if (name == null) {
      name = op.getName();
    }
    // Make sure we handle unary + and - correctly.
    if (argsLength == 1) {
      if ("+".equals(name)) {
        name = FunctionRegistry.UNARY_PLUS_FUNC_NAME;
      } else if ("-".equals(name)) {
        name = FunctionRegistry.UNARY_MINUS_FUNC_NAME;
      }
    }
    FunctionInfo hFn;
    try {
      hFn = handleExplicitCast(op, dt);
    } catch (SemanticException e) {
      LOG.warn("Failed to load udf " + name, e);
      hFn = null;
    }
    if (hFn == null) {
      try {
        hFn = name != null ? FunctionRegistry.getFunctionInfo(name) : null;
      } catch (SemanticException e) {
        LOG.warn("Failed to load udf " + name, e);
        hFn = null;
      }
    }
    return hFn == null ? null : hFn.getGenericUDF();
  }

  private static FunctionInfo handleExplicitCast(SqlOperator op, RelDataType dt)
      throws SemanticException {
    FunctionInfo castUDF = null;

    if (op.kind == SqlKind.CAST) {
      TypeInfo castType = TypeConverter.convert(dt);

      if (castType.equals(TypeInfoFactory.byteTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("tinyint");
      } else if (castType instanceof CharTypeInfo) {
        castUDF = handleCastForParameterizedType(castType, FunctionRegistry.getFunctionInfo("char"));
      } else if (castType instanceof VarcharTypeInfo) {
        castUDF = handleCastForParameterizedType(castType,
            FunctionRegistry.getFunctionInfo("varchar"));
      } else if (castType.equals(TypeInfoFactory.stringTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("string");
      } else if (castType.equals(TypeInfoFactory.booleanTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("boolean");
      } else if (castType.equals(TypeInfoFactory.shortTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("smallint");
      } else if (castType.equals(TypeInfoFactory.intTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("int");
      } else if (castType.equals(TypeInfoFactory.longTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("bigint");
      } else if (castType.equals(TypeInfoFactory.floatTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("float");
      } else if (castType.equals(TypeInfoFactory.doubleTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("double");
      } else if (castType.equals(TypeInfoFactory.timestampTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("timestamp");
      } else if (castType instanceof TimestampLocalTZTypeInfo) {
        castUDF = handleCastForParameterizedType(castType,
            FunctionRegistry.getFunctionInfo(serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME));
      } else if (castType.equals(TypeInfoFactory.dateTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("date");
      } else if (castType instanceof DecimalTypeInfo) {
        castUDF = handleCastForParameterizedType(castType,
            FunctionRegistry.getFunctionInfo("decimal"));
      } else if (castType.equals(TypeInfoFactory.binaryTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("binary");
      } else if (castType.equals(TypeInfoFactory.intervalDayTimeTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo(serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME);
      } else if (castType.equals(TypeInfoFactory.intervalYearMonthTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo(serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME);
      } else {
        throw new IllegalStateException("Unexpected type : " + castType.getQualifiedName());
      }
    }

    return castUDF;
  }

  private static FunctionInfo handleCastForParameterizedType(TypeInfo ti, FunctionInfo fi) {
    SettableUDF udf = (SettableUDF) fi.getGenericUDF();
    try {
      udf.setTypeInfo(ti);
    } catch (UDFArgumentException e) {
      throw new RuntimeException(e);
    }
    return new FunctionInfo(
        fi.getFunctionType(), fi.getDisplayName(), (GenericUDF) udf, fi.getResources());
  }

  // TODO: 1) handle Agg Func Name translation 2) is it correct to add func
  // args as child of func?
  public static ASTNode buildAST(SqlOperator op, List<ASTNode> children, RelDataType type) {
    HiveToken hToken = calciteToHiveToken.get(op);
    ASTNode node;
    if (hToken != null) {
      switch (op.kind) {
        case IN:
        case BETWEEN:
        case ROW:
        case ARRAY_VALUE_CONSTRUCTOR:
        case MAP_VALUE_CONSTRUCTOR:
        case IS_NOT_TRUE:
        case IS_TRUE:
        case IS_NOT_FALSE:
        case IS_FALSE:
        case IS_NOT_NULL:
        case IS_NULL:
        case CASE:
        case COALESCE:
        case EXTRACT:
        case FLOOR:
        case CEIL:
        case LIKE:
        case OTHER_FUNCTION:
          node = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
          node.addChild((ASTNode) ParseDriver.adaptor.create(hToken.type, hToken.text));
          break;
        default:
          node = (ASTNode) ParseDriver.adaptor.create(hToken.type, hToken.text);
      }
    } else {
      node = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
      if (op.kind != SqlKind.CAST) {
        if (op.kind == SqlKind.MINUS_PREFIX) {
          node = (ASTNode) ParseDriver.adaptor.create(HiveParser.MINUS, "MINUS");
        } else if (op.kind == SqlKind.PLUS_PREFIX) {
          node = (ASTNode) ParseDriver.adaptor.create(HiveParser.PLUS, "PLUS");
        } else {
          // Handle COUNT/SUM/AVG function for the case of COUNT(*) and COUNT(DISTINCT)
          if (op instanceof HiveSqlCountAggFunction ||
              op instanceof HiveSqlSumAggFunction ||
              op instanceof HiveSqlAverageAggFunction) {
            if (children.size() == 0) {
              node = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_FUNCTIONSTAR,
                "TOK_FUNCTIONSTAR");
            } else {
              CanAggregateDistinct distinctFunction = (CanAggregateDistinct) op;
              if (distinctFunction.isDistinct()) {
                node = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_FUNCTIONDI,
                    "TOK_FUNCTIONDI");
              }
            }
          }
          node.addChild((ASTNode) ParseDriver.adaptor.create(HiveParser.Identifier, op.getName()));
        }
      }
    }

    node.setTypeInfo(TypeConverter.convert(type));

    for (ASTNode c : children) {
      ParseDriver.adaptor.addChild(node, c);
    }
    return node;
  }

  /**
   * Build AST for flattened Associative expressions ('and', 'or'). Flattened
   * expressions is of the form or[x,y,z] which is originally represented as
   * "or[x, or[y, z]]".
   */
  public static ASTNode buildAST(SqlOperator op, List<ASTNode> children, int i) {
    if (i + 1 < children.size()) {
      HiveToken hToken = calciteToHiveToken.get(op);
      ASTNode curNode = ((ASTNode) ParseDriver.adaptor.create(hToken.type, hToken.text));
      ParseDriver.adaptor.addChild(curNode, children.get(i));
      ParseDriver.adaptor.addChild(curNode, buildAST(op, children, i + 1));
      return curNode;
    } else {
      return children.get(i);
    }

  }

  // TODO: this is not valid. Function names for built-in UDFs are specified in
  // FunctionRegistry, and only happen to match annotations. For user UDFs, the
  // name is what user specifies at creation time (annotation can be absent,
  // different, or duplicate some other function).
  private static String getName(GenericUDF hiveUDF) {
    String udfName = null;
    if (hiveUDF instanceof GenericUDFBridge) {
      udfName = ((GenericUDFBridge) hiveUDF).getUdfName();
    } else {
      Class<? extends GenericUDF> udfClass = hiveUDF.getClass();
      Annotation udfAnnotation = udfClass.getAnnotation(Description.class);

      if (udfAnnotation != null && udfAnnotation instanceof Description) {
        Description udfDescription = (Description) udfAnnotation;
        udfName = udfDescription.name();
        if (udfName != null) {
          String[] aliases = udfName.split(",");
          if (aliases.length > 0) {
            udfName = aliases[0];
          }
        }
      }

      if (udfName == null || udfName.isEmpty()) {
        udfName = hiveUDF.getClass().getName();
        int indx = udfName.lastIndexOf(".");
        if (indx >= 0) {
          indx += 1;
          udfName = udfName.substring(indx);
        }
      }
    }

    return udfName;
  }

  /**
   * This class is used to build immutable hashmaps in the static block above.
   */
  private static class StaticBlockBuilder {
    final Map<String, SqlOperator>    hiveToCalcite      = Maps.newHashMap();
    final Map<SqlOperator, HiveToken> calciteToHiveToken = Maps.newHashMap();
    final Map<SqlOperator, String>    reverseOperatorMap = Maps.newHashMap();

    StaticBlockBuilder() {
      registerFunction("+", SqlStdOperatorTable.PLUS, hToken(HiveParser.PLUS, "+"));
      registerFunction("-", SqlStdOperatorTable.MINUS, hToken(HiveParser.MINUS, "-"));
      registerFunction("*", SqlStdOperatorTable.MULTIPLY, hToken(HiveParser.STAR, "*"));
      registerFunction("/", SqlStdOperatorTable.DIVIDE, hToken(HiveParser.DIVIDE, "/"));
      registerFunction("%", SqlStdOperatorTable.MOD, hToken(HiveParser.MOD, "%"));
      registerFunction("and", SqlStdOperatorTable.AND, hToken(HiveParser.KW_AND, "and"));
      registerFunction("or", SqlStdOperatorTable.OR, hToken(HiveParser.KW_OR, "or"));
      registerFunction("=", SqlStdOperatorTable.EQUALS, hToken(HiveParser.EQUAL, "="));
      registerDuplicateFunction("==", SqlStdOperatorTable.EQUALS, hToken(HiveParser.EQUAL, "="));
      registerFunction("<", SqlStdOperatorTable.LESS_THAN, hToken(HiveParser.LESSTHAN, "<"));
      registerFunction("<=", SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
          hToken(HiveParser.LESSTHANOREQUALTO, "<="));
      registerFunction(">", SqlStdOperatorTable.GREATER_THAN, hToken(HiveParser.GREATERTHAN, ">"));
      registerFunction(">=", SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
          hToken(HiveParser.GREATERTHANOREQUALTO, ">="));
      registerFunction("not", SqlStdOperatorTable.NOT, hToken(HiveParser.KW_NOT, "not"));
      registerDuplicateFunction("!", SqlStdOperatorTable.NOT, hToken(HiveParser.KW_NOT, "not"));
      registerFunction("<>", SqlStdOperatorTable.NOT_EQUALS, hToken(HiveParser.NOTEQUAL, "<>"));
      registerDuplicateFunction("!=", SqlStdOperatorTable.NOT_EQUALS, hToken(HiveParser.NOTEQUAL, "<>"));
      registerFunction("in", HiveIn.INSTANCE, hToken(HiveParser.Identifier, "in"));
      registerFunction("between", HiveBetween.INSTANCE, hToken(HiveParser.Identifier, "between"));
      registerFunction("struct", SqlStdOperatorTable.ROW, hToken(HiveParser.Identifier, "struct"));
      registerFunction(FunctionRegistry.ARRAY_FUNC_NAME,
          SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, hToken(HiveParser.Identifier, FunctionRegistry.ARRAY_FUNC_NAME));
      registerFunction("map", SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, hToken(HiveParser.Identifier, "map"));
      registerFunction("isnotnull", SqlStdOperatorTable.IS_NOT_NULL, hToken(HiveParser.Identifier, "isnotnull"));
      registerFunction("isnull", SqlStdOperatorTable.IS_NULL, hToken(HiveParser.Identifier, "isnull"));
      registerFunction("isnottrue", SqlStdOperatorTable.IS_NOT_TRUE, hToken(HiveParser.Identifier, "isnottrue"));
      registerFunction("istrue", SqlStdOperatorTable.IS_TRUE, hToken(HiveParser.Identifier, "istrue"));
      registerFunction("isnotfalse", SqlStdOperatorTable.IS_NOT_FALSE, hToken(HiveParser.Identifier, "isnotfalse"));
      registerFunction("isfalse", SqlStdOperatorTable.IS_FALSE, hToken(HiveParser.Identifier, "isfalse"));
      registerFunction("is_not_distinct_from", SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, hToken(HiveParser.EQUAL_NS, "<=>"));
      registerDuplicateFunction("<=>", SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, hToken(HiveParser.EQUAL_NS, "<=>"));
      registerFunction("when", SqlStdOperatorTable.CASE, hToken(HiveParser.Identifier, "when"));
      registerDuplicateFunction("case", SqlStdOperatorTable.CASE, hToken(HiveParser.Identifier, "when"));
      registerDuplicateFunction("if", SqlStdOperatorTable.CASE, hToken(HiveParser.Identifier, "when"));
      registerFunction("coalesce", SqlStdOperatorTable.COALESCE, hToken(HiveParser.Identifier, "coalesce"));

      // timebased
      registerFunction("year", HiveExtractDate.YEAR,
          hToken(HiveParser.Identifier, "year"));
      registerFunction("quarter", HiveExtractDate.QUARTER,
          hToken(HiveParser.Identifier, "quarter"));
      registerFunction("month", HiveExtractDate.MONTH,
          hToken(HiveParser.Identifier, "month"));
      registerFunction("weekofyear", HiveExtractDate.WEEK,
          hToken(HiveParser.Identifier, "weekofyear"));
      registerFunction("day", HiveExtractDate.DAY,
          hToken(HiveParser.Identifier, "day"));
      registerFunction("hour", HiveExtractDate.HOUR,
          hToken(HiveParser.Identifier, "hour"));
      registerFunction("minute", HiveExtractDate.MINUTE,
          hToken(HiveParser.Identifier, "minute"));
      registerFunction("second", HiveExtractDate.SECOND,
          hToken(HiveParser.Identifier, "second"));
      registerFunction("floor_year", HiveFloorDate.YEAR,
          hToken(HiveParser.Identifier, "floor_year"));
      registerFunction("floor_quarter", HiveFloorDate.QUARTER,
          hToken(HiveParser.Identifier, "floor_quarter"));
      registerFunction("floor_month", HiveFloorDate.MONTH,
          hToken(HiveParser.Identifier, "floor_month"));
      registerFunction("floor_week", HiveFloorDate.WEEK,
          hToken(HiveParser.Identifier, "floor_week"));
      registerFunction("floor_day", HiveFloorDate.DAY,
          hToken(HiveParser.Identifier, "floor_day"));
      registerFunction("floor_hour", HiveFloorDate.HOUR,
          hToken(HiveParser.Identifier, "floor_hour"));
      registerFunction("floor_minute", HiveFloorDate.MINUTE,
          hToken(HiveParser.Identifier, "floor_minute"));
      registerFunction("floor_second", HiveFloorDate.SECOND,
          hToken(HiveParser.Identifier, "floor_second"));
      registerFunction("power", SqlStdOperatorTable.POWER, hToken(HiveParser.Identifier, "power"));
      registerDuplicateFunction("pow", SqlStdOperatorTable.POWER,
          hToken(HiveParser.Identifier, "power")
      );
      registerFunction("ceil", SqlStdOperatorTable.CEIL, hToken(HiveParser.Identifier, "ceil"));
      registerDuplicateFunction("ceiling", SqlStdOperatorTable.CEIL,
          hToken(HiveParser.Identifier, "ceil")
      );
      registerFunction("floor", SqlStdOperatorTable.FLOOR, hToken(HiveParser.Identifier, "floor"));
      registerFunction("log10", SqlStdOperatorTable.LOG10, hToken(HiveParser.Identifier, "log10"));
      registerFunction("ln", SqlStdOperatorTable.LN, hToken(HiveParser.Identifier, "ln"));
      registerFunction("cos", SqlStdOperatorTable.COS, hToken(HiveParser.Identifier, "cos"));
      registerFunction("sin", SqlStdOperatorTable.SIN, hToken(HiveParser.Identifier, "sin"));
      registerFunction("tan", SqlStdOperatorTable.TAN, hToken(HiveParser.Identifier, "tan"));
      registerFunction("concat", HiveConcat.INSTANCE,
          hToken(HiveParser.Identifier, "concat")
      );
      registerFunction("substring", SqlStdOperatorTable.SUBSTRING,
          hToken(HiveParser.Identifier, "substring")
      );
      registerFunction("like", SqlStdOperatorTable.LIKE, hToken(HiveParser.Identifier, "like"));
      registerFunction("exp", SqlStdOperatorTable.EXP, hToken(HiveParser.Identifier, "exp"));
      registerFunction("div", SqlStdOperatorTable.DIVIDE_INTEGER,
          hToken(HiveParser.DIV, "div")
      );
      registerFunction("sqrt", SqlStdOperatorTable.SQRT, hToken(HiveParser.Identifier, "sqrt"));
      registerFunction("lower", SqlStdOperatorTable.LOWER, hToken(HiveParser.Identifier, "lower"));
      registerFunction("upper", SqlStdOperatorTable.UPPER, hToken(HiveParser.Identifier, "upper"));
      registerFunction("abs", SqlStdOperatorTable.ABS, hToken(HiveParser.Identifier, "abs"));
      registerFunction("character_length", SqlStdOperatorTable.CHAR_LENGTH,
          hToken(HiveParser.Identifier, "character_length")
      );
      registerDuplicateFunction("char_length", SqlStdOperatorTable.CHAR_LENGTH,
          hToken(HiveParser.Identifier, "character_length")
      );
      registerFunction("length", SqlStdOperatorTable.CHARACTER_LENGTH,
          hToken(HiveParser.Identifier, "length")
      );
      registerFunction("trunc", HiveTruncSqlOperator.INSTANCE, hToken(HiveParser.Identifier, "trunc"));
      registerFunction("to_date", HiveToDateSqlOperator.INSTANCE, hToken(HiveParser.Identifier, "to_date"));
      registerFunction("to_unix_timestamp", HiveToUnixTimestampSqlOperator.INSTANCE,
          hToken(HiveParser.Identifier, "to_unix_timestamp"));
      registerFunction("unix_timestamp", HiveUnixTimestampSqlOperator.INSTANCE,
          hToken(HiveParser.Identifier, "unix_timestamp"));
      registerFunction("from_unixtime", HiveFromUnixTimeSqlOperator.INSTANCE,
          hToken(HiveParser.Identifier, "from_unixtime"));
      registerFunction("date_add", HiveDateAddSqlOperator.INSTANCE, hToken(HiveParser.Identifier, "date_add"));
      registerFunction("date_sub", HiveDateSubSqlOperator.INSTANCE, hToken(HiveParser.Identifier, "date_sub"));

      registerPlugin(DataSketchesFunctions.INSTANCE);
    }

    private void registerPlugin(HiveUDFPlugin plugin) {
      for (UDFDescriptor udfDesc : plugin.getDescriptors()) {
        Optional<SqlFunction> calciteFunction = udfDesc.getCalciteFunction();
        if (calciteFunction.isPresent()) {
          registerDuplicateFunction(udfDesc.getFunctionName(), calciteFunction.get(),
              hToken(HiveParser.Identifier, udfDesc.getFunctionName()));
        }
      }

    }

    private void registerFunction(String name, SqlOperator calciteFn, HiveToken hiveToken) {
      reverseOperatorMap.put(calciteFn, name);
      FunctionInfo hFn;
      try {
        hFn = FunctionRegistry.getFunctionInfo(name);
      } catch (SemanticException e) {
        LOG.warn("Failed to load udf " + name, e);
        hFn = null;
      }
      if (hFn != null) {
        String hFnName = getName(hFn.getGenericUDF());
        hiveToCalcite.put(hFnName, calciteFn);

        if (hiveToken != null) {
          calciteToHiveToken.put(calciteFn, hiveToken);
        }
      }
    }

    private void registerDuplicateFunction(String name, SqlOperator calciteFn, HiveToken hiveToken) {
      hiveToCalcite.put(name, calciteFn);
      if (hiveToken != null) {
        calciteToHiveToken.put(calciteFn, hiveToken);
      }
    }
  }

  private static HiveToken hToken(int type, String text) {
    return new HiveToken(type, text);
  }

  // UDAF is assumed to be deterministic
  public static class CalciteUDAF extends SqlAggFunction implements CanAggregateDistinct {
    private final boolean isDistinct;
    public CalciteUDAF(boolean isDistinct, String opName, SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker) {
      super(opName, SqlKind.OTHER_FUNCTION, returnTypeInference, operandTypeInference,
          operandTypeChecker, SqlFunctionCategory.USER_DEFINED_FUNCTION);
      this.isDistinct = isDistinct;
    }

    @Override
    public boolean isDistinct() {
      return isDistinct;
    }
  }

  private static class CalciteUDFInfo {
    private String                     udfName;
    private SqlReturnTypeInference     returnTypeInference;
    private SqlOperandTypeInference    operandTypeInference;
    private SqlOperandTypeChecker      operandTypeChecker;
  }

  private static CalciteUDFInfo getUDFInfo(String hiveUdfName,
      List<RelDataType> calciteArgTypes, RelDataType calciteRetType) {
    CalciteUDFInfo udfInfo = new CalciteUDFInfo();
    udfInfo.udfName = hiveUdfName;
    udfInfo.returnTypeInference = ReturnTypes.explicit(calciteRetType);
    udfInfo.operandTypeInference = InferTypes.explicit(calciteArgTypes);
    ImmutableList.Builder<SqlTypeFamily> typeFamilyBuilder = new ImmutableList.Builder<SqlTypeFamily>();
    for (RelDataType at : calciteArgTypes) {
      typeFamilyBuilder.add(Util.first(at.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
    }
    udfInfo.operandTypeChecker = OperandTypes.family(typeFamilyBuilder.build());
    return udfInfo;
  }

  public static SqlOperator getCalciteFn(String hiveUdfName,
      List<RelDataType> calciteArgTypes, RelDataType calciteRetType,
      boolean deterministic, boolean runtimeConstant)
      throws CalciteSemanticException {

    SqlOperator calciteOp;
    CalciteUDFInfo uInf = getUDFInfo(hiveUdfName, calciteArgTypes, calciteRetType);
    switch (hiveUdfName) {
      // Follow hive's rules for type inference as oppose to Calcite's
      // for return type.
      //TODO: Perhaps we should do this for all functions, not just +,-
      case "-":
        calciteOp = new SqlMonotonicBinaryOperator("-", SqlKind.MINUS, 40, true,
            uInf.returnTypeInference, uInf.operandTypeInference, OperandTypes.MINUS_OPERATOR);
        break;
      case "+":
        calciteOp = new SqlMonotonicBinaryOperator("+", SqlKind.PLUS, 40, true,
            uInf.returnTypeInference, uInf.operandTypeInference, OperandTypes.PLUS_OPERATOR);
        break;
      default:
        calciteOp = hiveToCalcite.get(hiveUdfName);
        if (null == calciteOp) {
          calciteOp = new HiveSqlFunction(uInf.udfName, SqlKind.OTHER_FUNCTION, uInf.returnTypeInference,
              uInf.operandTypeInference, uInf.operandTypeChecker,
              SqlFunctionCategory.USER_DEFINED_FUNCTION, deterministic, runtimeConstant);
        }
        break;
    }
    return calciteOp;
  }

  public static SqlAggFunction getCalciteAggFn(String hiveUdfName, boolean isDistinct,
      ImmutableList<RelDataType> calciteArgTypes, RelDataType calciteRetType) {
    SqlAggFunction calciteAggFn = (SqlAggFunction) hiveToCalcite.get(hiveUdfName);

    if (calciteAggFn == null) {
      CalciteUDFInfo udfInfo = getUDFInfo(hiveUdfName, calciteArgTypes, calciteRetType);

      switch (hiveUdfName.toLowerCase()) {
      case "sum":
        calciteAggFn = new HiveSqlSumAggFunction(
            isDistinct,
            udfInfo.returnTypeInference,
            udfInfo.operandTypeInference,
            udfInfo.operandTypeChecker);
        break;
      case "count":
        calciteAggFn = new HiveSqlCountAggFunction(
            isDistinct,
            udfInfo.returnTypeInference,
            udfInfo.operandTypeInference,
            udfInfo.operandTypeChecker);
        break;
      case "min":
        calciteAggFn = new HiveSqlMinMaxAggFunction(
            udfInfo.returnTypeInference,
            udfInfo.operandTypeInference,
            udfInfo.operandTypeChecker, true);
        break;
      case "max":
        calciteAggFn = new HiveSqlMinMaxAggFunction(
            udfInfo.returnTypeInference,
            udfInfo.operandTypeInference,
            udfInfo.operandTypeChecker, false);
        break;
      case "avg":
        calciteAggFn = new HiveSqlAverageAggFunction(
            isDistinct,
            udfInfo.returnTypeInference,
            udfInfo.operandTypeInference,
            udfInfo.operandTypeChecker);
        break;
      case "std":
      case "stddev":
      case "stddev_pop":
        calciteAggFn = new HiveSqlVarianceAggFunction(
            "stddev_pop",
            SqlKind.STDDEV_POP,
            udfInfo.returnTypeInference,
            udfInfo.operandTypeInference,
            udfInfo.operandTypeChecker);
        break;
      case "stddev_samp":
        calciteAggFn = new HiveSqlVarianceAggFunction(
            "stddev_samp",
            SqlKind.STDDEV_SAMP,
            udfInfo.returnTypeInference,
            udfInfo.operandTypeInference,
            udfInfo.operandTypeChecker);
        break;
      case "variance":
      case "var_pop":
        calciteAggFn = new HiveSqlVarianceAggFunction(
            "var_pop",
            SqlKind.VAR_POP,
            udfInfo.returnTypeInference,
            udfInfo.operandTypeInference,
            udfInfo.operandTypeChecker);
        break;
      case "var_samp":
        calciteAggFn = new HiveSqlVarianceAggFunction(
            "var_samp",
            SqlKind.VAR_SAMP,
            udfInfo.returnTypeInference,
            udfInfo.operandTypeInference,
            udfInfo.operandTypeChecker);
        break;
      default:
        calciteAggFn = new CalciteUDAF(
            isDistinct,
            udfInfo.udfName,
            udfInfo.returnTypeInference,
            udfInfo.operandTypeInference,
            udfInfo.operandTypeChecker);
        break;
      }
    }
    return calciteAggFn;
  }

  static class HiveToken {
    int      type;
    String   text;
    String[] args;

    HiveToken(int type, String text, String... args) {
      this.type = type;
      this.text = text;
      this.args = args;
    }
  }
}
