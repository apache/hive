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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.externalize.RelEnumTypes;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDigestIncludeType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlAverageAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlCountAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlMinMaxAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlSumAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlSumEmptyIsZeroAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlVarianceAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveBetween;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveConcat;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveDateAddSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveDateSubSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExtractDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFloorDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFromUnixTimeSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIn;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveToDateSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveToUnixTimestampSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTruncSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnixTimestampSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter;
import org.apache.hadoop.hive.ql.parse.type.RexNodeExprFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Hive extension of RelJson, used for serialization and deserialization.
 * This class might be smaller after upgrading Calcite to 1.33
 */
public class HiveRelJson extends RelJson {
  private final JsonBuilder jsonBuilder;
  private static final String NULLABLE = "nullable";
  private static final String OPERANDS = "operands";
  private static final String DISTINCT = "distinct";
  private static final String LITERAL = "literal";

  public HiveRelJson(JsonBuilder jsonBuilder) {
    super(jsonBuilder);
    this.jsonBuilder = jsonBuilder;
  }

  @Override
  public Object toJson(Object value) {
    if (value instanceof RelDistribution) {
      return toJson((RelDistribution) value);
    }
    if (value instanceof RexNode) {
      return toJson((RexNode) value);
    }
    if (value instanceof RelDataTypeField) {
      return toJson((RelDataTypeField) value);
    }
    if (value instanceof RelDataType) {
      return toJson((RelDataType) value);
    }
    if (value instanceof AggregateCall) {
      return toJson((AggregateCall) value);
    }
    if (value instanceof HiveTableScan.HiveTableScanTrait) {
      return ((HiveTableScan.HiveTableScanTrait) value).name();
    }
    return super.toJson(value);
  }

  @Override
  public Object toJson(AggregateCall node) {
    final Map<String, Object> map = (Map<String, Object>) super.toJson(node);
    map.put("type", toJson(node.getType()));
    if (node.getCollation() != RelCollations.EMPTY) {
      map.put("collation", toJson(node.getCollation()));
    }
    return map;
  }

  private Object toJson(RelDataTypeField node) {
    final Map<String, Object> map;
    if (node.getType().isStruct()) {
      map = jsonBuilder.map();
      map.put("fields", toJson(node.getType()));
    } else {
      map = (Map<String, Object>) toJson(node.getType());
    }
    map.put("name", node.getName());
    return map;
  }

  private Object toJson(RelDataType node) {
    if (node.isStruct()) {
      final List<Object> list = jsonBuilder.list();
      for (RelDataTypeField field : node.getFieldList()) {
        list.add(toJson(field));
      }
      return list;
    } else {
      final Map<String, Object> map = jsonBuilder.map();
      map.put("type", node.getSqlTypeName().name());
      map.put(NULLABLE, node.isNullable());
      if (node.getSqlTypeName().allowsPrec()) {
        map.put("precision", node.getPrecision());
      }
      if (node.getSqlTypeName().allowsScale()) {
        map.put("scale", node.getScale());
      }

      if (SqlTypeName.MAP == node.getSqlTypeName()) {
        map.put("key", toJson(node.getKeyType()));
        map.put("value", toJson(node.getValueType()));
      } else if (SqlTypeName.ARRAY == node.getSqlTypeName()) {
        map.put("component", toJson(node.getComponentType()));
      }
      return map;
    }
  }

  private Object toJson(RexNode node) {
    final Map<String, Object> map;
    switch (node.getKind()) {
      case FIELD_ACCESS:
        map = jsonBuilder.map();
        final RexFieldAccess fieldAccess = (RexFieldAccess) node;
        map.put("field", fieldAccess.getField().getName());
        map.put("expr", toJson(fieldAccess.getReferenceExpr()));
        return map;
      case LITERAL:
        return toJsonLiteral((RexLiteral) node);
      case INPUT_REF:
      case LOCAL_REF:
        map = jsonBuilder.map();
        map.put("input", ((RexSlot) node).getIndex());
        map.put("name", ((RexSlot) node).getName());
        map.put("type", toJson(node.getType()));
        return map;
      case CORREL_VARIABLE:
        map = jsonBuilder.map();
        map.put("correl", ((RexCorrelVariable) node).getName());
        map.put("type", toJson(node.getType()));
        return map;
      case DYNAMIC_PARAM:
        map = jsonBuilder.map();
        map.put("dynamic_param", true);
        map.put("index", ((RexDynamicParam) node).getIndex());
        return map;
      default:
        Map<String, Object> mapRexCall = toJsonRexCall(node);
        if (mapRexCall != null) {
          return mapRexCall;
        }
        throw new UnsupportedOperationException("unknown rex " + node);
    }
  }

  @Nullable
  private Map<String, Object> toJsonRexCall(RexNode node) {
    Map<String, Object> map = null;
    if (node instanceof RexCall) {
      final RexCall call = (RexCall) node;
      map = jsonBuilder.map();
      map.put("op", toJson(call.getOperator()));
      map.put("type", toJson(call.getType()));
      final List<Object> list = jsonBuilder.list();
      for (RexNode operand : call.getOperands()) {
        list.add(toJson(operand));
      }
      map.put(OPERANDS, list);
      switch (node.getKind()) {
        case CAST:
        case MAP_QUERY_CONSTRUCTOR:
        case MAP_VALUE_CONSTRUCTOR:
        case ARRAY_QUERY_CONSTRUCTOR:
        case ARRAY_VALUE_CONSTRUCTOR:
          map.put("type", toJson(node.getType()));
          break;
        default:
          break;
      }
      if (call.getOperator() instanceof SqlFunction &&
          (((SqlFunction) call.getOperator()).getFunctionType().isUserDefined())) {
        SqlOperator op = call.getOperator();
        map.put("class", op.getClass().getName());
        map.put("type", toJson(node.getType()));
        map.put("deterministic", op.isDeterministic());
        map.put("dynamic", op.isDynamicFunction());
      }
      if (call instanceof RexOver) {
        RexOver over = (RexOver) call;
        map.put(DISTINCT, over.isDistinct());
        map.put("type", toJson(node.getType()));
        map.put("window", toJson(over.getWindow()));
        map.put("ignoreNulls", toJson(over.ignoreNulls()));
      }
    }
    return map;
  }

  @NotNull
  private Map<String, Object> toJsonLiteral(RexLiteral literal) {
    final Map<String, Object> map;
    map = jsonBuilder.map();
    final Object value;
    if (SqlTypeFamily.TIMESTAMP == literal.getTypeName().getFamily()) {
      // Had to do this to prevent millis or nanos from getting trimmed
      value = literal.computeDigest(RexDigestIncludeType.NO_TYPE);
    } else {
      value = literal.getValue3();
    }
    map.put(LITERAL, RelEnumTypes.fromEnum(value));
    map.put("type", toJson(literal.getType()));
    return map;
  }

  // Upgrade to Calcite 1.23.0 to remove this method
  private Object toJson(RelDistribution relDistribution) {
    final Map<String, Object> map = jsonBuilder.map();
    map.put("type", relDistribution.getType().name());

    if (!relDistribution.getKeys().isEmpty()) {
      List<Object> keys = new ArrayList<>(relDistribution.getKeys().size());
      for (Integer key : relDistribution.getKeys()) {
        keys.add(toJson(key));
      }
      map.put("keys", keys);
    }
    return map;
  }

  private Map toJson(SqlOperator operator) {
    // User-defined operators are not yet handled.
    Map map = jsonBuilder.map();
    map.put("name", operator.getName());
    map.put("kind", operator.kind.toString());
    map.put("syntax", operator.getSyntax().toString());
    return map;
  }

  private RexNode makeLiteral(RexBuilder rexBuilder, Object literal, RelDataType type) {
    switch (type.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        literal = RexNodeExprFactory.makeHiveUnicodeString((String) literal);
        return rexBuilder.makeLiteral(literal, type, true);
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        Object timeStampLiteral = literal;
        if (literal instanceof Integer) {
          timeStampLiteral = (long) (int) literal;
        } else if (literal instanceof String) {
          timeStampLiteral = new TimestampString((String) literal);
        }
        return rexBuilder.makeLiteral(timeStampLiteral, type, false);
      case SYMBOL:
        return rexBuilder.makeLiteral(HiveRelEnumTypes.toEnum((String) literal), type, true);
      case DECIMAL:
        if (literal instanceof BigInteger) {
          return rexBuilder.makeLiteral(new BigDecimal((BigInteger) literal), type, true);
        }
        break;
      case DOUBLE:
        if (literal instanceof Integer) {
          return rexBuilder.makeExactLiteral(new BigDecimal((Integer) literal), type);
        }
        break;
      case FLOAT:
        if (literal instanceof Integer) {
          return rexBuilder.makeLiteral(new BigDecimal((Integer) literal), type, true);
        }
        break;
      default:
        break;
    }
    return rexBuilder.makeLiteral(literal, type, true);
  }

  private SqlOperator getOperatorFromDefault(String op) {
    // TODO: build a map, for more efficient lookup
    // TODO: look up based on SqlKind
    final List<SqlOperator> operatorList = SqlStdOperatorTable.instance().getOperatorList();
    for (SqlOperator operator : operatorList) {
      if (operator.getName().equals(op)) {
        return operator;
      }
    }
    return null;
  }

  public SqlAggFunction toAggregation(RelInput input, String aggName, Map<String, Object> map) {
    final Boolean distinct = (Boolean) map.get(DISTINCT);
    @SuppressWarnings("unchecked") final List<Object> operands = (List<Object>) map.get(OPERANDS);
    final RelDataType type = toType(input.getCluster().getTypeFactory(), map.get("type"));

    final SqlReturnTypeInference returnTypeInference = ReturnTypes.explicit(type);
    final ImmutableList.Builder<RelDataType> argTypesBuilder = new ImmutableList.Builder<>();
    for (Object e : operands) {
      if (e instanceof Integer) {
        argTypesBuilder.add(input.getInput().getRowType().getFieldList().get((Integer) e).getType());
      } else {
        argTypesBuilder.add(toRex(input, e).getType());
      }
    }
    final List<RelDataType> argTypes = argTypesBuilder.build();
    final SqlOperandTypeInference operandTypeInference = InferTypes.explicit(argTypes);
    final ImmutableList.Builder<SqlTypeFamily> typeFamilyBuilder = new ImmutableList.Builder<>();
    for (RelDataType at : argTypes) {
      typeFamilyBuilder.add(Util.first(at.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
    }
    final SqlOperandTypeChecker operandTypeChecker = OperandTypes.family(typeFamilyBuilder.build());
    switch (aggName) {
      case "$SUM0":
        return new HiveSqlSumEmptyIsZeroAggFunction(
            distinct, returnTypeInference, operandTypeInference, operandTypeChecker
        );
      case "sum":
        return new HiveSqlSumAggFunction(distinct, returnTypeInference, operandTypeInference, operandTypeChecker);
      case "count":
        return new HiveSqlCountAggFunction(distinct, returnTypeInference, operandTypeInference, operandTypeChecker);
      case "min":
        return new HiveSqlMinMaxAggFunction(returnTypeInference, operandTypeInference, operandTypeChecker, true);
      case "max":
        return new HiveSqlMinMaxAggFunction(returnTypeInference, operandTypeInference, operandTypeChecker, false);
      case "avg":
        return new HiveSqlAverageAggFunction(distinct, returnTypeInference, operandTypeInference, operandTypeChecker);
      case "std":
      case "stddev":
      case "stddev_pop":
        return new HiveSqlVarianceAggFunction("stddev_pop", SqlKind.STDDEV_POP, returnTypeInference, operandTypeInference,
            operandTypeChecker);
      case "stddev_samp":
        return new HiveSqlVarianceAggFunction("stddev_samp", SqlKind.STDDEV_SAMP, returnTypeInference,
            operandTypeInference, operandTypeChecker);
      case "variance":
      case "var_pop":
        return new HiveSqlVarianceAggFunction("var_pop", SqlKind.VAR_POP, returnTypeInference, operandTypeInference,
            operandTypeChecker);
      case "var_samp":
        return new HiveSqlVarianceAggFunction("var_samp", SqlKind.VAR_SAMP, returnTypeInference, operandTypeInference,
            operandTypeChecker);
      default:
        SqlOperator operator = getOperatorFromDefault(aggName);
        if (operator != null) {
          return (SqlAggFunction) operator;
        }
        return new SqlFunctionConverter.CalciteUDAF(distinct, aggName, returnTypeInference, operandTypeInference, operandTypeChecker);
    }
  }

  public RelDistribution toDistribution(Object o) {
    if (o instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) o;
      final RelDistribution.Type type =
          Util.enumVal(RelDistribution.Type.class,
              (String) map.get("type"));

      List<Integer> keys = new ArrayList<>();
      if (map.containsKey("keys")) {
        keys = (List<Integer>) map.get("keys");
      }
      return RelDistributions.of(type, ImmutableIntList.copyOf(keys));
    }
    return RelDistributions.ANY;
  }

  private SqlOperator toOp(RelInput input, String op, Map<String, Object> map) {
    switch (op) {
      case "IN":
        return HiveIn.INSTANCE;
      case "BETWEEN":
        return HiveBetween.INSTANCE;
      case "YEAR":
        return HiveExtractDate.YEAR;
      case "QUARTER":
        return HiveExtractDate.QUARTER;
      case "MONTH":
        return HiveExtractDate.MONTH;
      case "WEEK":
        return HiveExtractDate.WEEK;
      case "DAY":
        return HiveExtractDate.DAY;
      case "HOUR":
        return HiveExtractDate.HOUR;
      case "MINUTE":
        return HiveExtractDate.MINUTE;
      case "SECOND":
        return HiveExtractDate.SECOND;
      case "FLOOR_YEAR":
        return HiveFloorDate.YEAR;
      case "FLOOR_QUARTER":
        return HiveFloorDate.QUARTER;
      case "FLOOR_MONTH":
        return HiveFloorDate.MONTH;
      case "FLOOR_WEEK":
        return HiveFloorDate.WEEK;
      case "FLOOR_DAY":
        return HiveFloorDate.DAY;
      case "FLOOR_HOUR":
        return HiveFloorDate.HOUR;
      case "FLOOR_MINUTE":
        return HiveFloorDate.MINUTE;
      case "FLOOR_SECOND":
        return HiveFloorDate.SECOND;
      case "||":
        return HiveConcat.INSTANCE;
      case "TRUNC":
        return HiveTruncSqlOperator.INSTANCE;
      case "TO_DATE":
        return HiveToDateSqlOperator.INSTANCE;
      case "UNIX_TIMESTAMP":
        if (!((List) map.get(OPERANDS)).isEmpty()) {
          return HiveToUnixTimestampSqlOperator.INSTANCE;
        }
        return HiveUnixTimestampSqlOperator.INSTANCE;
      case "FROM_UNIXTIME":
        return HiveFromUnixTimeSqlOperator.INSTANCE;
      case "DATE_ADD":
        return HiveDateAddSqlOperator.INSTANCE;
      case "DATE_SUB":
        return HiveDateSubSqlOperator.INSTANCE;
      case "+":
        if (((List) map.get(OPERANDS)).size() > 1) {
          return SqlStdOperatorTable.PLUS;
        }
        return SqlStdOperatorTable.UNARY_PLUS;
      case "-":
        if (((List) map.get(OPERANDS)).size() > 1) {
          return SqlStdOperatorTable.MINUS;
        }
        return SqlStdOperatorTable.UNARY_MINUS;
      case "MAP":
        return SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR;
      case "ARRAY":
        return SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR;
      default:
        SqlOperator operator = getOperatorFromDefault(op);
        if (operator != null) {
          return operator;
        }
        final Object jsonType = map.get("type");
        final RelDataType type = toType(input.getCluster().getTypeFactory(), jsonType);
        final List operands = (List) map.get(OPERANDS);
        final List<RelDataType> operandsTypes =
            toRexList(input, operands).stream().map(RexNode::getType).collect(Collectors.toList());
        final List<SqlTypeFamily> typeFamily = operandsTypes.stream()
            .map(e -> Util.first(e.getSqlTypeName().getFamily(), SqlTypeFamily.ANY))
            .collect(Collectors.toList());
        final boolean isDeterministic = (boolean) map.get("deterministic");
        final boolean isDynamicFunction = (boolean) map.get("dynamic");
        return new org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelJson.CalciteSqlFn(
            op, SqlKind.OTHER_FUNCTION, ReturnTypes.explicit(type),
            InferTypes.explicit(operandsTypes), OperandTypes.family(typeFamily),
            SqlFunctionCategory.USER_DEFINED_FUNCTION, isDeterministic, isDynamicFunction
        );
    }
  }

  private static class CalciteSqlFn extends SqlFunction {
    private final boolean deterministic;
    private final boolean dynamicFunction;

    public CalciteSqlFn(String name, SqlKind kind, SqlReturnTypeInference returnTypeInference,
                        SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker,
                        SqlFunctionCategory category, boolean deterministic, boolean dynamicFunction) {
      super(name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, category);
      this.deterministic = deterministic;
      this.dynamicFunction = dynamicFunction;
    }

    @Override
    public boolean isDeterministic() {
      return deterministic;
    }

    @Override
    public boolean isDynamicFunction() {
      return dynamicFunction;
    }
  }

  public RexNode toRex(RelInput relInput, Object o) {
    final RelOptCluster cluster = relInput.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    if (o == null) {
      return null;
    } else if (o instanceof Map) {
      return toRexMap(relInput, o, cluster);
    } else if (o instanceof Boolean) {
      return rexBuilder.makeLiteral((Boolean) o);
    } else if (o instanceof String) {
      return rexBuilder.makeLiteral((String) o);
    } else if (o instanceof Number) {
      return makeNumberLiteral((Number) o, rexBuilder);
    } else {
      throw new UnsupportedOperationException("cannot convert to rex " + o);
    }
  }

  private RexNode toRexMap(RelInput relInput, Object o, RelOptCluster cluster) {
    RexBuilder rexBuilder = relInput.getCluster().getRexBuilder();
    Map map = (Map) o;
    final Map opMap = (Map) map.get("op");
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    if (opMap != null) {
      return toRexOperator(relInput, opMap, map, typeFactory);
    }
    final Integer input = (Integer) map.get("input");
    if (input != null) {
      return makeInputRef(relInput, input, rexBuilder);
    }
    final String field = (String) map.get("field");
    if (field != null) {
      final Object jsonExpr = map.get("expr");
      final RexNode expr = toRex(relInput, jsonExpr);
      return rexBuilder.makeFieldAccess(expr, field, true);
    }
    final String correl = (String) map.get("correl");
    if (correl != null) {
      final Object jsonType = map.get("type");
      RelDataType type = toType(typeFactory, jsonType);
      return rexBuilder.makeCorrel(type, new CorrelationId(correl));
    }
    if (map.containsKey(LITERAL)) {
      return makeLiteral(relInput, map, typeFactory);
    }
    if (map.containsKey("dynamic_param")) {
      int index = (int) map.get("index");
      return rexBuilder.makeDynamicParam(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.NULL), index);
    }
    throw new UnsupportedOperationException("cannot convert to rex " + o);
  }

  private static RexLiteral makeNumberLiteral(Number o, RexBuilder rexBuilder) {
    if (o instanceof Double || o instanceof Float) {
      return rexBuilder.makeApproxLiteral(BigDecimal.valueOf(o.doubleValue()));
    } else {
      return rexBuilder.makeExactLiteral(BigDecimal.valueOf(o.longValue()));
    }
  }

  private RexNode makeLiteral(RelInput relInput, Map map, RelDataTypeFactory typeFactory) {
    Object literal = map.get(LITERAL);
    RexBuilder rexBuilder = relInput.getCluster().getRexBuilder();
    final RelDataType type = toType(typeFactory, map.get("type"));
    if (literal == null) {
      return rexBuilder.makeNullLiteral(type);
    }
    if (type == null) {
      // In previous versions, type was not specified for all literals.
      // To keep backwards compatibility, if type is not specified
      // we just interpret the literal
      return toRex(relInput, literal);
    }
    return makeLiteral(rexBuilder, literal, type);
  }

  private static RexInputRef makeInputRef(RelInput relInput, Integer input, RexBuilder rexBuilder) {
    List<RelNode> inputNodes = relInput.getInputs();
    int i = input;
    for (RelNode inputNode : inputNodes) {
      final RelDataType rowType = inputNode.getRowType();
      if (i < rowType.getFieldCount()) {
        final RelDataTypeField field = rowType.getFieldList().get(i);
        return rexBuilder.makeInputRef(field.getType(), input);
      }
      i -= rowType.getFieldCount();
    }
    throw new RuntimeException("input field " + input + " is out of range");
  }

  private RexNode toRexOperator(RelInput relInput, Map opMap, Map map, RelDataTypeFactory typeFactory) {
    RexBuilder rexBuilder = relInput.getCluster().getRexBuilder();
    final String opName = (String) opMap.get("name");
    final List operands = (List) map.get(OPERANDS);
    final List<RexNode> rexOperands = toRexList(relInput, operands);
    final Object jsonType = map.get("type");
    final Map window = (Map) map.get("window");
    if (window != null) {
      return toRexWindow(relInput, opName, map, typeFactory, jsonType, window, rexOperands);
    } else {
      final SqlOperator operator = toOp(relInput, opName, map);
      final RelDataType type;
      if (jsonType != null) {
        type = toType(typeFactory, jsonType);
      } else {
        type = rexBuilder.deriveReturnType(operator, rexOperands);
      }
      return rexBuilder.makeCall(type, operator, rexOperands);
    }
  }

  private RexNode toRexWindow(RelInput relInput, String opName, Map map, RelDataTypeFactory typeFactory,
                              Object jsonType, Map window, List<RexNode> rexOperands) {
    final SqlAggFunction operator = toAggregation(relInput, opName, map);
    final RelDataType type = toType(typeFactory, jsonType);
    List<RexNode> partitionKeys = new ArrayList<>();
    if (window.containsKey("partition")) {
      partitionKeys = toRexList(relInput, (List) window.get("partition"));
    }
    List<RexFieldCollation> orderKeys = new ArrayList<>();
    if (window.containsKey("order")) {
      orderKeys = toRexFieldCollationList(relInput, (List) window.get("order"));
    }
    final RexWindowBound lowerBound;
    final RexWindowBound upperBound;
    final boolean physical;
    if (window.get("rows-lower") != null) {
      lowerBound = toRexWindowBound(relInput, (Map) window.get("rows-lower"));
      upperBound = toRexWindowBound(relInput, (Map) window.get("rows-upper"));
      physical = true;
    } else if (window.get("range-lower") != null) {
      lowerBound = toRexWindowBound(relInput, (Map) window.get("range-lower"));
      upperBound = toRexWindowBound(relInput, (Map) window.get("range-upper"));
      physical = false;
    } else {
      // No ROWS or RANGE clause
      lowerBound = null;
      upperBound = null;
      physical = false;
    }
    final boolean distinct = (Boolean) map.get(DISTINCT);
    final boolean ignoreNulls = (Boolean) map.get("ignoreNulls");
    return relInput.getCluster().getRexBuilder().makeOver(
        type,
        operator,
        rexOperands,
        partitionKeys,
        ImmutableList.copyOf(orderKeys),
        lowerBound,
        upperBound,
        physical,
        true,
        false,
        distinct,
        ignoreNulls);
  }

  private List<RexFieldCollation> toRexFieldCollationList(RelInput relInput, List<Map<String, Object>> order) {
    if (order == null) {
      return null;
    }

    List<RexFieldCollation> list = new ArrayList<>();
    for (Map<String, Object> o : order) {
      RexNode expr = toRex(relInput, o.get("expr"));
      Set<SqlKind> directions = new HashSet<>();
      if (RelFieldCollation.Direction.valueOf((String) o.get("direction")) == RelFieldCollation.Direction.DESCENDING) {
        directions.add(SqlKind.DESCENDING);
      }
      if (RelFieldCollation.NullDirection.valueOf((String) o.get("null-direction")) == RelFieldCollation.NullDirection.FIRST) {
        directions.add(SqlKind.NULLS_FIRST);
      } else {
        directions.add(SqlKind.NULLS_LAST);
      }
      list.add(new RexFieldCollation(expr, directions));
    }

    return list;
  }

  private List<RexNode> toRexList(RelInput relInput, List operands) {
    final List<RexNode> list = new ArrayList<>();
    for (Object operand : operands) {
      list.add(toRex(relInput, operand));
    }
    return list;
  }

  private RexWindowBound toRexWindowBound(RelInput input, Map<String, Object> map) {
    if (map == null) {
      return null;
    }

    final String type = (String) map.get("type");
    switch (type) {
      case "CURRENT_ROW":
        return RexWindowBounds.create(SqlWindow.createCurrentRow(SqlParserPos.ZERO), null);
      case "UNBOUNDED_PRECEDING":
        return RexWindowBounds.create(SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null);
      case "UNBOUNDED_FOLLOWING":
        return RexWindowBounds.create(SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO), null);
      case "PRECEDING":
        RexNode precedingOffset = toRex(input, map.get("offset"));
        return RexWindowBounds.create(
            null,
            input.getCluster()
                .getRexBuilder()
                .makeCall(
                    new SqlPostfixOperator("PRECEDING", SqlKind.PRECEDING, 20, ReturnTypes.ARG0, null, null),
                    precedingOffset));
      case "FOLLOWING":
        RexNode followingOffset = toRex(input, map.get("offset"));
        return RexWindowBounds.create(
            null,
            input.getCluster()
                .getRexBuilder()
                .makeCall(
                    new SqlPostfixOperator("FOLLOWING", SqlKind.FOLLOWING, 20, ReturnTypes.ARG0, null, null),
                    followingOffset));
      default:
        throw new UnsupportedOperationException("cannot convert type to rex window bound " + type);
    }
  }

  @Override
  public RelDataType toType(RelDataTypeFactory typeFactory, Object o) {
    if (o instanceof List) {
      return toType(typeFactory, (List<Map<String, Object>>) o);
    } else if (o instanceof Map) {
      return toType(typeFactory, (Map<String, Object>) o);
    } else {
      final SqlTypeName sqlTypeName = Util.enumVal(SqlTypeName.class, (String) o);
      return typeFactory.createSqlType(sqlTypeName);
    }
  }

  private RelDataType toType(RelDataTypeFactory typeFactory, Map<String, Object> o) {
    final Object fields = o.get("fields");
    if (fields != null) {
      // Nested struct
      return toType(typeFactory, fields);
    } else {
      final SqlTypeName sqlTypeName = Util.enumVal(SqlTypeName.class, (String) o.get("type"));
      final Integer precision = (Integer) o.get("precision");
      final Integer scale = (Integer) o.get("scale");
      RelDataType type;
      if (SqlTypeName.INTERVAL_TYPES.contains(sqlTypeName)) {
        return createSqlIntervalType(typeFactory, sqlTypeName);
      }
      if (SqlTypeName.ARRAY == sqlTypeName) {
        return createArrayType(typeFactory, o);
      }
      if (SqlTypeName.MAP == sqlTypeName) {
        return createMapType(typeFactory, o);
      }
      if (precision == null) {
        type = typeFactory.createSqlType(sqlTypeName);
      } else if (scale == null) {
        type = typeFactory.createSqlType(sqlTypeName, precision);
      } else {
        type = typeFactory.createSqlType(sqlTypeName, precision, scale);
      }
      if (SqlTypeUtil.inCharFamily(type)) {
        type = typeFactory.createTypeWithCharsetAndCollation(
            type, Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT
        );
      }
      final boolean nullable = (Boolean) o.get(NULLABLE);
      return typeFactory.createTypeWithNullability(type, nullable);
    }
  }

  private RelDataType createMapType(RelDataTypeFactory typeFactory, Map<String, Object> o) {
    RelDataType type;
    Object key = o.get("key");
    Object value = o.get("value");
    final Boolean nullable = (Boolean) o.get(NULLABLE);
    RelDataType keyType = toType(typeFactory, key);
    RelDataType valueType = toType(typeFactory, value);
    type = typeFactory.createMapType(keyType, valueType);
    if (nullable != null) {
      type = typeFactory.createTypeWithNullability(type, nullable);
    }
    return type;
  }

  private RelDataType createArrayType(RelDataTypeFactory typeFactory, Map<String, Object> o) {
    RelDataType type;
    final Boolean nullable = (Boolean) o.get(NULLABLE);
    final Object component = o.get("component");
    type = toType(typeFactory, component);
    type = typeFactory.createArrayType(type, -1);
    if (nullable != null) {
      type = typeFactory.createTypeWithNullability(type, nullable);
    }
    return type;
  }

  private static RelDataType createSqlIntervalType(RelDataTypeFactory typeFactory, SqlTypeName sqlTypeName) {
    TimeUnit startUnit = sqlTypeName.getStartUnit();
    TimeUnit endUnit = sqlTypeName.getEndUnit();
    return typeFactory.createSqlIntervalType(new SqlIntervalQualifier(startUnit, endUnit, SqlParserPos.ZERO));
  }

  private RelDataType toType(RelDataTypeFactory typeFactory, List<Map<String, Object>> o) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (Map<String, Object> jsonMap : o) {
      builder.add((String) jsonMap.get("name"), toType(typeFactory, jsonMap));
    }
    return builder.build();
  }
}
