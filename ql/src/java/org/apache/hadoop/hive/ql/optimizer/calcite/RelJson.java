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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
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
import org.apache.calcite.util.Util;
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
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveToDateSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTruncSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnixTimestampSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter.CalciteUDAF;
import org.apache.hadoop.hive.ql.parse.type.RexNodeExprFactory;

import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RelJson {
  private final Map<String, Constructor> constructorMap = new HashMap<>();

  public static final List<String> PACKAGES = ImmutableList.of(
      "org.apache.calcite.rel.",
      "org.apache.calcite.rel.core.",
      "org.apache.calcite.rel.logical.",
      "org.apache.calcite.adapter.jdbc.",
      "org.apache.calcite.adapter.jdbc.JdbcRules$");

  public RelJson() {
  }

  public Constructor getConstructor(String type) {
    Constructor constructor = constructorMap.get(type);
    if (constructor == null) {
      Class clazz = typeNameToClass(type);
      try {
        //noinspection unchecked
        constructor = clazz.getConstructor(RelInput.class);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException("class does not have required constructor, " + clazz + "(RelInput)");
      }
      constructorMap.put(type, constructor);
    }
    return constructor;
  }

  /**
   * Converts a type name to a class. E.g. {@code getClass("LogicalProject")}
   * returns {@link org.apache.calcite.rel.logical.LogicalProject}.class.
   */
  public Class typeNameToClass(String type) {
    if (!type.contains(".")) {
      for (String package_ : PACKAGES) {
        try {
          return Class.forName(package_ + type);
        } catch (ClassNotFoundException e) {
          // ignore
        }
      }
    }
    try {
      return Class.forName(type);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("unknown type " + type);
    }
  }

  public RelCollation toCollation(List<Map<String, Object>> jsonFieldCollations) {
    final List<RelFieldCollation> fieldCollations = new ArrayList<RelFieldCollation>();
    for (Map<String, Object> map : jsonFieldCollations) {
      fieldCollations.add(toFieldCollation(map));
    }
    return RelCollations.of(fieldCollations);
  }

  public RelFieldCollation toFieldCollation(Map<String, Object> map) {
    final Integer field = (Integer) map.get("field");
    final Direction direction = Util.enumVal(Direction.class, (String) map.get("direction"));
    final NullDirection nullDirection = Util.enumVal(NullDirection.class, (String) map.get("nulls"));
    return new RelFieldCollation(field, direction, nullDirection);
  }

  public RelDistribution toDistribution(Object o) {
    return RelDistributions.ANY; // TODO:
  }

  public RelDataType toType(RelDataTypeFactory typeFactory, Object o) {
    if (o instanceof List) {
      @SuppressWarnings("unchecked") final List<Map<String, Object>> jsonList = (List<Map<String, Object>>) o;
      final RelDataTypeFactory.Builder builder = typeFactory.builder();
      for (Map<String, Object> jsonMap : jsonList) {
        builder.add((String) jsonMap.get("name"), toType(typeFactory, jsonMap));
      }
      return builder.build();
    } else if (o instanceof Map) {
      @SuppressWarnings("unchecked") final Map<String, Object> map = (Map<String, Object>) o;
      final Object fields = map.get("fields");
      if (fields != null) {
        // Nested struct
        return toType(typeFactory, fields);
      } else {
        final SqlTypeName sqlTypeName = Util.enumVal(SqlTypeName.class, (String) map.get("type"));
        final Integer precision = (Integer) map.get("precision");
        final Integer scale = (Integer) map.get("scale");
        RelDataType type;
        if (SqlTypeName.INTERVAL_TYPES.contains(sqlTypeName)) {
          TimeUnit startUnit = sqlTypeName.getStartUnit();
          TimeUnit endUnit = sqlTypeName.getEndUnit();
          return typeFactory.createSqlIntervalType(new SqlIntervalQualifier(startUnit, endUnit, SqlParserPos.ZERO));
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
        final boolean nullable = (Boolean) map.get("nullable");
        return typeFactory.createTypeWithNullability(type, nullable);
      }
    } else {
      final SqlTypeName sqlTypeName = Util.enumVal(SqlTypeName.class, (String) o);
      return typeFactory.createSqlType(sqlTypeName);
    }
  }

  RexNode toRex(RelInput relInput, Object o) {
    final RelOptCluster cluster = relInput.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    if (o == null) {
      return null;
    } else if (o instanceof Map) {
      Map map = (Map) o;
      final Map opMap = (Map) map.get("op");
      final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
      if (opMap != null) {
        final String opName = (String) opMap.get("name");
        final List operands = (List) map.get("operands");
        final List<RexNode> rexOperands = toRexList(relInput, operands);
        final Object jsonType = map.get("type");
        final Map window = (Map) map.get("window");
        if (window != null) {
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
          final boolean distinct = (Boolean) map.get("distinct");
          return rexBuilder.makeOver(
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
              distinct);
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
      final Integer input = (Integer) map.get("input");
      if (input != null) {
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
      if (map.containsKey("literal")) {
        Object literal = map.get("literal");
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
      throw new UnsupportedOperationException("cannot convert to rex " + o);
    } else if (o instanceof Boolean) {
      return rexBuilder.makeLiteral((Boolean) o);
    } else if (o instanceof String) {
      return rexBuilder.makeLiteral((String) o);
    } else if (o instanceof Number) {
      final Number number = (Number) o;
      if (number instanceof Double || number instanceof Float) {
        return rexBuilder.makeApproxLiteral(BigDecimal.valueOf(number.doubleValue()));
      } else {
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(number.longValue()));
      }
    } else {
      throw new UnsupportedOperationException("cannot convert to rex " + o);
    }
  }

  private RexNode makeLiteral(RexBuilder rexBuilder, Object literal, RelDataType type) {
    switch (type.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        literal = RexNodeExprFactory.makeHiveUnicodeString((String) literal);
        return rexBuilder.makeLiteral(literal, type, true);

      case TIMESTAMP:
        long t;
        if (literal instanceof Number) {
          t = ((Number) literal).longValue();
        } else if (literal instanceof String) {
          t = Long.valueOf((String) literal);
        } else {
          throw new RuntimeException("Cannot create timestamp from parsed literal");
        }
        return rexBuilder.makeLiteral(t, type, false);

      default:
        return rexBuilder.makeLiteral(literal, type, true);
    }
  }

  private List<RexFieldCollation> toRexFieldCollationList(RelInput relInput, List<Map<String, Object>> order) {
    if (order == null) {
      return null;
    }

    List<RexFieldCollation> list = new ArrayList<>();
    for (Map<String, Object> o : order) {
      RexNode expr = toRex(relInput, o.get("expr"));
      Set<SqlKind> directions = new HashSet<>();
      if (Direction.valueOf((String) o.get("direction")) == Direction.DESCENDING) {
        directions.add(SqlKind.DESCENDING);
      }
      if (NullDirection.valueOf((String) o.get("null-direction")) == NullDirection.FIRST) {
        directions.add(SqlKind.NULLS_FIRST);
      } else {
        directions.add(SqlKind.NULLS_LAST);
      }
      list.add(new RexFieldCollation(expr, directions));
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
        return RexWindowBound.create(SqlWindow.createCurrentRow(SqlParserPos.ZERO), null);
      case "UNBOUNDED_PRECEDING":
        return RexWindowBound.create(SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null);
      case "UNBOUNDED_FOLLOWING":
        return RexWindowBound.create(SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO), null);
      case "PRECEDING":
        RexNode precedingOffset = toRex(input, map.get("offset"));
        return RexWindowBound.create(
            null,
            input.getCluster()
                .getRexBuilder()
                .makeCall(
                    new SqlPostfixOperator("PRECEDING", SqlKind.PRECEDING, 20, ReturnTypes.ARG0, null, null),
                    precedingOffset));
      case "FOLLOWING":
        RexNode followingOffset = toRex(input, map.get("offset"));
        return RexWindowBound.create(
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

  private List<RexNode> toRexList(RelInput relInput, List operands) {
    final List<RexNode> list = new ArrayList<>();
    for (Object operand : operands) {
      list.add(toRex(relInput, operand));
    }
    return list;
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
        return HiveUnixTimestampSqlOperator.INSTANCE;
      case "FROM_UNIXTIME":
        return HiveFromUnixTimeSqlOperator.INSTANCE;
      case "DATE_ADD":
        return HiveDateAddSqlOperator.INSTANCE;
      case "DATE_SUB":
        return HiveDateSubSqlOperator.INSTANCE;
      case "+":
        if (((List) map.get("operands")).size() > 1) {
          return SqlStdOperatorTable.PLUS;
        }
        return SqlStdOperatorTable.UNARY_PLUS;
      case "-":
        if (((List) map.get("operands")).size() > 1) {
          return SqlStdOperatorTable.MINUS;
        }
        return SqlStdOperatorTable.UNARY_MINUS;
      default:
        SqlOperator operator = getOperatorFromDefault(op);
        if (operator != null) {
          return operator;
        }
        final Object jsonType = map.get("type");
        final RelDataType type = toType(input.getCluster().getTypeFactory(), jsonType);
        final List operands = (List) map.get("operands");
        final List<RelDataType> operandsTypes =
            toRexList(input, operands).stream().map(RexNode::getType).collect(Collectors.toList());
        final List<SqlTypeFamily> typeFamily = operandsTypes.stream()
            .map(e -> Util.first(e.getSqlTypeName().getFamily(), SqlTypeFamily.ANY))
            .collect(Collectors.toList());
        final boolean isDeterministic = (boolean) map.get("deterministic");
        final boolean isDynamicFunction = (boolean) map.get("dynamic");
        return new CalciteSqlFn(op, SqlKind.OTHER_FUNCTION, ReturnTypes.explicit(type),
            InferTypes.explicit(operandsTypes), OperandTypes.family(typeFamily),
            SqlFunctionCategory.USER_DEFINED_FUNCTION, isDeterministic, isDynamicFunction);
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

  SqlAggFunction toAggregation(RelInput input, String aggName, Map<String, Object> map) {
    final Boolean distinct = (Boolean) map.get("distinct");
    @SuppressWarnings("unchecked") final List<Object> operands = (List<Object>) map.get("operands");
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
        return new CalciteUDAF(distinct, aggName, returnTypeInference, operandTypeInference, operandTypeChecker);
    }
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
}