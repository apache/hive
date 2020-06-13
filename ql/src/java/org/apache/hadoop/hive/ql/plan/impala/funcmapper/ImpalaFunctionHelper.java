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

package org.apache.hadoop.hive.ql.plan.impala.funcmapper;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;
import org.apache.hadoop.hive.ql.parse.type.RexNodeExprFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;

/**
 * Function helper for Impala.
 */
@Evolving
public class ImpalaFunctionHelper implements FunctionHelper {

  private static final BigDecimal TINYINT_MIN_VALUE = BigDecimal.valueOf(Byte.MIN_VALUE);
  private static final BigDecimal TINYINT_MAX_VALUE = BigDecimal.valueOf(Byte.MAX_VALUE);
  private static final BigDecimal SMALLINT_MIN_VALUE = BigDecimal.valueOf(Short.MIN_VALUE);
  private static final BigDecimal SMALLINT_MAX_VALUE = BigDecimal.valueOf(Short.MAX_VALUE);
  private static final BigDecimal INT_MIN_VALUE = BigDecimal.valueOf(Integer.MIN_VALUE);
  private static final BigDecimal INT_MAX_VALUE = BigDecimal.valueOf(Integer.MAX_VALUE);

  private final RexNodeExprFactory factory;
  private final RexExecutor rexExecutor;

  public ImpalaFunctionHelper(RexBuilder builder) {
    this.factory = new RexNodeExprFactory(builder, this);
    this.rexExecutor = new ImpalaRexExecutorImpl();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RexNodeExprFactory getRexNodeExprFactory() {
    return factory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RexExecutor getRexExecutor() {
    return rexExecutor;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FunctionInfo getFunctionInfo(String functionText) {
    // return null here as all the processing is done in getExpression.  This shouldn't
    // break any existing contract with the caller.
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RelDataType getReturnType(FunctionInfo functionInfo, List<RexNode> inputs) {
    // return null here as all the processing is done in getExpression.  This shouldn't
    // break any existing contract with the caller.
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<RexNode> convertInputs(FunctionInfo functionInfo, List<RexNode> inputs, RelDataType returnType) {
    return inputs;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RexNode getExpression(String functionText, FunctionInfo functionInfo, List<RexNode> inputs,
      RelDataType returnType) throws SemanticException {
    // special case. Internal_interval is the only functionText passed in that isn't a RexCall and
    // doesn't fit the rest of the algorithm flow.
    if (functionText.equals("internal_interval")) {
      return getRexNodeForInternalInterval(inputs);
    }

    // For the grouping() function, skip calling the Impala function resolver because
    // (a) Impala currently does not support this function and (b) we convert it to
    // an equivalent form later in ImpalaRexCall. However, we still have to create
    // the RexNode, so we use the supplied RexNodeExprFactory to create one.
    // Note that the grouping() function as implemented in Hive takes a first argument
    // of grouping__id and behaves more like a scalar function than an
    // aggregate function but we use the GROUPING operator here for creation because it
    // is consistent with how Calcite treats it.
    if (functionText.equals("grouping")) {
      return factory.getRexBuilder().makeCall(returnType == null ?
              factory.getRexBuilder().getTypeFactory().createSqlType(SqlTypeName.BIGINT) : returnType,
          SqlStdOperatorTable.GROUPING, inputs);
    }

    try {
      ImpalaFunctionResolver funcResolver =
          ImpalaFunctionResolverImpl.create(this, functionText, inputs, returnType);

      ImpalaFunctionSignature function =
          funcResolver.getFunction(ScalarFunctionDetails.SCALAR_BUILTINS_INSTANCE);

      // get the converted inputs which usually just means casting the inputs so
      // it matches the Impala function signature, In the case of time addition,
      // it will also flip the arguments so the time is the first argument.
      List<RexNode> convertedInputs = funcResolver.getConvertedInputs(function);

      RelDataType retType = funcResolver.getRetType(function, convertedInputs);
      return funcResolver.createRexNode(function, convertedInputs, retType);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  /**
   * Given a whole number, it returns smaller exact numeric
   * that can hold this value between bigint, int, smallint,
   * and tinyint.
   */
  public RexNode getExactWholeNumber(String value) {
    RexBuilder rexBuilder = factory.getRexBuilder();
    BigDecimal bd = new BigDecimal(Long.parseLong(value));
    SqlTypeName type = SqlTypeName.BIGINT;
    if (bd.compareTo(TINYINT_MIN_VALUE) >= 0 && bd.compareTo(TINYINT_MAX_VALUE) <= 0) {
      type = SqlTypeName.TINYINT;
    } else if (bd.compareTo(SMALLINT_MIN_VALUE) >= 0 && bd.compareTo(SMALLINT_MAX_VALUE) <= 0) {
      type = SqlTypeName.SMALLINT;
    } else if (bd.compareTo(INT_MIN_VALUE) >= 0 && bd.compareTo(INT_MAX_VALUE) <= 0) {
      type = SqlTypeName.INTEGER;
    }
    return rexBuilder.makeLiteral(bd,
        rexBuilder.getTypeFactory().createSqlType(type),
        false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public AggregateInfo getAggregateFunctionInfo(boolean isDistinct, boolean isAllColumns, String aggregateName, List<RexNode> aggregateParameters) throws SemanticException {
    return getAggregateCommon(isDistinct, isAllColumns, aggregateName, aggregateParameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public AggregateInfo getWindowAggregateFunctionInfo(boolean isDistinct, boolean isAllColumns, String aggregateName, List<RexNode> aggregateParameters) throws SemanticException {
    return getAggregateCommon(isDistinct, isAllColumns, aggregateName, aggregateParameters);
  }

  private AggregateInfo getAggregateCommon(boolean isDistinct, boolean isAllColumns, String aggregateName,
      List<RexNode> aggregateParameters) throws SemanticException {
    try {
      ImpalaFunctionResolver funcResolver =
          ImpalaFunctionResolverImpl.create(this, aggregateName, aggregateParameters, null);
      ImpalaFunctionSignature function =
          funcResolver.getFunction(AggFunctionDetails.AGG_BUILTINS_INSTANCE);

      List<RexNode> convertedInputs = funcResolver.getConvertedInputs(function);

      RelDataType retType = funcResolver.getRetType(function, convertedInputs);
      TypeInfo typeInfo = TypeConverter.convert(retType);
      return new AggregateInfo(convertedInputs, typeInfo, aggregateName, isDistinct);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  private RexNode getRexNodeForInternalInterval(List<RexNode> inputs) {
    Preconditions.checkState(inputs.size() == 2);
    int intervalType = RexLiteral.intValue(inputs.get(0));
    switch (intervalType) {
      case HiveParser.TOK_INTERVAL_YEAR_LITERAL:
        return factory.createIntervalYearConstantExpr(Integer.toString(RexLiteral.intValue(inputs.get(1))));
      case HiveParser.TOK_INTERVAL_MONTH_LITERAL:
        return factory.createIntervalMonthConstantExpr(Integer.toString(RexLiteral.intValue(inputs.get(1))));
      case HiveParser.TOK_INTERVAL_DAY_LITERAL:
        return factory.createIntervalDayConstantExpr(Integer.toString(RexLiteral.intValue(inputs.get(1))));
      case HiveParser.TOK_INTERVAL_HOUR_LITERAL:
        return factory.createIntervalHourConstantExpr(Integer.toString(RexLiteral.intValue(inputs.get(1))));
      case HiveParser.TOK_INTERVAL_MINUTE_LITERAL:
        return factory.createIntervalMinuteConstantExpr(Integer.toString(RexLiteral.intValue(inputs.get(1))));
      case HiveParser.TOK_INTERVAL_SECOND_LITERAL:
        return factory.createIntervalSecondConstantExpr(Integer.toString(RexLiteral.intValue(inputs.get(1))));
      default:
        throw new RuntimeException("Unknown interval type: " + intervalType);
    }
  }
}
