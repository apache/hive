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

package org.apache.hadoop.hive.impala.funcmapper;

import com.google.common.collect.Sets;
import java.nio.charset.Charset;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.CalciteUDFInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveMergeableAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlAverageAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlCountAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlMinMaxAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlSumAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlVarianceAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSqlFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.PartitionPruneRuleHelper;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter.CalciteUDAF;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;
import org.apache.hadoop.hive.ql.parse.type.RexNodeExprFactory;
import org.apache.hadoop.hive.impala.plan.ImpalaQueryContext;
import org.apache.hadoop.hive.impala.prune.ImpalaPartitionPruneRuleHelper;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

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
  private final ImpalaQueryContext queryContext;
  private final RexBuilder rexBuilder;
  private final PartitionPruneRuleHelper pruneRuleHelper;

  public ImpalaFunctionHelper(ImpalaQueryContext queryContext, RexBuilder builder) {
    this.factory = new RexNodeExprFactory(builder, this);
    this.rexExecutor = new ImpalaRexExecutorImpl(queryContext);
    this.queryContext = queryContext;
    this.rexBuilder = builder;
    this.pruneRuleHelper = new ImpalaPartitionPruneRuleHelper(queryContext, rexBuilder);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isStrictOperandTypes() {
    return true;
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
  public PartitionPruneRuleHelper getPartitionPruneRuleHelper() {
    return pruneRuleHelper;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FunctionInfo getFunctionInfo(String functionText) throws SemanticException {
    return new ImpalaFunctionInfo(functionText);
  }

  /**
   * {@inheritDoc}
   *
   * The FunctionInfo passed in will only contain the "displayName" as created in
   * the "getFunctionInfo" method.
   */
  @Override
  public RelDataType getReturnType(FunctionInfo functionInfo, List<RexNode> inputs
      ) throws SemanticException {
    try {
      ImpalaFunctionResolver funcResolver = ImpalaFunctionResolverImpl.create(this,
          functionInfo.getDisplayName(), inputs, null);

      ImpalaFunctionInfo impalaFunctionInfo = (ImpalaFunctionInfo) functionInfo;
      impalaFunctionInfo.setFunctionResolver(funcResolver);

      ImpalaFunctionSignature ifs =
          funcResolver.getFunction(ScalarFunctionDetails.SCALAR_BUILTINS_MAP);
      impalaFunctionInfo.setImpalaFunctionSignature(ifs);
      return funcResolver.getRetType(ifs, inputs);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * The FunctionInfo passed in will contain the "displayName" as created in
   * the "getFunctionInfo". In the case where the "FunctionHelper.getReturnType"
   * method is called, it will also contain the FunctionResolver and
   * ImpalaFunctionSignature. If the getReturnType method was not called, it
   * will create the FunctionResolver class and set it into the FunctionInfo.
   */
  @Override
  public List<RexNode> convertInputs(FunctionInfo functionInfo, List<RexNode> inputs,
      RelDataType returnType) throws SemanticException {
    try {
      ImpalaFunctionInfo impalaFunctionInfo = (ImpalaFunctionInfo) functionInfo;
      ImpalaFunctionResolver funcResolver = impalaFunctionInfo.getFunctionResolver();
      if (funcResolver == null) {
        funcResolver =
            ImpalaFunctionResolverImpl.create(this,functionInfo.getDisplayName(), inputs, returnType);
        impalaFunctionInfo.setFunctionResolver(funcResolver);
      }
      return funcResolver.getConvertedInputs(impalaFunctionInfo.getImpalaFunctionSignature());
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * The FunctionInfo passed in will contain the "displayName" as created in
   * the "getFunctionInfo". In the case where the "FunctionHelper.getReturnType"
   * method is called, it will also contain the FunctionResolver and
   * ImpalaFunctionSignature. If the getReturnType method was not called, it
   * will create the FunctionResolver class and set it into the FunctionInfo.
   */
  @Override
  public RexNode getExpression(String functionText, FunctionInfo functionInfo, List<RexNode> inputs,
      RelDataType returnType) throws SemanticException {
    try {
      ImpalaFunctionInfo impalaFunctionInfo = (ImpalaFunctionInfo) functionInfo;
      ImpalaFunctionResolver funcResolver = impalaFunctionInfo.getFunctionResolver();
      if (funcResolver == null) {
        funcResolver =
            ImpalaFunctionResolverImpl.create(this,functionInfo.getDisplayName(), inputs, returnType);
        impalaFunctionInfo.setFunctionResolver(funcResolver);
      }
      return funcResolver.createRexNode(impalaFunctionInfo.getImpalaFunctionSignature(),
          inputs, returnType);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SqlOperator getCalciteFunction(String functionName, List<RelDataType> argTypes,
      RelDataType retType, boolean deterministic, boolean runtimeConstant) {
    SqlOperator op = ImpalaOperatorTable.IMPALA_OPERATOR_MAP.get(functionName.toUpperCase());
    if (op == null) {
      CalciteUDFInfo udfInfo = CalciteUDFInfo.createUDFInfo(functionName, argTypes, retType);
      op = new HiveSqlFunction(udfInfo.udfName, SqlKind.OTHER_FUNCTION, udfInfo.returnTypeInference,
          udfInfo.operandTypeInference, udfInfo.operandTypeChecker,
          SqlFunctionCategory.USER_DEFINED_FUNCTION, deterministic, runtimeConstant);
    }
    return op;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SqlAggFunction getCalciteAggregateFunction(String functionName, boolean isDistinct,
      List<RelDataType> argTypes, RelDataType retType) {
    RelDataTypeFactory typeFactory = factory.getRexBuilder().getTypeFactory();
    CalciteUDFInfo functionInfo = CalciteUDFInfo.createUDFInfo(functionName,
        argTypes, retType);

    SqlAggFunction calciteAggregateFunction;
    switch (functionName.toLowerCase()) {
    case "sum":
      calciteAggregateFunction = new HiveSqlSumAggFunction(
          isDistinct,
          functionInfo.returnTypeInference,
          functionInfo.operandTypeInference,
          functionInfo.operandTypeChecker);
      break;
    case "count":
      calciteAggregateFunction = new HiveSqlCountAggFunction(
          isDistinct,
          functionInfo.returnTypeInference,
          functionInfo.operandTypeInference,
          functionInfo.operandTypeChecker);
      break;
    case "min":
      calciteAggregateFunction = new HiveSqlMinMaxAggFunction(
          functionInfo.returnTypeInference,
          functionInfo.operandTypeInference,
          functionInfo.operandTypeChecker, true);
      break;
    case "max":
      calciteAggregateFunction = new HiveSqlMinMaxAggFunction(
          functionInfo.returnTypeInference,
          functionInfo.operandTypeInference,
          functionInfo.operandTypeChecker, false);
      break;
    case "avg":
      calciteAggregateFunction = new HiveSqlAverageAggFunction(
          isDistinct,
          functionInfo.returnTypeInference,
          functionInfo.operandTypeInference,
          functionInfo.operandTypeChecker);
      break;
    case "std":
    case "stddev":
    case "stddev_samp":
      calciteAggregateFunction = new HiveSqlVarianceAggFunction(
          "stddev_samp",
          SqlKind.STDDEV_SAMP,
          functionInfo.returnTypeInference,
          functionInfo.operandTypeInference,
          functionInfo.operandTypeChecker);
      break;
    case "stddev_pop":
      calciteAggregateFunction = new HiveSqlVarianceAggFunction(
          "stddev_pop",
          SqlKind.STDDEV_POP,
          functionInfo.returnTypeInference,
          functionInfo.operandTypeInference,
          functionInfo.operandTypeChecker);
      break;
    case "variance":
    case "var_samp":
      calciteAggregateFunction = new HiveSqlVarianceAggFunction(
          "var_samp",
          SqlKind.VAR_SAMP,
          functionInfo.returnTypeInference,
          functionInfo.operandTypeInference,
          functionInfo.operandTypeChecker);
      break;
    case "var_pop":
      calciteAggregateFunction = new HiveSqlVarianceAggFunction(
          "var_pop",
          SqlKind.VAR_POP,
          functionInfo.returnTypeInference,
          functionInfo.operandTypeInference,
          functionInfo.operandTypeChecker);
      break;
    // Data sketches: HLL
    case "ds_hll_union":
    case "ds_hll_sketch":
      SqlReturnTypeInference returnHLLTypeInference = ReturnTypes.explicit(
          typeFactory.createTypeWithCharsetAndCollation(
              typeFactory.createTypeWithNullability(
                  typeFactory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE), true),
              Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT));
      calciteAggregateFunction = new HiveMergeableAggregate("ds_hll_union",
          SqlKind.OTHER_FUNCTION,
          returnHLLTypeInference,
          InferTypes.ANY_NULLABLE,
          OperandTypes.family(),
          null);
      if (functionName.equalsIgnoreCase("ds_hll_sketch")) {
        calciteAggregateFunction = new HiveMergeableAggregate("ds_hll_sketch",
            SqlKind.OTHER_FUNCTION,
            returnHLLTypeInference,
            InferTypes.ANY_NULLABLE,
            OperandTypes.family(),
            calciteAggregateFunction);
      }
      break;
    // Data sketches: KLL
    case "ds_kll_union":
    case "ds_kll_sketch":
      SqlReturnTypeInference returnKLLTypeInference = ReturnTypes.explicit(
          typeFactory.createTypeWithCharsetAndCollation(
              typeFactory.createTypeWithNullability(
                  typeFactory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE), true),
              Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT));
      calciteAggregateFunction = new HiveMergeableAggregate("ds_kll_union",
          SqlKind.OTHER_FUNCTION,
          returnKLLTypeInference,
          InferTypes.ANY_NULLABLE,
          OperandTypes.family(),
          null);
      if (functionName.equalsIgnoreCase("ds_kll_sketch")) {
        calciteAggregateFunction = new HiveMergeableAggregate("ds_kll_sketch",
            SqlKind.OTHER_FUNCTION,
            returnKLLTypeInference,
            InferTypes.ANY_NULLABLE,
            OperandTypes.family(),
            calciteAggregateFunction);
      }
      break;
    default:
      calciteAggregateFunction = new CalciteUDAF(
          isDistinct,
          functionName,
          functionInfo.returnTypeInference,
          functionInfo.operandTypeInference,
          functionInfo.operandTypeChecker);
      break;
    }
    return calciteAggregateFunction;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RexNode makeCall(RexBuilder builder, SqlOperator operator, List<RexNode> operandList)
      throws SemanticException {
    // Some operators need special handling to extract their name,
    // since the correspondence Impala-Calcite is not exactly 1:1.
    // For instance, CASE statement in Calcite corresponds with
    // Impala's WHEN expression.
    final String functionName;
    switch (operator.getKind()) {
    case CASE:
      functionName = "WHEN";
      break;
    default:
      functionName = operator.getName();
    }
    FunctionInfo functionInfo = getFunctionInfo(functionName);
    RelDataType returnType = getReturnType(functionInfo, operandList);
    List<RexNode> newInputs = convertInputs(functionInfo, operandList, returnType);
    return getExpression(functionName, functionInfo, newInputs, returnType);
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
   * Given a string, it returns a RexNode of Decimal type.  If allowNullValueConstantExpr
   * is false, then this returns null if the string cannot be parsed. If it is true, then
   * this returns a RexNode of Decimal type with a null value.
   */
  @Override
  public RexNode createDecimalConstantExpr(String value, boolean allowNullValueConstantExpr) {
    BigDecimal bd = null;
    try {
      bd = new BigDecimal(value);
    } catch (NumberFormatException e) {
      if (!allowNullValueConstantExpr) {
        return null;
      }
    }

    RexBuilder rexBuilder = factory.getRexBuilder();
    if (bd == null) {
      return rexBuilder.makeNullLiteral(
          rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DECIMAL));
    }

    int scale = bd.scale();
    // precision can never be less than scale, same logic as in FastHiveDecimalImpl
    int precision = bd.precision() < scale ? scale : bd.precision();
    return rexBuilder.makeExactLiteral(bd,
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DECIMAL, precision, scale));
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
          funcResolver.getFunction(AggFunctionDetails.AGG_BUILTINS_MAP);

      List<RexNode> convertedInputs = funcResolver.getConvertedInputs(function);

      RelDataType retType = funcResolver.getRetType(function, convertedInputs);
      TypeInfo typeInfo = TypeConverter.convert(retType);
      return new AggregateInfo(convertedInputs, typeInfo, aggregateName, isDistinct);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  public Set<SqlKind> getAggReduceSupported() {
    return Sets.immutableEnumSet(SqlKind.AVG, SqlKind.SUM0);
  }

  /**
   * {@inheritDoc}
   */
  public SqlKind getDefaultStandardDeviation() {
    return SqlKind.STDDEV_SAMP;
  }

  /**
   * {@inheritDoc}
   */
  public SqlKind getDefaultVariance() {
    return SqlKind.VAR_SAMP;
  }

  /**
   * Returns true if the name corresponds to a scalar function
   * in Impala.
   */
  public boolean isScalarFunction(String name) {
    return ScalarFunctionDetails.SCALAR_BUILTINS.contains(name.toUpperCase());
  }

  /**
   * Returns true if the name corresponds to an aggregate function
   * in Impala.
   */
  public boolean isAggregateFunction(String name) {
    return AggFunctionDetails.AGG_BUILTINS.contains(name.toUpperCase());
  }

  /**
   * Returns true if the name corresponds to an analytic function
   * in Impala.
   */
  public boolean isAnalyticFunction(String name) {
    return AggFunctionDetails.ANALYTIC_BUILTINS.contains(name.toUpperCase());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isMultiColumnClauseSupported() {
    // Currently Impala does not support multi-column IN clauses.
    return false;
  }
  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isAndFunction(FunctionInfo fi) {
    return fi.getDisplayName().equals("and");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isOrFunction(FunctionInfo fi) {
    return fi.getDisplayName().equals("or");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isInFunction(FunctionInfo fi) {
    return fi.getDisplayName().equals("in");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isCompareFunction(FunctionInfo fi) {
    switch (fi.getDisplayName()) {
      case "=":
      case "==":
      case "<":
      case "<=":
      case ">":
      case ">=":
      case "!=":
      case "<>":
        return true;
      default:
        return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isEqualFunction(FunctionInfo fi) {
    return fi.getDisplayName().equals("=") || fi.getDisplayName().equals("==");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isConsistentWithinQuery(FunctionInfo fi) {
    // TODO: This is taken from Impala FunctionCallExpr class.
    //       We should consolidate both methods.
    // CDPD-28609: Don't reduce the sleep function since it's useful for testing
    // scenarios in Impala.
    return !fi.getDisplayName().equals("rand") &&
        !fi.getDisplayName().equals("random") &&
        !fi.getDisplayName().equals("uuid") &&
        !fi.getDisplayName().equals("sleep");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isStateful(FunctionInfo fi) {
    //TODO: CDPD-15770: need to implement for validation checks.
    return false;
  }

  @Override
  public void validateFunction(String functionName, boolean windowSpec) throws SemanticException {
    boolean scalarFunction = isScalarFunction(functionName);
    boolean aggFunction = !scalarFunction && isAggregateFunction(functionName);
    boolean analyticFunction = !scalarFunction && isAnalyticFunction(functionName);
    if (!scalarFunction && !aggFunction && !analyticFunction) {
      throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(functionName));
    }
    if (!aggFunction && analyticFunction && !windowSpec) {
      throw new SemanticException(ErrorMsg.MISSING_OVER_CLAUSE.getMsg(functionName));
    }
  }
}
