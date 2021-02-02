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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.PartitionPruneRuleHelper;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;
import org.apache.hadoop.hive.ql.parse.type.RexNodeExprFactory;
import org.apache.hadoop.hive.impala.plan.ImpalaQueryContext;
import org.apache.hadoop.hive.impala.prune.ImpalaPartitionPruneRuleHelper;
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
    throw new RuntimeException("not implemented");
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
