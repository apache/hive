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

package org.apache.hadoop.hive.impala.expr;

import com.google.common.base.Preconditions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.impala.node.ImpalaRelUtil;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.FunctionParams;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

import java.util.Arrays;
import java.util.List;

/**
 * A FunctionCallExpr that has most of the analysis done by Calcite.
 * A FunctionCallExpr that is already marked as analyzed.
 */
public class ImpalaFunctionCallExpr extends FunctionCallExpr {

  private final Analyzer analyzer;

  private final float addedCost;

  // c'tor that takes an explicit FunctionParams argument
  public ImpalaFunctionCallExpr(Analyzer analyzer, Function fn, FunctionParams funcParams,
      RexCall rexCall, Type retType) throws HiveException {
    super(fn.getFunctionName(), funcParams);
    this.analyzer = analyzer;
    this.addedCost = getFunctionCallCost(rexCall);
    init(analyzer, fn, retType);
  }

  // c'tor that takes a list of Exprs that eventually get converted to FunctionParams
  public ImpalaFunctionCallExpr(Analyzer analyzer, Function fn, List<Expr> params,
      RexCall rexCall, Type retType) throws HiveException {
    super(fn.getFunctionName(), params);
    this.addedCost = getFunctionCallCost(rexCall);
    this.analyzer = analyzer;
    init(analyzer, fn, retType);
  }

  // c'tor which does not depend on Calcite's RexCall but is used when Impala's
  // FunctionParams are created or there is some modifications to it
  public ImpalaFunctionCallExpr(Analyzer analyzer, Function fn, FunctionParams funcParams,
      float addedCost, Type retType) throws HiveException {
    super(fn.getFunctionName(), funcParams);
    this.analyzer = analyzer;
    this.addedCost = addedCost;
    init(analyzer, fn, retType);
  }

  private void init(Analyzer analyzer, Function fn, Type retType) throws HiveException {
    try {
      this.fn_ = fn;
      this.type_ = retType;
      this.analyze(analyzer);
    } catch (AnalysisException e) {
      throw new HiveException("Exception in ImpalaFunctionCallExpr instantiation", e);
    }
  }

  public ImpalaFunctionCallExpr(ImpalaFunctionCallExpr other) {
    super(other);
    this.addedCost = other.addedCost;
    this.analyzer = other.analyzer;
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    // Functions have already gone through the analysis phase in Hive so the
    // analyzeImpl method is overridden.  However, the FunctionName object
    // still needs to be analyzed.  This allows Expr.toSql() to display the names
    // correctly in the explain plan.
    getFnName().analyze(analyzer);
  }

  /**
   * Compute evaluation cost.
   * This overrides the computeEvalCost in the base class.  In Impala,
   * there are a few different types of "FunctionCallExprs", but it didn't
   * really seem necessary to have multiple classes for FENG since most
   * of the differences were in analysis.  However, the "Eval cost" was also
   * different, since calling a function is worth "5" while an addition is worth "1".
   * If other methods need overriding in addition to computeEvalCost, we should
   * consider breaking things up into individuals classes again.
   */
  @Override
  protected float computeEvalCost() {
    return hasChildCosts() ? getChildCosts() + addedCost : UNKNOWN_COST;
  }

  private float getFunctionCallCost(RexCall rexCall) {
    // TODO: CDPD-8625:  replace isBinaryArithmetic with rexCall.isA(SqlKind.BINARY_ARITHMETIC) when
    // we upgrade to calcite 1.21
    if (rexCall == null || isBinaryArithmetic(rexCall.getKind())) {
      return ARITHMETIC_OP_COST;
    }
    return FUNCTION_CALL_COST;
  }

  @Override
  public Expr clone() { return new ImpalaFunctionCallExpr(this); }

  @Override
  public FunctionCallExpr cloneWithNewParams(FunctionParams params) {
    try {
      return new ImpalaFunctionCallExpr(this.analyzer, this.getFn(), params,
          this.addedCost, this.type_);
    } catch (HiveException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * We need to override resetAnalysisState so that Impala Analyzer keeps
   * the Expr in its analyzed state.
   */
  @Override
  public void resetAnalysisState() {
    try {
      // The parent FunctionCallExpr sets the fn_ to null and sets it back again
      // in analysisImpl.  Since we override analysisImpl, we save the Function here
      // before resetting and set it again.
      Function savedFunction = fn_;
      super.resetAnalysisState();
      fn_ = savedFunction;
      this.analyze(analyzer);
    } catch (AnalysisException e) {
      throw new RuntimeException("Exception reanalyzing expression.", e);
    }
  }

  private boolean isBinaryArithmetic(SqlKind sqlKind) {
    switch (sqlKind) {
      case PLUS:
      case MINUS:
      case TIMES:
      case DIVIDE:
      case MOD:
        return true;
      default:
        return false;
    }
  }

  public void resetFunction() throws HiveException, AnalysisException {
    if (fn_ instanceof AggregateFunction) {
      // since the function lookup is based on exact match of data types (unlike Impala's
      // builtin db which does allow matches based on implicit cast), we need to adjust
      // one or more operands based on the function type
      List<Type> operandTypes = Arrays.asList(collectChildReturnTypes());
      String funcName = getFnName().getFunction().toLowerCase();
      switch (funcName) {
      case "lag":
      case "lead":
        // at this point the function should have been standardized into
        // a 3 operand function and the second operand is the 'offset' which
        // should be an integer (tinyint/smallint/int/bigint) type
        Preconditions.checkArgument(operandTypes.size() == 3 &&
            operandTypes.get(1).isIntegerType());
        // upcast the second argument (offset) since it must always be BIGINT
        if (operandTypes.get(1) != Type.BIGINT) {
          operandTypes.set(1, Type.BIGINT);
          uncheckedCastChild(Type.BIGINT, 1);
        }
        // Last argument could be NULL with TYPE_NULL but since Impala BE expects
        // a concrete type, we cast it to the type of the first argument
        if (operandTypes.get(2) == Type.NULL) {
          Preconditions.checkArgument(operandTypes.get(0) != Type.NULL);
          operandTypes.set(2, operandTypes.get(0));
          uncheckedCastChild(operandTypes.get(0), 2);
        }
        fn_ = ImpalaRelUtil.getAggregateFunction(getFnName().getFunction(), getReturnType(), operandTypes);
        type_ = fn_.getReturnType();

        break;
      default:
        throw new HiveException("Unsupported aggregate function.");
      }
    }
  }
}
