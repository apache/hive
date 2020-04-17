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

package org.apache.hadoop.hive.ql.plan.impala.expr;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.FunctionParams;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

import java.util.List;

/**
 * A FunctionCallExpr that has most of the analysis done by Calcite.
 * A FunctionCallExpr that is already marked as analyzed.
 */
public class ImpalaFunctionCallExpr extends FunctionCallExpr {

  private final float addedCost;

  // c'tor that takes an explicit FunctionParams argument
  public ImpalaFunctionCallExpr(Analyzer analyzer, Function fn, FunctionParams funcParams,
      RexCall rexCall, Type retType) throws HiveException {
    super(fn.getFunctionName(), funcParams);
    this.addedCost = getFunctionCallCost(rexCall);
    init(analyzer, fn, rexCall, retType);
  }

  // c'tor that takes a list of Exprs that eventually get converted to FunctionParams
  public ImpalaFunctionCallExpr(Analyzer analyzer, Function fn, List<Expr> params,
      RexCall rexCall, Type retType) throws HiveException {
    super(fn.getFunctionName(), params);
    this.addedCost = getFunctionCallCost(rexCall);
    init(analyzer, fn, rexCall, retType);
  }

  private void init(Analyzer analyzer, Function fn, RexCall rexCall,
      Type retType) throws HiveException {
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
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
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

  /**
   * We need to override resetAnalysisState so that Impala Analyzer doesn't
   * attempt to reanalyze this.
   */
  @Override
  public void resetAnalysisState() {
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
}
