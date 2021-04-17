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

import java.util.List;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.impala.node.ImpalaRelUtil;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.analysis.AnalyticExpr;
import org.apache.impala.analysis.AnalyticWindow;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.FunctionParams;
import org.apache.impala.analysis.OrderByElement;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

/**
 * An ImpalaAnalyticExpr that has most of the analysis done by Calcite.
 */
public class ImpalaAnalyticExpr extends AnalyticExpr {
  private final Analyzer analyzer;

  public ImpalaAnalyticExpr(Analyzer analyzer, FunctionCallExpr fnCall,
      List<Expr> partitionExprs, List<OrderByElement> orderByElements, AnalyticWindow window)
      throws HiveException {
    super(fnCall, partitionExprs, orderByElements, window);
    try {
      this.type_ = fnCall.getReturnType();
      this.analyzer = analyzer;
      this.analyze(analyzer);
    } catch (AnalysisException e) {
      throw new HiveException("Exception in ImpalaAnalyticExpr instantiation", e);
    }
  }

  public ImpalaAnalyticExpr(ImpalaAnalyticExpr other) {
    super(other);
    this.type_ = getFnCall().getReturnType();
    this.analyzer = other.analyzer;
  }

  @Override
  public Expr clone() {
    return new ImpalaAnalyticExpr(this);
  }

  // Although the analyzeImpl implementation is typically not done for
  // other expr classes (since they have been through analysis in Calcite),
  // for this expr class, there are a few steps that are needed to get the
  // desired functionality.
  // TODO: CDPD-21517: Explore if window function standardization can be done
  // in Hive
  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    // Analytic functions need to be standardized into canonical forms that are
    // supported by Impala. For example, LAG(c1) is standardized to
    // LAG(c1, 1, NULL).
    // NOTE: at present we are calling this for a subset of functions to
    // avoid potential side effects for all analytic functions.
    FunctionCallExpr origFuncExpr = getFnCall();
    String fname = origFuncExpr.getFnName().getFunction().toLowerCase();
    if (isStandardizationNeeded(fname)) {
      this.standardize(analyzer);
      // If the function expr has changed, make relevant adjustments
      if (getFnCall() != origFuncExpr) {
        setChildren();
        Preconditions.checkArgument(getFnCall() instanceof ImpalaFunctionCallExpr);
        if (!isResetNeeded(getFnCall().getFnName().
            getFunction().toLowerCase())) {
          return;
        }
        // Since standardization may change the function signature, we need to find
        // the new matching function in the function registry
        try {
          ((ImpalaFunctionCallExpr) getFnCall()).resetFunction();
        } catch (HiveException e) {
          throw new AnalysisException("Encountered exception: ", e);
        }
      }
    }
  }

  // Overriding this method because a null literal may be created during the
  // standardize() call and we want it to use the ImpalaNullLiteral instead of
  // base NullLiteral. The reason is that the base NullLiteral's
  // resetAnalysisState() resets the type to NULL whereas we want any type that
  // was previously assigned to the null literal to be preserved.
  @Override
  protected Expr createNullLiteral() {
    try {
      return new ImpalaNullLiteral(analyzer, Type.NULL);
    } catch (HiveException e) {
      // Cannot throw AnalysisException here since the base method does not allow
      // it, hence throw a runtime exception
      throw new IllegalStateException("Encountered exception creating null literal: ", e);
    }
  }

  /**
   * We need to override resetAnalysisState so that Impala Analyzer keeps
   * the Expr in its analyzed state.
   */
  @Override
  protected void resetAnalysisState() {
    try {
      super.resetAnalysisState();
      this.analyze(analyzer);
    } catch (AnalysisException e) {
      throw new RuntimeException("Exception reanalyzing expression.", e);
    }
  }

  @Override
  protected FunctionCallExpr createRewrittenFunction(FunctionName funcName,
      FunctionParams funcParams) {
    Preconditions.checkArgument(getFnCall() instanceof ImpalaFunctionCallExpr);
    try {
      return ((ImpalaFunctionCallExpr) getFnCall()).
          getRewrittenFunction(funcName, funcParams);
    } catch (HiveException e) {
      // Cannot throw AnalysisException here since the base method does not allow
      // it, hence throw a runtime exception
      throw new IllegalStateException(
          "Encountered exception creating rewritten analytic expr: ", e);
    }
  }

  private boolean isStandardizationNeeded(String fname) {
    return fname.equals("lag") || fname.equals("lead") ||
        fname.equals("first_value") || fname.equals("last_value") ||
        fname.equals("row_number");
  }

  private boolean isResetNeeded(String fname) {
    return fname.equals("lag") || fname.equals("lead");
  }
}
