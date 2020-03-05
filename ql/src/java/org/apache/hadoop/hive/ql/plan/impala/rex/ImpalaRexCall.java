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

package org.apache.hadoop.hive.ql.plan.impala.rex;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaBinaryCompExpr;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaCaseExpr;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaFunctionCallExpr;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaInExpr;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaIsNullExpr;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaFunctionSignature;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaTypeConverter;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ScalarFunctionDetails;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ScalarFunctionUtil;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.CaseWhenClause;
import org.apache.impala.analysis.Expr;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;

import java.util.List;

/**
 * Static Helper class that returns Exprs for RexCall nodes.
 */
public class ImpalaRexCall {

  /*
   * Returns the Impala Expr object for ImpalaRexCall.
   */
  public static Expr getExpr(Analyzer analyzer, RexCall rexCall,
      List<Expr> params) throws HiveException {
    String funcName = rexCall.getOperator().getName().toLowerCase();
    SqlTypeName retType = rexCall.getType().getSqlTypeName();
    List<SqlTypeName> args = ImpalaTypeConverter.getSqlTypeNamesFromNodes(rexCall.getOperands());
    ImpalaFunctionSignature ifs = new ImpalaFunctionSignature(funcName, args, retType);
    ScalarFunctionDetails details = ScalarFunctionDetails.get(ifs);
    if (details == null) {
      throw new HiveException("Could not find function \"" + ifs + "\"");
    }

    Function fn = ScalarFunctionUtil.create(details);
    Preconditions.checkNotNull(fn);

    Type impalaRetType = ImpalaTypeConverter.getImpalaType(rexCall.getType());
    // TODO: CDPD-8625: replace isBinaryComparison with rexCall.isA(SqlKind.BINARY_COMPARISON) when
    // we upgrade to calcite 1.21
    if (isBinaryComparison(rexCall.getKind())) {
      return createBinaryCompExpr(analyzer, fn, params, rexCall.getOperator().getKind(), impalaRetType);
    }

    switch (rexCall.getOperator().getKind()) {
      case CASE:
        return createCaseExpr(analyzer, fn, params, impalaRetType);
      case IN:
        return createInExpr(analyzer, fn, params, false, impalaRetType);
      case NOT_IN:
        return createInExpr(analyzer, fn, params, true, impalaRetType);
      case IS_NULL:
        Preconditions.checkState(params.size() == 1);
        return new ImpalaIsNullExpr(analyzer, fn, params.get(0), false, impalaRetType);
      case IS_NOT_NULL:
        Preconditions.checkState(params.size() == 1);
        return new ImpalaIsNullExpr(analyzer, fn, params.get(0), true, impalaRetType);
    }
    return new ImpalaFunctionCallExpr(analyzer, fn, params, rexCall, impalaRetType);
  }

  private static Expr createCaseExpr(Analyzer analyzer, Function fn, List<Expr> params,
      Type retType) throws HiveException {
    List<CaseWhenClause> caseWhenClauses = Lists.newArrayList();
    Expr whenParam = null;
    // params alternate between "when" and the action expr
    for (Expr param : params) {
      if (whenParam == null) {
        whenParam = param;
      } else {
        caseWhenClauses.add(new CaseWhenClause(whenParam, param));
        whenParam = null;
      }
    }
    // Leftover 'when' param is the 'else' param, null if there is no leftover
    return new ImpalaCaseExpr(analyzer, fn, caseWhenClauses, whenParam, retType);
  }

  private static Expr createInExpr(Analyzer analyzer, Function fn, List<Expr> params,
      boolean notIn, Type retType) throws HiveException {
    List<Expr> inParams = Lists.newArrayList();
    Expr compareExpr = params.get(0);
    if (params.size() > 1) {
      inParams = params.subList(1, params.size());
    }
    return new ImpalaInExpr(analyzer, fn, compareExpr, inParams, notIn, retType);
  }

  private static Expr createBinaryCompExpr(Analyzer analyzer, Function fn, List<Expr> params,
      SqlKind sqlKind, Type retType) throws HiveException {
    assert params.size() == 2;
    BinaryPredicate.Operator op = null;
    switch (sqlKind) {
      case EQUALS:
        op = BinaryPredicate.Operator.EQ;
      break;
      case NOT_EQUALS:
        op = BinaryPredicate.Operator.NE;
      break;
      case GREATER_THAN:
        op = BinaryPredicate.Operator.GT;
      break;
      case GREATER_THAN_OR_EQUAL:
        op = BinaryPredicate.Operator.GE;
      break;
      case LESS_THAN:
        op = BinaryPredicate.Operator.LT;
      break;
      case LESS_THAN_OR_EQUAL:
        op = BinaryPredicate.Operator.LE;
      break;
      case IS_DISTINCT_FROM:
        op = BinaryPredicate.Operator.DISTINCT_FROM;
      break;
      case IS_NOT_DISTINCT_FROM:
        op = BinaryPredicate.Operator.NOT_DISTINCT;
      break;
      default:
        throw new RuntimeException("Unknown calcite op: " + sqlKind);
    }
    return new ImpalaBinaryCompExpr(analyzer, fn, op, params.get(0), params.get(1), retType);
  }

  /**
   * Impala does not have a function to support "CAST INT AS INT".  So
   * we need to remove the cast whenever we see it, at least until
   * that function exists in Impala.
   */
  public static RexNode removeRedundantCast(RexCall rexCall) {
    RexNode returnRexCall = rexCall;
    SqlTypeName s1 = rexCall.getType().getSqlTypeName();
    SqlTypeName s2 = rexCall.getOperands().get(0).getType().getSqlTypeName();
    // Impala has a CAST DECIMAL AS DECIMAL function for when the precision or scale
    // is different.
    if (rexCall.getKind() == SqlKind.CAST && s1.equals(s2) && s1 != SqlTypeName.DECIMAL) {
      returnRexCall = rexCall.getOperands().get(0);
    }
    return returnRexCall;
  }

  private static boolean isBinaryComparison(SqlKind sqlKind) {
    switch (sqlKind) {
      case EQUALS:
      case NOT_EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case IS_DISTINCT_FROM:
      case IS_NOT_DISTINCT_FROM:
        return true;
      default:
        return false;
    }
  }
}
