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

package org.apache.hadoop.hive.impala.rex;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.impala.expr.ImpalaBinaryCompExpr;
import org.apache.hadoop.hive.impala.expr.ImpalaCaseExpr;
import org.apache.hadoop.hive.impala.expr.ImpalaCastExpr;
import org.apache.hadoop.hive.impala.expr.ImpalaCompoundExpr;
import org.apache.hadoop.hive.impala.expr.ImpalaFunctionCallExpr;
import org.apache.hadoop.hive.impala.expr.ImpalaInExpr;
import org.apache.hadoop.hive.impala.expr.ImpalaIsNullExpr;
import org.apache.hadoop.hive.impala.expr.ImpalaTimestampArithmeticExpr;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaFunctionUtil;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaTypeConverter;
import org.apache.hadoop.hive.impala.funcmapper.ScalarFunctionDetails;
import org.apache.hadoop.hive.impala.funcmapper.TimeIntervalOpFunctionResolver;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.CaseWhenClause;
import org.apache.impala.analysis.CastExpr;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;

/**
 * Static Helper class that returns Exprs for RexCall nodes.
 */
public class ImpalaRexCall {
  private static final Logger LOG = LoggerFactory.getLogger(ImpalaRexCall.class);

  /*
   * Returns the Impala Expr object for ImpalaRexCall.
   */
  public static Expr getExpr(Analyzer analyzer, RexCall rexCall,
      List<Expr> params, RexBuilder rexBuilder) throws HiveException {

    Type calciteReturnType = ImpalaTypeConverter.createImpalaType(rexCall.getType());

    if (rexCall.getOperator() == SqlStdOperatorTable.GROUPING) {
      return processGroupingFunction(rexCall, params, calciteReturnType, analyzer);
    }

    String funcName = rexCall.getOperator().getName().toLowerCase();
    List<RexNode> operands = rexCall.getOperands();

    Function fn = getFunction(funcName, operands, rexCall.getType(), analyzer);
    Type impalaRetType = ImpalaTypeConverter.createImpalaType(fn.getReturnType(),
        rexCall.getType().getPrecision(), rexCall.getType().getScale());

    // TODO: CDPD-8625: replace isBinaryComparison with rexCall.isA(SqlKind.BINARY_COMPARISON) when
    // we upgrade to calcite 1.21
    if (isBinaryComparison(rexCall.getKind())) {
      return createBinaryCompExpr(analyzer, fn, params, rexCall.getOperator().getKind(), impalaRetType);
    }

    Expr retExpr;
    switch (rexCall.getOperator().getKind()) {
      case OR:
        retExpr = createCompoundExpr(analyzer, CompoundPredicate.Operator.OR, fn, params, impalaRetType);
        break;
      case AND:
        retExpr = createCompoundExpr(analyzer, CompoundPredicate.Operator.AND, fn, params, impalaRetType);
        break;
      case CASE:
        retExpr = createCaseExpr(analyzer, fn, params, impalaRetType);
        break;
      case IN:
        retExpr = createInExpr(analyzer, fn, params, false, impalaRetType);
        break;
      case NOT_IN:
        retExpr = createInExpr(analyzer, fn, params, true, impalaRetType);
        break;
      case IS_NULL:
        Preconditions.checkState(params.size() == 1);
        retExpr = new ImpalaIsNullExpr(analyzer, fn, params.get(0), false, impalaRetType);
        break;
      case IS_NOT_NULL:
        Preconditions.checkState(params.size() == 1);
        retExpr = new ImpalaIsNullExpr(analyzer, fn, params.get(0), true, impalaRetType);
        break;
      case EXTRACT:
        // for specific extract functions (e.g.year, month), there will be two parameters, and
        // we need to ignore the first redundant one. As an example, the parameter in the explain
        // cbo plan will show up like "MINUTE(FLAG(MINUTE), $10)" for the "extract minute" function.
        if (params.size() == 2) {
          retExpr = new ImpalaFunctionCallExpr(analyzer, fn, params.subList(1,2), rexCall, impalaRetType);
        } else {
        // CDPD-8867: normal 'extract function currently uses default ImpalaFunctionCallExpr
          retExpr = new ImpalaFunctionCallExpr(analyzer, fn, params, rexCall, impalaRetType);
        }
        break;
      case PLUS:
      case MINUS:
        if (TimeIntervalOpFunctionResolver.isTimestampArithExpr(fn.getName())) {
          retExpr = createTimestampArithExpr(analyzer, fn, params, rexCall, impalaRetType);
        } else {
          retExpr = new ImpalaFunctionCallExpr(analyzer, fn, params, rexCall, impalaRetType);
        }
        break;
      case BETWEEN:
        retExpr = createBetweenExpr(analyzer, rexCall, params);
        break;
      default:
        retExpr = new ImpalaFunctionCallExpr(analyzer, fn, params, rexCall, impalaRetType);
        break;
    }

    // If there is a mismatch between the return type of the Impala function and
    // the expected return type from Calcite, we need to cast so that the rest of
    // the Calcite expressions get the right type. This can happen for Impala functions
    // that only allow STRING types but Calcite recognizes the datatype as CHAR or
    // VARCHAR.
    if (!calciteReturnType.matchesType(impalaRetType)) {
      retExpr = new CastExpr(calciteReturnType, retExpr);
    }
    return retExpr;
  }

  /**
   * Create an Impala Expr corresponding to the grouping() function. Impala projects the
   * grouping_id field, which we leverage here for the grouping() function.
   * Suppose original RexCall in the CBO plan is:
   *    grouping($3, N)  where N = #bits to right shift
   * Converted Impala function:
   *    cast(bitand(shiftright($3, N), 1), Type.BIGINT)
   */
  private static Expr processGroupingFunction(RexCall rexCall, List<Expr> params, Type impalaRetType,
      Analyzer analyzer) throws HiveException {
    List<RexNode> operands = rexCall.getOperands();
    Preconditions.checkState(operands.size() == 2,
        "Multi column grouping function is not supported yet");
    Preconditions.checkState(operands.get(0) instanceof RexInputRef);
    try {
      // the shiftright function in Impala only accepts INT as the second parameter, whereas
      // the Calcite function has created this as a BIGINT, so convert the second parameter
      RelDataType refType = rexCall.getOperands().get(0).getType();
      // shiftRight expr
      BigDecimal value = new BigDecimal(((NumericLiteral) params.get(1)).getIntValue());
      NumericLiteral numPositions = new NumericLiteral(value, Type.INT);
      Type impalaRefType = ImpalaTypeConverter.createImpalaType(refType);
      List<Type> shiftRightArgs = ImmutableList.of(impalaRefType, Type.INT);
      ScalarFunctionDetails shiftRightFuncDetails =
          ScalarFunctionDetails.get("shiftright", shiftRightArgs, impalaRefType);
      Preconditions.checkNotNull(shiftRightFuncDetails);
      Function shiftRightFn = ImpalaFunctionUtil.create(shiftRightFuncDetails, analyzer);
      Expr shiftRightExpr = new ImpalaFunctionCallExpr(analyzer, shiftRightFn,
          ImmutableList.of(params.get(0), numPositions), rexCall, impalaRefType);
      // bitAnd expr
      NumericLiteral mask = new NumericLiteral(new BigDecimal(1), impalaRefType);
      List<Type> bitAndArgs = ImmutableList.of(impalaRefType, impalaRefType);
      ScalarFunctionDetails bitAndFuncDetails =
          ScalarFunctionDetails.get("bitand", bitAndArgs, impalaRefType);
      Preconditions.checkNotNull(bitAndFuncDetails);
      Function bitAndFn = ImpalaFunctionUtil.create(bitAndFuncDetails, analyzer);
      Expr bitAndExpr = new ImpalaFunctionCallExpr(analyzer, bitAndFn,
          ImmutableList.of(shiftRightExpr, mask), rexCall, impalaRefType);
      // cast expr
      Expr castExpr = new CastExpr(impalaRetType, bitAndExpr);
      castExpr.analyze(analyzer);
      return castExpr;
    } catch (AnalysisException e) {
      throw new HiveException("Encountered Impala exception: ", e);
    }
  }

  /**
   * Create an expression that unflattens the Calcite expressions because Impala only supports
   * binary expressions.
   */
  private static Expr createCompoundExpr(Analyzer analyzer, CompoundPredicate.Operator op, Function fn,
      List<Expr> params, Type retType) throws HiveException {
    // must be at least 2 params for the condition
    Preconditions.checkState(params.size() >= 2);
    Expr finalCondition = null;
    Expr condition2 = params.get(params.size() - 1);
    for (int i = params.size() - 2; i >= 0; --i) {
      Expr condition1 = params.get(i);
      finalCondition = new ImpalaCompoundExpr(analyzer, fn, op, condition1, condition2, retType);
      condition2 = finalCondition;
    }
    Preconditions.checkNotNull(finalCondition);
    return finalCondition;
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

  private static Expr createTimestampArithExpr(Analyzer analyzer, Function fn, List<Expr> params,
      RexCall rexCall, Type retType) throws HiveException {
    Preconditions.checkArgument(params.size() == 2);
    int intervalArg = (params.get(0) instanceof NumericLiteral) ? 0 : 1;
    int timestampArg = (intervalArg + 1) % 2;

    String timeUnitIdent = TimeIntervalOpFunctionResolver.getTimeUnitIdent(fn.getName());

    return new ImpalaTimestampArithmeticExpr(analyzer, fn, params.get(timestampArg),
        params.get(intervalArg), timeUnitIdent, retType);
  }

  /**
   * Impala does not have a function to support "CAST INT AS INT".  So
   * we need to remove the cast whenever we see it, at least until
   * that function exists in Impala.
   */
  public static RexNode removeRedundantCast(RexCall rexCall) {
    RexNode returnRexCall = rexCall;
    if (rexCall.getKind() != SqlKind.CAST) {
      return returnRexCall;
    }
    RelDataType r1 = rexCall.getType();
    RelDataType r2 = rexCall.getOperands().get(0).getType();
    // If the datatype has the same precision and scale, there is no need for the case (Impala
    // will fail in this case).
    if (r1.getSqlTypeName() == r2.getSqlTypeName() && r1.getPrecision() == r2.getPrecision() &&
        r1.getScale() == r2.getScale()) {
      returnRexCall = rexCall.getOperands().get(0);
    }
    return returnRexCall;
  }

  /**
   * Need to convert the 'between' calcite operator into its components, since
   * Impala does not support 'between' directly. The "invert" parameter is true
   * if it is "not between'
   */
  private static Expr createBetweenExpr(Analyzer analyzer, RexCall rexCall, List<Expr> params
       ) throws HiveException {
    Preconditions.checkState(rexCall.getOperands().size() == 4);
    Preconditions.checkState(rexCall.getOperands().get(0) instanceof RexLiteral);
    RexLiteral invertLiteral = (RexLiteral) rexCall.getOperands().get(0);
    Boolean invert = invertLiteral.getValueAs(Boolean.class);
    SqlTypeName sqlRetType = SqlTypeName.BOOLEAN;

    Type retType = ImpalaTypeConverter.createImpalaType(rexCall.getType());
    List<RexNode> operands = rexCall.getOperands();
    List<RelDataType> operandTypes = RexUtil.types(operands);

    // get all parameters to create the lower expression
    SqlKind lowerKind = invert ? SqlKind.LESS_THAN : SqlKind.GREATER_THAN_OR_EQUAL;
    String lowerName = invert ? "<" : ">=";
    List<RexNode> lowerArgs = Lists.newArrayList(operands.get(1), operands.get(2));
    List<Expr> lowerParams = Lists.newArrayList(params.get(1), params.get(2));
    Function lowerFn = getFunction(lowerName, lowerArgs, rexCall.getType(), analyzer);
    Preconditions.checkNotNull(lowerFn);
    Expr lowerExpr = createBinaryCompExpr(analyzer, lowerFn, lowerParams, lowerKind, retType);

    // get all parameters to create the upper expression
    SqlKind upperKind = invert ? SqlKind.GREATER_THAN : SqlKind.LESS_THAN_OR_EQUAL;
    String upperName = invert ? ">" : "<=";
    List<RexNode> upperArgs = Lists.newArrayList(operands.get(1), operands.get(3));
    List<Expr> upperParams = Lists.newArrayList(params.get(1), params.get(3));
    Function upperFn = getFunction(upperName, upperArgs, rexCall.getType(), analyzer);
    Preconditions.checkNotNull(upperFn);
    Expr upperExpr = createBinaryCompExpr(analyzer, upperFn, upperParams, upperKind, retType);

    // create the compound expression with the lower and upper expression
    List<Expr> impalaExprList = Lists.newArrayList(lowerExpr, upperExpr);
    SqlKind fnKind = invert ? SqlKind.OR : SqlKind.AND;
    CompoundPredicate.Operator op =
        invert ? CompoundPredicate.Operator.OR : CompoundPredicate.Operator.AND;

    List<Type> fnArgs = ImmutableList.of(Type.BOOLEAN, Type.BOOLEAN);
    ScalarFunctionDetails compoundFuncDetails =
        ScalarFunctionDetails.get(fnKind.toString().toLowerCase(), fnArgs, Type.BOOLEAN);
    Preconditions.checkNotNull(compoundFuncDetails);
    Function fnCompound = ImpalaFunctionUtil.create(compoundFuncDetails, analyzer);
    return createCompoundExpr(analyzer, op, fnCompound, impalaExprList, retType);
  }

  public static boolean isBinaryComparison(SqlKind sqlKind) {
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

  // Return true if the list of params have two different types of char types
  // declared (e.g. a VARCHAR and a STRING)
  private static boolean isMultipleCharTypes(List<Expr> params) {
    Type firstCharTypeParam = null;
    for (Expr p : params) {
      Type pType = p.getType();
      if (pType.matchesType(Type.VARCHAR) || pType.matchesType(Type.CHAR)
          || pType.matchesType(Type.STRING)) {
        if (firstCharTypeParam == null) {
          firstCharTypeParam = pType;
        } else {
          if (!firstCharTypeParam.matchesType(pType)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private static List<Expr> castParamsToString(List<Expr> params, List<RexNode> operands, Analyzer analyzer) {
    List<Expr> castParams = Lists.newArrayList();
    Preconditions.checkState(params.size() == operands.size());
    for (int i = 0; i < params.size(); ++i) {
      Expr param = params.get(i);
      if (param.getType().matchesType(Type.VARCHAR) || param.getType().matchesType(Type.CHAR)) {
        try {
          Function castFn = getFunction("cast", operands.subList(i, i+1),
	      ImpalaTypeConverter.getRelDataType(Type.STRING, true), analyzer);
          ImpalaCastExpr castExpr = new ImpalaCastExpr(analyzer, castFn, Type.STRING, param);
          castParams.add(castExpr);
        } catch (Exception e) {
          throw new RuntimeException("Casting exception: " + e);
        }
      } else { 
        castParams.add(param);
      }
    }
    return castParams;
  }

  private static List<RexNode> castRexNodesToString(List<RexNode> operands, RexBuilder rexBuilder) {
    List<RexNode> castOperands = Lists.newArrayList(); 
    for (RexNode operand : operands) {
      RelDataType relDataType = ImpalaTypeConverter.getNormalizedImpalaType(operand);
      SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
      if ((sqlTypeName == SqlTypeName.CHAR) || ((sqlTypeName == SqlTypeName.VARCHAR) &&
        relDataType.getPrecision() != Integer.MAX_VALUE)) {
          castOperands.add(rexBuilder.makeCast(
              ImpalaTypeConverter.getRelDataType(Type.STRING, true), operand, true));
      } else {
        castOperands.add(operand);
      }
    }
    return castOperands;
  }


  private static Function getFunction(String name, List<RexNode> args,
      RelDataType retType, Analyzer analyzer) throws HiveException {
    List<RelDataType> argTypes = ImpalaTypeConverter.getNormalizedImpalaTypeList(args);
    ScalarFunctionDetails details = ScalarFunctionDetails.get(
        name, argTypes, retType);
    if (details == null) {
      throw new HiveException("Could not find function \"" + name + "\" in Impala "
          + "with args " + argTypes + " and return type " + retType);
    }
    return ImpalaFunctionUtil.create(details, analyzer);
  }
}
