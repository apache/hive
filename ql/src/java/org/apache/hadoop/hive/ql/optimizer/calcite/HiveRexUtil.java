/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


public class HiveRexUtil {

  /**
   * Simplifies a boolean expression.
   *
   * <p>In particular:</p>
   * <ul>
   * <li>{@code simplify(x = 1 AND y = 2 AND NOT x = 1)}
   * returns {@code y = 2}</li>
   * <li>{@code simplify(x = 1 AND FALSE)}
   * returns {@code FALSE}</li>
   * </ul>
   */
  public static RexNode simplify(RexBuilder rexBuilder, RexNode e) {
    switch (e.getKind()) {
    case AND:
      return simplifyAnd(rexBuilder, (RexCall) e);
    case OR:
      return simplifyOr(rexBuilder, (RexCall) e);
    case CASE:
      return simplifyCase(rexBuilder, (RexCall) e);
    case IS_NULL:
      return ((RexCall) e).getOperands().get(0).getType().isNullable()
          ? e : rexBuilder.makeLiteral(false);
    case IS_NOT_NULL:
      return ((RexCall) e).getOperands().get(0).getType().isNullable()
          ? e : rexBuilder.makeLiteral(true);
    default:
      return e;
    }
  }

  private static RexNode simplifyCase(RexBuilder rexBuilder, RexCall call) {
    final List<RexNode> operands = call.getOperands();
    final List<RexNode> newOperands = new ArrayList<>();
    for (int i = 0; i < operands.size(); i++) {
      RexNode operand = operands.get(i);
      if (RexUtil.isCasePredicate(call, i)) {
        if (operand.isAlwaysTrue()) {
          // Predicate is always TRUE. Make value the ELSE and quit.
          newOperands.add(operands.get(i + 1));
          break;
        }
        if (operand.isAlwaysFalse()) {
          // Predicate is always FALSE. Skip predicate and value.
          ++i;
          continue;
        }
      }
      newOperands.add(operand);
    }
    assert newOperands.size() % 2 == 1;
    switch (newOperands.size()) {
    case 1:
      return rexBuilder.makeCast(call.getType(), newOperands.get(0));
    }
  trueFalse:
    if (call.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
      // Optimize CASE where every branch returns constant true or constant
      // false:
      //   CASE
      //   WHEN p1 THEN TRUE
      //   WHEN p2 THEN FALSE
      //   WHEN p3 THEN TRUE
      //   ELSE FALSE
      //   END
      final List<Pair<RexNode, RexNode>> pairs =
          casePairs(rexBuilder, newOperands);
      for (Ord<Pair<RexNode, RexNode>> pair : Ord.zip(pairs)) {
        if (!pair.e.getValue().isAlwaysTrue()
            && !pair.e.getValue().isAlwaysFalse()) {
          break trueFalse;
        }
      }
      final List<RexNode> terms = new ArrayList<>();
      final List<RexNode> notTerms = new ArrayList<>();
      for (Ord<Pair<RexNode, RexNode>> pair : Ord.zip(pairs)) {
        if (pair.e.getValue().isAlwaysTrue()) {
          terms.add(RexUtil.andNot(rexBuilder, pair.e.getKey(), notTerms));
        } else {
          notTerms.add(pair.e.getKey());
        }
      }
      return RexUtil.composeDisjunction(rexBuilder, terms, false);
    }
    if (newOperands.equals(operands)) {
      return call;
    }
    return call.clone(call.getType(), newOperands);
  }

  /** Given "CASE WHEN p1 THEN v1 ... ELSE e END"
   * returns [(p1, v1), ..., (true, e)]. */
  private static List<Pair<RexNode, RexNode>> casePairs(RexBuilder rexBuilder,
      List<RexNode> operands) {
    final ImmutableList.Builder<Pair<RexNode, RexNode>> builder =
        ImmutableList.builder();
    for (int i = 0; i < operands.size() - 1; i += 2) {
      builder.add(Pair.of(operands.get(i), operands.get(i + 1)));
    }
    builder.add(
        Pair.of((RexNode) rexBuilder.makeLiteral(true), Util.last(operands)));
    return builder.build();
  }

  public static RexNode simplifyAnd(RexBuilder rexBuilder, RexCall e) {
    final List<RexNode> terms = RelOptUtil.conjunctions(e);
    final List<RexNode> notTerms = new ArrayList<>();
    final List<RexNode> negatedTerms = new ArrayList<>();
    final List<RexNode> nullOperands = new ArrayList<>();
    final List<RexNode> notNullOperands = new ArrayList<>();
    final Set<RexNode> comparedOperands = new HashSet<>();
    for (int i = 0; i < terms.size(); i++) {
      final RexNode term = terms.get(i);
      if (!HiveCalciteUtil.isDeterministic(term)) {
        continue;
      }
      switch (term.getKind()) {
      case NOT:
        notTerms.add(
            ((RexCall) term).getOperands().get(0));
        terms.remove(i);
        --i;
        break;
      case LITERAL:
        if (!RexLiteral.booleanValue(term)) {
          return term; // false
        } else {
          terms.remove(i);
          --i;
        }
        break;
      case EQUALS:
      case NOT_EQUALS:
      case LESS_THAN:
      case GREATER_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN_OR_EQUAL:
        RexCall call = (RexCall) term;
        RexNode left = call.getOperands().get(0);
        comparedOperands.add(left);
        // if it is a cast, we include the inner reference
        if (left.getKind() == SqlKind.CAST) {
          RexCall leftCast = (RexCall) left;
          comparedOperands.add(leftCast.getOperands().get(0));
        }
        RexNode right = call.getOperands().get(1);
        comparedOperands.add(right);
        // if it is a cast, we include the inner reference
        if (right.getKind() == SqlKind.CAST) {
          RexCall rightCast = (RexCall) right;
          comparedOperands.add(rightCast.getOperands().get(0));
        }
        // Assume we have the expression a > 5.
        // Then we can derive the negated term: NOT(a <= 5).
        // But as the comparison is string based and thus operands order dependent,
        // we should also add the inverted negated term: NOT(5 >= a).
        // Observe that for creating the inverted term we invert the list of operands.
        RexCall negatedTerm = negate(rexBuilder, call);
        if (negatedTerm != null) {
          negatedTerms.add(negatedTerm);
          RexCall invertNegatedTerm = invert(rexBuilder, negatedTerm);
          if (invertNegatedTerm != null) {
            negatedTerms.add(invertNegatedTerm);
          }
        }
        break;
      case IN:
        comparedOperands.add(((RexCall) term).operands.get(0));
        break;
      case BETWEEN:
        comparedOperands.add(((RexCall) term).operands.get(1));
        break;
      case IS_NOT_NULL:
        notNullOperands.add(
                ((RexCall) term).getOperands().get(0));
        terms.remove(i);
        --i;
        break;
      case IS_NULL:
        nullOperands.add(
                ((RexCall) term).getOperands().get(0));
      }
    }
    if (terms.isEmpty() && notTerms.isEmpty() && notNullOperands.isEmpty()) {
      return rexBuilder.makeLiteral(true);
    }
    // If one column should be null and is in a comparison predicate,
    // it is not satisfiable.
    // Example. IS NULL(x) AND x < 5  - not satisfiable
    if (!Collections.disjoint(nullOperands, comparedOperands)) {
      return rexBuilder.makeLiteral(false);
    }
    // Remove not necessary IS NOT NULL expressions.
    //
    // Example. IS NOT NULL(x) AND x < 5  : x < 5
    for (RexNode operand : notNullOperands) {
      if (!comparedOperands.contains(operand)) {
        terms.add(
            rexBuilder.makeCall(
                SqlStdOperatorTable.IS_NOT_NULL, operand));
      }
    }
    // If one of the not-disjunctions is a disjunction that is wholly
    // contained in the disjunctions list, the expression is not
    // satisfiable.
    //
    // Example #1. x AND y AND z AND NOT (x AND y)  - not satisfiable
    // Example #2. x AND y AND NOT (x AND y)        - not satisfiable
    // Example #3. x AND y AND NOT (x AND y AND z)  - may be satisfiable
    final Set<String> termsSet = new HashSet<String>(
            Lists.transform(terms, HiveCalciteUtil.REX_STR_FN));
    for (RexNode notDisjunction : notTerms) {
      final Set<String> notSet = new HashSet<String>(
              Lists.transform(RelOptUtil.conjunctions(notDisjunction), HiveCalciteUtil.REX_STR_FN));
      if (termsSet.containsAll(notSet)) {
        return rexBuilder.makeLiteral(false);
      }
    }
    // Add the NOT disjunctions back in.
    for (RexNode notDisjunction : notTerms) {
      terms.add(
          rexBuilder.makeCall(
              SqlStdOperatorTable.NOT, notDisjunction));
    }
    // The negated terms
    for (RexNode notDisjunction : negatedTerms) {
      final Set<String> notSet = new HashSet<String>(
              Lists.transform(RelOptUtil.conjunctions(notDisjunction), HiveCalciteUtil.REX_STR_FN));
      if (termsSet.containsAll(notSet)) {
        return rexBuilder.makeLiteral(false);
      }
    }
    return RexUtil.composeConjunction(rexBuilder, terms, false);
  }

  /** Simplifies OR(x, x) into x, and similar. */
  public static RexNode simplifyOr(RexBuilder rexBuilder, RexCall call) {
    assert call.getKind() == SqlKind.OR;
    final List<RexNode> terms = RelOptUtil.disjunctions(call);
    for (int i = 0; i < terms.size(); i++) {
      final RexNode term = terms.get(i);
      switch (term.getKind()) {
      case LITERAL:
        if (RexLiteral.booleanValue(term)) {
          return term; // true
        } else {
          terms.remove(i);
          --i;
        }
      }
    }
    return RexUtil.composeDisjunction(rexBuilder, terms, false);
  }

  private static RexCall negate(RexBuilder rexBuilder, RexCall call) {
    switch (call.getKind()) {
      case EQUALS:
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS, call.getOperands());
      case NOT_EQUALS:
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, call.getOperands());
      case LESS_THAN:
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, call.getOperands());
      case GREATER_THAN:
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, call.getOperands());
      case LESS_THAN_OR_EQUAL:
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, call.getOperands());
      case GREATER_THAN_OR_EQUAL:
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, call.getOperands());
    }
    return null;
  }

  private static RexCall invert(RexBuilder rexBuilder, RexCall call) {
    switch (call.getKind()) {
      case LESS_THAN:
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
                Lists.reverse(call.getOperands()));
      case GREATER_THAN:
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
                Lists.reverse(call.getOperands()));
      case LESS_THAN_OR_EQUAL:
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                Lists.reverse(call.getOperands()));
      case GREATER_THAN_OR_EQUAL:
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                Lists.reverse(call.getOperands()));
    }
    return null;
  }
}
