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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


public class HiveRexUtil {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveRexUtil.class);


  /** Converts an expression to conjunctive normal form (CNF).
   *
   * <p>The following expression is in CNF:
   *
   * <blockquote>(a OR b) AND (c OR d)</blockquote>
   *
   * <p>The following expression is not in CNF:
   *
   * <blockquote>(a AND b) OR c</blockquote>
   *
   * but can be converted to CNF:
   *
   * <blockquote>(a OR c) AND (b OR c)</blockquote>
   *
   * <p>The following expression is not in CNF:
   *
   * <blockquote>NOT (a OR NOT b)</blockquote>
   *
   * but can be converted to CNF by applying de Morgan's theorem:
   *
   * <blockquote>NOT a AND b</blockquote>
   *
   * <p>Expressions not involving AND, OR or NOT at the top level are in CNF.
   */
  public static RexNode toCnf(RexBuilder rexBuilder, RexNode rex) {
    return new CnfHelper(rexBuilder).toCnf(rex);
  }

  public static RexNode toCnf(RexBuilder rexBuilder, int maxCNFNodeCount, RexNode rex) {
    return new CnfHelper(rexBuilder, maxCNFNodeCount).toCnf(rex);
  }

  /** Helps {@link org.apache.calcite.rex.RexUtil#toCnf}. */
  private static class CnfHelper {
    final RexBuilder rexBuilder;
    int currentCount;
    final int maxNodeCount;

    private CnfHelper(RexBuilder rexBuilder) {
      this(rexBuilder, Integer.MAX_VALUE);
    }

    private CnfHelper(RexBuilder rexBuilder, int maxNodeCount) {
      this.rexBuilder = rexBuilder;
      this.maxNodeCount = maxNodeCount == -1 ? Integer.MAX_VALUE : maxNodeCount;
    }

    public RexNode toCnf(RexNode rex) {
      try {
        this.currentCount = 0;
        return toCnf2(rex);
      } catch (OverflowError e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Transformation to CNF not carried out as number of resulting nodes "
                  + "in expression is greater than the max number of nodes allowed");
        }
        Util.swallow(e, null);
        return rex;
      }
    }

    private RexNode toCnf2(RexNode rex) {
      final List<RexNode> operands;
      switch (rex.getKind()) {
      case AND:
        incrementAndCheck();
        operands = RexUtil.flattenAnd(((RexCall) rex).getOperands());
        final List<RexNode> cnfOperands = Lists.newArrayList();
        for (RexNode node : operands) {
          RexNode cnf = toCnf2(node);
          switch (cnf.getKind()) {
          case AND:
            incrementAndCheck();
            cnfOperands.addAll(((RexCall) cnf).getOperands());
            break;
          default:
            incrementAndCheck();
            cnfOperands.add(cnf);
          }
        }
        return and(cnfOperands);
      case OR:
        incrementAndCheck();
        operands = RexUtil.flattenOr(((RexCall) rex).getOperands());
        final RexNode head = operands.get(0);
        final RexNode headCnf = toCnf2(head);
        final List<RexNode> headCnfs = RelOptUtil.conjunctions(headCnf);
        final RexNode tail = or(Util.skip(operands));
        final RexNode tailCnf = toCnf2(tail);
        final List<RexNode> tailCnfs = RelOptUtil.conjunctions(tailCnf);
        final List<RexNode> list = Lists.newArrayList();
        for (RexNode h : headCnfs) {
          for (RexNode t : tailCnfs) {
            list.add(or(ImmutableList.of(h, t)));
          }
        }
        return and(list);
      case NOT:
        final RexNode arg = ((RexCall) rex).getOperands().get(0);
        switch (arg.getKind()) {
        case NOT:
          return toCnf2(((RexCall) arg).getOperands().get(0));
        case OR:
          operands = ((RexCall) arg).getOperands();
          List<RexNode> transformedDisj = new ArrayList<>();
          for (RexNode input : RexUtil.flattenOr(operands)) {
            transformedDisj.add(rexBuilder.makeCall(input.getType(), SqlStdOperatorTable.NOT,
                    ImmutableList.of(input)));
          }
          return toCnf2(and(transformedDisj));
        case AND:
          operands = ((RexCall) arg).getOperands();
          List<RexNode> transformedConj = new ArrayList<>();
          for (RexNode input : RexUtil.flattenAnd(operands)) {
            transformedConj.add(rexBuilder.makeCall(input.getType(), SqlStdOperatorTable.NOT,
                    ImmutableList.of(input)));
          }
          return toCnf2(or(transformedConj));
        default:
          incrementAndCheck();
          return rex;
        }
      default:
        incrementAndCheck();
        return rex;
      }
    }

    private RexNode and(Iterable<? extends RexNode> nodes) {
      return RexUtil.composeConjunction(rexBuilder, nodes, false);
    }

    private RexNode or(Iterable<? extends RexNode> nodes) {
      return RexUtil.composeDisjunction(rexBuilder, nodes, false);
    }

    private void incrementAndCheck() {
      this.currentCount++;
      if (this.currentCount > this.maxNodeCount) {
        throw OverflowError.INSTANCE;
      }
    }

    @SuppressWarnings("serial")
    private static class OverflowError extends ControlFlowException {

      public static final OverflowError INSTANCE = new OverflowError();

      private OverflowError() {}
    }
  }


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
    return simplify(rexBuilder, e, false);
  }

  public static RexNode simplify(RexBuilder rexBuilder, RexNode e,
          boolean unknownAsFalse) {
    switch (e.getKind()) {
    case AND:
      return simplifyAnd(rexBuilder, (RexCall) e, unknownAsFalse);
    case OR:
      return simplifyOr(rexBuilder, (RexCall) e);
    case NOT:
      return simplifyNot(rexBuilder, (RexCall) e);
    case CASE:
      return simplifyCase(rexBuilder, (RexCall) e, unknownAsFalse);
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

  private static RexNode simplifyNot(RexBuilder rexBuilder, RexCall call) {
    final RexNode a = call.getOperands().get(0);
    switch (a.getKind()) {
    case NOT:
      // NOT NOT x ==> x
      return simplify(rexBuilder, ((RexCall) a).getOperands().get(0));
    }
    final SqlKind negateKind = a.getKind().negate();
    if (a.getKind() != negateKind) {
      return simplify(rexBuilder,
          rexBuilder.makeCall(op(negateKind),
              ImmutableList.of(((RexCall) a).getOperands().get(0))));
    }
    final SqlKind negateKind2 = negate(a.getKind());
    if (a.getKind() != negateKind2) {
      return simplify(rexBuilder,
          rexBuilder.makeCall(op(negateKind2), ((RexCall) a).getOperands()));
    }
    if (a.getKind() == SqlKind.AND) {
      // NOT distributivity for AND
      final List<RexNode> newOperands = new ArrayList<>();
      for (RexNode operand : ((RexCall) a).getOperands()) {
        newOperands.add(simplify(rexBuilder,
            rexBuilder.makeCall(SqlStdOperatorTable.NOT, operand)));
      }
      return simplify(rexBuilder,
          rexBuilder.makeCall(SqlStdOperatorTable.OR, newOperands));
    }
    if (a.getKind() == SqlKind.OR) {
      // NOT distributivity for OR
      final List<RexNode> newOperands = new ArrayList<>();
      for (RexNode operand : ((RexCall) a).getOperands()) {
        newOperands.add(simplify(rexBuilder,
            rexBuilder.makeCall(SqlStdOperatorTable.NOT, operand)));
      }
      return simplify(rexBuilder,
          rexBuilder.makeCall(SqlStdOperatorTable.AND, newOperands));
    }
    return call;
  }

  private static RexNode simplifyCase(RexBuilder rexBuilder, RexCall call,
          boolean unknownAsFalse) {
    final List<RexNode> operands = call.getOperands();
    final List<RexNode> newOperands = new ArrayList<>();
    final Set<String> values = new HashSet<>();
    for (int i = 0; i < operands.size(); i++) {
      RexNode operand = operands.get(i);
      if (RexUtil.isCasePredicate(call, i)) {
        if (operand.isAlwaysTrue()) {
          // Predicate is always TRUE. Make value the ELSE and quit.
          newOperands.add(operands.get(i + 1));
          if (unknownAsFalse && RexUtil.isNull(operands.get(i + 1))) {
            values.add(rexBuilder.makeLiteral(false).toString());
          } else {
            values.add(operands.get(i + 1).toString());
          }
          break;
        } else if (operand.isAlwaysFalse() || RexUtil.isNull(operand)) {
          // Predicate is always FALSE or NULL. Skip predicate and value.
          ++i;
          continue;
        }
      } else {
        if (unknownAsFalse && RexUtil.isNull(operand)) {
          values.add(rexBuilder.makeLiteral(false).toString());
        } else {
          values.add(operand.toString());
        }
      }
      newOperands.add(operand);
    }
    assert newOperands.size() % 2 == 1;
    if (newOperands.size() == 1 || values.size() == 1) {
      return rexBuilder.makeCast(call.getType(), newOperands.get(newOperands.size() - 1));
    }
  trueFalse:
    if (call.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
      // Optimize CASE where every branch returns constant true or constant
      // false.
      final List<Pair<RexNode, RexNode>> pairs =
          casePairs(rexBuilder, newOperands);
      // 1) Possible simplification if unknown is treated as false:
      //   CASE
      //   WHEN p1 THEN TRUE
      //   WHEN p2 THEN TRUE
      //   ELSE FALSE
      //   END
      // can be rewritten to: (p1 or p2)
      if (unknownAsFalse) {
        final List<RexNode> terms = new ArrayList<>();
        int pos = 0;
        for (; pos < pairs.size(); pos++) {
          // True block
          Pair<RexNode, RexNode> pair = pairs.get(pos);
          if (!pair.getValue().isAlwaysTrue()) {
            break;
          }
          terms.add(pair.getKey());
        }
        for (; pos < pairs.size(); pos++) {
          // False block
          Pair<RexNode, RexNode> pair = pairs.get(pos);
          if (!pair.getValue().isAlwaysFalse() && !RexUtil.isNull(pair.getValue())) {
            break;
          }
        }
        if (pos == pairs.size()) {
          return RexUtil.composeDisjunction(rexBuilder, terms, false);
        }
      }
      // 2) Another simplification
      //   CASE
      //   WHEN p1 THEN TRUE
      //   WHEN p2 THEN FALSE
      //   WHEN p3 THEN TRUE
      //   ELSE FALSE
      //   END
      // if p1...pn cannot be nullable
      for (Ord<Pair<RexNode, RexNode>> pair : Ord.zip(pairs)) {
        if (pair.e.getKey().getType().isNullable()) {
          break trueFalse;
        }
        if (!pair.e.getValue().isAlwaysTrue()
            && !pair.e.getValue().isAlwaysFalse()
            && (!unknownAsFalse || !RexUtil.isNull(pair.e.getValue()))) {
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

  public static RexNode simplifyAnd(RexBuilder rexBuilder, RexCall e,
          boolean unknownAsFalse) {
    final List<RexNode> terms = new ArrayList<>();
    final List<RexNode> notTerms = new ArrayList<>();
    RelOptUtil.decomposeConjunction(e, terms, notTerms);
    if (unknownAsFalse) {
      return simplifyAnd2ForUnknownAsFalse(rexBuilder, terms, notTerms);
    }
    return simplifyAnd2(rexBuilder, terms, notTerms);
  }

  public static RexNode simplifyAnd2(RexBuilder rexBuilder,
      List<RexNode> terms, List<RexNode> notTerms) {
    for (RexNode term : terms) {
      if (term.isAlwaysFalse()) {
        return rexBuilder.makeLiteral(false);
      }
    }
    if (terms.isEmpty() && notTerms.isEmpty()) {
      return rexBuilder.makeLiteral(true);
    }
    if (terms.size() == 1 && notTerms.isEmpty()) {
      // Make sure "x OR y OR x" (a single-term conjunction) gets simplified.
      return simplify(rexBuilder, terms.get(0));
    }
    // If one of the not-disjunctions is a disjunction that is wholly
    // contained in the disjunctions list, the expression is not
    // satisfiable.
    //
    // Example #1. x AND y AND z AND NOT (x AND y)  - not satisfiable
    // Example #2. x AND y AND NOT (x AND y)        - not satisfiable
    // Example #3. x AND y AND NOT (x AND y AND z)  - may be satisfiable
    for (RexNode notDisjunction : notTerms) {
      final List<RexNode> terms2 = RelOptUtil.conjunctions(notDisjunction);
      if (terms.containsAll(terms2)) {
        return rexBuilder.makeLiteral(false);
      }
    }
    // Add the NOT disjunctions back in.
    for (RexNode notDisjunction : notTerms) {
      terms.add(
          simplify(rexBuilder,
              rexBuilder.makeCall(SqlStdOperatorTable.NOT, notDisjunction)));
    }
    return RexUtil.composeConjunction(rexBuilder, terms, false);
  }

  /** As {@link #simplifyAnd2(RexBuilder, List, List)} but we assume that if the expression returns
   * UNKNOWN it will be interpreted as FALSE. */
  public static RexNode simplifyAnd2ForUnknownAsFalse(RexBuilder rexBuilder,
      List<RexNode> terms, List<RexNode> notTerms) {
    for (RexNode term : terms) {
      if (term.isAlwaysFalse()) {
        return rexBuilder.makeLiteral(false);
      }
    }
    if (terms.isEmpty() && notTerms.isEmpty()) {
      return rexBuilder.makeLiteral(true);
    }
    if (terms.size() == 1 && notTerms.isEmpty()) {
      // Make sure "x OR y OR x" (a single-term conjunction) gets simplified.
      return simplify(rexBuilder, terms.get(0), true);
    }
    // Try to simplify the expression
    final Set<String> negatedTerms = new HashSet<>();
    final Set<String> nullOperands = new HashSet<>();
    final Set<RexNode> notNullOperands = new LinkedHashSet<>();
    final Set<String> comparedOperands = new HashSet<>();
    for (int i = 0; i < terms.size(); i++) {
      final RexNode term = terms.get(i);
      if (!HiveCalciteUtil.isDeterministic(term)) {
        continue;
      }
      switch (term.getKind()) {
      case EQUALS:
      case NOT_EQUALS:
      case LESS_THAN:
      case GREATER_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN_OR_EQUAL:
        RexCall call = (RexCall) term;
        RexNode left = call.getOperands().get(0);
        comparedOperands.add(left.toString());
        // if it is a cast, we include the inner reference
        if (left.getKind() == SqlKind.CAST) {
          RexCall leftCast = (RexCall) left;
          comparedOperands.add(leftCast.getOperands().get(0).toString());
        }
        RexNode right = call.getOperands().get(1);
        comparedOperands.add(right.toString());
        // if it is a cast, we include the inner reference
        if (right.getKind() == SqlKind.CAST) {
          RexCall rightCast = (RexCall) right;
          comparedOperands.add(rightCast.getOperands().get(0).toString());
        }
        // Assume the expression a > 5 is part of a Filter condition.
        // Then we can derive the negated term: a <= 5.
        // But as the comparison is string based and thus operands order dependent,
        // we should also add the inverted negated term: 5 >= a.
        // Observe that for creating the inverted term we invert the list of operands.
        RexNode negatedTerm = negate(rexBuilder, call);
        if (negatedTerm != null) {
          negatedTerms.add(negatedTerm.toString());
          RexNode invertNegatedTerm = invert(rexBuilder, (RexCall) negatedTerm);
          if (invertNegatedTerm != null) {
            negatedTerms.add(invertNegatedTerm.toString());
          }
        }
        break;
      case IN:
        comparedOperands.add(((RexCall) term).operands.get(0).toString());
        break;
      case BETWEEN:
        comparedOperands.add(((RexCall) term).operands.get(1).toString());
        break;
      case IS_NOT_NULL:
        notNullOperands.add(((RexCall) term).getOperands().get(0));
        terms.remove(i);
        --i;
        break;
      case IS_NULL:
        nullOperands.add(((RexCall) term).getOperands().get(0).toString());
      }
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
      if (!comparedOperands.contains(operand.toString())) {
        terms.add(
            rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, operand));
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
      if (!HiveCalciteUtil.isDeterministic(notDisjunction)) {
        continue;
      }
      final List<String> terms2Set = Lists.transform(
              RelOptUtil.conjunctions(notDisjunction), HiveCalciteUtil.REX_STR_FN);
      if (termsSet.containsAll(terms2Set)) {
        return rexBuilder.makeLiteral(false);
      }
    }
    // Add the NOT disjunctions back in.
    for (RexNode notDisjunction : notTerms) {
      terms.add(
          simplify(rexBuilder,
              rexBuilder.makeCall(SqlStdOperatorTable.NOT, notDisjunction), true));
    }
    // The negated terms: only deterministic expressions
    for (String negatedTerm : negatedTerms) {
      if (termsSet.contains(negatedTerm)) {
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
        if (!RexLiteral.isNullLiteral(term)) {
          if (RexLiteral.booleanValue(term)) {
            return term; // true
          } else {
            terms.remove(i);
            --i;
          }
        }
      }
    }
    return RexUtil.composeDisjunction(rexBuilder, terms, false);
  }

  private static RexCall negate(RexBuilder rexBuilder, RexCall call) {
    switch (call.getKind()) {
      case EQUALS:
      case NOT_EQUALS:
      case LESS_THAN:
      case GREATER_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN_OR_EQUAL:
        return (RexCall) rexBuilder.makeCall(op(negate(call.getKind())), call.getOperands());
    }
    return null;
  }

  private static SqlKind negate(SqlKind kind) {
    switch (kind) {
      case EQUALS:
        return SqlKind.NOT_EQUALS;
      case NOT_EQUALS:
        return SqlKind.EQUALS;
      case LESS_THAN:
        return SqlKind.GREATER_THAN_OR_EQUAL;
      case GREATER_THAN:
        return SqlKind.LESS_THAN_OR_EQUAL;
      case LESS_THAN_OR_EQUAL:
        return SqlKind.GREATER_THAN;
      case GREATER_THAN_OR_EQUAL:
        return SqlKind.LESS_THAN;
    }
    return kind;
  }

  private static RexCall invert(RexBuilder rexBuilder, RexCall call) {
    switch (call.getKind()) {
      case EQUALS:
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                Lists.reverse(call.getOperands()));
      case NOT_EQUALS:
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS,
                Lists.reverse(call.getOperands()));
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

  private static SqlOperator op(SqlKind kind) {
    switch (kind) {
    case IS_FALSE:
      return SqlStdOperatorTable.IS_FALSE;
    case IS_TRUE:
      return SqlStdOperatorTable.IS_TRUE;
    case IS_UNKNOWN:
      return SqlStdOperatorTable.IS_UNKNOWN;
    case IS_NULL:
      return SqlStdOperatorTable.IS_NULL;
    case IS_NOT_FALSE:
      return SqlStdOperatorTable.IS_NOT_FALSE;
    case IS_NOT_TRUE:
      return SqlStdOperatorTable.IS_NOT_TRUE;
    case IS_NOT_NULL:
      return SqlStdOperatorTable.IS_NOT_NULL;
    case EQUALS:
      return SqlStdOperatorTable.EQUALS;
    case NOT_EQUALS:
      return SqlStdOperatorTable.NOT_EQUALS;
    case LESS_THAN:
      return SqlStdOperatorTable.LESS_THAN;
    case GREATER_THAN:
      return SqlStdOperatorTable.GREATER_THAN;
    case LESS_THAN_OR_EQUAL:
      return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
    case GREATER_THAN_OR_EQUAL:
      return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
    default:
      throw new AssertionError(kind);
    }
  }

  public static SqlKind invert(SqlKind kind) {
    switch (kind) {
      case EQUALS:
        return SqlKind.EQUALS;
      case NOT_EQUALS:
        return SqlKind.NOT_EQUALS;
      case LESS_THAN:
        return SqlKind.GREATER_THAN;
      case GREATER_THAN:
        return SqlKind.LESS_THAN;
      case LESS_THAN_OR_EQUAL:
        return SqlKind.GREATER_THAN_OR_EQUAL;
      case GREATER_THAN_OR_EQUAL:
        return SqlKind.LESS_THAN_OR_EQUAL;
    }
    return null;
  }

  public static class ExprSimplifier extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final boolean unknownAsFalse;
    private final Map<RexNode,Boolean> unknownAsFalseMap;

    public ExprSimplifier(RexBuilder rexBuilder, boolean unknownAsFalse) {
      this.rexBuilder = rexBuilder;
      this.unknownAsFalse = unknownAsFalse;
      this.unknownAsFalseMap = new HashMap<>();
    }

    @Override
    public RexNode visitCall(RexCall call) {
      Boolean unknownAsFalseCall = unknownAsFalse;
      if (unknownAsFalseCall) {
        switch (call.getKind()) {
        case AND:
        case CASE:
          unknownAsFalseCall = this.unknownAsFalseMap.get(call);
          if (unknownAsFalseCall == null) {
            // Top operator
            unknownAsFalseCall = true;
          }
          break;
        default:
          unknownAsFalseCall = false;
        }
        for (RexNode operand : call.operands) {
          this.unknownAsFalseMap.put(operand, unknownAsFalseCall);
        }
      }
      RexNode node = super.visitCall(call);
      RexNode simplifiedNode = HiveRexUtil.simplify(rexBuilder, node, unknownAsFalseCall);
      if (node == simplifiedNode) {
        return node;
      }
      if (simplifiedNode.getType().equals(call.getType())) {
        return simplifiedNode;
      }
      return rexBuilder.makeCast(call.getType(), simplifiedNode, true);
    }
  }

}
