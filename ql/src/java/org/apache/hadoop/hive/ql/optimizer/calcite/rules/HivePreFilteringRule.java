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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories.FilterFactory;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class HivePreFilteringRule extends RelOptRule {

  protected static final Logger LOG = LoggerFactory.getLogger(HivePreFilteringRule.class);

  private final FilterFactory filterFactory;

  // Max number of nodes when converting to CNF
  private final int maxCNFNodeCount;

  public HivePreFilteringRule(int maxCNFNodeCount) {
    super(operand(Filter.class, operand(RelNode.class, any())));
    this.filterFactory = HiveRelFactories.HIVE_FILTER_FACTORY;
    this.maxCNFNodeCount = maxCNFNodeCount;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final RelNode filterChild = call.rel(1);

    // If the filter is already on top of a TableScan,
    // we can bail out
    if (filterChild instanceof TableScan) {
      return false;
    }

    HiveRulesRegistry registry = call.getPlanner().getContext().unwrap(HiveRulesRegistry.class);

    // If this operator has been visited already by the rule,
    // we do not need to apply the optimization
    if (registry != null && registry.getVisited(this).contains(filter)) {
      return false;
    }

    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);

    // 0. Register that we have visited this operator in this rule
    HiveRulesRegistry registry = call.getPlanner().getContext().unwrap(HiveRulesRegistry.class);
    if (registry != null) {
      registry.registerVisited(this, filter);
    }

    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();

    // 1. Recompose filter possibly by pulling out common elements from DNF
    // expressions
    RexNode topFilterCondition = RexUtil.pullFactors(rexBuilder, filter.getCondition());

    // 2. We extract possible candidates to be pushed down
    List<RexNode> operandsToPushDown = new ArrayList<>();
    List<RexNode> deterministicExprs = new ArrayList<>();
    List<RexNode> nonDeterministicExprs = new ArrayList<>();

    switch (topFilterCondition.getKind()) {
    case AND:
      ImmutableList<RexNode> operands = RexUtil.flattenAnd(((RexCall) topFilterCondition)
          .getOperands());
      Set<String> operandsToPushDownDigest = new HashSet<String>();
      List<RexNode> extractedCommonOperands = null;

      for (RexNode operand : operands) {
        if (operand.getKind() == SqlKind.OR) {
          extractedCommonOperands = extractCommonOperands(rexBuilder, filter.getInput(), operand, maxCNFNodeCount);
          for (RexNode extractedExpr : extractedCommonOperands) {
            if (operandsToPushDownDigest.add(extractedExpr.toString())) {
              operandsToPushDown.add(extractedExpr);
            }
          }
        }

        // TODO: Make expr traversal recursive. Extend to traverse inside
        // elements of DNF/CNF & extract more deterministic pieces out.
        if (HiveCalciteUtil.isDeterministic(operand)) {
          deterministicExprs.add(operand);
        } else {
          nonDeterministicExprs.add(operand);
        }
      }

      // Pull out Deterministic exprs from non-deterministic and push down
      // deterministic expressions as a separate filter
      // NOTE: Hive by convention doesn't pushdown non deterministic expressions
      if (nonDeterministicExprs.size() > 0) {
        for (RexNode expr : deterministicExprs) {
          if (!operandsToPushDownDigest.contains(expr.toString())) {
            operandsToPushDown.add(expr);
            operandsToPushDownDigest.add(expr.toString());
          }
        }

        topFilterCondition = RexUtil.pullFactors(rexBuilder,
            RexUtil.composeConjunction(rexBuilder, nonDeterministicExprs, false));
      }

      break;

    case OR:
      operandsToPushDown = extractCommonOperands(rexBuilder, filter.getInput(), topFilterCondition, maxCNFNodeCount);
      break;
    default:
      return;
    }

    // 2. If we did not generate anything for the new predicate, we bail out
    if (operandsToPushDown.isEmpty()) {
      return;
    }

    // 3. If the new conjuncts are already present in the plan, we bail out
    final List<RexNode> newConjuncts = HiveCalciteUtil.getPredsNotPushedAlready(filter.getInput(),
        operandsToPushDown);
    RexNode newPredicate = RexUtil.composeConjunction(rexBuilder, newConjuncts, false);
    if (newPredicate.isAlwaysTrue()) {
      return;
    }

    // 4. Otherwise, we create a new condition
    final RexNode newChildFilterCondition = RexUtil.pullFactors(rexBuilder, newPredicate);

    // 5. We create the new filter that might be pushed down
    RelNode newChildFilter = filterFactory.createFilter(filter.getInput(), newChildFilterCondition);
    RelNode newTopFilter = filterFactory.createFilter(newChildFilter, topFilterCondition);

    // 6. We register both so we do not fire the rule on them again
    if (registry != null) {
      registry.registerVisited(this, newChildFilter);
      registry.registerVisited(this, newTopFilter);
    }

    call.transformTo(newTopFilter);

  }

  private static List<RexNode> extractCommonOperands(RexBuilder rexBuilder, RelNode input,
      RexNode condition, int maxCNFNodeCount) {
    assert condition.getKind() == SqlKind.OR;
    Multimap<String, RexNode> reductionCondition = LinkedHashMultimap.create();

    // Data structure to control whether a certain reference is present in every
    // operand
    Set<String> refsInAllOperands = null;

    // 1. We extract the information necessary to create the predicate for the
    // new filter; currently we support comparison functions, in and between
    ImmutableList<RexNode> operands = RexUtil.flattenOr(((RexCall) condition).getOperands());
    for (int i = 0; i < operands.size(); i++) {
      final RexNode operand = operands.get(i);

      final RexNode operandCNF = RexUtil.toCnf(rexBuilder, maxCNFNodeCount, operand);
      final List<RexNode> conjunctions = RelOptUtil.conjunctions(operandCNF);

      Set<String> refsInCurrentOperand = Sets.newHashSet();
      for (RexNode conjunction : conjunctions) {
        // We do not know what it is, we bail out for safety
        if (!(conjunction instanceof RexCall) || !HiveCalciteUtil.isDeterministic(conjunction)) {
          return new ArrayList<>();
        }
        RexCall conjCall = (RexCall) conjunction;
        Set<Integer> refs = HiveCalciteUtil.getInputRefs(conjCall);
        if (refs.size() != 1) {
          // We do not know what it is, we bail out for safety
          return new ArrayList<>();
        }
        RexNode ref = rexBuilder.makeInputRef(input, refs.iterator().next());
        String stringRef = ref.toString();
        reductionCondition.put(stringRef, conjCall);
        refsInCurrentOperand.add(stringRef);
      }

      // Updates the references that are present in every operand up till now
      if (i == 0) {
        refsInAllOperands = refsInCurrentOperand;
      } else {
        refsInAllOperands = Sets.intersection(refsInAllOperands, refsInCurrentOperand);
      }
      // If we did not add any factor or there are no common factors, we can
      // bail out
      if (refsInAllOperands.isEmpty()) {
        return new ArrayList<>();
      }
    }

    // 2. We gather the common factors and return them
    List<RexNode> commonOperands = new ArrayList<>();
    for (String ref : refsInAllOperands) {
      commonOperands
          .add(RexUtil.composeDisjunction(rexBuilder, reductionCondition.get(ref), false));
    }
    return commonOperands;
  }

}
