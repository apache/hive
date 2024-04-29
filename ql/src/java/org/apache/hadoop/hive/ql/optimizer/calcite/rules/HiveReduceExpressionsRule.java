/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;

import java.util.ArrayList;
import java.util.List;

/**
 * Collection of planner rules that apply various simplifying transformations on
 * RexNode trees. Currently, there are two transformations:
 *
 * <ul>
 * <li>Constant reduction, which evaluates constant subtrees, replacing them
 * with a corresponding RexLiteral
 * <li>Removal of redundant casts, which occurs when the argument into the cast
 * is the same as the type of the resulting cast expression
 * </ul>
 */
public final class HiveReduceExpressionsRule {

  private HiveReduceExpressionsRule() {
    throw new IllegalStateException("Instantiation not allowed");
  }

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter}.
   */
  public static final RelOptRule FILTER_INSTANCE = new HiveFilterReduceExpressionsRule(
      (ReduceExpressionsRule.FilterReduceExpressionsRule.FilterReduceExpressionsRuleConfig)
          ReduceExpressionsRule.FilterReduceExpressionsRule.FilterReduceExpressionsRuleConfig.DEFAULT
          .withOperandFor(HiveFilter.class)
          .withMatchNullability(false)
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .as(ReduceExpressionsRule.FilterReduceExpressionsRule.Config.class)
  );

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject}.
   */
  public static final RelOptRule PROJECT_INSTANCE = new HiveProjectReduceExpressionsRule(
      (ReduceExpressionsRule.ProjectReduceExpressionsRule.ProjectReduceExpressionsRuleConfig)
          ReduceExpressionsRule.ProjectReduceExpressionsRule.ProjectReduceExpressionsRuleConfig.DEFAULT
          .withOperandFor(HiveProject.class)
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .as(ReduceExpressionsRule.ProjectReduceExpressionsRule.Config.class)
  );

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin}.
   */
  public static final RelOptRule JOIN_INSTANCE =
      ReduceExpressionsRule.JoinReduceExpressionsRule.JoinReduceExpressionsRuleConfig.DEFAULT
          .withOperandFor(HiveJoin.class)
          .withMatchNullability(false)
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .as(ReduceExpressionsRule.JoinReduceExpressionsRule.Config.class)
          .toRule();

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin}.
   */
  public static final RelOptRule SEMIJOIN_INSTANCE =
      ReduceExpressionsRule.JoinReduceExpressionsRule.JoinReduceExpressionsRuleConfig.DEFAULT
          .withOperandFor(HiveSemiJoin.class)
          .withMatchNullability(false)
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .as(ReduceExpressionsRule.JoinReduceExpressionsRule.Config.class)
          .toRule();

  public static class HiveFilterReduceExpressionsRule extends ReduceExpressionsRule.FilterReduceExpressionsRule {

    protected HiveFilterReduceExpressionsRule(FilterReduceExpressionsRuleConfig config) {
      super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      List<RexNode> expList =
          Lists.newArrayList(filter.getCondition());
      RexNode newConditionExp;

      final RelMetadataQuery mq = call.getMetadataQuery();
      final RelOptPredicateList predicates =
          mq.getPulledUpPredicates(filter.getInput());

      boolean reduced;
      if (reduceExpressions(filter, expList, predicates, true,
          config.matchNullability(), config.treatDynamicCallsAsConstant())) {

        expList = getExpanded(expList, filter.getCluster().getRexBuilder());
        assert expList.size() == 1;

        if (expList.get(0).equals(filter.getCondition())) {
          newConditionExp = filter.getCondition();
          reduced = false;
        } else {
          newConditionExp = expList.get(0);
          reduced = true;
        }
      } else {
        // No reduction, but let's still test the original
        // predicate to see if it was already a constant,
        // in which case we don't need any runtime decision
        // about filtering.
        newConditionExp = filter.getCondition();
        reduced = false;
      }

      // Even if no reduction, let's still test the original
      // predicate to see if it was already a constant,
      // in which case we don't need any runtime decision
      // about filtering.
      if (newConditionExp.isAlwaysTrue()) {
        call.transformTo(
            filter.getInput());
      } else if (newConditionExp instanceof RexLiteral
          || RexUtil.isNullLiteral(newConditionExp, true)) {
        call.transformTo(createEmptyRelOrEquivalent(call, filter));
      } else if (reduced) {
        call.transformTo(call.builder()
            .push(filter.getInput())
            .filter(newConditionExp).build());
      } else {
        if (newConditionExp instanceof RexCall) {
          boolean reverse = newConditionExp.getKind() == SqlKind.NOT;
          if (reverse) {
            newConditionExp = ((RexCall) newConditionExp).getOperands().get(0);
          }
          reduceNotNullableFilter(call, filter, newConditionExp, reverse);
        }
        return;
      }

      // New plan is absolutely better than old plan.
      call.getPlanner().prune(filter);
    }

    private void reduceNotNullableFilter(
        RelOptRuleCall call,
        Filter filter,
        RexNode rexNode,
        boolean reverse) {
      // If the expression is a IS [NOT] NULL on a non-nullable
      // column, then we can either remove the filter or replace
      // it with an Empty.
      boolean alwaysTrue;
      switch (rexNode.getKind()) {
        case IS_NULL:
        case IS_UNKNOWN:
          alwaysTrue = false;
          break;
        case IS_NOT_NULL:
          alwaysTrue = true;
          break;
        default:
          return;
      }
      if (reverse) {
        alwaysTrue = !alwaysTrue;
      }
      RexNode operand = ((RexCall) rexNode).getOperands().get(0);
      if (operand instanceof RexInputRef) {
        RexInputRef inputRef = (RexInputRef) operand;
        if (!inputRef.getType().isNullable()) {
          if (alwaysTrue) {
            call.transformTo(filter.getInput());
          } else {
            call.transformTo(createEmptyRelOrEquivalent(call, filter));
          }
          // New plan is absolutely better than old plan.
          call.getPlanner().prune(filter);
        }
      }
    }
  }

  public static class HiveProjectReduceExpressionsRule extends ReduceExpressionsRule.ProjectReduceExpressionsRule {

    protected HiveProjectReduceExpressionsRule(ProjectReduceExpressionsRuleConfig config) {
      super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final RelMetadataQuery mq = call.getMetadataQuery();
      final RelOptPredicateList predicates =
          mq.getPulledUpPredicates(project.getInput());
      final List<RexNode> expList =
          Lists.newArrayList(project.getProjects());
      if (reduceExpressions(project, expList, predicates, false,
          config.matchNullability(), config.treatDynamicCallsAsConstant())) {

        if (project.getProjects().equals(getExpanded(expList, project.getCluster().getRexBuilder()))) {
          return;
        }

        assert !project.getProjects().equals(expList)
            : "Reduced expressions should be different from original expressions";

        call.transformTo(
            call.builder()
                .push(project.getInput())
                .project(expList, project.getRowType().getFieldNames())
                .build());

        // New plan is absolutely better than old plan.
        call.getPlanner().prune(project);
      }
    }
  }

  private static List<RexNode> getExpanded(List<RexNode> nodes, RexBuilder rexBuilder) {
    List<RexNode> result = new ArrayList<>();
    for (RexNode node: nodes) {
      result.add(RexUtil.expandSearch(rexBuilder, null, node));
    }

    return result;
  }
}

// End HiveReduceExpressionsRule.java
