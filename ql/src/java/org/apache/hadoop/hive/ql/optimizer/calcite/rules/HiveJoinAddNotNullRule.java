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
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.RelFactories.FilterFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinLeafPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public final class HiveJoinAddNotNullRule extends RelOptRule {

  public static final HiveJoinAddNotNullRule INSTANCE_JOIN =
          new HiveJoinAddNotNullRule(HiveJoin.class, HiveRelFactories.HIVE_FILTER_FACTORY);

  public static final HiveJoinAddNotNullRule INSTANCE_SEMIJOIN =
          new HiveJoinAddNotNullRule(HiveSemiJoin.class, HiveRelFactories.HIVE_FILTER_FACTORY);

  private final FilterFactory filterFactory;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an HiveJoinAddNotNullRule.
   */
  public HiveJoinAddNotNullRule(Class<? extends Join> clazz,
          RelFactories.FilterFactory filterFactory) {
    super(operand(clazz, any()));
    this.filterFactory = filterFactory;
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    RelNode lChild = join.getLeft();
    RelNode rChild = join.getRight();

    HiveRulesRegistry registry = call.getPlanner().getContext().unwrap(HiveRulesRegistry.class);
    assert registry != null;

    if (join.getJoinType() != JoinRelType.INNER) {
      return;
    }

    if (join.getCondition().isAlwaysTrue()) {
      return;
    }

    JoinPredicateInfo joinPredInfo;
    try {
      joinPredInfo = HiveCalciteUtil.JoinPredicateInfo.constructJoinPredicateInfo(join);
    } catch (CalciteSemanticException e) {
      return;
    }
    
    List<RexNode> leftJoinExprsList = new ArrayList<>();
    List<RexNode> rightJoinExprsList = new ArrayList<>();
    for (JoinLeafPredicateInfo joinLeafPredicateInfo : joinPredInfo.getEquiJoinPredicateElements()) {
        leftJoinExprsList.addAll(joinLeafPredicateInfo.getJoinExprs(0));
        rightJoinExprsList.addAll(joinLeafPredicateInfo.getJoinExprs(1));
    }

    // Build not null conditions
    final RelOptCluster cluster = join.getCluster();
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();

    Set<String> leftPushedPredicates = Sets.newHashSet(registry.getPushedPredicates(join, 0));
    final List<RexNode> newLeftConditions = getNotNullConditions(cluster,
            rexBuilder, leftJoinExprsList, leftPushedPredicates);
    Set<String> rightPushedPredicates = Sets.newHashSet(registry.getPushedPredicates(join, 1));
    final List<RexNode> newRightConditions = getNotNullConditions(cluster,
            rexBuilder, rightJoinExprsList, rightPushedPredicates);

    // Nothing will be added to the expression
    RexNode newLeftPredicate = RexUtil.composeConjunction(rexBuilder, newLeftConditions, false);
    RexNode newRightPredicate = RexUtil.composeConjunction(rexBuilder, newRightConditions, false);
    if (newLeftPredicate.isAlwaysTrue() && newRightPredicate.isAlwaysTrue()) {
      return;
    }

    if (!newLeftPredicate.isAlwaysTrue()) {
      RelNode curr = lChild;
      lChild = filterFactory.createFilter(lChild, newLeftPredicate);
      call.getPlanner().onCopy(curr, lChild);
    }
    if (!newRightPredicate.isAlwaysTrue()) {
      RelNode curr = rChild;
      rChild = filterFactory.createFilter(rChild, newRightPredicate);
      call.getPlanner().onCopy(curr, rChild);
    }

    Join newJoin = join.copy(join.getTraitSet(), join.getCondition(),
            lChild, rChild, join.getJoinType(), join.isSemiJoinDone());
    call.getPlanner().onCopy(join, newJoin);

    // Register information about created predicates
    registry.getPushedPredicates(newJoin, 0).addAll(leftPushedPredicates);
    registry.getPushedPredicates(newJoin, 1).addAll(rightPushedPredicates);

    call.transformTo(newJoin);
  }

  private static List<RexNode> getNotNullConditions(RelOptCluster cluster,
          RexBuilder rexBuilder, List<RexNode> inputJoinExprs,
          Set<String> pushedPredicates) {
    final List<RexNode> newConditions = Lists.newArrayList();

    for (RexNode rexNode : inputJoinExprs) {
        RexNode cond = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, rexNode);
        String digest = cond.toString();
        if (pushedPredicates.add(digest)) {
            newConditions.add(cond);
        }
    }
    return newConditions;
  }

}
