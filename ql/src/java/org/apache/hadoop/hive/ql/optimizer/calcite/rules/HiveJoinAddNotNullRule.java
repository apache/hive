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
import org.apache.calcite.sql.SqlKind;
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

/**
 * Responsible for adding not null rules to joins, when the declaration of a join implies that some coulmns
 * may not be null.
 */
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
    Join join = call.rel(0);
    if (join.getJoinType() == JoinRelType.FULL || join.getCondition().isAlwaysTrue()) {
      return;
    }

    JoinPredicateInfo joinPredInfo;
    try {
      joinPredInfo = HiveCalciteUtil.JoinPredicateInfo.constructJoinPredicateInfo(join);
    } catch (CalciteSemanticException e) {
      return;
    }

    HiveRulesRegistry registry = call.getPlanner().getContext().unwrap(HiveRulesRegistry.class);
    assert registry != null;

    Set<String> leftPushedPredicates = Sets.newHashSet(registry.getPushedPredicates(join, 0));
    Set<String> rightPushedPredicates = Sets.newHashSet(registry.getPushedPredicates(join, 1));

    boolean genPredOnLeft = join.getJoinType() == JoinRelType.RIGHT || join.getJoinType() == JoinRelType.INNER || join.isSemiJoin();
    boolean genPredOnRight = join.getJoinType() == JoinRelType.LEFT || join.getJoinType() == JoinRelType.INNER || join.isSemiJoin();

    RexNode newLeftPredicate = getNewPredicate(join, registry, joinPredInfo, leftPushedPredicates, genPredOnLeft, 0);
    RexNode newRightPredicate = getNewPredicate(join, registry, joinPredInfo, rightPushedPredicates, genPredOnRight, 1);

    if (newLeftPredicate.isAlwaysTrue() && newRightPredicate.isAlwaysTrue()) {
      return;
    }

    RelNode lChild = getNewChild(call, join, join.getLeft(), newLeftPredicate);
    RelNode rChild = getNewChild(call, join, join.getRight(), newRightPredicate);

    Join newJoin = join.copy(join.getTraitSet(), join.getCondition(), lChild, rChild, join.getJoinType(),
        join.isSemiJoinDone());
    call.getPlanner().onCopy(join, newJoin);

    // Register information about created predicates
    registry.getPushedPredicates(newJoin, 0).addAll(leftPushedPredicates);
    registry.getPushedPredicates(newJoin, 1).addAll(rightPushedPredicates);

    call.transformTo(newJoin);
  }

  private RexNode getNewPredicate(Join join, HiveRulesRegistry registry, JoinPredicateInfo joinPredInfo,
      Set<String> pushedPredicates, boolean genPred, int pos) {
    RexBuilder rexBuilder = join.getCluster().getRexBuilder();

    if (genPred) {
      List<RexNode> joinExprsList = new ArrayList<>();
      for (JoinLeafPredicateInfo joinLeafPredicateInfo : joinPredInfo.getEquiJoinPredicateElements()) {
        joinExprsList.addAll(joinLeafPredicateInfo.getJoinExprs(pos));
      }
      for (JoinLeafPredicateInfo joinLeafPredicateInfo : joinPredInfo.getNonEquiJoinPredicateElements()) {
        if (SqlKind.COMPARISON.contains(joinLeafPredicateInfo.getComparisonType())) {
          joinExprsList.addAll(joinLeafPredicateInfo.getJoinExprs(pos));
        }
      }

      List<RexNode> newConditions = getNotNullConditions(rexBuilder, joinExprsList, pushedPredicates);
      return RexUtil.composeConjunction(rexBuilder, newConditions, false);
    } else {
      return rexBuilder.makeLiteral(true);
    }
  }

  private static List<RexNode> getNotNullConditions(RexBuilder rexBuilder, List<RexNode> inputJoinExprs,
      Set<String> pushedPredicates) {
    List<RexNode> newConditions = Lists.newArrayList();

    for (RexNode rexNode : inputJoinExprs) {
      RexNode cond = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, rexNode);
      String digest = cond.toString();
      if (pushedPredicates.add(digest)) {
        newConditions.add(cond);
      }
    }
    return newConditions;
  }

  private RelNode getNewChild(RelOptRuleCall call, Join join, RelNode child, RexNode newPredicate) {
    if (!newPredicate.isAlwaysTrue()) {
      RelNode newChild = filterFactory.createFilter(child, newPredicate);
      call.getPlanner().onCopy(child, newChild);
      return newChild;
    }

    return child;
  }
}
