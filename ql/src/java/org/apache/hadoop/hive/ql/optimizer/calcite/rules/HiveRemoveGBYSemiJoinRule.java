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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinLeafPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Planner rule that removes a {@code Aggregate} from a HiveSemiJoin/HiveAntiJoin
 * right input.
 */
public class HiveRemoveGBYSemiJoinRule extends RelOptRule {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveRemoveGBYSemiJoinRule.class);

  public static final HiveRemoveGBYSemiJoinRule INSTANCE =
      new HiveRemoveGBYSemiJoinRule();

  public HiveRemoveGBYSemiJoinRule() {
    super(
        operand(Join.class,
            some(
                operand(RelNode.class, any()),
                operand(Aggregate.class, any()))),
        HiveRelFactories.HIVE_BUILDER, "HiveRemoveGBYSemiJoinRule");
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);

    if (join.getJoinType() != JoinRelType.SEMI && join.getJoinType() != JoinRelType.ANTI) {
      return;
    }

    final RelNode left = call.rel(1);
    final Aggregate rightAggregate= call.rel(2);

    // if grouping sets are involved do early return
    if(rightAggregate.getGroupType() != Aggregate.Group.SIMPLE) {
      return;
    }

    if(rightAggregate.indicator) {
      return;
    }

    // if there is any aggregate function this group by is not un-necessary
    if(!rightAggregate.getAggCallList().isEmpty()) {
      return;
    }

    JoinPredicateInfo joinPredInfo;
    try {
      joinPredInfo = HiveCalciteUtil.JoinPredicateInfo.constructJoinPredicateInfo(join);
    } catch (CalciteSemanticException e) {
      LOG.warn("Exception while extracting predicate info from {}", join);
      return;
    }
    if (!joinPredInfo.getNonEquiJoinPredicateElements().isEmpty()) {
      return;
    }
    ImmutableBitSet.Builder rightKeys = ImmutableBitSet.builder();
    for (JoinLeafPredicateInfo leftPredInfo : joinPredInfo.getEquiJoinPredicateElements()) {
      rightKeys.addAll(leftPredInfo.getProjsFromRightPartOfJoinKeysInChildSchema());
    }
    boolean shouldTransform = rightKeys.build().equals(
        ImmutableBitSet.range(rightAggregate.getGroupCount()));
    if(shouldTransform) {
      final RelBuilder relBuilder = call.builder();
      RelNode newRightInput = relBuilder.project(relBuilder.push(rightAggregate.getInput()).
          fields(rightAggregate.getGroupSet().asList())).build();
      RelNode newJoin;
      if (join.getJoinType() == JoinRelType.SEMI) {
        newJoin = call.builder().push(left).push(newRightInput)
                .semiJoin(join.getCondition()).build();
      } else {
        newJoin = call.builder().push(left).push(newRightInput)
                .antiJoin(join.getCondition()).build();
      }
      call.transformTo(newJoin);
    }
  }
}
// End HiveRemoveGBYSemiJoinRule
