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
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Planner rule that removes a {@code Aggregate} from a HiveSemiJoin
 * right input.
 */
public class HiveRemoveGBYSemiJoinRule extends RelOptRule {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveRemoveGBYSemiJoinRule.class);
  public static final HiveRemoveGBYSemiJoinRule INSTANCE =
      new HiveRemoveGBYSemiJoinRule();

  public HiveRemoveGBYSemiJoinRule() {
    super(
        operand(HiveSemiJoin.class,
            some(
                operand(RelNode.class, any()),
                operand(Aggregate.class, any()))),
        HiveRelFactories.HIVE_BUILDER, "HiveRemoveGBYSemiJoinRule");
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final HiveSemiJoin semijoin= call.rel(0);

    if(semijoin.getJoinType() != JoinRelType.INNER) {
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
    final JoinInfo joinInfo = semijoin.analyzeCondition();

    boolean shouldTransform = joinInfo.rightSet().equals(
        ImmutableBitSet.range(rightAggregate.getGroupCount()));
    if(shouldTransform) {
      final RelBuilder relBuilder = call.builder();
      RelNode newRightInput = relBuilder.project(relBuilder.push(rightAggregate.getInput()).
          fields(rightAggregate.getGroupSet().asList())).build();
      RelNode newSemiJoin = call.builder().push(left).push(newRightInput)
          .semiJoin(semijoin.getCondition()).build();
      call.transformTo(newSemiJoin);
    }
  }
}
// End HiveRemoveGBYSemiJoinRule
