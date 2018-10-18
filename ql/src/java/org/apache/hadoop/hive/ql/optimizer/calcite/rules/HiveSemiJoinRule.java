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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that creates a {@code SemiJoinRule} from a
 * {@link org.apache.calcite.rel.core.Join} on top of a
 * {@link org.apache.calcite.rel.logical.LogicalAggregate}.
 *
 * TODO Remove this rule and use Calcite's SemiJoinRule. Not possible currently
 * since Calcite doesnt use RelBuilder for this rule and we want to generate HiveSemiJoin rel here.
 */
public abstract class HiveSemiJoinRule extends RelOptRule {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveSemiJoinRule.class);

  public static final HiveProjectToSemiJoinRule INSTANCE_PROJECT =
          new HiveProjectToSemiJoinRule(HiveRelFactories.HIVE_BUILDER);

  public static final HiveAggregateToSemiJoinRule INSTANCE_AGGREGATE =
          new HiveAggregateToSemiJoinRule(HiveRelFactories.HIVE_BUILDER);

  private HiveSemiJoinRule(RelOptRuleOperand operand, RelBuilderFactory relBuilder) {
    super(operand, relBuilder, null);
  }

  protected void perform(RelOptRuleCall call, ImmutableBitSet topRefs,
          RelNode topOperator, Join join, RelNode left, Aggregate aggregate) {
    LOG.debug("Matched HiveSemiJoinRule");
    final RelOptCluster cluster = join.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final ImmutableBitSet rightBits =
        ImmutableBitSet.range(left.getRowType().getFieldCount(),
            join.getRowType().getFieldCount());
    if (topRefs.intersects(rightBits)) {
      return;
    }
    final JoinInfo joinInfo = join.analyzeCondition();
    if (!joinInfo.rightSet().equals(
        ImmutableBitSet.range(aggregate.getGroupCount()))) {
      // Rule requires that aggregate key to be the same as the join key.
      // By the way, neither a super-set nor a sub-set would work.
      return;
    }
    if(join.getJoinType() == JoinRelType.LEFT) {
      // since for LEFT join we are only interested in rows from LEFT we can get rid of right side
      call.transformTo(topOperator.copy(topOperator.getTraitSet(), ImmutableList.of(left)));
      return;
    }
    if (join.getJoinType() != JoinRelType.INNER) {
      return;
    }
    if (!joinInfo.isEqui()) {
      return;
    }
    LOG.debug("All conditions matched for HiveSemiJoinRule. Going to apply transformation.");
    final List<Integer> newRightKeyBuilder = Lists.newArrayList();
    final List<Integer> aggregateKeys = aggregate.getGroupSet().asList();
    for (int key : joinInfo.rightKeys) {
      newRightKeyBuilder.add(aggregateKeys.get(key));
    }
    final ImmutableIntList newRightKeys =
        ImmutableIntList.copyOf(newRightKeyBuilder);
    final RelNode newRight = aggregate.getInput();
    final RexNode newCondition =
        RelOptUtil.createEquiJoinCondition(left, joinInfo.leftKeys, newRight,
            newRightKeys, rexBuilder);

    RelNode semi = null;
    //HIVE-15458: we need to add a Project on top of Join since SemiJoin with Join as it's right input
    // is not expected further down the pipeline. see jira for more details
    if(aggregate.getInput() instanceof HepRelVertex
          && ((HepRelVertex)aggregate.getInput()).getCurrentRel() instanceof  Join) {
        Join rightJoin = (Join)(((HepRelVertex)aggregate.getInput()).getCurrentRel());
        List<RexNode> projects = new ArrayList<>();
        for(int i=0; i<rightJoin.getRowType().getFieldCount(); i++){
          projects.add(rexBuilder.makeInputRef(rightJoin, i));
        }
       RelNode topProject =  call.builder().push(rightJoin).project(projects, rightJoin.getRowType().getFieldNames(), true).build();
      semi = call.builder().push(left).push(topProject).semiJoin(newCondition).build();
    }
    else {
      semi = call.builder().push(left).push(aggregate.getInput()).semiJoin(newCondition).build();
    }
    call.transformTo(topOperator.copy(topOperator.getTraitSet(), ImmutableList.of(semi)));
  }

  /** SemiJoinRule that matches a Project on top of a Join with an Aggregate
   * as its right child. */
  public static class HiveProjectToSemiJoinRule extends HiveSemiJoinRule {

    /** Creates a HiveProjectToSemiJoinRule. */
    public HiveProjectToSemiJoinRule(RelBuilderFactory relBuilder) {
      super(
          operand(Project.class,
                  some(operand(Join.class,
                      some(
                          operand(RelNode.class, any()),
                          operand(Aggregate.class, any()))))),
              relBuilder);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final Join join = call.rel(1);
      final RelNode left = call.rel(2);
      final Aggregate aggregate = call.rel(3);
      final ImmutableBitSet topRefs =
              RelOptUtil.InputFinder.bits(project.getChildExps(), null);
      perform(call, topRefs, project, join, left, aggregate);
    }
  }

  /** SemiJoinRule that matches a Aggregate on top of a Join with an Aggregate
   * as its right child. */
  public static class HiveAggregateToSemiJoinRule extends HiveSemiJoinRule {

    /** Creates a HiveAggregateToSemiJoinRule. */
    public HiveAggregateToSemiJoinRule(RelBuilderFactory relBuilder) {
      super(
          operand(Aggregate.class,
              some(operand(Join.class,
                  some(
                      operand(RelNode.class, any()),
                      operand(Aggregate.class, any()))))),
          relBuilder);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Aggregate topAggregate = call.rel(0);
      final Join join = call.rel(1);
      final RelNode left = call.rel(2);
      final Aggregate aggregate = call.rel(3);
      // Gather columns used by aggregate operator
      final ImmutableBitSet.Builder topRefs = ImmutableBitSet.builder();
      topRefs.addAll(topAggregate.getGroupSet());
      for (AggregateCall aggCall : topAggregate.getAggCallList()) {
        topRefs.addAll(aggCall.getArgList());
        if (aggCall.filterArg != -1) {
          topRefs.set(aggCall.filterArg);
        }
      }
      perform(call, topRefs.build(), topAggregate, join, left, aggregate);
    }
  }

}

// End SemiJoinRule.java
