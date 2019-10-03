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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
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

  public static final HiveProjectToSemiJoinRuleSwapInputs INSTANCE_PROJECT_SWAPPED =
      new HiveProjectToSemiJoinRuleSwapInputs(HiveRelFactories.HIVE_BUILDER);

  public static final HiveAggregateToSemiJoinRule INSTANCE_AGGREGATE =
      new HiveAggregateToSemiJoinRule(HiveRelFactories.HIVE_BUILDER);

  private HiveSemiJoinRule(final RelOptRuleOperand operand, final RelBuilderFactory relBuilder) {
    super(operand, relBuilder, null);
  }

  private RelNode buildProject(final Aggregate aggregate, final RexBuilder rexBuilder,
                               final RelBuilder relBuilder) {
    assert (!aggregate.indicator && aggregate.getAggCallList().isEmpty());
    RelNode input = aggregate.getInput();
    List<Integer> groupingKeys = aggregate.getGroupSet().asList();
    List<RexNode> projects = new ArrayList<>();
    for (Integer keys:groupingKeys) {
      projects.add(rexBuilder.makeInputRef(input, keys.intValue()));
    }
    return relBuilder.push(aggregate.getInput()).project(projects).build();
  }

  private boolean needProject(final RelNode input, final RelNode aggregate) {
    if (input instanceof Join
        || input.getRowType().getFieldCount() != aggregate.getRowType().getFieldCount()) {
      return true;
    }
    return false;
  }

  protected void perform(final RelOptRuleCall call, final ImmutableBitSet topRefs,
                         final RelNode topOperator, final Join join, final RelNode left,
                         final Aggregate aggregate, final RelNode aggregateInput) {
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
    if (!joinInfo.isEqui()) {
      return;
    }

    if (join.getJoinType() == JoinRelType.LEFT) {
      // since for LEFT join we are only interested in rows from LEFT we can get rid of right side
      call.transformTo(topOperator.copy(topOperator.getTraitSet(), ImmutableList.of(left)));
      return;
    }
    if (join.getJoinType() != JoinRelType.INNER && !join.isSemiJoin()) {
      return;
    }

    LOG.debug("All conditions matched for HiveSemiJoinRule. Going to apply transformation.");
    final List<Integer> newRightKeyBuilder = Lists.newArrayList();
    final List<Integer> aggregateKeys = aggregate.getGroupSet().asList();
    for (int key : joinInfo.rightKeys) {
      newRightKeyBuilder.add(aggregateKeys.get(key));
    }
    final RelNode newRight = needProject(aggregateInput, aggregate)
        ? buildProject(aggregate, rexBuilder, call.builder()) : aggregateInput;
    final RexNode newCondition =
        RelOptUtil.createEquiJoinCondition(left, joinInfo.leftKeys, newRight,
                                           joinInfo.rightKeys, rexBuilder);

    RelNode semi = call.builder().push(left).push(newRight).semiJoin(newCondition).build();
    call.transformTo(topOperator.copy(topOperator.getTraitSet(), ImmutableList.of(semi)));
  }

  /** SemiJoinRule that matches a Project on top of a Join with an Aggregate
   * as its right child. */
  public static class HiveProjectToSemiJoinRule extends HiveSemiJoinRule {

    /** Creates a HiveProjectToSemiJoinRule. */
    public HiveProjectToSemiJoinRule(final RelBuilderFactory relBuilder) {
      super(
          operand(Project.class,
                  operand(Join.class,
                               some(
                                   operand(RelNode.class, any()),
                                   operand(Aggregate.class,
                                           operand(RelNode.class, any()))))),
          relBuilder);
    }

    @Override public void onMatch(final RelOptRuleCall call) {
      final Project project = call.rel(0);
      final Join join = call.rel(1);
      final RelNode left = call.rel(2);
      final Aggregate aggregate = call.rel(3);
      final ImmutableBitSet topRefs =
          RelOptUtil.InputFinder.bits(project.getChildExps(), null);
      final RelNode aggregateInput = call.rel(4);
      perform(call, topRefs, project, join, left, aggregate, aggregateInput);
    }
  }

  /** SemiJoinRule that matches a Project on top of a Join with an Aggregate
   * as its left child. */
  public static class HiveProjectToSemiJoinRuleSwapInputs extends HiveSemiJoinRule {

    /** Creates a HiveProjectToSemiJoinRule. */
    public HiveProjectToSemiJoinRuleSwapInputs(final RelBuilderFactory relBuilder) {
      super(
          operand(Project.class,
                  operand(Join.class,
                               some(
                                   operand(Aggregate.class, operand(RelNode.class, any())),
                                   operand(RelNode.class, any())))),
          relBuilder);
    }

    private Project swapInputs(final Join join, final Project topProject, final RelBuilder builder) {
      RexBuilder rexBuilder = join.getCluster().getRexBuilder();

      int rightInputSize = join.getRight().getRowType().getFieldCount();
      int leftInputSize = join.getLeft().getRowType().getFieldCount();
      List<RelDataTypeField> joinFields = join.getRowType().getFieldList();

      //swap the join inputs
      //adjust join condition
      int[] condAdjustments = new int[joinFields.size()];
      for (int i=0; i<joinFields.size(); i++) {
        if(i < leftInputSize) {
          //left side refs need to be moved by right input size
          condAdjustments[i] = rightInputSize;
        } else {
          //right side refs need to move "down" by left input size
          condAdjustments[i] = -leftInputSize;
        }
      }
      RexNode newJoinCond = join.getCondition().accept(new RelOptUtil.RexInputConverter(rexBuilder, joinFields,
                                                                                        joinFields, condAdjustments));
      Join swappedJoin = (Join)builder.push(join.getRight()).push(join.getLeft()).join(join.getJoinType(),
                                                                                       newJoinCond).build();
      // need to adjust project rex refs
      List<RexNode> newProjects = new ArrayList<>();

      List<RelDataTypeField> swappedJoinFeilds = swappedJoin.getRowType().getFieldList();
      for(RexNode project:topProject.getProjects()) {
        RexNode newProject = project.accept(new RelOptUtil.RexInputConverter(rexBuilder, swappedJoinFeilds,
                                                                             swappedJoinFeilds, condAdjustments));
        newProjects.add(newProject);
      }
      return (Project)builder.push(swappedJoin).project(newProjects).build();
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final Join join = call.rel(1);
      final Aggregate aggregate = call.rel(2);
      final RelNode aggreagateInput = call.rel(3);
      final RelNode right = call.rel(4);

      // make sure the following conditions are met
      //  Join is INNER
      // Join keys are same as gb keys
      //  project above is referring to inputs only from non-aggregate side
      final JoinInfo joinInfo = join.analyzeCondition();
      if(!joinInfo.isEqui()) {
        return;
      }
      if (!joinInfo.leftSet().equals(
          ImmutableBitSet.range(aggregate.getGroupCount()))) {
        // Rule requires that aggregate key to be the same as the join key.
        // By the way, neither a super-set nor a sub-set would work.
        return;
      }
      final ImmutableBitSet topRefs =
          RelOptUtil.InputFinder.bits(project.getChildExps(), null);

      final ImmutableBitSet leftBits =
          ImmutableBitSet.range(0, join.getLeft().getRowType().getFieldCount());

      if (topRefs.intersects(leftBits)) {
        return;
      }
      // it is safe to swap inputs
      final Project swappedProject = swapInputs(join, project, call.builder());
      final RelNode swappedJoin = swappedProject.getInput();
      assert(swappedJoin instanceof  Join);

      final ImmutableBitSet swappedTopRefs =
          RelOptUtil.InputFinder.bits(swappedProject.getChildExps(), null);

      perform(call, swappedTopRefs, swappedProject, (Join)swappedJoin, right, aggregate, aggreagateInput);
    }
  }

  /** SemiJoinRule that matches a Aggregate on top of a Join with an Aggregate
   * as its right child. */
  public static class HiveAggregateToSemiJoinRule extends HiveSemiJoinRule {

    /** Creates a HiveAggregateToSemiJoinRule. */
    public HiveAggregateToSemiJoinRule(final RelBuilderFactory relBuilder) {
      super(
          operand(Aggregate.class,
                  operand(Join.class,
                               some(
                                   operand(RelNode.class, any()),
                                   operand(Aggregate.class, operand(RelNode.class, any()))))),
          relBuilder);
    }

    @Override public void onMatch(final RelOptRuleCall call) {
      final Aggregate topAggregate = call.rel(0);
      final Join join = call.rel(1);
      final RelNode left = call.rel(2);
      final Aggregate aggregate = call.rel(3);
      final RelNode aggregateInput = call.rel(4);

      // Gather columns used by aggregate operator
      final ImmutableBitSet.Builder topRefs = ImmutableBitSet.builder();
      topRefs.addAll(topAggregate.getGroupSet());
      for (AggregateCall aggCall : topAggregate.getAggCallList()) {
        topRefs.addAll(aggCall.getArgList());
        if (aggCall.filterArg != -1) {
          topRefs.set(aggCall.filterArg);
        }
      }
      perform(call, topRefs.build(), topAggregate, join, left, aggregate, aggregateInput);
    }
  }

}

// End SemiJoinRule.java
