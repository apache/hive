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

import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.GroupKey;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * Class that gathers SemiJoin conversion rules.
 */
public class HiveSemiJoinRule {

  public static final HiveProjectJoinToSemiJoinRule INSTANCE_PROJECT =
      new HiveProjectJoinToSemiJoinRule();

  public static final HiveAggregateJoinToSemiJoinRule INSTANCE_AGGREGATE =
      new HiveAggregateJoinToSemiJoinRule();

  public static final HiveProjectJoinToSemiJoinRuleSwapInputs INSTANCE_PROJECT_SWAPPED =
      new HiveProjectJoinToSemiJoinRuleSwapInputs();

  public static final HiveAggregateJoinToSemiJoinRuleSwapInputs INSTANCE_AGGREGATE_SWAPPED =
      new HiveAggregateJoinToSemiJoinRuleSwapInputs();

  private HiveSemiJoinRule() {
    // Exists only to defeat instantiation.
  }

  /**
   * Planner rule that creates a {@code SemiJoinRule} from a
   * {@link org.apache.calcite.rel.core.Join} on top of a
   * {@link org.apache.calcite.rel.core.Aggregate}.
   */
  private abstract static class HiveSemiJoinRuleBase<T extends RelNode> extends RelOptRule {

    protected static final Logger LOG = LoggerFactory.getLogger(HiveSemiJoinRuleBase.class);

    protected HiveSemiJoinRuleBase(final Class<T> clazz, final RelBuilderFactory relBuilder) {
      super(
          operand(clazz,
              operand(Join.class,
                  operand(RelNode.class, any()),
                  operand(Aggregate.class,
                      operand(RelNode.class, any())))),
          relBuilder, null);
    }

    protected HiveSemiJoinRuleBase(final RelOptRuleOperand operand, final RelBuilderFactory relBuilder) {
      super(operand, relBuilder, null);
    }

    @Override
    public void onMatch(final RelOptRuleCall call) {
      final T topOperator = call.rel(0);
      final Join join = call.rel(1);

      if (join.isSemiJoin()) {
        // bail out, nothing to do
        return;
      }

      final RelNode left = call.rel(2);
      final Aggregate aggregate = call.rel(3);
      final RelNode aggregateInput = call.rel(4);
      final ImmutableBitSet topRefs = extractUsedFields(topOperator);
      perform(call, topRefs, topOperator, join, left, aggregate, aggregateInput);
    }

    private boolean needProject(final RelNode input, final RelNode aggregate) {
      return input instanceof Join
          || input.getRowType().getFieldCount() != aggregate.getRowType().getFieldCount();
    }

    protected void perform(final RelOptRuleCall call, final ImmutableBitSet topRefs,
                           final T topOperator, final Join join, final RelNode left,
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
      if (join.getJoinType() != JoinRelType.INNER) {
        return;
      }

      LOG.debug("All conditions matched for HiveSemiJoinRule. Going to apply transformation.");
      final ImmutableBitSet leftNeededRefs = topRefs.union(ImmutableBitSet.of(joinInfo.leftKeys));
      final boolean updateLeft = leftNeededRefs.cardinality() != left.getRowType().getFieldCount();
      final RelNode newLeft = updateLeft
          ? buildProjectLeftInput(left, leftNeededRefs, rexBuilder, call.builder()) : left;
      final RelNode newRight = needProject(aggregateInput, aggregate)
          ? buildProjectRightInput(aggregate, rexBuilder, call.builder()) : aggregateInput;
      final ImmutableIntList leftKeys;
      if (updateLeft) {
        List<Integer> newLeftKeys = new ArrayList<>();
        for (int pos : joinInfo.leftKeys) {
          newLeftKeys.add(leftNeededRefs.indexOf(pos));
        }
        leftKeys = ImmutableIntList.copyOf(newLeftKeys);
      } else {
        leftKeys = joinInfo.leftKeys;
      }
      final RexNode newCondition = RelOptUtil.createEquiJoinCondition(
          newLeft, leftKeys, newRight, joinInfo.rightKeys, rexBuilder);

      RelNode semi = call.builder().push(newLeft).push(newRight).semiJoin(newCondition).build();
      int[] adjustments = new int[left.getRowType().getFieldCount()];
      for (int i = 0; i < adjustments.length; i++) {
        adjustments[i] = leftNeededRefs.indexOf(i) - i;
      }
      call.transformTo(
          recreateTopOperatorUnforced(
              call.builder(), rexBuilder, adjustments, topOperator, semi));
    }

    private RelNode buildProjectLeftInput(final RelNode node, final ImmutableBitSet neededRefs,
        final RexBuilder rexBuilder, final RelBuilder builder) {
      List<RexNode> exprs = new ArrayList<>();
      for (int pos : neededRefs) {
        exprs.add(rexBuilder.makeInputRef(node, pos));
      }
      return builder.push(node).project(exprs).build();
    }

    private RelNode buildProjectRightInput(final Aggregate aggregate, final RexBuilder rexBuilder,
        final RelBuilder relBuilder) {
      assert (aggregate.getGroupType() == Group.SIMPLE && aggregate.getAggCallList().isEmpty());
      RelNode input = aggregate.getInput();
      List<Integer> groupingKeys = aggregate.getGroupSet().asList();
      List<RexNode> projects = new ArrayList<>();
      for (Integer keys:groupingKeys) {
        projects.add(rexBuilder.makeInputRef(input, keys));
      }
      return relBuilder.push(aggregate.getInput()).project(projects).build();
    }

    protected abstract ImmutableBitSet extractUsedFields(T operator);

    protected abstract T recreateTopOperator(RelBuilder builder, RexBuilder rexBuilder,
        int[] adjustments, T topOperator, RelNode inputOperator);

    protected abstract RelNode recreateTopOperatorUnforced(RelBuilder builder, RexBuilder rexBuilder,
        int[] adjustments, T topOperator, RelNode inputOperator);
  }

  /**
   * SemiJoinRule that matches a Project on top of a Join with an Aggregate
   * as its right child.
   */
  protected static class HiveProjectJoinToSemiJoinRule extends HiveSemiJoinRuleBase<Project> {

    protected HiveProjectJoinToSemiJoinRule() {
      super(Project.class, HiveRelFactories.HIVE_BUILDER);
    }

    @Override
    protected ImmutableBitSet extractUsedFields(Project project) {
      return RelOptUtil.InputFinder.bits(project.getProjects(), null);
    }

    @Override
    protected Project recreateTopOperator(RelBuilder builder, RexBuilder rexBuilder,
        int[] adjustments, Project topProject, RelNode newInputOperator) {
      return (Project) recreateProjectOperator(builder, rexBuilder, adjustments,
          topProject, newInputOperator, true);
    }

    @Override
    protected RelNode recreateTopOperatorUnforced(RelBuilder builder, RexBuilder rexBuilder,
        int[] adjustments, Project topProject, RelNode newInputOperator) {
      return recreateProjectOperator(builder, rexBuilder, adjustments,
          topProject, newInputOperator, false);
    }
  }

  /**
   * SemiJoinRule that matches a Aggregate on top of a Join with an Aggregate
   * as its right child.
   */
  protected static class HiveAggregateJoinToSemiJoinRule extends HiveSemiJoinRuleBase<Aggregate> {

    /** Creates a HiveAggregateJoinToSemiJoinRule. */
    protected HiveAggregateJoinToSemiJoinRule() {
      super(Aggregate.class, HiveRelFactories.HIVE_BUILDER);
    }

    @Override
    protected ImmutableBitSet extractUsedFields(Aggregate aggregate) {
      return HiveCalciteUtil.extractRefs(aggregate);
    }

    @Override
    protected Aggregate recreateTopOperator(RelBuilder builder, RexBuilder rexBuilder,
        int[] adjustments, Aggregate topAggregate, RelNode newInputOperator) {
      return (Aggregate) recreateAggregateOperator(builder, adjustments, topAggregate, newInputOperator);
    }

    @Override
    protected RelNode recreateTopOperatorUnforced(RelBuilder builder, RexBuilder rexBuilder,
        int[] adjustments, Aggregate topAggregate, RelNode newInputOperator) {
      return recreateAggregateOperator(builder, adjustments, topAggregate, newInputOperator);
    }
  }

  /**
   * Rule that swaps Join inputs if they can potentially lead to SemiJoin conversion.
   */
  private abstract static class HiveToSemiJoinRuleSwapInputs<T extends RelNode> extends HiveSemiJoinRuleBase<T> {

    protected HiveToSemiJoinRuleSwapInputs(final Class<T> clazz, final RelBuilderFactory relBuilder) {
      super(
          operand(clazz,
              operand(Join.class,
                  operand(Aggregate.class, operand(RelNode.class, any())),
                  operand(RelNode.class, any()))),
          relBuilder);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final T topOperator = call.rel(0);
      final Join join = call.rel(1);

      if (join.isSemiJoin()) {
        // bail out, nothing to do
        return;
      }

      final Aggregate aggregate = call.rel(2);
      final RelNode aggregateInput = call.rel(3);
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
      final ImmutableBitSet topRefs = extractUsedFields(topOperator);

      final ImmutableBitSet leftBits =
          ImmutableBitSet.range(0, join.getLeft().getRowType().getFieldCount());

      if (topRefs.intersects(leftBits)) {
        return;
      }
      // it is safe to swap inputs
      final T swappedTopOperator = swapInputs(join, topOperator, call.builder());
      final Join swappedJoin = (Join) swappedTopOperator.getInput(0);

      final ImmutableBitSet swappedTopRefs = extractUsedFields(swappedTopOperator);

      perform(call, swappedTopRefs, swappedTopOperator, swappedJoin, right, aggregate, aggregateInput);
    }

    protected T swapInputs(final Join join, final T topOperator, final RelBuilder builder) {
      RexBuilder rexBuilder = join.getCluster().getRexBuilder();

      int rightInputSize = join.getRight().getRowType().getFieldCount();
      int leftInputSize = join.getLeft().getRowType().getFieldCount();
      List<RelDataTypeField> joinFields = join.getRowType().getFieldList();

      //swap the join inputs
      //adjust join condition
      int[] adjustments = new int[joinFields.size()];
      for (int i=0; i<joinFields.size(); i++) {
        if(i < leftInputSize) {
          //left side refs need to be moved by right input size
          adjustments[i] = rightInputSize;
        } else {
          //right side refs need to move "down" by left input size
          adjustments[i] = -leftInputSize;
        }
      }
      RexNode newJoinCond = join.getCondition().accept(
          new RelOptUtil.RexInputConverter(rexBuilder, joinFields, joinFields, adjustments));
      RelNode swappedJoin = builder
          .push(join.getRight())
          .push(join.getLeft())
          .join(join.getJoinType(), newJoinCond)
          .build();
      // need to adjust top refs
      return recreateTopOperator(builder, rexBuilder, adjustments, topOperator, swappedJoin);
    }
  }

  /**
   * Rule that matches an Project on top of a Join and tries to transform it
   * into a SemiJoin swapping its inputs.
   */
  protected static class HiveProjectJoinToSemiJoinRuleSwapInputs extends HiveToSemiJoinRuleSwapInputs<Project> {

    protected HiveProjectJoinToSemiJoinRuleSwapInputs() {
      super(Project.class, HiveRelFactories.HIVE_BUILDER);
    }

    @Override
    protected ImmutableBitSet extractUsedFields(Project project) {
      return RelOptUtil.InputFinder.bits(project.getProjects(), null);
    }

    @Override
    protected Project recreateTopOperator(RelBuilder builder, RexBuilder rexBuilder,
        int[] adjustments, Project topProject, RelNode newInputOperator) {
      return (Project) recreateProjectOperator(builder, rexBuilder, adjustments,
          topProject, newInputOperator, true);
    }

    @Override
    protected RelNode recreateTopOperatorUnforced(RelBuilder builder, RexBuilder rexBuilder,
        int[] adjustments, Project topProject, RelNode newInputOperator) {
      return recreateProjectOperator(builder, rexBuilder, adjustments,
          topProject, newInputOperator, false);
    }
  }

  /**
   * Rule that matches an Aggregate on top of a Join and tries to transform it
   * into a SemiJoin swapping its inputs.
   */
  protected static class HiveAggregateJoinToSemiJoinRuleSwapInputs extends HiveToSemiJoinRuleSwapInputs<Aggregate> {

    protected HiveAggregateJoinToSemiJoinRuleSwapInputs() {
      super(Aggregate.class, HiveRelFactories.HIVE_BUILDER);
    }

    @Override
    protected ImmutableBitSet extractUsedFields(Aggregate aggregate) {
      return HiveCalciteUtil.extractRefs(aggregate);
    }

    @Override
    protected Aggregate recreateTopOperator(RelBuilder builder, RexBuilder rexBuilder,
        int[] adjustments, Aggregate topAggregate, RelNode newInputOperator) {
      return (Aggregate) recreateAggregateOperator(builder, adjustments, topAggregate, newInputOperator);
    }

    @Override
    protected RelNode recreateTopOperatorUnforced(RelBuilder builder, RexBuilder rexBuilder,
        int[] adjustments, Aggregate topAggregate, RelNode newInputOperator) {
      return recreateAggregateOperator(builder, adjustments, topAggregate, newInputOperator);
    }
  }

  protected static RelNode recreateProjectOperator(RelBuilder builder, RexBuilder rexBuilder,
      int[] adjustments, Project topProject, RelNode newInputOperator, boolean force) {
    List<RexNode> newProjects = new ArrayList<>();

    List<RelDataTypeField> swappedJoinFields = newInputOperator.getRowType().getFieldList();
    for(RexNode project : topProject.getProjects()) {
      RexNode newProject = project.accept(
          new RelOptUtil.RexInputConverter(rexBuilder, swappedJoinFields, swappedJoinFields, adjustments));
      newProjects.add(newProject);
    }
    return builder
        .push(newInputOperator)
        .project(newProjects, ImmutableList.of(), force)
        .build();
  }

  protected static RelNode recreateAggregateOperator(RelBuilder builder,
      int[] adjustments, Aggregate topAggregate, RelNode newInputOperator) {
    builder.push(newInputOperator);

    ImmutableBitSet.Builder newGroupSet = ImmutableBitSet.builder();
    for (int pos : topAggregate.getGroupSet()) {
      newGroupSet.set(pos + adjustments[pos]);
    }
    GroupKey groupKey;
    if (topAggregate.getGroupType() == Group.SIMPLE) {
      groupKey = builder.groupKey(newGroupSet.build());
    } else {
      List<ImmutableBitSet> newGroupSets = new ArrayList<>();
      for (ImmutableBitSet groupingSet : topAggregate.getGroupSets()) {
        ImmutableBitSet.Builder newGroupingSet = ImmutableBitSet.builder();
        for (int pos : groupingSet) {
          newGroupingSet.set(pos + adjustments[pos]);
        }
        newGroupSets.add(newGroupingSet.build());
      }
      groupKey = builder.groupKey(newGroupSet.build(), newGroupSets);
    }

    List<AggregateCall> newAggCallList = new ArrayList<>();
    for (AggregateCall aggregateCall : topAggregate.getAggCallList()) {
      List<Integer> newArgList = aggregateCall.getArgList()
          .stream()
          .map(pos -> pos + adjustments[pos])
          .collect(Collectors.toList());
      int newFilterArg = aggregateCall.filterArg != -1
          ? aggregateCall.filterArg + adjustments[aggregateCall.filterArg]
          : -1;
      RelCollation newCollation = aggregateCall.getCollation() != null
          ? RelCollations.of(
          aggregateCall.getCollation().getFieldCollations()
              .stream()
              .map(fc -> fc.withFieldIndex(fc.getFieldIndex() + adjustments[fc.getFieldIndex()]))
              .collect(Collectors.toList()))
          : null;
      newAggCallList.add(aggregateCall.copy(newArgList, newFilterArg, newCollation));
    }

    return builder
        .push(newInputOperator)
        .aggregate(groupKey, newAggCallList)
        .build();
  }
}
