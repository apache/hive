/**
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveGroupingID;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Planner rule that recognizes a {@link HiveAggregate}
 * on top of a {@link HiveProject} and if possible
 * aggregate through the project or removes the project.
 *
 * <p>This is only possible when the grouping expressions and arguments to
 * the aggregate functions are field references (i.e. not expressions).
 *
 * <p>In some cases, this rule has the effect of trimming: the aggregate will
 * use fewer columns than the project did.
 */
public class HiveAggregateProjectMergeRule extends RelOptRule {
  public static final HiveAggregateProjectMergeRule INSTANCE =
      new HiveAggregateProjectMergeRule();

  /** Private constructor. */
  private HiveAggregateProjectMergeRule() {
    super(
        operand(HiveAggregate.class,
            operand(HiveProject.class, any())));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    // Rule cannot be applied if there are GroupingId because it will change the
    // value as the position will be changed.
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (aggCall.getAggregation().equals(HiveGroupingID.INSTANCE)) {
        return false;
      }
    }
    return super.matches(call);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final HiveAggregate aggregate = call.rel(0);
    final HiveProject project = call.rel(1);
    RelNode x = apply(aggregate, project);
    if (x != null) {
      call.transformTo(x);
    }
  }

  public static RelNode apply(HiveAggregate aggregate,
      HiveProject project) {
    final List<Integer> newKeys = Lists.newArrayList();
    final Map<Integer, Integer> map = new HashMap<>();
    for (int key : aggregate.getGroupSet()) {
      final RexNode rex = project.getProjects().get(key);
      if (rex instanceof RexInputRef) {
        final int newKey = ((RexInputRef) rex).getIndex();
        newKeys.add(newKey);
        map.put(key, newKey);
      } else {
        // Cannot handle "GROUP BY expression"
        return null;
      }
    }

    final ImmutableBitSet newGroupSet = aggregate.getGroupSet().permute(map);
    ImmutableList<ImmutableBitSet> newGroupingSets = null;
    if (aggregate.indicator) {
      newGroupingSets =
          ImmutableBitSet.ORDERING.immutableSortedCopy(
              ImmutableBitSet.permute(aggregate.getGroupSets(), map));
    }

    final ImmutableList.Builder<AggregateCall> aggCalls =
        ImmutableList.builder();
    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      final ImmutableList.Builder<Integer> newArgs = ImmutableList.builder();
      for (int arg : aggregateCall.getArgList()) {
        final RexNode rex = project.getProjects().get(arg);
        if (rex instanceof RexInputRef) {
          newArgs.add(((RexInputRef) rex).getIndex());
        } else {
          // Cannot handle "AGG(expression)"
          return null;
        }
      }
      final int newFilterArg;
      if (aggregateCall.filterArg >= 0) {
        final RexNode rex = project.getProjects().get(aggregateCall.filterArg);
        if (!(rex instanceof RexInputRef)) {
          return null;
        }
        newFilterArg = ((RexInputRef) rex).getIndex();
      } else {
        newFilterArg = -1;
      }
      aggCalls.add(aggregateCall.copy(newArgs.build(), newFilterArg));
    }

    final Aggregate newAggregate =
        aggregate.copy(aggregate.getTraitSet(), project.getInput(),
            aggregate.indicator, newGroupSet, newGroupingSets,
            aggCalls.build());

    // Add a project if the group set is not in the same order or
    // contains duplicates.
    RelNode rel = newAggregate;
    if (!newKeys.equals(newGroupSet.asList())) {
      final List<Integer> posList = Lists.newArrayList();
      for (int newKey : newKeys) {
        posList.add(newGroupSet.indexOf(newKey));
      }
      if (aggregate.indicator) {
        for (int newKey : newKeys) {
          posList.add(aggregate.getGroupCount() + newGroupSet.indexOf(newKey));
        }
      }
      for (int i = newAggregate.getGroupCount()
                   + newAggregate.getIndicatorCount();
           i < newAggregate.getRowType().getFieldCount(); i++) {
        posList.add(i);
      }
      rel = HiveRelOptUtil.createProject(
          HiveRelFactories.HIVE_BUILDER.create(aggregate.getCluster(), null),
          rel, posList);

    }

    return rel;
  }
}

// End AggregateProjectMergeRule.java
