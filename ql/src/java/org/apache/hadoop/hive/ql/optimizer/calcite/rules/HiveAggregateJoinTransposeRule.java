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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Planner rule that pushes an
 * {@link org.apache.calcite.rel.core.Aggregate}
 * past a {@link org.apache.calcite.rel.core.Join}.
 */
public class HiveAggregateJoinTransposeRule extends AggregateJoinTransposeRule {

  /** Extended instance of the rule that can push down aggregate functions. */
  public static final HiveAggregateJoinTransposeRule INSTANCE =
      new HiveAggregateJoinTransposeRule(HiveAggregate.class, HiveJoin.class,
          HiveRelFactories.HIVE_BUILDER, true);

  private final boolean allowFunctions;

  /** Creates an AggregateJoinTransposeRule that may push down functions. */
  private HiveAggregateJoinTransposeRule(Class<? extends Aggregate> aggregateClass,
      Class<? extends Join> joinClass,
      RelBuilderFactory relBuilderFactory,
      boolean allowFunctions) {
    super(aggregateClass, joinClass, relBuilderFactory, true);
    this.allowFunctions = allowFunctions;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Join join = call.rel(1);
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();

    // If any aggregate functions do not support splitting, bail out
    // If any aggregate call has a filter, bail out
    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      if (aggregateCall.getAggregation().unwrap(SqlSplittableAggFunction.class)
          == null) {
        return;
      }
      if (aggregateCall.filterArg >= 0) {
        return;
      }
    }

    // If it is not an inner join, we do not push the
    // aggregate operator
    if (join.getJoinType() != JoinRelType.INNER) {
      return;
    }

    if (!allowFunctions && !aggregate.getAggCallList().isEmpty()) {
      return;
    }

    // Do the columns used by the join appear in the output of the aggregate?
    final ImmutableBitSet aggregateColumns = aggregate.getGroupSet();
    final RelMetadataQuery mq = call.getMetadataQuery();
    final ImmutableBitSet keyColumns = keyColumns(aggregateColumns,
        mq.getPulledUpPredicates(join).pulledUpPredicates);
    final ImmutableBitSet joinColumns =
        RelOptUtil.InputFinder.bits(join.getCondition());
    final boolean allColumnsInAggregate =
        keyColumns.contains(joinColumns);
    final ImmutableBitSet belowAggregateColumns =
        aggregateColumns.union(joinColumns);

    // Split join condition
    final List<Integer> leftKeys = Lists.newArrayList();
    final List<Integer> rightKeys = Lists.newArrayList();
    final List<Boolean> filterNulls = Lists.newArrayList();
    RexNode nonEquiConj =
        RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(),
            join.getCondition(), leftKeys, rightKeys, filterNulls);
    // If it contains non-equi join conditions, we bail out
    if (!nonEquiConj.isAlwaysTrue()) {
      return;
    }

    // Push each aggregate function down to each side that contains all of its
    // arguments. Note that COUNT(*), because it has no arguments, can go to
    // both sides.
    final Map<Integer, Integer> map = new HashMap<>();
    final List<Side> sides = new ArrayList<>();
    int uniqueCount = 0;
    int offset = 0;
    int belowOffset = 0;
    for (int s = 0; s < 2; s++) {
      final Side side = new Side();
      final RelNode joinInput = join.getInput(s);
      int fieldCount = joinInput.getRowType().getFieldCount();
      final ImmutableBitSet fieldSet =
          ImmutableBitSet.range(offset, offset + fieldCount);
      final ImmutableBitSet belowAggregateKeyNotShifted =
          belowAggregateColumns.intersect(fieldSet);
      for (Ord<Integer> c : Ord.zip(belowAggregateKeyNotShifted)) {
        map.put(c.e, belowOffset + c.i);
      }
      final ImmutableBitSet belowAggregateKey =
          belowAggregateKeyNotShifted.shift(-offset);
      final boolean unique;
      if (!allowFunctions) {
        assert aggregate.getAggCallList().isEmpty();
        // If there are no functions, it doesn't matter as much whether we
        // aggregate the inputs before the join, because there will not be
        // any functions experiencing a cartesian product effect.
        //
        // But finding out whether the input is already unique requires a call
        // to areColumnsUnique that currently (until [CALCITE-1048] "Make
        // metadata more robust" is fixed) places a heavy load on
        // the metadata system.
        //
        // So we choose to imagine the the input is already unique, which is
        // untrue but harmless.
        //
        unique = true;
      } else {
        final Boolean unique0 =
            mq.areColumnsUnique(joinInput, belowAggregateKey);
        unique = unique0 != null && unique0;
      }
      if (unique) {
        ++uniqueCount;
        side.newInput = joinInput;
      } else {
        List<AggregateCall> belowAggCalls = new ArrayList<>();
        final SqlSplittableAggFunction.Registry<AggregateCall>
            belowAggCallRegistry = registry(belowAggCalls);
        final Mappings.TargetMapping mapping =
            s == 0
                ? Mappings.createIdentity(fieldCount)
                : Mappings.createShiftMapping(fieldCount + offset, 0, offset,
                    fieldCount);
        for (Ord<AggregateCall> aggCall : Ord.zip(aggregate.getAggCallList())) {
          final SqlAggFunction aggregation = aggCall.e.getAggregation();
          final SqlSplittableAggFunction splitter =
              Preconditions.checkNotNull(
                  aggregation.unwrap(SqlSplittableAggFunction.class));
          final AggregateCall call1;
          if (fieldSet.contains(ImmutableBitSet.of(aggCall.e.getArgList()))) {
            call1 = splitter.split(aggCall.e, mapping);
          } else {
            call1 = splitter.other(rexBuilder.getTypeFactory(), aggCall.e);
          }
          if (call1 != null) {
            side.split.put(aggCall.i,
                belowAggregateKey.cardinality()
                    + belowAggCallRegistry.register(call1));
          }
        }
        side.newInput = relBuilder.push(joinInput)
            .aggregate(relBuilder.groupKey(belowAggregateKey, null),
                belowAggCalls)
            .build();
      }
      offset += fieldCount;
      belowOffset += side.newInput.getRowType().getFieldCount();
      sides.add(side);
    }

    if (uniqueCount == 2) {
      // Both inputs to the join are unique. There is nothing to be gained by
      // this rule. In fact, this aggregate+join may be the result of a previous
      // invocation of this rule; if we continue we might loop forever.
      return;
    }

    // Update condition
    final Mapping mapping = (Mapping) Mappings.target(
        map::get,
        join.getRowType().getFieldCount(),
        belowOffset);
    final RexNode newCondition =
        RexUtil.apply(mapping, join.getCondition());

    // Create new join
    relBuilder.push(sides.get(0).newInput)
        .push(sides.get(1).newInput)
        .join(join.getJoinType(), newCondition);

    // Aggregate above to sum up the sub-totals
    final List<AggregateCall> newAggCalls = new ArrayList<>();
    final int groupIndicatorCount =
        aggregate.getGroupCount() + aggregate.getIndicatorCount();
    final int newLeftWidth = sides.get(0).newInput.getRowType().getFieldCount();
    final List<RexNode> projects =
        new ArrayList<>(
            rexBuilder.identityProjects(relBuilder.peek().getRowType()));
    for (Ord<AggregateCall> aggCall : Ord.zip(aggregate.getAggCallList())) {
      final SqlAggFunction aggregation = aggCall.e.getAggregation();
      final SqlSplittableAggFunction splitter =
          Preconditions.checkNotNull(
              aggregation.unwrap(SqlSplittableAggFunction.class));
      final Integer leftSubTotal = sides.get(0).split.get(aggCall.i);
      final Integer rightSubTotal = sides.get(1).split.get(aggCall.i);
      newAggCalls.add(
          splitter.topSplit(rexBuilder, registry(projects),
              groupIndicatorCount, relBuilder.peek().getRowType(), aggCall.e,
              leftSubTotal == null ? -1 : leftSubTotal,
              rightSubTotal == null ? -1 : rightSubTotal + newLeftWidth));
    }

    relBuilder.project(projects);

    boolean aggConvertedToProjects = false;
    if (allColumnsInAggregate) {
      // let's see if we can convert aggregate into projects
      List<RexNode> projects2 = new ArrayList<>();
      for (int key : Mappings.apply(mapping, aggregate.getGroupSet())) {
        projects2.add(relBuilder.field(key));
      }
      for (AggregateCall newAggCall : newAggCalls) {
        final SqlSplittableAggFunction splitter =
            newAggCall.getAggregation().unwrap(SqlSplittableAggFunction.class);
        if (splitter != null) {
          final RelDataType rowType = relBuilder.peek().getRowType();
          projects2.add(splitter.singleton(rexBuilder, rowType, newAggCall));
        }
      }
      if (projects2.size()
          == aggregate.getGroupSet().cardinality() + newAggCalls.size()) {
        // We successfully converted agg calls into projects.
        relBuilder.project(projects2);
        aggConvertedToProjects = true;
      }
    }

    if (!aggConvertedToProjects) {
      relBuilder.aggregate(
          relBuilder.groupKey(Mappings.apply(mapping, aggregate.getGroupSet()),
              Mappings.apply2(mapping, aggregate.getGroupSets())),
          newAggCalls);
    }

    // Make a cost based decision to pick cheaper plan
    RelNode r = relBuilder.build();
    RelOptCost afterCost = mq.getCumulativeCost(r);
    RelOptCost beforeCost = mq.getCumulativeCost(aggregate);
    if (afterCost.isLt(beforeCost)) {
      call.transformTo(r);
    }
  }

  /** Computes the closure of a set of columns according to a given list of
   * constraints. Each 'x = y' constraint causes bit y to be set if bit x is
   * set, and vice versa. */
  private static ImmutableBitSet keyColumns(ImmutableBitSet aggregateColumns,
      ImmutableList<RexNode> predicates) {
    SortedMap<Integer, BitSet> equivalence = new TreeMap<>();
    for (RexNode predicate : predicates) {
      populateEquivalences(equivalence, predicate);
    }
    ImmutableBitSet keyColumns = aggregateColumns;
    for (Integer aggregateColumn : aggregateColumns) {
      final BitSet bitSet = equivalence.get(aggregateColumn);
      if (bitSet != null) {
        keyColumns = keyColumns.union(bitSet);
      }
    }
    return keyColumns;
  }

  private static void populateEquivalences(Map<Integer, BitSet> equivalence,
      RexNode predicate) {
    switch (predicate.getKind()) {
    case EQUALS:
      RexCall call = (RexCall) predicate;
      final List<RexNode> operands = call.getOperands();
      if (operands.get(0) instanceof RexInputRef) {
        final RexInputRef ref0 = (RexInputRef) operands.get(0);
        if (operands.get(1) instanceof RexInputRef) {
          final RexInputRef ref1 = (RexInputRef) operands.get(1);
          populateEquivalence(equivalence, ref0.getIndex(), ref1.getIndex());
          populateEquivalence(equivalence, ref1.getIndex(), ref0.getIndex());
        }
      }
    }
  }

  private static void populateEquivalence(Map<Integer, BitSet> equivalence,
      int i0, int i1) {
    BitSet bitSet = equivalence.get(i0);
    if (bitSet == null) {
      bitSet = new BitSet();
      equivalence.put(i0, bitSet);
    }
    bitSet.set(i1);
  }

  /** Creates a {@link org.apache.calcite.sql.SqlSplittableAggFunction.Registry}
   * that is a view of a list. */
  private static <E> SqlSplittableAggFunction.Registry<E> registry(
      final List<E> list) {
    return new SqlSplittableAggFunction.Registry<E>() {
      public int register(E e) {
        int i = list.indexOf(e);
        if (i < 0) {
          i = list.size();
          list.add(e);
        }
        return i;
      }
    };
  }

  /** Work space for an input to a join. */
  private static class Side {
    final Map<Integer, Integer> split = new HashMap<>();
    RelNode newInput;
  }
}

// End AggregateJoinTransposeRule.java

