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
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
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
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Planner rule that pushes an
 * {@link org.apache.calcite.rel.core.Aggregate}
 * past a {@link org.apache.calcite.rel.core.Join}.
 */
public class HiveAggregateJoinTransposeRule extends AggregateJoinTransposeRule {

  private static final Logger LOG = LoggerFactory.getLogger(HiveAggregateJoinTransposeRule.class);

  private final boolean allowFunctions;
  private final AtomicInteger noColsMissingStats;
  private boolean costBased;
  private boolean uniqueBased;

  /** Creates an AggregateJoinTransposeRule that may push down functions.  */
  public HiveAggregateJoinTransposeRule(AtomicInteger noColsMissingStats, boolean costBased, boolean uniqueBased) {
    super(HiveAggregate.class, HiveJoin.class, HiveRelFactories.HIVE_BUILDER, true);
    this.costBased = costBased;
    this.uniqueBased = uniqueBased;
    this.allowFunctions = true;
    this.noColsMissingStats = noColsMissingStats;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    try {
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

      // If it is not an inner join or a semi-join, we do not push the
      // aggregate operator
      if (join.getJoinType() != JoinRelType.INNER) {
        return;
      }

      if (!allowFunctions && !aggregate.getAggCallList().isEmpty()) {
        return;
      }

      boolean groupingUnique = isGroupingUnique(join, aggregate.getGroupSet());

      if (!groupingUnique && !costBased) {
        // groupingUnique is not satisfied ; and cost based decision is disabled.
        // there is no need to check further - the transformation may not happen
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
              mq.areColumnsUnique(joinInput, belowAggregateKey, true);
          unique = unique0 != null && unique0;
        }
        if (unique) {
          ++uniqueCount;
          relBuilder.push(joinInput);
          relBuilder.project(belowAggregateKey.asList().stream().map(relBuilder::field).collect(Collectors.toList()));
          side.newInput = relBuilder.build();
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

      RelNode r = relBuilder.build();
      boolean transform = false;
      if (uniqueBased && aggConvertedToProjects) {
        transform = groupingUnique;
      }
      if (!transform && costBased) {
        RelOptCost afterCost = mq.getCumulativeCost(r);
        RelOptCost beforeCost = mq.getCumulativeCost(aggregate);
        transform = afterCost.isLt(beforeCost);
      }
      if (transform) {
        call.transformTo(r);
      }
    } catch (Exception e) {
      if (noColsMissingStats.get() > 0) {
        LOG.warn("Missing column stats (see previous messages), skipping aggregate-join transpose in CBO");
        noColsMissingStats.set(0);
      } else {
        throw e;
      }
    }
  }

  /**
   * Determines weather the give grouping is unique.
   *
   * Consider a join which might produce non-unique rows; but later the results are aggregated again.
   * This method determines if there are sufficient columns in the grouping which have been present previously as unique column(s).
   */
  private boolean isGroupingUnique(RelNode input, ImmutableBitSet groups) {
    if (groups.isEmpty()) {
      return false;
    }
    if(input instanceof HepRelVertex) {
      HepRelVertex vertex = (HepRelVertex) input;
      return isGroupingUnique(vertex.getCurrentRel(), groups);
    }

    RelMetadataQuery mq = input.getCluster().getMetadataQuery();
    Set<ImmutableBitSet> uKeys = mq.getUniqueKeys(input);
    if (uKeys == null) {
      return false;
    }
    for (ImmutableBitSet u : uKeys) {
      if (groups.contains(u)) {
        return true;
      }
    }
    if (input instanceof Join) {
      Join join = (Join) input;
      JoinInfo ji = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());
      if (ji.isEqui()) {
        ImmutableBitSet newGroup = groups.intersect(InputFinder.bits(join.getCondition()));
        RelNode l = join.getLeft();
        RelNode r = join.getRight();

        int joinFieldCount = join.getRowType().getFieldCount();
        int lFieldCount = l.getRowType().getFieldCount();

        ImmutableBitSet groupL = newGroup.get(0, lFieldCount);
        ImmutableBitSet groupR = newGroup.get(lFieldCount, joinFieldCount).shift(-lFieldCount);

        if (isGroupingUnique(l, groupL)) {
          return true;
        }
        if (isGroupingUnique(r, groupR)) {
          return true;
        }
      }
    }
    if (input instanceof Project) {
      Project project = (Project) input;
      ImmutableBitSet.Builder newGroup = ImmutableBitSet.builder();
      for (int g : groups.asList()) {
        RexNode rex = project.getProjects().get(g);
        if (rex instanceof RexInputRef) {
          RexInputRef rexInputRef = (RexInputRef) rex;
          newGroup.set(rexInputRef.getIndex());
        }
      }
      return isGroupingUnique(project.getInput(), newGroup.build());
    }
    return false;
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
    default:
      break;
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

