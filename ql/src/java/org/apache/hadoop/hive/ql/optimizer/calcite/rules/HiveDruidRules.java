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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.adapter.druid.DruidRules.DruidAggregateProjectRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidAggregateRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidFilterRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidHavingFilterRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidPostAggregationProjectRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidProjectRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidSortRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.rules.AggregateFilterTransposeRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Druid rules with Hive builder factory.
 *
 * Simplify this class when upgrading to Calcite 1.26 using
 * <a href="https://issues.apache.org/jira/browse/CALCITE-4200">CALCITE-4200</a>
 */
public class HiveDruidRules {

  public static final DruidFilterRule FILTER = RelRule.Config.EMPTY
    .withOperandSupplier(b0 ->
      b0.operand(Filter.class).oneInput(b1 ->
        b1.operand(DruidQuery.class).noInputs()))
    .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
    .as(DruidFilterRule.Config.class)
    .toRule();

  public static final DruidProjectRule PROJECT = RelRule.Config.EMPTY
    .withOperandSupplier(b0 ->
      b0.operand(Project.class).oneInput(b1 ->
        b1.operand(DruidQuery.class).noInputs()))
    .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
    .as(DruidProjectRule.Config.class)
    .toRule();

  public static final DruidAggregateRule AGGREGATE = RelRule.Config.EMPTY
    .withOperandSupplier(b0 ->
      b0.operand(Aggregate.class).oneInput(b1 ->
        b1.operand(DruidQuery.class).noInputs()))
    .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
    .as(DruidAggregateRule.Config.class)
    .toRule();

  public static final DruidAggregateProjectRule AGGREGATE_PROJECT = RelRule.Config.EMPTY
    .withOperandSupplier(b0 ->
      b0.operand(Aggregate.class).oneInput(b1 ->
        b1.operand(Project.class).oneInput(b2 ->
          b2.operand(DruidQuery.class).noInputs())))
    .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
    .as(DruidAggregateProjectRule.Config.class)
    .toRule();

  public static final DruidSortRule SORT = RelRule.Config.EMPTY
    .withOperandSupplier(b0 ->
      b0.operand(Sort.class).oneInput(b1 ->
        b1.operand(DruidQuery.class).noInputs()))
    .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
    .as(DruidSortRule.Config.class)
    .toRule();

  public static final SortProjectTransposeRule SORT_PROJECT_TRANSPOSE = SortProjectTransposeRule.Config.DEFAULT
    .withOperandFor(Sort.class, Project.class, DruidQuery.class)
    .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
    .as(SortProjectTransposeRule.Config.class)
    .toRule();

  public static final ProjectFilterTransposeRule PROJECT_FILTER_TRANSPOSE = ProjectFilterTransposeRule.Config.DEFAULT
    .withOperandFor(Project.class, Filter.class, DruidQuery.class)
    .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
    .as(ProjectFilterTransposeRule.Config.class)
    .toRule();

  public static final FilterProjectTransposeRule FILTER_PROJECT_TRANSPOSE = CoreRules.FILTER_PROJECT_TRANSPOSE.config
    .withOperandFor(Filter.class, Project.class, DruidQuery.class)
    .withCopyFilter(true)
    .withCopyProject(true)
    .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
    .as(FilterProjectTransposeRule.Config.class)
    .toRule();

  public static final AggregateFilterTransposeRule AGGREGATE_FILTER_TRANSPOSE =
    CoreRules.AGGREGATE_FILTER_TRANSPOSE.config
      .withOperandFor(Aggregate.class, Filter.class, DruidQuery.class)
      .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
      .as(AggregateFilterTransposeRule.Config.class)
      .toRule();

  public static final FilterAggregateTransposeRule FILTER_AGGREGATE_TRANSPOSE =
    CoreRules.FILTER_AGGREGATE_TRANSPOSE.config
      .withOperandFor(Filter.class, Aggregate.class, DruidQuery.class)
      .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
      .as(FilterAggregateTransposeRule.Config.class)
      .toRule();

  public static final DruidPostAggregationProjectRule POST_AGGREGATION_PROJECT = RelRule.Config.EMPTY
    .withOperandSupplier(b0 ->
      b0.operand(Project.class).oneInput(b1 ->
        b1.operand(DruidQuery.class).noInputs()))
    .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
    .as(DruidPostAggregationProjectRule.Config.class)
    .toRule();

  public static final DruidHavingFilterRule HAVING_FILTER_RULE = RelRule.Config.EMPTY
    .withOperandSupplier(b0 ->
      b0.operand(Filter.class).oneInput(b1 ->
        b1.operand(DruidQuery.class).noInputs()))
    .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
    .as(DruidHavingFilterRule.Config.class)
    .toRule();

  public static final AggregateExpandDistinctAggregatesDruidRule EXPAND_SINGLE_DISTINCT_AGGREGATES_DRUID_RULE =
      new AggregateExpandDistinctAggregatesDruidRule(HiveRelFactories.HIVE_BUILDER);

  public static final DateRangeRules.FilterDateRangeRule FILTER_DATE_RANGE_RULE =
                new DateRangeRules.FilterDateRangeRule(HiveRelFactories.HIVE_BUILDER);

  /**
   * This is a simplified version of {@link org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule}
   * The goal of this simplified version is to help pushing single count distinct as multi-phase aggregates.
   * This is an okay solution before we actually support grouping sets push-down to Druid.
   * We are limiting it to one Distinct count to avoid expensive cross join and running into issue
   * https://issues.apache.org/jira/browse/HIVE-19601
   */
  public static class AggregateExpandDistinctAggregatesDruidRule extends RelOptRule {

    public AggregateExpandDistinctAggregatesDruidRule(RelBuilderFactory relBuilderFactory) {
      super(operand(Aggregate.class, operand(DruidQuery.class, none())), relBuilderFactory,
          null
      );
    }
    @Override public void onMatch(RelOptRuleCall call) {
      Aggregate aggregate =  call.rel(0);
      if (!aggregate.containsDistinctCall()) {
        return;
      }
      final long numCountDistinct = aggregate.getAggCallList()
          .stream()
          .filter(aggregateCall -> aggregateCall.getAggregation().getKind().equals(SqlKind.COUNT) &&
              aggregateCall.isDistinct())
          .count();
      if (numCountDistinct != 1) {
        return;
      }

      // Find all of the agg expressions. We use a LinkedHashSet to ensure determinism.
      int nonDistinctAggCallCount = 0;  // find all aggregate calls without distinct
      int filterCount = 0;
      int unsupportedNonDistinctAggCallCount = 0;
      final Set<Pair<List<Integer>, Integer>> argLists = new LinkedHashSet<>();
      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        if (aggCall.filterArg >= 0) {
          ++filterCount;
        }
        if (!aggCall.isDistinct()) {
          ++nonDistinctAggCallCount;
          final SqlKind aggCallKind = aggCall.getAggregation().getKind();
          // We only support COUNT/SUM/MIN/MAX for the "single" count distinct optimization
          switch (aggCallKind) {
          case COUNT:
          case SUM:
          case SUM0:
          case MIN:
          case MAX:
            break;
          default:
            ++unsupportedNonDistinctAggCallCount;
          }
        } else {
          argLists.add(Pair.of(aggCall.getArgList(), aggCall.filterArg));
        }
      }
      // If only one distinct aggregate and one or more non-distinct aggregates,
      // we can generate multi-phase aggregates
      if (numCountDistinct == 1 // one distinct aggregate
          && filterCount == 0 // no filter
          && unsupportedNonDistinctAggCallCount == 0 // sum/min/max/count in non-distinct aggregate
          && nonDistinctAggCallCount > 0) { // one or more non-distinct aggregates
        final RelBuilder relBuilder = call.builder();
        convertSingletonDistinct(relBuilder, aggregate, argLists);
        call.transformTo(relBuilder.build());
        return;
      }
    }

    /**
     * Converts an aggregate with one distinct aggregate and one or more
     * non-distinct aggregates to multi-phase aggregates (see reference example
     * below).
     *
     * @param relBuilder Contains the input relational expression
     * @param aggregate  Original aggregate
     * @param argLists   Arguments and filters to the distinct aggregate function
     *
     */
    private RelBuilder convertSingletonDistinct(RelBuilder relBuilder,
        Aggregate aggregate, Set<Pair<List<Integer>, Integer>> argLists) {

      // In this case, we are assuming that there is a single distinct function.
      // So make sure that argLists is of size one.
      Preconditions.checkArgument(argLists.size() == 1);

      // For example,
      //    SELECT deptno, COUNT(*), SUM(bonus), MIN(DISTINCT sal)
      //    FROM emp
      //    GROUP BY deptno
      //
      // becomes
      //
      //    SELECT deptno, SUM(cnt), SUM(bonus), MIN(sal)
      //    FROM (
      //          SELECT deptno, COUNT(*) as cnt, SUM(bonus), sal
      //          FROM EMP
      //          GROUP BY deptno, sal)            // Aggregate B
      //    GROUP BY deptno                        // Aggregate A
      relBuilder.push(aggregate.getInput());

      final List<AggregateCall> originalAggCalls = aggregate.getAggCallList();
      final ImmutableBitSet originalGroupSet = aggregate.getGroupSet();

      // Add the distinct aggregate column(s) to the group-by columns,
      // if not already a part of the group-by
      final SortedSet<Integer> bottomGroupSet = new TreeSet<>();
      bottomGroupSet.addAll(aggregate.getGroupSet().asList());
      for (AggregateCall aggCall : originalAggCalls) {
        if (aggCall.isDistinct()) {
          bottomGroupSet.addAll(aggCall.getArgList());
          break;  // since we only have single distinct call
        }
      }

      // Generate the intermediate aggregate B, the one on the bottom that converts
      // a distinct call to group by call.
      // Bottom aggregate is the same as the original aggregate, except that
      // the bottom aggregate has converted the DISTINCT aggregate to a group by clause.
      final List<AggregateCall> bottomAggregateCalls = new ArrayList<>();
      for (AggregateCall aggCall : originalAggCalls) {
        // Project the column corresponding to the distinct aggregate. Project
        // as-is all the non-distinct aggregates
        if (!aggCall.isDistinct()) {
          final AggregateCall newCall =
              AggregateCall.create(aggCall.getAggregation(), false,
                  aggCall.isApproximate(), aggCall.getArgList(), -1,
                  ImmutableBitSet.of(bottomGroupSet).cardinality(),
                  relBuilder.peek(), null, aggCall.name);
          bottomAggregateCalls.add(newCall);
        }
      }
      // Generate the aggregate B (see the reference example above)
      relBuilder.push(
          aggregate.copy(
              aggregate.getTraitSet(), relBuilder.build(),
              false, ImmutableBitSet.of(bottomGroupSet), null, bottomAggregateCalls));

      // Add aggregate A (see the reference example above), the top aggregate
      // to handle the rest of the aggregation that the bottom aggregate hasn't handled
      final List<AggregateCall> topAggregateCalls = Lists.newArrayList();
      // Use the remapped arguments for the (non)distinct aggregate calls
      int nonDistinctAggCallProcessedSoFar = 0;
      for (AggregateCall aggCall : originalAggCalls) {
        final AggregateCall newCall;
        if (aggCall.isDistinct()) {
          List<Integer> newArgList = new ArrayList<>();
          for (int arg : aggCall.getArgList()) {
            newArgList.add(bottomGroupSet.headSet(arg).size());
          }
          newCall =
              AggregateCall.create(aggCall.getAggregation(),
                  false,
                  aggCall.isApproximate(),
                  newArgList,
                  -1,
                  originalGroupSet.cardinality(),
                  relBuilder.peek(),
                  aggCall.getType(),
                  aggCall.name);
        } else {
          // If aggregate B had a COUNT aggregate call the corresponding aggregate at
          // aggregate A must be SUM. For other aggregates, it remains the same.
          final List<Integer> newArgs =
              Lists.newArrayList(bottomGroupSet.size() + nonDistinctAggCallProcessedSoFar);
          if (aggCall.getAggregation().getKind() == SqlKind.COUNT) {
            newCall =
                AggregateCall.create(new SqlSumEmptyIsZeroAggFunction(), false,
                    aggCall.isApproximate(), newArgs, -1,
                    originalGroupSet.cardinality(), relBuilder.peek(),
                    aggCall.getType(), aggCall.getName());
          } else {
            newCall =
                AggregateCall.create(aggCall.getAggregation(), false,
                    aggCall.isApproximate(), newArgs, -1,
                    originalGroupSet.cardinality(),
                    relBuilder.peek(), aggCall.getType(), aggCall.name);
          }
          nonDistinctAggCallProcessedSoFar++;
        }

        topAggregateCalls.add(newCall);
      }

      // Populate the group-by keys with the remapped arguments for aggregate A
      // The top groupset is basically an identity (first X fields of aggregate B's
      // output), minus the distinct aggCall's input.
      final Set<Integer> topGroupSet = new HashSet<>();
      int groupSetToAdd = 0;
      for (int bottomGroup : bottomGroupSet) {
        if (originalGroupSet.get(bottomGroup)) {
          topGroupSet.add(groupSetToAdd);
        }
        groupSetToAdd++;
      }
      relBuilder.push(
          aggregate.copy(aggregate.getTraitSet(),
              relBuilder.build(), aggregate.indicator,
              ImmutableBitSet.of(topGroupSet), null, topAggregateCalls));
      return relBuilder;
    }
  }
}
