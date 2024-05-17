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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveGroupingID;

/**
 * Rule that matches an aggregate with grouping sets and splits it into an aggregate
 * without grouping sets (bottom) and an aggregate with grouping sets (top).
 */
public class HiveAggregateSplitRule extends RelOptRule {

  public static final HiveAggregateSplitRule INSTANCE =
      new HiveAggregateSplitRule(HiveAggregate.class, HiveRelFactories.HIVE_BUILDER);

  private HiveAggregateSplitRule(Class<? extends Aggregate> aggregateClass,
      RelBuilderFactory relBuilderFactory) {
    super(
        operandJ(aggregateClass, null, agg -> agg.getGroupType() != Group.SIMPLE, any()),
        relBuilderFactory, null);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelBuilder relBuilder = call.builder();

    // If any aggregate is distinct, bail out
    // If any aggregate is the grouping id, bail out
    // If any aggregate call has a filter, bail out
    // If any aggregate functions do not support splitting, bail out
    final ImmutableBitSet bottomAggregateGroupSet = aggregate.getGroupSet();
    final List<AggregateCall> topAggregateCalls = new ArrayList<>();
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      AggregateCall aggregateCall = aggregate.getAggCallList().get(i);
      if (aggregateCall.isDistinct()) {
        return;
      }
      if (aggregateCall.getAggregation().equals(HiveGroupingID.INSTANCE)) {
        return;
      }
      if (aggregateCall.filterArg >= 0) {
        return;
      }
      SqlAggFunction aggFunction =
          HiveRelBuilder.getRollup(aggregateCall.getAggregation());
      if (aggFunction == null) {
        return;
      }
      topAggregateCalls.add(
          AggregateCall.create(aggFunction,
              aggregateCall.isDistinct(), aggregateCall.isApproximate(),
              ImmutableList.of(bottomAggregateGroupSet.cardinality() + i), -1,
              aggregateCall.collation, aggregateCall.type,
              aggregateCall.name));
    }

    final Boolean isUnique =
        aggregate.getCluster().getMetadataQuery().areColumnsUnique(aggregate.getInput(), bottomAggregateGroupSet);
    if (isUnique != null && isUnique) {
      // Nothing to do, probably already pushed
      return;
    }

    final ImmutableBitSet topAggregateGroupSet = ImmutableBitSet.range(0, bottomAggregateGroupSet.cardinality());

    final Map<Integer, Integer> map = new HashMap<>();
    bottomAggregateGroupSet.forEach(k -> map.put(k, map.size()));
    ImmutableList<ImmutableBitSet> topAggregateGroupSets = ImmutableBitSet.ORDERING.immutableSortedCopy(
        ImmutableBitSet.permute(aggregate.groupSets, map));

    relBuilder.push(aggregate.getInput())
        .aggregate(relBuilder.groupKey(bottomAggregateGroupSet), aggregate.getAggCallList())
        .aggregate(relBuilder.groupKey(topAggregateGroupSet, topAggregateGroupSets), topAggregateCalls);

    call.transformTo(relBuilder.build());
  }

}
