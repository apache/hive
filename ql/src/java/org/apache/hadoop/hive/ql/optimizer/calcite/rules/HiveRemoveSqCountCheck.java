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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Planner rule that removes UDF sq_count_check from a plan if
 *  1) either group by keys in a subquery are constant and there is no windowing or grouping sets
 *  2) OR there are no group by keys but only aggregate
 *  Both of the above case will produce at most one row, therefore it is safe to remove sq_count_check
 *    which was introduced earlier in the plan to ensure that this condition is met at run time
 */
public class HiveRemoveSqCountCheck extends RelOptRule {

  public static final HiveRemoveSqCountCheck INSTANCE =
          new HiveRemoveSqCountCheck();

  //match if there is filter (sq_count_check) as right input of a join which is left
  // input of another join
  public HiveRemoveSqCountCheck() {
    super(operand(Join.class,
        some(
            operand(Project.class,
                operand(Join.class,
                    some(
                        operand(RelNode.class, any()),
                        operand(Filter.class, any())))
            ),
            operand(Project.class,
                operand(Aggregate.class,
                    any()))
        )
    ), HiveRelFactories.HIVE_BUILDER, "HiveRemoveSqCountCheck");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final RelNode filter = call.rel(4);
    if(filter instanceof HiveFilter) {
      HiveFilter hiveFilter = (HiveFilter)filter;
      // check if it has sq_count_check
      return isSqlCountCheck(hiveFilter);
    }
    return false;
  }

  private boolean isSqlCountCheck(final HiveFilter filter) {
    // look at hivesubqueryremoverule to see how is this filter created
    if(filter.getCondition() instanceof RexCall) {
      final RexCall condition = (RexCall)filter.getCondition();
      return condition.getOperator().getName().equals("sq_count_check");
    }
    return false;
  }

  private boolean isAggregateWithoutGbyKeys(final Aggregate agg) {
    return agg.getGroupCount() == 0;
  }

  private boolean isAggWithConstantGbyKeys(final Aggregate aggregate, RelOptRuleCall call) {
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelOptPredicateList predicates =
        mq.getPulledUpPredicates(aggregate.getInput());
    if (predicates == null) {
      return false;
    }
    final NavigableMap<Integer, RexNode> map = new TreeMap<>();
    for (int key : aggregate.getGroupSet()) {
      final RexInputRef ref =
          rexBuilder.makeInputRef(aggregate.getInput(), key);
      if (predicates.constantMap.containsKey(ref)) {
        map.put(key, predicates.constantMap.get(ref));
      }
    }

    // None of the group expressions are constant. Nothing to do.
    if (map.isEmpty()) {
      return false;
    }

    final int groupCount = aggregate.getGroupCount();
    if (groupCount == map.size()) {
      return true;
    }
    return false;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Join topJoin= call.rel(0);
    final Join join = call.rel(2);
    final Aggregate aggregate = call.rel(6);

    // in presence of grouping sets we can't remove sq_count_check
    if(aggregate.indicator) {
      return;
    }
    if(isAggregateWithoutGbyKeys(aggregate) || isAggWithConstantGbyKeys(aggregate, call)) {
      // join(left, join.getRight)
      RelNode newJoin = HiveJoin.getJoin(topJoin.getCluster(), join.getLeft(),  topJoin.getRight(),
          topJoin.getCondition(), topJoin.getJoinType());
      call.transformTo(newJoin);
    }
  }
}
