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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;

import com.google.common.collect.Lists;

/**
 * Planner rule that reduces aggregate functions in
 * {@link org.apache.calcite.rel.core.Aggregate}s to simpler forms.
 *
 * <p>Rewrites:
 * <ul>
 *
 * <li>COUNT(x) &rarr; COUNT(*) if x is not nullable
 * </ul>
 */
public class HiveAggregateReduceRule extends RelOptRule {

  /** The singleton. */
  public static final HiveAggregateReduceRule INSTANCE =
      new HiveAggregateReduceRule();

  /** Private constructor. */
  private HiveAggregateReduceRule() {
    super(operand(HiveAggregate.class, any()),
            HiveRelFactories.HIVE_BUILDER, null);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final RelBuilder relBuilder = call.builder();
    final Aggregate aggRel = (Aggregate) call.rel(0);
    final RexBuilder rexBuilder = aggRel.getCluster().getRexBuilder();

    // We try to rewrite COUNT(x) into COUNT(*) if x is not nullable.
    // We remove duplicate aggregate calls as well.
    boolean rewrite = false;
    boolean identity = true;
    final Map<AggregateCall, Integer> mapping = new HashMap<>();
    final List<Integer> indexes = new ArrayList<>();
    final List<AggregateCall> aggCalls = aggRel.getAggCallList();
    final List<AggregateCall> newAggCalls = new ArrayList<>(aggCalls.size());
    int nextIdx = aggRel.getGroupCount() + aggRel.getIndicatorCount();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall aggCall = aggCalls.get(i);
      if (aggCall.getAggregation().getKind() == SqlKind.COUNT && !aggCall.isDistinct()) {
        final List<Integer> args = aggCall.getArgList();
        final List<Integer> nullableArgs = new ArrayList<>(args.size());
        for (int arg : args) {
          if (aggRel.getInput().getRowType().getFieldList().get(arg).getType().isNullable()) {
            nullableArgs.add(arg);
          }
        }
        if (nullableArgs.size() != args.size()) {
          aggCall = aggCall.copy(nullableArgs, aggCall.filterArg);
          rewrite = true;
        }
      }
      Integer idx = mapping.get(aggCall);
      if (idx == null) {
        newAggCalls.add(aggCall);
        idx = nextIdx++;
        mapping.put(aggCall, idx);
      } else {
        rewrite = true;
        identity = false;
      }
      indexes.add(idx);
    }

    if (rewrite) {
      // We trigger the transform
      final Aggregate newAggregate = aggRel.copy(aggRel.getTraitSet(), aggRel.getInput(),
              aggRel.indicator, aggRel.getGroupSet(), aggRel.getGroupSets(),
              newAggCalls);
      if (identity) {
        call.transformTo(newAggregate);
      } else {
        final int offset = aggRel.getGroupCount() + aggRel.getIndicatorCount();
        final List<RexNode> projList = Lists.newArrayList();
        for (int i = 0; i < offset; ++i) {
          projList.add(
              rexBuilder.makeInputRef(
                  aggRel.getRowType().getFieldList().get(i).getType(), i));
        }
        for (int i = offset; i < aggRel.getRowType().getFieldCount(); ++i) {
          projList.add(
              rexBuilder.makeInputRef(
                  aggRel.getRowType().getFieldList().get(i).getType(), indexes.get(i-offset)));
        }
        call.transformTo(relBuilder.push(newAggregate).project(projList).build());
      }
    }
  }

}
