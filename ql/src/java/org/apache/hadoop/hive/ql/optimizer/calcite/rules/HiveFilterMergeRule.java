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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.Bug;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;

/**
 * Mostly a copy of {@link org.apache.calcite.rel.rules.FilterMergeRule}.
 * However, it relies in relBuilder to create the new condition and thus
 * simplifies/flattens the predicate before creating the new filter.
 */
public class HiveFilterMergeRule extends RelOptRule {

  public static final HiveFilterMergeRule INSTANCE =
      new HiveFilterMergeRule();

  /** Private constructor. */
  private HiveFilterMergeRule() {
    super(operand(HiveFilter.class,
        operand(HiveFilter.class, any())),
        HiveRelFactories.HIVE_BUILDER, null);
    if (Bug.CALCITE_3982_FIXED) {
      throw new AssertionError("Remove logic in HiveFilterMergeRule when [CALCITE-3982] "
          + "has been fixed and use directly Calcite's FilterMergeRule instead.");
    }
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final HiveFilter topFilter = call.rel(0);
    final HiveFilter bottomFilter = call.rel(1);

    final RelBuilder relBuilder = call.builder();
    relBuilder.push(bottomFilter.getInput())
        .filter(bottomFilter.getCondition(), topFilter.getCondition());

    call.transformTo(relBuilder.build());
  }
}
