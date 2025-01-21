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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;

import com.google.common.collect.ImmutableList;

import static java.util.Collections.singletonList;

public class HiveFilterSortTransposeRule<T extends RelNode> extends RelOptRule {

  public static final HiveFilterSortTransposeRule<HiveSortLimit> SORT_LIMIT_INSTANCE =
      new HiveFilterSortTransposeRule<>(HiveSortLimit.class);
  public static final HiveFilterSortTransposeRule<HiveSortExchange> SORT_EXCHANGE_INSTANCE =
      new HiveFilterSortTransposeRule<>(HiveSortExchange.class);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a HiveFilterSortTransposeRule.
   */
  private HiveFilterSortTransposeRule(Class<T> clazz) {
    super(
        operand(
            HiveFilter.class,
            operand(clazz, any())));
  }

  //~ Methods ----------------------------------------------------------------

  public boolean matches(RelOptRuleCall call) {
    final T sort = call.rel(1);

    // If sort contains a limit operation, we bail out
    if (HiveCalciteUtil.limitRelNode(sort)) {
      return false;
    }

    return true;
  }

  public void onMatch(RelOptRuleCall call) {
    final HiveFilter filter = call.rel(0);
    final T sort = call.rel(1);

    final RelNode newFilter = filter.copy(sort.getInput(0).getTraitSet(),
            ImmutableList.of(sort.getInput(0)));
    final RelNode newSort = sort.copy(sort.getTraitSet(), singletonList(newFilter));

    call.transformTo(newSort);
  }
}
