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
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;

import com.google.common.collect.ImmutableList;

public class HiveFilterSortTransposeRule extends RelOptRule {

  public static final HiveFilterSortTransposeRule INSTANCE =
      new HiveFilterSortTransposeRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a HiveFilterSortTransposeRule.
   */
  private HiveFilterSortTransposeRule() {
    super(
        operand(
            HiveFilter.class,
            operand(HiveSortLimit.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  public boolean matches(RelOptRuleCall call) {
    final HiveSortLimit sort = call.rel(1);

    // If sort contains a limit operation, we bail out
    if (HiveCalciteUtil.limitRelNode(sort)) {
      return false;
    }

    return true;
  }

  public void onMatch(RelOptRuleCall call) {
    final HiveFilter filter = call.rel(0);
    final HiveSortLimit sort = call.rel(1);

    final RelNode newFilter = filter.copy(sort.getInput().getTraitSet(),
            ImmutableList.<RelNode>of(sort.getInput()));
    final HiveSortLimit newSort = sort.copy(sort.getTraitSet(),
            newFilter, sort.collation, sort.offset, sort.fetch);

    call.transformTo(newSort);
  }

}
