/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;

/**
 * This rule will merge two HiveSortLimit operators.
 * 
 * It is applied when the top match is a pure limit operation (no sorting).
 * 
 * If the bottom operator is not synthetic and does not contain a limit,
 * we currently bail out. Thus, we avoid a lot of unnecessary limit operations
 * in the middle of the execution plan that could create performance regressions.
 */
public class HiveSortMergeRule extends RelOptRule {

  public static final HiveSortMergeRule INSTANCE =
      new HiveSortMergeRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a HiveSortProjectTransposeRule.
   */
  private HiveSortMergeRule() {
    super(
        operand(
            HiveSortLimit.class,
            operand(HiveSortLimit.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveSortLimit topSortLimit = call.rel(0);
    final HiveSortLimit bottomSortLimit = call.rel(1);

    // If top operator is not a pure limit, we bail out
    if (!HiveCalciteUtil.pureLimitRelNode(topSortLimit)) {
      return false;
    }

    // If the bottom operator is not synthetic and it does not contain a limit,
    // we will bail out; we do not want to end up with limits all over the tree
    if (topSortLimit.isRuleCreated() && !bottomSortLimit.isRuleCreated() &&
            bottomSortLimit.fetch == null) {
      return false;
    }

    return true;
  }

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    final HiveSortLimit topSortLimit = call.rel(0);
    final HiveSortLimit bottomSortLimit = call.rel(1);

    // Lowest limit
    final RexNode newLimit;
    if (bottomSortLimit.fetch != null && RexLiteral.intValue(topSortLimit.fetch)
            >= RexLiteral.intValue(bottomSortLimit.fetch)) {
      newLimit = bottomSortLimit.fetch;
    } else {
      newLimit = topSortLimit.fetch;
    }

    final HiveSortLimit newSort = bottomSortLimit.copy(bottomSortLimit.getTraitSet(),
            bottomSortLimit.getInput(), bottomSortLimit.collation, null, newLimit);

    call.transformTo(newSort);
  }

}
