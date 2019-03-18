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
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;

/**
 * Planner rule that removes
 * a {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit}.
 * Note that this is different from HiveSortRemoveRule because this is not based on statistics
 */
public final class HiveSortLimitRemoveRule extends RelOptRule {

  public static final HiveSortLimitRemoveRule INSTANCE =
      new HiveSortLimitRemoveRule();

  private HiveSortLimitRemoveRule() {
    super(operand(HiveSortLimit.class, any()), HiveRelFactories.HIVE_BUILDER, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveSortLimit sortLimit = call.rel(0);

    Double maxRowCount = call.getMetadataQuery().getMaxRowCount(sortLimit.getInput());
    if (maxRowCount != null &&(maxRowCount <= 1)) {
      return true;
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final HiveSortLimit sortLimit = call.rel(0);

    // We remove the limit operator
    call.transformTo(sortLimit.getInput());
  }
}
