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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;

public class HiveSortProjectTransposeRule extends SortProjectTransposeRule {

  public static final HiveSortProjectTransposeRule INSTANCE =
      new HiveSortProjectTransposeRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a HiveSortProjectTransposeRule.
   */
  private HiveSortProjectTransposeRule() {
    super(
        operand(
            HiveSortLimit.class,
            operand(HiveProject.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveSortLimit sortLimit = call.rel(0);

    // If does not contain a limit operation, we bail out
    if (!HiveCalciteUtil.limitRelNode(sortLimit)) {
      return false;
    }

    return true;
  }

}
