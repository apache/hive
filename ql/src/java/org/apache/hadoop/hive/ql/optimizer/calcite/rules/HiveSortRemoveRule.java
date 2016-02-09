/**
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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;

/**
 * Planner rule that removes
 * a {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit}.
 */
public class HiveSortRemoveRule extends RelOptRule {

  protected final float reductionProportion;
  protected final float reductionTuples;

  //~ Constructors -----------------------------------------------------------

  public HiveSortRemoveRule(float reductionProportion, long reductionTuples) {
    this(operand(HiveSortLimit.class, any()), reductionProportion, reductionTuples);
  }

  private HiveSortRemoveRule(RelOptRuleOperand operand, float reductionProportion,
          long reductionTuples) {
    super(operand);
    this.reductionProportion = reductionProportion;
    this.reductionTuples = reductionTuples;
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveSortLimit sortLimit = call.rel(0);

    // If it is not created by HiveSortJoinReduceRule, we cannot remove it
    if (!sortLimit.isRuleCreated()) {
      return false;
    }

    // Finally, if we do not reduce the size input enough, we bail out
    int limit = RexLiteral.intValue(sortLimit.fetch);
    Double rowCount = RelMetadataQuery.instance().getRowCount(sortLimit.getInput());
    if (rowCount != null && limit <= reductionProportion * rowCount &&
            rowCount - limit >= reductionTuples) {
      return false;
    }

    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final HiveSortLimit sortLimit = call.rel(0);

    // We remove the limit operator
    call.transformTo(sortLimit.getInput());
  }

}
