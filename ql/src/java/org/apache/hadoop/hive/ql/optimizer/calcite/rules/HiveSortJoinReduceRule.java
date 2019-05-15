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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;

/**
 * Planner rule that pushes
 * a {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit}
 * past a {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin}.
 */
public class HiveSortJoinReduceRule extends RelOptRule {

  public static final HiveSortJoinReduceRule INSTANCE =
          new HiveSortJoinReduceRule();

  //~ Constructors -----------------------------------------------------------

  private HiveSortJoinReduceRule() {
    super(
        operand(
            HiveSortLimit.class,
            operand(HiveJoin.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveSortLimit sortLimit = call.rel(0);
    final HiveJoin join = call.rel(1);

    // If sort does not contain a limit operation or limit is 0, we bail out
    if (!HiveCalciteUtil.limitRelNode(sortLimit) ||
            RexLiteral.intValue(sortLimit.fetch) == 0) {
      return false;
    }

    // 1) If join is not a left or right outer, we bail out
    // 2) If any sort column is not part of the input where the
    // sort is pushed, we bail out
    RelNode reducedInput;
    if (join.getJoinType() == JoinRelType.LEFT) {
      reducedInput = join.getLeft();
      if (sortLimit.getCollation() != RelCollations.EMPTY) {
        for (RelFieldCollation relFieldCollation
            : sortLimit.getCollation().getFieldCollations()) {
          if (relFieldCollation.getFieldIndex()
              >= join.getLeft().getRowType().getFieldCount()) {
            return false;
          }
        }
      }
    } else if (join.getJoinType() == JoinRelType.RIGHT) {
      reducedInput = join.getRight();
      if (sortLimit.getCollation() != RelCollations.EMPTY) {
        for (RelFieldCollation relFieldCollation
            : sortLimit.getCollation().getFieldCollations()) {
          if (relFieldCollation.getFieldIndex()
              < join.getLeft().getRowType().getFieldCount()) {
            return false;
          }
        }
      }
    } else {
      return false;
    }

    // Finally, if we do not reduce the input size, we bail out
    final int offset = sortLimit.offset == null ? 0 : RexLiteral.intValue(sortLimit.offset);
    final RelMetadataQuery mq = call.getMetadataQuery();
    if (offset + RexLiteral.intValue(sortLimit.fetch)
            >= mq.getRowCount(reducedInput)) {
      return false;
    }

    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final HiveSortLimit sortLimit = call.rel(0);
    final HiveJoin join = call.rel(1);
    RelNode inputLeft = join.getLeft();
    RelNode inputRight = join.getRight();

    // We create a new sort operator on the corresponding input
    if (join.getJoinType() == JoinRelType.LEFT) {
      inputLeft = sortLimit.copy(sortLimit.getTraitSet(), inputLeft,
              sortLimit.getCollation(), sortLimit.offset, sortLimit.fetch);
      ((HiveSortLimit) inputLeft).setRuleCreated(true);
    } else {
      // Adjust right collation
      final RelCollation rightCollation =
              RelCollationTraitDef.INSTANCE.canonize(
                  RelCollations.shift(sortLimit.getCollation(),
                      -join.getLeft().getRowType().getFieldCount()));
      inputRight = sortLimit.copy(sortLimit.getTraitSet().replace(rightCollation), inputRight,
              rightCollation, sortLimit.offset, sortLimit.fetch);
      ((HiveSortLimit) inputRight).setRuleCreated(true);
    }
    // We copy the join and the top sort operator
    RelNode result = join.copy(join.getTraitSet(), join.getCondition(), inputLeft,
            inputRight, join.getJoinType(), join.isSemiJoinDone());
    result = sortLimit.copy(sortLimit.getTraitSet(), result, sortLimit.getCollation(),
            sortLimit.offset, sortLimit.fetch);

    call.transformTo(result);
  }

}
