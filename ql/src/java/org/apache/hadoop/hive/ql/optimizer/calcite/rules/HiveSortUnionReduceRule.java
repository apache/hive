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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;

/**
 * Planner rule that pushes a
 * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit}
 * past a
 * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion}.
 */
public class HiveSortUnionReduceRule extends RelOptRule {

  /**
   * Rule instance for Union implementation that does not preserve the ordering
   * of its inputs. Thus, it makes no sense to match this rule if the Sort does
   * not have a limit, i.e., {@link Sort#fetch} is null.
   */
  public static final HiveSortUnionReduceRule INSTANCE = new HiveSortUnionReduceRule();

  // ~ Constructors -----------------------------------------------------------

  private HiveSortUnionReduceRule() {
    super(
        operand(
            HiveSortLimit.class,
            operand(HiveUnion.class, any())));
  }

  // ~ Methods ----------------------------------------------------------------

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveSortLimit sort = call.rel(0);
    final HiveUnion union = call.rel(1);

    // We only apply this rule if Union.all is true.
    // And Sort.fetch is not null and it is more than 0.
    return union.all && sort.fetch != null
        // Calcite bug CALCITE-987
        && RexLiteral.intValue(sort.fetch) > 0;
  }

  public void onMatch(RelOptRuleCall call) {
    final HiveSortLimit sort = call.rel(0);
    final HiveUnion union = call.rel(1);
    List<RelNode> inputs = new ArrayList<>();
    // Thus, we use 'finishPushSortPastUnion' as a flag to identify if we have finished pushing the
    // sort past a union.
    boolean finishPushSortPastUnion = true;
    final int offset = sort.offset == null ? 0 : RexLiteral.intValue(sort.offset);
    for (RelNode input : union.getInputs()) {
      // If we do not reduce the input size, we bail out
      if (RexLiteral.intValue(sort.fetch) + offset < call.getMetadataQuery().getRowCount(input)) {
        finishPushSortPastUnion = false;
        // Here we do some query rewrite. We first get the new fetchRN, which is
        // a sum of offset and fetch.
        // We then push it through by creating a new branchSort with the new
        // fetchRN but no offset.
        RexNode fetchRN = sort.getCluster().getRexBuilder()
            .makeExactLiteral(BigDecimal.valueOf(RexLiteral.intValue(sort.fetch) + offset));
        HiveSortLimit branchSort = sort.copy(sort.getTraitSet(), input, sort.getCollation(), null,
            fetchRN);
        branchSort.setRuleCreated(true);
        inputs.add(branchSort);
      } else {
        inputs.add(input);
      }
    }
    // there is nothing to change
    if (finishPushSortPastUnion) {
      return;
    }
    // create new union and sort
    HiveUnion unionCopy = (HiveUnion) union.copy(union.getTraitSet(), inputs, union.all);
    HiveSortLimit result = sort.copy(sort.getTraitSet(), unionCopy, sort.getCollation(), sort.offset,
        sort.fetch);
    call.transformTo(result);
  }
}
