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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Rule to transpose Filter and TableFunctionScan RelNodes
 */
public class HiveFilterTableFunctionTransposeRule extends RelOptRule {

  public static final HiveFilterTableFunctionTransposeRule INSTANCE =
          new HiveFilterTableFunctionTransposeRule(HiveRelFactories.HIVE_BUILDER);

  public HiveFilterTableFunctionTransposeRule(RelBuilderFactory relBuilderFactory) {
    super(operand(HiveFilter.class, operand(HiveTableFunctionScan.class, any())),
        relBuilderFactory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Filter filterRel = call.rel(0);
    final HiveTableFunctionScan tableFunctionScanRel = call.rel(1);

    RexNode condition = filterRel.getCondition();
    if (!HiveCalciteUtil.isDeterministic(condition)) {
      return false;
    }

    // If the HiveTableFunctionScan is a special inline(array(...))
    // udtf, the table is generated from this node. The underlying
    // RelNode will be a dummy table so no filter condition should
    // pass through the HiveTableFunctionScan.
    if (isInlineArray(tableFunctionScanRel)) {
      return false;
    }

    // If the HiveTableFunctionScan is not a lateral view, the return type
    // for the RelNode only contains the output of the udtf so no filter
    // condition can be passed through.
    if (!tableFunctionScanRel.isLateralView()) {
      return false;
    }

    RelNode inputRel = tableFunctionScanRel.getInput(0);

    // The TableFunctionScan is always created such that all the input RelNode
    // fields are present in its RelNode.  If a Filter has an InputRef that is
    // greater then the number of the RelNode below the TableFunctionScan, that
    // means it was a field created by the TableFunctionScan and thus the Filter
    // cannot be pushed through.
    //
    // We check for each individual conjunction (breaking it up by top level 'and'
    // conditions).
    int numFieldsInInput = inputRel.getRowType().getFieldCount();

    for (RexNode ce : RelOptUtil.conjunctions(filterRel.getCondition())) {
      Set<Integer> inputRefs = HiveCalciteUtil.getInputRefs(ce);

      boolean canBePushed = true;
      for (Integer inputRef : inputRefs) {
        if (inputRef >= numFieldsInInput) {
          canBePushed = false;
          break;
        }
      }

      if (canBePushed) {
        return true;
      }
    }
    return false;
  }

  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final HiveTableFunctionScan tfs = call.rel(1);
    final RelNode inputRel = tfs.getInput(0);
    final int numFieldsInInput = inputRel.getRowType().getFieldCount();

    final List<RexNode> newPartKeyFilterConditions = new ArrayList<>();
    final List<RexNode> unpushedFilterConditions = new ArrayList<>();

    // Check for each individual 'and' condition so that we can push partial
    // expressions through.
    for (RexNode ce : RelOptUtil.conjunctions(filter.getCondition())) {
      Set<Integer> inputRefs = HiveCalciteUtil.getInputRefs(ce);
      boolean canBePushed = true;
      // We can only push if all the InputRef pointers are referencing the
      // input RelNode to the TableFunctionScan
      for (Integer inputRef : inputRefs) {
        if (inputRef >= numFieldsInInput) {
          canBePushed = false;
          break;
        }
      }
      if (canBePushed) {
        newPartKeyFilterConditions.add(ce);
      } else {
        unpushedFilterConditions.add(ce);
      }
    }

    // The "matches" check should guarantee there's something to push.
    Preconditions.checkState(!newPartKeyFilterConditions.isEmpty());
    final RexNode filterCondToPushBelowProj = RexUtil.composeConjunction(
        filter.getCluster().getRexBuilder(), newPartKeyFilterConditions, true);

    // Create the new filter with the pushed through conditions
    final RelNode newFilter =
        filter.copy(filter.getTraitSet(), tfs.getInput(0), filterCondToPushBelowProj);

    // If there are conditions that cannot be pushed through, generate the RexNode
    final RexNode unpushedFilCondAboveProj = unpushedFilterConditions.isEmpty()
        ? null
        : RexUtil.composeConjunction(filter.getCluster().getRexBuilder(),
            unpushedFilterConditions, true);

    // Generate the new TableFunctionScanNode with the Filter InputRel
    final RelNode tableFunctionScanNode = tfs.copy(tfs.getTraitSet(), ImmutableList.of(newFilter),
        tfs.getCall(), tfs.getElementType(), tfs.getRowType(), tfs.getColumnMappings());

    // If there are expressions that couldn't be pushed through, generate the filter above the
    // TableFunctionScan with these conditions.
    final RelNode topLevelNode = unpushedFilCondAboveProj == null
        ? tableFunctionScanNode
        : filter.copy(filter.getTraitSet(), tableFunctionScanNode, unpushedFilCondAboveProj);

    call.transformTo(topLevelNode);
  }

  private boolean isInlineArray(HiveTableFunctionScan tableFunctionScanRel) {
    RexCall udtfCall = (RexCall) tableFunctionScanRel.getCall();
    if (!FunctionRegistry.INLINE_FUNC_NAME.equalsIgnoreCase(udtfCall.getOperator().getName())) {
      return false;
    }
    Preconditions.checkState(!udtfCall.getOperands().isEmpty());
    RexNode operand = udtfCall.getOperands().get(0);
    if (!(operand instanceof RexCall)) {
      return false;
    }
    RexCall firstOperand = (RexCall) operand;
    if (!FunctionRegistry.ARRAY_FUNC_NAME.equalsIgnoreCase(firstOperand.getOperator().getName())) {
      return false;
    }
    return true;
  }
}
