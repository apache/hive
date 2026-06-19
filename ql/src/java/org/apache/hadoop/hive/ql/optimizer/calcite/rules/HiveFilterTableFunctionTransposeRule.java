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
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.optimizer.calcite.Bug;
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
 *
 * We cannot use Calcite's FilterTableFunctionTransposeRule because that rule
 * uses LogicalFilter and LogicalTableFunctionScan.  We should remove this
 * class when CALCITE-5985 is fixed (and remove the CALCITE_5985_FIXED entry
 * in Bug.java)
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
    if (Bug.CALCITE_5985_FIXED) {
      throw new IllegalStateException("Class is redundant after fix is merged into Calcite");
    }

    final Filter filterRel = call.rel(0);
    final HiveTableFunctionScan tfs = call.rel(1);

    RexNode condition = filterRel.getCondition();
    if (!HiveCalciteUtil.isDeterministic(condition)) {
      return false;
    }

    // The TableFunctionScan is always created such that all the input RelNode
    // fields are present in its RelNode.  If a Filter has an InputRef that is
    // greater then the number of the RelNode below the TableFunctionScan, that
    // means it was a field created by the TableFunctionScan and thus the Filter
    // cannot be pushed through.
    //
    // We check for each individual conjunction (breaking it up by top level 'and'
    // conditions).
    for (RexNode ce : RelOptUtil.conjunctions(filterRel.getCondition())) {
      if (canBePushed(HiveCalciteUtil.getInputRefs(ce), tfs)) {
        return true;
      }
    }
    return false;
  }

  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final HiveTableFunctionScan tfs = call.rel(1);
    final RelBuilder builder = call.builder();

    final List<RexNode> newPartKeyFilterConditions = new ArrayList<>();
    final List<RexNode> unpushedFilterConditions = new ArrayList<>();

    // Check for each individual 'and' condition so that we can push partial
    // expressions through.
    for (RexNode ce : RelOptUtil.conjunctions(filter.getCondition())) {
      // We can only push if all the InputRef pointers are referencing the
      // input RelNode to the TableFunctionScan
      if (canBePushed(HiveCalciteUtil.getInputRefs(ce), tfs)) {
        newPartKeyFilterConditions.add(ce);
      } else {
        unpushedFilterConditions.add(ce);
      }
    }

    // The "matches" check should guarantee there's something to push.
    final RexNode filterCondToPushBelowProj = RexUtil.composeConjunction(
        filter.getCluster().getRexBuilder(), newPartKeyFilterConditions, true);

    builder.push(tfs.getInput(0)).filter(filterCondToPushBelowProj);

    // If there are conditions that cannot be pushed through, generate the RexNode
    final RexNode unpushedFilCondAboveProj = unpushedFilterConditions.isEmpty()
        ? null
        : RexUtil.composeConjunction(filter.getCluster().getRexBuilder(),
            unpushedFilterConditions, true);

    // Generate the new TableFunctionScanNode with the Filter InputRel
    final RelNode tableFunctionScanNode = tfs.copy(tfs.getTraitSet(),
        ImmutableList.of(builder.build()), tfs.getCall(), tfs.getElementType(),
        tfs.getRowType(), tfs.getColumnMappings());

    builder.clear();
    builder.push(tableFunctionScanNode);

    if (unpushedFilCondAboveProj != null) {
      builder.filter(unpushedFilCondAboveProj);
    }

    call.transformTo(builder.build());
  }

  // If any of the inputRefs are references to a field that is not mapped into the inputRelNode,
  // the condition cannot be pushed.
  private boolean canBePushed(Set<Integer> inputRefs, HiveTableFunctionScan tfs) {
    Set<RelColumnMapping> columnMappings = tfs.getColumnMappings();
    if (inputRefs.isEmpty()) {
      return true;
    }

    if (CollectionUtils.isEmpty(columnMappings)) {
      return false;
    }

    for (Integer inputRef : inputRefs) {
      if (!tfs.containsInputRefMapping(inputRef)) {
        return false;
      }
    }
    return true;
  }
}
