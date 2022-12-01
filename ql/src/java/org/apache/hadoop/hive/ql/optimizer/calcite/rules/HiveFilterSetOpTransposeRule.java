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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

public class HiveFilterSetOpTransposeRule extends FilterSetOpTransposeRule {

  public static final HiveFilterSetOpTransposeRule INSTANCE =
          new HiveFilterSetOpTransposeRule(HiveRelFactories.HIVE_BUILDER);

  /**
   * Creates a HiveFilterSetOpTransposeRule.
   * This rule rewrites
   *       Fil
   *        |
   *      Union
   *       / \
   *     Op1 Op2
   * to
   *       Union
   *         /\
   *         FIL
   *         | |
   *       Op1 Op2
   */
  private HiveFilterSetOpTransposeRule(RelBuilderFactory relBuilderFactory) {
    super(relBuilderFactory);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Filter filterRel = call.rel(0);
    RexNode condition = filterRel.getCondition();
    if (!HiveCalciteUtil.isDeterministic(condition)) {
      return false;
    }

    return super.matches(call);
  }


  //~ Methods ----------------------------------------------------------------

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Filter filterRel = call.rel(0);
    final SetOp setOp = call.rel(1);

    final RexNode condition = filterRel.getCondition();

    // create filters on top of each setop child, modifying the filter
    // condition to reference each setop child
    final RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();
    final List<RelDataTypeField> origFields = setOp.getRowType().getFieldList();
    final int[] adjustments = new int[origFields.size()];
    final List<RelNode> newSetOpInputs = new ArrayList<>();

    for (int index = 0; index < setOp.getInputs().size(); index++) {
      RelNode input = setOp.getInput(index);
      RexNode newCondition = condition.accept(new RelOptUtil.RexInputConverter(rexBuilder,
          origFields, input.getRowType().getFieldList(), adjustments));
      newSetOpInputs.add(relBuilder.push(input).filter(newCondition).build());
    }

    // create a new setop whose children are the filters created above
    SetOp newSetOp = setOp.copy(setOp.getTraitSet(), newSetOpInputs);
    call.transformTo(newSetOp);
  }
}
