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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIntersect;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;
import org.apache.calcite.util.Util;

/**
 * Planner rule that merges multiple union into one
 * Before the rule, it is 
 *                        union all-branch1
 *                            |-----union all-branch2
 *                                      |-----branch3
 * After the rule, it becomes
 *                        union all-branch1
 *                            |-----branch2
 *                            |-----branch3
 * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion}
 */
public class HiveUnionMergeRule extends RelOptRule {

  public static final HiveUnionMergeRule INSTANCE = new HiveUnionMergeRule();

  // ~ Constructors -----------------------------------------------------------

  private HiveUnionMergeRule() {
    super(
        operand(HiveUnion.class, operand(RelNode.class, any()), operand(RelNode.class, any())));
  }

  // ~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final HiveUnion topUnion = call.rel(0);

    final HiveUnion bottomUnion;
    if (call.rel(2) instanceof HiveUnion) {
      bottomUnion = call.rel(2);
    } else if (call.rel(1) instanceof HiveUnion) {
      bottomUnion = call.rel(1);
    } else {
      return;
    }

    List<RelNode> inputs = new ArrayList<>();
    if (call.rel(2) instanceof HiveUnion) {
      for (int i = 0; i < topUnion.getInputs().size(); i++) {
        if (i != 1) {
          inputs.add(topUnion.getInput(i));
        }
      }
      inputs.addAll(bottomUnion.getInputs());
    } else {
      inputs.addAll(bottomUnion.getInputs());
      inputs.addAll(Util.skip(topUnion.getInputs()));
    }

    HiveUnion newUnion = (HiveUnion) topUnion.copy(
        topUnion.getTraitSet(), inputs, true);
    call.transformTo(newUnion);
  }
}
