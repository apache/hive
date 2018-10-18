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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIntersect;
import org.apache.calcite.util.Util;

/**
 * Planner rule that merges multiple intersect into one
 * Before the rule, it is 
 *                        intersect-branch1
 *                            |-----intersect-branch2
 *                                      |-----branch3
 * After the rule, it becomes
 *                        intersect-branch1
 *                            |-----branch2
 *                            |-----branch3
 * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIntersect}
 */
public class HiveIntersectMergeRule extends RelOptRule {

  public static final HiveIntersectMergeRule INSTANCE = new HiveIntersectMergeRule();

  // ~ Constructors -----------------------------------------------------------

  private HiveIntersectMergeRule() {
    super(
        operand(HiveIntersect.class, operand(RelNode.class, any()), operand(RelNode.class, any())));
  }

  // ~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final HiveIntersect topHiveIntersect = call.rel(0);

    final HiveIntersect bottomHiveIntersect;
    if (call.rel(2) instanceof HiveIntersect) {
      bottomHiveIntersect = call.rel(2);
    } else if (call.rel(1) instanceof HiveIntersect) {
      bottomHiveIntersect = call.rel(1);
    } else {
      return;
    }

    boolean all = topHiveIntersect.all;
    // top is distinct, we can always merge whether bottom is distinct or not
    // top is all, we can only merge if bottom is also all
    // that is to say, we should bail out if top is all and bottom is distinct
    if (all && !bottomHiveIntersect.all) {
      return;
    }

    List<RelNode> inputs = new ArrayList<>();
    if (call.rel(2) instanceof HiveIntersect) {
      inputs.add(topHiveIntersect.getInput(0));
      inputs.addAll(bottomHiveIntersect.getInputs());
    } else {
      inputs.addAll(bottomHiveIntersect.getInputs());
      inputs.addAll(Util.skip(topHiveIntersect.getInputs()));
    }

    HiveIntersect newIntersect = (HiveIntersect) topHiveIntersect.copy(
        topHiveIntersect.getTraitSet(), inputs, all);
    call.transformTo(newIntersect);
  }
}
