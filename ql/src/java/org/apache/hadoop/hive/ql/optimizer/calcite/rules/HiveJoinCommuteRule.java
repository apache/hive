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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.util.Permutation;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

/**
 * Planner rule that permutes the inputs of a Join, if it has a Project on top
 * that simply swaps the fields of both inputs.
 */
public class HiveJoinCommuteRule extends RelOptRule {

  public static final HiveJoinCommuteRule INSTANCE = new HiveJoinCommuteRule(
          HiveProject.class, HiveJoin.class);


  public HiveJoinCommuteRule(Class<? extends Project> projClazz,
      Class<? extends Join> joinClazz) {
    super(operand(projClazz,
            operand(joinClazz, any())));
  }

  public void onMatch(final RelOptRuleCall call) {
    Project topProject = call.rel(0);
    Join join = call.rel(1);

    // 1. We check if it is a permutation project. If it is
    //    not, or this is the identity, the rule will do nothing
    final Permutation topPermutation = topProject.getPermutation();
    if (topPermutation == null) {
      return;
    }
    if (topPermutation.isIdentity()) {
      return;
    }

    // 2. We swap the join
    final RelNode swapped = JoinCommuteRule.swap(join,true);
    if (swapped == null) {
      return;
    }

    // 3. The result should have a project on top, otherwise we
    //    bail out.
    if (swapped instanceof Join) {
      return;
    }

    // 4. We check if it is a permutation project. If it is
    //    not, or this is the identity, the rule will do nothing
    final Project bottomProject = (Project) swapped;
    final Permutation bottomPermutation = bottomProject.getPermutation();
    if (bottomPermutation == null) {
      return;
    }
    if (bottomPermutation.isIdentity()) {
      return;
    }

    // 5. If the product of the topPermutation and bottomPermutation yields
    //    the identity, then we can swap the join and remove the project on
    //    top.
    final Permutation product = topPermutation.product(bottomPermutation);
    if (!product.isIdentity()) {
      return;
    }

    // 6. Return the new join as a replacement
    final Join swappedJoin = (Join) bottomProject.getInput(0);
    call.transformTo(swappedJoin);
  }

}
