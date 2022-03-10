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

import static org.apache.calcite.plan.RelOptRule.any;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.some;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

/**
 * Planner rules based on {link@ org.apache.calcite.rel.rules.JoinProjectTransposeRule} specialized to Hive.
 */
public final class HiveJoinProjectTransposeRule {

  private HiveJoinProjectTransposeRule() { }

  public static final HiveJoinProjectTransposeRuleBase LEFT_PROJECT_BTW_JOIN =
      new HiveJoinProjectBtwJoinTransposeRule(
          operand(HiveJoin.class,
              operand(HiveProject.class, operand(HiveJoin.class, any())),
              operand(RelNode.class, any())),
          "JoinProjectTransposeRule(Project-Join-Other)",
          true);

  public static final HiveJoinProjectTransposeRuleBase RIGHT_PROJECT_BTW_JOIN =
      new HiveJoinProjectBtwJoinTransposeRule(
          operand(HiveJoin.class,
              operand(RelNode.class, any()),
              operand(HiveProject.class, operand(HiveJoin.class, any()))),
          "JoinProjectTransposeRule(Other-Project-Join)",
          false);

  private static final class HiveJoinProjectBtwJoinTransposeRule extends HiveJoinProjectTransposeRuleBase {

    private final boolean leftJoin;

    private HiveJoinProjectBtwJoinTransposeRule(
        RelOptRuleOperand operand, String description, boolean leftJoin) {
      super(operand, description, true, HiveRelFactories.HIVE_BUILDER);

      this.leftJoin = leftJoin;
    }

    @Override
    protected boolean hasLeftChild(RelOptRuleCall call) {
      return leftJoin;
    }

    @Override
    protected boolean hasRightChild(RelOptRuleCall call) {
      return !leftJoin;
    }
  }

  public static final HiveJoinProjectTransposeRuleBase BOTH_PROJECT =
      new HiveJoinProjectTransposeRuleBase(
          operand(HiveJoin.class,
              operand(HiveProject.class, any()),
              operand(HiveProject.class, any())),
          "JoinProjectTransposeRule(Project-Project)",
          false, HiveRelFactories.HIVE_BUILDER);

  public static final HiveJoinProjectTransposeRuleBase LEFT_PROJECT =
      new HiveJoinProjectTransposeRuleBase(
          operand(HiveJoin.class,
              some(operand(HiveProject.class, any()))),
          "JoinProjectTransposeRule(Project-Other)",
          false, HiveRelFactories.HIVE_BUILDER);

  public static final HiveJoinProjectTransposeRuleBase RIGHT_PROJECT =
      new HiveJoinProjectTransposeRuleBase(
          operand(
              HiveJoin.class,
              operand(RelNode.class, any()),
              operand(HiveProject.class, any())),
          "JoinProjectTransposeRule(Other-Project)",
          false, HiveRelFactories.HIVE_BUILDER);

  public static final HiveJoinProjectTransposeRuleBase BOTH_PROJECT_INCLUDE_OUTER =
      new HiveJoinProjectTransposeRuleBase(
          operand(HiveJoin.class,
              operand(HiveProject.class, any()),
              operand(HiveProject.class, any())),
          "Join(IncludingOuter)ProjectTransposeRule(Project-Project)",
          true, HiveRelFactories.HIVE_BUILDER);

  public static final HiveJoinProjectTransposeRuleBase LEFT_PROJECT_INCLUDE_OUTER =
      new HiveJoinProjectTransposeRuleBase(
          operand(HiveJoin.class,
              some(operand(HiveProject.class, any()))),
          "Join(IncludingOuter)ProjectTransposeRule(Project-Other)",
          true, HiveRelFactories.HIVE_BUILDER);

  public static final HiveJoinProjectTransposeRuleBase RIGHT_PROJECT_INCLUDE_OUTER =
      new HiveJoinProjectTransposeRuleBase(
          operand(
              HiveJoin.class,
              operand(RelNode.class, any()),
              operand(HiveProject.class, any())),
          "Join(IncludingOuter)ProjectTransposeRule(Other-Project)",
          true, HiveRelFactories.HIVE_BUILDER);

  private static class HiveJoinProjectTransposeRuleBase extends JoinProjectTransposeRule {

    private HiveJoinProjectTransposeRuleBase(
        RelOptRuleOperand operand, String description,
        boolean includeOuter, RelBuilderFactory relBuilderFactory) {
      super(operand, description, includeOuter, relBuilderFactory);
    }

    public void onMatch(RelOptRuleCall call) {
      //TODO: this can be removed once CALCITE-3824 is released
      Join joinRel = call.rel(0);

      //TODO:https://issues.apache.org/jira/browse/HIVE-23921
      if (joinRel.getJoinType() == JoinRelType.ANTI) {
        return;
      }
      HiveProject proj;
      if (hasLeftChild(call)) {
        proj = call.rel(1);
        if (proj.containsOver()) {
          return;
        }
      }
      if (hasRightChild(call)) {
        proj = (HiveProject) getRightChild(call);
        if (proj.containsOver()) {
          return;
        }
      }
      super.onMatch(call);
    }
  }
}
