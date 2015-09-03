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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

public class HiveJoinProjectTransposeRule extends JoinProjectTransposeRule {

  public static final HiveJoinProjectTransposeRule BOTH_PROJECT =
      new HiveJoinProjectTransposeRule(
          operand(HiveJoin.class,
              operand(HiveProject.class, any()),
              operand(HiveProject.class, any())),
          "JoinProjectTransposeRule(Project-Project)",
          false, HiveProject.DEFAULT_PROJECT_FACTORY);

  public static final HiveJoinProjectTransposeRule LEFT_PROJECT =
      new HiveJoinProjectTransposeRule(
          operand(HiveJoin.class,
              some(operand(HiveProject.class, any()))),
          "JoinProjectTransposeRule(Project-Other)",
          false, HiveProject.DEFAULT_PROJECT_FACTORY);

  public static final HiveJoinProjectTransposeRule RIGHT_PROJECT =
      new HiveJoinProjectTransposeRule(
          operand(
              HiveJoin.class,
              operand(RelNode.class, any()),
              operand(HiveProject.class, any())),
          "JoinProjectTransposeRule(Other-Project)",
          false, HiveProject.DEFAULT_PROJECT_FACTORY);

  public static final HiveJoinProjectTransposeRule BOTH_PROJECT_INCLUDE_OUTER =
      new HiveJoinProjectTransposeRule(
          operand(HiveJoin.class,
              operand(HiveProject.class, any()),
              operand(HiveProject.class, any())),
          "Join(IncludingOuter)ProjectTransposeRule(Project-Project)",
          true, HiveProject.DEFAULT_PROJECT_FACTORY);

  public static final HiveJoinProjectTransposeRule LEFT_PROJECT_INCLUDE_OUTER =
      new HiveJoinProjectTransposeRule(
          operand(HiveJoin.class,
              some(operand(HiveProject.class, any()))),
          "Join(IncludingOuter)ProjectTransposeRule(Project-Other)",
          true, HiveProject.DEFAULT_PROJECT_FACTORY);

  public static final HiveJoinProjectTransposeRule RIGHT_PROJECT_INCLUDE_OUTER =
      new HiveJoinProjectTransposeRule(
          operand(
              HiveJoin.class,
              operand(RelNode.class, any()),
              operand(HiveProject.class, any())),
          "Join(IncludingOuter)ProjectTransposeRule(Other-Project)",
          true, HiveProject.DEFAULT_PROJECT_FACTORY);


  private HiveJoinProjectTransposeRule(
      RelOptRuleOperand operand, String description,
      boolean includeOuter, ProjectFactory projectFactory) {
    super(operand, description, includeOuter, projectFactory);
  }

}
