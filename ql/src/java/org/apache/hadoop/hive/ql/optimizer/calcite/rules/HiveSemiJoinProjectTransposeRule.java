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

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;

/**
 * This rule is similar to {@link org.apache.calcite.rel.rules.SemiJoinProjectTransposeRule}.
 * However, it works on Hive nodes rather than logical nodes.
 *
 * <p>The rule pushes a Semi-join down in a tree past a Project if the
 * Project is followed by a Join. The intention is to remove Projects
 * between Joins.
 *
 * <p>SemiJoin(Project(X), Y) &rarr; Project(SemiJoin(X, Y))
 */
public class HiveSemiJoinProjectTransposeRule extends RelOptRule {
  public static final HiveSemiJoinProjectTransposeRule INSTANCE =
      new HiveSemiJoinProjectTransposeRule(HiveRelFactories.HIVE_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SemiJoinProjectTransposeRule.
   */
  private HiveSemiJoinProjectTransposeRule(RelBuilderFactory relBuilderFactory) {
    super(
        operandJ(Join.class, null, Join::isSemiJoin,
            operand(Project.class, operand(Join.class, any())),
            operand(RelNode.class, any())),
        relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------
  @Override
  public void onMatch(RelOptRuleCall call) {
    Join semiJoin = call.rel(0);
    Project project = call.rel(1);

    // Convert the LHS semi-join keys to reference the child projection
    // expression; all projection expressions must be RexInputRefs,
    // otherwise, we wouldn't have created this semi-join.

    // convert the semi-join condition to reflect the LHS with the project
    // pulled up
    RexNode newCondition = adjustCondition(project, semiJoin);

    Join newSemiJoin =
        HiveSemiJoin.getSemiJoin(project.getCluster(), project.getTraitSet(),
            project.getInput(), semiJoin.getRight(), newCondition);

    // Create the new projection.  Note that the projection expressions
    // are the same as the original because they only reference the LHS
    // of the semi-join and the semi-join only projects out the LHS
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(newSemiJoin);
    relBuilder.project(project.getProjects(), project.getRowType().getFieldNames());

    call.transformTo(relBuilder.build());
  }

  /**
   * Pulls the project above the semijoin and returns the resulting semijoin
   * condition. As a result, the semijoin condition should be modified such
   * that references to the LHS of a semijoin should now reference the
   * children of the project that's on the LHS.
   *
   * @param project  Project on the LHS of the semijoin
   * @param semiJoin the semijoin
   * @return the modified semijoin condition
   */
  private RexNode adjustCondition(Project project, Join semiJoin) {
    // create two RexPrograms -- the bottom one representing a
    // concatenation of the project and the RHS of the semi-join and the
    // top one representing the semi-join condition

    RexBuilder rexBuilder = project.getCluster().getRexBuilder();
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    RelNode rightChild = semiJoin.getRight();

    // for the bottom RexProgram, the input is a concatenation of the
    // child of the project and the RHS of the semi-join
    RelDataType bottomInputRowType =
        SqlValidatorUtil.deriveJoinRowType(
            project.getInput().getRowType(),
            rightChild.getRowType(),
            JoinRelType.INNER,
            typeFactory,
            null,
            semiJoin.getSystemFieldList());
    RexProgramBuilder bottomProgramBuilder =
        new RexProgramBuilder(bottomInputRowType, rexBuilder);

    // add the project expressions, then add input references for the RHS
    // of the semi-join
    for (Pair<RexNode, String> pair : project.getNamedProjects()) {
      bottomProgramBuilder.addProject(pair.left, pair.right);
    }
    int nLeftFields = project.getInput().getRowType().getFieldCount();
    List<RelDataTypeField> rightFields =
        rightChild.getRowType().getFieldList();
    int nRightFields = rightFields.size();
    for (int i = 0; i < nRightFields; i++) {
      final RelDataTypeField field = rightFields.get(i);
      RexNode inputRef =
          rexBuilder.makeInputRef(
              field.getType(), i + nLeftFields);
      bottomProgramBuilder.addProject(inputRef, field.getName());
    }
    RexProgram bottomProgram = bottomProgramBuilder.getProgram();

    // input rowType into the top program is the concatenation of the
    // project and the RHS of the semi-join
    RelDataType topInputRowType =
        SqlValidatorUtil.deriveJoinRowType(
            project.getRowType(),
            rightChild.getRowType(),
            JoinRelType.INNER,
            typeFactory,
            null,
            semiJoin.getSystemFieldList());
    RexProgramBuilder topProgramBuilder =
        new RexProgramBuilder(
            topInputRowType,
            rexBuilder);
    topProgramBuilder.addIdentity();
    topProgramBuilder.addCondition(semiJoin.getCondition());
    RexProgram topProgram = topProgramBuilder.getProgram();

    // merge the programs and expand out the local references to form
    // the new semi-join condition; it now references a concatenation of
    // the project's child and the RHS of the semi-join
    RexProgram mergedProgram =
        RexProgramBuilder.mergePrograms(
            topProgram,
            bottomProgram,
            rexBuilder);

    return mergedProgram.expandLocalRef(
        mergedProgram.getCondition());
  }
}
