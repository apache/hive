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
          HiveProject.DEFAULT_PROJECT_FACTORY);

  public static final HiveJoinProjectTransposeRule LEFT_PROJECT =
      new HiveJoinProjectTransposeRule(
          operand(HiveJoin.class,
              some(operand(HiveProject.class, any()))),
          "JoinProjectTransposeRule(Project-Other)",
          HiveProject.DEFAULT_PROJECT_FACTORY);

  public static final HiveJoinProjectTransposeRule RIGHT_PROJECT =
      new HiveJoinProjectTransposeRule(
          operand(
              HiveJoin.class,
              operand(RelNode.class, any()),
              operand(HiveProject.class, any())),
          "JoinProjectTransposeRule(Other-Project)",
          HiveProject.DEFAULT_PROJECT_FACTORY);

  private final ProjectFactory projectFactory;


  private HiveJoinProjectTransposeRule(
      RelOptRuleOperand operand,
      String description, ProjectFactory pFactory) {
    super(operand, description, pFactory);
    this.projectFactory = pFactory;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join joinRel = call.rel(0);
    JoinRelType joinType = joinRel.getJoinType();

    Project leftProj;
    Project rightProj;
    RelNode leftJoinChild;
    RelNode rightJoinChild;

    // see if at least one input's projection doesn't generate nulls
    if (hasLeftChild(call)) {
      leftProj = call.rel(1);
      leftJoinChild = getProjectChild(call, leftProj, true);
    } else {
      leftProj = null;
      leftJoinChild = call.rel(1);
    }
    if (hasRightChild(call)) {
      rightProj = getRightChild(call);
      rightJoinChild = getProjectChild(call, rightProj, false);
    } else {
      rightProj = null;
      rightJoinChild = joinRel.getRight();
    }
    if ((leftProj == null) && (rightProj == null)) {
      return;
    }

    // Construct two RexPrograms and combine them.  The bottom program
    // is a join of the projection expressions from the left and/or
    // right projects that feed into the join.  The top program contains
    // the join condition.

    // Create a row type representing a concatenation of the inputs
    // underneath the projects that feed into the join.  This is the input
    // into the bottom RexProgram.  Note that the join type is an inner
    // join because the inputs haven't actually been joined yet.
    RelDataType joinChildrenRowType =
        Join.deriveJoinRowType(
            leftJoinChild.getRowType(),
            rightJoinChild.getRowType(),
            JoinRelType.INNER,
            joinRel.getCluster().getTypeFactory(),
            null,
            Collections.<RelDataTypeField>emptyList());

    // Create projection expressions, combining the projection expressions
    // from the projects that feed into the join.  For the RHS projection
    // expressions, shift them to the right by the number of fields on
    // the LHS.  If the join input was not a projection, simply create
    // references to the inputs.
    int nProjExprs = joinRel.getRowType().getFieldCount();
    List<Pair<RexNode, String>> projects =
        new ArrayList<Pair<RexNode, String>>();
    RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();

    createProjectExprs(
        leftProj,
        leftJoinChild,
        0,
        rexBuilder,
        joinChildrenRowType.getFieldList(),
        projects);

    List<RelDataTypeField> leftFields =
        leftJoinChild.getRowType().getFieldList();
    int nFieldsLeft = leftFields.size();
    createProjectExprs(
        rightProj,
        rightJoinChild,
        nFieldsLeft,
        rexBuilder,
        joinChildrenRowType.getFieldList(),
        projects);

    List<RelDataType> projTypes = new ArrayList<RelDataType>();
    for (int i = 0; i < nProjExprs; i++) {
      projTypes.add(projects.get(i).left.getType());
    }
    RelDataType projRowType =
        rexBuilder.getTypeFactory().createStructType(
            projTypes,
            Pair.right(projects));

    // create the RexPrograms and merge them
    RexProgram bottomProgram =
        RexProgram.create(
            joinChildrenRowType,
            Pair.left(projects),
            null,
            projRowType,
            rexBuilder);
    RexProgramBuilder topProgramBuilder =
        new RexProgramBuilder(
            projRowType,
            rexBuilder);
    topProgramBuilder.addIdentity();
    topProgramBuilder.addCondition(joinRel.getCondition());
    RexProgram topProgram = topProgramBuilder.getProgram();
    RexProgram mergedProgram =
        RexProgramBuilder.mergePrograms(
            topProgram,
            bottomProgram,
            rexBuilder);

    // expand out the join condition and construct a new LogicalJoin that
    // directly references the join children without the intervening
    // ProjectRels
    RexNode newCondition =
        mergedProgram.expandLocalRef(
            mergedProgram.getCondition());
    Join newJoinRel =
        joinRel.copy(joinRel.getTraitSet(), newCondition,
            leftJoinChild, rightJoinChild, joinRel.getJoinType(),
            joinRel.isSemiJoinDone());

    // expand out the new projection expressions; if the join is an
    // outer join, modify the expressions to reference the join output
    List<RexNode> newProjExprs = new ArrayList<RexNode>();
    List<RexLocalRef> projList = mergedProgram.getProjectList();
    List<RelDataTypeField> newJoinFields =
        newJoinRel.getRowType().getFieldList();
    int nJoinFields = newJoinFields.size();
    int[] adjustments = new int[nJoinFields];
    for (int i = 0; i < nProjExprs; i++) {
      RexNode newExpr = mergedProgram.expandLocalRef(projList.get(i));
      if (joinType != JoinRelType.INNER) {
        newExpr =
            newExpr.accept(
                new RelOptUtil.RexInputConverter(
                    rexBuilder,
                    joinChildrenRowType.getFieldList(),
                    newJoinFields,
                    adjustments));
      }
      newProjExprs.add(newExpr);
    }

    // finally, create the projection on top of the join
    RelNode newProjRel = projectFactory.createProject(newJoinRel, newProjExprs,
        joinRel.getRowType().getFieldNames());

    call.transformTo(newProjRel);
  }

  /**
   * Creates projection expressions corresponding to one of the inputs into
   * the join
   *
   * @param projRel            the projection input into the join (if it exists)
   * @param joinChild          the child of the projection input (if there is a
   *                           projection); otherwise, this is the join input
   * @param adjustmentAmount   the amount the expressions need to be shifted by
   * @param rexBuilder         rex builder
   * @param joinChildrenFields concatenation of the fields from the left and
   *                           right join inputs (once the projections have been
   *                           removed)
   * @param projects           Projection expressions &amp; names to be created
   */
  private void createProjectExprs(
      Project projRel,
      RelNode joinChild,
      int adjustmentAmount,
      RexBuilder rexBuilder,
      List<RelDataTypeField> joinChildrenFields,
      List<Pair<RexNode, String>> projects) {
    List<RelDataTypeField> childFields =
        joinChild.getRowType().getFieldList();
    if (projRel != null) {
      List<Pair<RexNode, String>> namedProjects =
          projRel.getNamedProjects();
      int nChildFields = childFields.size();
      int[] adjustments = new int[nChildFields];
      for (int i = 0; i < nChildFields; i++) {
        adjustments[i] = adjustmentAmount;
      }
      for (Pair<RexNode, String> pair : namedProjects) {
        RexNode e = pair.left;
        if (adjustmentAmount != 0) {
          // shift the references by the adjustment amount
          e = e.accept(
              new RelOptUtil.RexInputConverter(
                  rexBuilder,
                  childFields,
                  joinChildrenFields,
                  adjustments));
        }
        projects.add(Pair.of(e, pair.right));
      }
    } else {
      // no projection; just create references to the inputs
      for (int i = 0; i < childFields.size(); i++) {
        final RelDataTypeField field = childFields.get(i);
        projects.add(
            Pair.of(
                (RexNode) rexBuilder.makeInputRef(
                    field.getType(),
                    i + adjustmentAmount),
                field.getName()));
      }
    }
  }
}
