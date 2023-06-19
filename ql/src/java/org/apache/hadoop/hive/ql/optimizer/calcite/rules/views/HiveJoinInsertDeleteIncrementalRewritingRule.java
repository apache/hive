package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;/*
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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;

import java.util.ArrayList;
import java.util.List;

/**
 * This rule will perform a rewriting to prepare the plan for incremental
 * view maintenance in case there is no aggregation operator but some of the
 * source tables has delete operations, so we can avoid the INSERT OVERWRITE and use a
 * MULTI INSERT statement instead: one insert branch for inserted rows
 * and another for inserting deleted rows to delete delta.
 * Since CBO plan does not contain the INSERT branches we focus on the SELECT part of the plan in this rule.
 * See also {@link CalcitePlanner}
 *
 * FROM (select mv.ROW__ID, mv.a, mv.b, true as flag from mv) mv
 * RIGHT OUTER JOIN (SELECT _source_.ROW__IS_DELETED,_source_.a, _source_.b FROM _source_) source
 * ON (mv.a &lt;=&gt; source.a AND mv.b &lt;=&gt; source.b)
 * INSERT INTO TABLE mv_delete_delta
 *   SELECT mv.ROW__ID
 *   WHERE source.ROW__IS__DELETED AND flag
 * INSERT INTO TABLE mv
 *   SELECT source.a, source.b
 *   WHERE NOT source.ROW__IS__DELETED
 *   SORT BY mv.ROW__ID;
 */
public class HiveJoinInsertDeleteIncrementalRewritingRule extends RelOptRule {

  public static final HiveJoinInsertDeleteIncrementalRewritingRule INSTANCE =
          new HiveJoinInsertDeleteIncrementalRewritingRule();

  private HiveJoinInsertDeleteIncrementalRewritingRule() {
    super(operand(Union.class, any()),
            HiveRelFactories.HIVE_BUILDER,
            "HiveJoinInsertDeleteIncrementalRewritingRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Union union = call.rel(0);
    RexBuilder rexBuilder = union.getCluster().getRexBuilder();
    // First branch is query, second branch is MV
    // 1) First branch is query, second branch is MV
    final RelNode joinLeftInput = union.getInput(1);
    final RelNode joinRightInput = union.getInput(0);

    // 2) Build conditions for join and start adding
    // expressions for project operator
    List<RexNode> projExprs = new ArrayList<>();
    List<RexNode> joinConjs = new ArrayList<>();
    for (int leftPos = 0; leftPos < joinLeftInput.getRowType().getFieldCount(); leftPos++) {
      RexNode leftRef = rexBuilder.makeInputRef(
              joinLeftInput.getRowType().getFieldList().get(leftPos).getType(), leftPos);
      RexNode rightRef = rexBuilder.makeInputRef(
              joinRightInput.getRowType().getFieldList().get(leftPos).getType(),
              leftPos + joinLeftInput.getRowType().getFieldCount());

      projExprs.add(rightRef);

      joinConjs.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, leftRef, rightRef));
    }

    RexNode joinCond = RexUtil.composeConjunction(rexBuilder, joinConjs);

    // 3) Build plan
    RelNode newNode = call.builder()
            .push(union.getInput(1))
            .push(union.getInput(0))
            .join(JoinRelType.RIGHT, joinCond)
            .project(projExprs)
            .build();
    call.transformTo(newNode);
  }

  public static class FilterPropagator extends HiveRowIsDeletedPropagator {

    private boolean foundTopRightJoin;

    public FilterPropagator(RelBuilder relBuilder) {
      super(relBuilder);
    }

    @Override
    public RelNode propagate(RelNode relNode) {
      foundTopRightJoin = false;
      return super.propagate(relNode);
    }

    @Override
    public RelNode visit(HiveProject project) {
      if (!foundTopRightJoin) {
        return visitChild(project, 0, project.getInput());
      }

      // continue traversal and propagate rowIsDeleted column
      return super.visit(project);
    }

    @Override
    public RelNode visit(HiveJoin join) {
      if (!foundTopRightJoin) {
        if (join.getJoinType() != JoinRelType.RIGHT) {
          // continue search for top Right Join node
          return visitChildren(join);
        }

        foundTopRightJoin = true;
        return createFilter(join);
      }

      // continue traversal and propagate rowIsDeleted column
      return super.visit(join);
    }

    private RelNode createFilter(HiveJoin join) {
      RexBuilder rexBuilder = relBuilder.getRexBuilder();
      // This should be a Scan on the MV
      RelNode leftInput = join.getLeft();

      // This branch is querying the rows should be inserted/deleted into the view since the last rebuild.
      RelNode rightInput = join.getRight();

      RelNode tmpJoin = visitChild(join, 1, rightInput);
      RelNode newRightInput = tmpJoin.getInput(1);

      List<RexNode> leftProjects = new ArrayList<>(leftInput.getRowType().getFieldCount() + 1);
      List<String> leftProjectNames = new ArrayList<>(leftInput.getRowType().getFieldCount() + 1);
      for (int i = 0; i < leftInput.getRowType().getFieldCount(); ++i) {
        RelDataTypeField relDataTypeField = leftInput.getRowType().getFieldList().get(i);
        leftProjects.add(rexBuilder.makeInputRef(relDataTypeField.getType(), i));
        leftProjectNames.add(relDataTypeField.getName());
      }
      List<RexNode> projects = new ArrayList<>(leftProjects.size() + newRightInput.getRowType().getFieldCount());
      projects.addAll(leftProjects);
      List<String> projectNames = new ArrayList<>(leftProjects.size() + newRightInput.getRowType().getFieldCount());
      projectNames.addAll(leftProjectNames);

      leftProjects.add(rexBuilder.makeLiteral(true));
      leftProjectNames.add("flag");

      leftInput = relBuilder
          .push(leftInput)
          .project(leftProjects, leftProjectNames)
          .build();

      // Create input ref to flag. It is used in filter condition later.
      int flagIndex = leftProjects.size() - 1;
      RexNode flagNode = rexBuilder.makeInputRef(
          leftInput.getRowType().getFieldList().get(flagIndex).getType(), flagIndex);

      // Create input ref to rowIsDeleteColumn. It is used in filter condition later.
      RelDataType newRowType = newRightInput.getRowType();
      int rowIsDeletedIdx = newRowType.getFieldCount() - 1;
      RexNode rowIsDeleted = rexBuilder.makeInputRef(
          newRowType.getFieldList().get(rowIsDeletedIdx).getType(),
          leftInput.getRowType().getFieldCount() + rowIsDeletedIdx);

      RexNode deleteBranchFilter = rexBuilder.makeCall(SqlStdOperatorTable.AND, flagNode, rowIsDeleted);
      RexNode insertBranchFilter = rexBuilder.makeCall(SqlStdOperatorTable.NOT, rowIsDeleted);

      for (int i = 0; i < newRowType.getFieldCount() - 1; ++i) {
        RelDataTypeField relDataTypeField = newRowType.getFieldList().get(i);
        projects.add(rexBuilder.makeInputRef(relDataTypeField.getType(), leftInput.getRowType().getFieldCount() + i));
        projectNames.add(relDataTypeField.getName());
      }

      RexNode newJoinCondition = new InputRefShifter(leftInput.getRowType().getFieldCount() - 1, relBuilder)
          .apply(join.getCondition());

      // Create new Top Right Join and a Filter. The filter condition is used in CalcitePlanner.fixUpASTJoinIncrementalRebuild().
      return relBuilder
              .push(leftInput)
              .push(newRightInput)
              .join(join.getJoinType(), newJoinCondition)
              .filter(rexBuilder.makeCall(SqlStdOperatorTable.OR, deleteBranchFilter, insertBranchFilter))
              .project(projects, projectNames)
              .build();
    }
  }
}
