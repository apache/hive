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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttle;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttleImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link HiveRelShuttle} to propagate rowIsDeleted column to all HiveRelNodes' rowType in the plan.
 * General rule: we expect that the rowIsDeleted column is the last column in the input rowType of the current
 * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode}.
 */
public class HiveRowIsDeletedPropagator extends HiveRelShuttleImpl {

  private final RelBuilder relBuilder;
  private boolean foundTopRightJoin;

  public HiveRowIsDeletedPropagator(RelBuilder relBuilder) {
    this.relBuilder = relBuilder;
  }

  public RelNode propagate(RelNode relNode) {
    foundTopRightJoin = false;
    return relNode.accept(this);
  }

  /**
   * Create a Projection on top of TS that contains all columns from TS.
   * Let rowIsDeleted the last column in the new Project.
   * Enable fetching Deleted rows in TS.
   * @param scan - TS to transform
   * @return - new TS and a optionally a Project on top of it.
   */
  @Override
  public RelNode visit(HiveTableScan scan) {
    RelDataType tableRowType = scan.getTable().getRowType();
    RelDataTypeField column = tableRowType.getField(
        VirtualColumn.ROWISDELETED.getName(), false, false);
    if (column == null) {
      // This should not happen since Virtual columns are propagated for all native table scans in
      // CalcitePlanner.genTableLogicalPlan()
      throw new ColumnPropagationException("TableScan " + scan + " row schema does not contain " +
          VirtualColumn.ROWISDELETED.getName() + " virtual column");
    }

    RexBuilder rexBuilder = relBuilder.getRexBuilder();

    List<RexNode> projects = new ArrayList<>(tableRowType.getFieldCount());
    List<String> projectNames = new ArrayList<>(tableRowType.getFieldCount());
    populateProjects(rexBuilder, tableRowType, projects, projectNames);
    // Propagated column is already in the TS move it to the end
    RexNode propagatedColumn = projects.remove(column.getIndex());
    projects.add(propagatedColumn);
    String propagatedColumnName = projectNames.remove(column.getIndex());
    projectNames.add(propagatedColumnName);

    // Note: as a nature of Calcite if row schema of TS and the new Project would be exactly the same no
    // Project is created.
    return relBuilder
        .push(scan.enableFetchDeletedRows())
        .project(projects, projectNames)
        .build();
  }

  /**
   * Create a new Project with original projected columns plus add rowIsDeleted as last column referencing
   * the last column of the input {@link RelNode}.
   * @param project - {@link HiveProject to transform}
   * @return new Project
   */
  @Override
  public RelNode visit(HiveProject project) {
    if (!foundTopRightJoin) {
      return super.visit(project);
    }

    RelNode newProject = visitChild(project, 0, project.getInput());
    RelNode projectInput = newProject.getInput(0);
    int rowIsDeletedIndex = projectInput.getRowType().getFieldCount() - 1;
    List<RexNode> newProjects = new ArrayList<>(project.getRowType().getFieldCount() + 1);
    newProjects.addAll(project.getProjects());

    RexNode rowIsDeleted = relBuilder.getRexBuilder().makeInputRef(
            projectInput.getRowType().getFieldList().get(rowIsDeletedIndex).getType(), rowIsDeletedIndex);
    newProjects.add(rowIsDeleted);

    return relBuilder
            .push(projectInput)
            .project(newProjects)
            .build();
  }

  /**
   * Create new Join and a Project on top of it.
   * @param join - {@link HiveJoin} to transform
   * @return - new Join with a Project on top
   */
  @Override
  public RelNode visit(HiveJoin join) {
    if (!foundTopRightJoin) {
      if (join.getJoinType() != JoinRelType.RIGHT) {
        return super.visit(join);
      }

      foundTopRightJoin = true;
      // This should be a Scan on the MV
      RelNode leftInput = join.getLeft();

      // This branch is querying the rows should be inserted/deleted into the view since the last rebuild.
      RelNode rightInput = join.getRight();

      RelNode tmpJoin = visitChild(join, 1, rightInput);
      RelNode newRightInput = tmpJoin.getInput(1);

      // Create input ref to rowIsDeleteColumn. It is used in filter condition later.
      RelDataType newRowType = newRightInput.getRowType();
      int rowIsDeletedIdx = newRowType.getFieldCount() - 1;
      RexBuilder rexBuilder = relBuilder.getRexBuilder();
      RexNode rowIsDeleted = rexBuilder.makeInputRef(
          newRowType.getFieldList().get(rowIsDeletedIdx).getType(),
          leftInput.getRowType().getFieldCount() + rowIsDeletedIdx);

      List<RexNode> projects = new ArrayList<>(newRowType.getFieldCount());
      List<String> projectNames = new ArrayList<>(newRowType.getFieldCount());
      for (int i = 0; i < leftInput.getRowType().getFieldCount(); ++i) {
        RelDataTypeField relDataTypeField = leftInput.getRowType().getFieldList().get(i);
        projects.add(rexBuilder.makeInputRef(relDataTypeField.getType(), i));
        projectNames.add(relDataTypeField.getName());
      }
      for (int i = 0; i < newRowType.getFieldCount() - 1; ++i) {
        RelDataTypeField relDataTypeField = newRowType.getFieldList().get(i);
        projects.add(rexBuilder.makeInputRef(relDataTypeField.getType(), leftInput.getRowType().getFieldCount() + i));
        projectNames.add(relDataTypeField.getName());
      }

      // Create new Top Right Join and a Filter. The filter condition is used in CalcitePlanner.fixUpASTJoinIncrementalRebuild().
      return relBuilder
          .push(leftInput)
          .push(newRightInput)
          .join(join.getJoinType(), join.getCondition())
          .filter(rexBuilder.makeCall(SqlStdOperatorTable.OR,
              rowIsDeleted, rexBuilder.makeCall(SqlStdOperatorTable.NOT, rowIsDeleted)))
          .project(projects, projectNames)
          .build();
    }

    // Propagate rowISDeleted to left input
    RelNode tmpJoin = visitChild(join, 0, join.getInput(0));
    RelNode leftInput = tmpJoin.getInput(0);
    RelDataType leftRowType = tmpJoin.getInput(0).getRowType();
    int leftRowIsDeletedIndex = leftRowType.getFieldCount() - 1;
    // Propagate rowISDeleted to right input
    tmpJoin = visitChild(join, 1, join.getInput(1));
    RelNode rightInput = tmpJoin.getInput(1);
    RelDataType rightRowType = rightInput.getRowType();
    int rightRowIsDeletedIndex = rightRowType.getFieldCount() - 1;

    // Create input ref to rowIsDeleted columns in left and right inputs
    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RexNode leftRowIsDeleted = rexBuilder.makeInputRef(
            leftRowType.getFieldList().get(leftRowIsDeletedIndex).getType(), leftRowIsDeletedIndex);
    RexNode rightRowIsDeleted = rexBuilder.makeInputRef(
            rightRowType.getFieldList().get(rightRowIsDeletedIndex).getType(),
            leftRowType.getFieldCount() + rightRowIsDeletedIndex);

    // Collect projected columns: all columns from both inputs except rowIsDeleted columns
    List<RexNode> projects = new ArrayList<>(leftRowType.getFieldCount() + rightRowType.getFieldCount() - 1);
    List<String> projectNames = new ArrayList<>(leftRowType.getFieldCount() + rightRowType.getFieldCount() - 1);
    populateProjects(rexBuilder, leftRowType, 0, leftRowType.getFieldCount() - 1, projects, projectNames);
    populateProjects(rexBuilder, rightRowType, leftRowType.getFieldCount(), rightRowType.getFieldCount() - 1, projects, projectNames);
    // Add rowIsDeleted column to project
    projects.add(rexBuilder.makeCall(SqlStdOperatorTable.OR, leftRowIsDeleted, rightRowIsDeleted));
    projectNames.add("rowIsDeleted");

    // Shift column references refers columns coming from right input by one in join condition since the new left input
    // has a new column
    RexNode newJoinCondition = new InputRefShifter(leftRowType.getFieldCount() - 1, relBuilder)
        .apply(join.getCondition());

    return relBuilder
            .push(leftInput)
            .push(rightInput)
            .join(join.getJoinType(), newJoinCondition)
            .project(projects)
            .build();
  }

  private static class InputRefShifter extends RexShuttle {
    private final int startIndex;
    private final RelBuilder relBuilder;

    private InputRefShifter(int startIndex, RelBuilder relBuilder) {
      this.startIndex = startIndex;
      this.relBuilder = relBuilder;
    }

    /**
     * Shift input reference index by one if the referenced column index is higher or equals with the startIndex.
     * @param inputRef - {@link RexInputRef} to transform
     * @return new {@link RexInputRef} if the referenced column index is higher or equals with the startIndex,
     * original otherwise
     */
    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      if (inputRef.getIndex() >= startIndex) {
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        return rexBuilder.makeInputRef(inputRef.getType(), inputRef.getIndex() + 1);
      }
      return inputRef;
    }
  }

  private void populateProjects(RexBuilder rexBuilder, RelDataType inputRowType,
                                List<RexNode> projects, List<String> projectNames) {
    populateProjects(rexBuilder, inputRowType, 0, inputRowType.getFieldCount(), projects, projectNames);
  }
  private void populateProjects(RexBuilder rexBuilder, RelDataType inputRowType, int offset, int length,
                                List<RexNode> projects, List<String> projectNames) {
    for (int i = 0; i < length; ++i) {
      RelDataTypeField relDataTypeField = inputRowType.getFieldList().get(i);
      projects.add(rexBuilder.makeInputRef(relDataTypeField.getType(), offset + i));
      projectNames.add(relDataTypeField.getName());
    }
  }
}
