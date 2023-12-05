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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttle;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttleImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
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

  protected final RelBuilder relBuilder;

  public HiveRowIsDeletedPropagator(RelBuilder relBuilder) {
    this.relBuilder = relBuilder;
  }

  public RelNode propagate(RelNode relNode) {
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
    projects.add(rexBuilder.makeCall(SqlStdOperatorTable.NOT, propagatedColumn));
    projectNames.remove(column.getIndex());
    projectNames.add("_deleted");
    projectNames.add("_inserted");

    // Note: as a nature of Calcite if row schema of TS and the new Project would be exactly the same no
    // Project is created.
    return relBuilder
        .push(scan.setTableScanTrait(HiveTableScan.HiveTableScanTrait.FetchDeletedRows))
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
    RelNode newProject = visitChild(project, 0, project.getInput());
    RelNode projectInput = newProject.getInput(0);

    List<RexNode> newProjects = new ArrayList<>(project.getRowType().getFieldCount() + 1);
    newProjects.addAll(project.getProjects());
    newProjects.add(createInputRef(projectInput, 2));
    newProjects.add(createInputRef(projectInput, 1));

    return relBuilder
            .push(projectInput)
            .project(newProjects)
            .build();
  }

  protected RexNode createInputRef(RelNode relNode, int negativeOffset) {
    int index = relNode.getRowType().getFieldCount() - negativeOffset;
    return relBuilder.getRexBuilder().makeInputRef(
        relNode.getRowType().getFieldList().get(index).getType(), index);
  }

  @Override
  public RelNode visit(HiveFilter filter) {
    RelNode filterInput = filter.getInput();
    HiveFilter newFilter = (HiveFilter) visitChild(filter, 0, filterInput);

    if (!(filterInput instanceof HiveJoin)) {
      return newFilter;
    }

    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RelNode projectTopOnJoin = newFilter.getInput(0);
    RexNode anyDeleted = createInputRef(projectTopOnJoin, 2);
    RexNode anyInserted = createInputRef(projectTopOnJoin, 1);

    RexNode additionalFilter = createFilterCondition(rexBuilder, newFilter.getCondition(), anyDeleted, anyInserted);

    RexNode newFilterCondition = rexBuilder.makeCall(
        SqlStdOperatorTable.AND, newFilter.getCondition(), additionalFilter);

    return relBuilder
        .push(projectTopOnJoin)
        .filter(newFilterCondition)
        .build();
  }

  /*
   * Generate a filter condition to filter out rows which has a matching join key and
   * it is a result of an inserted row and a deleted row
   *
   * Example:
   * left.key, right.key, left.writeId > x, right.writeId > y, left.deleted, right.deleted
   *    1         1             true            true           false         true
   *    2         2             true            true           true          false
   */
  protected RexNode createFilterCondition(
      RexBuilder rexBuilder, RexNode filterCondition, RexNode anyDeleted, RexNode anyInserted) {
    RexCall topNode = (RexCall) filterCondition;
    // Searching for predicate OR($7, $2)
    //          HiveJoin(condition=[AND(=($0, $4), OR($7, $2))], joinType=[inner])
    //            HiveProject(..., $f2=[<(1, $6.writeid)])
    //              ...
    //            HiveProject(..., $f3=[<(1, $6.writeid)])
    //              ...
    // Where $7 and $2 references the predicates with writeId in join inputs
    // If there is no Join condition on the view definition query the topNode is the one we need
    // or the last conjunct otherwise
    RexCall writeIdDisjunction;
    if (topNode.isA(SqlKind.OR)) {
      writeIdDisjunction = topNode;
    } else {
      writeIdDisjunction = (RexCall) topNode.operands.get(topNode.operands.size() - 1);
    }

    List<RexNode> conjunctOperands = new ArrayList<>(writeIdDisjunction.operands.size() + 1);
    conjunctOperands.addAll(writeIdDisjunction.operands);
    conjunctOperands.add(anyDeleted);
    conjunctOperands.add(anyInserted);
    return rexBuilder.makeCall(SqlStdOperatorTable.NOT, RexUtil.composeConjunction(rexBuilder,  conjunctOperands));
  }

  /**
   * Create new Join and a Project on top of it.
   * @param join - {@link HiveJoin} to transform
   * @return - new Join with a Project on top
   */
  @Override
  public RelNode visit(HiveJoin join) {
    // Propagate rowISDeleted to left input
    RelNode tmpJoin = visitChild(join, 0, join.getInput(0));
    RelNode leftInput = tmpJoin.getInput(0);
    RelDataType leftRowType = tmpJoin.getInput(0).getRowType();
    // Propagate rowISDeleted to right input
    tmpJoin = visitChild(join, 1, join.getInput(1));
    RelNode rightInput = tmpJoin.getInput(1);
    RelDataType rightRowType = rightInput.getRowType();
    int rightAnyDeletedIndex = rightRowType.getFieldCount() - 2;
    int rightAnyInsertedIndex = rightRowType.getFieldCount() - 1;

    // Create input ref to rowIsDeleted columns in left and right inputs
    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RexNode leftAnyDeleted = createInputRef(leftInput, 2);
    RexNode leftAnyInserted = createInputRef(leftInput, 1);
    RexNode rightAnyDeleted = rexBuilder.makeInputRef(
            rightRowType.getFieldList().get(rightAnyDeletedIndex).getType(),
            leftRowType.getFieldCount() + rightAnyDeletedIndex);
    RexNode rightAnyInserted = rexBuilder.makeInputRef(
            rightRowType.getFieldList().get(rightAnyInsertedIndex).getType(),
            leftRowType.getFieldCount() + rightAnyInsertedIndex);

    RexNode newJoinCondition;
    int newLeftFieldCount;
    int shift;
    if (leftRowType.getField("_any_inserted", false, false) == null) {
      shift = 1;
      newLeftFieldCount = leftRowType.getFieldCount() - 1;
    } else {
      shift = 2;
      newLeftFieldCount = leftRowType.getFieldCount() - 2;
    }
    // Shift column references refers columns coming from right input in join condition since the new left input
    // has a new columns
    newJoinCondition = new InputRefShifter(newLeftFieldCount, relBuilder).apply(join.getCondition());

    // Collect projected columns: all columns from both inputs
    List<RexNode> projects = new ArrayList<>(newLeftFieldCount + rightRowType.getFieldCount() + 1);
    List<String> projectNames = new ArrayList<>(newLeftFieldCount + rightRowType.getFieldCount() + 1);
    populateProjects(rexBuilder, leftRowType, 0, newLeftFieldCount, projects, projectNames);
    populateProjects(rexBuilder, rightRowType, leftRowType.getFieldCount(), rightRowType.getFieldCount() - 1, projects, projectNames);

    // Add rowIsDeleted column to project
    projects.add(rexBuilder.makeCall(SqlStdOperatorTable.OR, leftAnyDeleted, rightAnyDeleted));
    projects.add(rexBuilder.makeCall(SqlStdOperatorTable.OR, leftAnyInserted, rightAnyInserted));
    projectNames.add("_any_deleted");
    projectNames.add("_any_inserted");

    return relBuilder
            .push(leftInput)
            .push(rightInput)
            .join(join.getJoinType(), newJoinCondition)
            .project(projects, projectNames)
            .build();
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
