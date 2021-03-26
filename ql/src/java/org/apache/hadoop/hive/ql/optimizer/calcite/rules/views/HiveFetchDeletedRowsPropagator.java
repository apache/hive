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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttleImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HiveFetchDeletedRowsPropagator extends HiveRelShuttleImpl {

  private final RelBuilder relBuilder;

  public HiveFetchDeletedRowsPropagator(RelBuilder relBuilder) {
    this.relBuilder = relBuilder;
  }

  public RelNode propagate(RelNode relNode) {
    return relNode.accept(this);
  }

  @Override
  public RelNode visit(HiveTableScan scan) {
    RelDataType tableRowType = scan.getTable().getRowType();
    RelDataTypeField column = tableRowType.getField(
            VirtualColumn.ROWISDELETED.getName(), false, false);
    RexBuilder rexBuilder = relBuilder.getRexBuilder();

    List<RexNode> projects;
    List<String> projectNames;
    if (column == null) {
      RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
      RexNode propagatedColumn = rexBuilder.makeLiteral(
          Boolean.FALSE,
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BOOLEAN), true),
          false);
      projects = new ArrayList<>(tableRowType.getFieldCount() + 1);
      projectNames = new ArrayList<>(tableRowType.getFieldCount() + 1);
      populateProjects(rexBuilder, tableRowType, projects, projectNames);
      projects.add(propagatedColumn);
      projectNames.add("rowIsDeleted");
    } else {
      projects = new ArrayList<>(tableRowType.getFieldCount());
      projectNames = new ArrayList<>(tableRowType.getFieldCount());
      populateProjects(rexBuilder, tableRowType, projects, projectNames);
      // Propagated column is already in the TS move it to the end
      RexNode propagatedColumn = projects.remove(column.getIndex());
      projects.add(propagatedColumn);
      String propagatedColumnName = projectNames.remove(column.getIndex());
      projectNames.add(propagatedColumnName);
    }

    return relBuilder
            .push(scan.withFetchDeletedRows())
            .project(projects, projectNames)
            .build();
  }

  @Override
  public RelNode visit(HiveProject project) {
    RelNode newProject = visitChild(project, 0, project.getInput());
    RelNode projectInput = newProject.getInput(0);
    int rowIsNullIndex = projectInput.getRowType().getFieldCount() - 1;
    List<RexNode> newProjects = new ArrayList<>(project.getRowType().getFieldCount() + 1);
    newProjects.addAll(project.getProjects());

    RexNode rowIsNull = relBuilder.getRexBuilder().makeInputRef(
            projectInput.getRowType().getFieldList().get(rowIsNullIndex).getType(), rowIsNullIndex);
    newProjects.add(rowIsNull);

    return relBuilder
            .push(projectInput)
            .project(newProjects)
            .build();
  }

  @Override
  public RelNode visit(HiveJoin join) {
    RelNode newJoin = visitChild(join, 0, join.getInput(0));
    RelNode leftInput = newJoin.getInput(0);
    RelDataType leftRowType = newJoin.getInput(0).getRowType();
    int leftRowIsDeletedIndex = leftRowType.getFieldCount() - 1;
    newJoin = visitChild(join, 1, join.getInput(1));
    RelNode rightInput = newJoin.getInput(1);
    RelDataType rightRowType = rightInput.getRowType();
    int rightRowIsDeletedIndex = rightRowType.getFieldCount() - 1;

    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RexNode leftRowIsDeleted = rexBuilder.makeInputRef(
            leftRowType.getFieldList().get(leftRowIsDeletedIndex).getType(), leftRowIsDeletedIndex);
    RexNode rightRowIsDeleted = rexBuilder.makeInputRef(
            rightRowType.getFieldList().get(rightRowIsDeletedIndex).getType(),
            leftRowType.getFieldCount() + rightRowIsDeletedIndex);

    List<RexNode> projects = new ArrayList<>(leftRowType.getFieldCount() + rightRowType.getFieldCount() - 1);
    List<String> projectNames = new ArrayList<>(leftRowType.getFieldCount() + rightRowType.getFieldCount() - 1);
    populateProjects(rexBuilder, leftRowType, 0, leftRowType.getFieldCount() - 1, projects, projectNames);
    populateProjects(rexBuilder, rightRowType, leftRowType.getFieldCount(), rightRowType.getFieldCount() - 1, projects, projectNames);
    projects.add(rexBuilder.makeCall(SqlStdOperatorTable.OR, leftRowIsDeleted, rightRowIsDeleted));
    projectNames.add("rowIsDeleted");

    RexNode newJoinCondition = new JoinConditionShifter(leftRowType.getFieldCount() - 1, relBuilder)
        .apply(join.getCondition());

    return relBuilder
            .push(leftInput)
            .push(rightInput)
            .join(join.getJoinType(), newJoinCondition)
            .project(projects)
            .build();
  }

  private static class JoinConditionShifter extends RexShuttle {
    private final int rightStartIndex;
    private final RelBuilder relBuilder;

    private JoinConditionShifter(int rightStartIndex, RelBuilder relBuilder) {
      this.rightStartIndex = rightStartIndex;
      this.relBuilder = relBuilder;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      if (inputRef.getIndex() >= rightStartIndex) {
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
