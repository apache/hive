package org.apache.hadoop.hive.ql.optimizer.calcite;/*
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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

import java.util.ArrayList;
import java.util.List;

public class HiveDeletedRowPropagator extends HiveRelShuttleImpl {

  private final RelBuilder relBuilder;

  public HiveDeletedRowPropagator(RelBuilder relBuilder) {
    this.relBuilder = relBuilder;
  }

  public RelNode propagate(RelNode relNode) {
    return relNode.accept(this);
  }

//  @Override
//  public RelNode visit(HiveFilter filter) {
//    RelNode input = visitChild(filter, 0, filter.getInput());
//    return relBuilder
//            .push(input)
//            .filter(filter.getCondition())
//            .build();
//  }

  @Override
  public RelNode visit(HiveProject project) {
    RelNode projectInput = visitChild(project, 0, project.getInput());
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
    visitChild(join, 0, join.getInput(0));
    visitChild(join, 1, join.getInput(1));
    RelNode leftInput = join.getLeft();
    int leftRowIsNullIndex = leftInput.getRowType().getFieldCount() - 1;
    RelNode rightInput = join.getRight();
    int rightRowIsNullIndex = rightInput.getRowType().getFieldCount() - 1;

    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RexNode leftRowIsNull = rexBuilder.makeInputRef(
            leftInput.getRowType().getFieldList().get(leftRowIsNullIndex).getType(), leftRowIsNullIndex);
    RexNode rightRowIsNull = rexBuilder.makeInputRef(
            rightInput.getRowType().getFieldList().get(leftRowIsNullIndex).getType(),
            leftInput.getRowType().getFieldCount() + rightRowIsNullIndex);

    List<RexNode> projects = new ArrayList<>(join.getRowType().getFieldCount() + 1);
    for (int i = 0; i < join.getRowType().getFieldCount(); ++i) {
      projects.add(rexBuilder.makeInputRef(join.getRowType().getFieldList().get(i).getType(), i));
    }
    projects.add(rexBuilder.makeCall(SqlStdOperatorTable.AND, leftRowIsNull, rightRowIsNull));

    return relBuilder
            .push(leftInput)
            .push(rightInput)
            .project(projects)
            .build();
  }
}
