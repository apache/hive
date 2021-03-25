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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFieldTrimmerRule;

public class HiveFetchDeletedRowsRule2 extends HiveFieldTrimmerRule {


  public HiveFetchDeletedRowsRule2() {
    super(operand(Join.class, any()), false, "HiveFetchDeletedRowsRule");
  }

  @Override
  protected boolean canGo(RelOptRuleCall call, RelNode node) {
    return ((Join) node).getJoinType() == JoinRelType.RIGHT;
  }

  @Override
  protected RelNode trim(RelOptRuleCall call, RelNode node) {
    Join join = (Join) node;

    RelNode leftInput = join.getLeft();

    RelNode rightInput = join.getRight();
    RelNode newRightInput = new HiveFetchDeletedRowsPropagator(call.builder(), null).propagate(rightInput);

    RelDataType newRowType = newRightInput.getRowType();
    int rowIsDeletedIdx = newRowType.getFieldCount() - 1;
    RexBuilder rexBuilder = call.builder().getRexBuilder();
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

    return call.builder()
        .push(leftInput)
        .push(newRightInput)
        .join(join.getJoinType(), join.getCondition())
        .filter(rexBuilder.makeCall(SqlStdOperatorTable.OR,
            rowIsDeleted, rexBuilder.makeCall(SqlStdOperatorTable.NOT, rowIsDeleted)))
        .project(projects, projectNames)
        .build();
  }
}
