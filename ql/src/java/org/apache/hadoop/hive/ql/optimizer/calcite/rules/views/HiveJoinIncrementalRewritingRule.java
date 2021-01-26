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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

import java.util.ArrayList;
import java.util.List;

public class HiveJoinIncrementalRewritingRule extends RelOptRule {

  public static final HiveJoinIncrementalRewritingRule INSTANCE =
          new HiveJoinIncrementalRewritingRule();

  private HiveJoinIncrementalRewritingRule() {
    super(operand(Union.class, any()),
            HiveRelFactories.HIVE_BUILDER,
            "HiveJoinIncrementalRewritingRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Union union = call.rel(0);
    RexBuilder rexBuilder = union.getCluster().getRexBuilder();
    // First branch is query, second branch is MV
    // 1) First branch is query, second branch is MV
    final RelNode joinLeftInput = union.getInput(1);
    final RelNode joinRightInput = union.getInput(0);

    // 2) Build conditions for join and filter and start adding
    // expressions for project operator
    List<RexNode> projExprs = new ArrayList<>();
    List<RexNode> joinConjs = new ArrayList<>();
//    List<RexNode> filterConjs = new ArrayList<>();
    for (int leftPos = 0; leftPos < joinLeftInput.getRowType().getFieldCount(); leftPos++) {
      RexNode leftRef = rexBuilder.makeInputRef(
              joinLeftInput.getRowType().getFieldList().get(leftPos).getType(), leftPos);
      RexNode rightRef = rexBuilder.makeInputRef(
              joinRightInput.getRowType().getFieldList().get(leftPos).getType(),
              leftPos + joinLeftInput.getRowType().getFieldCount());

      projExprs.add(rightRef);
      joinConjs.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ImmutableList.of(leftRef, rightRef)));
//      filterConjs.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, ImmutableList.of(leftRef)));
    }

    RexNode joinCond = rexBuilder.makeCall(SqlStdOperatorTable.AND, joinConjs);
//    RexNode filterCond = rexBuilder.makeCall(SqlStdOperatorTable.AND, filterConjs);
    // 3) Build plan
    RelNode newNode = call.builder()
            .push(union.getInput(1))
            .push(union.getInput(0))
            .join(JoinRelType.RIGHT, joinCond)
//            .filter(rexBuilder.makeCall(SqlStdOperatorTable.OR, ImmutableList.of(joinCond, filterCond)))
            .project(projExprs)
            .build();
    call.transformTo(newNode);
  }
}
