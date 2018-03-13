/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlMinMaxAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlSumAggFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * This rule will perform a rewriting to prepare the plan for incremental
 * view maintenance in case there exist aggregation operator, so we can
 * avoid the INSERT OVERWRITE and use a MERGE statement instead.
 *
 * In particular, the INSERT OVERWRITE maintenance will look like this
 * (in SQL):
 * INSERT OVERWRITE mv
 * SELECT a, b, SUM(s) as s, SUM(c) AS c
 * FROM (
 *   SELECT * from mv --OLD DATA
 *   UNION ALL
 *   SELECT a, b, SUM(x) AS s, COUNT(*) AS c --NEW DATA
 *   FROM TAB_A
 *   JOIN TAB_B ON (TAB_A.a = TAB_B.z)
 *   WHERE TAB_A.ROW_ID > 5
 *   GROUP BY a, b) inner_subq
 * GROUP BY a, b;
 *
 * We need to transform that into:
 * MERGE INTO mv
 * USING (
 *   SELECT a, b, SUM(x) AS s, COUNT(*) AS c --NEW DATA
 *   FROM TAB_A
 *   JOIN TAB_B ON (TAB_A.a = TAB_B.z)
 *   WHERE TAB_A.ROW_ID > 5
 *   GROUP BY a, b) source
 * ON (mv.a = source.a AND mv.b = source.b)
 * WHEN MATCHED AND mv.c + source.c <> 0
 *   THEN UPDATE SET mv.s = mv.s + source.s, mv.c = mv.c + source.c
 * WHEN NOT MATCHED
 *   THEN INSERT VALUES (source.a, source.b, s, c);
 *
 * To be precise, we need to convert it into a MERGE rewritten as:
 * FROM mv right outer join _source_ source
 * ON (mv.a = source.a AND mv.b = source.b)
 * INSERT INTO TABLE mv
 *   SELECT source.a, source.b, s, c
 *   WHERE mv.a IS NULL AND mv2.b IS NULL
 * INSERT INTO TABLE mv
 *   SELECT mv.ROW__ID, source.a, source.b, mv.s + source.s, mv.c + source.c
 *   WHERE source.a=mv.a AND source.b=mv.b
 *   SORT BY mv.ROW__ID;
 */
public class HiveAggregateIncrementalRewritingRule extends RelOptRule {

  public static final HiveAggregateIncrementalRewritingRule INSTANCE =
      new HiveAggregateIncrementalRewritingRule();

  private HiveAggregateIncrementalRewritingRule() {
    super(operand(Aggregate.class, operand(Union.class, any())),
        HiveRelFactories.HIVE_BUILDER,
        "HiveAggregateIncrementalRewritingRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate agg = call.rel(0);
    final Union union = call.rel(1);
    final RexBuilder rexBuilder =
        agg.getCluster().getRexBuilder();
    // 1) First branch is query, second branch is MV
    final RelNode joinLeftInput = union.getInput(1);
    final RelNode joinRightInput = union.getInput(0);
    // 2) Build conditions for join and filter and start adding
    // expressions for project operator
    List<RexNode> projExprs = new ArrayList<>();
    List<RexNode> joinConjs = new ArrayList<>();
    List<RexNode> filterConjs = new ArrayList<>();
    int groupCount = agg.getGroupCount();
    int totalCount = agg.getGroupCount() + agg.getAggCallList().size();
    for (int leftPos = 0, rightPos = totalCount;
         leftPos < groupCount; leftPos++, rightPos++) {
      RexNode leftRef = rexBuilder.makeInputRef(
          joinLeftInput.getRowType().getFieldList().get(leftPos).getType(), leftPos);
      RexNode rightRef = rexBuilder.makeInputRef(
          joinRightInput.getRowType().getFieldList().get(leftPos).getType(), rightPos);
      projExprs.add(rightRef);
      joinConjs.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
          ImmutableList.of(leftRef, rightRef)));
      filterConjs.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
          ImmutableList.of(leftRef)));
    }
    // 3) Add the expressions that correspond to the aggregation
    // functions
    RexNode caseFilterCond = rexBuilder.makeCall(SqlStdOperatorTable.AND, filterConjs);
    for (int i = 0, leftPos = groupCount, rightPos = totalCount + groupCount;
         leftPos < totalCount; i++, leftPos++, rightPos++) {
      // case when mv2.deptno IS NULL AND mv2.deptname IS NULL then s else source.s + mv2.s end
      RexNode leftRef = rexBuilder.makeInputRef(
          joinLeftInput.getRowType().getFieldList().get(leftPos).getType(), leftPos);
      RexNode rightRef = rexBuilder.makeInputRef(
          joinRightInput.getRowType().getFieldList().get(leftPos).getType(), rightPos);
      // Generate SQLOperator for merging the aggregations
      RexNode elseReturn;
      SqlAggFunction aggCall = agg.getAggCallList().get(i).getAggregation();
      switch (aggCall.getKind()) {
      case SUM:
        // SUM and COUNT are rolled up as SUM, hence SUM represents both here
        elseReturn = rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
            ImmutableList.of(rightRef, leftRef));
        break;
      case MIN: {
        RexNode condInnerCase = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
            ImmutableList.of(rightRef, leftRef));
        elseReturn = rexBuilder.makeCall(SqlStdOperatorTable.CASE,
            ImmutableList.of(condInnerCase, rightRef, leftRef));
        }
        break;
      case MAX: {
        RexNode condInnerCase = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
            ImmutableList.of(rightRef, leftRef));
        elseReturn = rexBuilder.makeCall(SqlStdOperatorTable.CASE,
            ImmutableList.of(condInnerCase, rightRef, leftRef));
        }
        break;
      default:
        throw new AssertionError("Found an aggregation that could not be"
            + " recognized: " + aggCall);
      }
      projExprs.add(rexBuilder.makeCall(SqlStdOperatorTable.CASE,
          ImmutableList.of(caseFilterCond, rightRef, elseReturn)));
    }
    RexNode joinCond = rexBuilder.makeCall(SqlStdOperatorTable.AND, joinConjs);
    RexNode filterCond = rexBuilder.makeCall(SqlStdOperatorTable.AND, filterConjs);
    // 3) Build plan
    RelNode newNode = call.builder()
        .push(union.getInput(1))
        .push(union.getInput(0))
        .join(JoinRelType.RIGHT, joinCond)
        .filter(rexBuilder.makeCall(SqlStdOperatorTable.OR, ImmutableList.of(joinCond, filterCond)))
        .project(projExprs)
        .build();
    call.transformTo(newNode);
  }

}
