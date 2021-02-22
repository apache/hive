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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

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
 *   WHERE TAB_A.ROW_ID &gt; 5
 *   GROUP BY a, b) inner_subq
 * GROUP BY a, b;
 *
 * We need to transform that into:
 * MERGE INTO mv
 * USING (
 *   SELECT a, b, SUM(x) AS s, COUNT(*) AS c --NEW DATA
 *   FROM TAB_A
 *   JOIN TAB_B ON (TAB_A.a = TAB_B.z)
 *   WHERE TAB_A.ROW_ID &gt; 5
 *   GROUP BY a, b) source
 * ON (mv.a <=> source.a AND mv.b <=> source.b)
 * WHEN MATCHED AND mv.c + source.c &lt;&gt; 0
 *   THEN UPDATE SET mv.s = mv.s + source.s, mv.c = mv.c + source.c
 * WHEN NOT MATCHED
 *   THEN INSERT VALUES (source.a, source.b, s, c);
 *
 * To be precise, we need to convert it into a MERGE rewritten as:
 * FROM (select *, true flag from mv) mv right outer join _source_ source
 * ON (mv.a <=> source.a AND mv.b <=> source.b)
 * INSERT INTO TABLE mv
 *   SELECT source.a, source.b, s, c
 *   WHERE mv.flag IS NULL
 * INSERT INTO TABLE mv
 *   SELECT mv.ROW__ID, source.a, source.b,
 *     CASE WHEN mv.s IS NULL AND source.s IS NULL THEN NULL
 *          WHEN mv.s IS NULL THEN source.s
 *          WHEN source.s IS NULL THEN mv.s
 *          ELSE mv.s + source.s END,
 *          &lt;same CASE statement for mv.c + source.c like above&gt;
 *   WHERE mv.flag
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
    RelNode joinLeftInput = union.getInput(1);
    final RelNode joinRightInput = union.getInput(0);

    // 2) Introduce a Project on top of MV scan having all columns from the view plus a boolean literal which indicates
    // whether the row with the key values coming from the joinRightInput exists in the view:
    // - true means exist
    // - null means not exists
    // Project also needed to encapsulate the view scan by a subquery -> this is required by
    // CalcitePlanner.fixUpASTAggregateIncrementalRebuild
    List<RexNode> mvCols = new ArrayList<>(joinLeftInput.getRowType().getFieldCount());
    for (int i = 0; i < joinLeftInput.getRowType().getFieldCount(); ++i) {
      mvCols.add(rexBuilder.makeInputRef(
              joinLeftInput.getRowType().getFieldList().get(i).getType(), i));
    }
    mvCols.add(rexBuilder.makeLiteral(true));

    joinLeftInput = call.builder()
            .push(joinLeftInput)
            .project(mvCols)
            .build();

    // 3) Build conditions for join and start adding
    // expressions for project operator
    List<RexNode> projExprs = new ArrayList<>();
    List<RexNode> joinConjs = new ArrayList<>();
    int groupCount = agg.getGroupCount();
    int totalCount = agg.getGroupCount() + agg.getAggCallList().size();
    for (int leftPos = 0, rightPos = totalCount + 1;
         leftPos < groupCount; leftPos++, rightPos++) {
      RexNode leftRef = rexBuilder.makeInputRef(
          joinLeftInput.getRowType().getFieldList().get(leftPos).getType(), leftPos);
      RexNode rightRef = rexBuilder.makeInputRef(
          joinRightInput.getRowType().getFieldList().get(leftPos).getType(), rightPos);
      projExprs.add(rightRef);

      RexNode nsEqExpr = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
              ImmutableList.of(leftRef, rightRef));
      joinConjs.add(nsEqExpr);
    }

    // 4) Create join node
    RexNode joinCond = RexUtil.composeConjunction(rexBuilder, joinConjs);
    RelNode join = call.builder()
            .push(joinLeftInput)
            .push(joinRightInput)
            .join(JoinRelType.RIGHT, joinCond)
            .build();

    int flagIndex = joinLeftInput.getRowType().getFieldCount() - 1;
    RexNode flagNode = rexBuilder.makeInputRef(
            join.getRowType().getFieldList().get(flagIndex).getType(), flagIndex);

    // 5) Add the expressions that correspond to the aggregation
    // functions
    for (int i = 0, leftPos = groupCount, rightPos = totalCount + 1 + groupCount;
         leftPos < totalCount; i++, leftPos++, rightPos++) {
      // case when source.s is null and mv2.s is null then null
      // case when source.s IS null then mv2.s
      // case when mv2.s IS null then source.s
      // else source.s + mv2.s end
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
      // According to SQL standard (and Hive) Aggregate functions eliminates null values however operators used in
      // elseReturn expressions returns null if one of their operands is null
      // hence we need a null check of both operands.
      // Note: If both are null, we will fall into branch    WHEN leftNull THEN rightRef
      RexNode leftNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, leftRef);
      RexNode rightNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, rightRef);
      projExprs.add(rexBuilder.makeCall(SqlStdOperatorTable.CASE,
              leftNull, rightRef,
              rightNull, leftRef,
              elseReturn));
    }

    // 6) Build plan
    // Split this filter condition in CalcitePlanner.fixUpASTAggregateIncrementalRebuild:
    // First disjunct for update branch
    // Second disjunct for insert branch
    RexNode filterCond = rexBuilder.makeCall(
            SqlStdOperatorTable.OR, flagNode, rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, flagNode));
    RelNode newNode = call.builder()
        .push(join)
        .filter(filterCond)
        .project(projExprs)
        .build();
    call.transformTo(newNode);
  }
}
