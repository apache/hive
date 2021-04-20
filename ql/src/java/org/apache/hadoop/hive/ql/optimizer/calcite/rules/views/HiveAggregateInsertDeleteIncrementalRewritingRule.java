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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveHepExtractRelNodeRule;

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
 * INSERT INTO TABLE mv                                       <- (insert new rows into the view)
 *   SELECT source.a, source.b, s, c
 *   WHERE mv.flag IS NULL
 * INSERT INTO TABLE mv                                       <- (update existing rows in the view)
 *   SELECT mv.ROW__ID, source.a, source.b,
 *     CASE WHEN mv.s IS NULL AND source.s IS NULL THEN NULL
 *          WHEN mv.s IS NULL THEN source.s
 *          WHEN source.s IS NULL THEN mv.s
 *          ELSE mv.s + source.s END,
 *     CASE WHEN mv.s IS NULL AND source.s IS NULL THEN NULL
 *          WHEN mv.s IS NULL THEN source.s
 *          WHEN source.s IS NULL THEN mv.s
 *          ELSE mv.s + source.s END countStar,
 *   WHERE mv.flag AND countStar > 0
 *   SORT BY mv.ROW__ID;
 * INSERT INTO TABLE mv                                       <- (delete delta)
 *   SELECT mv.ROW__ID
 *   WHERE mv.flag AND countStar = 0
 *   SORT BY mv.ROW__ID;
 */
public class HiveAggregateInsertDeleteIncrementalRewritingRule extends RelOptRule {

  public static final HiveAggregateInsertDeleteIncrementalRewritingRule INSTANCE =
      new HiveAggregateInsertDeleteIncrementalRewritingRule();

  private HiveAggregateInsertDeleteIncrementalRewritingRule() {
    super(operand(Aggregate.class, operand(Union.class, operand(Aggregate.class, any()))),
        HiveRelFactories.HIVE_BUILDER,
        "HiveAggregateInsertDeleteIncrementalRewritingRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
//    final Aggregate topAgg = call.rel(0);
    final Union union = call.rel(1);
    final Aggregate queryAgg = call.rel(2);
    RelBuilder relBuilder = call.builder();
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();

    // 1) First branch is query, second branch is MV
    RelNode joinLeftInput = union.getInput(1);

    RelNode aggInput = queryAgg.getInput();
    aggInput = HiveHepExtractRelNodeRule.execute(aggInput);

    aggInput = new HiveRowIsDeletedPropagator(relBuilder).propagate(aggInput);

    int rowIsDeletedIdx = aggInput.getRowType().getFieldCount() - 1;
    RexNode rowIsDeletedNode = rexBuilder.makeInputRef(
            aggInput.getRowType().getFieldList().get(rowIsDeletedIdx).getType(), rowIsDeletedIdx);

    int countIdx = -1;
    List<RelBuilder.AggCall> newAggregateCalls = new ArrayList<>(queryAgg.getAggCallList().size());
    for (int i = 0; i < queryAgg.getAggCallList().size(); ++i) {
      AggregateCall aggregateCall = queryAgg.getAggCallList().get(i);
      if (aggregateCall.getAggregation().getKind() == SqlKind.COUNT && aggregateCall.getArgList().size() == 0) {
        countIdx = i + queryAgg.getGroupCount();
      }

      RexNode argument;
      SqlAggFunction aggFunction;
      switch (aggregateCall.getAggregation().getKind()) {
        case COUNT:
          aggFunction = SqlStdOperatorTable.SUM;
          argument = relBuilder.literal(1);
          break;
        case SUM:
          aggFunction = SqlStdOperatorTable.SUM;
          Integer argumentIdx = aggregateCall.getArgList().get(0);
          argument = rexBuilder.makeInputRef(
                  aggInput.getRowType().getFieldList().get(argumentIdx).getType(), argumentIdx);
          break;
        default:
          throw new AssertionError("Found an aggregation that could not be"
                  + " recognized: " + aggregateCall);
      }

      RexNode minus = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, relBuilder.literal(-1), argument);
      RexNode newArgument = rexBuilder.makeCall(SqlStdOperatorTable.CASE, rowIsDeletedNode, minus, argument);

      newAggregateCalls.add(relBuilder.aggregateCall(aggFunction, newArgument));
    }

    if (countIdx == -1) {
      // Count(*) not found. It is required to determine if a group should be deleted from the view.
      // Can not rewrite, bail out;
      return;
    }

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

    joinLeftInput = relBuilder
            .push(joinLeftInput)
            .project(mvCols)
            .build();

    RelNode joinRightInput = relBuilder
            .push(aggInput)
            .aggregate(relBuilder.groupKey(queryAgg.getGroupSet()), newAggregateCalls)
            .build();

    // 3) Build conditions for join and start adding
    // expressions for project operator
    List<RexNode> projExprs = new ArrayList<>();
    List<RexNode> joinConjs = new ArrayList<>();
    int groupCount = queryAgg.getGroupCount();
    int totalCount = queryAgg.getGroupCount() + queryAgg.getAggCallList().size();
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
    RelNode join = relBuilder
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
      SqlAggFunction aggCall = queryAgg.getAggCallList().get(i).getAggregation();
      switch (aggCall.getKind()) {
      case SUM:
      case COUNT:
        // SUM and COUNT are rolled up as SUM, hence SUM represents both here
        elseReturn = rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
            ImmutableList.of(rightRef, leftRef));
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

    RexNode countStarCase = projExprs.get(countIdx);

    // 6) Build plan
    // Split this filter condition in CalcitePlanner.fixUpASTAggregateIncrementalRebuild:
    // First disjunct for update branch
    // Second disjunct for insert branch
//    RexNode filterCond = rexBuilder.makeCall(
//            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, countStarCase, relBuilder.literal(0));

    RexNode countStarGT0 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, countStarCase, relBuilder.literal(0));
    RexNode countStarEq0 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, countStarCase, relBuilder.literal(0));
    RexNode insert = rexBuilder.makeCall(SqlStdOperatorTable.AND,
            rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, flagNode), countStarGT0);
    RexNode update = rexBuilder.makeCall(SqlStdOperatorTable.AND, flagNode, countStarGT0);
    RexNode delete = rexBuilder.makeCall(SqlStdOperatorTable.AND, flagNode, countStarEq0);

    RelNode newNode = relBuilder
        .push(join)
        .filter(rexBuilder.makeCall(SqlStdOperatorTable.OR, insert, update, delete))
        .project(projExprs)
        .build();
    call.transformTo(newNode);
  }
}
