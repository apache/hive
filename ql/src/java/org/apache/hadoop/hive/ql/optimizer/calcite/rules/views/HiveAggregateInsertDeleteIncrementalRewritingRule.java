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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

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
 * ON (mv.a &lt;=&gt; source.a AND mv.b &lt;=&gt; source.b)
 * WHEN MATCHED AND mv.c + source.c &lt;&gt; 0
 *   THEN UPDATE SET mv.s = mv.s + source.s, mv.c = mv.c + source.c
 * WHEN MATCHED AND countStar = 0 THEN DELETE
 * WHEN NOT MATCHED
 *   THEN INSERT VALUES (source.a, source.b, s, c);
 *
 * To be precise, we need to convert it into a MERGE rewritten as:
 * FROM (select *, true flag from mv) mv right outer join _source_ source
 * ON (mv.a &lt;=&gt; source.a AND mv.b &lt;=&gt; source.b)
 * INSERT INTO TABLE mv                                       &lt;- (insert new rows into the view)
 *   SELECT source.a, source.b, s, c
 *   WHERE mv.flag IS NULL
 * INSERT INTO TABLE mv                                       &lt;- (update existing rows in the view)
 *   SELECT mv.ROW__ID, source.a, source.b,
 *     CASE WHEN mv.s IS NULL AND source.s IS NULL THEN NULL  &lt;- (use case expression to handle nulls from both sides)
 *          WHEN mv.s IS NULL THEN source.s
 *          WHEN source.s IS NULL THEN mv.s
 *          ELSE mv.s + source.s END,
 *     CASE WHEN mv.s IS NULL AND source.s IS NULL THEN NULL
 *          WHEN mv.s IS NULL THEN source.s
 *          WHEN source.s IS NULL THEN mv.s
 *          ELSE mv.s + source.s END countStar,
 *   WHERE mv.flag AND countStar &lt;&gt; 0
 *   SORT BY mv.ROW__ID;
 * INSERT INTO TABLE mv                                       &lt;- (delete from the views)
 *   SELECT mv.ROW__ID
 *   WHERE mv.flag AND countStar = 0
 *   SORT BY mv.ROW__ID;
 *
 * @see org.apache.hadoop.hive.ql.parse.CalcitePlanner
 */
public class HiveAggregateInsertDeleteIncrementalRewritingRule extends HiveAggregateIncrementalRewritingRuleBase<
        HiveAggregateInsertDeleteIncrementalRewritingRule.IncrementalComputePlanWithDeletedRows> {

  public static final HiveAggregateInsertDeleteIncrementalRewritingRule INSTANCE =
      new HiveAggregateInsertDeleteIncrementalRewritingRule();

  private HiveAggregateInsertDeleteIncrementalRewritingRule() {
    super(operand(Aggregate.class, operand(Union.class, operand(Aggregate.class, any()))),
        HiveRelFactories.HIVE_BUILDER,
        "HiveAggregateInsertDeleteIncrementalRewritingRule", 2);
  }

  @Override
  protected IncrementalComputePlanWithDeletedRows createJoinRightInput(RelOptRuleCall call) {
    RelBuilder relBuilder = call.builder();
    Aggregate aggregate = call.rel(2);
    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RelNode aggInput = aggregate.getInput();

    // Propagate rowIsDeleted column
    aggInput = HiveHepExtractRelNodeRule.execute(aggInput);
    aggInput = new HiveRowIsDeletedPropagator(relBuilder).propagate(aggInput);

    int rowIsDeletedIdx = aggInput.getRowType().getFieldCount() - 1;
    RexNode rowIsDeletedNode = rexBuilder.makeInputRef(
            aggInput.getRowType().getFieldList().get(rowIsDeletedIdx).getType(), rowIsDeletedIdx);

    // Search for count(*)
    // and create case expression to aggregate inserted/deleted values:
    //
    // SELECT
    //   sum(case when t1.ROW__IS__DELETED then -b else b end) sumb,
    //   sum(case when t1.ROW__IS__DELETED then -1 else 1 end) countStar
    int countIdx = -1;
    List<RelBuilder.AggCall> newAggregateCalls = new ArrayList<>(aggregate.getAggCallList().size());
    for (int i = 0; i < aggregate.getAggCallList().size(); ++i) {
      AggregateCall aggregateCall = aggregate.getAggCallList().get(i);
      if (aggregateCall.getAggregation().getKind() == SqlKind.COUNT && aggregateCall.getArgList().size() == 0) {
        countIdx = i + aggregate.getGroupCount();
      }

      RexNode argument;
      SqlAggFunction aggFunction;
      switch (aggregateCall.getAggregation().getKind()) {
        case COUNT:
          aggFunction = SqlStdOperatorTable.SUM;

          // count(*)
          if (aggregateCall.getArgList().isEmpty()) {
            argument = relBuilder.literal(1);
          } else {
            // count(<column_name>)
            argument = genArgumentForCountColumn(relBuilder, aggInput, aggregateCall.getArgList().get(0));
          }
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
      // count(*) not found. It is required to determine if a group should be deleted from the view.
      // Can not rewrite, bail out;
      return null;
    }

    return new IncrementalComputePlanWithDeletedRows(relBuilder
            .push(aggInput)
            .aggregate(relBuilder.groupKey(aggregate.getGroupSet()), newAggregateCalls)
            .build(),
            countIdx);
  }

  private RexNode genArgumentForCountColumn(RelBuilder relBuilder, RelNode aggInput, int argumentIdx) {
    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RexNode countArg = rexBuilder.makeInputRef(
            aggInput.getRowType().getFieldList().get(argumentIdx).getType(), argumentIdx);
    RexNode isNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, countArg);
    return rexBuilder.makeCall(
            SqlStdOperatorTable.CASE, isNull, relBuilder.literal(0), relBuilder.literal(1));
  }

  /**
   * Extend {@link HiveAggregateIncrementalRewritingRuleBase.IncrementalComputePlan} with countStarIndex. It is
   * required for building the filter condition that distribute the rows to the insert/update/delete branches.
   */
  static class IncrementalComputePlanWithDeletedRows
          extends HiveAggregateIncrementalRewritingRuleBase.IncrementalComputePlan {
    private final int countStarIndex;

    public IncrementalComputePlanWithDeletedRows(RelNode rightInput, int countStarIndex) {
      super(rightInput);
      this.countStarIndex = countStarIndex;
    }
  }

  @Override
  protected RexNode createFilterCondition(IncrementalComputePlanWithDeletedRows incrementalComputePlan,
                                          RexNode flagNode, List<RexNode> projExprs, RelBuilder relBuilder) {
    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RexNode countStarCase = projExprs.get(incrementalComputePlan.countStarIndex);
    RexNode countStarGT0 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, countStarCase, relBuilder.literal(0));
    RexNode countStarEq0 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, countStarCase, relBuilder.literal(0));
    RexNode insert = rexBuilder.makeCall(SqlStdOperatorTable.AND,
            rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, flagNode), countStarGT0);
    RexNode update = rexBuilder.makeCall(SqlStdOperatorTable.AND, flagNode, countStarGT0);
    RexNode delete = rexBuilder.makeCall(SqlStdOperatorTable.AND, flagNode, countStarEq0);
    return rexBuilder.makeCall(SqlStdOperatorTable.OR, insert, update, delete);
  }
}
