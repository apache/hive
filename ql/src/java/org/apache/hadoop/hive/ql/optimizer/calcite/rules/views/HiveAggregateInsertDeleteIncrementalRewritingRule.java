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
public class HiveAggregateInsertDeleteIncrementalRewritingRule
        extends HiveAggregateIncrementalRewritingRuleBase<HiveAggregateInsertDeleteIncrementalRewritingRule.RightInputWithDeletedRows> {

  public static final HiveAggregateInsertDeleteIncrementalRewritingRule INSTANCE =
      new HiveAggregateInsertDeleteIncrementalRewritingRule();

  private HiveAggregateInsertDeleteIncrementalRewritingRule() {
    super(operand(Aggregate.class, operand(Union.class, operand(Aggregate.class, any()))),
        HiveRelFactories.HIVE_BUILDER,
        "HiveAggregateInsertDeleteIncrementalRewritingRule", 2);
  }

  @Override
  protected RightInputWithDeletedRows createJoinRightInput(Aggregate aggregate, RelBuilder relBuilder) {
    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RelNode aggInput = aggregate.getInput();
    aggInput = HiveHepExtractRelNodeRule.execute(aggInput);

    aggInput = new HiveRowIsDeletedPropagator(relBuilder).propagate(aggInput);

    int rowIsDeletedIdx = aggInput.getRowType().getFieldCount() - 1;
    RexNode rowIsDeletedNode = rexBuilder.makeInputRef(
            aggInput.getRowType().getFieldList().get(rowIsDeletedIdx).getType(), rowIsDeletedIdx);

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
      // count(*) not found. It is required to determine if a group should be deleted from the view.
      // Can not rewrite, bail out;
      return null;
    }

    return new RightInputWithDeletedRows(relBuilder
            .push(aggInput)
            .aggregate(relBuilder.groupKey(aggregate.getGroupSet()), newAggregateCalls)
            .build(),
            countIdx);
  }

  @Override
  protected RexNode createFilterCondition(
          RightInputWithDeletedRows rightInput, RexNode flagNode, List<RexNode> projExprs, RelBuilder relBuilder) {
    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RexNode countStarCase = projExprs.get(rightInput.countStarIndex);
    RexNode countStarGT0 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, countStarCase, relBuilder.literal(0));
    RexNode countStarEq0 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, countStarCase, relBuilder.literal(0));
    RexNode insert = rexBuilder.makeCall(SqlStdOperatorTable.AND,
            rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, flagNode), countStarGT0);
    RexNode update = rexBuilder.makeCall(SqlStdOperatorTable.AND, flagNode, countStarGT0);
    RexNode delete = rexBuilder.makeCall(SqlStdOperatorTable.AND, flagNode, countStarEq0);
    return rexBuilder.makeCall(SqlStdOperatorTable.OR, insert, update, delete);
  }

  protected static class RightInputWithDeletedRows extends RightInput {
    private final int countStarIndex;

    public RightInputWithDeletedRows(RelNode rightInput, int countStarIndex) {
      super(rightInput);
      this.countStarIndex = countStarIndex;
    }
  }
}
