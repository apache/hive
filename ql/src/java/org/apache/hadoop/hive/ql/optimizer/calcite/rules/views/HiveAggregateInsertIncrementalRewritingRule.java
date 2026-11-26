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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.ddl.view.materialized.alter.rebuild.AlterMaterializedViewRebuildAnalyzer;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

import java.util.List;

/**
 * This rule will perform a rewriting to prepare the plan for incremental
 * view maintenance in case there exist aggregation operator, so we can
 * avoid the INSERT OVERWRITE and use a MERGE statement instead.
 * <br>
 * <pre>
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
 * </pre>
 * We need to transform that into:
 * <pre>
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
 * WHEN NOT MATCHED
 *   THEN INSERT VALUES (source.a, source.b, s, c);
 * </pre>
 * To be precise, we need to convert it into a MERGE rewritten as:
 * <pre>
 * FROM (select *, true flag from mv) mv right outer join _source_ source
 * ON (mv.a &lt;=&gt; source.a AND mv.b &lt;=&gt; source.b)
 * INSERT INTO TABLE mv                     &lt;- (insert the new version of the updated rows)
 *   SELECT source.a, source.b,
 *     CASE WHEN mv.s IS NULL AND source.s IS NULL THEN NULL
 *          WHEN mv.s IS NULL THEN source.s
 *          WHEN source.s IS NULL THEN mv.s
 *          ELSE mv.s + source.s END,
 *          &lt;same CASE statement for mv.c + source.c like above&gt;
 *   WHERE mv.flag
 * INSERT INTO TABLE mv                     &lt;- (delete the old version of the updated rows)
 *   SELECT mv.ROW__ID
 *   WHERE mv.flag
 *   SORT BY mv.ROW__ID
 * INSERT INTO TABLE mv                     &lt;- (insert new rows into the view)
 *   SELECT source.a, source.b, s, c
 *   WHERE mv.flag IS NULL;
 * </pre>
 * The update is split to an insert and a delete branch.
 * <ul>
 *   <li>In case of native ACID views the delete branch selects ROW__ID and is sorted by it. It has no distribute
 *   keys: the deleted rows are distributed by the bucket id of ROW__ID, like the deleted rows of any ACID
 *   DELETE/UPDATE/MERGE.</li>
 *   <li>In case of non-native views (Iceberg) the delete branch selects the delete columns of the storage handler
 *   instead of ROW__ID, it is distributed by the storage handler's distribute columns (FILE__PATH in case of V3) and
 *   sorted by its sort columns (none in case of V3).</li>
 * </ul>
 *
 * @see org.apache.hadoop.hive.ql.parse.CalcitePlanner
 * @see AlterMaterializedViewRebuildAnalyzer
 */
public class HiveAggregateInsertIncrementalRewritingRule extends HiveAggregateIncrementalRewritingRuleBase<
        HiveAggregateIncrementalRewritingRuleBase.IncrementalComputePlan> {

  public static final HiveAggregateInsertIncrementalRewritingRule INSTANCE =
      new HiveAggregateInsertIncrementalRewritingRule();

  private HiveAggregateInsertIncrementalRewritingRule() {
    super(operand(Aggregate.class, operand(Union.class, any())),
        HiveRelFactories.HIVE_BUILDER,
        "HiveAggregateIncrementalRewritingRule", 0);
  }

  @Override
  protected IncrementalComputePlan createJoinRightInput(RelOptRuleCall call) {
    RelNode union = call.rel(1);
    return new IncrementalComputePlan(union.getInput(0));
  }

  @Override
  protected RexNode createFilterCondition(IncrementalComputePlan incrementalComputePlan,
                                          RexNode flagNode, List<RexNode> projExprs, RelBuilder relBuilder) {
    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    return rexBuilder.makeCall(
            SqlStdOperatorTable.OR, flagNode, rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, flagNode));
  }

  @Override
  protected RexNode createAggregateNode(
          SqlAggFunction aggCall, RexNode leftRef, RexNode rightRef, RexBuilder rexBuilder) {

    switch (aggCall.getKind()) {
      case MIN: {
        RexNode condInnerCase = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
                ImmutableList.of(rightRef, leftRef));
        return rexBuilder.makeCall(SqlStdOperatorTable.CASE,
                ImmutableList.of(condInnerCase, rightRef, leftRef));
      }
      case MAX: {
        RexNode condInnerCase = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
                ImmutableList.of(rightRef, leftRef));
        return rexBuilder.makeCall(SqlStdOperatorTable.CASE,
                ImmutableList.of(condInnerCase, rightRef, leftRef));
      }
      default:
        return super.createAggregateNode(aggCall, leftRef, rightRef, rexBuilder);
    }
  }
}
