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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveHepExtractRelNodeRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.findRexTableInputRefs;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveAggregateInsertDeleteIncrementalRewritingRule.createJoinRightInput;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveAggregatePartitionIncrementalRewritingRule.findPartitionColumnIndexes;

/**
 * Rule to prepare the plan for incremental view maintenance if the view is partitioned and insert only:
 * Insert overwrite the partitions which are affected since the last rebuild only and leave the
 * rest of the partitions intact.
 *
 * Assume that we have a materialized view partitioned on column a and writeId was 1 at the last rebuild:
 *
 * CREATE MATERIALIZED VIEW mat1 PARTITIONED ON (a) STORED AS ORC TBLPROPERTIES ("transactional"="true", "transactional_properties"="insert_only") AS
 * SELECT a, b, sum(c) sumc, count(*) countstar FROM t1 GROUP BY b, a;
 *
 * 1. Query all rows from source tables since the last rebuild.
 * 2. Query all rows from MV which are in any of the partitions queried in 1.
 * 3. Take the union of rows from 1. and 2. and perform the same aggregations defined in the MV
 *
 * SELECT a, b, sum(sumc), sum(countstar) counttotal FROM (
 *   SELECT a, b, sumc, countstar FROM mat1
 *   LEFT SEMI JOIN (SELECT a FROM t1('acid.fetch.deleted.rows'='true') WHERE ROW__ID.writeId > 1 GROUP BY b, a) q ON (mat1.a <=> q.a)
 *   UNION ALL
 *   SELECT a, b,
 *     sum(CASE WHEN t1.ROW__IS__DELETED THEN -c ELSE c END) sumc,
 *     sum(CASE WHEN t1.ROW__IS__DELETED THEN -1 ELSE 1 END) countstar
 *   FROM t1('acid.fetch.deleted.rows'='true')
 *   WHERE ROW__ID.writeId > 1
 *   GROUP BY b, a
 * ) sub
 * GROUP BY a, b
 * HAVING counttotal > 0
 * ORDER BY a, b;
 *
 */
public class HiveAggregatePartitionInsertDeleteIncrementalRewritingRule extends RelOptRule {
  private static final Logger LOG = LoggerFactory.getLogger(HiveAggregatePartitionInsertDeleteIncrementalRewritingRule.class);

  public static final HiveAggregatePartitionInsertDeleteIncrementalRewritingRule INSTANCE =
          new HiveAggregatePartitionInsertDeleteIncrementalRewritingRule();

  private HiveAggregatePartitionInsertDeleteIncrementalRewritingRule() {
    super(operand(Aggregate.class, operand(Union.class, operand(Aggregate.class, any()))),
            HiveRelFactories.HIVE_BUILDER, "HiveAggregatePartitionInsertDeleteIncrementalRewritingRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RexBuilder rexBuilder = call.builder().getRexBuilder();

    final Aggregate aggregate = call.rel(0);
    final Union union = call.rel(1);
    final Aggregate queryAgg = call.rel(2);
    final RelNode mvBranch = union.getInput(1);
    RelBuilder relBuilder = call.builder();

    Set<Integer> partitionColumnIndexes = findPartitionColumnIndexes(mvBranch, rexBuilder);
    if (partitionColumnIndexes == null) {
      return;
    }

    HiveAggregateInsertDeleteIncrementalRewritingRule.IncrementalComputePlanWithDeletedRows plan =
            createJoinRightInput(queryAgg, call.builder());
    if (plan == null) {
      return;
    }

    RelNode queryBranch = plan.rightInput;
    int countStarIdx = plan.getCountStarIndex();

    List<RexNode> joinConjs = new ArrayList<>();
    for (int partColIndex: partitionColumnIndexes) {
      RexNode leftRef = rexBuilder.makeInputRef(
              mvBranch.getRowType().getFieldList().get(partColIndex).getType(), partColIndex);
      RexNode rightRef = rexBuilder.makeInputRef(
              queryBranch.getRowType().getFieldList().get(partColIndex).getType(),
              partColIndex + mvBranch.getRowType().getFieldCount());

      joinConjs.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, leftRef, rightRef));
    }

    RexNode joinCond = RexUtil.composeConjunction(rexBuilder, joinConjs);

    RexNode countStar = rexBuilder.makeInputRef(
            aggregate.getRowType().getFieldList().get(countStarIdx).getType(), countStarIdx);
    RexNode countStarGT0 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, countStar, relBuilder.literal(0));

    RelBuilder builder = call.builder();
    RelNode newNode = builder
            .push(mvBranch)
            .push(queryBranch)
            .join(JoinRelType.SEMI, joinCond)
            .push(queryBranch)
            .union(union.all)
            .aggregate(builder.groupKey(aggregate.getGroupSet()), aggregate.getAggCallList())
            .filter(countStarGT0)
            .build();
    call.transformTo(newNode);
  }
}
