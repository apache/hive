package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;
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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.findRexTableInputRefs;

/**
 * Rule to prepare the plan for incremental view maintenance if the view is partitioned and insert only:
 * Insert overwrite the partitions which are affected since the last rebuild only and leave the
 * rest of the partitions intact.
 *
 * Assume that we have a materialized view partitioned on column a and writeId was 1 at the last rebuild:
 *
 * CREATE MATERIALIZED VIEW mat1 PARTITIONED ON (a) STORED AS ORC TBLPROPERTIES ("transactional"="true", "transactional_properties"="insert_only") AS
 * SELECT a, b, sum(c) sumc FROM t1 GROUP BY b, a;
 *
 * 1. Query all rows from source tables since the last rebuild.
 * 2. Query all rows from MV which are in any of the partitions queried in 1.
 * 3. Take the union of rows from 1. and 2. and perform the same aggregations defined in the MV
 *
 * SELECT a, b, sum(sumc) FROM (
 *     SELECT a, b, sumc FROM mat1
 *     LEFT SEMI JOIN (SELECT a, b, sum(c) FROM t1 WHERE ROW__ID.writeId &gt; 1 GROUP BY b, a) q ON (mat1.a &lt;=&gt; q.a)
 *     UNION ALL
 *     SELECT a, b, sum(c) sumc FROM t1 WHERE ROW__ID.writeId &gt; 1 GROUP BY b, a
 * ) sub
 * GROUP BY b, a
 */
public class HiveAggregatePartitionIncrementalRewritingRule extends RelOptRule {
  private static final Logger LOG = LoggerFactory.getLogger(HiveAggregatePartitionIncrementalRewritingRule.class);

  public static final HiveAggregatePartitionIncrementalRewritingRule INSTANCE =
          new HiveAggregatePartitionIncrementalRewritingRule();

  private HiveAggregatePartitionIncrementalRewritingRule() {
    super(operand(Aggregate.class, operand(Union.class, any())),
            HiveRelFactories.HIVE_BUILDER, "HiveAggregatePartitionIncrementalRewritingRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RexBuilder rexBuilder = call.builder().getRexBuilder();

    final Aggregate aggregate = call.rel(0);
    final Union union = call.rel(1);
    final RelNode queryBranch = union.getInput(0);
    final RelNode mvBranch = union.getInput(1);

    // find Partition col indexes in mvBranch top operator row schema
    // mvBranch can be more complex than just a TS on the MV and the partition columns indexes in the top Operator's
    // row schema may differ from the one in the TS row schema. Example:
    // Project($2, $0, $1)
    //   TableScan(table=materialized_view1, schema=a, b, part_col)
    RelMetadataQuery relMetadataQuery = RelMetadataQuery.instance();
    int partitionColumnCount = -1;
    List<Integer> partitionColumnIndexes = new ArrayList<>();
    for (int i = 0; i < mvBranch.getRowType().getFieldList().size(); ++i) {
      RelDataTypeField relDataTypeField = mvBranch.getRowType().getFieldList().get(i);
      RexInputRef inputRef = rexBuilder.makeInputRef(relDataTypeField.getType(), i);

      Set<RexNode> expressionLineage = relMetadataQuery.getExpressionLineage(mvBranch, inputRef);
      if (expressionLineage == null || expressionLineage.size() != 1) {
        continue;
      }

      Set<RexTableInputRef> tableInputRefs = findRexTableInputRefs(expressionLineage.iterator().next());
      if (tableInputRefs.size() != 1) {
        continue;
      }

      RexTableInputRef tableInputRef = tableInputRefs.iterator().next();
      RelOptHiveTable relOptHiveTable = (RelOptHiveTable) tableInputRef.getTableRef().getTable();
      if (!(relOptHiveTable.getHiveTableMD().isMaterializedView())) {
        LOG.warn("{} is not a materialized view, bail out.", relOptHiveTable.getQualifiedName());
        return;
      }

      partitionColumnCount = relOptHiveTable.getPartColInfoMap().size();
      if (relOptHiveTable.getPartColInfoMap().containsKey(tableInputRef.getIndex())) {
        partitionColumnIndexes.add(i);
      }
    }

    if (partitionColumnCount <= 0 || partitionColumnIndexes.size() != partitionColumnCount) {
      LOG.debug("Could not find all partition column lineages, bail out.");
      return;
    }

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

    RelBuilder builder = call.builder();
    RelNode newNode = builder
            .push(mvBranch)
            .push(queryBranch)
            .join(JoinRelType.SEMI, joinCond)
            .push(queryBranch)
            .union(union.all)
            .aggregate(builder.groupKey(aggregate.getGroupSet()), aggregate.getAggCallList())
            .build();
    call.transformTo(newNode);
  }
}
