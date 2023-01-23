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
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.common.type.SnapshotContext;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This rule will rewrite the materialized view with information about
 * its invalidation data. In particular, if any of the tables used by the
 * materialization has been updated since the materialization was created,
 * it will introduce a filter operator on top of that table in the materialization
 * definition, making explicit the data contained in it so the rewriting
 * algorithm can use this information to rewrite the query as a combination of the
 * outdated materialization data and the new original data in the source tables.
 * If the data in the source table matches the current data in the snapshot,
 * no filter is created.
 */
public class HiveAugmentIcebergMaterializationRule extends RelOptRule {

  private final RexBuilder rexBuilder;
  private final Set<RelNode> visited;
  private final Map<String, SnapshotContext> storedSnapshot;

  public HiveAugmentIcebergMaterializationRule(
          RexBuilder rexBuilder, Map<String, SnapshotContext> storedSnapshot) {
    super(operand(TableScan.class, any()),
        HiveRelFactories.HIVE_BUILDER, "HiveAugmentMaterializationRule");
    this.rexBuilder = rexBuilder;
    this.storedSnapshot = storedSnapshot;
    this.visited = new HashSet<>();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final TableScan tableScan = call.rel(0);
    if (!visited.add(tableScan)) {
      // Already visited
      return;
    }

    RelOptHiveTable hiveTable = (RelOptHiveTable) tableScan.getTable();
    Table table = hiveTable.getHiveTableMD();

    SnapshotContext tableSnapshot = storedSnapshot.get(table.getFullyQualifiedName());
    if (tableSnapshot.equals(table.getStorageHandler().getCurrentSnapshotContext(table))) {
      return;
    }

    table.setVersionIntervalFrom(tableSnapshot);

    int rowIDPos = tableScan.getTable().getRowType().getField(
        VirtualColumn.ROWID.getName(), false, false).getIndex();
    RexNode rowIDFieldAccess = rexBuilder.makeFieldAccess(
        rexBuilder.makeInputRef(tableScan.getTable().getRowType().getFieldList().get(rowIDPos).getType(), rowIDPos),
        0);

    final RelBuilder relBuilder = call.builder();
    relBuilder.push(tableScan);
    List<RexNode> conds = new ArrayList<>();
    RelDataType varcharType = relBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    final RexNode literalHighWatermark = rexBuilder.makeLiteral(
        tableSnapshot.toString(), varcharType, false);
    conds.add(
        rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            ImmutableList.of(rowIDFieldAccess, literalHighWatermark)));
    relBuilder.filter(conds);
    call.transformTo(relBuilder.build());
  }
}