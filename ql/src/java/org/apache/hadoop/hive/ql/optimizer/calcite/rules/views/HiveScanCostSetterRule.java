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

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.SourceTable;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.MaterializedViewMetadata;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import java.util.HashMap;
import java.util.Map;

/**
 * Rule for setting the number of rows read by TableScan operators.
 *
 * TableScan operators provide the number of rows read by the operator based in the number of
 * rows in the table. This rule is applied on incremental materialized view plans to overwrite this to the
 * number of rows inserted since the last rebuild of the view only.
 */
public class HiveScanCostSetterRule extends RelOptRule {

  public static HiveScanCostSetterRule with(RelOptMaterialization materialization) {
    MaterializedViewMetadata mvMetadata = ((RelOptHiveTable) materialization.tableRel.getTable())
            .getHiveTableMD().getMVMetadata();
    Map<String, SourceTable> sourceTableMap = new HashMap<>(mvMetadata.getSourceTables().size());
    for (SourceTable sourceTable : mvMetadata.getSourceTables()) {
      Table table = sourceTable.getTable();
      sourceTableMap.put(
              TableName.getQualified(table.getCatName(), table.getDbName(), table.getTableName()), sourceTable);
    }

    return new HiveScanCostSetterRule(sourceTableMap);
  }

  private final Map<String, SourceTable> sourceTableMap;

  public HiveScanCostSetterRule(Map<String, SourceTable> sourceTableMap) {
    super(operand(TableScan.class, none()),
            HiveRelFactories.HIVE_BUILDER, "HiveScanCostSetterRule");
    this.sourceTableMap = sourceTableMap;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    HiveTableScan tableScan = call.rel(0);
    RelOptHiveTable relOptHiveTable = (RelOptHiveTable) tableScan.getTable();
    org.apache.hadoop.hive.ql.metadata.Table table = relOptHiveTable.getHiveTableMD();
    String fullyQualifiedName = TableName.getQualified(table.getCatName(), table.getDbName(), table.getTableName());
    SourceTable sourceTable = sourceTableMap.get(fullyQualifiedName);
    if (sourceTable == null) {
      return;
    }

    HiveTableScan newTableScan = new HiveTableScan(
            tableScan.getCluster(),
            tableScan.getTraitSet(),
            relOptHiveTable.setRowCount(sourceTable.getInsertedCount()),
            tableScan.getTableAlias(),
            tableScan.getConcatQbIDAlias(),
            false,
            false,
            tableScan.getTableScanTrait());

    call.transformTo(
            call.builder()
                    .push(newTableScan)
                    .build());
  }
}
