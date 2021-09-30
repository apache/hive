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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

/**
 * This rule turns on populating writeId of insert only table scans.
 * Currently fetching writeId from insert-only tables is not turned on automatically:
 * 1. only not compacted records has valid writeId.
 * 2. the writeId and bucketId is populated into the ROW_ID struct however the third field
 * of the struct (rowId) is always 0.
 *
 * This feature is only used when rebuilding materialized view incrementally when the view has
 * insert-only source tables.
 */
public class HiveInsertOnlyScanWriteIdRule extends RelOptRule {

  public static final HiveInsertOnlyScanWriteIdRule INSTANCE = new HiveInsertOnlyScanWriteIdRule();

  private HiveInsertOnlyScanWriteIdRule() {
    super(operand(HiveTableScan.class, none()));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    HiveTableScan tableScan = call.rel(0);
    Table tableMD = ((RelOptHiveTable) tableScan.getTable()).getHiveTableMD();
    return !tableMD.isMaterializedView() && AcidUtils.isInsertOnlyTable(tableMD);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    HiveTableScan tableScan = call.rel(0);
    RelNode newTableScan = call.builder()
            .push(tableScan.setTableScanTrait(HiveTableScan.HiveTableScanTrait.FetchInsertOnlyBucketIds))
            .build();
    call.transformTo(newTableScan);
  }
}
