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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import java.util.Collections;

public class HivePartitionPruneRule extends RelOptRule {

  HiveConf conf;

  public HivePartitionPruneRule(HiveConf conf) {
    super(operand(RelNode.class, operand(HiveTableScan.class, none())));
    this.conf = conf;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    HiveTableScan tScan = call.rel(1);
    HiveFilter filter = null;
    if (call.rel(0) instanceof HiveFilter) {
      performWithFilter(call, (HiveFilter) call.rel(0), tScan);
    } else {
      performWithoutFilter(call, tScan);
    }
  }

  private void performWithFilter(RelOptRuleCall call, Filter filter,
      HiveTableScan tScan) {
    // Original table
    RelOptHiveTable hiveTable = (RelOptHiveTable) tScan.getTable();
    if (filter == null && !hiveTable.isPartitionListFetched()) {
    }

    // Copy original table scan and table
    HiveTableScan tScanCopy = tScan.copyIncludingTable(tScan.getRowType());
    RelOptHiveTable hiveTableCopy = (RelOptHiveTable) tScanCopy.getTable();

    // Execute partition pruning
    RexNode predicate = filter.getCondition();
    Pair<RexNode, RexNode> predicates = PartitionPrune
        .extractPartitionPredicates(filter.getCluster(), hiveTableCopy, predicate);
    RexNode partColExpr = predicates.left;
    hiveTableCopy.computePartitionList(conf, partColExpr, tScanCopy.getPartOrVirtualCols());

    if (StringUtils.equals(hiveTableCopy.getPartitionListKey(), hiveTable.getPartitionListKey())) {
      // Nothing changed, we do not need to produce a new expression
      return;
    }

    call.transformTo(filter.copy(
        filter.getTraitSet(), Collections.singletonList(tScanCopy)));
  }

  private void performWithoutFilter(RelOptRuleCall call, HiveTableScan tScan) {
    // Original table
    RelOptHiveTable hiveTable = (RelOptHiveTable) tScan.getTable();
    if (hiveTable.isPartitionListFetched()) {
      return;
    }

    // Copy original table scan and table
    HiveTableScan tScanCopy = tScan.copyIncludingTable(tScan.getRowType());
    RelOptHiveTable hiveTableCopy = (RelOptHiveTable) tScanCopy.getTable();

    hiveTableCopy.computePartitionList(conf, null, tScanCopy.getPartOrVirtualCols());

    RelNode relNode = call.rel(0);
    call.transformTo(relNode.copy(
        relNode.getTraitSet(), Collections.singletonList(tScanCopy)));
  }
}
