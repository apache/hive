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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;

import java.util.Collections;

public class HivePartitionPruneRule extends RelOptRule {

  HiveConf conf;

  public HivePartitionPruneRule(HiveConf conf) {
    super(operand(RelNode.class, operand(HiveTableScan.class, none())));
    this.conf = conf;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelNode relNode= call.rel(0);
    HiveTableScan tScan = call.rel(1);
    perform(call, relNode, tScan);
  }

  protected void perform(RelOptRuleCall call, RelNode relNode,
      HiveTableScan tScan) {
    try {
      FunctionHelper functionHelper = call.getPlanner().getContext().unwrap(FunctionHelper.class);
      PartitionPruneRuleHelper ruleHelper = functionHelper.getPartitionPruneRuleHelper();

      HiveFilter filter = (relNode instanceof HiveFilter) ? (HiveFilter) relNode : null;
      // For Hive, there is no need to compute partition list if no filter is used.
      if (filter == null && !ruleHelper.shouldComputeWithoutFilter()) {
        return;
      }

      // Original table
      RelOptHiveTable hiveTable = (RelOptHiveTable) tScan.getTable();

      // Copy original table scan and table
      HiveTableScan tScanCopy = tScan.copyIncludingTable(tScan.getRowType());
      RelOptHiveTable hiveTableCopy = (RelOptHiveTable) tScanCopy.getTable();

      RulePartitionPruner pruner = ruleHelper.createRulePartitionPruner(tScanCopy, hiveTableCopy,
          filter);

      hiveTableCopy.computePartitionList(conf, pruner);

      if (StringUtils.equals(hiveTableCopy.getPartitionListKey(), hiveTable.getPartitionListKey())) {
        // Nothing changed, we do not need to produce a new expression
        return;
      }

      call.transformTo(relNode.copy(
          relNode.getTraitSet(), Collections.singletonList(tScanCopy)));
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }
}
