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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;

import java.util.Collections;
import java.util.List;

public abstract class HivePartitionPruneRule extends RelOptRule {
  protected final HiveConf conf;
  protected HivePartitionPruneRule(HiveConf conf, RelOptRuleOperand operand, String description) {
    super(operand, description);
    this.conf = conf;
  }

  private int getPartitionListSize(RelOptHiveTable hiveTable) {
    if (hiveTable.getPrunedPartitionList() == null) {
      return Integer.MAX_VALUE;
    }
    return hiveTable.getPrunedPartitionList().getPartitions().size();
  }

  protected void perform(RelOptRuleCall call, HiveFilter filter, HiveTableScan tScan) {
    RelOptHiveTable hiveTable = (RelOptHiveTable) tScan.getTable();
    if (hiveTable.getName().equals(
        SemanticAnalyzer.DUMMY_DATABASE + "." + SemanticAnalyzer.DUMMY_TABLE)) {
      return;
    }

    FunctionHelper functionHelper = call.getPlanner().getContext().unwrap(FunctionHelper.class);
    PartitionPruneRuleHelper ruleHelper =
      functionHelper.getPartitionPruneRuleHelper();
    HiveTableScan tScanCopy = tScan.copyIncludingTable(tScan.getRowType(), ruleHelper);
    RelOptHiveTable hiveTableCopy = (RelOptHiveTable) tScanCopy.getTable();
    if (!hiveTableCopy.computeCacheWithFilter(filter)) {
      try {
        RulePartitionPruner pruner = ruleHelper.createRulePartitionPruner(tScanCopy, hiveTableCopy,
            filter);
        hiveTableCopy.computePartitionList(pruner);
      } catch (HiveException e) {
        throw new RuntimeException(e);
      }
    }

    if (StringUtils.equals(hiveTableCopy.getPartitionListKey(), hiveTable.getPartitionListKey())) {
      // Nothing changed, we do not need to produce a new expression
      return;
    }

    int baseParts = getPartitionListSize(hiveTable);
    int copyParts = getPartitionListSize(hiveTableCopy);
    if (baseParts <= copyParts) {
      // original table has better filtering on partitions
      return;
    }

    if (filter == null) {
      call.transformTo(tScanCopy);
    } else {
      call.transformTo(filter.copy(filter.getTraitSet(), Collections.singletonList(tScanCopy)));
    }
  }

  private static class TableScan extends HivePartitionPruneRule {
    private TableScan(HiveConf conf) {
      super(conf, operand(HiveTableScan.class, any()),
          "HivePartitionPruneRule(TableScan)");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveTableScan tScan = call.rel(0);
      if (((RelOptHiveTable) tScan.getTable()).getPrunedPartitionList() != null) {
        // if there is a partition list at this point either this rule has already ran or
        // the filter rule has ran.
        return;
      }
      perform(call, null, tScan);
    }
  }

  private static class FilterTableScan extends HivePartitionPruneRule {
    private FilterTableScan(HiveConf conf) {
      super(conf, operand(HiveFilter.class, operand(HiveTableScan.class, any())),
          "HivePartitionPruneRule(Filter)");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveFilter filter = call.rel(0);
      final HiveTableScan tScan = call.rel(1);
      perform(call, filter, tScan);
    }
  }

  public static HivePartitionPruneRule[] createRules(HiveConf hConf) {
    // Creates a list of rules in the order they are designed to be applied in
    return new HivePartitionPruneRule[] { new FilterTableScan(hConf), new TableScan(hConf) };
  }

  public static void addRules(RelOptPlanner planner, HiveConf conf) {
    for (HivePartitionPruneRule rule : createRules(conf)) {
      planner.addRule(rule);
    }
  }
}
