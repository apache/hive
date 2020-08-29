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

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ExprNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import java.util.Map;
/**
 * HiveRulePartitionPruner
 *
 * Used to retrieve specific pruned PartitionList for the Hive engine.
 */
public class HiveRulePartitionPruner implements RulePartitionPruner {

  private final HiveTableScan scan;

  private final RelOptHiveTable table;

  private final RexNode pruneNode;

  public HiveRulePartitionPruner(RelOptHiveTable table) {
    this.table = table;
    this.scan = null;
    this.pruneNode = null;
  }

  public HiveRulePartitionPruner(HiveTableScan scan, RelOptHiveTable table,
      HiveFilter filter) {
    this.scan = scan;
    this.table = table;
    Pair<RexNode, RexNode> pairPredicates = (filter == null)
        ? null
        : PartitionPrune.extractPartitionPredicates(filter.getCluster(), table,
            filter.getCondition());
    this.pruneNode = pairPredicates == null ? null : pairPredicates.left;
  }

  public PrunedPartitionList prune(HiveConf conf, Map<String, PrunedPartitionList> partitionCache)
      throws HiveException {
    if (pruneNode == null || InputFinder.bits(pruneNode).length() == 0) {
      return getNonPruneList(conf, partitionCache);
    }

    ExprNodeDesc pruneExpr = pruneNode.accept(new ExprNodeConverter(table.getName(),
        table.getRowType(),  scan.getPartOrVirtualCols(), table.getTypeFactory()));

    return PartitionPruner.prune(table.getHiveTableMD(), pruneExpr, conf, table.getName(),
        partitionCache);
  }

  public PrunedPartitionList getNonPruneList(HiveConf conf,
      Map<String, PrunedPartitionList> partitionCache) throws HiveException {
    return PartitionPruner.prune(table.getHiveTableMD(), null, conf, table.getName(),
        partitionCache);
  }
}
