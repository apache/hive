/**
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
package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveTableScanRel;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.Pair;

public class HivePartitionPrunerRule extends RelOptRule {

  HiveConf conf;

  public HivePartitionPrunerRule(HiveConf conf) {
    super(operand(HiveFilterRel.class, operand(HiveTableScanRel.class, none())));
    this.conf = conf;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    HiveFilterRel filter = call.rel(0);
    HiveTableScanRel tScan = call.rel(1);
    perform(call, filter, tScan);
  }

  protected void perform(RelOptRuleCall call, FilterRelBase filter,
      HiveTableScanRel tScan) {

    RelOptHiveTable hiveTable = (RelOptHiveTable) tScan.getTable();
    RexNode predicate = filter.getCondition();

    Pair<RexNode, RexNode> predicates = PartitionPruner
        .extractPartitionPredicates(filter.getCluster(), hiveTable, predicate);
    RexNode partColExpr = predicates.left;
    hiveTable.computePartitionList(conf, partColExpr);
  }
}
