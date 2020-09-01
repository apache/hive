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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive specific helper class to handle prune partitioning.
 * Both "compute" public methods return a "PrunedPartitionList" which contains
 * all partitions to be kept after pruning.
 */
public class HivePartitionPruneRuleHelper implements PartitionPruneRuleHelper {

  protected static final Logger LOG = LoggerFactory.getLogger(HivePartitionPruneRuleHelper.class.getName());

  /**
   * For Hive, when there is no filter above the TableScan, we return false
   * because we do not want to compute without the filter.
   */
  @Override
  public boolean shouldComputeWithoutFilter() {
    return false;
  }

  @Override
  public RulePartitionPruner createRulePartitionPruner(HiveTableScan scan,
      RelOptHiveTable table, HiveFilter filter) throws HiveException {
    return new HiveRulePartitionPruner(scan, table, filter);
  }
}
