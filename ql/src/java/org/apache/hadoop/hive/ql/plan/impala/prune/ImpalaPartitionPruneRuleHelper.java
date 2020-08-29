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
package org.apache.hadoop.hive.ql.plan.impala.prune;

import org.apache.calcite.rex.RexBuilder;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.RulePartitionPruner;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.PartitionPruneRuleHelper;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaQueryContext;
import org.apache.impala.common.ImpalaException;

/**
 * Impala specific helper class to handle prune partitioning.
 * Both "compute" public methods return a "PrunedPartitionList" which contains
 * all partitions to be kept after pruning.
 */
public class ImpalaPartitionPruneRuleHelper implements PartitionPruneRuleHelper {

  private final ImpalaQueryContext queryContext;
  private final RexBuilder rexBuilder;

  public ImpalaPartitionPruneRuleHelper(ImpalaQueryContext queryContext, RexBuilder rexBuilder) {
    this.queryContext = queryContext;
    this.rexBuilder = rexBuilder;
  }

  /**
   * We always need to compute the partition list because the Impala request requires
   * all partitions and file metadata to be present.
   */
  @Override
  public boolean shouldComputeWithoutFilter() {
    return true;
  }

  @Override
  public RulePartitionPruner createRulePartitionPruner(HiveTableScan scan,
      RelOptHiveTable table, HiveFilter filter) throws HiveException {
    // special case when there is no table (e.g. 'select 1')
    if (table.getName().equals(
        SemanticAnalyzer.DUMMY_DATABASE + "." + SemanticAnalyzer.DUMMY_TABLE)) {
      return new ImpalaRulePartitionPruner();
    }
    try {
      return new ImpalaRulePartitionPruner(scan, table, filter, queryContext, rexBuilder);
    } catch (ImpalaException|MetaException e) {
      throw new HiveException(e);
    }
  }
}
