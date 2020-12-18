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
package org.apache.hadoop.hive.impala.prune;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.PrunerUtils;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.RulePartitionPruner;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.impala.plan.ImpalaQueryContext;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaConjuncts;
import org.apache.impala.common.ImpalaException;

import java.util.Map;

/**
 * ImpalaRuleNonPartitionedPruner.
 *
 * Class to create a PrunedPartitionList on a nonpartitioned table. Even though
 * there are no partitions, the creator/caller still expects a PartitionPrunedList
 * for every table CDPD-19490 filed to deal with this confusion, but it needs to be
 * fixed for both Impala and Hive. For this case, it will contain table information only.
 */
public class ImpalaRuleNonPartitionedPruner implements RulePartitionPruner {
  private final RelOptHiveTable table;

  public ImpalaRuleNonPartitionedPruner(RelOptHiveTable table, ImpalaQueryContext queryContext)
      throws HiveException, ImpalaException, MetaException {
    Preconditions.checkNotNull(table);
    this.table = table;
    ImpalaBasicHdfsTable impalaTable =
        queryContext.getBasicTable(table.getHiveTableMD().getTTable());
    if (impalaTable == null) {
      impalaTable = ImpalaBasicTableCreator.createNonPartitionedTable(table, queryContext);
      queryContext.cacheBasicTable(table.getHiveTableMD().getTTable(), impalaTable);
    }
  }

  @Override
  public PrunedPartitionList getPartitionPruneList(HiveConf conf,
      Map<String, PrunedPartitionList> partitionCache) throws HiveException {
    String tableKey = PrunerUtils.getTableKey(table.getHiveTableMD());
    PrunedPartitionList cachedValue = partitionCache.get(tableKey);
    if (cachedValue != null) {
      return cachedValue;
    }
    Table t = new Table(table.getHiveTableMD().getTTable());
    PrunedPartitionList ppl = new ImpalaPrunedPartitionList(t, tableKey,
        Sets.newHashSet(new Partition(table.getHiveTableMD())), false, Lists.newArrayList(),
        null, ImpalaConjuncts.create());
    partitionCache.put(tableKey, ppl);
    return ppl;
  }
}
