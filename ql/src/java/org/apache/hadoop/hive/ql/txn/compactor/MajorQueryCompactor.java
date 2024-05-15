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
package org.apache.hadoop.hive.ql.txn.compactor;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.AcidUtils;

import java.io.IOException;
import java.util.List;

/**
 * Class responsible of running query based major compaction.
 */
final class MajorQueryCompactor extends QueryCompactor {

  @Override
  public boolean run(CompactorContext context) throws IOException {
    HiveConf hiveConf = context.getConf();
    Table table = context.getTable();
    AcidUtils
        .setAcidOperationalProperties(hiveConf, true, AcidUtils.getAcidOperationalProperties(table.getParameters()));
    StorageDescriptor storageDescriptor = context.getSd();
    ValidWriteIdList writeIds = context.getValidWriteIdList();

    HiveConf conf = new HiveConf(hiveConf);
    /*
     * For now, we will group splits on tez so that we end up with all bucket files,
     * with same bucket number in one map task.
     */
    conf.set(HiveConf.ConfVars.SPLIT_GROUPING_MODE.varname, CompactorUtil.COMPACTOR);

    String tmpTableName = getTempTableName(table);
    Path tmpTablePath = QueryCompactor.Util.getCompactionResultDir(storageDescriptor, writeIds,
        conf, true, false, false, null);

    List<String> createQueries = getCreateQueries(tmpTableName, table, tmpTablePath.toString());
    List<String> compactionQueries = getCompactionQueries(table, context.getPartition(), tmpTableName);
    List<String> dropQueries = getDropQueries(tmpTableName);
    runCompactionQueries(conf, tmpTableName, storageDescriptor, writeIds, context.getCompactionInfo(),
        Lists.newArrayList(tmpTablePath), createQueries, compactionQueries, dropQueries,
            table.getParameters());
    return true;
  }

  /**
   * Note on ordering of rows in the temp table:
   * We need each final bucket file sorted by original write id (ascending), bucket (ascending) and row id (ascending).
   * (current write id will be the same as original write id).
   * We will be achieving the ordering via a custom split grouper for compactor.
   * See {@link org.apache.hadoop.hive.conf.HiveConf.ConfVars#SPLIT_GROUPING_MODE} for the config description.
   */
  private List<String> getCreateQueries(String fullName, Table t, String tmpTableLocation) {
    return Lists.newArrayList(new CompactionQueryBuilderFactory().getCompactionQueryBuilder(
        CompactionType.MAJOR, false)
        .setOperation(CompactionQueryBuilder.Operation.CREATE)
        .setResultTableName(fullName)
        .setSourceTab(t)
        .setLocation(tmpTableLocation)
        .build());
  }

  private List<String> getCompactionQueries(Table t, Partition p, String tmpName) {

    return Lists.newArrayList(
        new CompactionQueryBuilderFactory().getCompactionQueryBuilder(
            CompactionType.MAJOR, false)
            .setOperation(CompactionQueryBuilder.Operation.INSERT)
            .setResultTableName(tmpName)
            .setSourceTab(t)
            .setSourcePartition(p)
        .build());
  }

  private List<String> getDropQueries(String tmpTableName) {
    return Lists.newArrayList(
        new CompactionQueryBuilderFactory().getCompactionQueryBuilder(
            CompactionType.MAJOR, false)
            .setOperation(CompactionQueryBuilder.Operation.DROP)
            .setResultTableName(tmpTableName)
            .build());
  }
}
