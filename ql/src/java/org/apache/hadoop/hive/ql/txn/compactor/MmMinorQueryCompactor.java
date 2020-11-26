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
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.common.util.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Run a minor query compaction on an insert only (MM) table.
 */
final class MmMinorQueryCompactor extends QueryCompactor {

  private static final Logger LOG = LoggerFactory.getLogger(MmMinorQueryCompactor.class.getName());

  @Override void runCompaction(HiveConf hiveConf, Table table, Partition partition,
      StorageDescriptor storageDescriptor, ValidWriteIdList writeIds, CompactionInfo compactionInfo)
      throws IOException {
    LOG.debug(
        "Going to delete directories for aborted transactions for MM table " + table.getDbName()
            + "." + table.getTableName());

    AcidUtils.Directory dir = AcidUtils
        .getAcidState(null, new Path(storageDescriptor.getLocation()), hiveConf, writeIds,
            Ref.from(false), false);
    QueryCompactor.Util.removeFilesForMmTable(hiveConf, dir);

    HiveConf driverConf = setUpDriverSession(hiveConf);

    String tmpPrefix = table.getDbName() + ".tmp_minor_compactor_" + table.getTableName() + "_";
    String tmpTableName = tmpPrefix + System.currentTimeMillis();
    String resultTmpTableName = tmpTableName + "_result";
    Path resultDeltaDir = QueryCompactor.Util.getCompactionResultDir(storageDescriptor, writeIds, driverConf,
        false, false, false);

    List<String> createTableQueries = getCreateQueries(tmpTableName, table, storageDescriptor, dir,
        writeIds, resultDeltaDir);
    List<String> compactionQueries = getCompactionQueries(tmpTableName, resultTmpTableName, table);
    List<String> dropQueries = getDropQueries(tmpTableName);
    runCompactionQueries(driverConf, tmpTableName, storageDescriptor, writeIds, compactionInfo,
        Lists.newArrayList(resultDeltaDir), createTableQueries, compactionQueries, dropQueries);
  }

  /**
   * Move files from "result table" directory to table/partition to compact's directory.
   */
  @Override protected void commitCompaction(String dest, String tmpTableName, HiveConf conf,
      ValidWriteIdList actualWriteIds, long compactorTxnId) throws IOException, HiveException {
    Util.cleanupEmptyDir(conf, tmpTableName);
  }

  /**
   * Get a list of create/alter table queries. These tables serves as temporary data source for
   * query based minor compaction. The following tables are created:
   * <ol>
   *   <li>tmpTable - "source table": temporary, external, partitioned table. Each partition
   *     points to exactly one delta directory in the table/partition to compact</li>
   *   <li>tmpTable_result - "result table" : temporary table which stores the aggregated
   *     results of the minor compaction query until the compaction can be committed</li>
   * </ol>
   *
   * @param tmpTableBase name of the first temp table (second will be $tmpTableBase_result)
   * @param t Table to compact
   * @param sd storage descriptor of table or partition to compact
   * @param dir the parent directory of delta directories
   * @param writeIds ValidWriteIdList for the table/partition we are compacting
   * @param resultDeltaDir the final location for the
   * @return List of 3 query strings: 2 create table, 1 alter table
   */
  private List<String> getCreateQueries(String tmpTableBase, Table t, StorageDescriptor sd,
      AcidUtils.Directory dir, ValidWriteIdList writeIds, Path resultDeltaDir) {
    List<String> queries = Lists.newArrayList(
        getCreateQuery(tmpTableBase, t, sd, null, true),
        getCreateQuery(tmpTableBase + "_result", t, sd, resultDeltaDir.toString(), false)
    );
    String alterQuery = buildAlterTableQuery(tmpTableBase, dir, writeIds);
    if (!alterQuery.isEmpty()) {
      queries.add(alterQuery);
    }
    return queries;
  }

  private String getCreateQuery(String newTableName, Table t, StorageDescriptor sd,
      String location, boolean isPartitioned) {
    return new CompactionQueryBuilder(
        CompactionQueryBuilder.CompactionType.MINOR_INSERT_ONLY,
        CompactionQueryBuilder.Operation.CREATE,
        newTableName)
        .setSourceTab(t)
        .setStorageDescriptor(sd)
        .setLocation(location)
        .setPartitioned(isPartitioned)
        .build();
  }

  /**
   * Builds an alter table query, which adds partitions pointing to location of delta directories.
   *
   * @param tableName name of the temp table to be altered
   * @param dir the parent directory of delta directories
   * @param validWriteIdList valid write ids for the table/partition to compact
   * @return alter table statement.
   */
  private String buildAlterTableQuery(String tableName, AcidUtils.Directory dir,
      ValidWriteIdList validWriteIdList) {
    return new CompactionQueryBuilder(CompactionQueryBuilder.CompactionType.MINOR_INSERT_ONLY,
        CompactionQueryBuilder.Operation.ALTER, tableName)
        .setDir(dir)
        .setValidWriteIdList(validWriteIdList)
        .build();
  }

  /**
   * Get a list containing just the minor compaction query. The query selects the content of the
   * source temporary table and inserts it into the resulttable. It will look like:
   * <ol>
   *  <li>insert into table $tmpTableBase_result select `col_1`, .. from tmpTableBase</li>
   * </ol>
   *
   * @param sourceTmpTableName an unique identifier, which helps to find all the temporary tables
   * @param resultTmpTableName
   * @return list of compaction queries, always non-null
   */
  private List<String> getCompactionQueries(String sourceTmpTableName, String resultTmpTableName,
      Table sourceTable) {
    return Lists.newArrayList(
        new CompactionQueryBuilder(
            CompactionQueryBuilder.CompactionType.MINOR_INSERT_ONLY,
            CompactionQueryBuilder.Operation.INSERT,
            resultTmpTableName)
        .setSourceTabForInsert(sourceTmpTableName)
        .setSourceTab(sourceTable)
        .build()
    );
  }

  /**
   * Get list of drop table statements.
   * @param tmpTableBase an unique identifier, which helps to find all the temp tables
   * @return list of drop table statements, always non-null
   */
  private List<String> getDropQueries(String tmpTableBase) {
    return Lists.newArrayList(
        getDropQuery(tmpTableBase),
        getDropQuery(tmpTableBase + "_result")
        );
  }

  private String getDropQuery(String tableToDrop) {
    return new CompactionQueryBuilder(
        CompactionQueryBuilder.CompactionType.MINOR_INSERT_ONLY,
        CompactionQueryBuilder.Operation.DROP,
        tableToDrop).build();
  }

  private HiveConf setUpDriverSession(HiveConf hiveConf) {
    HiveConf driverConf = new HiveConf(hiveConf);
    driverConf.set(HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT.varname, "column");
    driverConf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_FETCH_COLUMN_STATS, false);
    driverConf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_ESTIMATE_STATS, false);
    return driverConf;
  }
}
