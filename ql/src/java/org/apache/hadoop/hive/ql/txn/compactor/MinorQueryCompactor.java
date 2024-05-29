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
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class responsible for handling query based minor compaction.
 */
final class MinorQueryCompactor extends QueryCompactor {

  private static final Logger LOG = LoggerFactory.getLogger(MinorQueryCompactor.class.getName());

  @Override
  public boolean run(CompactorContext context) throws IOException {
    LOG.info("Running query based minor compaction");
    HiveConf hiveConf = context.getConf();
    Table table = context.getTable();
    AcidUtils
        .setAcidOperationalProperties(hiveConf, true, AcidUtils.getAcidOperationalProperties(table.getParameters()));
    StorageDescriptor storageDescriptor = context.getSd();
    AcidDirectory dir = context.getAcidDirectory();
    ValidWriteIdList writeIds = context.getValidWriteIdList();
    // Set up the session for driver.
    HiveConf conf = new HiveConf(hiveConf);
    conf.set(HiveConf.ConfVars.SPLIT_GROUPING_MODE.varname, CompactorUtil.COMPACTOR);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_FETCH_COLUMN_STATS, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_ESTIMATE_STATS, false);

    String tmpTableName =
        table.getDbName() + "_tmp_compactor_" + table.getTableName() + "_" + System.currentTimeMillis();

    Path resultDeltaDir = QueryCompactor.Util.getCompactionResultDir(storageDescriptor,
        writeIds, conf, false, false, false, dir);
    Path resultDeleteDeltaDir = QueryCompactor.Util.getCompactionResultDir(storageDescriptor,
        writeIds, conf, false, true, false, dir);

    List<String> createQueries = getCreateQueries(table, tmpTableName, dir, writeIds,
        resultDeltaDir, resultDeleteDeltaDir);
    List<String> compactionQueries = getCompactionQueries(tmpTableName, table, writeIds);
    List<String> dropQueries = getDropQueries(tmpTableName);

    runCompactionQueries(conf, tmpTableName, storageDescriptor, writeIds, context.getCompactionInfo(),
        Lists.newArrayList(resultDeltaDir, resultDeleteDeltaDir), createQueries,
        compactionQueries, dropQueries, table.getParameters());
    return true;
  }


  @Override
  protected void commitCompaction(String dest, String tmpTableName, HiveConf conf,
      ValidWriteIdList actualWriteIds, long compactorTxnId) throws IOException, HiveException {
    Util.cleanupEmptyTableDir(conf, AcidUtils.DELTA_PREFIX + tmpTableName + "_result");
    Util.cleanupEmptyTableDir(conf, AcidUtils.DELETE_DELTA_PREFIX + tmpTableName + "_result");
  }

  /**
   * Get a list of create/alter table queries. These tables serves as temporary data source for query based
   * minor compaction. The following tables are created:
   * <ol>
   *   <li>tmpDelta, tmpDeleteDelta - temporary, external, partitioned table, having the schema of an ORC ACID file.
   *   Each partition corresponds to exactly one delta/delete-delta directory</li>
   *   <li>tmpDeltaResult, tmpDeleteDeltaResult - temporary table which stores the aggregated results of the minor
   *   compaction query</li>
   * </ol>
   * @param table the source table, where the compaction is running on
   * @param tempTableBase an unique identifier which is used to create delta/delete-delta temp tables
   * @param dir the directory, where the delta directories resides
   * @param writeIds list of valid write ids, used to filter out delta directories which are not relevant for compaction
   * @param tmpTableResultLocation result delta dir
   * @param tmpTableDeleteResultLocation result delete delta dir
   * @return list of create/alter queries, always non-null
   */
  private List<String> getCreateQueries(Table table, String tempTableBase, AcidDirectory dir,
      ValidWriteIdList writeIds, Path tmpTableResultLocation, Path tmpTableDeleteResultLocation) {
    List<String> queries = new ArrayList<>();
    // create delta temp table
    String tmpTableName = AcidUtils.DELTA_PREFIX + tempTableBase;
    queries.add(buildCreateTableQuery(table, tmpTableName, true, false, null));
    String alterQuery = buildAlterTableQuery(tmpTableName, dir, writeIds, false);
    if (!alterQuery.isEmpty()) {
      queries.add(alterQuery);
    }
    // create delta result temp table
    queries.add(buildCreateTableQuery(table, tmpTableName + "_result", false, true,
        tmpTableResultLocation.toString()));

    // create delete delta temp tables
    String tmpDeleteTableName = AcidUtils.DELETE_DELTA_PREFIX + tempTableBase;
    queries.add(buildCreateTableQuery(table, tmpDeleteTableName,  true, false, null));

    alterQuery = buildAlterTableQuery(tmpDeleteTableName, dir, writeIds, true);
    if (!alterQuery.isEmpty()) {
      queries.add(alterQuery);
    }
    // create delete delta result temp table
    queries.add(buildCreateTableQuery(table, tmpDeleteTableName + "_result",  false, true,
        tmpTableDeleteResultLocation.toString()));
    return queries;
  }

  /**
   * Helper method, which builds a create table query. The create query is customized based on the input arguments, but
   * the schema of the table is the same as an ORC ACID file schema.
   * @param table he source table, where the compaction is running on
   * @param newTableName name of the table to be created
   * @param isPartitioned true, if new table should be partitioned
   * @param isBucketed true, if the new table should be bucketed
   * @param location location of the table, can be null
   * @return a create table statement, always non-null. Example:
   * <p>
   *   if source table schema is: (a:int, b:int)
   * </p>
   * the corresponding create statement is:
   * <p>
   *   CREATE TEMPORARY EXTERNAL TABLE tmp_table (`operation` int, `originalTransaction` bigint, `bucket` int,
   *   `rowId` bigint, `currentTransaction` bigint, `row` struct<`a` :int, `b` :int> PARTITIONED BY (`file_name` string)
   *   STORED AS ORC TBLPROPERTIES ('transactional'='false','queryminorcomp'='true');
   * </p>
   */
  private String buildCreateTableQuery(Table table, String newTableName, boolean isPartitioned,
      boolean isBucketed, String location) {
    return new CompactionQueryBuilderFactory().getCompactionQueryBuilder(
        CompactionType.MINOR, false)
        .setOperation(CompactionQueryBuilder.Operation.CREATE)
        .setResultTableName(newTableName)
        .setSourceTab(table)
        .setBucketed(isBucketed)
        .setPartitioned(isPartitioned)
        .setLocation(location)
        .build();
  }

  /**
   * Builds an alter table query, which adds partitions pointing to location of delta directories.
   * @param tableName name of the to be altered table
   * @param dir the parent directory of delta directories
   * @param validWriteIdList list of valid write IDs
   * @param isDeleteDelta if true, only the delete delta directories will be mapped as new partitions, otherwise only
   *                      the delta directories
   * @return alter table statement
   */
  private String buildAlterTableQuery(String tableName, AcidDirectory dir,
      ValidWriteIdList validWriteIdList, boolean isDeleteDelta) {
    return new CompactionQueryBuilderFactory().getCompactionQueryBuilder(
        CompactionType.MINOR, false)
        .setOperation(CompactionQueryBuilder.Operation.ALTER)
        .setResultTableName(tableName)
        .setDir(dir)
        .setValidWriteIdList(validWriteIdList)
        .setIsDeleteDelta(isDeleteDelta)
        .build();
  }

  /**
   * Get a list of compaction queries which fills up the delta/delete-delta temporary result tables.
   * @param tmpTableBase an unique identifier, which helps to find all the temporary tables
   * @param table
   * @param validWriteIdList list of valid write IDs. This list is used to filter out aborted/open
   *                         transactions
   * @return list of compaction queries, always non-null
   */
  private List<String> getCompactionQueries(String tmpTableBase, Table table, ValidWriteIdList validWriteIdList) {
    List<String> queries = new ArrayList<>();
    String sourceTableName = AcidUtils.DELTA_PREFIX + tmpTableBase;
    String resultTableName = sourceTableName + "_result";
    queries.add(buildCompactionQuery(sourceTableName, resultTableName, table, validWriteIdList));
    String sourceDeleteTableName = AcidUtils.DELETE_DELTA_PREFIX + tmpTableBase;
    String resultDeleteTableName = sourceDeleteTableName + "_result";
    queries.add(buildCompactionQuery(sourceDeleteTableName, resultDeleteTableName, table, validWriteIdList));
    return queries;
  }

  /**
   * Build a minor compaction query. A compaction query selects the content of the source temporary table and inserts
   * it into the result table, filtering out all rows which belong to open/aborted transactions.
   * @param sourceTableName the name of the source table
   * @param resultTableName the name of the result table
   * @param table the table to compact
   * @param validWriteIdList list of valid write IDs
   * @return compaction query, always non-null
   */
  private String buildCompactionQuery(String sourceTableName, String resultTableName, Table table,
      ValidWriteIdList validWriteIdList) {
    return new CompactionQueryBuilderFactory().getCompactionQueryBuilder(
        CompactionType.MINOR, false)
        .setOperation(CompactionQueryBuilder.Operation.INSERT)
        .setResultTableName(resultTableName)
        .setSourceTabForInsert(sourceTableName)
        .setSourceTab(table)
        .setValidWriteIdList(validWriteIdList)
        .build();
  }

  /**
   * Get list of drop table statements.
   * @param tmpTableBase an unique identifier, which helps to find all the tables used in query based minor compaction
   * @return list of drop table statements, always non-null
   */
  private List<String> getDropQueries(String tmpTableBase) {
    return Lists.newArrayList(
        getDropQuery(AcidUtils.DELTA_PREFIX + tmpTableBase),
        getDropQuery(AcidUtils.DELETE_DELTA_PREFIX + tmpTableBase),
        getDropQuery(AcidUtils.DELTA_PREFIX + tmpTableBase + "_result"),
        getDropQuery(AcidUtils.DELETE_DELTA_PREFIX + tmpTableBase + "_result"));
  }

  private String getDropQuery(String tableToDrop) {
    return new CompactionQueryBuilderFactory().getCompactionQueryBuilder(
        CompactionType.MINOR, false)
        .setOperation(CompactionQueryBuilder.Operation.DROP)
        .setResultTableName(tableToDrop)
        .build();
  }
}
