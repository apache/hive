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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.common.util.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Class responsible for handling query based minor compaction.
 */
final class MinorQueryCompactor extends QueryCompactor {

  public static final String MINOR_COMP_TBL_PROP = "queryminorcomp";
  private static final Logger LOG = LoggerFactory.getLogger(MinorQueryCompactor.class.getName());

  @Override void runCompaction(HiveConf hiveConf, Table table, Partition partition, StorageDescriptor storageDescriptor,
      ValidWriteIdList writeIds, CompactionInfo compactionInfo) throws IOException {
    LOG.info("Running query based minor compaction");
    AcidUtils
        .setAcidOperationalProperties(hiveConf, true, AcidUtils.getAcidOperationalProperties(table.getParameters()));
    AcidUtils.Directory dir = AcidUtils
        .getAcidState(null, new Path(storageDescriptor.getLocation()), hiveConf, writeIds, Ref.from(false), false,
            table.getParameters(), false);
    if (!Util.isEnoughToCompact(compactionInfo.isMajorCompaction(), dir, storageDescriptor)) {
      return;
    }
    // Set up the session for driver.
    HiveConf conf = new HiveConf(hiveConf);
    conf.set(HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT.varname, "column");
    conf.set(HiveConf.ConfVars.SPLIT_GROUPING_MODE.varname, "compactor");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_FETCH_COLUMN_STATS, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_ESTIMATE_STATS, false);
    String tmpTableName =
        table.getDbName() + "_tmp_compactor_" + table.getTableName() + "_" + System.currentTimeMillis();
    List<String> createQueries = getCreateQueries(table, tmpTableName, dir, writeIds);
    List<String> compactionQueries = getCompactionQueries(tmpTableName, writeIds.getInvalidWriteIds());
    List<String> dropQueries = getDropQueries(tmpTableName);

    runCompactionQueries(conf, tmpTableName, storageDescriptor, writeIds, compactionInfo, createQueries,
        compactionQueries, dropQueries);
  }

  @Override protected void commitCompaction(String dest, String tmpTableName, HiveConf conf,
      ValidWriteIdList actualWriteIds, long compactorTxnId) throws IOException, HiveException {
    // get result temp tables;
    String deltaTableName = AcidUtils.DELTA_PREFIX + tmpTableName + "_result";
    commitCompaction(deltaTableName, dest, false, conf, actualWriteIds, compactorTxnId);

    String deleteDeltaTableName = AcidUtils.DELETE_DELTA_PREFIX + tmpTableName + "_result";
    commitCompaction(deleteDeltaTableName, dest, true, conf, actualWriteIds, compactorTxnId);
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
   * @return list of create/alter queries, always non-null
   */
  private List<String> getCreateQueries(Table table, String tempTableBase, AcidUtils.Directory dir,
      ValidWriteIdList writeIds) {
    List<String> queries = new ArrayList<>();
    // create delta temp table
    String tmpTableName = AcidUtils.DELTA_PREFIX + tempTableBase;
    queries.add(buildCreateTableQuery(table, tmpTableName, true, true, false));
    buildAlterTableQuery(tmpTableName, dir, writeIds, false).ifPresent(queries::add);
    // create delta result temp table
    queries.add(buildCreateTableQuery(table, tmpTableName + "_result", false, false, true));

    // create delete delta temp tables
    String tmpDeleteTableName = AcidUtils.DELETE_DELTA_PREFIX + tempTableBase;
    queries.add(buildCreateTableQuery(table, tmpDeleteTableName, true, true, false));
    buildAlterTableQuery(tmpDeleteTableName, dir, writeIds, true).ifPresent(queries::add);
    // create delete delta result temp table
    queries.add(buildCreateTableQuery(table, tmpDeleteTableName + "_result", false, false, true));
    return queries;
  }

  /**
   * Helper method, which builds a create table query. The create query is customized based on the input arguments, but
   * the schema of the table is the same as an ORC ACID file schema.
   * @param table he source table, where the compaction is running on
   * @param newTableName name of the table to be created
   * @param isExternal true, if new table should be external
   * @param isPartitioned true, if new table should be partitioned
   * @param isBucketed true, if the new table should be bucketed
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
  private String buildCreateTableQuery(Table table, String newTableName, boolean isExternal, boolean isPartitioned,
      boolean isBucketed) {
    StringBuilder query = new StringBuilder("create temporary ");
    if (isExternal) {
      query.append("external ");
    }
    query.append("table ").append(newTableName).append(" (");
    // Acid virtual columns
    query.append(
        "`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, "
            + "`row` struct<");
    List<FieldSchema> cols = table.getSd().getCols();
    boolean isFirst = true;
    // Actual columns
    for (FieldSchema col : cols) {
      if (!isFirst) {
        query.append(", ");
      }
      isFirst = false;
      query.append("`").append(col.getName()).append("` ").append(":").append(col.getType());
    }
    query.append(">)");
    if (isPartitioned) {
      query.append(" partitioned by (`file_name` string)");
    }
    int bucketingVersion = 0;
    if (isBucketed) {
      int numBuckets = 1;
      try {
        org.apache.hadoop.hive.ql.metadata.Table t = Hive.get().getTable(table.getDbName(), table.getTableName());
        numBuckets = Math.max(t.getNumBuckets(), numBuckets);
        bucketingVersion = t.getBucketingVersion();
      } catch (HiveException e) {
        LOG.info("Error finding table {}. Minor compaction result will use 0 buckets.", table.getTableName());
      } finally {
        query.append(" clustered by (`bucket`)").append(" sorted by (`bucket`, `originalTransaction`, `rowId`)")
            .append(" into ").append(numBuckets).append(" buckets");
      }
    }

    query.append(" stored as orc");
    query.append(" tblproperties ('transactional'='false'");
    query.append(", '");
    query.append(MINOR_COMP_TBL_PROP);
    query.append("'='true'");
    if (isBucketed) {
      query.append(", 'bucketing_version'='")
          .append(bucketingVersion)
          .append("')");
    } else {
      query.append(")");
    }
    return query.toString();
  }

  /**
   * Builds an alter table query, which adds partitions pointing to location of delta directories.
   * @param tableName name of the to be altered table
   * @param dir the parent directory of delta directories
   * @param validWriteIdList list of valid write IDs
   * @param isDeleteDelta if true, only the delete delta directories will be mapped as new partitions, otherwise only
   *                      the delta directories
   * @return alter table statement wrapped in {@link Optional}.
   */
  private Optional<String> buildAlterTableQuery(String tableName, AcidUtils.Directory dir,
      ValidWriteIdList validWriteIdList, boolean isDeleteDelta) {
    // add partitions
    if (!dir.getCurrentDirectories().isEmpty()) {
      long minWriteID = validWriteIdList.getMinOpenWriteId() == null ? 1 : validWriteIdList.getMinOpenWriteId();
      long highWatermark = validWriteIdList.getHighWatermark();
      List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories().stream().filter(
          delta -> delta.isDeleteDelta() == isDeleteDelta && delta.getMaxWriteId() <= highWatermark
              && delta.getMinWriteId() >= minWriteID)
          .collect(Collectors.toList());
      if (!deltas.isEmpty()) {
        StringBuilder query = new StringBuilder().append("alter table ").append(tableName);
        query.append(" add ");
        deltas.forEach(
            delta -> query.append("partition (file_name='").append(delta.getPath().getName()).append("') location '")
                .append(delta.getPath()).append("' "));
        return Optional.of(query.toString());
      }
    }
    return Optional.empty();
  }

  /**
   * Get a list of compaction queries which fills up the delta/delete-delta temporary result tables.
   * @param tmpTableBase an unique identifier, which helps to find all the temporary tables
   * @param invalidWriteIds list of invalid write IDs. This list is used to filter out aborted/open transactions
   * @return list of compaction queries, always non-null
   */
  private List<String> getCompactionQueries(String tmpTableBase, long[] invalidWriteIds) {
    List<String> queries = new ArrayList<>();
    String sourceTableName = AcidUtils.DELTA_PREFIX + tmpTableBase;
    String resultTableName = sourceTableName + "_result";
    queries.add(buildCompactionQuery(sourceTableName, resultTableName, invalidWriteIds));
    String sourceDeleteTableName = AcidUtils.DELETE_DELTA_PREFIX + tmpTableBase;
    String resultDeleteTableName = sourceDeleteTableName + "_result";
    queries.add(buildCompactionQuery(sourceDeleteTableName, resultDeleteTableName, invalidWriteIds));
    return queries;
  }

  /**
   * Build a minor compaction query. A compaction query selects the content of the source temporary table and inserts
   * it into the result table, filtering out all rows which belong to open/aborted transactions.
   * @param sourceTableName the name of the source table
   * @param resultTableName the name of the result table
   * @param invalidWriteIds list of invalid write IDs
   * @return compaction query, always non-null
   */
  private String buildCompactionQuery(String sourceTableName, String resultTableName, long[] invalidWriteIds) {
    StringBuilder query = new StringBuilder().append("insert into table ").append(resultTableName)
        .append(" select `operation`, `originalTransaction`, `bucket`, `rowId`, `currentTransaction`, `row` from ")
        .append(sourceTableName);
    if (invalidWriteIds.length > 0) {
      query.append(" where `originalTransaction` not in (")
          .append(StringUtils.join(ArrayUtils.toObject(invalidWriteIds), ",")).append(")");
    }

    return query.toString();
  }

  /**
   * Get list of drop table statements.
   * @param tmpTableBase an unique identifier, which helps to find all the tables used in query based minor compaction
   * @return list of drop table statements, always non-null
   */
  private List<String> getDropQueries(String tmpTableBase) {
    List<String> queries = new ArrayList<>();
    String dropStm = "drop table if exists ";
    queries.add(dropStm + AcidUtils.DELTA_PREFIX + tmpTableBase);
    queries.add(dropStm + AcidUtils.DELETE_DELTA_PREFIX + tmpTableBase);
    queries.add(dropStm + AcidUtils.DELTA_PREFIX + tmpTableBase + "_result");
    queries.add(dropStm + AcidUtils.DELETE_DELTA_PREFIX + tmpTableBase + "_result");
    return queries;
  }

  /**
   * Creates the delta directory and moves the result files.
   * @param deltaTableName name of the temporary table, where the results are stored
   * @param dest destination path, where the result should be moved
   * @param isDeleteDelta is the destination a delete delta directory
   * @param conf hive configuration
   * @param actualWriteIds list of valid write Ids
   * @param compactorTxnId transaction Id of the compaction
   * @throws HiveException the result files cannot be moved
   * @throws IOException the destination delta directory cannot be created
   */
  private void commitCompaction(String deltaTableName, String dest, boolean isDeleteDelta, HiveConf conf,
      ValidWriteIdList actualWriteIds, long compactorTxnId) throws HiveException, IOException {
    org.apache.hadoop.hive.ql.metadata.Table deltaTable = Hive.get().getTable(deltaTableName);
    Util.moveContents(new Path(deltaTable.getSd().getLocation()), new Path(dest), false, isDeleteDelta, conf,
        actualWriteIds, compactorTxnId);
  }

}
