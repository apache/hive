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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.DriverUtils;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Class responsible of running query based major compaction.
 */
class MajorQueryCompactor extends QueryCompactor {

  private static final Logger LOG = LoggerFactory.getLogger(MajorQueryCompactor.class.getName());

  @Override
  void runCompaction(HiveConf hiveConf, Table table, Partition partition, StorageDescriptor storageDescriptor,
      ValidWriteIdList writeIds, CompactionInfo compactionInfo) throws IOException {
    AcidUtils
        .setAcidOperationalProperties(hiveConf, true, AcidUtils.getAcidOperationalProperties(table.getParameters()));

    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    SessionState sessionState = DriverUtils.setUpSessionState(hiveConf, user, true);
    // Set up the session for driver.
    HiveConf conf = new HiveConf(hiveConf);
    conf.set(HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT.varname, "column");
    /*
     * For now, we will group splits on tez so that we end up with all bucket files,
     * with same bucket number in one map task.
     */
    conf.set(HiveConf.ConfVars.SPLIT_GROUPING_MODE.varname, "compactor");
    String tmpPrefix = table.getDbName() + "_tmp_compactor_" + table.getTableName() + "_";
    String tmpTableName = tmpPrefix + System.currentTimeMillis();
    long compactorTxnId = CompactorMR.CompactorMap.getCompactorTxnId(conf);
    try {
      // Create a temporary table under the temp location --> db/tbl/ptn/_tmp_1234/db.tmp_compactor_tbl_1234
      String query = buildCrudMajorCompactionCreateTableQuery(tmpTableName, table);
      LOG.info("Running major compaction query into temp table with create definition: {}", query);
      try {
        DriverUtils.runOnDriver(conf, user, sessionState, query);
      } catch (Exception ex) {
        Throwable cause = ex;
        while (cause != null && !(cause instanceof AlreadyExistsException)) {
          cause = cause.getCause();
        }
        if (cause == null) {
          throw new IOException(ex);
        }
      }
      query = buildCrudMajorCompactionQuery(table, partition, tmpTableName);
      LOG.info("Running major compaction via query: {}", query);
      /*
       * This will create bucket files like:
       * db/db_tmp_compactor_tbl_1234/00000_0
       * db/db_tmp_compactor_tbl_1234/00001_0
       */
      DriverUtils.runOnDriver(conf, user, sessionState, query, writeIds, compactorTxnId);
      /*
       * This achieves a final layout like (wid is the highest valid write id for this major compaction):
       * db/tbl/ptn/base_wid/bucket_00000
       * db/tbl/ptn/base_wid/bucket_00001
       */
      org.apache.hadoop.hive.ql.metadata.Table tempTable = Hive.get().getTable(tmpTableName);
      String tmpLocation = tempTable.getSd().getLocation();
      commitCrudMajorCompaction(tmpLocation, tmpTableName, storageDescriptor.getLocation(), conf, writeIds,
          compactorTxnId);
    } catch (HiveException e) {
      LOG.error("Error doing query based major compaction", e);
      throw new IOException(e);
    } finally {
      try {
        DriverUtils.runOnDriver(conf, user, sessionState, "drop table if exists " + tmpTableName);
      } catch (HiveException e) {
        LOG.error("Unable to delete drop temp table {} which was created for running major compaction", tmpTableName);
        LOG.error(ExceptionUtils.getStackTrace(e));
      }
    }
  }

  /**
   * Note on ordering of rows in the temp table:
   * We need each final bucket file soreted by original write id (ascending), bucket (ascending) and row id (ascending).
   * (current write id will be the same as original write id).
   * We will be achieving the ordering via a custom split grouper for compactor.
   * See {@link org.apache.hadoop.hive.conf.HiveConf.ConfVars#SPLIT_GROUPING_MODE} for the config description.
   * See {@link org.apache.hadoop.hive.ql.exec.tez.SplitGrouper#getCompactorSplitGroups(InputSplit[], Configuration)}
   *  for details on the mechanism.
   */
  private String buildCrudMajorCompactionCreateTableQuery(String fullName, Table t) {
    StringBuilder query = new StringBuilder("create temporary table ").append(fullName).append(" (");
    // Acid virtual columns
    query.append(
        "`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, "
            + "`row` struct<");
    List<FieldSchema> cols = t.getSd().getCols();
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
    query.append(" stored as orc");
    query.append(" tblproperties ('transactional'='false')");
    return query.toString();
  }

  private String buildCrudMajorCompactionQuery(Table t, Partition p, String tmpName) {
    String fullName = t.getDbName() + "." + t.getTableName();
    StringBuilder query = new StringBuilder("insert into table " + tmpName + " ");
    StringBuilder filter = new StringBuilder();
    if (p != null) {
      filter.append(" where ");
      List<String> vals = p.getValues();
      List<FieldSchema> keys = t.getPartitionKeys();
      assert keys.size() == vals.size();
      for (int i = 0; i < keys.size(); ++i) {
        filter.append(i == 0 ? "`" : " and `").append(keys.get(i).getName()).append("`='").append(vals.get(i))
            .append("'");
      }
    }
    query.append(" select validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId), ROW__ID.writeId, "
        + "ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT(");
    List<FieldSchema> cols = t.getSd().getCols();
    for (int i = 0; i < cols.size(); ++i) {
      query.append(i == 0 ? "'" : ", '").append(cols.get(i).getName()).append("', ").append(cols.get(i).getName());
    }
    query.append(") from ").append(fullName).append(filter);
    return query.toString();
  }

  /**
   * Move and rename bucket files from the temp table (tmpTableName), to the new base path under the source table/ptn.
   * Since the temp table is a non-transactional table, it has file names in the "original" format.
   * Also, due to split grouping in
   * {@link org.apache.hadoop.hive.ql.exec.tez.SplitGrouper#getCompactorSplitGroups(InputSplit[], Configuration)},
   * we will end up with one file per bucket.
   */
  private void commitCrudMajorCompaction(String from, String tmpTableName, String to, HiveConf conf,
      ValidWriteIdList actualWriteIds, long compactorTxnId) throws IOException, HiveException {
    Path fromPath = new Path(from);
    Path toPath = new Path(to);
    Path tmpTablePath = new Path(fromPath, tmpTableName);
    FileSystem fs = fromPath.getFileSystem(conf);
    // Assume the high watermark can be used as maximum transaction ID.
    long maxTxn = actualWriteIds.getHighWatermark();
    // Get a base_wid path which will be the new compacted base
    AcidOutputFormat.Options options =
        new AcidOutputFormat.Options(conf).writingBase(true).isCompressed(false).maximumWriteId(maxTxn).bucket(0)
            .statementId(-1);
    Path newBaseDir = AcidUtils.createFilename(toPath, options).getParent();
    if (!fs.exists(fromPath)) {
      LOG.info("{} not found.  Assuming 0 splits. Creating {}", from, newBaseDir);
      fs.mkdirs(newBaseDir);
      return;
    }
    LOG.info("Moving contents of {} to {}", tmpTablePath, to);
    /*
     * Currently mapping file with name 0000_0 to bucket_00000, 0000_1 to bucket_00001 and so on
     * TODO/ToThink:
     * Q. Can file with name 0000_0 under temp table be deterministically renamed to bucket_00000 in the destination?
     */
    //    List<String> buckCols = t.getSd().getBucketCols();
    FileStatus[] children = fs.listStatus(fromPath);
    for (FileStatus filestatus : children) {
      String originalFileName = filestatus.getPath().getName();
      // This if() may not be required I think...
      if (AcidUtils.ORIGINAL_PATTERN.matcher(originalFileName).matches()) {
        int bucketId = AcidUtils.parseBucketId(filestatus.getPath());
        options = new AcidOutputFormat.Options(conf).writingBase(true).isCompressed(false).maximumWriteId(maxTxn)
            .bucket(bucketId).statementId(-1).visibilityTxnId(compactorTxnId);
        Path finalBucketFile = AcidUtils.createFilename(toPath, options);
        Hive.moveFile(conf, filestatus.getPath(), finalBucketFile, true, false, false);
      }
    }
    fs.delete(fromPath, true);
  }
}
