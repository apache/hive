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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
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
 * Common interface for query based compactions.
 */
abstract class QueryCompactor {

  private static final Logger LOG = LoggerFactory.getLogger(QueryCompactor.class.getName());
  private static final String TMPDIR = "_tmp";

  /**
   * Start a query based compaction.
   * @param hiveConf hive configuration
   * @param table the table, where the compaction should run
   * @param partition the partition, where the compaction should run
   * @param storageDescriptor this is the resolved storage descriptor
   * @param writeIds valid write IDs used to filter rows while they're being read for compaction
   * @param compactionInfo provides info about the type of compaction
   * @throws IOException compaction cannot be finished.
   */
  abstract void runCompaction(HiveConf hiveConf, Table table, Partition partition, StorageDescriptor storageDescriptor,
      ValidWriteIdList writeIds, CompactionInfo compactionInfo) throws IOException, HiveException;

  /**
   * This is the final step of the compaction, which can vary based on compaction type. Usually this involves some file
   * operation.
   * @param dest The final directory; basically an SD directory.
   * @param tmpTableName The name of the temporary table.
   * @param conf hive configuration.
   * @param actualWriteIds valid write Ids used to fetch the high watermark Id.
   * @param compactorTxnId transaction, that the compacter started.
   * @throws IOException failed to execute file system operation.
   * @throws HiveException failed to execute file operation within hive.
   */
  protected abstract void commitCompaction(String dest, String tmpTableName, HiveConf conf,
      ValidWriteIdList actualWriteIds, long compactorTxnId) throws IOException, HiveException;

  /**
   * Run all the queries which performs the compaction.
   * @param conf hive configuration, must be not null.
   * @param tmpTableName The name of the temporary table.
   * @param storageDescriptor this is the resolved storage descriptor.
   * @param writeIds valid write IDs used to filter rows while they're being read for compaction.
   * @param compactionInfo provides info about the type of compaction.
   * @param resultDirs the delta/base directory that is created as the result of compaction.
   * @param createQueries collection of queries which creates the temporary tables.
   * @param compactionQueries collection of queries which uses data from the original table and writes in temporary
   *                          tables.
   * @param dropQueries queries which drops the temporary tables.
   * @throws IOException error during the run of the compaction.
   */
  void runCompactionQueries(HiveConf conf, String tmpTableName, StorageDescriptor storageDescriptor,
      ValidWriteIdList writeIds, CompactionInfo compactionInfo, List<Path> resultDirs,
      List<String> createQueries, List<String> compactionQueries, List<String> dropQueries)
      throws IOException {
    Util.disableLlapCaching(conf);
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    SessionState sessionState = DriverUtils.setUpSessionState(conf, user, true);
    long compactorTxnId = CompactorMR.CompactorMap.getCompactorTxnId(conf);
    try {
      for (String query : createQueries) {
        try {
          LOG.info("Running {} compaction query into temp table with query: {}",
              compactionInfo.isMajorCompaction() ? "major" : "minor", query);
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
      }
      for (String query : compactionQueries) {
        LOG.info("Running {} compaction via query: {}", compactionInfo.isMajorCompaction() ? "major" : "minor", query);
        DriverUtils.runOnDriver(conf, user, sessionState, query, writeIds, compactorTxnId);
      }
      commitCompaction(storageDescriptor.getLocation(), tmpTableName, conf, writeIds, compactorTxnId);
    } catch (HiveException e) {
      LOG.error("Error doing query based {} compaction", compactionInfo.isMajorCompaction() ? "major" : "minor", e);
      removeResultDirs(resultDirs, conf);
      throw new IOException(e);
    } finally {
      try {
        for (String query : dropQueries) {
          LOG.info("Running {} compaction query into temp table with query: {}",
              compactionInfo.isMajorCompaction() ? "major" : "minor", query);
          DriverUtils.runOnDriver(conf, user, sessionState, query);
        }
      } catch (HiveException e) {
        LOG.error("Unable to drop temp table {} which was created for running {} compaction", tmpTableName,
            compactionInfo.isMajorCompaction() ? "major" : "minor");
        LOG.error(ExceptionUtils.getStackTrace(e));
      }
    }
  }

  /**
   * Call in case compaction failed. Removes the new empty compacted delta/base.
   * Cleaner would handle this later but clean up now just in case.
   */
  private void removeResultDirs(List<Path> resultDirPaths, HiveConf conf) throws IOException {
    for (Path path : resultDirPaths) {
      LOG.info("Compaction failed, removing directory: " + path.toString());
      FileSystem fs = path.getFileSystem(conf);
      if (!fs.listFiles(path, false).hasNext()) {
        fs.delete(path, true);
      }
    }
  }

  /**
   * Collection of some helper functions.
   */
  static class Util {

    /**
     * Get the path of the base, delta, or delete delta directory that will be the final
     * destination of the files during compaction.
     *
     * @param sd storage descriptor of table or partition to compact
     * @param writeIds list of valid writeids
     * @param conf HiveConf
     * @param writingBase if true, we are creating a base directory, otherwise a delta
     * @param createDeleteDelta if true, the delta dir we are creating is a delete delta
     * @param bucket0 whether to specify 0 as the bucketid
     *
     * @return Path of new base/delta/delete delta directory
     */
    static Path getCompactionResultDir(StorageDescriptor sd, ValidWriteIdList writeIds, HiveConf conf,
        boolean writingBase, boolean createDeleteDelta, boolean bucket0) {
      long minOpenWriteId = writeIds.getMinOpenWriteId() == null ? 1 : writeIds.getMinOpenWriteId();
      long highWatermark = writeIds.getHighWatermark();
      long compactorTxnId = CompactorMR.CompactorMap.getCompactorTxnId(conf);
      AcidOutputFormat.Options options =
          new AcidOutputFormat.Options(conf).isCompressed(false).minimumWriteId(minOpenWriteId)
              .maximumWriteId(highWatermark).statementId(-1).visibilityTxnId(compactorTxnId)
              .writingBase(writingBase).writingDeleteDelta(createDeleteDelta);
      if (bucket0) {
        options = options.bucket(0);
      }
      Path location = new Path(sd.getLocation());
      return AcidUtils.baseOrDeltaSubdirPath(location, options);
    }

    /**
     * Unless caching is explicitly required for ETL queries this method disables it.
     * LLAP cache content lookup is file based, and since compaction alters the file structure it is not beneficial to
     * cache anything here, as it won't (and actually can't) ever be looked up later.
     * @param conf the Hive configuration
     */
    private static void disableLlapCaching(HiveConf conf) {
      String llapIOETLSkipFormat = conf.getVar(HiveConf.ConfVars.LLAP_IO_ETL_SKIP_FORMAT);
      if (!"none".equals(llapIOETLSkipFormat)) {
        // Unless caching is explicitly required for ETL queries - disable it.
        conf.setVar(HiveConf.ConfVars.LLAP_IO_ETL_SKIP_FORMAT, "all");
      }
    }

    /**
     * Remove the root directory of a table if it's empty.
     * @param conf the Hive configuration
     * @param tmpTableName name of the table
     * @throws IOException the directory cannot be deleted
     * @throws HiveException the table is not found
     */
    static void cleanupEmptyDir(HiveConf conf, String tmpTableName) throws IOException, HiveException {
      org.apache.hadoop.hive.ql.metadata.Table tmpTable = Hive.get().getTable(tmpTableName);
      if (tmpTable != null) {
        Path path = new Path(tmpTable.getSd().getLocation());
        FileSystem fs = path.getFileSystem(conf);
        if (!fs.listFiles(path, false).hasNext()) {
          fs.delete(path, true);
        }
      }
    }
    /**
     * Remove the delta directories of aborted transactions.
     */
    static void removeFilesForMmTable(HiveConf conf, AcidUtils.Directory dir) throws IOException {
      List<Path> filesToDelete = dir.getAbortedDirectories();
      if (filesToDelete.size() < 1) {
        return;
      }
      LOG.info("About to remove " + filesToDelete.size() + " aborted directories from " + dir);
      FileSystem fs = filesToDelete.get(0).getFileSystem(conf);
      for (Path dead : filesToDelete) {
        LOG.debug("Going to delete path " + dead.toString());
        fs.delete(dead, true);
      }
    }
  }
}
