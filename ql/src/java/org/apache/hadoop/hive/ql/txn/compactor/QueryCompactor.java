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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
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
import java.util.Optional;
import java.util.UUID;

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
      ValidWriteIdList writeIds, CompactionInfo compactionInfo) throws IOException;

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
   * @param createQueries collection of queries which creates the temporary tables.
   * @param compactionQueries collection of queries which uses data from the original table and writes in temporary
   *                          tables.
   * @param dropQueries queries which drops the temporary tables.
   * @throws IOException error during the run of the compaction.
   */
  protected void runCompactionQueries(HiveConf conf, String tmpTableName, StorageDescriptor storageDescriptor,
      ValidWriteIdList writeIds, CompactionInfo compactionInfo, List<String> createQueries,
      List<String> compactionQueries, List<String> dropQueries) throws IOException {
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
   * Collection of some helper functions.
   */
  static class Util {
    /**
     * Generate a random tmp path, under the provided storage.
     * @param sd storage descriptor, must be not null.
     * @return path, always not null
     */
    static String generateTmpPath(StorageDescriptor sd) {
      return sd.getLocation() + "/" + TMPDIR + "_" + UUID.randomUUID().toString();
    }

    /**
     * Check whether the result directory exits and contains compacted result files. If no splits are found, create
     * an empty directory at the destination path, matching a base/delta directory naming convention.
     * @param sourcePath the checked source location
     * @param destPath the destination, where the new directory should be created
     * @param isMajorCompaction is called from a major compaction
     * @param isDeleteDelta is the output used as delete delta directory
     * @param conf hive configuration
     * @param validWriteIdList maximum transaction id
     * @return true, if the check was successful
     * @throws IOException the new directory cannot be created
     */
    private static boolean resultHasSplits(Path sourcePath, Path destPath, boolean isMajorCompaction,
        boolean isDeleteDelta, HiveConf conf, ValidWriteIdList validWriteIdList) throws IOException {
      FileSystem fs = sourcePath.getFileSystem(conf);
      long minOpenWriteId = validWriteIdList.getMinOpenWriteId() == null ? 1 : validWriteIdList.getMinOpenWriteId();
      long highWatermark = validWriteIdList.getHighWatermark();
      AcidOutputFormat.Options options =
          new AcidOutputFormat.Options(conf).writingBase(isMajorCompaction).writingDeleteDelta(isDeleteDelta)
              .isCompressed(false).minimumWriteId(minOpenWriteId)
              .maximumWriteId(highWatermark).bucket(0).statementId(-1);
      Path newDeltaDir = AcidUtils.createFilename(destPath, options).getParent();
      if (!fs.exists(sourcePath)) {
        LOG.info("{} not found. Assuming 0 splits. Creating {}", sourcePath, newDeltaDir);
        fs.mkdirs(newDeltaDir);
        return false;
      }
      return true;
    }

    /**
     * Create the base/delta directory matching the naming conventions and move the result files of the compaction
     * into it.
     * @param sourcePath location of the result files
     * @param destPath destination path of the result files, without the base/delta directory
     * @param isMajorCompaction is this called from a major compaction
     * @param isDeleteDelta is the destination is a delete delta directory
     * @param conf hive configuration
     * @param validWriteIdList list of valid write Ids
     * @param compactorTxnId transaction Id of the compaction
     * @throws IOException the destination directory cannot be created
     * @throws HiveException the result files cannot be moved to the destination directory
     */
    static void moveContents(Path sourcePath, Path destPath, boolean isMajorCompaction, boolean isDeleteDelta,
        HiveConf conf, ValidWriteIdList validWriteIdList, long compactorTxnId) throws IOException, HiveException {
      if (!resultHasSplits(sourcePath, destPath, isMajorCompaction, isDeleteDelta, conf, validWriteIdList)) {
        return;
      }
      LOG.info("Moving contents of {} to {}", sourcePath, destPath);
      FileSystem fs = sourcePath.getFileSystem(conf);
      long minOpenWriteId = validWriteIdList.getMinOpenWriteId() == null ? 1 : validWriteIdList.getMinOpenWriteId();
      long highWatermark = validWriteIdList.getHighWatermark();
      for (FileStatus fileStatus : fs.listStatus(sourcePath)) {
        String originalFileName = fileStatus.getPath().getName();
        if (AcidUtils.ORIGINAL_PATTERN.matcher(originalFileName).matches()) {
          Optional<Integer> bucketId = AcidUtils.parseBucketIdFromRow(fs, fileStatus.getPath());
          if (bucketId.isPresent()) {
            AcidOutputFormat.Options options =
                new AcidOutputFormat.Options(conf).writingBase(isMajorCompaction).writingDeleteDelta(isDeleteDelta)
                    .isCompressed(false).minimumWriteId(minOpenWriteId)
                    .maximumWriteId(highWatermark).bucket(bucketId.get()).statementId(-1)
                    .visibilityTxnId(compactorTxnId);
            Path finalBucketFile = AcidUtils.createFilename(destPath, options);
            Hive.moveFile(conf, fileStatus.getPath(), finalBucketFile, true, false, false);
          }
        }
      }
      fs.delete(sourcePath, true);
    }

    /**
     * Unless caching is explicitly required for ETL queries this method disables it.
     * LLAP cache content lookup is file based, and since compaction alters the file structure it is not beneficial to
     * cache anything here, as it won't (and actually can't) ever be looked up later.
     * @param conf the Hive configuration
     */
    static void disableLlapCaching(HiveConf conf) {
      String llapIOETLSkipFormat = conf.getVar(HiveConf.ConfVars.LLAP_IO_ETL_SKIP_FORMAT);
      if (!"none".equals(llapIOETLSkipFormat)) {
        // Unless caching is explicitly required for ETL queries - disable it.
        conf.setVar(HiveConf.ConfVars.LLAP_IO_ETL_SKIP_FORMAT, "all");
      }
    }

  }
}
