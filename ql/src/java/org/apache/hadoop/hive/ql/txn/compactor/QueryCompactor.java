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
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.utils.StringableMap;
import org.apache.hadoop.hive.ql.DriverUtils;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.stream.Stream;

/**
 * Common interface for query based compactions.
 */
abstract class QueryCompactor implements Compactor {

  private static final Logger LOG = LoggerFactory.getLogger(QueryCompactor.class.getName());
  private static final String COMPACTOR_PREFIX = "compactor.";

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
  protected void commitCompaction(String dest, String tmpTableName, HiveConf conf,
      ValidWriteIdList actualWriteIds, long compactorTxnId) throws IOException, HiveException {}

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
      List<String> createQueries, List<String> compactionQueries, List<String> dropQueries,
      Map<String, String> tblProperties) throws IOException {
    String queueName = HiveConf.getVar(conf, HiveConf.ConfVars.COMPACTOR_JOB_QUEUE);
    if (queueName != null && queueName.length() > 0) {
      conf.set(TezConfiguration.TEZ_QUEUE_NAME, queueName);
    }
    Util.disableLlapCaching(conf);
    conf.set(HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT.varname, "column");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_HDFS_ENCRYPTION_SHIM_CACHE_ON, false);
    Util.overrideConfProps(conf, compactionInfo, tblProperties);
    String user = compactionInfo.runAs;
    SessionState sessionState = DriverUtils.setUpSessionState(conf, user, true);
    long compactorTxnId = Compactor.getCompactorTxnId(conf);
    try {
      for (String query : createQueries) {
        try {
          LOG.info("Running {} compaction query into temp table with query: {}", compactionInfo.type, query);
          DriverUtils.runOnDriver(conf, sessionState, query);
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
        LOG.info("Running {} compaction via query: {}", compactionInfo.type, query);
        if (CompactionType.MINOR.equals(compactionInfo.type)) {
          // There was an issue with the query-based MINOR compaction (HIVE-23763), that the row distribution between the FileSinkOperators
          // was not correlated correctly with the bucket numbers. So we could end up with files containing rows from
          // multiple buckets or rows from the same bucket could end up in different FileSinkOperator. This behaviour resulted
          // corrupted files. To fix this, the FileSinkOperator has been extended to be able to handle rows from different buckets.
          // But we also had to be sure that all rows from the same bucket would end up in the same FileSinkOperator. Therefore
          // the ReduceSinkOperator has also been extended to distribute the rows by bucket numbers. To use this logic,
          // these two optimisations have to be turned off for the MINOR compaction. The MAJOR compaction works differently
          // and its query doesn't use reducers, so these optimisations should not be turned off for MAJOR compaction.
          conf.set("hive.optimize.bucketingsorting", "false");
          conf.set("hive.vectorized.execution.enabled", "false");
        }
        DriverUtils.runOnDriver(conf, sessionState, query, writeIds, compactorTxnId);
      }
      commitCompaction(storageDescriptor.getLocation(), tmpTableName, conf, writeIds, compactorTxnId);
    } catch (HiveException e) {
      LOG.error("Error doing query based {} compaction", compactionInfo.type, e);
      removeResultDirs(resultDirs, conf);
      throw new IOException(e);
    } finally {
      try {
        for (String query : dropQueries) {
          LOG.info("Running {} compaction query into temp table with query: {}", compactionInfo.type, query);
          DriverUtils.runOnDriver(conf, sessionState, query);
        }
      } catch (HiveException e) {
        LOG.error("Unable to drop temp table {} which was created for running {} compaction", tmpTableName,
            compactionInfo.type);
        LOG.error(ExceptionUtils.getStackTrace(e));
      }
    }
  }

  protected String getTempTableName(Table table) {
    return table.getDbName() + ".tmp_compactor_" + table.getTableName() + "_" + System.currentTimeMillis();
  }

  /**
   * Call in case compaction failed. Removes the new empty compacted delta/base.
   * Cleaner would handle this later but clean up now just in case.
   */
  private void removeResultDirs(List<Path> resultDirPaths, HiveConf conf) throws IOException {
    for (Path path : resultDirPaths) {
      LOG.info("Compaction failed, removing directory: " + path.toString());
      Util.cleanupEmptyDir(conf, path);
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
     * @param directory AcidUtils.Directory - only required for minor compaction result (delta) dirs
     *
     * @return Path of new base/delta/delete delta directory
     */
    static Path getCompactionResultDir(StorageDescriptor sd, ValidWriteIdList writeIds, HiveConf conf,
        boolean writingBase, boolean createDeleteDelta, boolean bucket0, AcidDirectory directory) {
      long minWriteID = writingBase ? 1 : getMinWriteID(directory);
      long highWatermark = writeIds.getHighWatermark();
      long compactorTxnId = Compactor.getCompactorTxnId(conf);
      AcidOutputFormat.Options options =
          new AcidOutputFormat.Options(conf).isCompressed(false).minimumWriteId(minWriteID)
              .maximumWriteId(highWatermark).statementId(-1).visibilityTxnId(compactorTxnId)
              .writingBase(writingBase).writingDeleteDelta(createDeleteDelta);
      if (bucket0) {
        options = options.bucket(0);
      }
      Path location = new Path(sd.getLocation());
      return AcidUtils.baseOrDeltaSubdirPath(location, options);
    }

    /**
     * Get the min writeId for the new result directory. This only matters if the result directory will be a delta
     * directory i.e. minor compaction.
     * AcidUtils.Directory sorts delta directory names in alphabetical order: First it lists the delete deltas
     * (delete_delta_x_y) then deltas (delta_x_y), both sorted by x, which is the min write id we're looking for.
     * Get the the minimum value of x.
     * @param directory holds information about the deltas we are compacting
     * @return the smallest min write id found in deltas and delete deltas
     */
    private static long getMinWriteID(AcidDirectory directory) {
      long minWriteID = Long.MAX_VALUE;
      for (AcidUtils.ParsedDelta delta : directory.getCurrentDirectories()) {
        minWriteID = Math.min(delta.getMinWriteId(), minWriteID);
        if (!delta.isDeleteDelta()) {
          break;
        }
      }
      return minWriteID;
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
    static void cleanupEmptyTableDir(HiveConf conf, String tmpTableName)
        throws IOException, HiveException {
      org.apache.hadoop.hive.ql.metadata.Table tmpTable = Hive.get().getTable(tmpTableName);
      if (tmpTable != null) {
        cleanupEmptyDir(conf, new Path(tmpTable.getSd().getLocation()));
      }
    }

    /**
     * Remove the directory if it's empty.
     * @param conf the Hive configuration
     * @param path path of the directory
     * @throws IOException if any IO error occurs
     */
    static void cleanupEmptyDir(HiveConf conf, Path path) throws IOException {
      FileSystem fs = path.getFileSystem(conf);
      try {
        if (!fs.listFiles(path, false).hasNext()) {
          fs.delete(path, true);
        }
      } catch (FileNotFoundException e) {
        // Ignore the case when the dir was already removed
        LOG.warn("Ignored exception during cleanup {}", path, e);
      }
    }

    /**
     * Remove the delta directories of aborted transactions.
     */
    static void removeFilesForMmTable(HiveConf conf, AcidDirectory dir) throws IOException {
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

    static void overrideConfProps(HiveConf conf, CompactionInfo ci, Map<String, String> properties) {
      Stream.of(properties, new StringableMap(ci.properties))
              .filter(Objects::nonNull)
              .flatMap(map -> map.entrySet().stream())
              .filter(entry -> entry.getKey().startsWith(COMPACTOR_PREFIX))
              .forEach(entry -> {
                String property = entry.getKey().substring(COMPACTOR_PREFIX.length());
                conf.set(property, entry.getValue());
              });
    }

    /**
     * Returns whether merge compaction must be enabled or not.
     * @param conf Hive configuration
     * @param directory the directory to be scanned
     * @param validWriteIdList list of valid write IDs
     * @param storageDescriptor storage descriptor of the underlying table
     * @return true, if merge compaction must be enabled
     */
    static boolean isMergeCompaction(HiveConf conf, AcidDirectory directory,
                                     ValidWriteIdList validWriteIdList,
                                     StorageDescriptor storageDescriptor) {
      return conf.getBoolVar(HiveConf.ConfVars.HIVE_MERGE_COMPACTION_ENABLED)
              && !hasDeleteOrAbortedDirectories(directory, validWriteIdList)
              && storageDescriptor.getOutputFormat().equalsIgnoreCase("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
    }

    /**
     * Scan a directory for delete deltas or aborted directories.
     * @param directory the directory to be scanned
     * @param validWriteIdList list of valid write IDs
     * @return true, if delete or aborted directory found
     */
    static boolean hasDeleteOrAbortedDirectories(AcidDirectory directory, ValidWriteIdList validWriteIdList) {
      if (!directory.getCurrentDirectories().isEmpty()) {
        final long minWriteId = validWriteIdList.getMinOpenWriteId() == null ? 1 : validWriteIdList.getMinOpenWriteId();
        final long maxWriteId = validWriteIdList.getHighWatermark();
        return directory.getCurrentDirectories().stream()
                .filter(AcidUtils.ParsedDeltaLight::isDeleteDelta)
                .filter(delta -> delta.getMinWriteId() >= minWriteId)
                .anyMatch(delta -> delta.getMaxWriteId() <= maxWriteId) || !directory.getAbortedDirectories().isEmpty();
      }
      return true;
    }

    /**
     * Collect the list of all bucket file paths, which belong to the same bucket Id. This method scans all the base
     * and delta dirs.
     * @param conf hive configuration, must be not null
     * @param dir the root directory of delta dirs
     * @param includeBaseDir true, if the base directory should be scanned
     * @param isMm
     * @return map of bucket ID -> bucket files
     * @throws IOException an error happened during the reading of the directory/bucket file
     */
    private static Map<Integer, List<Reader>> matchBucketIdToBucketFiles(HiveConf conf, AcidDirectory dir,
                                                                       boolean includeBaseDir, boolean isMm) throws IOException {
      Map<Integer, List<Reader>> result = new HashMap<>();
      if (includeBaseDir && dir.getBaseDirectory() != null) {
        getBucketFiles(conf, dir.getBaseDirectory(), isMm, result);
      }
      for (AcidUtils.ParsedDelta deltaDir : dir.getCurrentDirectories()) {
        Path deltaDirPath = deltaDir.getPath();
        getBucketFiles(conf, deltaDirPath, isMm, result);
      }
      return result;
    }

    /**
     * Collect the list of all bucket file paths, which belong to the same bucket Id. This method checks only one
     * directory.
     * @param conf hive configuration, must be not null
     * @param dirPath the directory to be scanned.
     * @param isMm collect bucket files fron insert only directories
     * @param bucketIdToBucketFilePath the result of the scan
     * @throws IOException an error happened during the reading of the directory/bucket file
     */
    private static void getBucketFiles(HiveConf conf, Path dirPath, boolean isMm, Map<Integer, List<Reader>> bucketIdToBucketFilePath) throws IOException {
      FileSystem fs = dirPath.getFileSystem(conf);
      FileStatus[] fileStatuses =
              fs.listStatus(dirPath, isMm ? AcidUtils.originalBucketFilter : AcidUtils.bucketFileFilter);
      for (FileStatus f : fileStatuses) {
        final Path fPath = f.getPath();
        Matcher matcher = isMm ? AcidUtils.LEGACY_BUCKET_DIGIT_PATTERN
                .matcher(fPath.getName()) : AcidUtils.BUCKET_PATTERN.matcher(fPath.getName());
        if (!matcher.find()) {
          String errorMessage = String
                  .format("Found a bucket file matching the bucket pattern! %s Matcher=%s", fPath.toString(),
                          matcher.toString());
          LOG.error(errorMessage);
          throw new IllegalArgumentException(errorMessage);
        }
        int bucketNum = matcher.groupCount() > 0 ? Integer.parseInt(matcher.group(1)) : Integer.parseInt(matcher.group());
        bucketIdToBucketFilePath.computeIfAbsent(bucketNum, ArrayList::new);
        Reader reader = OrcFile.createReader(fs, fPath);
        bucketIdToBucketFilePath.computeIfPresent(bucketNum, (k, v) -> v).add(reader);
      }
    }

    /**
     * Generate output path for compaction. This can be used to generate delta or base directories.
     * @param conf hive configuration, must be non-null
     * @param writeIds list of valid write IDs
     * @param isBaseDir if base directory path should be generated
     * @param sd the resolved storadge descriptor
     * @return output path, always non-null
     */
    static Path getCompactionOutputDirPath(HiveConf conf, ValidWriteIdList writeIds, boolean isBaseDir,
                                           StorageDescriptor sd) {
      long minOpenWriteId = writeIds.getMinOpenWriteId() == null ? 1 : writeIds.getMinOpenWriteId();
      long highWatermark = writeIds.getHighWatermark();
      long compactorTxnId = Compactor.getCompactorTxnId(conf);
      AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf).writingBase(isBaseDir)
              .writingDeleteDelta(false).isCompressed(false).minimumWriteId(minOpenWriteId)
              .maximumWriteId(highWatermark).statementId(-1).visibilityTxnId(compactorTxnId);
      return AcidUtils.baseOrDeltaSubdirPath(new Path(sd.getLocation()), options);
    }

    /**
     * Merge ORC files from base/delta directories. If the directories contains multiple buckets, the result will also
     * contain the same amount.
     * @param conf hive configuration
     * @param includeBaseDir if base directory should be scanned for orc files
     * @param dir the root directory of the table/partition
     * @param outputDirPath the result directory path
     * @param isMm merge orc files from insert only tables
     * @throws IOException error occurred during file operation
     */
    static boolean mergeOrcFiles(HiveConf conf, boolean includeBaseDir, AcidDirectory dir,
                              Path outputDirPath, boolean isMm) throws IOException {
      Map<Integer, List<Reader>> bucketIdToBucketFiles = matchBucketIdToBucketFiles(conf, dir, includeBaseDir, isMm);
      OrcFileMerger fileMerger = new OrcFileMerger(conf);
      for (Map.Entry<Integer, List<Reader>> e : bucketIdToBucketFiles.entrySet()) {
        fileMerger.checkCompatibility(e.getValue());
      }
      boolean isCompatible = true;
      for (Map.Entry<Integer, List<Reader>> e : bucketIdToBucketFiles.entrySet()) {
        isCompatible &= fileMerger.checkCompatibility(e.getValue());
      }
      if (isCompatible) {
        for (Map.Entry<Integer, List<Reader>> e : bucketIdToBucketFiles.entrySet()) {
          Path path = isMm ? new Path(outputDirPath, String.format(AcidUtils.LEGACY_FILE_BUCKET_DIGITS,
                  e.getKey()) + "_0") : new Path(outputDirPath, AcidUtils.BUCKET_PREFIX + String.format(AcidUtils.BUCKET_DIGITS,
                  e.getKey()));
          fileMerger.mergeFiles(e.getValue(), path);
        }
        return true;
      } else {
        return false;
      }
    }
  }
}
