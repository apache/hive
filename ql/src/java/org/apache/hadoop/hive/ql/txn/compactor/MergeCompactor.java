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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

final class MergeCompactor extends QueryCompactor {

  private static final Logger LOG = LoggerFactory.getLogger(MergeCompactor.class.getName());

  @Override
  public boolean run(CompactorContext context) throws IOException, HiveException, InterruptedException {
    HiveConf hiveConf = context.getConf();
    Table table = context.getTable();
    AcidDirectory dir = context.getAcidDirectory();
    ValidWriteIdList writeIds = context.getValidWriteIdList();
    StorageDescriptor storageDescriptor = context.getSd();
    CompactionInfo compactionInfo = context.getCompactionInfo();

    if (isMergeCompaction(hiveConf, dir, storageDescriptor)) {
      // Only inserts happened, it is much more performant to merge the files than running a query
      Path outputDirPath = getOutputDirPath(hiveConf, writeIds,
              compactionInfo.isMajorCompaction(), storageDescriptor);
      try {
        return mergeFiles(hiveConf, compactionInfo.isMajorCompaction(),
                dir, outputDirPath, AcidUtils.isInsertOnlyTable(table.getParameters()));
      } catch (Throwable t) {
        // Error handling, just delete the output directory,
        // and fall back to query based compaction.
        FileSystem fs = outputDirPath.getFileSystem(hiveConf);
        if (fs.exists(outputDirPath)) {
          fs.delete(outputDirPath, true);
        }
        return false;
      }
    } else {
      return false;
    }
  }

  /**
   * Returns whether merge compaction must be enabled or not.
   * @param conf Hive configuration
   * @param directory the directory to be scanned
   * @param storageDescriptor storage descriptor of the underlying table
   * @return true, if merge compaction must be enabled
   */
  private boolean isMergeCompaction(HiveConf conf, AcidDirectory directory,
                                   StorageDescriptor storageDescriptor) {
    return HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_MERGE_COMPACTION_ENABLED)
            && storageDescriptor.getOutputFormat().equalsIgnoreCase("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
            && !hasDeleteOrAbortedDirectories(directory);
  }

  /**
   * Scan a directory for delete deltas or aborted directories.
   * @param directory the directory to be scanned
   * @return true, if delete or aborted directory found
   */
  private boolean hasDeleteOrAbortedDirectories(AcidDirectory directory) {
    if (!directory.getCurrentDirectories().isEmpty()) {
      return directory.getCurrentDirectories().stream()
              .anyMatch(AcidUtils.ParsedDeltaLight::isDeleteDelta) || !directory.getAbortedDirectories().isEmpty();
    }
    return true;
  }

  /**
   * Collect the list of all bucket file paths, which belong to the same bucket Id. This method scans all the base
   * and delta dirs.
   * @param conf hive configuration, must be not null
   * @param dir the root directory of delta dirs
   * @param includeBaseDir true, if the base directory should be scanned
   * @param isMm true, if the table is an insert only table
   * @return map of bucket ID -> bucket files
   * @throws IOException an error happened during the reading of the directory/bucket file
   */
  private Map<Integer, List<Reader>> matchBucketIdToBucketFiles(HiveConf conf, AcidDirectory dir,
                                                                       boolean includeBaseDir, boolean isMm) throws IOException {
    Map<Integer, List<Reader>> result = new HashMap<>();
    if (includeBaseDir && dir.getBaseDirectory() != null) {
      result.putAll(getBucketFiles(conf, dir.getBaseDirectory(), isMm));
    }
    for (AcidUtils.ParsedDelta deltaDir : dir.getCurrentDirectories()) {
      Path deltaDirPath = deltaDir.getPath();
      Map<Integer, List<Reader>> intermediateResult = getBucketFiles(conf, deltaDirPath, isMm);
      intermediateResult.forEach((k, v) -> result.merge(k, v, (v1, v2) -> {
        v1.addAll(v2);
        return v1;
      }));
    }
    return result;
  }

  /**
   * Collect the list of all bucket file paths, which belong to the same bucket Id. This method checks only one
   * directory.
   * @param conf hive configuration, must be not null
   * @param dirPath the directory to be scanned.
   * @param isMm collect bucket files fron insert only directories
   * @throws IOException an error happened during the reading of the directory/bucket file
   */
  private Map<Integer, List<Reader>> getBucketFiles(HiveConf conf, Path dirPath, boolean isMm) throws IOException {
    Map<Integer, List<Reader>> bucketIdToBucketFilePath = new HashMap<>();
    FileSystem fs = dirPath.getFileSystem(conf);
    FileStatus[] fileStatuses =
            fs.listStatus(dirPath, isMm ? AcidUtils.originalBucketFilter : AcidUtils.bucketFileFilter);
    for (FileStatus f : fileStatuses) {
      final Path fPath = f.getPath();
      Matcher matcher = isMm ? AcidUtils.LEGACY_BUCKET_DIGIT_PATTERN
              .matcher(fPath.getName()) : AcidUtils.BUCKET_PATTERN.matcher(fPath.getName());
      if (!matcher.find()) {
        String errorMessage = String
                .format("Found a bucket file which did not match the bucket pattern! %s Matcher=%s", fPath,
                        matcher);
        LOG.error(errorMessage);
        throw new IllegalArgumentException(errorMessage);
      }
      int bucketNum = matcher.groupCount() > 0 ? Integer.parseInt(matcher.group(1)) : Integer.parseInt(matcher.group());
      bucketIdToBucketFilePath.computeIfAbsent(bucketNum, ArrayList::new);
      Reader reader = OrcFile.createReader(fs, fPath);
      bucketIdToBucketFilePath.computeIfPresent(bucketNum, (k, v) -> v).add(reader);
    }
    return bucketIdToBucketFilePath;
  }

  /**
   * Generate output path for compaction. This can be used to generate delta or base directories.
   * @param conf hive configuration, must be non-null
   * @param writeIds list of valid write IDs
   * @param isBaseDir if base directory path should be generated
   * @param sd the resolved storadge descriptor
   * @return output path, always non-null
   */
  private Path getOutputDirPath(HiveConf conf, ValidWriteIdList writeIds, boolean isBaseDir,
                                         StorageDescriptor sd) {
    long minOpenWriteId = writeIds.getMinOpenWriteId() == null ? 1 : writeIds.getMinOpenWriteId();
    long highWatermark = writeIds.getHighWatermark();
    long compactorTxnId = Compactor.getCompactorTxnId(conf);
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
            .writingBase(isBaseDir).writingDeleteDelta(false)
            .isCompressed(false)
            .minimumWriteId(minOpenWriteId).maximumWriteId(highWatermark)
            .statementId(-1).visibilityTxnId(compactorTxnId);
    return AcidUtils.baseOrDeltaSubdirPath(new Path(sd.getLocation()), options);
  }

  /**
   * Merge files from base/delta directories. If the directories contains multiple buckets, the result will also
   * contain the same amount.
   * @param conf hive configuration
   * @param includeBaseDir if base directory should be scanned for orc files
   * @param dir the root directory of the table/partition
   * @param outputDirPath the result directory path
   * @param isMm merge orc files from insert only tables
   * @throws IOException error occurred during file operation
   */
  private boolean mergeFiles(HiveConf conf, boolean includeBaseDir, AcidDirectory dir,
                               Path outputDirPath, boolean isMm) throws IOException {
    Map<Integer, List<Reader>> bucketIdToBucketFiles = matchBucketIdToBucketFiles(conf, dir, includeBaseDir, isMm);
    OrcFileMerger fileMerger = new OrcFileMerger(conf);
    boolean isCompatible = bucketIdToBucketFiles.values().stream().allMatch(fileMerger::checkCompatibility);
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
