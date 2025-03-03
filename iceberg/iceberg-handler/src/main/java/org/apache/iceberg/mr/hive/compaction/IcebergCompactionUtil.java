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

package org.apache.iceberg.mr.hive.compaction;

import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorContext;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class IcebergCompactionUtil {

  private IcebergCompactionUtil() {

  }

  /**
   * This method implements a common filter that is used in several places in Iceberg compaction code.
   * Its aim is to determine if the provided file needs to be handled when compacting a partition whose path is equal to
   * the provided partitionPath. Returns true when one of the following conditions is true, otherwise returns false:
   *  1. table is unpartitioned
   *  2. partitionPath is null and the file belongs to the non-latest partition spec
   *  3. partitionPath is not null and the file belongs to the partition whose path is the partitionPath
   * @param table the iceberg table
   * @param partitionPath partition path
   * @param file Data or Delete file
   */
  public static boolean shouldIncludeForCompaction(Table table, String partitionPath, ContentFile<?> file) {
    return !table.spec().isPartitioned() ||
        partitionPath == null && file.specId() != table.spec().specId() ||
        partitionPath != null &&
            table.specs().get(file.specId()).partitionToPath(file.partition()).equals(partitionPath);
  }

  public static boolean shouldIncludeForCompaction(Table table, String partitionPath, ContentFile<?> file,
      long fileSizeThreshold) {
    return shouldIncludeForCompaction(table, partitionPath, file) &&
        (fileSizeThreshold == -1 || file.fileSizeInBytes() < fileSizeThreshold);
  }

  /**
   * Returns table's list of data files as following:
   *  1. If the table is unpartitioned, returns all data files.
   *  2. If partitionPath is not provided, returns all data files that belong to the non-latest partition spec.
   *  3. If partitionPath is provided, returns all data files that belong to the corresponding partition.
   * @param table the iceberg table
   * @param partitionPath partition path
   */
  public static List<DataFile> getDataFiles(Table table, String partitionPath, long fileSizeThreshold) {
    CloseableIterable<FileScanTask> fileScanTasks =
        table.newScan().useSnapshot(table.currentSnapshot().snapshotId()).ignoreResiduals().planFiles();
    CloseableIterable<FileScanTask> filteredFileScanTasks =
        CloseableIterable.filter(fileScanTasks, t -> {
          DataFile file = t.asFileScanTask().file();
          return shouldIncludeForCompaction(table, partitionPath, file, fileSizeThreshold);
        });
    return Lists.newArrayList(CloseableIterable.transform(filteredFileScanTasks, t -> t.file()));
  }

  /**
   * Returns table's list of delete files as following:
   *  1. If the table is unpartitioned, returns all delete files.
   *  2. If partitionPath is not provided, returns all delete files that belong to the non-latest partition spec.
   *  3. If partitionPath is provided, returns all delete files that belong to corresponding partition.
   * @param table the iceberg table
   * @param partitionPath partition path
   */
  public static List<DeleteFile> getDeleteFiles(Table table, String partitionPath) {
    Table deletesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.POSITION_DELETES);
    CloseableIterable<ScanTask> deletesScanTasks = deletesTable.newBatchScan().planFiles();
    CloseableIterable<ScanTask> filteredDeletesScanTasks =
        CloseableIterable.filter(deletesScanTasks, t -> {
          DeleteFile file = ((PositionDeletesScanTask) t).file();
          return shouldIncludeForCompaction(table, partitionPath, file);
        });
    return Lists.newArrayList(CloseableIterable.transform(filteredDeletesScanTasks,
        t -> ((PositionDeletesScanTask) t).file()));
  }

  /**
   * Returns target file size as following:
   * In case of Minor compaction:
   *  1. When COMPACTION_FILE_SIZE_THRESHOLD is defined, returns it.
   *  2. Otherwise, calculates the file size threshold as:
   *       COMPACTION_FILE_SIZE_THRESHOLD * TableProperties.HIVE_ICEBERG_COMPACTION_TARGET_FILE_SIZE
   *     This makes Compaction evaluator consider data files with size less than file size threshold as undersized
   *     segment files eligible for minor compaction (as per Amoro compaction evaluator, which is minor compaction
   *     in Hive).
   * In case of Major compaction returns -1.
   * @param ci the compaction info
   * @param conf Hive configuration
   */
  public static long getFileSizeThreshold(CompactionInfo ci, HiveConf conf) {
    switch (ci.type) {
      case MINOR:
        return Optional.ofNullable(ci.getProperty(CompactorContext.COMPACTION_FILE_SIZE_THRESHOLD))
            .map(HiveConf::toSizeBytes).orElse((long) (HiveConf.toSizeBytes(HiveConf.getVar(conf,
                HiveConf.ConfVars.HIVE_ICEBERG_COMPACTION_TARGET_FILE_SIZE)) *
                TableProperties.SELF_OPTIMIZING_MIN_TARGET_SIZE_RATIO_DEFAULT));
      case MAJOR:
        return -1;
      default:
        throw new RuntimeException(String.format("Unsupported compaction type %s", ci.type));
    }
  }

  /**
   * Returns target file size as following:
   *  1. When COMPACTION_FILE_SIZE_THRESHOLD is defined, calculates target size as:
   *       COMPACTION_FILE_SIZE_THRESHOLD / TableProperties.SELF_OPTIMIZING_MIN_TARGET_SIZE_RATIO_DEFAULT
   *     This makes Compaction evaluator consider data files with size less than COMPACTION_FILE_SIZE_THRESHOLD as
   *     fragments eligible for major (as per Amoro definition, minor compaction in Hive) compaction.
   *  2. Otherwise, returns the default HIVE_ICEBERG_COMPACTION_TARGET_FILE_SIZE from HiveConf.
   * @param ci the compaction info
   * @param conf Hive configuration
   */
  public static long getTargetFileSize(CompactionInfo ci, HiveConf conf) {
    return Optional.ofNullable(ci.getProperty(CompactorContext.COMPACTION_FILE_SIZE_THRESHOLD))
        .map(HiveConf::toSizeBytes).map(x -> (long) (x / TableProperties.SELF_OPTIMIZING_MIN_TARGET_SIZE_RATIO_DEFAULT))
        .orElse(HiveConf.toSizeBytes(HiveConf.getVar(conf,
            HiveConf.ConfVars.HIVE_ICEBERG_COMPACTION_TARGET_FILE_SIZE)));
  }
}
