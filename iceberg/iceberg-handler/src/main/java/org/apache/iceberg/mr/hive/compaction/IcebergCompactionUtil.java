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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.compaction;

import java.util.List;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class IcebergCompactionUtil {

  private IcebergCompactionUtil() {

  }

  /**
   * This method implements a common filter that is used in several places in Iceberg compaction code.
   * Its aim is to determine if the provided file needs to be handled when compacting a partition whose path is equal to
   * the provided partitionName. Returns true when one of the following conditions is true, otherwise returns false:
   *  1. table is unpartitioned
   *  2. partitionName is null and the file belongs to the non-latest partition spec
   *  3. partitionName is not null and the file belongs to the partition of that name
   * @param table the iceberg table
   * @param partitionName partition name
   * @param file Data or Delete file
   */
  public static boolean shouldIncludeForCompaction(Table table, String partitionName, ContentFile<?> file) {
    return !table.spec().isPartitioned() ||
        partitionName == null && file.specId() != table.spec().specId() ||
        partitionName != null &&
            IcebergTableUtil.toPartitionName(table.specs().get(file.specId()), file.partition())
                .equals(partitionName);
  }

  public static boolean shouldIncludeForCompaction(Table table, String partitionName, ContentFile<?> file,
      long fileSizeThreshold) {
    return shouldIncludeForCompaction(table, partitionName, file) &&
        (fileSizeThreshold == -1 || file.fileSizeInBytes() < fileSizeThreshold);
  }

  /**
   * Returns table's list of data files as following:
   *  1. If the table is unpartitioned, returns all data files.
   *  2. If partitionName is not provided, returns all data files that belong to the non-latest partition spec.
   *  3. If partitionName is provided, returns all data files that belong to the corresponding partition.
   * @param table the iceberg table
   * @param partitionName partition name
   */
  public static List<DataFile> getDataFiles(Table table, Long snapshotId, String partitionName,
      long fileSizeThreshold) {
    CloseableIterable<ScanTask> scanTasks =
        table.newBatchScan().useSnapshot(snapshotId).planFiles();
    CloseableIterable<ScanTask> filteredScanTasks =
        CloseableIterable.filter(scanTasks, t -> {
          DataFile file = t.asFileScanTask().file();
          return shouldIncludeForCompaction(table, partitionName, file, fileSizeThreshold);
        });
    return Lists.newArrayList(CloseableIterable.transform(filteredScanTasks, t -> t.asFileScanTask().file()));
  }

  /**
   * Returns table's list of delete files as following:
   *  1. If the table is unpartitioned, returns all delete files.
   *  2. If partitionName is not provided, returns all delete files that belong to the non-latest partition spec.
   *  3. If partitionName is provided, returns all delete files that belong to corresponding partition.
   * @param table the iceberg table
   * @param partitionName partition name
   */
  public static List<DeleteFile> getDeleteFiles(Table table, Long snapshotId, String partitionName) {
    Table deletesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.POSITION_DELETES);
    CloseableIterable<ScanTask> deletesScanTasks = deletesTable.newBatchScan().useSnapshot(snapshotId).planFiles();
    CloseableIterable<ScanTask> filteredDeletesScanTasks =
        CloseableIterable.filter(deletesScanTasks, t -> {
          DeleteFile file = ((PositionDeletesScanTask) t).file();
          return shouldIncludeForCompaction(table, partitionName, file);
        });
    return Lists.newArrayList(CloseableIterable.transform(filteredDeletesScanTasks,
        t -> ((PositionDeletesScanTask) t).file()));
  }
}
