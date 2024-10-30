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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class IcebergCompactionUtil {

  private IcebergCompactionUtil() {

  }

  /**
   * Returns true if:
   *  1. When the table is unpartitioned.
   *  2. When partitionPath is not provided and the file belongs to the non-latest partition spec.
   *  3. When partitionPath is provided and the file belongs to the same partition.
   * @param table the iceberg table
   * @param partitionPath partition path
   * @param file Data or Delete file
   */
  public static boolean doesFileMatchPartition(Table table, String partitionPath, ContentFile<?> file) {
    return !table.spec().isPartitioned() ||
        partitionPath == null && file.specId() != table.spec().specId() ||
        partitionPath != null &&
            table.specs().get(file.specId()).partitionToPath(file.partition()).equals(partitionPath);
  }

  /**
   * Returns table's list of data files as following:
   *  1. If the table is unpartitioned, returns all data files.
   *  2. If partitionPath is not provided, returns all data files that belong to the non-latest partition spec.
   *  3. If partitionPath is provided, returns all data files that belong to the corresponding partition.
   * @param table the iceberg table
   * @param partitionPath partition path
   */
  public static List<DataFile> getDataFiles(Table table, String partitionPath) {
    CloseableIterable<FileScanTask> fileScanTasks =
        table.newScan().useSnapshot(table.currentSnapshot().snapshotId()).ignoreResiduals().planFiles();
    CloseableIterable<FileScanTask> filteredFileScanTasks =
        CloseableIterable.filter(fileScanTasks, t -> {
          DataFile file = t.asFileScanTask().file();
          return doesFileMatchPartition(table, partitionPath, file);
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
          return doesFileMatchPartition(table, partitionPath, file);
        });
    return Lists.newArrayList(CloseableIterable.transform(filteredDeletesScanTasks,
        t -> ((PositionDeletesScanTask) t).file()));
  }
}
