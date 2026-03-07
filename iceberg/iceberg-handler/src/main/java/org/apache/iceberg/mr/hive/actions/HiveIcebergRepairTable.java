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

package org.apache.iceberg.mr.hive.actions;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.StreamSupport;
import org.apache.hadoop.hive.ql.ddl.misc.msck.MsckDesc;
import org.apache.hadoop.hive.ql.ddl.misc.msck.MsckResult;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repairs an Iceberg table by removing dangling file references.
 * <p>
 * Detects and removes references to data files that are missing from the filesystem
 * but still referenced in metadata. Supports dry-run mode and parallel execution.
 */
public class HiveIcebergRepairTable {

  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergRepairTable.class);
  private static final int DEFAULT_NUM_THREADS = 4;

  private final Table table;
  private final MsckDesc desc;
  private final int numThreads;

  public HiveIcebergRepairTable(Table table, MsckDesc desc) {
    this(table, desc, DEFAULT_NUM_THREADS);
  }

  public HiveIcebergRepairTable(Table table, MsckDesc desc, int numThreads) {
    this.table = table;
    this.desc = desc;
    this.numThreads = numThreads;
  }

  /**
   * Executes the repair operation within a provided transaction.
   *
   * @param transaction the Iceberg transaction to use
   * @return repair result containing number of issues fixed and log message
   * @throws IOException if metadata validation or file check fails
   */
  public MsckResult execute(Transaction transaction) throws IOException {
    List<String> missingFiles = getMissingFiles();

    if (missingFiles.isEmpty()) {
      String msg = "No missing files detected";
      LOG.info(msg);
      return new MsckResult(0, msg, new java.util.ArrayList<>());
    } else if (desc.isRepair()) {
      // Only commit changes if not in dry-run mode
      DeleteFiles deleteFiles = transaction.newDelete();
      for (String path : missingFiles) {
        deleteFiles.deleteFile(path);
      }
      deleteFiles.commit();
    }

    String summaryMsg = desc.isRepair() ?
        "Removed %d dangling file reference(s)".formatted(missingFiles.size()) :
        "Would remove %d dangling file reference(s)".formatted(missingFiles.size());
    LOG.info(summaryMsg);

    String detailedMsg = desc.isRepair() ?
        "Iceberg table repair completed: %s".formatted(summaryMsg) :
        "Iceberg table repair (dry-run): %s".formatted(summaryMsg);

    return new MsckResult(missingFiles.size(), detailedMsg, missingFiles);
  }

  /**
   * Executes the repair operation, automatically creating and committing a transaction.
   *
   * @return repair result containing removed files and statistics
   * @throws IOException if metadata validation or file check fails
   */
  public MsckResult execute() throws IOException {
    Transaction transaction = table.newTransaction();
    MsckResult result = execute(transaction);
    if (desc.isRepair() && result.numIssues() > 0) {
      transaction.commitTransaction();
    }
    return result;
  }

  /**
   * Finds all missing data files by checking their physical existence in parallel.
   *
   * @return list of file paths for missing data files
   * @throws IOException if the file check operation fails or is interrupted
   */
  private List<String> getMissingFiles() throws IOException {
    try (ExecutorService executorService = IcebergTableUtil.newFixedThreadPool(
        "repair-table-" + table.name(), numThreads);
         CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
      return executorService.submit(() ->
          StreamSupport.stream(fileScanTasks.spliterator(), true)
              .map(task -> task.file().location())
              .filter(path -> !table.io().newInputFile(path).exists())
              .toList()
      ).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while checking for missing files", e);

    } catch (ExecutionException e) {
      throw new IOException("Failed to check for missing files: " + e.getMessage(), e);
    }
  }
}
