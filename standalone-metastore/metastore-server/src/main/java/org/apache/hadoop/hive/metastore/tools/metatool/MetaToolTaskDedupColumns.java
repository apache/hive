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

package org.apache.hadoop.hive.metastore.tools.metatool;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hive.metastore.tools.MetaToolObjectStore;

class MetaToolTaskDedupColumns extends MetaToolTask {
  @Override
  void execute() {
    String[] params = getCl().getDedupColumnsParams();
    String catalogFilter = params.length > 0 ? params[0] : null;
    String dbFilter = params.length > 1 ? params[1] : null;
    String tableFilter = params.length > 2 ? params[2] : null;
    boolean isDryRun = getCl().isDryRun();
    boolean isVerbose = getCl().isVerbose();
    
    final AtomicReference<String> progress = new AtomicReference<>();
    AtomicBoolean stopped = new AtomicBoolean(false);
    Thread daemon = null;
    if (isVerbose) {
      daemon = new Thread(() -> {
        while (!stopped.get()) {
          try {
            Thread.sleep(30 * 1000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
          String message = progress.get();
          if (message != null) {
            System.out.println(message);
          }
        }
      });
      daemon.setDaemon(true);
      daemon.start();
    }
    MetaToolObjectStore.DedupColumnsResult result;
    try {
      result = getObjectStore().dedupColumns(catalogFilter, dbFilter, tableFilter, progress, isDryRun, isVerbose);
      printSummary(result, isDryRun, isVerbose);
    } finally {
      if (daemon != null) {
        stopped.set(true);
        daemon.interrupt();
      }
    }
    if (daemon != null) {
      stopped.set(true);
      daemon.interrupt();
    }
    if (result.getException() != null) {
      throw new IllegalStateException("HiveMetaTool: failed to de-duplicate column descriptors for all tables",
          result.getException());
    }
  }

  private void printSummary(MetaToolObjectStore.DedupColumnsResult result, boolean isDryRun, boolean isVerbose) {
    System.out.println(isDryRun ?
        "Dry run of -dedupColumns.." :
        "De-duplicated column descriptors successfully.");
    System.out.println("Tables scanned: " + result.getTablesScanned());
    System.out.println("Tables with duplicate column descriptors: " + result.getTablesWithDuplicates());
    System.out.println("Partition storage descriptors " + (isDryRun ? "to update" : "updated") + ": "
        + result.getStorageDescriptorsUpdated());
    System.out.println("Column descriptors " + (isDryRun ? "to remove" : "removed") + ": "
        + result.getColumnDescriptorsRemoved());
    if (isVerbose) {
      for (String detail : result.getDetails()) {
        System.out.println(detail);
      }
    }
  }
}
