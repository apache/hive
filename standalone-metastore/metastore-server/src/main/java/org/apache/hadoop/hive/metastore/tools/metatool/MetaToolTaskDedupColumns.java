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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.DeadlineException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.tools.ColumnDeduplicator;
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
    
    Thread daemon = null;
    MetaToolObjectStore.DedupColumnsResult result = null;
    AtomicReference<String> progress = new AtomicReference<>();
    AtomicBoolean stopped = new AtomicBoolean(false);
    long timeoutMs = getCl().getDedupColumnsTimeoutSeconds() != null ?
        getCl().getDedupColumnsTimeoutSeconds() * 1000L :
        MetastoreConf.getTimeVar(getObjectStore().getConf(), MetastoreConf.ConfVars.DEDUP_COLUMNS_TIMEOUT,
            TimeUnit.MILLISECONDS);
    Deadline.registerIfNot(timeoutMs);
    boolean timerStarted = false;
    try {
      timerStarted = Deadline.startTimer("dedupColumns");
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
      result = getObjectStore().dedupColumns(catalogFilter, dbFilter, tableFilter, progress, isDryRun, isVerbose,
          getCl().isClearMarker());
    } catch (Throwable ex) {
      ex.printStackTrace(System.err);
    } finally {
      if (daemon != null) {
        stopped.set(true);
        daemon.interrupt();
      }
      if (timerStarted) {
        try {
          Deadline.stopTimer();
        } catch (MetaException ignored) {
        }
      }
    }

    if (result != null) {
      printSummary(result, isDryRun, isVerbose);
      if (result.getException() != null) {
        DeadlineException deadline = findDeadlineCause(result.getException());
        if (deadline != null) {
          long timeoutSeconds = Math.max(1, timeoutMs / 1000);
          System.err.println("HiveMetaTool -dedupColumns timed out after " + timeoutSeconds
              + " second(s). The configured limit is -timeout <seconds> or "
              + MetastoreConf.ConfVars.DEDUP_COLUMNS_TIMEOUT.getVarname() + " (default in minutes).");
          System.err.println("Increase the limit with -timeout <seconds> or set "
              + MetastoreConf.ConfVars.DEDUP_COLUMNS_TIMEOUT.getVarname()
              + " to a larger value (minutes), then re-run. Tables already processed can be skipped if marked "
              + "with " + ColumnDeduplicator.COLUMN_DEDUPLICATOR_MARKER + ".");
          return;
        }
        throw new IllegalStateException("HiveMetaTool: failed to de-duplicate column descriptors for all tables",
            result.getException());
      }
    }
  }

  private static DeadlineException findDeadlineCause(Throwable ex) {
    for (Throwable t = ex; t != null; t = t.getCause()) {
      if (t instanceof DeadlineException) {
        return (DeadlineException) t;
      }
    }
    return null;
  }

  private void printSummary(MetaToolObjectStore.DedupColumnsResult result, boolean isDryRun, boolean isVerbose) {
    System.out.println(isDryRun ?
        "Dry run of -dedupColumns.." :
        "De-duplicated column descriptors..");
    System.out.println("Tables scanned: " + result.getTablesScanned());
    System.out.println("Tables with duplicate column descriptors: " + result.getTablesWithDuplicates());
    System.out.println("Partition storage descriptors " + (isDryRun ? "to update" : "updated") + ": "
        + result.getStorageDescriptorsUpdated());
    if (!isDryRun) {
      System.out.println("Column descriptors removed: " + result.getColumnDescriptorsRemoved());
      if (result.getTablesValidated() > 0) {
        System.out.println("Post-commit validation passed for " + result.getTablesValidated() + " table(s).");
      }
    }
    if (isVerbose) {
      for (String detail : result.getDetails()) {
        System.out.println(detail);
      }
    }
  }
}
