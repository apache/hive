/**
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

package org.apache.hadoop.hive.ql.hooks;

import java.util.HashMap;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Implementation of a pre AND post execute hook that updates the access times
 * for all the inputs.
 *
 * It is required that this hook is put as the last pre-hook and the first
 * post-hook. Invoked as pre-hook, it will start a background thread to update
 * update time of all partitions and tables in the input set. Invoked as
 * post-hook, it will wait the background thread to finish. And fail the query
 * if the background thread fails.
 */
public class FbUpdateInputAccessTimeHook implements ExecuteWithHookContext {
  static final private Log LOG = LogFactory
      .getLog("hive.ql.hooks.FbUpdateInputAccessTimeHook");

  private Hive db;
  static private Object staticLock = new Object();
  static private HookThread hookThread = null;
  static private HookContext lastHookContext = null;

  @Override
  public void run(HookContext hookContext) throws Exception {
    assert (hookContext != null);

    // if no input, there is no need to start the backgrond thread.
    if (hookContext.getInputs() == null || hookContext.getInputs().isEmpty()) {
      return;
    }

    // This race condition should never happen. But since we use static
    // member to keep some global states, we lock it in case it happens
    // because of a bug, we won't produce unpredictable results
    synchronized (staticLock) {
      // there is no flag to determine it is pre-hook or post-hook.
      // We just simply make the assumption that if one hook context
      // is passed again, it is post hook.
      if (lastHookContext == hookContext) {
        lastHookContext = null;
        runPosthook(hookContext);
      } else if (lastHookContext != null || hookThread != null) {
        // If we don't forget to put the hook in post-execution hooks,
        // likely the previous task failed so that post-hook didn't have
        // chance to be executed.
        //
        // Ideally this error message should print to SessionState's error
        // stream if assigned. However, it is not in HookContext.
        // We use standard error message for now.
        System.err.println(
            "WARNING: FbUpdateInputAccessTimeHook doesn't start with a clear " +
            "state. Ignore this message if the previous query failed. If " +
            "previous task succeeded, check whether " +
            "FbUpdateInputAccessTimeHook is among the post-execution hooks");

        if (hookThread != null) {
          System.err.println("Waiting for pending background thread of " +
              "FbUpdateInputAccessTimeHook to finish...");
          hookThread.join();
          System.err.println("Background thread of FbUpdateInputAccessTimeHook" +
                             " finished.");
          hookThread = null;
        }
        lastHookContext = hookContext;
        runPrehook(hookContext);
      } else {
        if (!hookContext.getCompleteTaskList().isEmpty()) {
          throw new HiveException(
              "FbUpdateInputAccessTimeHook is not a part of "
                  + "pre-execution hook?");
        }
        lastHookContext = hookContext;
        runPrehook(hookContext);
      }
    }
  }

  private void runPrehook(HookContext hookContext) {
    LOG.info("run as pre-execution hook");
    hookThread = new HookThread(hookContext.getConf(), hookContext.getInputs(),
        hookContext.getOutputs());
    hookThread.start();
  }

  private void runPosthook(HookContext hookContext) throws HiveException {
    LOG.info("run as post-execution hook");
    if (hookThread != null) {
      HookThread pendingThread = hookThread;
      try {
        pendingThread.join();
      } catch (InterruptedException e) {
        throw new HiveException(
            "Background thread in FbUpdateInputAccessTimeHook failed", e);
      } finally {
        hookThread = null;
      }

      if (!pendingThread.isSuccessful()) {
        if (pendingThread.getHiveException() != null) {
          throw new HiveException("FbUpdateInputAccessTimeHook failed",
              pendingThread.getHiveException());
        } else if (pendingThread.getInvalidOperationException() != null) {
          throw new HiveException("FbUpdateInputAccessTimeHook failed",
              pendingThread.getInvalidOperationException());
        } else {
          throw new HiveException("FbUpdateInputAccessTimeHook failed with "
              + "Unhandled Exception.");
        }
      }
    } else {
      throw new HiveException(
          "FbUpdateInputAccessTimeHook is not one of pre-execution hook, "
              + "but it is one of the post-execution hook.");
    }
  }

  /**
   * class for the background thread
   *
   * @author sdong
   *
   */
  class HookThread extends Thread {
    Set<ReadEntity> inputs;
    Set<WriteEntity> outputs;
    HiveConf hiveConf;
    boolean success;

    HiveException hiveException;
    InvalidOperationException invalidOperationException;

    HookThread(HiveConf hiveConf, Set<ReadEntity> inputs,
        Set<WriteEntity> outputs) {
      this.hiveConf = hiveConf;
      this.inputs = inputs;
      this.outputs = outputs;
      success = false;
    }

    public boolean isSuccessful() {
      return success;
    }

    public HiveException getHiveException() {
      return hiveException;
    }

    public InvalidOperationException getInvalidOperationException() {
      return invalidOperationException;
    }

    private void updateTableAccessTime(HashMap<String, Table> tableMap,
        Table table, int lastAccessTime) throws HiveException,
        InvalidOperationException {
      if (!tableMap.containsKey(table.getTableName())) {
        Table t = db.getTable(table.getTableName());
        t.setLastAccessTime(lastAccessTime);
        db.alterTable(t.getTableName(), t);
        tableMap.put(table.getTableName(), t);
      }
    }

    public void run() {
      try {
        if (db == null) {
          try {
            db = Hive.get(hiveConf);
          } catch (HiveException e) {
            // ignore
            db = null;
            return;
          }
        }

        int lastAccessTime = (int) (System.currentTimeMillis() / 1000);

        HashMap<String, Table> tableMap = new HashMap<String, Table> ();

        for (ReadEntity re : inputs) {
          // Set the last query time
          ReadEntity.Type typ = re.getType();
          switch (typ) {
          // It is possible that read and write entities contain a old
          // version
          // of the object, before it was modified by StatsTask.
          // Get the latest versions of the object
          case TABLE: {
            updateTableAccessTime(tableMap, re.getTable(),
                lastAccessTime);
            break;
          }
          case PARTITION: {
            Partition p = re.getPartition();
            updateTableAccessTime(tableMap, p.getTable(), lastAccessTime);
            // table already in the map after updating tables' access time
            Table t = tableMap.get(p.getTable().getTableName());
            p = db.getPartition(t, p.getSpec(), false);
            p.setLastAccessTime(lastAccessTime);
            db.alterPartition(t.getTableName(), p);
            break;
          }
          default:
            // ignore dummy inputs
            break;
          }
        }
        success = true;
      } catch (HiveException e) {
        hiveException = e;
      } catch (InvalidOperationException e) {
        invalidOperationException = e;
      }
    }
  }
}
