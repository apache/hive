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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.metrics.AcidMetricService;
import org.apache.hadoop.hive.metastore.metrics.PerfLogger;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorUtil.ThrowingRunnable;
import org.apache.hadoop.hive.ql.txn.compactor.handler.Handler;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.apache.hadoop.hive.metastore.HMSHandler.getMSForConf;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

/**
 * A runnable class which takes in handler and cleaning request and deletes the files
 * according to the cleaning request.
 */
public class FSRemover implements ThrowingRunnable<MetaException> {
  private static final Logger LOG = LoggerFactory.getLogger(FSRemover.class);
  private final Handler handler;
  private final CleaningRequest cr;
  private final ReplChangeManager replChangeManager;

  public FSRemover(Handler handler, CleaningRequest cr) throws MetaException {
    this.handler = handler;
    this.cr = cr;
    this.replChangeManager = ReplChangeManager.getInstance(this.handler.getConf());
  }

  @Override
  public void run() throws MetaException {
    PerfLogger perfLogger = PerfLogger.getPerfLogger(false);
    try {
      if (handler.isMetricsEnabled()) {
        perfLogger.perfLogBegin(FSRemover.class.getName(), cr.getCleanerMetric());
      }
      handler.beforeExecutingCleaningRequest(cr);
      Callable<List<Path>> cleanUpTask;
      cleanUpTask = () -> removeFiles(cr);

      Ref<List<Path>> removedFiles = Ref.from(new ArrayList<>());
      if (CompactorUtil.runJobAsSelf(cr.runAs())) {
        removedFiles.value = cleanUpTask.call();
      } else {
        LOG.info("Cleaning as user {} for {}", cr.runAs(), cr.getFullPartitionName());
        UserGroupInformation ugi = UserGroupInformation.createProxyUser(cr.runAs(),
                UserGroupInformation.getLoginUser());
        try {
          ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            removedFiles.value = cleanUpTask.call();
            return null;
          });
        } finally {
          try {
            FileSystem.closeAllForUGI(ugi);
          } catch (IOException exception) {
            LOG.error("Could not clean up file-system handles for UGI: {} for {}", ugi,
                    cr.getFullPartitionName(), exception);
          }
        }
      }
      handler.afterExecutingCleaningRequest(cr, removedFiles.value);
    } catch (Exception ex) {
      LOG.error("Caught exception when cleaning, unable to complete cleaning of {} due to {}", cr,
              StringUtils.stringifyException(ex));
      handler.failureExecutingCleaningRequest(cr, ex);
    } finally {
      if (handler.isMetricsEnabled()) {
        perfLogger.perfLogEnd(FSRemover.class.getName(), cr.getCleanerMetric());
      }
    }
  }

  /**
   * @param cr Cleaning request
   * @return List of deleted files if any files were removed
   */
  private List<Path> removeFiles(CleaningRequest cr)
          throws IOException, MetaException {
    List<Path> deleted = new ArrayList<>();
    if (cr.isDropPartition()) {
      LockRequest lockRequest = createLockRequest(cr, 0, LockType.EXCL_WRITE, DataOperationType.DELETE);
      LockResponse res = null;

      try {
        res = handler.getTxnHandler().lock(lockRequest);
        if (res.getState() == LockState.ACQUIRED) {
          deleted = remove(cr);
        }
      } catch (NoSuchTxnException | TxnAbortedException e) {
        LOG.error("Error while trying to acquire exclusive write lock: {}", e.getMessage());
        throw new MetaException(e.getMessage());
      } finally {
        if (res != null) {
          try {
            handler.getTxnHandler().unlock(new UnlockRequest(res.getLockid()));
          } catch (NoSuchLockException | TxnOpenException e) {
            LOG.error("Error while trying to release exclusive write lock: {}", e.getMessage());
          }
        }
      }
    } else {
      deleted = remove(cr);
    }
    if (!deleted.isEmpty()) {
      AcidMetricService.updateMetricsFromCleaner(cr.getDbName(), cr.getTableName(), cr.getPartitionName(),
              deleted, handler.getConf(), handler.getTxnHandler());
    }
    return deleted;
  }

  private List<Path> remove(CleaningRequest cr)
          throws MetaException, IOException {
    List<Path> deleted = new ArrayList<>();
    if (cr.getObsoleteDirs().isEmpty()) {
      return deleted;
    }
    LOG.info("About to remove {} obsolete directories from {}. {}", cr.getObsoleteDirs().size(),
            cr.getLocation(), CompactorUtil.getDebugInfo(cr.getObsoleteDirs()));
    boolean needCmRecycle;
    try {
      Database db = getMSForConf(handler.getConf()).getDatabase(getDefaultCatalog(handler.getConf()), cr.getDbName());
      needCmRecycle = ReplChangeManager.isSourceOfReplication(db);
    } catch (NoSuchObjectException ex) {
      // can not drop a database which is a source of replication
      needCmRecycle = false;
    }
    for (Path dead : cr.getObsoleteDirs()) {
      LOG.debug("Going to delete path " + dead.toString());
      if (needCmRecycle) {
        replChangeManager.recycle(dead, ReplChangeManager.RecycleType.MOVE, cr.isPurge());
      }
      if (FileUtils.moveToTrash(cr.getFs(), dead, handler.getConf(), cr.isPurge())) {
        deleted.add(dead);
      }
    }
    return deleted;
  }

  protected LockRequest createLockRequest(CleaningRequest cr, long txnId, LockType lockType, DataOperationType opType) {
    return createLockRequest(cr.getDbName(), cr.getTableName(), cr.getPartitionName(), cr.runAs(), txnId, lockType, opType);
  }

  private LockRequest createLockRequest(String dbName, String tableName, String partName, String runAs,
                                        long txnId, LockType lockType, DataOperationType opType) {
    String agentInfo = Thread.currentThread().getName();
    LockRequestBuilder requestBuilder = new LockRequestBuilder(agentInfo);
    requestBuilder.setUser(runAs);
    requestBuilder.setTransactionId(txnId);

    LockComponentBuilder lockCompBuilder = new LockComponentBuilder()
            .setLock(lockType)
            .setOperationType(opType)
            .setDbName(dbName)
            .setTableName(tableName)
            .setIsTransactional(true);

    if (partName != null) {
      lockCompBuilder.setPartitionName(partName);
    }
    requestBuilder.addLockComponent(lockCompBuilder.build());

    requestBuilder.setZeroWaitReadEnabled(!handler.getConf().getBoolVar(HiveConf.ConfVars.TXN_OVERWRITE_X_LOCK) ||
            !handler.getConf().getBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK));
    return requestBuilder.build();
  }
}
