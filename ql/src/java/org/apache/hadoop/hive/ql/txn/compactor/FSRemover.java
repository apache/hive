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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.metrics.AcidMetricService;
import org.apache.hadoop.hive.metastore.metrics.PerfLogger;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.ql.txn.compactor.handler.CleaningRequestHandler;
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
 * A runnable class which takes in cleaningRequestHandler and cleaning request and deletes the files
 * according to the cleaning request.
 */
public class FSRemover {
  private static final Logger LOG = LoggerFactory.getLogger(FSRemover.class);
  private final HiveConf conf;
  private final ReplChangeManager replChangeManager;

  public FSRemover(HiveConf conf, ReplChangeManager replChangeManager) {
    this.conf = conf;
    this.replChangeManager = replChangeManager;
  }

  public void clean(CleaningRequestHandler handler, CleaningRequest cr) throws MetaException {
    PerfLogger perfLogger = PerfLogger.getPerfLogger(false);
    try {
      if (handler.isMetricsEnabled()) {
        perfLogger.perfLogBegin(FSRemover.class.getName(), cr.getCleanerMetric());
      }
      handler.beforeExecutingCleaningRequest(cr);
      Callable<List<Path>> cleanUpTask;
      cleanUpTask = () -> removeFiles(cr, handler);

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
          closeUgi(cr, ugi);
        }
      }
      boolean success = handler.afterExecutingCleaningRequest(cr, removedFiles.value);
      if (success) {
        LOG.info("FSRemover has successfully cleaned up the cleaning request: {}", cr);
      } else {
        LOG.info("FSRemover has failed to clean up the cleaning request: {}", cr);
      }
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
  private List<Path> removeFiles(CleaningRequest cr, CleaningRequestHandler crHandler)
          throws IOException, MetaException {
    List<Path> deleted = new ArrayList<>();
    if (cr.isDropPartition()) {
      LockRequest lockRequest = createLockRequest(cr, 0, LockType.EXCL_WRITE, DataOperationType.DELETE);
      LockResponse res = null;

      try {
        res = crHandler.getTxnHandler().lock(lockRequest);
        if (res.getState() == LockState.ACQUIRED) {
          deleted = remove(cr);
        }
      } catch (NoSuchTxnException | TxnAbortedException e) {
        LOG.error("Error while trying to acquire exclusive write lock: {}", e.getMessage());
        throw new MetaException(e.getMessage());
      } finally {
        if (res != null) {
          try {
            crHandler.getTxnHandler().unlock(new UnlockRequest(res.getLockid()));
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
              deleted, conf, crHandler.getTxnHandler());
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
      Database db = getMSForConf(conf).getDatabase(getDefaultCatalog(conf), cr.getDbName());
      needCmRecycle = ReplChangeManager.isSourceOfReplication(db);
    } catch (NoSuchObjectException ex) {
      // can not drop a database which is a source of replication
      needCmRecycle = false;
    }
    for (Path dead : cr.getObsoleteDirs()) {
      LOG.debug("Going to delete path: {}", dead);
      if (needCmRecycle) {
        replChangeManager.recycle(dead, ReplChangeManager.RecycleType.MOVE, cr.isPurge());
      }
      if (FileUtils.moveToTrash(cr.getFs(), dead, conf, cr.isPurge())) {
        deleted.add(dead);
      }
    }
    return deleted;
  }

  protected LockRequest createLockRequest(CleaningRequest cr, long txnId, LockType lockType, DataOperationType opType) {
    String agentInfo = Thread.currentThread().getName();
    LockRequestBuilder requestBuilder = new LockRequestBuilder(agentInfo);
    requestBuilder.setUser(cr.runAs());
    requestBuilder.setTransactionId(txnId);

    LockComponentBuilder lockCompBuilder = new LockComponentBuilder()
            .setLock(lockType)
            .setOperationType(opType)
            .setDbName(cr.getDbName())
            .setTableName(cr.getTableName())
            .setIsTransactional(true);

    if (cr.getPartitionName() != null) {
      lockCompBuilder.setPartitionName(cr.getPartitionName());
    }
    requestBuilder.addLockComponent(lockCompBuilder.build());

    requestBuilder.setZeroWaitReadEnabled(!conf.getBoolVar(HiveConf.ConfVars.TXN_OVERWRITE_X_LOCK) ||
            !conf.getBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK));
    return requestBuilder.build();
  }

  private void closeUgi(CleaningRequest cr, UserGroupInformation ugi) {
    try {
      FileSystem.closeAllForUGI(ugi);
    } catch (IOException exception) {
      LOG.error("Could not clean up file-system handles for UGI: {} for {}", ugi,
              cr.getFullPartitionName(), exception);
    }
  }
}
