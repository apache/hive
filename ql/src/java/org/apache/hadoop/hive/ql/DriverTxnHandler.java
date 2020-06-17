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

package org.apache.hadoop.hive.ql;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class DriverTxnHandler {
  private static final String CLASS_NAME = Driver.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private static final LogHelper CONSOLE = new LogHelper(LOG);
  private static final int SHUTDOWN_HOOK_PRIORITY = 0;

  private final DriverContext driverContext;
  private final DriverState driverState;
  private final ValidTxnManager validTxnManager;

  private final List<HiveLock> hiveLocks = new ArrayList<HiveLock>();

  private Context context;
  private Runnable shutdownRunner;

  public DriverTxnHandler(DriverContext driverContext, DriverState driverState, ValidTxnManager validTxnManager) {
    this.driverContext = driverContext;
    this.driverState = driverState;
    this.validTxnManager = validTxnManager;
  }

  public void createTxnManager() throws CommandProcessorException {
    try {
      // Initialize the transaction manager.  This must be done before analyze is called.
      HiveTxnManager queryTxnManager = (driverContext.getInitTxnManager() != null) ?
          driverContext.getInitTxnManager() : SessionState.get().initTxnMgr(driverContext.getConf());

      if (queryTxnManager instanceof Configurable) {
        ((Configurable) queryTxnManager).setConf(driverContext.getConf());
      }
      driverContext.setTxnManager(queryTxnManager);
      driverContext.getQueryState().setTxnManager(queryTxnManager);

      // In case when user Ctrl-C twice to kill Hive CLI JVM, we want to release locks
      // if compile is being called multiple times, clear the old shutdownhook
      ShutdownHookManager.removeShutdownHook(shutdownRunner);
      shutdownRunner = new Runnable() {
        @Override
        public void run() {
          try {
            releaseLocksAndCommitOrRollback(false, driverContext.getTxnManager());
          } catch (LockException e) {
            LOG.warn("Exception when releasing locks in ShutdownHook for Driver: " +
                e.getMessage());
          }
        }
      };
      ShutdownHookManager.addShutdownHook(shutdownRunner, SHUTDOWN_HOOK_PRIORITY);
    } catch (LockException e) {
      ErrorMsg error = ErrorMsg.getErrorMsg(e.getMessage());
      String errorMessage = "FAILED: " + e.getClass().getSimpleName() + " [Error "  + error.getErrorCode()  + "]:";

      CONSOLE.printError(errorMessage, "\n" + StringUtils.stringifyException(e));
      throw DriverUtils.createProcessorException(driverContext, error.getErrorCode(), errorMessage, error.getSQLState(),
          e);
    }
  }

  public void setContext(Context context) {
    this.context = context;
  }

  public void acquireLocksIfNeeded() throws CommandProcessorException {
    if (requiresLock()) {
      acquireLocks();
    }
  }

  public boolean requiresLock() {
    if (!DriverUtils.checkConcurrency(driverContext)) {
      LOG.info("Concurrency mode is disabled, not creating a lock manager");
      return false;
    }

    // Lock operations themselves don't require the lock.
    if (isExplicitLockOperation()) {
      return false;
    }

    if (!HiveConf.getBoolVar(driverContext.getConf(), ConfVars.HIVE_LOCK_MAPRED_ONLY)) {
      return true;
    }

    if (driverContext.getConf().get(Constants.HIVE_QUERY_EXCLUSIVE_LOCK) != null) {
      return true;
    }

    Queue<Task<?>> tasks = new LinkedList<Task<?>>();
    tasks.addAll(driverContext.getPlan().getRootTasks());
    while (tasks.peek() != null) {
      Task<?> task = tasks.remove();
      if (task.requireLock()) {
        return true;
      }

      if (task instanceof ConditionalTask) {
        tasks.addAll(((ConditionalTask)task).getListTasks());
      }

      if (task.getChildTasks() != null) {
        tasks.addAll(task.getChildTasks());
      }
      // does not add back up task here, because back up task should be the same type of the original task.
    }

    return false;
  }

  private boolean isExplicitLockOperation() {
    HiveOperation currentOpt = driverContext.getPlan().getOperation();
    if (currentOpt != null) {
      switch (currentOpt) {
      case LOCKDB:
      case UNLOCKDB:
      case LOCKTABLE:
      case UNLOCKTABLE:
        return true;
      default:
        return false;
      }
    }
    return false;
  }

  /**
   * Acquire read and write locks needed by the statement. The list of objects to be locked are obtained from the inputs
   * and outputs populated by the compiler. Locking strategy depends on HiveTxnManager and HiveLockManager configured.
   *
   * This method also records the list of valid transactions. This must be done after any transactions have been opened.
   */
  private void acquireLocks() throws CommandProcessorException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.ACQUIRE_READ_WRITE_LOCKS);

    if (!driverContext.getTxnManager().isTxnOpen() && driverContext.getTxnManager().supportsAcid()) {
      /* non acid txn managers don't support txns but fwd lock requests to lock managers
         acid txn manager requires all locks to be associated with a txn so if we end up here w/o an open txn
         it's because we are processing something like "use <database> which by definition needs no locks */
      return;
    }

    try {
      setWriteIdForAcidFileSinks();
      allocateWriteIdForAcidAnalyzeTable();
      boolean hasAcidDdl = setWriteIdForAcidDdl();
      acquireLocksInternal();

      if (driverContext.getPlan().hasAcidResourcesInQuery() || hasAcidDdl) {
        validTxnManager.recordValidWriteIds();
      }
    } catch (Exception e) {
      String errorMessage;
      if (driverState.isDestroyed() || driverState.isAborted() || driverState.isClosed()) {
        errorMessage = String.format("Ignore lock acquisition related exception in terminal state (%s): %s",
            driverState.toString(), e.getMessage());
        CONSOLE.printInfo(errorMessage);
      } else {
        errorMessage = String.format("FAILED: Error in acquiring locks: %s", e.getMessage());
        CONSOLE.printError(errorMessage, "\n" + StringUtils.stringifyException(e));
      }
      throw DriverUtils.createProcessorException(driverContext, 10, errorMessage, ErrorMsg.findSQLState(e.getMessage()),
          e);
    } finally {
      perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.ACQUIRE_READ_WRITE_LOCKS);
    }
  }

  private void setWriteIdForAcidFileSinks() throws SemanticException, LockException {
    if (!driverContext.getPlan().getAcidSinks().isEmpty()) {
      List<FileSinkDesc> acidSinks = new ArrayList<>(driverContext.getPlan().getAcidSinks());
      //sorting makes tests easier to write since file names and ROW__IDs depend on statementId
      //so this makes (file name -> data) mapping stable
      acidSinks.sort((FileSinkDesc fsd1, FileSinkDesc fsd2) -> fsd1.getDirName().compareTo(fsd2.getDirName()));
      for (FileSinkDesc acidSink : acidSinks) {
        TableDesc tableInfo = acidSink.getTableInfo();
        TableName tableName = HiveTableName.of(tableInfo.getTableName());
        long writeId = driverContext.getTxnManager().getTableWriteId(tableName.getDb(), tableName.getTable());
        acidSink.setTableWriteId(writeId);

        /**
         * it's possible to have > 1 FileSink writing to the same table/partition
         * e.g. Merge stmt, multi-insert stmt when mixing DP and SP writes
         * Insert ... Select ... Union All Select ... using
         * {@link org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator#UNION_SUDBIR_PREFIX}
         */
        acidSink.setStatementId(driverContext.getTxnManager().getStmtIdAndIncrement());
        String unionAllSubdir = "/" + AbstractFileMergeOperator.UNION_SUDBIR_PREFIX;
        if (acidSink.getInsertOverwrite() && acidSink.getDirName().toString().contains(unionAllSubdir) &&
            acidSink.isFullAcidTable()) {
          throw new UnsupportedOperationException("QueryId=" + driverContext.getPlan().getQueryId() +
              " is not supported due to OVERWRITE and UNION ALL.  Please use truncate + insert");
        }
      }
    }
  }

  private void allocateWriteIdForAcidAnalyzeTable() throws LockException {
    if (driverContext.getPlan().getAcidAnalyzeTable() != null) {
      Table table = driverContext.getPlan().getAcidAnalyzeTable().getTable();
      driverContext.getTxnManager().getTableWriteId(table.getDbName(), table.getTableName());
    }
  }

  private boolean setWriteIdForAcidDdl() throws SemanticException, LockException {
    DDLDescWithWriteId acidDdlDesc = driverContext.getPlan().getAcidDdlDesc();
    boolean hasAcidDdl = acidDdlDesc != null && acidDdlDesc.mayNeedWriteId();
    if (hasAcidDdl) {
      String fqTableName = acidDdlDesc.getFullTableName();
      TableName tableName = HiveTableName.of(fqTableName);
      long writeId = driverContext.getTxnManager().getTableWriteId(tableName.getDb(), tableName.getTable());
      acidDdlDesc.setWriteId(writeId);
    }
    return hasAcidDdl;
  }

  private void acquireLocksInternal() throws CommandProcessorException, LockException {
    /* It's imperative that {@code acquireLocks()} is called for all commands so that
       HiveTxnManager can transition its state machine correctly */
    String userFromUGI = DriverUtils.getUserFromUGI(driverContext);
    driverContext.getTxnManager().acquireLocks(driverContext.getPlan(), context, userFromUGI, driverState);
    List<HiveLock> locks = context.getHiveLocks();
    LOG.info("Operation {} obtained {} locks", driverContext.getPlan().getOperation(),
        ((locks == null) ? 0 : locks.size()));
    // This check is for controlling the correctness of the current state
    if (driverContext.getTxnManager().recordSnapshot(driverContext.getPlan()) &&
        !driverContext.isValidTxnListsGenerated()) {
      throw new IllegalStateException("Need to record valid WriteID list but there is no valid TxnID list (" +
          JavaUtils.txnIdToString(driverContext.getTxnManager().getCurrentTxnId()) +
          ", queryId: " + driverContext.getPlan().getQueryId() + ")");
    }
  }

  public void addHiveLocksFromContext() {
    hiveLocks.addAll(context.getHiveLocks());
  }

  public void release() {
    release(!hiveLocks.isEmpty());
  }

  public void destroy() {
    boolean isTxnOpen =
        driverContext != null &&
        driverContext.getTxnManager() != null &&
        driverContext.getTxnManager().isTxnOpen();
    release(!hiveLocks.isEmpty() || isTxnOpen);
  }

  private void release(boolean releaseLocks) {
    if (releaseLocks) {
      try {
        releaseLocksAndCommitOrRollback(false);
      } catch (LockException e) {
        LOG.warn("Exception when releasing locking in destroy: " + e.getMessage());
      }
    }
    ShutdownHookManager.removeShutdownHook(shutdownRunner);
  }

  public void releaseLocksAndCommitOrRollback(boolean commit) throws LockException {
    releaseLocksAndCommitOrRollback(commit, driverContext.getTxnManager());
  }

  public void releaseLocksAndCommitOrRollback(boolean commit, HiveTxnManager txnManager) throws LockException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.RELEASE_LOCKS);
    
    // If we've opened a transaction we need to commit or rollback rather than explicitly releasing the locks.
    driverContext.getConf().unset(ValidTxnList.VALID_TXNS_KEY);
    driverContext.getConf().unset(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY);
    if (!DriverUtils.checkConcurrency(driverContext)) {
      return;
    }

    if (txnManager.isTxnOpen()) {
      commitOrRollback(commit, txnManager);
    } else {
      releaseLocks(txnManager, hiveLocks);
    }
    hiveLocks.clear();
    if (context != null) {
      context.setHiveLocks(null);
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.RELEASE_LOCKS);
  }

  private void commitOrRollback(boolean commit, HiveTxnManager txnManager) throws LockException {
    if (commit) {
      if (driverContext.getConf().getBoolVar(ConfVars.HIVE_IN_TEST) &&
          driverContext.getConf().getBoolVar(ConfVars.HIVETESTMODEROLLBACKTXN)) {
        txnManager.rollbackTxn();
      } else {
        txnManager.commitTxn(); //both commit & rollback clear ALL locks for this transaction
      }
    } else {
      txnManager.rollbackTxn();
    }
  }

  private void releaseLocks(HiveTxnManager txnManager, List<HiveLock> hiveLocks) throws LockException {
    if (context != null && context.getHiveLocks() != null) {
      hiveLocks.addAll(context.getHiveLocks());
    }
    txnManager.releaseLocks(hiveLocks);
  }
}
