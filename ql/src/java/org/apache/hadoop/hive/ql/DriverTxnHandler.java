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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
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

import com.google.common.base.Strings;

/**
 * Handles transaction related duties of the Driver.
 */
class DriverTxnHandler {
  private static final String CLASS_NAME = Driver.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private static final LogHelper CONSOLE = new LogHelper(LOG);
  private static final int SHUTDOWN_HOOK_PRIORITY = 0;

  private final DriverContext driverContext;
  private final DriverState driverState;

  private final List<HiveLock> hiveLocks = new ArrayList<HiveLock>();

  private Context context;
  private Runnable txnRollbackRunner;

  DriverTxnHandler(DriverContext driverContext, DriverState driverState) {
    this.driverContext = driverContext;
    this.driverState = driverState;
  }

  void createTxnManager() throws CommandProcessorException {
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
      ShutdownHookManager.removeShutdownHook(txnRollbackRunner);
      txnRollbackRunner = new Runnable() {
        @Override
        public void run() {
          try {
            endTransactionAndCleanup(false, driverContext.getTxnManager());
          } catch (LockException e) {
            LOG.warn("Exception when releasing locks in ShutdownHook for Driver: " +
                e.getMessage());
          }
        }
      };
      ShutdownHookManager.addShutdownHook(txnRollbackRunner, SHUTDOWN_HOOK_PRIORITY);
    } catch (LockException e) {
      ErrorMsg error = ErrorMsg.getErrorMsg(e.getMessage());
      String errorMessage = "FAILED: " + e.getClass().getSimpleName() + " [Error "  + error.getErrorCode()  + "]:";

      CONSOLE.printError(errorMessage, "\n" + StringUtils.stringifyException(e));
      throw DriverUtils.createProcessorException(driverContext, error.getErrorCode(), errorMessage, error.getSQLState(),
          e);
    }
  }

  void setContext(Context context) {
    this.context = context;
  }

  void cleanupTxnList() {
    driverContext.getConf().unset(ValidTxnList.VALID_TXNS_KEY);
  }

  void acquireLocksIfNeeded() throws CommandProcessorException {
    if (requiresLock()) {
      acquireLocks();
    }
  }

  private boolean requiresLock() {
    if (!DriverUtils.checkConcurrency(driverContext)) {
      LOG.info("Concurrency mode is disabled, not creating a lock manager");
      return false;
    }

    // Lock operations themselves don't require the lock.
    if (isExplicitLockOperation()) {
      return false;
    }

    // no execution is going to be attempted, skip acquiring locks
    if (context.isExplainSkipExecution()) {
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
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.ACQUIRE_READ_WRITE_LOCKS);

    if (!driverContext.getTxnManager().isTxnOpen() && driverContext.getTxnManager().supportsAcid()) {
      /* non acid txn managers don't support txns but fwd lock requests to lock managers
         acid txn manager requires all locks to be associated with a txn so if we end up here w/o an open txn
         it's because we are processing something like "use <database> which by definition needs no locks */
      return;
    }

    try {
      // Ensure we answer any metadata calls with fresh responses
      driverContext.getQueryState().disableHMSCache();
      setWriteIdForAcidFileSinks();
      allocateWriteIdForAcidAnalyzeTable();
      boolean hasAcidDdl = setWriteIdForAcidDdl();
      acquireLocksInternal();

      if (driverContext.getPlan().hasAcidResourcesInQuery() || hasAcidDdl) {
        recordValidWriteIds();
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
      driverContext.getQueryState().enableHMSCache();
      perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.ACQUIRE_READ_WRITE_LOCKS);
    }
  }

  void setWriteIdForAcidFileSinks() throws SemanticException, LockException {
    if (!driverContext.getPlan().getAcidSinks().isEmpty()) {
      List<FileSinkDesc> acidSinks = new ArrayList<>(driverContext.getPlan().getAcidSinks());
      //sorting makes tests easier to write since file names and ROW__IDs depend on statementId
      //so this makes (file name -> data) mapping stable
      acidSinks.sort((FileSinkDesc fsd1, FileSinkDesc fsd2) -> fsd1.getDirName().compareTo(fsd2.getDirName()));

      // If the direct insert is on, sort the FSOs by moveTaskId as well because the dir is the same for all except the union use cases.
      boolean isDirectInsertOn = false;
      for (FileSinkDesc acidSink : acidSinks) {
        if (acidSink.isDirectInsert()) {
          isDirectInsertOn = true;
          break;
        }
      }
      if (isDirectInsertOn) {
        acidSinks.sort((FileSinkDesc fsd1, FileSinkDesc fsd2) -> fsd1.getMoveTaskId().compareTo(fsd2.getMoveTaskId()));
      }

      int maxStmtId = -1;
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
        maxStmtId = Math.max(acidSink.getStatementId(), maxStmtId);
        String unionAllSubdir = "/" + AbstractFileMergeOperator.UNION_SUDBIR_PREFIX;
        if (acidSink.getInsertOverwrite() && acidSink.getDirName().toString().contains(unionAllSubdir) &&
            acidSink.isFullAcidTable()) {
          throw new UnsupportedOperationException("QueryId=" + driverContext.getPlan().getQueryId() +
              " is not supported due to OVERWRITE and UNION ALL.  Please use truncate + insert");
        }
      }
      if (HiveConf.getBoolVar(driverContext.getConf(), ConfVars.HIVE_EXTEND_BUCKET_ID_RANGE)) {
        for (FileSinkDesc each : acidSinks) {
          each.setMaxStmtId(maxStmtId);
        }
      }
    }
  }

  private void allocateWriteIdForAcidAnalyzeTable() throws LockException {
    if (driverContext.getPlan().getAcidAnalyzeTable() != null) {
      Table table = driverContext.getPlan().getAcidAnalyzeTable().getTable();
      driverContext.getTxnManager().setTableWriteId(
          table.getDbName(), table.getTableName(), driverContext.getAnalyzeTableWriteId());
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

  /**
   *  Write the current set of valid write ids for the operated acid tables into the configuration so
   *  that it can be read by the input format.
   */
  ValidTxnWriteIdList recordValidWriteIds() throws LockException {
    String txnString = driverContext.getConf().get(ValidTxnList.VALID_TXNS_KEY);
    if (Strings.isNullOrEmpty(txnString)) {
      throw new IllegalStateException("calling recordValidWriteIds() without initializing ValidTxnList " +
          JavaUtils.txnIdToString(driverContext.getTxnManager().getCurrentTxnId()));
    }

    ValidTxnWriteIdList txnWriteIds = getTxnWriteIds(txnString);
    setValidWriteIds(txnWriteIds);
    driverContext.getTxnManager().addWriteIdsToMinHistory(driverContext.getPlan(), txnWriteIds);
    
    LOG.debug("Encoding valid txn write ids info {} txnid: {}", txnWriteIds.toString(),
        driverContext.getTxnManager().getCurrentTxnId());
    return txnWriteIds;
  }

  private ValidTxnWriteIdList getTxnWriteIds(String txnString) throws LockException {
    List<String> txnTables = getTransactionalTables(getTables(true, true));
    ValidTxnWriteIdList txnWriteIds = null;
    if (driverContext.getCompactionWriteIds() != null) {
      // This is kludgy: here we need to read with Compactor's snapshot/txn rather than the snapshot of the current
      // {@code txnMgr}, in effect simulating a "flashback query" but can't actually share compactor's txn since it
      // would run multiple statements.  See more comments in {@link org.apache.hadoop.hive.ql.txn.compactor.Worker}
      // where it start the compactor txn*/
      if (txnTables.size() != 1) {
        throw new LockException("Unexpected tables in compaction: " + txnTables);
      }
      txnWriteIds = new ValidTxnWriteIdList(driverContext.getCompactorTxnId());
      txnWriteIds.addTableValidWriteIdList(driverContext.getCompactionWriteIds());
    } else {
      txnWriteIds = driverContext.getTxnManager().getValidWriteIds(txnTables, txnString);
    }
    if (driverContext.getTxnType() == TxnType.READ_ONLY && !getTables(false, true).isEmpty()) {
      throw new IllegalStateException(String.format(
          "Inferred transaction type '%s' doesn't conform to the actual query string '%s'",
          driverContext.getTxnType(), driverContext.getQueryState().getQueryString()));
    }
    return txnWriteIds;
  }

  private void setValidWriteIds(ValidTxnWriteIdList txnWriteIds) {
    driverContext.getConf().set(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY, txnWriteIds.toString());
    if (driverContext.getPlan().getFetchTask() != null) {
      // This is needed for {@link HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION} optimization which initializes JobConf
      // in FetchOperator before recordValidTxns() but this has to be done after locks are acquired to avoid race
      // conditions in ACID. This case is supported only for single source query.
      Operator<?> source = driverContext.getPlan().getFetchTask().getWork().getSource();
      if (source instanceof TableScanOperator) {
        TableScanOperator tsOp = (TableScanOperator)source;
        String fullTableName = AcidUtils.getFullTableName(tsOp.getConf().getDatabaseName(),
            tsOp.getConf().getTableName());
        ValidWriteIdList writeIdList = txnWriteIds.getTableValidWriteIdList(fullTableName);
        if (tsOp.getConf().isTranscationalTable() && (writeIdList == null)) {
          throw new IllegalStateException(String.format(
              "ACID table: %s is missing from the ValidWriteIdList config: %s", fullTableName, txnWriteIds.toString()));
        }
        if (writeIdList != null) {
          driverContext.getPlan().getFetchTask().setValidWriteIdList(writeIdList.toString());
        }
      }
    }
  }

  /**
   * Checks whether txn list has been invalidated while planning the query.
   * This would happen if query requires exclusive/semi-shared lock, and there has been a committed transaction
   * on the table over which the lock is required.
   */
  boolean isValidTxnListState() throws LockException {
    // 1) Get valid txn list.
    String txnString = driverContext.getConf().get(ValidTxnList.VALID_TXNS_KEY);
    if (txnString == null) {
      return true; // Not a transactional op, nothing more to do
    }

    // 2) Get locks that are relevant:
    // - Exclusive for INSERT OVERWRITE, when shared write is disabled (HiveConf.TXN_WRITE_X_LOCK=false).
    // - Excl-write for UPDATE/DELETE, when shared write is disabled, INSERT OVERWRITE - when enabled.
    Set<String> nonSharedLockedTables = getNonSharedLockedTables();
    if (nonSharedLockedTables.isEmpty()) {
      return true; // Nothing to check
    }

    // 3) Get txn tables that are being written
    String txnWriteIdListString = driverContext.getConf().get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY);
    if (Strings.isNullOrEmpty(txnWriteIdListString)) {
      return true; // Nothing to check
    }

    // 4) Check if there is conflict
    long txnId = driverContext.getTxnManager().getLatestTxnIdInConflict();
    if (txnId <= 0) {
      return true;
    }
    if (txnId > driverContext.getTxnManager().getCurrentTxnId()) {
      driverContext.setOutdatedTxn(true);
    }
    return false;
  }

  private Set<String> getNonSharedLockedTables() {
    if (CollectionUtils.isEmpty(context.getHiveLocks())) {
      return Collections.emptySet(); // Nothing to check
    }

    Set<String> nonSharedLockedTables = new HashSet<>();
    for (HiveLock lock : context.getHiveLocks()) {
      if (lock.mayContainComponents()) {
        // The lock may have multiple components, e.g., DbHiveLock, hence we need to check for each of them
        for (LockComponent lockComponent : lock.getHiveLockComponents()) {
          // We only consider tables for which we hold either an exclusive or a excl-write lock
          if ((lockComponent.getType() == LockType.EXCLUSIVE || lockComponent.getType() == LockType.EXCL_WRITE) &&
              lockComponent.getTablename() != null && !DbTxnManager.GLOBAL_LOCKS.equals(lockComponent.getDbname())) {
            nonSharedLockedTables.add(TableName.getDbTable(lockComponent.getDbname(), lockComponent.getTablename()));
          }
        }
      } else {
        // The lock has a single components, e.g., SimpleHiveLock or ZooKeeperHiveLock.
        // Pos 0 of lock paths array contains dbname, pos 1 contains tblname
        if ((lock.getHiveLockMode() == HiveLockMode.EXCLUSIVE || lock.getHiveLockMode() == HiveLockMode.SEMI_SHARED) &&
            lock.getHiveLockObject().getPaths().length == 2) {
          nonSharedLockedTables.add(
              TableName.getDbTable(lock.getHiveLockObject().getPaths()[0], lock.getHiveLockObject().getPaths()[1]));
        }
      }
    }
    return nonSharedLockedTables;
  }

  private Map<String, Table> getTables(boolean inputNeeded, boolean outputNeeded) {
    Map<String, Table> tables = new HashMap<>();
    if (inputNeeded) {
      driverContext.getPlan().getInputs().forEach(input -> addTableFromEntity(input, tables));
    }
    if (outputNeeded) {
      driverContext.getPlan().getOutputs().forEach(output -> addTableFromEntity(output, tables));
    }
    return tables;
  }

  private void addTableFromEntity(Entity entity, Map<String, Table> tables) {
    Table table;
    switch (entity.getType()) {
    case TABLE:
      table = entity.getTable();
      break;
    case PARTITION:
    case DUMMYPARTITION:
      table = entity.getPartition().getTable();
      break;
    default:
      return;
    }
    String fullTableName = AcidUtils.getFullTableName(table.getDbName(), table.getTableName());
    tables.put(fullTableName, table);
  }

  void rollback(CommandProcessorException cpe) throws CommandProcessorException {
    try {
      endTransactionAndCleanup(false);
    } catch (LockException e) {
      LOG.error("rollback() FAILED: " + cpe); //make sure not to loose
      DriverUtils.handleHiveException(driverContext, e, 12, "Additional info in hive.log at \"rollback() FAILED\"");
    }
  }

  void handleTransactionAfterExecution() throws CommandProcessorException {
    try {
      //since set autocommit starts an implicit txn, close it
      if (driverContext.getTxnManager().isImplicitTransactionOpen(context) ||
          driverContext.getPlan().getOperation() == HiveOperation.COMMIT) {
        endTransactionAndCleanup(true);
      } else if (driverContext.getPlan().getOperation() == HiveOperation.ROLLBACK) {
        endTransactionAndCleanup(false);
      } else if (!driverContext.getTxnManager().isTxnOpen() &&
          driverContext.getQueryState().getHiveOperation() == HiveOperation.REPLLOAD) {
        // repl load during migration, commits the explicit txn and start some internal txns. Call
        // releaseLocksAndCommitOrRollback to do the clean up.
        endTransactionAndCleanup(false);
      }
      // if none of the above is true, then txn (if there is one started) is not finished
    } catch (LockException e) {
      DriverUtils.handleHiveException(driverContext, e, 12, null);
    }
  }

  private List<String> getTransactionalTables(Map<String, Table> tables) {
    return tables.entrySet().stream()
      .filter(entry -> AcidUtils.isTransactionalTable(entry.getValue()))
      .map(Map.Entry::getKey)
      .collect(Collectors.toList());
  }

  void addHiveLocksFromContext() {
    hiveLocks.addAll(context.getHiveLocks());
  }

  void release() {
    release(!hiveLocks.isEmpty());
  }

  void destroy(String queryIdFromDriver) {
    // We need cleanup transactions, even if we did not acquired locks yet
    // However TxnManager is bound to session, so wee need to check if it is already handling a new query
    boolean isTxnOpen =
        driverContext != null &&
        driverContext.getTxnManager() != null &&
        driverContext.getTxnManager().isTxnOpen() &&
        org.apache.commons.lang3.StringUtils.equals(queryIdFromDriver, driverContext.getTxnManager().getQueryid());

    release(!hiveLocks.isEmpty() || isTxnOpen);
  }

  private void release(boolean releaseLocks) {
    if (releaseLocks) {
      try {
        endTransactionAndCleanup(false);
      } catch (LockException e) {
        LOG.warn("Exception when releasing locking in destroy: " + e.getMessage());
      }
    }
    ShutdownHookManager.removeShutdownHook(txnRollbackRunner);
  }

  void endTransactionAndCleanup(boolean commit) throws LockException {
    endTransactionAndCleanup(commit, driverContext.getTxnManager());
    ShutdownHookManager.removeShutdownHook(txnRollbackRunner);
    txnRollbackRunner = null;
  }

  // When Hive query is being interrupted via cancel request, both the background pool thread (HiveServer2-Background), 
  // executing the query, and the HttpHandler thread (HiveServer2-Handler), running the HiveSession.cancelOperation logic, 
  // might call the below method concurrently. To prevent a race condition, marking it as synchronized.
  synchronized void endTransactionAndCleanup(boolean commit, HiveTxnManager txnManager) throws LockException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.RELEASE_LOCKS);

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

    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.RELEASE_LOCKS);
  }

  private void commitOrRollback(boolean commit, HiveTxnManager txnManager) throws LockException {
    if (commit) {
      if (driverContext.getConf().getBoolVar(ConfVars.HIVE_IN_TEST) &&
          driverContext.getConf().getBoolVar(ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN)) {
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
    if (!hiveLocks.isEmpty()) {
      txnManager.releaseLocks(hiveLocks);
    }
  }
}
