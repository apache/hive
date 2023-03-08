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

import java.io.DataInput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.cache.results.CacheUsage;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lock.CompileLock;
import org.apache.hadoop.hive.ql.lock.CompileLockFactory;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.formatting.JsonMetaDataFormatter;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatter;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSource;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.LineageState;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.wm.WmContext;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

/**
 * Compiles and executes HQL commands.
 */
public class Driver implements IDriver {

  private static final String CLASS_NAME = Driver.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private static final LogHelper CONSOLE = new LogHelper(LOG);

  private static final String SNAPSHOT_WAS_OUTDATED_WHEN_LOCKS_WERE_ACQUIRED =
      "snapshot was outdated when locks were acquired";

  private int maxRows = 100;

  private final DriverContext driverContext;
  private final DriverState driverState = new DriverState();
  private final DriverTxnHandler driverTxnHandler;

  private Context context;
  private TaskQueue taskQueue;

  @VisibleForTesting
  public Driver(HiveConf conf) {
    this(new QueryState.Builder().withGenerateNewQueryId(true).withHiveConf(conf).build());
  }

  // Pass lineageState when a driver instantiates another Driver to run or compile another query
  public Driver(HiveConf conf, Context ctx, LineageState lineageState) {
    this(QueryState.getNewQueryState(conf, lineageState), null);
    context = ctx;
  }

  public Driver(QueryState queryState) {
    this(queryState, null, null);
  }

  public Driver(QueryState queryState, QueryInfo queryInfo) {
    this(queryState, queryInfo, null);
  }

  public Driver(QueryState queryState, ValidWriteIdList compactionWriteIds, long compactorTxnId) {
    this(queryState);
    driverContext.setCompactionWriteIds(compactionWriteIds);
    driverContext.setCompactorTxnId(compactorTxnId);
  }

  public Driver(QueryState queryState, long analyzeTableWriteId) {
    this(queryState);
    driverContext.setAnalyzeTableWriteId(analyzeTableWriteId);
  }

  public Driver(QueryState queryState, QueryInfo queryInfo, HiveTxnManager txnManager) {
    driverContext = new DriverContext(queryState, queryInfo, new HookRunner(queryState.getConf(), CONSOLE),
        txnManager);
    driverTxnHandler = new DriverTxnHandler(driverContext, driverState);
  }

  @Override
  public Context getContext() {
    return context;
  }

  @Override
  public HiveConf getConf() {
    return driverContext.getConf();
  }

  /**
   * Compiles and executes an HQL command.
   */
  @Override
  public CommandProcessorResponse run(String command) throws CommandProcessorException {
    return run(command, false);
  }

  /**
   * Executes a previously compiled HQL command.
   */
  @Override
  public CommandProcessorResponse run() throws CommandProcessorException {
    return run(null, true);
  }

  private CommandProcessorResponse run(String command, boolean alreadyCompiled) throws CommandProcessorException {
    try {
      runInternal(command, alreadyCompiled);
      return new CommandProcessorResponse(getSchema(), null);
    } catch (CommandProcessorException cpe) {
      processRunException(cpe);
      throw cpe;
    }
  }

  private void runInternal(String command, boolean alreadyCompiled) throws CommandProcessorException {
    DriverState.setDriverState(driverState);

    QueryPlan plan = driverContext.getPlan();
    if (plan != null && plan.isPrepareQuery() && !plan.isExplain()) {
      LOG.info("Skip running tasks for prepare plan");
      return;
    }

    setInitialStateForRun(alreadyCompiled);

    // a flag that helps to set the correct driver state in finally block by tracking if
    // the method has been returned by an error or not.
    boolean isFinishedWithError = true;
    try {
      HiveDriverRunHookContext hookContext = new HiveDriverRunHookContextImpl(driverContext.getConf(),
          alreadyCompiled ? context.getCmd() : command);
      runPreDriverHooks(hookContext);

      if (!alreadyCompiled) {
        compileInternal(command, true);
      } else {
        driverContext.getPlan().setQueryStartTime(driverContext.getQueryDisplay().getQueryStartTime());
      }

      // Reset the PerfLogger so that it doesn't retain any previous values.
      // Any value from compilation phase can be obtained through the map set in queryDisplay during compilation.
      PerfLogger perfLogger = SessionState.getPerfLogger(true);

      // the reason that we set the txn manager for the cxt here is because each query has its own ctx object.
      // The txn mgr is shared across the same instance of Driver, which can run multiple queries.
      context.setHiveTxnManager(driverContext.getTxnManager());

      DriverUtils.checkInterrupted(driverState, driverContext, "at acquiring the lock.", null, null);

      lockAndRespond();

      if (validateTxnList()) {
        // the reason that we set the txn manager for the cxt here is because each query has its own ctx object.
        // The txn mgr is shared across the same instance of Driver, which can run multiple queries.
        context.setHiveTxnManager(driverContext.getTxnManager());
        perfLogger = SessionState.getPerfLogger(true);
      }
      execute();

      FetchTask fetchTask = driverContext.getPlan().getFetchTask();
      if (fetchTask != null) {
        fetchTask.setTaskQueue(null);
        fetchTask.setQueryPlan(null);
        try {
          fetchTask.execute();
          driverContext.setFetchTask(fetchTask);
        } catch (Throwable e) {
          throw new CommandProcessorException(e);
        }
      }
      driverTxnHandler.handleTransactionAfterExecution();

      driverContext.getQueryDisplay().setPerfLogStarts(QueryDisplay.Phase.EXECUTION, perfLogger.getStartTimes());
      driverContext.getQueryDisplay().setPerfLogEnds(QueryDisplay.Phase.EXECUTION, perfLogger.getEndTimes());

      runPostDriverHooks(hookContext);
      isFinishedWithError = false;
    } finally {
      if (driverState.isAborted()) {
        closeInProcess(true);
      } else {
        releaseResources();
      }
      driverState.executionFinishedWithLocking(isFinishedWithError);
    }

    SessionState.getPerfLogger().cleanupPerfLogMetrics();
  }

  /**
   * @return If the txn manager should be set.
   */
  private boolean validateTxnList() throws CommandProcessorException {
    int retryShapshotCount = 0;
    int maxRetrySnapshotCount = HiveConf.getIntVar(driverContext.getConf(),
        HiveConf.ConfVars.HIVE_TXN_MAX_RETRYSNAPSHOT_COUNT);
    boolean shouldSet = false;

    try {
      do {
        driverContext.setOutdatedTxn(false);
        // Inserts will not invalidate the snapshot, that could cause duplicates.
        if (!driverTxnHandler.isValidTxnListState()) {
          LOG.info("Re-compiling after acquiring locks, attempt #" + retryShapshotCount);
          // Snapshot was outdated when locks were acquired, hence regenerate context, txn list and retry.
          // TODO: Lock acquisition should be moved before analyze, this is a bit hackish.
          // Currently, we acquire a snapshot, compile the query with that snapshot, and then - acquire locks.
          // If snapshot is still valid, we continue as usual.
          // But if snapshot is not valid, we recompile the query.
          if (driverContext.isOutdatedTxn()) {
            // Later transaction invalidated the snapshot, a new transaction is required
            LOG.info("Snapshot is outdated, re-initiating transaction ...");
            driverContext.getTxnManager().rollbackTxn();

            String userFromUGI = DriverUtils.getUserFromUGI(driverContext);
            driverContext.getTxnManager().openTxn(context, userFromUGI, driverContext.getTxnType());
            lockAndRespond();
          } else {
            // We need to clear the possibly cached writeIds for the prior transaction, so new writeIds
            // are allocated since writeIds need to be committed in increasing order. It helps in cases
            // like:
            // txnId   writeId
            // 10      71  <--- commit first
            // 11      69
            // 12      70
            // in which the transaction is not out of date, but the writeId would not be increasing.
            // This would be a problem in an UPDATE, since it would end up generating delete
            // deltas for a future writeId - which in turn causes scans to not think they are deleted.
            // The scan basically does last writer wins for a given row which is determined by
            // max(committingWriteId) for a given ROW__ID(originalWriteId, bucketId, rowId). So the
            // data add ends up being > than the data delete.
            driverContext.getTxnManager().clearCaches();
          }
          driverContext.setRetrial(true);
          driverContext.getConf().set(ValidTxnList.VALID_TXNS_KEY,
              driverContext.getTxnManager().getValidTxns().toString());

          if (driverContext.getPlan().hasAcidResourcesInQuery()) {
            compileInternal(context.getCmd(), true);
            driverTxnHandler.recordValidWriteIds();
            driverTxnHandler.setWriteIdForAcidFileSinks();
          }
          // Since we're reusing the compiled plan, we need to update its start time for current run
          driverContext.getPlan().setQueryStartTime(driverContext.getQueryDisplay().getQueryStartTime());
          driverContext.setRetrial(false);
        }
        // Re-check snapshot only in case we had to release locks and open a new transaction,
        // otherwise exclusive locks should protect output tables/partitions in snapshot from concurrent writes.
      } while (driverContext.isOutdatedTxn() && ++retryShapshotCount <= maxRetrySnapshotCount);

      shouldSet = shouldSetTxnManager(retryShapshotCount, maxRetrySnapshotCount);
    } catch (LockException | SemanticException e) {
      DriverUtils.handleHiveException(driverContext, e, 13, null);
    }

    return shouldSet;
  }

  private boolean shouldSetTxnManager(int retryShapshotCount, int maxRetrySnapshotCount)
      throws CommandProcessorException {
    if (retryShapshotCount > maxRetrySnapshotCount) {
      // Throw exception
      HiveException e = new HiveException(
          "Operation could not be executed, " + SNAPSHOT_WAS_OUTDATED_WHEN_LOCKS_WERE_ACQUIRED + ".");
      DriverUtils.handleHiveException(driverContext, e, 14, null);
    }

    return retryShapshotCount != 0;
  }

  private void setInitialStateForRun(boolean alreadyCompiled) throws CommandProcessorException {
    driverState.lock();
    try {
      if (alreadyCompiled) {
        if (driverState.isCompiled()) {
          driverState.executing();
        } else {
          String errorMessage = "FAILED: Precompiled query has been cancelled or closed.";
          CONSOLE.printError(errorMessage);
          throw DriverUtils.createProcessorException(driverContext, 12, errorMessage, null, null);
        }
      } else {
        driverState.compiling();
      }
    } finally {
      driverState.unlock();
    }
  }

  private void runPreDriverHooks(HiveDriverRunHookContext hookContext) throws CommandProcessorException {
    try {
      driverContext.getHookRunner().runPreDriverHooks(hookContext);
    } catch (Exception e) {
      String errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
      CONSOLE.printError(errorMessage + "\n" + StringUtils.stringifyException(e));
      throw DriverUtils.createProcessorException(driverContext, 12, errorMessage,
          ErrorMsg.findSQLState(e.getMessage()), e);
    }
  }

  public void lockAndRespond() throws CommandProcessorException {
    // Assumes the query has already been compiled
    if (driverContext.getPlan() == null) {
      throw new IllegalStateException(
          "No previously compiled query for driver - queryId=" + driverContext.getQueryState().getQueryId());
    }

    try {
      driverTxnHandler.acquireLocksIfNeeded();
    } catch (CommandProcessorException cpe) {
      driverTxnHandler.rollback(cpe);
      throw cpe;
    }
  }

  private void execute() throws CommandProcessorException {
    try {
      taskQueue = new TaskQueue(context); // for canceling the query (should be bound to session?)
      Executor executor = new Executor(context, driverContext, driverState, taskQueue);
      executor.execute();
    } catch (CommandProcessorException cpe) {
      driverTxnHandler.rollback(cpe);
      throw cpe;
    }
  }

  private void runPostDriverHooks(HiveDriverRunHookContext hookContext) throws CommandProcessorException {
    try {
      driverContext.getHookRunner().runPostDriverHooks(hookContext);
    } catch (Exception e) {
      String errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
      CONSOLE.printError(errorMessage + "\n" + StringUtils.stringifyException(e));
      throw DriverUtils.createProcessorException(driverContext, 12, errorMessage,
          ErrorMsg.findSQLState(e.getMessage()), e);
    }
  }

  private void processRunException(CommandProcessorException cpe) {
    SessionState ss = SessionState.get();
    if (ss == null) {
      return;
    }

    MetaDataFormatter mdf = MetaDataFormatUtils.getFormatter(ss.getConf());
    if (!(mdf instanceof JsonMetaDataFormatter)) {
      return;
    }

    /* Here we want to encode the error in machine readable way (e.g. JSON). Ideally, errorCode would always be set
     * to a canonical error defined in ErrorMsg. In practice that is rarely the case, so the messy logic below tries
     * to tease out canonical error code if it can.  Exclude stack trace from output when the error is a
     * specific/expected one. It's written to stdout for backward compatibility (WebHCat consumes it).*/
    try {
      if (cpe.getCause() == null) {
        mdf.error(ss.out, cpe.getMessage(), cpe.getResponseCode(), cpe.getSqlState());
        return;
      }
      ErrorMsg canonicalErr = ErrorMsg.getErrorMsg(cpe.getResponseCode());
      if (canonicalErr != null && canonicalErr != ErrorMsg.GENERIC_ERROR) {
        /* Some HiveExceptions (e.g. SemanticException) don't set canonical ErrorMsg explicitly, but there is logic
         * (e.g. #compile()) to find an appropriate canonical error and return its code as error code. In this case
         * we want to preserve it for downstream code to interpret */
        mdf.error(ss.out, cpe.getMessage(), cpe.getResponseCode(), cpe.getSqlState(), null);
        return;
      }
      if (cpe.getCause() instanceof HiveException) {
        HiveException rc = (HiveException)cpe.getCause();
        mdf.error(ss.out, cpe.getMessage(), rc.getCanonicalErrorMsg().getErrorCode(), cpe.getSqlState(),
            rc.getCanonicalErrorMsg() == ErrorMsg.GENERIC_ERROR ? StringUtils.stringifyException(rc) : null);
      } else {
        ErrorMsg canonicalMsg = ErrorMsg.getErrorMsg(cpe.getCause().getMessage());
        mdf.error(ss.out, cpe.getMessage(), canonicalMsg.getErrorCode(), cpe.getSqlState(),
            StringUtils.stringifyException(cpe.getCause()));
      }
    } catch (HiveException ex) {
      CONSOLE.printError("Unable to JSON-encode the error", StringUtils.stringifyException(ex));
    }
    return;
  }

  @Override
  public CommandProcessorResponse compileAndRespond(String command) throws CommandProcessorException {
    return compileAndRespond(command, false);
  }

  public CommandProcessorResponse compileAndRespond(String command, boolean cleanupTxnList)
      throws CommandProcessorException {
    try {
      compileInternal(command, false);
      return new CommandProcessorResponse(getSchema(), null);
    } catch (CommandProcessorException cpe) {
      throw cpe;
    } finally {
      if (cleanupTxnList) {
        // Valid txn list might be generated for a query compiled using this command, thus we need to reset it
        driverTxnHandler.cleanupTxnList();
      }
    }
  }

  private void compileInternal(String command, boolean deferClose) throws CommandProcessorException {
    Metrics metrics = MetricsFactory.getInstance();
    if (metrics != null) {
      metrics.incrementCounter(MetricsConstant.WAITING_COMPILE_OPS, 1);
    }
    PerfLogger perfLogger = SessionState.getPerfLogger(true);
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.WAIT_COMPILE);

    try (CompileLock compileLock = CompileLockFactory.newInstance(driverContext.getConf(), command)) {
      boolean success = compileLock.tryAcquire();

      perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.WAIT_COMPILE);

      if (metrics != null) {
        metrics.decrementCounter(MetricsConstant.WAITING_COMPILE_OPS, 1);
      }
      if (!success) {
        String errorMessage = ErrorMsg.COMPILE_LOCK_TIMED_OUT.getErrorCodedMsg();
        throw DriverUtils.createProcessorException(driverContext, ErrorMsg.COMPILE_LOCK_TIMED_OUT.getErrorCode(),
            errorMessage, null, null);
      }

      try {
        compile(command, true, deferClose);
      } catch (CommandProcessorException cpe) {
        try {
          driverTxnHandler.endTransactionAndCleanup(false);
        } catch (LockException e) {
          LOG.warn("Exception in releasing locks", e);
        }
        throw cpe;
      }
    }
    //Save compile-time PerfLogging for WebUI.
    //Execution-time Perf logs are done by either another thread's PerfLogger or a reset PerfLogger.
    driverContext.getQueryDisplay().setPerfLogStarts(QueryDisplay.Phase.COMPILATION, perfLogger.getStartTimes());
    driverContext.getQueryDisplay().setPerfLogEnds(QueryDisplay.Phase.COMPILATION, perfLogger.getEndTimes());
  }

  /**
   * Compiles a new HQL command, but potentially resets taskID counter. Not resetting task counter is useful for
   * generating re-entrant QL queries.
   *
   * @param command  The HiveQL query to compile
   * @param resetTaskIds Resets taskID counter if true.
   * @return 0 for ok
   */
  @VisibleForTesting
  public int compile(String command, boolean resetTaskIds) {
    try {
      compile(command, resetTaskIds, false);
      return 0;
    } catch (CommandProcessorException cpr) {
      return cpr.getErrorCode();
    }
  }

  /**
   * Compiles an HQL command, creates an execution plan for it.
   *
   * @param command  The HiveQL query to compile
   * @param resetTaskIds Resets taskID counter if true.
   * @param deferClose indicates if the close/destroy should be deferred when the process has been interrupted, it
   *        should be set to true if the compile is called within another method like runInternal, which defers the
   *        close to the called in that method.
   */
  @VisibleForTesting
  public void compile(String command, boolean resetTaskIds, boolean deferClose) throws CommandProcessorException {
    prepareForCompile(resetTaskIds);

    Compiler compiler = new Compiler(context, driverContext, driverState);
    QueryPlan plan = compiler.compile(command, deferClose);
    driverContext.setPlan(plan);

    compileFinished(deferClose);
  }

  private void prepareForCompile(boolean resetTaskIds) throws CommandProcessorException {
    driverTxnHandler.createTxnManager();
    DriverState.setDriverState(driverState);
    prepareContext();
    setQueryId();

    if (resetTaskIds) {
      TaskFactory.resetId();
    }
  }

  private void prepareContext() throws CommandProcessorException {
    String originalCboInfo = context != null ? context.cboInfo : null;
    if (context != null && context.getExplainAnalyze() != AnalyzeState.RUNNING) {
      // close the existing ctx etc before compiling a new query, but does not destroy driver
      if (!driverContext.isRetrial()) {
        closeInProcess(false);
      } else {
        // On retrial we need to maintain information from the prior context. Such
        // as the currently held locks.
        context = new Context(context);
        releaseResources();
      }
    }

    if (context == null) {
      context = new Context(driverContext.getConf());
      context.setCboInfo(originalCboInfo);
    }

    context.setHiveTxnManager(driverContext.getTxnManager());
    context.setStatsSource(driverContext.getStatsSource());
    context.setHDFSCleanup(true);

    driverTxnHandler.setContext(context);

    if (SessionState.get() != null) {
      QueryState queryState = getQueryState();
      SessionState.get().addQueryState(queryState.getQueryId(), queryState);
    }
  }

  private void setQueryId() {
    String queryId = Strings.isNullOrEmpty(driverContext.getQueryState().getQueryId()) ?
        QueryPlan.makeQueryId() : driverContext.getQueryState().getQueryId();

    driverContext.getQueryDisplay().setQueryId(queryId);

    setTriggerContext(queryId);
  }

  private void setTriggerContext(String queryId) {
    long queryStartTime;
    // query info is created by SQLOperation which will have start time of the operation. When JDBC Statement is not
    // used queryInfo will be null, in which case we take creation of Driver instance as query start time (which is also
    // the time when query display object is created)
    if (driverContext.getQueryInfo() != null) {
      queryStartTime = driverContext.getQueryInfo().getBeginTime();
    } else {
      queryStartTime = driverContext.getQueryDisplay().getQueryStartTime();
    }
    WmContext wmContext = new WmContext(queryStartTime, queryId);
    context.setWmContext(wmContext);
  }

  private void compileFinished(boolean deferClose) {
    if (DriverState.getDriverState().isAborted() && !deferClose) {
      closeInProcess(true);
    }
  }

  /**
   * @return The current query plan associated with this Driver, if any.
   */
  @Override
  public QueryPlan getPlan() {
    return driverContext.getPlan();
  }

  /**
   * @return The current FetchTask associated with the Driver's plan, if any.
   */
  @Override
  public FetchTask getFetchTask() {
    return driverContext.getFetchTask();
  }

  public void releaseLocksAndCommitOrRollback(boolean commit) throws LockException {
    releaseLocksAndCommitOrRollback(commit, driverContext.getTxnManager());
  }

  /**
   * @param commit if there is an open transaction and if true, commit,
   *               if false rollback.  If there is no open transaction this parameter is ignored.
   * @param txnManager an optional existing transaction manager retrieved earlier from the session
   *
   **/
  @VisibleForTesting
  public void releaseLocksAndCommitOrRollback(boolean commit, HiveTxnManager txnManager) throws LockException {
    driverTxnHandler.endTransactionAndCleanup(commit, txnManager);
  }

  /**
   * Release some resources after a query is executed while keeping the result around.
   */
  public void releaseResources() {
    releasePlan();
    releaseTaskQueue();
  }

  public PlanMapper getPlanMapper() {
    return context.getPlanMapper();
  }

  @Override
  public boolean isFetchingTable() {
    return driverContext.getFetchTask() != null;
  }

  @Override
  public Schema getSchema() {
    return driverContext.getSchema();
  }

  @Override
  public boolean hasResultSet() {
    // TODO explain should use a FetchTask for reading
    for (Task<?> task : driverContext.getPlan().getRootTasks()) {
      if (task.getClass() == ExplainTask.class) {
        return true;
      }
    }

    return driverContext.getPlan().getFetchTask() != null && driverContext.getPlan().getResultSchema() != null &&
        driverContext.getPlan().getResultSchema().isSetFieldSchemas();
  }

  @Override
  public void resetFetch() throws IOException {
    if (driverState.isDestroyed() || driverState.isClosed()) {
      throw new IOException("FAILED: driver has been cancelled, closed or destroyed.");
    }
    if (isFetchingTable()) {
      try {
        driverContext.getFetchTask().resetFetch();
      } catch (Exception e) {
        throw new IOException("Error resetting the current fetch task", e);
      }
    } else {
      context.resetStream();
      driverContext.setResStream(null);
    }
  }

  /**
   * Set the maximum number of rows returned by getResults.
   */
  @Override
  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public boolean getResults(List results) throws IOException {
    if (driverState.isDestroyed() || driverState.isClosed()) {
      throw new IOException("FAILED: query has been cancelled, closed, or destroyed.");
    }

    if (isFetchingTable()) {
      return getFetchingTableResults(results);
    }

    if (driverContext.getResStream() == null) {
      // If the driver does not have a stream and neither does the context, return
      DataInput contextStream = context.getStream();
      if (contextStream == null) {
        return false;
      }
      driverContext.setResStream(contextStream);
    }

    int numRows = 0;
    ByteStream.Output bos = new ByteStream.Output();
    while (numRows < maxRows) {
      if (driverContext.getResStream() == null) {
        return (numRows > 0);
      }

      bos.reset();
      Utilities.StreamStatus streamStatus;
      try {
        streamStatus = Utilities.readColumn(driverContext.getResStream(), bos);
        String row = getRow(bos, streamStatus);
        if (row != null) {
          numRows++;
          results.add(row);
        }
      } catch (IOException e) {
        CONSOLE.printError("FAILED: Unexpected IO exception : " + e.getMessage());
        return false;
      }

      if (streamStatus == Utilities.StreamStatus.EOF) {
        driverContext.setResStream(context.getStream());
      }
    }
    return true;
  }

  @SuppressWarnings("rawtypes")
  private boolean getFetchingTableResults(List results) throws IOException {
    // If result set serialization to thrift object is enabled, and if the destination table is indeed written using
    // ThriftJDBCBinarySerDe, read one row from the output sequence file, since it is a blob of row batches.
    if (driverContext.getFetchTask().getWork().isUsingThriftJDBCBinarySerDe()) {
      maxRows = 1;
    }
    driverContext.getFetchTask().setMaxRows(maxRows);
    return driverContext.getFetchTask().fetch(results);
  }

  private String getRow(ByteStream.Output bos, Utilities.StreamStatus streamStatus) {
    final String row;
    if (bos.getLength() > 0) {
      row = new String(bos.getData(), 0, bos.getLength(), StandardCharsets.UTF_8);
    } else if (streamStatus == Utilities.StreamStatus.TERMINATED) {
      row = "";
    } else {
      row = null;
    }
    return row;
  }

  // Close and release resources within a running query process. Since it runs under
  // driver state COMPILING, EXECUTING or INTERRUPT, it would not have race condition
  // with the releases probably running in the other closing thread.
  private int closeInProcess(boolean destroyed) {
    releaseTaskQueue();
    releasePlan();
    releaseCachedResult();
    releaseFetchTask();
    releaseResStream();
    releaseContext();
    if (destroyed) {
      driverTxnHandler.release();
    }
    return 0;
  }

  // is called to stop the query if it is running, clean query results, and release resources.
  @Override
  public void close() {
    driverState.lock();
    try {
      releaseTaskQueue();
      if (driverState.isCompiling() || driverState.isExecuting()) {
        driverState.abort();
      }
      releasePlan();
      releaseContext();
      releaseCachedResult();
      releaseFetchTask();
      releaseResStream();
      driverState.closed();
    } finally {
      driverState.unlock();
      DriverState.removeDriverState();
    }
    destroy();
  }

  // TaskQueue could be released in the query and close processes at same
  // time, which needs to be thread protected.
  private void releaseTaskQueue() {
    driverState.lock();
    try {
      if (taskQueue != null) {
        taskQueue.shutdown();
        taskQueue = null;
      }
    } catch (Exception e) {
      LOG.debug("Exception while shutting down the task runner", e);
    } finally {
      driverState.unlock();
    }
  }

  private void releasePlan() {
    try {
      driverContext.setPlan(null);
    } catch (Exception e) {
      LOG.debug("Exception while clearing the Fetch task", e);
    }
  }

  private void releaseContext() {
    try {
      if (context != null) {
        boolean deleteResultDir = true;
        // don't let context delete result dirs and scratch dirs if result was cached
        if (driverContext.getCacheUsage() != null
            && driverContext.getCacheUsage().getStatus() == CacheUsage.CacheStatus.QUERY_USING_CACHE) {
          deleteResultDir = false;

        }
        context.clear(deleteResultDir);
        if (context.getHiveLocks() != null) {
          driverTxnHandler.addHiveLocksFromContext();
          context.setHiveLocks(null);
        }
        context = null;
      }

      if (SessionState.get() != null) {
        QueryState queryState = getQueryState();
        // If the driver object is reused for several queries, make sure we empty the HMS query cache
        Map<Object, Object> queryCache = SessionState.get().getQueryCache(queryState.getQueryId());
        if (queryCache != null) {
          queryCache.clear();
        }
        queryState.disableHMSCache();
        // Remove any query state reference from the session state
        SessionState.get().removeQueryState(queryState.getQueryId());
      }
    } catch (Exception e) {
      LOG.debug("Exception while clearing the context ", e);
    }
  }

  private void releaseResStream() {
    try {
      if (driverContext.getResStream() != null) {
        ((FSDataInputStream) driverContext.getResStream()).close();
        driverContext.setResStream(null);
      }
    } catch (Exception e) {
      LOG.debug(" Exception while closing the resStream ", e);
    }
  }

  private void releaseFetchTask() {
    try {
      if (driverContext.getFetchTask() != null) {
        driverContext.getFetchTask().clearFetch();
        driverContext.setFetchTask(null);
      }
    } catch (Exception e) {
      LOG.debug(" Exception while clearing the FetchTask ", e);
    }
  }

  private void releaseCachedResult() {
    // Assumes the reader count has been incremented automatically by the results cache by either
    // lookup or creating the cache entry.
    if (driverContext.getUsedCacheEntry() != null) {
      driverContext.getUsedCacheEntry().releaseReader();
      driverContext.setUsedCacheEntry(null);
    } else if (hasBadCacheAttempt()) {
      // This query create a pending cache entry but it was never saved with real results, cleanup.
      // This step is required, as there may be queries waiting on this pending cache entry.
      // Removing/invalidating this entry will notify the waiters that this entry cannot be used.
      try {
        QueryResultsCache.getInstance().removeEntry(driverContext.getCacheUsage().getCacheEntry());
      } catch (Exception err) {
        LOG.error("Error removing failed cache entry " + driverContext.getCacheUsage().getCacheEntry(), err);
      }
    }
    driverContext.setCacheUsage(null);
  }

  private boolean hasBadCacheAttempt() {
    // Check if the query results were cacheable, and created a pending cache entry.
    // If we successfully saved the results, the usage would have changed to QUERY_USING_CACHE.
    return (driverContext.getCacheUsage() != null &&
        driverContext.getCacheUsage().getStatus() == CacheUsage.CacheStatus.CAN_CACHE_QUERY_RESULTS &&
        driverContext.getCacheUsage().getCacheEntry() != null);
  }

  // is usually called after close() to commit or rollback a query and end the driver life cycle.
  // do not understand why it is needed and wonder if it could be combined with close.
  @Override
  public void destroy() {
    driverState.lock();
    try {
      // in the cancel case where the driver state is INTERRUPTED, destroy will be deferred to
      // the query process
      if (driverState.isDestroyed()) {
        return;
      } else {
        driverState.descroyed();
      }
    } finally {
      driverState.unlock();
    }
    driverTxnHandler.destroy(driverContext.getQueryState().getQueryId());
  }

  @Override
  public QueryDisplay getQueryDisplay() {
    return driverContext.getQueryDisplay();
  }

  /**
   * Set the HS2 operation handle's guid string.
   * @param operationId base64 encoded guid string
   */
  @Override
  public void setOperationId(String operationId) {
    driverContext.setOperationId(operationId);
  }

  @Override
  public QueryState getQueryState() {
    return driverContext.getQueryState();
  }

  public HookRunner getHookRunner() {
    return driverContext.getHookRunner();
  }

  public void setStatsSource(StatsSource runtimeStatsSource) {
    driverContext.setStatsSource(runtimeStatsSource);
  }

  public StatsSource getStatsSource() {
    return driverContext.getStatsSource();
  }
}
