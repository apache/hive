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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
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

  // Exception message that ReExecutionRetryLockPlugin will recognize
  public static final String SNAPSHOT_WAS_OUTDATED_WHEN_LOCKS_WERE_ACQUIRED =
      "snapshot was outdated when locks were acquired";

  private int maxRows = 100;
  private ByteStream.Output bos = new ByteStream.Output();

  private final DriverContext driverContext;
  private final DriverState driverState = new DriverState();
  private final DriverTxnHandler driverTxnHandler;

  private Context context;
  private TaskQueue taskQueue;

  @Override
  public Schema getSchema() {
    return driverContext.getSchema();
  }

  @Override
  public Context getContext() {
    return context;
  }

  public PlanMapper getPlanMapper() {
    return context.getPlanMapper();
  }

  /**
   * Set the maximum number of rows returned by getResults.
   */
  @Override
  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

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

  public Driver(QueryState queryState, QueryInfo queryInfo, HiveTxnManager txnManager,
      ValidWriteIdList compactionWriteIds, long compactorTxnId) {
    this(queryState, queryInfo, txnManager);
    driverContext.setCompactionWriteIds(compactionWriteIds);
    driverContext.setCompactorTxnId(compactorTxnId);
  }

  public Driver(QueryState queryState, QueryInfo queryInfo, HiveTxnManager txnManager) {
    driverContext = new DriverContext(queryState, queryInfo, new HookRunner(queryState.getConf(), CONSOLE),
        txnManager);
    driverTxnHandler = new DriverTxnHandler(this, driverContext, driverState);
  }

  /**
   * Compiles a new HQL command, but potentially resets taskID counter. Not resetting task counter is useful for
   * generating re-entrant QL queries.
   * 
   * @param command  The HiveQL query to compile
   * @param resetTaskIds Resets taskID counter if true.
   * @return 0 for ok
   */
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
   * @param deferClose indicates if the close/destroy should be deferred when the process has been interrupted, it
   *        should be set to true if the compile is called within another method like runInternal, which defers the
   *        close to the called in that method.
   * @param resetTaskIds Resets taskID counter if true.
   */
  @VisibleForTesting
  public void compile(String command, boolean resetTaskIds, boolean deferClose) throws CommandProcessorException {
    preparForCompile(resetTaskIds);

    Compiler compiler = new Compiler(context, driverContext, driverState);
    QueryPlan plan = compiler.compile(command, deferClose);
    driverContext.setPlan(plan);

    compileFinished(deferClose);
  }

  private void compileFinished(boolean deferClose) {
    if (DriverState.getDriverState().isAborted() && !deferClose) {
      closeInProcess(true);
    }
  }

  private void preparForCompile(boolean resetTaskIds) throws CommandProcessorException {
    driverTxnHandler.createTxnManager();
    DriverState.setDriverState(driverState);
    prepareContext();
    setQueryId();

    if (resetTaskIds) {
      TaskFactory.resetId();
    }
  }

  private void prepareContext() throws CommandProcessorException {
    if (context != null && context.getExplainAnalyze() != AnalyzeState.RUNNING) {
      // close the existing ctx etc before compiling a new query, but does not destroy driver
      closeInProcess(false);
    }

    try {
      if (context == null) {
        context = new Context(driverContext.getConf());
      }
    } catch (IOException e) {
      throw new CommandProcessorException(e);
    }

    context.setHiveTxnManager(driverContext.getTxnManager());
    context.setStatsSource(driverContext.getStatsSource());
    context.setHDFSCleanup(true);

    driverTxnHandler.setContext(context);
  }

  private void setQueryId() {
    String queryId = Strings.isNullOrEmpty(driverContext.getQueryState().getQueryId()) ?
        QueryPlan.makeQueryId() : driverContext.getQueryState().getQueryId();

    SparkSession ss = SessionState.get().getSparkSession();
    if (ss != null) {
      ss.onQuerySubmission(queryId);
    }
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

  @Override
  public HiveConf getConf() {
    return driverContext.getConf();
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
   * Release some resources after a query is executed
   * while keeping the result around.
   */
  public void releaseResources() {
    releasePlan();
    releaseTaskQueue();
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
      SessionState ss = SessionState.get();
      if (ss == null) {
        throw cpe;
      }
      MetaDataFormatter mdf = MetaDataFormatUtils.getFormatter(ss.getConf());
      if (!(mdf instanceof JsonMetaDataFormatter)) {
        throw cpe;
      }
      /*Here we want to encode the error in machine readable way (e.g. JSON)
       * Ideally, errorCode would always be set to a canonical error defined in ErrorMsg.
       * In practice that is rarely the case, so the messy logic below tries to tease
       * out canonical error code if it can.  Exclude stack trace from output when
       * the error is a specific/expected one.
       * It's written to stdout for backward compatibility (WebHCat consumes it).*/
      try {
        if (cpe.getCause() == null) {
          mdf.error(ss.out, cpe.getMessage(), cpe.getResponseCode(), cpe.getSqlState());
          throw cpe;
        }
        ErrorMsg canonicalErr = ErrorMsg.getErrorMsg(cpe.getResponseCode());
        if (canonicalErr != null && canonicalErr != ErrorMsg.GENERIC_ERROR) {
          /*Some HiveExceptions (e.g. SemanticException) don't set
            canonical ErrorMsg explicitly, but there is logic
            (e.g. #compile()) to find an appropriate canonical error and
            return its code as error code. In this case we want to
            preserve it for downstream code to interpret*/
          mdf.error(ss.out, cpe.getMessage(), cpe.getResponseCode(), cpe.getSqlState(), null);
          throw cpe;
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
      throw cpe;
    }
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

  private void compileInternal(String command, boolean deferClose) throws CommandProcessorException {
    Metrics metrics = MetricsFactory.getInstance();
    if (metrics != null) {
      metrics.incrementCounter(MetricsConstant.WAITING_COMPILE_OPS, 1);
    }

    PerfLogger perfLogger = SessionState.getPerfLogger(true);
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.WAIT_COMPILE);

    try (CompileLock compileLock = CompileLockFactory.newInstance(driverContext.getConf(), command)) {
      boolean success = compileLock.tryAcquire();

      perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.WAIT_COMPILE);

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
          LOG.warn("Exception in releasing locks. " + StringUtils.stringifyException(e));
        }
        throw cpe;
      }
    }
    //Save compile-time PerfLogging for WebUI.
    //Execution-time Perf logs are done by either another thread's PerfLogger
    //or a reset PerfLogger.
    driverContext.getQueryDisplay().setPerfLogStarts(QueryDisplay.Phase.COMPILATION, perfLogger.getStartTimes());
    driverContext.getQueryDisplay().setPerfLogEnds(QueryDisplay.Phase.COMPILATION, perfLogger.getEndTimes());
  }

  private void runInternal(String command, boolean alreadyCompiled) throws CommandProcessorException {
    DriverState.setDriverState(driverState);

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

    // a flag that helps to set the correct driver state in finally block by tracking if
    // the method has been returned by an error or not.
    boolean isFinishedWithError = true;
    try {
      HiveDriverRunHookContext hookContext = new HiveDriverRunHookContextImpl(driverContext.getConf(),
          alreadyCompiled ? context.getCmd() : command);
      // Get all the driver run hooks and pre-execute them.
      try {
        driverContext.getHookRunner().runPreDriverHooks(hookContext);
      } catch (Exception e) {
        String errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
        CONSOLE.printError(errorMessage + "\n" + StringUtils.stringifyException(e));
        throw DriverUtils.createProcessorException(driverContext, 12, errorMessage,
            ErrorMsg.findSQLState(e.getMessage()), e);
      }

      if (!alreadyCompiled) {
        // compile internal will automatically reset the perf logger
        compileInternal(command, true);
      } else {
        // Since we're reusing the compiled plan, we need to update its start time for current run
        driverContext.getPlan().setQueryStartTime(driverContext.getQueryDisplay().getQueryStartTime());
      }

      //Reset the PerfLogger so that it doesn't retain any previous values.
      // Any value from compilation phase can be obtained through the map set in queryDisplay during compilation.
      PerfLogger perfLogger = SessionState.getPerfLogger(true);

      // the reason that we set the txn manager for the cxt here is because each
      // query has its own ctx object. The txn mgr is shared across the
      // same instance of Driver, which can run multiple queries.
      context.setHiveTxnManager(driverContext.getTxnManager());

      DriverUtils.checkInterrupted(driverState, driverContext, "at acquiring the lock.", null, null);

      lockAndRespond();
      driverTxnHandler.validateTxnListState();

      try {
        taskQueue = new TaskQueue(context); // for canceling the query (should be bound to session?)
        Executor executor = new Executor(context, driverContext, driverState, taskQueue);
        executor.execute();
      } catch (CommandProcessorException cpe) {
        driverTxnHandler.rollback(cpe);
        throw cpe;
      }

      //if needRequireLock is false, the release here will do nothing because there is no lock
      driverTxnHandler.handleTransactionAfterExecution();

      driverContext.getQueryDisplay().setPerfLogStarts(QueryDisplay.Phase.EXECUTION, perfLogger.getStartTimes());
      driverContext.getQueryDisplay().setPerfLogEnds(QueryDisplay.Phase.EXECUTION, perfLogger.getEndTimes());

      // Take all the driver run hooks and post-execute them.
      try {
        driverContext.getHookRunner().runPostDriverHooks(hookContext);
      } catch (Exception e) {
        String errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
        CONSOLE.printError(errorMessage + "\n" + StringUtils.stringifyException(e));
        throw DriverUtils.createProcessorException(driverContext, 12, errorMessage,
            ErrorMsg.findSQLState(e.getMessage()), e);
      }
      isFinishedWithError = false;
    } finally {
      if (driverState.isAborted()) {
        closeInProcess(true);
      } else {
        // only release the related resources ctx, taskQueue as normal
        releaseResources();
      }

      driverState.executionFinishedWithLocking(isFinishedWithError);
    }

    SessionState.getPerfLogger().cleanupPerfLogMetrics();
  }

  @Override
  public boolean isFetchingTable() {
    return driverContext.getFetchTask() != null;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public boolean getResults(List res) throws IOException {
    if (driverState.isDestroyed() || driverState.isClosed()) {
      throw new IOException("FAILED: query has been cancelled, closed, or destroyed.");
    }

    if (isFetchingTable()) {
      /**
       * If resultset serialization to thrift object is enabled, and if the destination table is
       * indeed written using ThriftJDBCBinarySerDe, read one row from the output sequence file,
       * since it is a blob of row batches.
       */
      if (driverContext.getFetchTask().getWork().isUsingThriftJDBCBinarySerDe()) {
        maxRows = 1;
      }
      driverContext.getFetchTask().setMaxRows(maxRows);
      return driverContext.getFetchTask().fetch(res);
    }

    if (driverContext.getResStream() == null) {
      driverContext.setResStream(context.getStream());
    }
    if (driverContext.getResStream() == null) {
      return false;
    }

    int numRows = 0;

    while (numRows < maxRows) {
      final String row;

      if (driverContext.getResStream() == null) {
        return (numRows > 0);
      }

      bos.reset();
      Utilities.StreamStatus ss;
      try {
        ss = Utilities.readColumn(driverContext.getResStream(), bos);
        if (bos.getLength() > 0) {
          row = new String(bos.getData(), 0, bos.getLength(), StandardCharsets.UTF_8);
        } else if (ss == Utilities.StreamStatus.TERMINATED) {
          row = "";
        } else {
          row = null;
        }

        if (row != null) {
          numRows++;
          res.add(row);
        }
      } catch (IOException e) {
        CONSOLE.printError("FAILED: Unexpected IO exception : " + e.getMessage());
        return false;
      }

      if (ss == Utilities.StreamStatus.EOF) {
        driverContext.setResStream(context.getStream());
      }
    }
    return true;
  }

  @Override
  public void resetFetch() throws IOException {
    if (driverState.isDestroyed() || driverState.isClosed()) {
      throw new IOException("FAILED: driver has been cancelled, closed or destroyed.");
    }
    if (isFetchingTable()) {
      try {
        driverContext.getFetchTask().clearFetch();
      } catch (Exception e) {
        throw new IOException("Error closing the current fetch task", e);
      }
      // FetchTask should not depend on the plan.
      driverContext.getFetchTask().initialize(driverContext.getQueryState(), null, null, context);
    } else {
      context.resetStream();
      driverContext.setResStream(null);
    }
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
      if (driverContext.getPlan() != null) {
        FetchTask fetchTask = driverContext.getPlan().getFetchTask();
        if (fetchTask != null) {
          fetchTask.setTaskQueue(null);
          fetchTask.setQueryPlan(null);
        }
        driverContext.setFetchTask(fetchTask);
      }
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

  private boolean hasBadCacheAttempt() {
    // Check if the query results were cacheable, and created a pending cache entry.
    // If we successfully saved the results, the usage would have changed to QUERY_USING_CACHE.
    return (driverContext.getCacheUsage() != null &&
        driverContext.getCacheUsage().getStatus() == CacheUsage.CacheStatus.CAN_CACHE_QUERY_RESULTS &&
        driverContext.getCacheUsage().getCacheEntry() != null);
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
    driverTxnHandler.destroy();
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
}
