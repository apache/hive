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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.cache.results.CacheUsage;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.lock.CompileLock;
import org.apache.hadoop.hive.ql.lock.CompileLockFactory;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.formatting.JsonMetaDataFormatter;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatter;
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.TableDesc;
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
import org.apache.hive.common.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

public class Driver implements IDriver {

  private static final String CLASS_NAME = Driver.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private static final LogHelper CONSOLE = new LogHelper(LOG);
  private static final int SHUTDOWN_HOOK_PRIORITY = 0;
  private Runnable shutdownRunner = null;

  private int maxRows = 100;
  private ByteStream.Output bos = new ByteStream.Output();

  private final DriverContext driverContext;
  private final DriverState driverState = new DriverState();
  private final List<HiveLock> hiveLocks = new ArrayList<HiveLock>();
  private final ValidTxnManager validTxnManager;

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
   * Set the maximum number of rows returned by getResults
   */
  @Override
  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

  @VisibleForTesting
  public Driver(HiveConf conf) {
    this(new QueryState.Builder().withGenerateNewQueryId(true).withHiveConf(conf).build());
  }

  // Pass lineageState when a driver instantiates another Driver to run
  // or compile another query
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
    validTxnManager = new ValidTxnManager(this, driverContext);
  }

  /**
   * Compile a new query, but potentially reset taskID counter.  Not resetting task counter
   * is useful for generating re-entrant QL queries.
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

  // deferClose indicates if the close/destroy should be deferred when the process has been
  // interrupted, it should be set to true if the compile is called within another method like
  // runInternal, which defers the close to the called in that method.
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
    createTransactionManager();
    DriverState.setDriverState(driverState);
    prepareContext();
    setQueryId();

    if (resetTaskIds) {
      TaskFactory.resetId();
    }
  }

  private void createTransactionManager() throws CommandProcessorException {
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

  /**
   * Acquire read and write locks needed by the statement. The list of objects to be locked are
   * obtained from the inputs and outputs populated by the compiler.  Locking strategy depends on
   * HiveTxnManager and HiveLockManager configured
   *
   * This method also records the list of valid transactions.  This must be done after any
   * transactions have been opened.
   * @throws CommandProcessorException
   **/
  private void acquireLocks() throws CommandProcessorException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.ACQUIRE_READ_WRITE_LOCKS);

    if(!driverContext.getTxnManager().isTxnOpen() && driverContext.getTxnManager().supportsAcid()) {
      /*non acid txn managers don't support txns but fwd lock requests to lock managers
        acid txn manager requires all locks to be associated with a txn so if we
        end up here w/o an open txn it's because we are processing something like "use <database>
        which by definition needs no locks*/
      return;
    }
    try {
      String userFromUGI = DriverUtils.getUserFromUGI(driverContext);

      // Set the table write id in all of the acid file sinks
      if (!driverContext.getPlan().getAcidSinks().isEmpty()) {
        List<FileSinkDesc> acidSinks = new ArrayList<>(driverContext.getPlan().getAcidSinks());
        //sorting makes tests easier to write since file names and ROW__IDs depend on statementId
        //so this makes (file name -> data) mapping stable
        acidSinks.sort((FileSinkDesc fsd1, FileSinkDesc fsd2) ->
          fsd1.getDirName().compareTo(fsd2.getDirName()));
        for (FileSinkDesc desc : acidSinks) {
          TableDesc tableInfo = desc.getTableInfo();
          final TableName tn = HiveTableName.ofNullable(tableInfo.getTableName());
          long writeId = driverContext.getTxnManager().getTableWriteId(tn.getDb(), tn.getTable());
          desc.setTableWriteId(writeId);

          /**
           * it's possible to have > 1 FileSink writing to the same table/partition
           * e.g. Merge stmt, multi-insert stmt when mixing DP and SP writes
           * Insert ... Select ... Union All Select ... using
           * {@link org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator#UNION_SUDBIR_PREFIX}
           */
          desc.setStatementId(driverContext.getTxnManager().getStmtIdAndIncrement());
          String unionAllSubdir = "/" + AbstractFileMergeOperator.UNION_SUDBIR_PREFIX;
          if(desc.getInsertOverwrite() && desc.getDirName().toString().contains(unionAllSubdir) &&
              desc.isFullAcidTable()) {
            throw new UnsupportedOperationException("QueryId=" + driverContext.getPlan().getQueryId() +
                " is not supported due to OVERWRITE and UNION ALL.  Please use truncate + insert");
          }
        }
      }

      if (driverContext.getPlan().getAcidAnalyzeTable() != null) {
        // Allocate write ID for the table being analyzed.
        Table t = driverContext.getPlan().getAcidAnalyzeTable().getTable();
        driverContext.getTxnManager().getTableWriteId(t.getDbName(), t.getTableName());
      }


      DDLDescWithWriteId acidDdlDesc = driverContext.getPlan().getAcidDdlDesc();
      boolean hasAcidDdl = acidDdlDesc != null && acidDdlDesc.mayNeedWriteId();
      if (hasAcidDdl) {
        String fqTableName = acidDdlDesc.getFullTableName();
        final TableName tn = HiveTableName.ofNullableWithNoDefault(fqTableName);
        long writeId = driverContext.getTxnManager().getTableWriteId(tn.getDb(), tn.getTable());
        acidDdlDesc.setWriteId(writeId);
      }

      /*It's imperative that {@code acquireLocks()} is called for all commands so that
      HiveTxnManager can transition its state machine correctly*/
      driverContext.getTxnManager().acquireLocks(driverContext.getPlan(), context, userFromUGI, driverState);
      final List<HiveLock> locks = context.getHiveLocks();
      LOG.info("Operation {} obtained {} locks", driverContext.getPlan().getOperation(),
          ((locks == null) ? 0 : locks.size()));
      // This check is for controlling the correctness of the current state
      if (driverContext.getTxnManager().recordSnapshot(driverContext.getPlan()) &&
          !driverContext.isValidTxnListsGenerated()) {
        throw new IllegalStateException(
            "Need to record valid WriteID list but there is no valid TxnID list (" +
                JavaUtils.txnIdToString(driverContext.getTxnManager().getCurrentTxnId()) +
                ", queryId:" + driverContext.getPlan().getQueryId() + ")");
      }

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
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.RELEASE_LOCKS);
    HiveTxnManager txnMgr;
    if (txnManager == null) {
      // Default to driver's txn manager if no txn manager specified
      txnMgr = driverContext.getTxnManager();
    } else {
      txnMgr = txnManager;
    }
    // If we've opened a transaction we need to commit or rollback rather than explicitly
    // releasing the locks.
    driverContext.getConf().unset(ValidTxnList.VALID_TXNS_KEY);
    driverContext.getConf().unset(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY);
    if (!DriverUtils.checkConcurrency(driverContext)) {
      return;
    }
    if (txnMgr.isTxnOpen()) {
      if (commit) {
        if (driverContext.getConf().getBoolVar(ConfVars.HIVE_IN_TEST) &&
            driverContext.getConf().getBoolVar(ConfVars.HIVETESTMODEROLLBACKTXN)) {
          txnMgr.rollbackTxn();
        }
        else {
          txnMgr.commitTxn();//both commit & rollback clear ALL locks for this tx
        }
      } else {
        txnMgr.rollbackTxn();
      }
    } else {
      //since there is no tx, we only have locks for current query (if any)
      if (context != null && context.getHiveLocks() != null) {
        hiveLocks.addAll(context.getHiveLocks());
      }
      txnMgr.releaseLocks(hiveLocks);
    }
    hiveLocks.clear();
    if (context != null) {
      context.setHiveLocks(null);
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.RELEASE_LOCKS);
  }

  /**
   * Release some resources after a query is executed
   * while keeping the result around.
   */
  public void releaseResources() {
    releasePlan();
    releaseTaskQueue();
  }

  @Override
  public CommandProcessorResponse run(String command) throws CommandProcessorException {
    return run(command, false);
  }

  @Override
  public CommandProcessorResponse run() throws CommandProcessorException {
    return run(null, true);
  }

  public CommandProcessorResponse run(String command, boolean alreadyCompiled) throws CommandProcessorException {

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
        // Valid txn list might be generated for a query compiled using this
        // command, thus we need to reset it
        driverContext.getConf().unset(ValidTxnList.VALID_TXNS_KEY);
      }
    }
  }

  public void lockAndRespond() throws CommandProcessorException {
    // Assumes the query has already been compiled
    if (driverContext.getPlan() == null) {
      throw new IllegalStateException(
          "No previously compiled query for driver - queryId=" + driverContext.getQueryState().getQueryId());
    }

    if (requiresLock()) {
      try {
        acquireLocks();
      } catch (CommandProcessorException cpe) {
        rollback(cpe);
        throw cpe;
      }
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
          releaseLocksAndCommitOrRollback(false);
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

      try {
        if (!validTxnManager.isValidTxnListState()) {
          LOG.info("Compiling after acquiring locks");
          // Snapshot was outdated when locks were acquired, hence regenerate context,
          // txn list and retry
          // TODO: Lock acquisition should be moved before analyze, this is a bit hackish.
          // Currently, we acquire a snapshot, we compile the query wrt that snapshot,
          // and then, we acquire locks. If snapshot is still valid, we continue as usual.
          // But if snapshot is not valid, we recompile the query.
          driverContext.setRetrial(true);
          driverContext.getBackupContext().addSubContext(context);
          driverContext.getBackupContext().setHiveLocks(context.getHiveLocks());
          context = driverContext.getBackupContext();
          driverContext.getConf().set(ValidTxnList.VALID_TXNS_KEY,
              driverContext.getTxnManager().getValidTxns().toString());
          if (driverContext.getPlan().hasAcidResourcesInQuery()) {
            validTxnManager.recordValidWriteIds();
          }

          if (!alreadyCompiled) {
            // compile internal will automatically reset the perf logger
            compileInternal(command, true);
          } else {
            // Since we're reusing the compiled plan, we need to update its start time for current run
            driverContext.getPlan().setQueryStartTime(driverContext.getQueryDisplay().getQueryStartTime());
          }

          if (!validTxnManager.isValidTxnListState()) {
            // Throw exception
            throw handleHiveException(new HiveException("Operation could not be executed"), 14);
          }

          //Reset the PerfLogger
          perfLogger = SessionState.getPerfLogger(true);

          // the reason that we set the txn manager for the cxt here is because each
          // query has its own ctx object. The txn mgr is shared across the
          // same instance of Driver, which can run multiple queries.
          context.setHiveTxnManager(driverContext.getTxnManager());
        }
      } catch (LockException e) {
        throw handleHiveException(e, 13);
      }

      try {
        taskQueue = new TaskQueue(context); // for canceling the query (should be bound to session?)
        Executor executor = new Executor(context, driverContext, driverState, taskQueue);
        executor.execute();
      } catch (CommandProcessorException cpe) {
        rollback(cpe);
        throw cpe;
      }

      //if needRequireLock is false, the release here will do nothing because there is no lock
      try {
        //since set autocommit starts an implicit txn, close it
        if (driverContext.getTxnManager().isImplicitTransactionOpen() ||
            driverContext.getPlan().getOperation() == HiveOperation.COMMIT) {
          releaseLocksAndCommitOrRollback(true);
        }
        else if(driverContext.getPlan().getOperation() == HiveOperation.ROLLBACK) {
          releaseLocksAndCommitOrRollback(false);
        } else if (!driverContext.getTxnManager().isTxnOpen() &&
            driverContext.getQueryState().getHiveOperation() == HiveOperation.REPLLOAD) {
          // repl load during migration, commits the explicit txn and start some internal txns. Call
          // releaseLocksAndCommitOrRollback to do the clean up.
          releaseLocksAndCommitOrRollback(false);
        } else {
          //txn (if there is one started) is not finished
        }
      } catch (LockException e) {
        throw handleHiveException(e, 12);
      }

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

      driverState.lock();
      try {
        driverState.executionFinished(isFinishedWithError);
      } finally {
        driverState.unlock();
      }
    }

    SessionState.getPerfLogger().cleanupPerfLogMetrics();
  }

  private void rollback(CommandProcessorException cpe) throws CommandProcessorException {

    //console.printError(cpr.toString());
    try {
      releaseLocksAndCommitOrRollback(false);
    } catch (LockException e) {
      LOG.error("rollback() FAILED: " + cpe); //make sure not to loose
      handleHiveException(e, 12, "Additional info in hive.log at \"rollback() FAILED\"");
    }
  }

  private CommandProcessorException handleHiveException(HiveException e, int ret) throws CommandProcessorException {
    return handleHiveException(e, ret, null);
  }

  private CommandProcessorException handleHiveException(HiveException e, int ret, String rootMsg)
      throws CommandProcessorException {
    String errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
    if(rootMsg != null) {
      errorMessage += "\n" + rootMsg;
    }
    String sqlState = e.getCanonicalErrorMsg() != null ?
        e.getCanonicalErrorMsg().getSQLState() : ErrorMsg.findSQLState(e.getMessage());
    CONSOLE.printError(errorMessage + "\n" + StringUtils.stringifyException(e));
    throw DriverUtils.createProcessorException(driverContext, ret, errorMessage, sqlState, e);
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
    if (!HiveConf.getBoolVar(driverContext.getConf(), ConfVars.HIVE_LOCK_MAPRED_ONLY)) {
      return true;
    }
    if (driverContext.getConf().get(Constants.HIVE_QUERY_EXCLUSIVE_LOCK) != null) {
      return true;
    }
    Queue<Task<?>> tasks = new LinkedList<Task<?>>();
    tasks.addAll(driverContext.getPlan().getRootTasks());
    while (tasks.peek() != null) {
      Task<?> tsk = tasks.remove();
      if (tsk.requireLock()) {
        return true;
      }
      if (tsk instanceof ConditionalTask) {
        tasks.addAll(((ConditionalTask)tsk).getListTasks());
      }
      if (tsk.getChildTasks() != null) {
        tasks.addAll(tsk.getChildTasks());
      }
      // does not add back up task here, because back up task should be the same
      // type of the original task.
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

  @Override
  public boolean isFetchingTable() {
    return driverContext.getFetchTask() != null;
  }

  @SuppressWarnings("unchecked")
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
          hiveLocks.addAll(context.getHiveLocks());
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
      if (!hiveLocks.isEmpty()) {
        try {
          releaseLocksAndCommitOrRollback(false);
        } catch (LockException e) {
          LOG.warn("Exception when releasing locking in destroy: " +
              e.getMessage());
        }
      }
      ShutdownHookManager.removeShutdownHook(shutdownRunner);
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
    boolean isTxnOpen = driverContext != null
        && driverContext.getTxnManager() != null
        && driverContext.getTxnManager().isTxnOpen();
    if (!hiveLocks.isEmpty() || isTxnOpen) {
      try {
        releaseLocksAndCommitOrRollback(false);
      } catch (LockException e) {
        LOG.warn("Exception when releasing locking in destroy: " +
            e.getMessage());
      }
    }
    ShutdownHookManager.removeShutdownHook(shutdownRunner);
  }

  @Override
  public QueryDisplay getQueryDisplay() {
    return driverContext.getQueryDisplay();
  }

  /**
   * Set the HS2 operation handle's guid string
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
