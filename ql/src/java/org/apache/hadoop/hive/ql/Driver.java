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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.cache.results.CacheUsage;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache.CacheEntry;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.DagUtils;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.TaskResult;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookUtils;
import org.apache.hadoop.hive.ql.hooks.PrivateHookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.formatting.JsonMetaDataFormatter;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatter;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ColumnAccessInfo;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContextImpl;
import org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.plan.mapper.RuntimeStatsSource;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationUtils;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivObjectActionType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.session.LineageState;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.wm.WmContext;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hive.common.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;


public class Driver implements IDriver {

  static final private String CLASS_NAME = Driver.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  static final private LogHelper console = new LogHelper(LOG);
  static final int SHUTDOWN_HOOK_PRIORITY = 0;
  private final QueryInfo queryInfo;
  private Runnable shutdownRunner = null;

  private int maxRows = 100;
  ByteStream.Output bos = new ByteStream.Output();

  private final HiveConf conf;
  private final boolean isParallelEnabled;
  private DataInput resStream;
  private Context ctx;
  private DriverContext driverCxt;
  private QueryPlan plan;
  private Schema schema;
  private String errorMessage;
  private String SQLState;
  private Throwable downstreamError;

  private FetchTask fetchTask;
  List<HiveLock> hiveLocks = new ArrayList<HiveLock>();

  // A limit on the number of threads that can be launched
  private int maxthreads;

  private String userName;

  // HS2 operation handle guid string
  private String operationId;

  // For WebUI.  Kept alive after queryPlan is freed.
  private final QueryDisplay queryDisplay = new QueryDisplay();
  private LockedDriverState lDrvState = new LockedDriverState();

  // Query specific info
  private final QueryState queryState;

  // Query hooks that execute before compilation and after execution
  private HookRunner hookRunner;

  // Transaction manager the Driver has been initialized with (can be null).
  // If this is set then this Transaction manager will be used during query
  // compilation/execution rather than using the current session's transaction manager.
  // This might be needed in a situation where a Driver is nested within an already
  // running Driver/query - the nested Driver requires a separate transaction manager
  // so as not to conflict with the outer Driver/query which is using the session
  // transaction manager.
  private final HiveTxnManager initTxnMgr;

  // Transaction manager used for the query. This will be set at compile time based on
  // either initTxnMgr or from the SessionState, in that order.
  private HiveTxnManager queryTxnMgr;
  private RuntimeStatsSource runtimeStatsSource;

  // Boolean to store information about whether valid txn list was generated
  // for current query.
  private boolean validTxnListsGenerated;

  private CacheUsage cacheUsage;
  private CacheEntry usedCacheEntry;

  private enum DriverState {
    INITIALIZED,
    COMPILING,
    COMPILED,
    EXECUTING,
    EXECUTED,
    // a state that the driver enters after close() has been called to clean the query results
    // and release the resources after the query has been executed
    CLOSED,
    // a state that the driver enters after destroy() is called and it is the end of driver life cycle
    DESTROYED,
    ERROR
  }

  public static class LockedDriverState {
    // a lock is used for synchronizing the state transition and its associated
    // resource releases
    public final ReentrantLock stateLock = new ReentrantLock();
    public DriverState driverState = DriverState.INITIALIZED;
    public AtomicBoolean aborted = new AtomicBoolean();
    private static ThreadLocal<LockedDriverState> lds = new ThreadLocal<LockedDriverState>() {
      @Override
      protected LockedDriverState initialValue() {
        return new LockedDriverState();
      }
    };

    public static void setLockedDriverState(LockedDriverState lDrv) {
      lds.set(lDrv);
    }

    public static LockedDriverState getLockedDriverState() {
      return lds.get();
    }

    public static void removeLockedDriverState() {
      if (lds != null) {
        lds.remove();
      }
    }

    public boolean isAborted() {
      return aborted.get();
    }

    public void abort() {
      aborted.set(true);
    }

    @Override
    public String toString() {
      return String.format("%s(aborted:%s)", driverState, aborted.get());
    }
  }

  private boolean checkConcurrency() {
    boolean supportConcurrency = conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY);
    if (!supportConcurrency) {
      LOG.info("Concurrency mode is disabled, not creating a lock manager");
      return false;
    }
    return true;
  }

  /**
   * Return the status information about the Map-Reduce cluster
   */
  public ClusterStatus getClusterStatus() throws Exception {
    ClusterStatus cs;
    try {
      JobConf job = new JobConf(conf);
      JobClient jc = new JobClient(job);
      cs = jc.getClusterStatus();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    LOG.info("Returning cluster status: " + cs.toString());
    return cs;
  }


  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public Context getContext() {
    return ctx;
  }

  public PlanMapper getPlanMapper() {
    return ctx.getPlanMapper();
  }

  /**
   * Get a Schema with fields represented with native Hive types
   */
  private static Schema getSchema(BaseSemanticAnalyzer sem, HiveConf conf) {
    Schema schema = null;

    // If we have a plan, prefer its logical result schema if it's
    // available; otherwise, try digging out a fetch task; failing that,
    // give up.
    if (sem == null) {
      // can't get any info without a plan
    } else if (sem.getResultSchema() != null) {
      List<FieldSchema> lst = sem.getResultSchema();
      schema = new Schema(lst, null);
    } else if (sem.getFetchTask() != null) {
      FetchTask ft = sem.getFetchTask();
      TableDesc td = ft.getTblDesc();
      // partitioned tables don't have tableDesc set on the FetchTask. Instead
      // they have a list of PartitionDesc objects, each with a table desc.
      // Let's
      // try to fetch the desc for the first partition and use it's
      // deserializer.
      if (td == null && ft.getWork() != null && ft.getWork().getPartDesc() != null) {
        if (ft.getWork().getPartDesc().size() > 0) {
          td = ft.getWork().getPartDesc().get(0).getTableDesc();
        }
      }

      if (td == null) {
        LOG.info("No returning schema.");
      } else {
        String tableName = "result";
        List<FieldSchema> lst = null;
        try {
          lst = HiveMetaStoreUtils.getFieldsFromDeserializer(tableName, td.getDeserializer(conf));
        } catch (Exception e) {
          LOG.warn("Error getting schema: "
              + org.apache.hadoop.util.StringUtils.stringifyException(e));
        }
        if (lst != null) {
          schema = new Schema(lst, null);
        }
      }
    }
    if (schema == null) {
      schema = new Schema();
    }
    LOG.info("Returning Hive schema: " + schema);
    return schema;
  }

  /**
   * Get a Schema with fields represented with Thrift DDL types
   */
  public Schema getThriftSchema() throws Exception {
    Schema schema;
    try {
      schema = getSchema();
      if (schema != null) {
        List<FieldSchema> lst = schema.getFieldSchemas();
        // Go over the schema and convert type to thrift type
        if (lst != null) {
          for (FieldSchema f : lst) {
            f.setType(ColumnType.typeToThriftType(f.getType()));
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    LOG.info("Returning Thrift schema: " + schema);
    return schema;
  }

  /**
   * Return the maximum number of rows returned by getResults
   */
  public int getMaxRows() {
    return maxRows;
  }

  /**
   * Set the maximum number of rows returned by getResults
   */
  @Override
  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

  public Driver(HiveConf conf) {
    this(new QueryState.Builder().withGenerateNewQueryId(true).withHiveConf(conf).build(), null);
  }

  // Pass lineageState when a driver instantiates another Driver to run
  // or compile another query
  // NOTE: only used from index related classes
  public Driver(HiveConf conf, LineageState lineageState) {
    this(getNewQueryState(conf, lineageState), null);
  }

  // Pass lineageState when a driver instantiates another Driver to run
  // or compile another query
  public Driver(HiveConf conf, Context ctx, LineageState lineageState) {
    this(getNewQueryState(conf, lineageState), null, null);
    this.ctx = ctx;
  }

  // Pass lineageState when a driver instantiates another Driver to run
  // or compile another query
  public Driver(HiveConf conf, String userName, LineageState lineageState) {
    this(getNewQueryState(conf, lineageState), userName, null);
  }

  public Driver(QueryState queryState, String userName) {
    this(queryState, userName, null, null);
  }

  public Driver(QueryState queryState, String userName, QueryInfo queryInfo) {
    this(queryState, userName, queryInfo, null);
  }

  public Driver(QueryState queryState, String userName, QueryInfo queryInfo, HiveTxnManager txnMgr) {
    this.queryState = queryState;
    this.conf = queryState.getConf();
    isParallelEnabled = (conf != null)
        && HiveConf.getBoolVar(conf, ConfVars.HIVE_SERVER2_PARALLEL_COMPILATION);
    this.userName = userName;
    this.hookRunner = new HookRunner(conf, console);
    this.queryInfo = queryInfo;
    this.initTxnMgr = txnMgr;
  }

  /**
   * Generating the new QueryState object. Making sure, that the new queryId is generated.
   * @param conf The HiveConf which should be used
   * @param lineageState a LineageState to be set in the new QueryState object
   * @return The new QueryState object
   */
  private static QueryState getNewQueryState(HiveConf conf, LineageState lineageState) {
    return new QueryState.Builder()
        .withGenerateNewQueryId(true)
        .withHiveConf(conf)
        .withLineageState(lineageState)
        .build();
  }

  /**
   * Compile a new query. Any currently-planned query associated with this Driver is discarded.
   * Do not reset id for inner queries(index, etc). Task ids are used for task identity check.
   *
   * @param command
   *          The SQL query to compile.
   */
  @Override
  public int compile(String command) {
    return compile(command, true);
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
    } catch (CommandProcessorResponse cpr) {
      return cpr.getErrorCode();
    }
  }

  // deferClose indicates if the close/destroy should be deferred when the process has been
  // interrupted, it should be set to true if the compile is called within another method like
  // runInternal, which defers the close to the called in that method.
  private void compile(String command, boolean resetTaskIds, boolean deferClose) throws CommandProcessorResponse {
    PerfLogger perfLogger = SessionState.getPerfLogger(true);
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.DRIVER_RUN);
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.COMPILE);
    lDrvState.stateLock.lock();
    try {
      lDrvState.driverState = DriverState.COMPILING;
    } finally {
      lDrvState.stateLock.unlock();
    }

    command = new VariableSubstitution(new HiveVariableSource() {
      @Override
      public Map<String, String> getHiveVariable() {
        return SessionState.get().getHiveVariables();
      }
    }).substitute(conf, command);

    String queryStr = command;

    try {
      // command should be redacted to avoid to logging sensitive data
      queryStr = HookUtils.redactLogString(conf, command);
    } catch (Exception e) {
      LOG.warn("WARNING! Query command could not be redacted." + e);
    }

    checkInterrupted("at beginning of compilation.", null, null);

    if (ctx != null && ctx.getExplainAnalyze() != AnalyzeState.RUNNING) {
      // close the existing ctx etc before compiling a new query, but does not destroy driver
      closeInProcess(false);
    }

    if (resetTaskIds) {
      TaskFactory.resetId();
    }

    LockedDriverState.setLockedDriverState(lDrvState);

    String queryId = queryState.getQueryId();

    if (ctx != null) {
      setTriggerContext(queryId);
    }
    //save some info for webUI for use after plan is freed
    this.queryDisplay.setQueryStr(queryStr);
    this.queryDisplay.setQueryId(queryId);

    LOG.info("Compiling command(queryId=" + queryId + "): " + queryStr);

    conf.setQueryString(queryStr);
    // FIXME: sideeffect will leave the last query set at the session level
    SessionState.get().getConf().setQueryString(queryStr);
    SessionState.get().setupQueryCurrentTimestamp();

    // Whether any error occurred during query compilation. Used for query lifetime hook.
    boolean compileError = false;
    boolean parseError = false;

    try {

      // Initialize the transaction manager.  This must be done before analyze is called.
      if (initTxnMgr != null) {
        queryTxnMgr = initTxnMgr;
      } else {
        queryTxnMgr = SessionState.get().initTxnMgr(conf);
      }
      if (queryTxnMgr instanceof Configurable) {
        ((Configurable) queryTxnMgr).setConf(conf);
      }
      queryState.setTxnManager(queryTxnMgr);

      // In case when user Ctrl-C twice to kill Hive CLI JVM, we want to release locks
      // if compile is being called multiple times, clear the old shutdownhook
      ShutdownHookManager.removeShutdownHook(shutdownRunner);
      final HiveTxnManager txnMgr = queryTxnMgr;
      shutdownRunner = new Runnable() {
        @Override
        public void run() {
          try {
            releaseLocksAndCommitOrRollback(false, txnMgr);
          } catch (LockException e) {
            LOG.warn("Exception when releasing locks in ShutdownHook for Driver: " +
                e.getMessage());
          }
        }
      };
      ShutdownHookManager.addShutdownHook(shutdownRunner, SHUTDOWN_HOOK_PRIORITY);

      checkInterrupted("before parsing and analysing the query", null, null);

      if (ctx == null) {
        ctx = new Context(conf);
        setTriggerContext(queryId);
      }

      ctx.setRuntimeStatsSource(runtimeStatsSource);
      ctx.setCmd(command);
      ctx.setHDFSCleanup(true);

      perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.PARSE);

      // Trigger query hook before compilation
      hookRunner.runBeforeParseHook(command);

      ASTNode tree;
      try {
        tree = ParseUtils.parse(command, ctx);
      } catch (ParseException e) {
        parseError = true;
        throw e;
      } finally {
        hookRunner.runAfterParseHook(command, parseError);
      }
      perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.PARSE);

      hookRunner.runBeforeCompileHook(command);

      perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.ANALYZE);

      // Flush the metastore cache.  This assures that we don't pick up objects from a previous
      // query running in this same thread.  This has to be done after we get our semantic
      // analyzer (this is when the connection to the metastore is made) but before we analyze,
      // because at that point we need access to the objects.
      Hive.get().getMSC().flushCache();

      BaseSemanticAnalyzer sem;
      // Do semantic analysis and plan generation
      if (hookRunner.hasPreAnalyzeHooks()) {
        HiveSemanticAnalyzerHookContext hookCtx = new HiveSemanticAnalyzerHookContextImpl();
        hookCtx.setConf(conf);
        hookCtx.setUserName(userName);
        hookCtx.setIpAddress(SessionState.get().getUserIpAddress());
        hookCtx.setCommand(command);
        hookCtx.setHiveOperation(queryState.getHiveOperation());

        tree =  hookRunner.runPreAnalyzeHooks(hookCtx, tree);
        sem = SemanticAnalyzerFactory.get(queryState, tree);
        openTransaction();
        // TODO: Lock acquisition should be moved before this method call
        // when we want to implement lock-based concurrency control
        generateValidTxnList();
        sem.analyze(tree, ctx);
        hookCtx.update(sem);

        hookRunner.runPostAnalyzeHooks(hookCtx, sem.getAllRootTasks());
      } else {
        sem = SemanticAnalyzerFactory.get(queryState, tree);
        openTransaction();
        // TODO: Lock acquisition should be moved before this method call
        // when we want to implement lock-based concurrency control
        generateValidTxnList();
        sem.analyze(tree, ctx);
      }
      LOG.info("Semantic Analysis Completed");

      // Retrieve information about cache usage for the query.
      if (conf.getBoolVar(HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_ENABLED)) {
        cacheUsage = sem.getCacheUsage();
      }

      // validate the plan
      sem.validate();
      perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.ANALYZE);

      checkInterrupted("after analyzing query.", null, null);

      // get the output schema
      schema = getSchema(sem, conf);
      plan = new QueryPlan(queryStr, sem, perfLogger.getStartTime(PerfLogger.DRIVER_RUN), queryId,
        queryState.getHiveOperation(), schema);


      conf.set("mapreduce.workflow.id", "hive_" + queryId);
      conf.set("mapreduce.workflow.name", queryStr);

      // initialize FetchTask right here
      if (plan.getFetchTask() != null) {
        plan.getFetchTask().initialize(queryState, plan, null, ctx.getOpContext());
      }

      //do the authorization check
      if (!sem.skipAuthorization() &&
          HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)) {

        try {
          perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.DO_AUTHORIZATION);
          doAuthorization(queryState.getHiveOperation(), sem, command);
        } catch (AuthorizationException authExp) {
          console.printError("Authorization failed:" + authExp.getMessage()
              + ". Use SHOW GRANT to get more details.");
          errorMessage = authExp.getMessage();
          SQLState = "42000";
          throw createProcessorResponse(403);
        } finally {
          perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.DO_AUTHORIZATION);
        }
      }

      if (conf.getBoolVar(ConfVars.HIVE_LOG_EXPLAIN_OUTPUT)) {
        String explainOutput = getExplainOutput(sem, plan, tree);
        if (explainOutput != null) {
          LOG.info("EXPLAIN output for queryid " + queryId + " : "
            + explainOutput);
          if (conf.isWebUiQueryInfoCacheEnabled()) {
            queryDisplay.setExplainPlan(explainOutput);
          }
        }
      }
    } catch (CommandProcessorResponse cpr) {
      throw cpr;
    } catch (Exception e) {
      checkInterrupted("during query compilation: " + e.getMessage(), null, null);

      compileError = true;
      ErrorMsg error = ErrorMsg.getErrorMsg(e.getMessage());
      errorMessage = "FAILED: " + e.getClass().getSimpleName();
      if (error != ErrorMsg.GENERIC_ERROR) {
        errorMessage += " [Error "  + error.getErrorCode()  + "]:";
      }

      // HIVE-4889
      if ((e instanceof IllegalArgumentException) && e.getMessage() == null && e.getCause() != null) {
        errorMessage += " " + e.getCause().getMessage();
      } else {
        errorMessage += " " + e.getMessage();
      }

      if (error == ErrorMsg.TXNMGR_NOT_ACID) {
        errorMessage += ". Failed command: " + queryStr;
      }

      SQLState = error.getSQLState();
      downstreamError = e;
      console.printError(errorMessage, "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw createProcessorResponse(error.getErrorCode());
    } finally {
      // Trigger post compilation hook. Note that if the compilation fails here then
      // before/after execution hook will never be executed.
      if (!parseError) {
        try {
          hookRunner.runAfterCompilationHook(command, compileError);
        } catch (Exception e) {
          LOG.warn("Failed when invoking query after-compilation hook.", e);
        }
      }

      double duration = perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.COMPILE)/1000.00;
      ImmutableMap<String, Long> compileHMSTimings = dumpMetaCallTimingWithoutEx("compilation");
      queryDisplay.setHmsTimings(QueryDisplay.Phase.COMPILATION, compileHMSTimings);

      boolean isInterrupted = lDrvState.isAborted();
      if (isInterrupted && !deferClose) {
        closeInProcess(true);
      }
      lDrvState.stateLock.lock();
      try {
        if (isInterrupted) {
          lDrvState.driverState = deferClose ? DriverState.EXECUTING : DriverState.ERROR;
        } else {
          lDrvState.driverState = compileError ? DriverState.ERROR : DriverState.COMPILED;
        }
      } finally {
        lDrvState.stateLock.unlock();
      }

      if (isInterrupted) {
        LOG.info("Compiling command(queryId=" + queryId + ") has been interrupted after " + duration + " seconds");
      } else {
        LOG.info("Completed compiling command(queryId=" + queryId + "); Time taken: " + duration + " seconds");
      }
    }
  }

  private void setTriggerContext(final String queryId) {
    final long queryStartTime;
    // query info is created by SQLOperation which will have start time of the operation. When JDBC Statement is not
    // used queryInfo will be null, in which case we take creation of Driver instance as query start time (which is also
    // the time when query display object is created)
    if (queryInfo != null) {
      queryStartTime = queryInfo.getBeginTime();
    } else {
      queryStartTime = queryDisplay.getQueryStartTime();
    }
    WmContext wmContext = new WmContext(queryStartTime, queryId);
    ctx.setWmContext(wmContext);
  }

  private void openTransaction() throws LockException, CommandProcessorResponse {
    if (checkConcurrency() && startImplicitTxn(queryTxnMgr)) {
      String userFromUGI = getUserFromUGI();
      if (!queryTxnMgr.isTxnOpen()) {
        if (userFromUGI == null) {
          throw createProcessorResponse(10);
        }
        long txnid = queryTxnMgr.openTxn(ctx, userFromUGI);
      }
    }
  }

  private void generateValidTxnList() throws LockException {
    // Record current valid txn list that will be used throughout the query
    // compilation and processing. We only do this if 1) a transaction
    // was already opened and 2) the list has not been recorded yet,
    // e.g., by an explicit open transaction command.
    validTxnListsGenerated = false;
    String currentTxnString = conf.get(ValidTxnList.VALID_TXNS_KEY);
    if (queryTxnMgr.isTxnOpen() && (currentTxnString == null || currentTxnString.isEmpty())) {
      try {
        recordValidTxns(queryTxnMgr);
        validTxnListsGenerated = true;
      } catch (LockException e) {
        LOG.error("Exception while acquiring valid txn list", e);
        throw e;
      }
    }
  }

  private boolean startImplicitTxn(HiveTxnManager txnManager) throws LockException {
    boolean shouldOpenImplicitTxn = !ctx.isExplainPlan();
    //this is dumb. HiveOperation is not always set. see HIVE-16447/HIVE-16443
    switch (queryState.getHiveOperation() == null ? HiveOperation.QUERY : queryState.getHiveOperation()) {
      case COMMIT:
      case ROLLBACK:
        if(!txnManager.isTxnOpen()) {
          throw new LockException(null, ErrorMsg.OP_NOT_ALLOWED_WITHOUT_TXN, queryState.getHiveOperation().getOperationName());
        }
      case SWITCHDATABASE:
      case SET_AUTOCOMMIT:
        /**
         * autocommit is here for completeness.  TM doesn't use it.  If we want to support JDBC
         * semantics (or any other definition of autocommit) it should be done at session level.
         */
      case SHOWDATABASES:
      case SHOWTABLES:
      case SHOWCOLUMNS:
      case SHOWFUNCTIONS:
      case SHOWPARTITIONS:
      case SHOWLOCKS:
      case SHOWVIEWS:
      case SHOW_ROLES:
      case SHOW_ROLE_PRINCIPALS:
      case SHOW_COMPACTIONS:
      case SHOW_TRANSACTIONS:
      case ABORT_TRANSACTIONS:
      case KILL_QUERY:
        shouldOpenImplicitTxn = false;
        //this implies that no locks are needed for such a command
    }
    return shouldOpenImplicitTxn;
  }

  private int handleInterruptionWithHook(String msg, HookContext hookContext,
      PerfLogger perfLogger) {
    SQLState = "HY008";  //SQLState for cancel operation
    errorMessage = "FAILED: command has been interrupted: " + msg;
    console.printError(errorMessage);
    if (hookContext != null) {
      try {
        invokeFailureHooks(perfLogger, hookContext, errorMessage, null);
      } catch (Exception e) {
        LOG.warn("Caught exception attempting to invoke Failure Hooks", e);
      }
    }
    return 1000;
  }

  private void checkInterrupted(String msg, HookContext hookContext, PerfLogger perfLogger) throws CommandProcessorResponse {
    if (lDrvState.isAborted()) {
      throw createProcessorResponse(handleInterruptionWithHook(msg, hookContext, perfLogger));
    }
  }

  private ImmutableMap<String, Long> dumpMetaCallTimingWithoutEx(String phase) {
    try {
      return Hive.get().dumpAndClearMetaCallTiming(phase);
    } catch (HiveException he) {
      LOG.warn("Caught exception attempting to write metadata call information " + he, he);
    }
    return null;
  }

  /**
   * Returns EXPLAIN EXTENDED output for a semantically
   * analyzed query.
   *
   * @param sem semantic analyzer for analyzed query
   * @param plan query plan
   * @param astTree AST tree dump
   * @throws java.io.IOException
   */
  private String getExplainOutput(BaseSemanticAnalyzer sem, QueryPlan plan,
      ASTNode astTree) throws IOException {
    String ret = null;
    ExplainTask task = new ExplainTask();
    task.initialize(queryState, plan, null, ctx.getOpContext());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    try {
      List<Task<?>> rootTasks = sem.getAllRootTasks();
      task.getJSONPlan(ps, rootTasks, sem.getFetchTask(), false, true, true);
      ret = baos.toString();
    } catch (Exception e) {
      LOG.warn("Exception generating explain output: " + e, e);
    }

    return ret;
  }

  /**
   * Do authorization using post semantic analysis information in the semantic analyzer
   * The original command is also passed so that authorization interface can provide
   * more useful information in logs.
   * @param sem SemanticAnalyzer used to parse input query
   * @param command input query
   * @throws HiveException
   * @throws AuthorizationException
   */
  public static void doAuthorization(HiveOperation op, BaseSemanticAnalyzer sem, String command)
      throws HiveException, AuthorizationException {
    SessionState ss = SessionState.get();
    Hive db = sem.getDb();

    Set<ReadEntity> additionalInputs = new HashSet<ReadEntity>();
    for (Entity e : sem.getInputs()) {
      if (e.getType() == Entity.Type.PARTITION) {
        additionalInputs.add(new ReadEntity(e.getTable()));
      }
    }

    Set<WriteEntity> additionalOutputs = new HashSet<WriteEntity>();
    for (WriteEntity e : sem.getOutputs()) {
      if (e.getType() == Entity.Type.PARTITION) {
        additionalOutputs.add(new WriteEntity(e.getTable(), e.getWriteType()));
      }
    }

    // The following union operation returns a union, which traverses over the
    // first set once and then  then over each element of second set, in order,
    // that is not contained in first. This means it doesn't replace anything
    // in first set, and would preserve the WriteType in WriteEntity in first
    // set in case of outputs list.
    Set<ReadEntity> inputs = Sets.union(sem.getInputs(), additionalInputs);
    Set<WriteEntity> outputs = Sets.union(sem.getOutputs(), additionalOutputs);

    if (ss.isAuthorizationModeV2()) {
      // get mapping of tables to columns used
      ColumnAccessInfo colAccessInfo = sem.getColumnAccessInfo();
      // colAccessInfo is set only in case of SemanticAnalyzer
      Map<String, List<String>> selectTab2Cols = colAccessInfo != null ? colAccessInfo
          .getTableToColumnAccessMap() : null;
      Map<String, List<String>> updateTab2Cols = sem.getUpdateColumnAccessInfo() != null ?
          sem.getUpdateColumnAccessInfo().getTableToColumnAccessMap() : null;
      doAuthorizationV2(ss, op, inputs, outputs, command, selectTab2Cols, updateTab2Cols);
     return;
    }
    if (op == null) {
      throw new HiveException("Operation should not be null");
    }
    HiveAuthorizationProvider authorizer = ss.getAuthorizer();
    if (op.equals(HiveOperation.CREATEDATABASE)) {
      authorizer.authorize(
          op.getInputRequiredPrivileges(), op.getOutputRequiredPrivileges());
    } else if (op.equals(HiveOperation.CREATETABLE_AS_SELECT)
        || op.equals(HiveOperation.CREATETABLE)) {
      authorizer.authorize(
          db.getDatabase(SessionState.get().getCurrentDatabase()), null,
          HiveOperation.CREATETABLE_AS_SELECT.getOutputRequiredPrivileges());
    } else {
      if (op.equals(HiveOperation.IMPORT)) {
        ImportSemanticAnalyzer isa = (ImportSemanticAnalyzer) sem;
        if (!isa.existsTable()) {
          authorizer.authorize(
              db.getDatabase(SessionState.get().getCurrentDatabase()), null,
              HiveOperation.CREATETABLE_AS_SELECT.getOutputRequiredPrivileges());
        }
      }
    }
    if (outputs != null && outputs.size() > 0) {
      for (WriteEntity write : outputs) {
        if (write.isDummy() || write.isPathType()) {
          continue;
        }
        if (write.getType() == Entity.Type.DATABASE) {
          if (!op.equals(HiveOperation.IMPORT)){
            // We skip DB check for import here because we already handle it above
            // as a CTAS check.
            authorizer.authorize(write.getDatabase(),
                null, op.getOutputRequiredPrivileges());
          }
          continue;
        }

        if (write.getType() == WriteEntity.Type.PARTITION) {
          Partition part = db.getPartition(write.getTable(), write
              .getPartition().getSpec(), false);
          if (part != null) {
            authorizer.authorize(write.getPartition(), null,
                    op.getOutputRequiredPrivileges());
            continue;
          }
        }

        if (write.getTable() != null) {
          authorizer.authorize(write.getTable(), null,
                  op.getOutputRequiredPrivileges());
        }
      }
    }

    if (inputs != null && inputs.size() > 0) {
      Map<Table, List<String>> tab2Cols = new HashMap<Table, List<String>>();
      Map<Partition, List<String>> part2Cols = new HashMap<Partition, List<String>>();

      //determine if partition level privileges should be checked for input tables
      Map<String, Boolean> tableUsePartLevelAuth = new HashMap<String, Boolean>();
      for (ReadEntity read : inputs) {
        if (read.isDummy() || read.isPathType() || read.getType() == Entity.Type.DATABASE) {
          continue;
        }
        Table tbl = read.getTable();
        if ((read.getPartition() != null) || (tbl != null && tbl.isPartitioned())) {
          String tblName = tbl.getTableName();
          if (tableUsePartLevelAuth.get(tblName) == null) {
            boolean usePartLevelPriv = (tbl.getParameters().get(
                "PARTITION_LEVEL_PRIVILEGE") != null && ("TRUE"
                .equalsIgnoreCase(tbl.getParameters().get(
                    "PARTITION_LEVEL_PRIVILEGE"))));
            if (usePartLevelPriv) {
              tableUsePartLevelAuth.put(tblName, Boolean.TRUE);
            } else {
              tableUsePartLevelAuth.put(tblName, Boolean.FALSE);
            }
          }
        }
      }

      // column authorization is checked through table scan operators.
      getTablePartitionUsedColumns(op, sem, tab2Cols, part2Cols, tableUsePartLevelAuth);

      // cache the results for table authorization
      Set<String> tableAuthChecked = new HashSet<String>();
      for (ReadEntity read : inputs) {
        // if read is not direct, we do not need to check its autho.
        if (read.isDummy() || read.isPathType() || !read.isDirect()) {
          continue;
        }
        if (read.getType() == Entity.Type.DATABASE) {
          authorizer.authorize(read.getDatabase(), op.getInputRequiredPrivileges(), null);
          continue;
        }
        Table tbl = read.getTable();
        if (tbl.isView() && sem instanceof SemanticAnalyzer) {
          tab2Cols.put(tbl,
              sem.getColumnAccessInfo().getTableToColumnAccessMap().get(tbl.getCompleteName()));
        }
        if (read.getPartition() != null) {
          Partition partition = read.getPartition();
          tbl = partition.getTable();
          // use partition level authorization
          if (Boolean.TRUE.equals(tableUsePartLevelAuth.get(tbl.getTableName()))) {
            List<String> cols = part2Cols.get(partition);
            if (cols != null && cols.size() > 0) {
              authorizer.authorize(partition.getTable(),
                  partition, cols, op.getInputRequiredPrivileges(),
                  null);
            } else {
              authorizer.authorize(partition,
                  op.getInputRequiredPrivileges(), null);
            }
            continue;
          }
        }

        // if we reach here, it means it needs to do a table authorization
        // check, and the table authorization may already happened because of other
        // partitions
        if (tbl != null && !tableAuthChecked.contains(tbl.getTableName()) &&
            !(Boolean.TRUE.equals(tableUsePartLevelAuth.get(tbl.getTableName())))) {
          List<String> cols = tab2Cols.get(tbl);
          if (cols != null && cols.size() > 0) {
            authorizer.authorize(tbl, null, cols,
                op.getInputRequiredPrivileges(), null);
          } else {
            authorizer.authorize(tbl, op.getInputRequiredPrivileges(),
                null);
          }
          tableAuthChecked.add(tbl.getTableName());
        }
      }

    }
  }

  private static void getTablePartitionUsedColumns(HiveOperation op, BaseSemanticAnalyzer sem,
      Map<Table, List<String>> tab2Cols, Map<Partition, List<String>> part2Cols,
      Map<String, Boolean> tableUsePartLevelAuth) throws HiveException {
    // for a select or create-as-select query, populate the partition to column
    // (par2Cols) or
    // table to columns mapping (tab2Cols)
    if (op.equals(HiveOperation.CREATETABLE_AS_SELECT) || op.equals(HiveOperation.QUERY)) {
      SemanticAnalyzer querySem = (SemanticAnalyzer) sem;
      ParseContext parseCtx = querySem.getParseContext();

      for (Map.Entry<String, TableScanOperator> topOpMap : querySem.getParseContext().getTopOps()
          .entrySet()) {
        TableScanOperator tableScanOp = topOpMap.getValue();
        if (!tableScanOp.isInsideView()) {
          Table tbl = tableScanOp.getConf().getTableMetadata();
          List<Integer> neededColumnIds = tableScanOp.getNeededColumnIDs();
          List<FieldSchema> columns = tbl.getCols();
          List<String> cols = new ArrayList<String>();
          for (int i = 0; i < neededColumnIds.size(); i++) {
            cols.add(columns.get(neededColumnIds.get(i)).getName());
          }
          // map may not contain all sources, since input list may have been
          // optimized out
          // or non-existent tho such sources may still be referenced by the
          // TableScanOperator
          // if it's null then the partition probably doesn't exist so let's use
          // table permission
          if (tbl.isPartitioned()
              && Boolean.TRUE.equals(tableUsePartLevelAuth.get(tbl.getTableName()))) {
            String alias_id = topOpMap.getKey();

            PrunedPartitionList partsList = PartitionPruner.prune(tableScanOp, parseCtx, alias_id);
            Set<Partition> parts = partsList.getPartitions();
            for (Partition part : parts) {
              List<String> existingCols = part2Cols.get(part);
              if (existingCols == null) {
                existingCols = new ArrayList<String>();
              }
              existingCols.addAll(cols);
              part2Cols.put(part, existingCols);
            }
          } else {
            List<String> existingCols = tab2Cols.get(tbl);
            if (existingCols == null) {
              existingCols = new ArrayList<String>();
            }
            existingCols.addAll(cols);
            tab2Cols.put(tbl, existingCols);
          }
        }
      }
    }
  }

  private static void doAuthorizationV2(SessionState ss, HiveOperation op, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, String command, Map<String, List<String>> tab2cols,
      Map<String, List<String>> updateTab2Cols) throws HiveException {

    /* comment for reviewers -> updateTab2Cols needed to be separate from tab2cols because if I
    pass tab2cols to getHivePrivObjects for the output case it will trip up insert/selects,
    since the insert will get passed the columns from the select.
     */

    HiveAuthzContext.Builder authzContextBuilder = new HiveAuthzContext.Builder();
    authzContextBuilder.setUserIpAddress(ss.getUserIpAddress());
    authzContextBuilder.setForwardedAddresses(ss.getForwardedAddresses());
    authzContextBuilder.setCommandString(command);

    HiveOperationType hiveOpType = getHiveOperationType(op);
    List<HivePrivilegeObject> inputsHObjs = getHivePrivObjects(inputs, tab2cols);
    List<HivePrivilegeObject> outputHObjs = getHivePrivObjects(outputs, updateTab2Cols);

    ss.getAuthorizerV2().checkPrivileges(hiveOpType, inputsHObjs, outputHObjs, authzContextBuilder.build());
  }

  private static List<HivePrivilegeObject> getHivePrivObjects(
      Set<? extends Entity> privObjects, Map<String, List<String>> tableName2Cols) {
    List<HivePrivilegeObject> hivePrivobjs = new ArrayList<HivePrivilegeObject>();
    if(privObjects == null){
      return hivePrivobjs;
    }
    for(Entity privObject : privObjects){
      HivePrivilegeObjectType privObjType =
          AuthorizationUtils.getHivePrivilegeObjectType(privObject.getType());
      if(privObject.isDummy()) {
        //do not authorize dummy readEntity or writeEntity
        continue;
      }
      if(privObject instanceof ReadEntity && !((ReadEntity)privObject).isDirect()){
        // In case of views, the underlying views or tables are not direct dependencies
        // and are not used for authorization checks.
        // This ReadEntity represents one of the underlying tables/views, so skip it.
        // See description of the isDirect in ReadEntity
        continue;
      }
      if(privObject instanceof WriteEntity && ((WriteEntity)privObject).isTempURI()){
        //do not authorize temporary uris
        continue;
      }
      //support for authorization on partitions needs to be added
      String dbname = null;
      String objName = null;
      List<String> partKeys = null;
      List<String> columns = null;
      String className = null;
      switch(privObject.getType()){
      case DATABASE:
        dbname = privObject.getDatabase().getName();
        break;
      case TABLE:
        dbname = privObject.getTable().getDbName();
        objName = privObject.getTable().getTableName();
        columns = tableName2Cols == null ? null :
            tableName2Cols.get(Table.getCompleteName(dbname, objName));
        break;
      case DFS_DIR:
      case LOCAL_DIR:
        objName = privObject.getD().toString();
        break;
      case FUNCTION:
        if(privObject.getDatabase() != null) {
          dbname = privObject.getDatabase().getName();
        }
        objName = privObject.getFunctionName();
        className = privObject.getClassName();
        break;
      case DUMMYPARTITION:
      case PARTITION:
        // not currently handled
        continue;
      case SERVICE_NAME:
        objName = privObject.getServiceName();
        break;
        default:
          throw new AssertionError("Unexpected object type");
      }
      HivePrivObjectActionType actionType = AuthorizationUtils.getActionType(privObject);
      HivePrivilegeObject hPrivObject = new HivePrivilegeObject(privObjType, dbname, objName,
          partKeys, columns, actionType, null, className);
      hivePrivobjs.add(hPrivObject);
    }
    return hivePrivobjs;
  }

  private static HiveOperationType getHiveOperationType(HiveOperation op) {
    return HiveOperationType.valueOf(op.name());
  }

  @Override
  public HiveConf getConf() {
    return conf;
  }

  /**
   * @return The current query plan associated with this Driver, if any.
   */
  @Override
  public QueryPlan getPlan() {
    return plan;
  }

  /**
   * @return The current FetchTask associated with the Driver's plan, if any.
   */
  @Override
  public FetchTask getFetchTask() {
    return fetchTask;
  }

  // Write the current set of valid transactions into the conf file
  private void recordValidTxns(HiveTxnManager txnMgr) throws LockException {
    String oldTxnString = conf.get(ValidTxnList.VALID_TXNS_KEY);
    if ((oldTxnString != null) && (oldTxnString.length() > 0)) {
      throw new IllegalStateException("calling recordValidTxn() more than once in the same " +
              JavaUtils.txnIdToString(txnMgr.getCurrentTxnId()));
    }
    ValidTxnList txnList = txnMgr.getValidTxns();
    String txnStr = txnList.toString();
    conf.set(ValidTxnList.VALID_TXNS_KEY, txnStr);
    LOG.debug("Encoding valid txns info " + txnStr + " txnid:" + txnMgr.getCurrentTxnId());
  }

  // Write the current set of valid write ids for the operated acid tables into the conf file so
  // that it can be read by the input format.
  private void recordValidWriteIds(HiveTxnManager txnMgr) throws LockException {
    String txnString = conf.get(ValidTxnList.VALID_TXNS_KEY);
    if ((txnString == null) || (txnString.isEmpty())) {
      throw new IllegalStateException("calling recordValidWritsIdss() without initializing ValidTxnList " +
              JavaUtils.txnIdToString(txnMgr.getCurrentTxnId()));
    }
    ValidTxnWriteIdList txnWriteIds = txnMgr.getValidWriteIds(getTransactionalTableList(plan), txnString);
    String writeIdStr = txnWriteIds.toString();
    conf.set(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY, writeIdStr);
    if (plan.getFetchTask() != null) {
      /**
       * This is needed for {@link HiveConf.ConfVars.HIVEFETCHTASKCONVERSION} optimization which
       * initializes JobConf in FetchOperator before recordValidTxns() but this has to be done
       * after locks are acquired to avoid race conditions in ACID.
       * This case is supported only for single source query.
       */
      Operator<?> source = plan.getFetchTask().getWork().getSource();
      if (source instanceof TableScanOperator) {
        TableScanOperator tsOp = (TableScanOperator)source;
        String fullTableName = AcidUtils.getFullTableName(tsOp.getConf().getDatabaseName(),
                                                          tsOp.getConf().getTableName());
        ValidWriteIdList writeIdList = txnWriteIds.getTableValidWriteIdList(fullTableName);
        if (tsOp.getConf().isTranscationalTable() && (writeIdList == null)) {
          throw new IllegalStateException("ACID table: " + fullTableName
                  + " is missing from the ValidWriteIdList config: " + writeIdStr);
        }
        if (writeIdList != null) {
          plan.getFetchTask().setValidWriteIdList(writeIdList.toString());
        }
      }
    }
    LOG.debug("Encoding valid txn write ids info " + writeIdStr + " txnid:" + txnMgr.getCurrentTxnId());
  }

  // Make the list of transactional tables list which are getting read or written by current txn
  private List<String> getTransactionalTableList(QueryPlan plan) {
    Set<String> tableList = new HashSet<>();

    for (ReadEntity input : plan.getInputs()) {
      addTableFromEntity(input, tableList);
    }
    for (WriteEntity output : plan.getOutputs()) {
      addTableFromEntity(output, tableList);
    }
    return new ArrayList<String>(tableList);
  }

  private void addTableFromEntity(Entity entity, Collection<String> tableList) {
    Table tbl;
    switch (entity.getType()) {
      case TABLE: {
        tbl = entity.getTable();
        break;
      }
      case PARTITION:
      case DUMMYPARTITION: {
        tbl = entity.getPartition().getTable();
        break;
      }
      default: {
        return;
      }
    }
    if (!AcidUtils.isTransactionalTable(tbl)) {
      return;
    }
    String fullTableName = AcidUtils.getFullTableName(tbl.getDbName(), tbl.getTableName());
    tableList.add(fullTableName);
  }

  private String getUserFromUGI() {
    // Don't use the userName member, as it may or may not have been set.  Get the value from
    // conf, which calls into getUGI to figure out who the process is running as.
    try {
      return conf.getUser();
    } catch (IOException e) {
      errorMessage = "FAILED: Error in determining user while acquiring locks: " + e.getMessage();
      SQLState = ErrorMsg.findSQLState(e.getMessage());
      downstreamError = e;
      console.printError(errorMessage,
        "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
    return null;
  }

  /**
   * Acquire read and write locks needed by the statement. The list of objects to be locked are
   * obtained from the inputs and outputs populated by the compiler.  Locking strategy depends on
   * HiveTxnManager and HiveLockManager configured
   *
   * This method also records the list of valid transactions.  This must be done after any
   * transactions have been opened.
   * @throws CommandProcessorResponse
   **/
  private void acquireLocks() throws CommandProcessorResponse {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.ACQUIRE_READ_WRITE_LOCKS);

    if(!queryTxnMgr.isTxnOpen() && queryTxnMgr.supportsAcid()) {
      /*non acid txn managers don't support txns but fwd lock requests to lock managers
        acid txn manager requires all locks to be associated with a txn so if we
        end up here w/o an open txn it's because we are processing something like "use <database>
        which by definition needs no locks*/
      return;
    }
    try {
      String userFromUGI = getUserFromUGI();
      if(userFromUGI == null) {
        throw createProcessorResponse(10);
      }
      // Set the table write id in all of the acid file sinks
      if (haveAcidWrite()) {
        List<FileSinkDesc> acidSinks = new ArrayList<>(plan.getAcidSinks());
        //sorting makes tests easier to write since file names and ROW__IDs depend on statementId
        //so this makes (file name -> data) mapping stable
        acidSinks.sort((FileSinkDesc fsd1, FileSinkDesc fsd2) ->
          fsd1.getDirName().compareTo(fsd2.getDirName()));
        for (FileSinkDesc desc : acidSinks) {
          TableDesc tableInfo = desc.getTableInfo();
          long writeId = queryTxnMgr.getTableWriteId(Utilities.getDatabaseName(tableInfo.getTableName()),
                  Utilities.getTableName(tableInfo.getTableName()));
          desc.setTableWriteId(writeId);

          //it's possible to have > 1 FileSink writing to the same table/partition
          //e.g. Merge stmt, multi-insert stmt when mixing DP and SP writes
          desc.setStatementId(queryTxnMgr.getStmtIdAndIncrement());
        }
      }
      /*It's imperative that {@code acquireLocks()} is called for all commands so that
      HiveTxnManager can transition its state machine correctly*/
      queryTxnMgr.acquireLocks(plan, ctx, userFromUGI, lDrvState);
      // This check is for controlling the correctness of the current state
      if (queryTxnMgr.recordSnapshot(plan) && !validTxnListsGenerated) {
        throw new IllegalStateException("calling recordValidTxn() more than once in the same " +
            JavaUtils.txnIdToString(queryTxnMgr.getCurrentTxnId()));
      }
      if (plan.hasAcidResourcesInQuery()) {
        recordValidWriteIds(queryTxnMgr);
      }
    } catch (Exception e) {
      errorMessage = "FAILED: Error in acquiring locks: " + e.getMessage();
      SQLState = ErrorMsg.findSQLState(e.getMessage());
      downstreamError = e;
      console.printError(errorMessage, "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw createProcessorResponse(10);
    } finally {
      perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.ACQUIRE_READ_WRITE_LOCKS);
    }
  }

  private boolean haveAcidWrite() {
    return !plan.getAcidSinks().isEmpty();
  }

  public void releaseLocksAndCommitOrRollback(boolean commit) throws LockException {
    releaseLocksAndCommitOrRollback(commit, queryTxnMgr);
  }

  /**
   * @param commit if there is an open transaction and if true, commit,
   *               if false rollback.  If there is no open transaction this parameter is ignored.
   * @param txnManager an optional existing transaction manager retrieved earlier from the session
   *
   **/
  @VisibleForTesting
  public void releaseLocksAndCommitOrRollback(boolean commit, HiveTxnManager txnManager)
      throws LockException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.RELEASE_LOCKS);
    HiveTxnManager txnMgr;
    if (txnManager == null) {
      // Default to driver's txn manager if no txn manager specified
      txnMgr = queryTxnMgr;
    } else {
      txnMgr = txnManager;
    }
    // If we've opened a transaction we need to commit or rollback rather than explicitly
    // releasing the locks.
    conf.unset(ValidTxnList.VALID_TXNS_KEY);
    conf.unset(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY);
    if(!checkConcurrency()) {
      return;
    }
    if (txnMgr.isTxnOpen()) {
      if (commit) {
        if(conf.getBoolVar(ConfVars.HIVE_IN_TEST) && conf.getBoolVar(ConfVars.HIVETESTMODEROLLBACKTXN)) {
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
      if (ctx != null && ctx.getHiveLocks() != null) {
        hiveLocks.addAll(ctx.getHiveLocks());
      }
      txnMgr.releaseLocks(hiveLocks);
    }
    hiveLocks.clear();
    if (ctx != null) {
      ctx.setHiveLocks(null);
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.RELEASE_LOCKS);
  }

  /**
   * Release some resources after a query is executed
   * while keeping the result around.
   */
  public void releaseResources() {
    releasePlan();
    releaseDriverContext();
  }

  @Override

  public CommandProcessorResponse run(String command) {
    return run(command, false);
  }

  @Override
  public CommandProcessorResponse run() {
    return run(null, true);
  }

  public CommandProcessorResponse run(String command, boolean alreadyCompiled) {

    try {
      runInternal(command, alreadyCompiled);
      return createProcessorResponse(0);
    } catch (CommandProcessorResponse cpr) {

    SessionState ss = SessionState.get();
    if(ss == null) {
      return cpr;
    }
    MetaDataFormatter mdf = MetaDataFormatUtils.getFormatter(ss.getConf());
    if(!(mdf instanceof JsonMetaDataFormatter)) {
      return cpr;
    }
    /*Here we want to encode the error in machine readable way (e.g. JSON)
     * Ideally, errorCode would always be set to a canonical error defined in ErrorMsg.
     * In practice that is rarely the case, so the messy logic below tries to tease
     * out canonical error code if it can.  Exclude stack trace from output when
     * the error is a specific/expected one.
     * It's written to stdout for backward compatibility (WebHCat consumes it).*/
    try {
      if(downstreamError == null) {
        mdf.error(ss.out, errorMessage, cpr.getResponseCode(), SQLState);
        return cpr;
      }
      ErrorMsg canonicalErr = ErrorMsg.getErrorMsg(cpr.getResponseCode());
      if(canonicalErr != null && canonicalErr != ErrorMsg.GENERIC_ERROR) {
        /*Some HiveExceptions (e.g. SemanticException) don't set
          canonical ErrorMsg explicitly, but there is logic
          (e.g. #compile()) to find an appropriate canonical error and
          return its code as error code. In this case we want to
          preserve it for downstream code to interpret*/
        mdf.error(ss.out, errorMessage, cpr.getResponseCode(), SQLState, null);
        return cpr;
      }
      if(downstreamError instanceof HiveException) {
        HiveException rc = (HiveException) downstreamError;
        mdf.error(ss.out, errorMessage,
                rc.getCanonicalErrorMsg().getErrorCode(), SQLState,
                rc.getCanonicalErrorMsg() == ErrorMsg.GENERIC_ERROR ?
                        org.apache.hadoop.util.StringUtils.stringifyException(rc)
                        : null);
      }
      else {
        ErrorMsg canonicalMsg =
                ErrorMsg.getErrorMsg(downstreamError.getMessage());
        mdf.error(ss.out, errorMessage, canonicalMsg.getErrorCode(),
                SQLState, org.apache.hadoop.util.StringUtils.
                stringifyException(downstreamError));
      }
    }
    catch(HiveException ex) {
      console.printError("Unable to JSON-encode the error",
              org.apache.hadoop.util.StringUtils.stringifyException(ex));
    }
    return cpr;
    }
  }

  @Override
  public CommandProcessorResponse compileAndRespond(String command) {
    return compileAndRespond(command, false);
  }

  public CommandProcessorResponse compileAndRespond(String command, boolean cleanupTxnList) {
    try {
      compileInternal(command, false);
      return createProcessorResponse(0);
    } catch (CommandProcessorResponse e) {
      return e;
    } finally {
      if (cleanupTxnList) {
        // Valid txn list might be generated for a query compiled using this
        // command, thus we need to reset it
        conf.unset(ValidTxnList.VALID_TXNS_KEY);
      }
    }
  }

  public void lockAndRespond() throws CommandProcessorResponse {
    // Assumes the query has already been compiled
    if (plan == null) {
      throw new IllegalStateException(
          "No previously compiled query for driver - queryId=" + queryState.getQueryId());
    }

    if (requiresLock()) {
      try {
        acquireLocks();
      } catch (CommandProcessorResponse cpr) {
        rollback(cpr);
        throw cpr;
      }
    }
  }

  private static final ReentrantLock globalCompileLock = new ReentrantLock();

  private void compileInternal(String command, boolean deferClose) throws CommandProcessorResponse {
    Metrics metrics = MetricsFactory.getInstance();
    if (metrics != null) {
      metrics.incrementCounter(MetricsConstant.WAITING_COMPILE_OPS, 1);
    }

    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.WAIT_COMPILE);
    final ReentrantLock compileLock = tryAcquireCompileLock(isParallelEnabled,
      command);
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.WAIT_COMPILE);
    if (metrics != null) {
      metrics.decrementCounter(MetricsConstant.WAITING_COMPILE_OPS, 1);
    }

    if (compileLock == null) {
      throw createProcessorResponse(ErrorMsg.COMPILE_LOCK_TIMED_OUT.getErrorCode());
    }


    try {
      compile(command, true, deferClose);
    } catch (CommandProcessorResponse cpr) {
      try {
        releaseLocksAndCommitOrRollback(false);
      } catch (LockException e) {
        LOG.warn("Exception in releasing locks. " + org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
      throw cpr;
    } finally {
      compileLock.unlock();
    }

    //Save compile-time PerfLogging for WebUI.
    //Execution-time Perf logs are done by either another thread's PerfLogger
    //or a reset PerfLogger.
    queryDisplay.setPerfLogStarts(QueryDisplay.Phase.COMPILATION, perfLogger.getStartTimes());
    queryDisplay.setPerfLogEnds(QueryDisplay.Phase.COMPILATION, perfLogger.getEndTimes());
  }

  /**
   * Acquires the compile lock. If the compile lock wait timeout is configured,
   * it will acquire the lock if it is not held by another thread within the given
   * waiting time.
   * @return the ReentrantLock object if the lock was successfully acquired,
   *         or {@code null} if compile lock wait timeout is configured and
   *         either the waiting time elapsed before the lock could be acquired
   *         or if the current thread is interrupted.
   */
  private ReentrantLock tryAcquireCompileLock(boolean isParallelEnabled,
    String command) {
    final ReentrantLock compileLock = isParallelEnabled ?
        SessionState.get().getCompileLock() : globalCompileLock;
    long maxCompileLockWaitTime = HiveConf.getTimeVar(
      this.conf, ConfVars.HIVE_SERVER2_COMPILE_LOCK_TIMEOUT,
      TimeUnit.SECONDS);

    final String lockAcquiredMsg = "Acquired the compile lock.";
    // First shot without waiting.
    try {
      if (compileLock.tryLock(0, TimeUnit.SECONDS)) {
        LOG.debug(lockAcquiredMsg);
        return compileLock;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Interrupted Exception ignored", e);
      }
      return null;
    }

    // If the first shot fails, then we log the waiting messages.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Waiting to acquire compile lock: " + command);
    }

    if (maxCompileLockWaitTime > 0) {
      try {
        if(!compileLock.tryLock(maxCompileLockWaitTime, TimeUnit.SECONDS)) {
          errorMessage = ErrorMsg.COMPILE_LOCK_TIMED_OUT.getErrorCodedMsg();
          LOG.error(errorMessage + ": " + command);
          return null;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Interrupted Exception ignored", e);
        }
        return null;
      }
    } else {
      compileLock.lock();
    }

    LOG.debug(lockAcquiredMsg);
    return compileLock;
  }

  private void runInternal(String command, boolean alreadyCompiled) throws CommandProcessorResponse {
    errorMessage = null;
    SQLState = null;
    downstreamError = null;
    LockedDriverState.setLockedDriverState(lDrvState);

    lDrvState.stateLock.lock();
    try {
      if (alreadyCompiled) {
        if (lDrvState.driverState == DriverState.COMPILED) {
          lDrvState.driverState = DriverState.EXECUTING;
        } else {
          errorMessage = "FAILED: Precompiled query has been cancelled or closed.";
          console.printError(errorMessage);
          throw createProcessorResponse(12);
        }
      } else {
        lDrvState.driverState = DriverState.COMPILING;
      }
    } finally {
      lDrvState.stateLock.unlock();
    }

    // a flag that helps to set the correct driver state in finally block by tracking if
    // the method has been returned by an error or not.
    boolean isFinishedWithError = true;
    try {
      HiveDriverRunHookContext hookContext = new HiveDriverRunHookContextImpl(conf,
          alreadyCompiled ? ctx.getCmd() : command);
      // Get all the driver run hooks and pre-execute them.
      try {
        hookRunner.runPreDriverHooks(hookContext);
      } catch (Exception e) {
        errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
        SQLState = ErrorMsg.findSQLState(e.getMessage());
        downstreamError = e;
        console.printError(errorMessage + "\n"
            + org.apache.hadoop.util.StringUtils.stringifyException(e));
        throw createProcessorResponse(12);
      }

      PerfLogger perfLogger = null;

      if (!alreadyCompiled) {
        // compile internal will automatically reset the perf logger
        compileInternal(command, true);
        // then we continue to use this perf logger
        perfLogger = SessionState.getPerfLogger();
      } else {
        // reuse existing perf logger.
        perfLogger = SessionState.getPerfLogger();
        // Since we're reusing the compiled plan, we need to update its start time for current run
        plan.setQueryStartTime(perfLogger.getStartTime(PerfLogger.DRIVER_RUN));
      }
      // the reason that we set the txn manager for the cxt here is because each
      // query has its own ctx object. The txn mgr is shared across the
      // same instance of Driver, which can run multiple queries.
      ctx.setHiveTxnManager(queryTxnMgr);

      checkInterrupted("at acquiring the lock.", null, null);

      lockAndRespond();

      try {
        execute();
      } catch (CommandProcessorResponse cpr) {
        rollback(cpr);
        throw cpr;
      }

      //if needRequireLock is false, the release here will do nothing because there is no lock
      try {
        //since set autocommit starts an implicit txn, close it
        if(queryTxnMgr.isImplicitTransactionOpen() || plan.getOperation() == HiveOperation.COMMIT) {
          releaseLocksAndCommitOrRollback(true);
        }
        else if(plan.getOperation() == HiveOperation.ROLLBACK) {
          releaseLocksAndCommitOrRollback(false);
        }
        else {
          //txn (if there is one started) is not finished
        }
      } catch (LockException e) {
        throw handleHiveException(e, 12);
      }

      perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.DRIVER_RUN);
      queryDisplay.setPerfLogStarts(QueryDisplay.Phase.EXECUTION, perfLogger.getStartTimes());
      queryDisplay.setPerfLogEnds(QueryDisplay.Phase.EXECUTION, perfLogger.getEndTimes());

      // Take all the driver run hooks and post-execute them.
      try {
        hookRunner.runPostDriverHooks(hookContext);
      } catch (Exception e) {
        errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
        SQLState = ErrorMsg.findSQLState(e.getMessage());
        downstreamError = e;
        console.printError(errorMessage + "\n"
            + org.apache.hadoop.util.StringUtils.stringifyException(e));
        throw createProcessorResponse(12);
      }
      isFinishedWithError = false;
    } finally {
      if (lDrvState.isAborted()) {
        closeInProcess(true);
      } else {
        // only release the related resources ctx, driverContext as normal
        releaseResources();
      }

      lDrvState.stateLock.lock();
      try {
        lDrvState.driverState = isFinishedWithError ? DriverState.ERROR : DriverState.EXECUTED;
      } finally {
        lDrvState.stateLock.unlock();
      }
    }
  }

  private CommandProcessorResponse rollback(CommandProcessorResponse cpr) throws CommandProcessorResponse {

    //console.printError(cpr.toString());
    try {
      releaseLocksAndCommitOrRollback(false);
    }
    catch (LockException e) {
      LOG.error("rollback() FAILED: " + cpr);//make sure not to loose
      handleHiveException(e, 12, "Additional info in hive.log at \"rollback() FAILED\"");
    }
    return cpr;
  }

  private CommandProcessorResponse handleHiveException(HiveException e, int ret) throws CommandProcessorResponse {
    return handleHiveException(e, ret, null);
  }

  private CommandProcessorResponse handleHiveException(HiveException e, int ret, String rootMsg) throws CommandProcessorResponse {
    errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
    if(rootMsg != null) {
      errorMessage += "\n" + rootMsg;
    }
    SQLState = e.getCanonicalErrorMsg() != null ?
      e.getCanonicalErrorMsg().getSQLState() : ErrorMsg.findSQLState(e.getMessage());
    downstreamError = e;
    console.printError(errorMessage + "\n"
      + org.apache.hadoop.util.StringUtils.stringifyException(e));
    throw createProcessorResponse(ret);
  }
  private boolean requiresLock() {
    if (!checkConcurrency()) {
      return false;
    }
    // Lock operations themselves don't require the lock.
    if (isExplicitLockOperation()){
      return false;
    }
    if (!HiveConf.getBoolVar(conf, ConfVars.HIVE_LOCK_MAPRED_ONLY)) {
      return true;
    }
    Queue<Task<? extends Serializable>> taskQueue = new LinkedList<Task<? extends Serializable>>();
    taskQueue.addAll(plan.getRootTasks());
    while (taskQueue.peek() != null) {
      Task<? extends Serializable> tsk = taskQueue.remove();
      if (tsk.requireLock()) {
        return true;
      }
      if (tsk instanceof ConditionalTask) {
        taskQueue.addAll(((ConditionalTask)tsk).getListTasks());
      }
      if (tsk.getChildTasks()!= null) {
        taskQueue.addAll(tsk.getChildTasks());
      }
      // does not add back up task here, because back up task should be the same
      // type of the original task.
    }
    return false;
  }

  private boolean isExplicitLockOperation() {
    HiveOperation currentOpt = plan.getOperation();
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

  private CommandProcessorResponse createProcessorResponse(int ret) {
    SessionState.getPerfLogger().cleanupPerfLogMetrics();
    queryDisplay.setErrorMessage(errorMessage);
    if(downstreamError != null && downstreamError instanceof HiveException) {
      ErrorMsg em = ((HiveException)downstreamError).getCanonicalErrorMsg();
      if(em != null) {
        return new CommandProcessorResponse(ret, errorMessage, SQLState,
          schema, downstreamError, em.getErrorCode(), null);
      }
    }
    return new CommandProcessorResponse(ret, errorMessage, SQLState, downstreamError);
  }

  private void useFetchFromCache(CacheEntry cacheEntry) {
    // Change query FetchTask to use new location specified in results cache.
    FetchTask fetchTaskFromCache = (FetchTask) TaskFactory.get(cacheEntry.getFetchWork());
    fetchTaskFromCache.initialize(queryState, plan, null, ctx.getOpContext());
    plan.setFetchTask(fetchTaskFromCache);
    cacheUsage = new CacheUsage(CacheUsage.CacheStatus.QUERY_USING_CACHE, cacheEntry);
  }

  private void preExecutionCacheActions() throws Exception {
    if (cacheUsage != null) {
      if (cacheUsage.getStatus() == CacheUsage.CacheStatus.CAN_CACHE_QUERY_RESULTS &&
          plan.getFetchTask() != null) {
        // The results of this query execution might be cacheable.
        // Add a placeholder entry in the cache so other queries know this result is pending.
        CacheEntry pendingCacheEntry =
            QueryResultsCache.getInstance().addToCache(cacheUsage.getQueryInfo());
        if (pendingCacheEntry != null) {
          // Update cacheUsage to reference the pending entry.
          this.cacheUsage.setCacheEntry(pendingCacheEntry);
        }
      }
    }
  }

  private void postExecutionCacheActions() throws Exception {
    if (cacheUsage != null) {
      if (cacheUsage.getStatus() == CacheUsage.CacheStatus.QUERY_USING_CACHE) {
        // Using a previously cached result.
        CacheEntry cacheEntry = cacheUsage.getCacheEntry();

        // Reader count already incremented during cache lookup.
        // Save to usedCacheEntry to ensure reader is released after query.
        this.usedCacheEntry = cacheEntry;
      } else if (cacheUsage.getStatus() == CacheUsage.CacheStatus.CAN_CACHE_QUERY_RESULTS &&
          cacheUsage.getCacheEntry() != null &&
          plan.getFetchTask() != null) {
        // Save results to the cache for future queries to use.
        PerfLogger perfLogger = SessionState.getPerfLogger();
        perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SAVE_TO_RESULTS_CACHE);

        boolean savedToCache = QueryResultsCache.getInstance().setEntryValid(
            cacheUsage.getCacheEntry(),
            plan.getFetchTask().getWork());
        LOG.info("savedToCache: {}", savedToCache);
        if (savedToCache) {
          useFetchFromCache(cacheUsage.getCacheEntry());
          // setEntryValid() already increments the reader count. Set usedCacheEntry so it gets released.
          this.usedCacheEntry = cacheUsage.getCacheEntry();
        }

        perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SAVE_TO_RESULTS_CACHE);
      }
    }
  }

  private void execute() throws CommandProcessorResponse {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.DRIVER_EXECUTE);

    boolean noName = StringUtils.isEmpty(conf.get(MRJobConfig.JOB_NAME));
    int maxlen = conf.getIntVar(HiveConf.ConfVars.HIVEJOBNAMELENGTH);
    Metrics metrics = MetricsFactory.getInstance();

    String queryId = queryState.getQueryId();
    // Get the query string from the conf file as the compileInternal() method might
    // hide sensitive information during query redaction.
    String queryStr = conf.getQueryString();

    lDrvState.stateLock.lock();
    try {
      // if query is not in compiled state, or executing state which is carried over from
      // a combined compile/execute in runInternal, throws the error
      if (lDrvState.driverState != DriverState.COMPILED &&
          lDrvState.driverState != DriverState.EXECUTING) {
        SQLState = "HY008";
        errorMessage = "FAILED: unexpected driverstate: " + lDrvState + ", for query " + queryStr;
        console.printError(errorMessage);
        throw createProcessorResponse(1000);
      } else {
        lDrvState.driverState = DriverState.EXECUTING;
      }
    } finally {
      lDrvState.stateLock.unlock();
    }

    maxthreads = HiveConf.getIntVar(conf, HiveConf.ConfVars.EXECPARALLETHREADNUMBER);

    HookContext hookContext = null;

    // Whether there's any error occurred during query execution. Used for query lifetime hook.
    boolean executionError = false;

    try {
      LOG.info("Executing command(queryId=" + queryId + "): " + queryStr);
      // compile and execute can get called from different threads in case of HS2
      // so clear timing in this thread's Hive object before proceeding.
      Hive.get().clearMetaCallTiming();

      plan.setStarted();

      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().startQuery(queryStr, queryId);
        SessionState.get().getHiveHistory().logPlanProgress(plan);
      }
      resStream = null;

      SessionState ss = SessionState.get();

      hookContext = new PrivateHookContext(plan, queryState, ctx.getPathToCS(), SessionState.get().getUserName(),
          ss.getUserIpAddress(), InetAddress.getLocalHost().getHostAddress(), operationId,
          ss.getSessionId(), Thread.currentThread().getName(), ss.isHiveServerQuery(), perfLogger, queryInfo, ctx);
      hookContext.setHookType(HookContext.HookType.PRE_EXEC_HOOK);

      hookRunner.runPreHooks(hookContext);

      // Trigger query hooks before query execution.
      hookRunner.runBeforeExecutionHook(queryStr, hookContext);

      setQueryDisplays(plan.getRootTasks());
      int mrJobs = Utilities.getMRTasks(plan.getRootTasks()).size();
      int jobs = mrJobs + Utilities.getTezTasks(plan.getRootTasks()).size()
          + Utilities.getSparkTasks(plan.getRootTasks()).size();
      if (jobs > 0) {
        logMrWarning(mrJobs);
        console.printInfo("Query ID = " + queryId);
        console.printInfo("Total jobs = " + jobs);
      }
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setQueryProperty(queryId, Keys.QUERY_NUM_TASKS,
            String.valueOf(jobs));
        SessionState.get().getHiveHistory().setIdToTableMap(plan.getIdToTableNameMap());
      }
      String jobname = Utilities.abbreviate(queryStr, maxlen - 6);

      // A runtime that launches runnable tasks as separate Threads through
      // TaskRunners
      // As soon as a task isRunnable, it is put in a queue
      // At any time, at most maxthreads tasks can be running
      // The main thread polls the TaskRunners to check if they have finished.

      checkInterrupted("before running tasks.", hookContext, perfLogger);

      DriverContext driverCxt = new DriverContext(ctx);
      driverCxt.prepare(plan);

      ctx.setHDFSCleanup(true);
      this.driverCxt = driverCxt; // for canceling the query (should be bound to session?)

      SessionState.get().setMapRedStats(new LinkedHashMap<>());
      SessionState.get().setStackTraces(new HashMap<>());
      SessionState.get().setLocalMapRedErrors(new HashMap<>());

      // Add root Tasks to runnable
      for (Task<? extends Serializable> tsk : plan.getRootTasks()) {
        // This should never happen, if it does, it's a bug with the potential to produce
        // incorrect results.
        assert tsk.getParentTasks() == null || tsk.getParentTasks().isEmpty();
        driverCxt.addToRunnable(tsk);

        if (metrics != null) {
          tsk.updateTaskMetrics(metrics);
        }
      }

      preExecutionCacheActions();

      perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.RUN_TASKS);
      // Loop while you either have tasks running, or tasks queued up
      while (driverCxt.isRunning()) {

        // Launch upto maxthreads tasks
        Task<? extends Serializable> task;
        while ((task = driverCxt.getRunnable(maxthreads)) != null) {
          TaskRunner runner = launchTask(task, queryId, noName, jobname, jobs, driverCxt);
          if (!runner.isRunning()) {
            break;
          }
        }

        // poll the Tasks to see which one completed
        TaskRunner tskRun = driverCxt.pollFinished();
        if (tskRun == null) {
          continue;
        }
        /*
          This should be removed eventually. HIVE-17814 gives more detail
          explanation of whats happening and HIVE-17815 as to why this is done.
          Briefly for replication the graph is huge and so memory pressure is going to be huge if
          we keep a lot of references around.
        */
        String opName = plan.getOperationName();
        boolean isReplicationOperation = opName.equals(HiveOperation.REPLDUMP.getOperationName())
            || opName.equals(HiveOperation.REPLLOAD.getOperationName());
        if (!isReplicationOperation) {
          hookContext.addCompleteTask(tskRun);
        }

        queryDisplay.setTaskResult(tskRun.getTask().getId(), tskRun.getTaskResult());

        Task<? extends Serializable> tsk = tskRun.getTask();
        TaskResult result = tskRun.getTaskResult();

        int exitVal = result.getExitVal();
        checkInterrupted("when checking the execution result.", hookContext, perfLogger);

        if (exitVal != 0) {
          Task<? extends Serializable> backupTask = tsk.getAndInitBackupTask();
          if (backupTask != null) {
            setErrorMsgAndDetail(exitVal, result.getTaskError(), tsk);
            console.printError(errorMessage);
            errorMessage = "ATTEMPT: Execute BackupTask: " + backupTask.getClass().getName();
            console.printError(errorMessage);

            // add backup task to runnable
            if (DriverContext.isLaunchable(backupTask)) {
              driverCxt.addToRunnable(backupTask);
            }
            continue;

          } else {
            setErrorMsgAndDetail(exitVal, result.getTaskError(), tsk);
            if (driverCxt.isShutdown()) {
              errorMessage = "FAILED: Operation cancelled. " + errorMessage;
            }
            invokeFailureHooks(perfLogger, hookContext,
              errorMessage + Strings.nullToEmpty(tsk.getDiagnosticsMessage()), result.getTaskError());
            SQLState = "08S01";

            // 08S01 (Communication error) is the default sql state.  Override the sqlstate
            // based on the ErrorMsg set in HiveException.
            if (result.getTaskError() instanceof HiveException) {
              ErrorMsg errorMsg = ((HiveException) result.getTaskError()).
                  getCanonicalErrorMsg();
              if (errorMsg != ErrorMsg.GENERIC_ERROR) {
                SQLState = errorMsg.getSQLState();
              }
            }

            console.printError(errorMessage);
            driverCxt.shutdown();
            // in case we decided to run everything in local mode, restore the
            // the jobtracker setting to its initial value
            ctx.restoreOriginalTracker();
            throw createProcessorResponse(exitVal);
          }
        }

        driverCxt.finished(tskRun);

        if (SessionState.get() != null) {
          SessionState.get().getHiveHistory().setTaskProperty(queryId, tsk.getId(),
              Keys.TASK_RET_CODE, String.valueOf(exitVal));
          SessionState.get().getHiveHistory().endTask(queryId, tsk);
        }

        if (tsk.getChildTasks() != null) {
          for (Task<? extends Serializable> child : tsk.getChildTasks()) {
            if (DriverContext.isLaunchable(child)) {
              driverCxt.addToRunnable(child);
            }
          }
        }
      }
      perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.RUN_TASKS);

      postExecutionCacheActions();

      // in case we decided to run everything in local mode, restore the
      // the jobtracker setting to its initial value
      ctx.restoreOriginalTracker();

      if (driverCxt.isShutdown()) {
        SQLState = "HY008";
        errorMessage = "FAILED: Operation cancelled";
        invokeFailureHooks(perfLogger, hookContext, errorMessage, null);
        console.printError(errorMessage);
        throw createProcessorResponse(1000);
      }

      // remove incomplete outputs.
      // Some incomplete outputs may be added at the beginning, for eg: for dynamic partitions.
      // remove them
      HashSet<WriteEntity> remOutputs = new LinkedHashSet<WriteEntity>();
      for (WriteEntity output : plan.getOutputs()) {
        if (!output.isComplete()) {
          remOutputs.add(output);
        }
      }

      for (WriteEntity output : remOutputs) {
        plan.getOutputs().remove(output);
      }


      hookContext.setHookType(HookContext.HookType.POST_EXEC_HOOK);

      hookRunner.runPostExecHooks(hookContext);

      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setQueryProperty(queryId, Keys.QUERY_RET_CODE,
            String.valueOf(0));
        SessionState.get().getHiveHistory().printRowCount(queryId);
      }
      releasePlan(plan);
    } catch (CommandProcessorResponse cpr) {
      executionError = true;
      throw cpr;
    } catch (Throwable e) {
      executionError = true;

      checkInterrupted("during query execution: \n" + e.getMessage(), hookContext, perfLogger);

      ctx.restoreOriginalTracker();
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setQueryProperty(queryId, Keys.QUERY_RET_CODE,
            String.valueOf(12));
      }
      // TODO: do better with handling types of Exception here
      errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
      if (hookContext != null) {
        try {
          invokeFailureHooks(perfLogger, hookContext, errorMessage, e);
        } catch (Exception t) {
          LOG.warn("Failed to invoke failure hook", t);
        }
      }
      SQLState = "08S01";
      downstreamError = e;
      console.printError(errorMessage + "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw createProcessorResponse(12);
    } finally {
      // Trigger query hooks after query completes its execution.
      try {
        hookRunner.runAfterExecutionHook(queryStr, hookContext, executionError);
      } catch (Exception e) {
        LOG.warn("Failed when invoking query after execution hook", e);
      }

      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().endQuery(queryId);
      }
      if (noName) {
        conf.set(MRJobConfig.JOB_NAME, "");
      }
      double duration = perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.DRIVER_EXECUTE)/1000.00;

      ImmutableMap<String, Long> executionHMSTimings = dumpMetaCallTimingWithoutEx("execution");
      queryDisplay.setHmsTimings(QueryDisplay.Phase.EXECUTION, executionHMSTimings);

      Map<String, MapRedStats> stats = SessionState.get().getMapRedStats();
      if (stats != null && !stats.isEmpty()) {
        long totalCpu = 0;
        console.printInfo("MapReduce Jobs Launched: ");
        for (Map.Entry<String, MapRedStats> entry : stats.entrySet()) {
          console.printInfo("Stage-" + entry.getKey() + ": " + entry.getValue());
          totalCpu += entry.getValue().getCpuMSec();
        }
        console.printInfo("Total MapReduce CPU Time Spent: " + Utilities.formatMsecToStr(totalCpu));
      }
      lDrvState.stateLock.lock();
      try {
        lDrvState.driverState = executionError ? DriverState.ERROR : DriverState.EXECUTED;
      } finally {
        lDrvState.stateLock.unlock();
      }
      if (lDrvState.isAborted()) {
        LOG.info("Executing command(queryId=" + queryId + ") has been interrupted after " + duration + " seconds");
      } else {
        LOG.info("Completed executing command(queryId=" + queryId + "); Time taken: " + duration + " seconds");
      }
    }

    if (console != null) {
      console.printInfo("OK");
    }
  }

  private void releasePlan(QueryPlan plan) {
    // Plan maybe null if Driver.close is called in another thread for the same Driver object
    lDrvState.stateLock.lock();
    try {
      if (plan != null) {
        plan.setDone();
        if (SessionState.get() != null) {
          try {
            SessionState.get().getHiveHistory().logPlanProgress(plan);
          } catch (Exception e) {
            // Log and ignore
            LOG.warn("Could not log query plan progress", e);
          }
        }
      }
    } finally {
      lDrvState.stateLock.unlock();
    }
  }

  private void setQueryDisplays(List<Task<? extends Serializable>> tasks) {
    if (tasks != null) {
      Set<Task<? extends Serializable>> visited = new HashSet<Task<? extends Serializable>>();
      while (!tasks.isEmpty()) {
        tasks = setQueryDisplays(tasks, visited);
      }
    }
  }

  private List<Task<? extends Serializable>> setQueryDisplays(
          List<Task<? extends Serializable>> tasks,
          Set<Task<? extends Serializable>> visited) {
    List<Task<? extends Serializable>> childTasks = new ArrayList<>();
    for (Task<? extends Serializable> task : tasks) {
      if (visited.contains(task)) {
        continue;
      }
      task.setQueryDisplay(queryDisplay);
      if (task.getDependentTasks() != null) {
        childTasks.addAll(task.getDependentTasks());
      }
      visited.add(task);
    }
    return childTasks;
  }

  private void logMrWarning(int mrJobs) {
    if (mrJobs <= 0 || !("mr".equals(HiveConf.getVar(conf, ConfVars.HIVE_EXECUTION_ENGINE)))) {
      return;
    }
    String warning = HiveConf.generateMrDeprecationWarning();
    LOG.warn(warning);
  }

  private void setErrorMsgAndDetail(int exitVal, Throwable downstreamError, Task tsk) {
    this.downstreamError = downstreamError;
    errorMessage = "FAILED: Execution Error, return code " + exitVal + " from " + tsk.getClass().getName();
    if(downstreamError != null) {
      //here we assume that upstream code may have parametrized the msg from ErrorMsg
      //so we want to keep it
      errorMessage += ". " + downstreamError.getMessage();
    }
    else {
      ErrorMsg em = ErrorMsg.getErrorMsg(exitVal);
      if (em != null) {
        errorMessage += ". " +  em.getMsg();
      }
    }
  }

  private void invokeFailureHooks(PerfLogger perfLogger,
      HookContext hookContext, String errorMessage, Throwable exception) throws Exception {
    hookContext.setHookType(HookContext.HookType.ON_FAILURE_HOOK);
    hookContext.setErrorMessage(errorMessage);
    hookContext.setException(exception);
    // Get all the failure execution hooks and execute them.
    hookRunner.runFailureHooks(hookContext);
  }

  /**
   * Launches a new task
   *
   * @param tsk
   *          task being launched
   * @param queryId
   *          Id of the query containing the task
   * @param noName
   *          whether the task has a name set
   * @param jobname
   *          name of the task, if it is a map-reduce job
   * @param jobs
   *          number of map-reduce jobs
   * @param cxt
   *          the driver context
   */
  private TaskRunner launchTask(Task<? extends Serializable> tsk, String queryId, boolean noName,
      String jobname, int jobs, DriverContext cxt) throws HiveException {
    if (SessionState.get() != null) {
      SessionState.get().getHiveHistory().startTask(queryId, tsk, tsk.getClass().getName());
    }
    if (tsk.isMapRedTask() && !(tsk instanceof ConditionalTask)) {
      if (noName) {
        conf.set(MRJobConfig.JOB_NAME, jobname + " (" + tsk.getId() + ")");
      }
      conf.set(DagUtils.MAPREDUCE_WORKFLOW_NODE_NAME, tsk.getId());
      Utilities.setWorkflowAdjacencies(conf, plan);
      cxt.incCurJobNo(1);
      console.printInfo("Launching Job " + cxt.getCurJobNo() + " out of " + jobs);
    }
    tsk.initialize(queryState, plan, cxt, ctx.getOpContext());
    TaskRunner tskRun = new TaskRunner(tsk);

    cxt.launching(tskRun);
    // Launch Task
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.EXECPARALLEL) && tsk.canExecuteInParallel()) {
      // Launch it in the parallel mode, as a separate thread only for MR tasks
      if (LOG.isInfoEnabled()){
        LOG.info("Starting task [" + tsk + "] in parallel");
      }
      tskRun.start();
    } else {
      if (LOG.isInfoEnabled()){
        LOG.info("Starting task [" + tsk + "] in serial mode");
      }
      tskRun.runSequential();
    }
    return tskRun;
  }

  @Override
  public boolean isFetchingTable() {
    return fetchTask != null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean getResults(List res) throws IOException {
    if (lDrvState.driverState == DriverState.DESTROYED || lDrvState.driverState == DriverState.CLOSED) {
      throw new IOException("FAILED: query has been cancelled, closed, or destroyed.");
    }

    if (isFetchingTable()) {
      /**
       * If resultset serialization to thrift object is enabled, and if the destination table is
       * indeed written using ThriftJDBCBinarySerDe, read one row from the output sequence file,
       * since it is a blob of row batches.
       */
      if (fetchTask.getWork().isUsingThriftJDBCBinarySerDe()) {
        maxRows = 1;
      }
      fetchTask.setMaxRows(maxRows);
      return fetchTask.fetch(res);
    }

    if (resStream == null) {
      resStream = ctx.getStream();
    }
    if (resStream == null) {
      return false;
    }

    int numRows = 0;
    String row = null;

    while (numRows < maxRows) {
      if (resStream == null) {
        if (numRows > 0) {
          return true;
        } else {
          return false;
        }
      }

      bos.reset();
      Utilities.StreamStatus ss;
      try {
        ss = Utilities.readColumn(resStream, bos);
        if (bos.getLength() > 0) {
          row = new String(bos.getData(), 0, bos.getLength(), "UTF-8");
        } else if (ss == Utilities.StreamStatus.TERMINATED) {
          row = new String();
        }

        if (row != null) {
          numRows++;
          res.add(row);
        }
        row = null;
      } catch (IOException e) {
        console.printError("FAILED: Unexpected IO exception : " + e.getMessage());
        return false;
      }

      if (ss == Utilities.StreamStatus.EOF) {
        resStream = ctx.getStream();
      }
    }
    return true;
  }

  @Override
  public void resetFetch() throws IOException {
    if (lDrvState.driverState == DriverState.DESTROYED || lDrvState.driverState == DriverState.CLOSED) {
      throw new IOException("FAILED: driver has been cancelled, closed or destroyed.");
    }
    if (isFetchingTable()) {
      try {
        fetchTask.clearFetch();
      } catch (Exception e) {
        throw new IOException("Error closing the current fetch task", e);
      }
      // FetchTask should not depend on the plan.
      fetchTask.initialize(queryState, null, null, ctx.getOpContext());
    } else {
      ctx.resetStream();
      resStream = null;
    }
  }

  // DriverContext could be released in the query and close processes at same
  // time, which needs to be thread protected.
  private void releaseDriverContext() {
    lDrvState.stateLock.lock();
    try {
      if (driverCxt != null) {
        driverCxt.shutdown();
        driverCxt = null;
      }
    } catch (Exception e) {
      LOG.debug("Exception while shutting down the task runner", e);
    } finally {
      lDrvState.stateLock.unlock();
    }
  }

  private void releasePlan() {
    try {
      if (plan != null) {
        fetchTask = plan.getFetchTask();
        if (fetchTask != null) {
          fetchTask.setDriverContext(null);
          fetchTask.setQueryPlan(null);
        }
      }
      plan = null;
    } catch (Exception e) {
      LOG.debug("Exception while clearing the Fetch task", e);
    }
  }

  private void releaseContext() {
    try {
      if (ctx != null) {
        ctx.clear();
        if (ctx.getHiveLocks() != null) {
          hiveLocks.addAll(ctx.getHiveLocks());
          ctx.setHiveLocks(null);
        }
        ctx = null;
      }
    } catch (Exception e) {
      LOG.debug("Exception while clearing the context ", e);
    }
  }

  private void releaseResStream() {
    try {
      if (resStream != null) {
        ((FSDataInputStream) resStream).close();
        resStream = null;
      }
    } catch (Exception e) {
      LOG.debug(" Exception while closing the resStream ", e);
    }
  }

  private void releaseFetchTask() {
    try {
      if (fetchTask != null) {
        fetchTask.clearFetch();
        fetchTask = null;
      }
    } catch (Exception e) {
      LOG.debug(" Exception while clearing the FetchTask ", e);
    }
  }

  private boolean hasBadCacheAttempt() {
    // Check if the query results were cacheable, and created a pending cache entry.
    // If we successfully saved the results, the usage would have changed to QUERY_USING_CACHE.
    return (cacheUsage != null &&
        cacheUsage.getStatus() == CacheUsage.CacheStatus.CAN_CACHE_QUERY_RESULTS &&
        cacheUsage.getCacheEntry() != null);
  }

  private void releaseCachedResult() {
    // Assumes the reader count has been incremented automatically by the results cache by either
    // lookup or creating the cache entry.
    if (usedCacheEntry != null) {
      usedCacheEntry.releaseReader();
      usedCacheEntry = null;
    } else if (hasBadCacheAttempt()) {
      // This query create a pending cache entry but it was never saved with real results, cleanup.
      // This step is required, as there may be queries waiting on this pending cache entry.
      // Removing/invalidating this entry will notify the waiters that this entry cannot be used.
      try {
        QueryResultsCache.getInstance().removeEntry(cacheUsage.getCacheEntry());
      } catch (Exception err) {
        LOG.error("Error removing failed cache entry " + cacheUsage.getCacheEntry(), err);
      }
    }
    cacheUsage = null;
  }

  // Close and release resources within a running query process. Since it runs under
  // driver state COMPILING, EXECUTING or INTERRUPT, it would not have race condition
  // with the releases probably running in the other closing thread.
  private int closeInProcess(boolean destroyed) {
    releaseDriverContext();
    releasePlan();
    releaseCachedResult();
    releaseFetchTask();
    releaseResStream();
    releaseContext();
    if(destroyed) {
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
    lDrvState.stateLock.lock();
    try {
      releaseDriverContext();
      if (lDrvState.driverState == DriverState.COMPILING ||
          lDrvState.driverState == DriverState.EXECUTING) {
        lDrvState.abort();
      }
      releasePlan();
      releaseCachedResult();
      releaseFetchTask();
      releaseResStream();
      releaseContext();
      lDrvState.driverState = DriverState.CLOSED;
    } finally {
      lDrvState.stateLock.unlock();
      LockedDriverState.removeLockedDriverState();
    }
    destroy();
  }

  // is usually called after close() to commit or rollback a query and end the driver life cycle.
  // do not understand why it is needed and wonder if it could be combined with close.
  @Override
  public void destroy() {
    lDrvState.stateLock.lock();
    try {
      // in the cancel case where the driver state is INTERRUPTED, destroy will be deferred to
      // the query process
      if (lDrvState.driverState == DriverState.DESTROYED) {
        return;
      } else {
        lDrvState.driverState = DriverState.DESTROYED;
      }
    } finally {
      lDrvState.stateLock.unlock();
    }
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


  public org.apache.hadoop.hive.ql.plan.api.Query getQueryPlan() throws IOException {
    return plan.getQueryPlan();
  }

  public String getErrorMsg() {
    return errorMessage;
  }


  @Override
  public QueryDisplay getQueryDisplay() {
    return queryDisplay;
  }

  /**
   * Set the HS2 operation handle's guid string
   * @param opId base64 encoded guid string
   */
  @Override
  public void setOperationId(String opId) {
    this.operationId = opId;
  }

  public QueryState getQueryState() {
    return queryState;
  }

  public HookRunner getHookRunner() {
    return hookRunner;
  }

  public void setRuntimeStatsSource(RuntimeStatsSource runtimeStatsSource) {
    this.runtimeStatsSource = runtimeStatsSource;
  }

}
