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
import java.util.List;

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.hooks.HookUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContextImpl;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.reexec.ReCompileException;
import org.apache.hadoop.hive.ql.security.authorization.command.CommandAuthorizer;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.util.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * The compiler compiles the command, by creating a QueryPlan from a String command.
 * Also opens a transaction if necessary.
 */
public class Compiler {
  private static final String CLASS_NAME = Driver.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private static final LogHelper CONSOLE = new LogHelper(LOG);

  private final Context context;
  private final DriverContext driverContext;
  private final QueryState queryState;
  private final DriverState driverState;
  private final PerfLogger perfLogger = SessionState.getPerfLogger();

  private ASTNode tree;

  public Compiler(Context context, DriverContext driverContext, DriverState driverState) {
    this.context = context;
    this.driverContext = driverContext;

    this.queryState = driverContext.getQueryState();
    queryState.setValidTxnList(this::openTxnAndGetValidTxnList);

    this.driverState = driverState;
  }

  /**
   * @param deferClose indicates if the close/destroy should be deferred when the process has been interrupted
   *             it should be set to true if the compile method is called within another method like runInternal,
   *             which defers the close to the called in that method.
   */
  public QueryPlan compile(String rawCommand, boolean deferClose) throws CommandProcessorException {
    initialize(rawCommand);

    Throwable compileException = null;
    boolean parsed = false;
    QueryPlan plan = null;
    try {
      DriverUtils.checkInterrupted(driverState, driverContext, "before parsing and analysing the query", null, null);

      parse();
      parsed = true;
      BaseSemanticAnalyzer sem = analyze();

      DriverUtils.checkInterrupted(driverState, driverContext, "after analyzing query.", null, null);

      plan = createPlan(sem);
      
      if (HiveOperation.START_TRANSACTION == queryState.getHiveOperation()
          || plan.hasAcidResources()) {
        openTxnAndGetValidTxnList();
      }
      verifyTxnState();
      
      initializeFetchTask(plan);
      authorize(sem);
      explainOutput(sem, plan);
    } catch (CommandProcessorException cpe) {
      compileException = cpe.getCause();
      throw cpe;
    } catch (Exception e) {
      compileException = e;
      DriverUtils.checkInterrupted(driverState, driverContext, "during query compilation: " + e.getMessage(), null,
          null);
      handleException(e);
    } finally {
      cleanUp(compileException, parsed, deferClose);
    }

    return plan;
  }

  private void initialize(String rawCommand) throws CommandProcessorException {
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.COMPILE);
    driverState.compilingWithLocking();

    VariableSubstitution variableSubstitution = new VariableSubstitution(
        () -> SessionState.get().getHiveVariables());
    String command = variableSubstitution.substitute(driverContext.getConf(), rawCommand);

    String queryStr = command;
    try {
      // command should be redacted to avoid to logging sensitive data
      queryStr = HookUtils.redactLogString(driverContext.getConf(), command);
    } catch (Exception e) {
      LOG.warn("WARNING! Query command could not be redacted." + e);
    }

    DriverUtils.checkInterrupted(driverState, driverContext, "at beginning of compilation.", null, null);

    context.setCmd(command);
    driverContext.getQueryDisplay().setQueryStr(queryStr);
    LOG.info("Compiling command(queryId=" + driverContext.getQueryId() + "): " + queryStr);

    driverContext.getConf().setQueryString(queryStr);
    // FIXME: side effect will leave the last query set at the session level
    if (SessionState.get() != null) {
      SessionState.get().getConf().setQueryString(queryStr);
      SessionState.get().setupQueryCurrentTimestamp();
    }
  }

  private void parse() throws ParseException {
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.PARSE);

    // Trigger query hook before compilation
    driverContext.getHookRunner().runBeforeParseHook(context.getCmd());

    boolean success = false;
    try {
      tree = ParseUtils.parse(context.getCmd(), context);
      success = true;
    } finally {
      driverContext.getHookRunner().runAfterParseHook(context.getCmd(), !success);
    }
    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.PARSE);
  }

  private BaseSemanticAnalyzer analyze() throws Exception {
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.ANALYZE);

    driverContext.getHookRunner().runBeforeCompileHook(context.getCmd());

    // clear CurrentFunctionsInUse set, to capture new set of functions
    // that SemanticAnalyzer finds are in use
    SessionState.get().getCurrentFunctionsInUse().clear();

    // Flush the metastore cache.  This assures that we don't pick up objects from a previous
    // query running in this same thread.  This has to be done after we get our semantic
    // analyzer (this is when the connection to the metastore is made) but before we analyze,
    // because at that point we need access to the objects.
    Hive.get().getMSC().flushCache();

    boolean executeHooks = driverContext.getHookRunner().hasPreAnalyzeHooks();

    HiveSemanticAnalyzerHookContext hookCtx = new HiveSemanticAnalyzerHookContextImpl();
    if (executeHooks) {
      hookCtx.setConf(driverContext.getConf());
      hookCtx.setUserName(SessionState.get().getUserName());
      hookCtx.setIpAddress(SessionState.get().getUserIpAddress());
      hookCtx.setCommand(context.getCmd());
      hookCtx.setHiveOperation(queryState.getHiveOperation());

      tree = driverContext.getHookRunner().runPreAnalyzeHooks(hookCtx, tree);
    }
    
    // SemanticAnalyzerFactory also sets the hive operation in query state
    BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(queryState, tree);

    if (HiveOperation.REPLDUMP == queryState.getHiveOperation() && !driverContext.isRetrial()) {
      setLastReplIdForDump(queryState.getConf());
    }
    driverContext.setValidTxnListsGenerated(false);

    // Do semantic analysis and plan generation
    try {
      sem.startAnalysis();
      sem.analyze(tree, context);
    } finally {
      sem.endAnalysis();
    }

    if (executeHooks) {
      hookCtx.update(sem);
      driverContext.getHookRunner().runPostAnalyzeHooks(hookCtx, sem.getAllRootTasks());
    }

    LOG.info("Semantic Analysis Completed (retrial = {})", driverContext.isRetrial());

    // Retrieve information about cache usage for the query.
    if (driverContext.getConf().getBoolVar(HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_ENABLED)) {
      driverContext.setCacheUsage(sem.getCacheUsage());
    }

    // validate the plan
    sem.validate();

    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.ANALYZE);
    return sem;
  }

  /**
   * Last repl id should be captured before opening txn by current REPL DUMP operation.
   * This is needed to avoid losing data which are added/modified by concurrent txns when bootstrap
   * dump in progress.
   * @param conf Query configurations
   * @throws HiveException
   * @throws TException
   */
  private void setLastReplIdForDump(HiveConf conf) throws HiveException, TException {
    // Last logged notification event id would be the last repl Id for the current REPl DUMP.
    Hive hiveDb = Hive.get();
    long lastReplId = hiveDb.getMSC().getCurrentNotificationEventId().getEventId();
    conf.setLong(ReplUtils.LAST_REPL_ID_KEY, lastReplId);
    LOG.debug("Setting " + ReplUtils.LAST_REPL_ID_KEY + " = " + lastReplId);
  }

  private String openTxnAndGetValidTxnList() {
    String txnString = driverContext.getConf().get(ValidTxnList.VALID_TXNS_KEY);
    if (SessionState.get().isCompaction()) {
      return txnString;
    }
    HiveTxnManager txnMgr = driverContext.getTxnManager();
    try {
      openTransaction(txnMgr);
      if (txnMgr.isTxnOpen() && Strings.isEmpty(txnString)) {
        txnString = generateValidTxnList(txnMgr);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to open a new transaction", e);
    }
    return txnString;
  }

  private void openTransaction(HiveTxnManager txnMgr) throws LockException, CommandProcessorException {
    if (txnMgr.isTxnOpen() || !DriverUtils.checkConcurrency(driverContext)
        || !startImplicitTxn()) {
      return;
    }
    TxnType txnType = AcidUtils.getTxnType(driverContext.getConf(), tree);
    driverContext.setTxnType(txnType);
    
    HiveOperation hiveOperation = queryState.getHiveOperation();
    if ((HiveOperation.REPLDUMP == hiveOperation || HiveOperation.REPLLOAD == hiveOperation) 
          && !context.isExplainPlan()) {
      context.setReplPolicy(PlanUtils.stripQuotes(tree.getChild(0).getText()));
    }
    String userFromUGI = DriverUtils.getUserFromUGI(driverContext);
    txnMgr.openTxn(context, userFromUGI, txnType);
  }
  
  private boolean startImplicitTxn() {
    //this is dumb. HiveOperation is not always set. see HIVE-16447/HIVE-16443
    HiveOperation hiveOperation = queryState.getHiveOperation();
    switch (hiveOperation == null ? HiveOperation.QUERY : hiveOperation) {
      case COMMIT:
      case ROLLBACK:
      case SWITCHDATABASE:
      case SET_AUTOCOMMIT:
        /**
         * autocommit is here for completeness.  TM doesn't use it.  If we want to support JDBC
         * semantics (or any other definition of autocommit) it should be done at session level.
         */
      case SHOWDATABASES:
      case SHOWTABLES:
      case SHOW_TABLESTATUS:
      case SHOW_TBLPROPERTIES:
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
        return false;
      //this implies that no locks are needed for such a command
      default:
        return true; // TODO: check if we could optimize !context.isExplainPlan()
    }
  }
  
  private void verifyTxnState() throws LockException {
    HiveOperation hiveOperation = queryState.getHiveOperation();
    if (hiveOperation != null && hiveOperation.isRequiresOpenTransaction()
          && !driverContext.getTxnManager().isTxnOpen()) {
      throw new LockException(null, ErrorMsg.OP_NOT_ALLOWED_WITHOUT_TXN, hiveOperation.getOperationName());
    }
  }

  private String generateValidTxnList(HiveTxnManager txnMgr) throws LockException {
    // Record current valid txn list that will be used throughout the query
    // compilation and processing. We only do this if 1) a transaction
    // was already opened and 2) the list has not been recorded yet,
    // e.g., by an explicit open transaction command.
    try {
      String txnString = txnMgr.getValidTxns().toString();
      driverContext.getConf().set(ValidTxnList.VALID_TXNS_KEY, txnString);
      LOG.debug("Encoding valid txns info {}, txnid: {}", txnString, txnMgr.getCurrentTxnId());
      
      driverContext.setValidTxnListsGenerated(true);
      return txnString;
    } catch (LockException e) {
      LOG.error("Exception while acquiring valid txn list", e);
      throw e;
    }
  }

  private QueryPlan createPlan(BaseSemanticAnalyzer sem) {
    // get the output schema
    setSchema(sem);
    QueryPlan plan = new QueryPlan(driverContext.getQueryString(), sem,
        driverContext.getQueryDisplay().getQueryStartTime(), driverContext.getQueryId(),
        queryState.getHiveOperation(), driverContext.getSchema());
    // save the optimized plan and sql for the explain
    plan.setOptimizedCBOPlan(context.getCalcitePlan());
    plan.setOptimizedQueryString(context.getOptimizedSql());

    // this is required so that later driver can skip executing prepare queries
    if (sem.isPrepareQuery()) {
      plan.setPrepareQuery(true);
    }
    return plan;
  }

  protected void initializeFetchTask(QueryPlan plan) {
    // for PREPARE statement we should avoid initializing operators
    if (plan.isPrepareQuery()) {
      return;
    }
    // initialize FetchTask right here
    if (plan.getFetchTask() != null) {
      plan.getFetchTask().initialize(queryState, plan, null, context);
    }
  }

  /**
   * Get a Schema with fields represented with native Hive types.
   */
  private void setSchema(BaseSemanticAnalyzer sem) {
    Schema schema = new Schema();

    // If we have a plan, prefer its logical result schema if it's available; otherwise, try digging out a fetch task;
    // failing that, give up.
    if (sem == null) {
      LOG.info("No semantic analyzer, using empty schema.");
    } else if (sem.getResultSchema() != null) {
      List<FieldSchema> lst = sem.getResultSchema();
      schema = new Schema(lst, null);
    } else if (sem.getFetchTask() != null) {
      FetchTask ft = sem.getFetchTask();
      TableDesc td = ft.getTblDesc();
      // partitioned tables don't have tableDesc set on the FetchTask. Instead they have a list of PartitionDesc
      // objects, each with a table desc. Let's try to fetch the desc for the first partition and use it's deserializer.
      if (td == null && ft.getWork() != null && ft.getWork().getPartDesc() != null) {
        if (ft.getWork().getPartDesc().size() > 0) {
          td = ft.getWork().getPartDesc().get(0).getTableDesc();
        }
      }

      if (td == null) {
        LOG.info("No returning schema, using empty schema");
      } else {
        String tableName = "result";
        List<FieldSchema> lst = null;
        try {
          lst = HiveMetaStoreUtils.getFieldsFromDeserializer(tableName, td.getDeserializer(driverContext.getConf()),
              driverContext.getConf());
        } catch (Exception e) {
          LOG.warn("Error getting schema", e);
        }
        if (lst != null) {
          schema = new Schema(lst, null);
        }
      }
    }

    LOG.info("Created Hive schema: " + schema);
    driverContext.setSchema(schema);
  }

  private void authorize(BaseSemanticAnalyzer sem) throws HiveException, CommandProcessorException {
    // do the authorization check
    if (!sem.skipAuthorization()) {
      try {
        perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.DO_AUTHORIZATION);
        // Authorization check for kill query will be in KillQueryImpl
        // As both admin or operation owner can perform the operation.
        // Which is not directly supported in authorizer
        if (queryState.getHiveOperation() != HiveOperation.KILL_QUERY) {
          CommandAuthorizer.doAuthorization(queryState.getHiveOperation(), sem, context.getCmd());
        }
      } catch (AuthorizationException authExp) {
        CONSOLE.printError("Authorization failed:" + authExp.getMessage() + ". Use SHOW GRANT to get more details.");
        throw DriverUtils.createProcessorException(driverContext, 403, authExp.getMessage(), "42000", null);
      } finally {
        perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.DO_AUTHORIZATION);
      }
    }
  }

  private void explainOutput(BaseSemanticAnalyzer sem, QueryPlan plan) throws IOException {
    if (driverContext.getConf().getBoolVar(ConfVars.HIVE_LOG_EXPLAIN_OUTPUT) ||
        driverContext.getConf().getBoolVar(ConfVars.HIVE_SERVER2_WEBUI_EXPLAIN_OUTPUT)) {
      String explainOutput = ExplainTask.getExplainOutput(sem, plan, tree, queryState,
          context, driverContext.getConf());
      if (explainOutput != null) {
        if (driverContext.getConf().getBoolVar(ConfVars.HIVE_LOG_EXPLAIN_OUTPUT)) {
          if (driverContext.getConf().getBoolVar(ConfVars.HIVE_LOG_EXPLAIN_OUTPUT_TO_CONSOLE)) {
            CONSOLE.printInfo("EXPLAIN output for queryid " + driverContext.getQueryId() + " : " + explainOutput);
          } else {
            LOG.info("EXPLAIN output for queryid " + driverContext.getQueryId() + " : " + explainOutput);
          }
        }
        if (driverContext.getConf().isWebUiQueryInfoCacheEnabled() &&
            driverContext.getConf().getBoolVar(ConfVars.HIVE_SERVER2_WEBUI_EXPLAIN_OUTPUT)) {
          driverContext.getQueryDisplay().setExplainPlan(explainOutput);
        }
      }
    }
  }

  private void handleException(Exception e) throws CommandProcessorException {
    ErrorMsg error = ErrorMsg.getErrorMsg(e.getMessage());
    String errorMessage = "FAILED: " + e.getClass().getSimpleName();
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
      errorMessage += ". Failed command: " + driverContext.getQueryString();
    }

    if (!(e instanceof ReCompileException)) {
      CONSOLE.printError(errorMessage, "\n" + StringUtils.stringifyException(e));
    }
    throw DriverUtils.createProcessorException(driverContext, error.getErrorCode(), errorMessage, error.getSQLState(),
        e);
  }

  private void cleanUp(Throwable compileException, boolean parsed, boolean deferClose) {
    double duration = perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.COMPILE) / 1000.00;
    // Trigger post compilation hook. Note that if the compilation fails here then
    // before/after execution hook will never be executed.
    if (parsed) {
      try {
        driverContext.getHookRunner().runAfterCompilationHook(driverContext, context, compileException);
      } catch (Exception e) {
        LOG.warn("Failed when invoking query after-compilation hook.", e);
      }
    }

    ImmutableMap<String, Long> compileHMSTimings = Hive.dumpMetaCallTimingWithoutEx("compilation");
    driverContext.getQueryDisplay().setHmsTimings(QueryDisplay.Phase.COMPILATION, compileHMSTimings);

    if (driverState.isAborted()) {
      driverState.compilationInterruptedWithLocking(deferClose);
      LOG.info("Compiling command(queryId={}) has been interrupted after {} seconds", driverContext.getQueryId(),
          duration);
    } else {
      driverState.compilationFinishedWithLocking(compileException != null);
      LOG.info("Completed compiling command(queryId={}); Time taken: {} seconds", driverContext.getQueryId(),
          duration);
    }
  }
}
