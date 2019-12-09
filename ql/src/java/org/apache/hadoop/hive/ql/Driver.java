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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

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
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.cache.results.CacheUsage;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache.CacheEntry;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator;
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
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.PrivateHookContext;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lock.CompileLock;
import org.apache.hadoop.hive.ql.lock.CompileLockFactory;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
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
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.hive.common.util.TxnIdUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

public class Driver implements IDriver {

  private static final String CLASS_NAME = Driver.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private static final LogHelper CONSOLE = new LogHelper(LOG);
  private static final int SHUTDOWN_HOOK_PRIORITY = 0;
  private Runnable shutdownRunner = null;

  private int maxRows = 100;
  private ByteStream.Output bos = new ByteStream.Output();

  private DataInput resStream;
  private Context context;
  private final DriverContext driverContext;
  private TaskQueue taskQueue;
  private final List<HiveLock> hiveLocks = new ArrayList<HiveLock>();

  // HS2 operation handle guid string
  private String operationId;

  private DriverState driverState = new DriverState();

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

  public Driver(QueryState queryState, QueryInfo queryInfo, HiveTxnManager txnManager) {
    driverContext = new DriverContext(queryState, queryInfo, new HookRunner(queryState.getConf(), CONSOLE),
        txnManager);
  }

  public Driver(QueryState queryState, QueryInfo queryInfo, HiveTxnManager txnManager,
      ValidWriteIdList compactionWriteIds, long compactorTxnId) {
    this(queryState, queryInfo, txnManager);
    driverContext.setCompactionWriteIds(compactionWriteIds);
    driverContext.setCompactorTxnId(compactorTxnId);
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

  // Checks whether txn list has been invalidated while planning the query.
  // This would happen if query requires exclusive/semi-shared lock, and there
  // has been a committed transaction on the table over which the lock is
  // required.
  private boolean isValidTxnListState() throws LockException {
    // 1) Get valid txn list.
    String txnString = driverContext.getConf().get(ValidTxnList.VALID_TXNS_KEY);
    if (txnString == null) {
      // Not a transactional op, nothing more to do
      return true;
    }
    ValidTxnList currentTxnList = driverContext.getTxnManager().getValidTxns();
    String currentTxnString = currentTxnList.toString();
    if (currentTxnString.equals(txnString)) {
      // Still valid, nothing more to do
      return true;
    }
    // 2) Get locks that are relevant:
    // - Exclusive for INSERT OVERWRITE.
    // - Semi-shared for UPDATE/DELETE.
    if (context.getHiveLocks() == null || context.getHiveLocks().isEmpty()) {
      // Nothing to check
      return true;
    }
    Set<String> nonSharedLocks = new HashSet<>();
    for (HiveLock lock : context.getHiveLocks()) {
      if (lock.mayContainComponents()) {
        // The lock may have multiple components, e.g., DbHiveLock, hence we need
        // to check for each of them
        for (LockComponent lckCmp : lock.getHiveLockComponents()) {
          // We only consider tables for which we hold either an exclusive
          // or a shared write lock
          if ((lckCmp.getType() == LockType.EXCLUSIVE ||
              lckCmp.getType() == LockType.SHARED_WRITE) &&
              lckCmp.getTablename() != null) {
            nonSharedLocks.add(
                TableName.getDbTable(
                    lckCmp.getDbname(), lckCmp.getTablename()));
          }
        }
      } else {
        // The lock has a single components, e.g., SimpleHiveLock or ZooKeeperHiveLock.
        // Pos 0 of lock paths array contains dbname, pos 1 contains tblname
        if ((lock.getHiveLockMode() == HiveLockMode.EXCLUSIVE ||
            lock.getHiveLockMode() == HiveLockMode.SEMI_SHARED) &&
            lock.getHiveLockObject().getPaths().length == 2) {
          nonSharedLocks.add(
              TableName.getDbTable(
                  lock.getHiveLockObject().getPaths()[0], lock.getHiveLockObject().getPaths()[1]));
        }
      }
    }
    // 3) Get txn tables that are being written
    String txnWriteIdListStr = driverContext.getConf().get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY);
    if (txnWriteIdListStr == null || txnWriteIdListStr.length() == 0) {
      // Nothing to check
      return true;
    }
    ValidTxnWriteIdList txnWriteIdList = new ValidTxnWriteIdList(txnWriteIdListStr);
    Map<String, Table> writtenTables = getWrittenTables(driverContext.getPlan());

    ValidTxnWriteIdList currentTxnWriteIds =
        driverContext.getTxnManager().getValidWriteIds(
            writtenTables.entrySet().stream()
                .filter(e -> AcidUtils.isTransactionalTable(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList()),
            currentTxnString);

    for (Map.Entry<String, Table> tableInfo : writtenTables.entrySet()) {
      String fullQNameForLock = TableName.getDbTable(
          tableInfo.getValue().getDbName(),
          MetaStoreUtils.encodeTableName(tableInfo.getValue().getTableName()));
      if (nonSharedLocks.contains(fullQNameForLock)) {
        // Check if table is transactional
        if (AcidUtils.isTransactionalTable(tableInfo.getValue())) {
          // Check that write id is still valid
          if (!TxnIdUtils.checkEquivalentWriteIds(
              txnWriteIdList.getTableValidWriteIdList(tableInfo.getKey()),
              currentTxnWriteIds.getTableValidWriteIdList(tableInfo.getKey()))) {
            // Write id has changed, it is not valid anymore,
            // we need to recompile
            return false;
          }
        }
        nonSharedLocks.remove(fullQNameForLock);
      }
    }
    if (!nonSharedLocks.isEmpty()) {
      throw new LockException("Wrong state: non-shared locks contain information for tables that have not" +
          " been visited when trying to validate the locks from query tables.\n" +
          "Tables: " + writtenTables.keySet() + "\n" +
          "Remaining locks after check: " + nonSharedLocks);
    }
    // It passes the test, it is valid
    return true;
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

  // Write the current set of valid write ids for the operated acid tables into the conf file so
  // that it can be read by the input format.
  private ValidTxnWriteIdList recordValidWriteIds(HiveTxnManager txnMgr) throws LockException {
    String txnString = driverContext.getConf().get(ValidTxnList.VALID_TXNS_KEY);
    if ((txnString == null) || (txnString.isEmpty())) {
      throw new IllegalStateException("calling recordValidWritsIdss() without initializing ValidTxnList " +
              JavaUtils.txnIdToString(txnMgr.getCurrentTxnId()));
    }
    List<String> txnTables = getTransactionalTables(driverContext.getPlan());
    ValidTxnWriteIdList txnWriteIds = null;
    if (driverContext.getCompactionWriteIds() != null) {
      /**
       * This is kludgy: here we need to read with Compactor's snapshot/txn
       * rather than the snapshot of the current {@code txnMgr}, in effect
       * simulating a "flashback query" but can't actually share compactor's
       * txn since it would run multiple statements.  See more comments in
       * {@link org.apache.hadoop.hive.ql.txn.compactor.Worker} where it start
       * the compactor txn*/
      if (txnTables.size() != 1) {
        throw new LockException("Unexpected tables in compaction: " + txnTables);
      }
      txnWriteIds = new ValidTxnWriteIdList(driverContext.getCompactorTxnId());
      txnWriteIds.addTableValidWriteIdList(driverContext.getCompactionWriteIds());
    } else {
      txnWriteIds = txnMgr.getValidWriteIds(txnTables, txnString);
    }
    if (driverContext.getTxnType() == TxnType.READ_ONLY && !getWrittenTables(driverContext.getPlan()).isEmpty()) {
      throw new IllegalStateException(String.format(
          "Inferred transaction type '%s' doesn't conform to the actual query string '%s'",
          driverContext.getTxnType(), driverContext.getQueryState().getQueryString()));
    }

    String writeIdStr = txnWriteIds.toString();
    driverContext.getConf().set(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY, writeIdStr);
    if (driverContext.getPlan().getFetchTask() != null) {
      /**
       * This is needed for {@link HiveConf.ConfVars.HIVEFETCHTASKCONVERSION} optimization which
       * initializes JobConf in FetchOperator before recordValidTxns() but this has to be done
       * after locks are acquired to avoid race conditions in ACID.
       * This case is supported only for single source query.
       */
      Operator<?> source = driverContext.getPlan().getFetchTask().getWork().getSource();
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
          driverContext.getPlan().getFetchTask().setValidWriteIdList(writeIdList.toString());
        }
      }
    }
    LOG.debug("Encoding valid txn write ids info " + writeIdStr + " txnid:" + txnMgr.getCurrentTxnId());
    return txnWriteIds;
  }

  // Make the list of transactional tables that are read or written by current txn
  private List<String> getTransactionalTables(QueryPlan plan) {
    Map<String, Table> tables = new HashMap<>();
    plan.getInputs().forEach(
        input -> addTableFromEntity(input, tables)
    );
    plan.getOutputs().forEach(
        output -> addTableFromEntity(output, tables)
    );
    return tables.entrySet().stream()
      .filter(entry -> AcidUtils.isTransactionalTable(entry.getValue()))
      .map(Map.Entry::getKey)
      .collect(Collectors.toList());
  }

  // Make the map of tables written by current txn
  private Map<String, Table> getWrittenTables(QueryPlan plan) {
    Map<String, Table> tables = new HashMap<>();
    plan.getOutputs().forEach(
        output -> addTableFromEntity(output, tables)
    );
    return tables;
  }

  private void addTableFromEntity(Entity entity, Map<String, Table> tables) {
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
    String fullTableName = AcidUtils.getFullTableName(tbl.getDbName(), tbl.getTableName());
    tables.put(fullTableName, tbl);
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
        recordValidWriteIds(driverContext.getTxnManager());
      }

    } catch (Exception e) {
      String errorMessage = "FAILED: Error in acquiring locks: " + e.getMessage();
      CONSOLE.printError(errorMessage, "\n" + StringUtils.stringifyException(e));
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
        if (!isValidTxnListState()) {
          LOG.info("Compiling after acquiring locks");
          // Snapshot was outdated when locks were acquired, hence regenerate context,
          // txn list and retry
          // TODO: Lock acquisition should be moved before analyze, this is a bit hackish.
          // Currently, we acquire a snapshot, we compile the query wrt that snapshot,
          // and then, we acquire locks. If snapshot is still valid, we continue as usual.
          // But if snapshot is not valid, we recompile the query.
          driverContext.setRetrial(true);
          driverContext.getBackupContext().addRewrittenStatementContext(context);
          driverContext.getBackupContext().setHiveLocks(context.getHiveLocks());
          context = driverContext.getBackupContext();
          driverContext.getConf().set(ValidTxnList.VALID_TXNS_KEY,
              driverContext.getTxnManager().getValidTxns().toString());
          if (driverContext.getPlan().hasAcidResourcesInQuery()) {
            recordValidWriteIds(driverContext.getTxnManager());
          }

          if (!alreadyCompiled) {
            // compile internal will automatically reset the perf logger
            compileInternal(command, true);
          } else {
            // Since we're reusing the compiled plan, we need to update its start time for current run
            driverContext.getPlan().setQueryStartTime(driverContext.getQueryDisplay().getQueryStartTime());
          }

          if (!isValidTxnListState()) {
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
        execute();
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

  private void useFetchFromCache(CacheEntry cacheEntry) {
    // Change query FetchTask to use new location specified in results cache.
    FetchTask fetchTaskFromCache = (FetchTask) TaskFactory.get(cacheEntry.getFetchWork());
    fetchTaskFromCache.initialize(driverContext.getQueryState(), driverContext.getPlan(), null, context);
    driverContext.getPlan().setFetchTask(fetchTaskFromCache);
    driverContext.setCacheUsage(new CacheUsage(CacheUsage.CacheStatus.QUERY_USING_CACHE, cacheEntry));
  }

  private void preExecutionCacheActions() throws Exception {
    if (driverContext.getCacheUsage() != null) {
      if (driverContext.getCacheUsage().getStatus() == CacheUsage.CacheStatus.CAN_CACHE_QUERY_RESULTS &&
          driverContext.getPlan().getFetchTask() != null) {
        ValidTxnWriteIdList txnWriteIdList = null;
        if (driverContext.getPlan().hasAcidResourcesInQuery()) {
          txnWriteIdList = AcidUtils.getValidTxnWriteIdList(driverContext.getConf());
        }
        // The results of this query execution might be cacheable.
        // Add a placeholder entry in the cache so other queries know this result is pending.
        CacheEntry pendingCacheEntry =
            QueryResultsCache.getInstance().addToCache(driverContext.getCacheUsage().getQueryInfo(), txnWriteIdList);
        if (pendingCacheEntry != null) {
          // Update cacheUsage to reference the pending entry.
          this.driverContext.getCacheUsage().setCacheEntry(pendingCacheEntry);
        }
      }
    }
  }

  private void postExecutionCacheActions() throws Exception {
    if (driverContext.getCacheUsage() != null) {
      if (driverContext.getCacheUsage().getStatus() == CacheUsage.CacheStatus.QUERY_USING_CACHE) {
        // Using a previously cached result.
        CacheEntry cacheEntry = driverContext.getCacheUsage().getCacheEntry();

        // Reader count already incremented during cache lookup.
        // Save to usedCacheEntry to ensure reader is released after query.
        driverContext.setUsedCacheEntry(cacheEntry);
      } else if (driverContext.getCacheUsage().getStatus() == CacheUsage.CacheStatus.CAN_CACHE_QUERY_RESULTS &&
          driverContext.getCacheUsage().getCacheEntry() != null &&
          driverContext.getPlan().getFetchTask() != null) {
        // Save results to the cache for future queries to use.
        PerfLogger perfLogger = SessionState.getPerfLogger();
        perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SAVE_TO_RESULTS_CACHE);

        ValidTxnWriteIdList txnWriteIdList = null;
        if (driverContext.getPlan().hasAcidResourcesInQuery()) {
          txnWriteIdList = AcidUtils.getValidTxnWriteIdList(driverContext.getConf());
        }
        CacheEntry cacheEntry = driverContext.getCacheUsage().getCacheEntry();
        boolean savedToCache = QueryResultsCache.getInstance().setEntryValid(
            cacheEntry,
            driverContext.getPlan().getFetchTask().getWork());
        LOG.info("savedToCache: {} ({})", savedToCache, cacheEntry);
        if (savedToCache) {
          useFetchFromCache(driverContext.getCacheUsage().getCacheEntry());
          // setEntryValid() already increments the reader count. Set usedCacheEntry so it gets released.
          driverContext.setUsedCacheEntry(driverContext.getCacheUsage().getCacheEntry());
        }

        perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SAVE_TO_RESULTS_CACHE);
      }
    }
  }

  private void execute() throws CommandProcessorException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.DRIVER_EXECUTE);

    boolean noName = Strings.isNullOrEmpty(driverContext.getConf().get(MRJobConfig.JOB_NAME));

    int maxlen;
    if ("spark".equals(driverContext.getConf().getVar(ConfVars.HIVE_EXECUTION_ENGINE))) {
      maxlen = driverContext.getConf().getIntVar(HiveConf.ConfVars.HIVESPARKJOBNAMELENGTH);
    } else {
      maxlen = driverContext.getConf().getIntVar(HiveConf.ConfVars.HIVEJOBNAMELENGTH);
    }
    Metrics metrics = MetricsFactory.getInstance();

    String queryId = driverContext.getPlan().getQueryId();
    // Get the query string from the conf file as the compileInternal() method might
    // hide sensitive information during query redaction.
    String queryStr = driverContext.getConf().getQueryString();

    driverState.lock();
    try {
      // if query is not in compiled state, or executing state which is carried over from
      // a combined compile/execute in runInternal, throws the error
      if (!driverState.isCompiled() && !driverState.isExecuting()) {
        String errorMessage = "FAILED: unexpected driverstate: " + driverState + ", for query " + queryStr;
        CONSOLE.printError(errorMessage);
        throw DriverUtils.createProcessorException(driverContext, 1000, errorMessage, "HY008", null);
      } else {
        driverState.executing();
      }
    } finally {
      driverState.unlock();
    }

    HookContext hookContext = null;

    // Whether there's any error occurred during query execution. Used for query lifetime hook.
    boolean executionError = false;

    try {
      LOG.info("Executing command(queryId=" + queryId + "): " + queryStr);
      // compile and execute can get called from different threads in case of HS2
      // so clear timing in this thread's Hive object before proceeding.
      Hive.get().clearMetaCallTiming();

      driverContext.getPlan().setStarted();

      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().startQuery(queryStr, queryId);
        SessionState.get().getHiveHistory().logPlanProgress(driverContext.getPlan());
      }
      resStream = null;

      SessionState ss = SessionState.get();

      // TODO: should this use getUserFromAuthenticator?
      hookContext = new PrivateHookContext(driverContext.getPlan(), driverContext.getQueryState(),
          context.getPathToCS(), SessionState.get().getUserName(), ss.getUserIpAddress(),
          InetAddress.getLocalHost().getHostAddress(), operationId, ss.getSessionId(), Thread.currentThread().getName(),
          ss.isHiveServerQuery(), perfLogger, driverContext.getQueryInfo(), context);
      hookContext.setHookType(HookContext.HookType.PRE_EXEC_HOOK);

      driverContext.getHookRunner().runPreHooks(hookContext);

      // Trigger query hooks before query execution.
      driverContext.getHookRunner().runBeforeExecutionHook(queryStr, hookContext);

      setQueryDisplays(driverContext.getPlan().getRootTasks());
      int mrJobs = Utilities.getMRTasks(driverContext.getPlan().getRootTasks()).size();
      int jobs = mrJobs + Utilities.getTezTasks(driverContext.getPlan().getRootTasks()).size()
          + Utilities.getSparkTasks(driverContext.getPlan().getRootTasks()).size();
      if (jobs > 0) {
        logMrWarning(mrJobs);
        CONSOLE.printInfo("Query ID = " + queryId);
        CONSOLE.printInfo("Total jobs = " + jobs);
      }
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setQueryProperty(queryId, Keys.QUERY_NUM_TASKS,
            String.valueOf(jobs));
        SessionState.get().getHiveHistory().setIdToTableMap(driverContext.getPlan().getIdToTableNameMap());
      }
      String jobname = Utilities.abbreviate(queryStr, maxlen - 6);

      // A runtime that launches runnable tasks as separate Threads through
      // TaskRunners
      // As soon as a task isRunnable, it is put in a queue
      // At any time, at most maxthreads tasks can be running
      // The main thread polls the TaskRunners to check if they have finished.

      DriverUtils.checkInterrupted(driverState, driverContext, "before running tasks.", hookContext, perfLogger);

      taskQueue = new TaskQueue(context); // for canceling the query (should be bound to session?)
      taskQueue.prepare(driverContext.getPlan());

      context.setHDFSCleanup(true);

      SessionState.get().setMapRedStats(new LinkedHashMap<>());
      SessionState.get().setStackTraces(new HashMap<>());
      SessionState.get().setLocalMapRedErrors(new HashMap<>());

      // Add root Tasks to runnable
      for (Task<?> tsk : driverContext.getPlan().getRootTasks()) {
        // This should never happen, if it does, it's a bug with the potential to produce
        // incorrect results.
        assert tsk.getParentTasks() == null || tsk.getParentTasks().isEmpty();
        taskQueue.addToRunnable(tsk);

        if (metrics != null) {
          tsk.updateTaskMetrics(metrics);
        }
      }

      preExecutionCacheActions();

      perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.RUN_TASKS);
      // Loop while you either have tasks running, or tasks queued up
      while (taskQueue.isRunning()) {
        // Launch upto maxthreads tasks
        Task<?> task;
        int maxthreads = HiveConf.getIntVar(driverContext.getConf(), HiveConf.ConfVars.EXECPARALLETHREADNUMBER);
        while ((task = taskQueue.getRunnable(maxthreads)) != null) {
          TaskRunner runner = launchTask(task, queryId, noName, jobname, jobs, taskQueue);
          if (!runner.isRunning()) {
            break;
          }
        }

        // poll the Tasks to see which one completed
        TaskRunner tskRun = taskQueue.pollFinished();
        if (tskRun == null) {
          continue;
        }
        /*
          This should be removed eventually. HIVE-17814 gives more detail
          explanation of whats happening and HIVE-17815 as to why this is done.
          Briefly for replication the graph is huge and so memory pressure is going to be huge if
          we keep a lot of references around.
        */
        String opName = driverContext.getPlan().getOperationName();
        boolean isReplicationOperation = opName.equals(HiveOperation.REPLDUMP.getOperationName())
            || opName.equals(HiveOperation.REPLLOAD.getOperationName());
        if (!isReplicationOperation) {
          hookContext.addCompleteTask(tskRun);
        }

        driverContext.getQueryDisplay().setTaskResult(tskRun.getTask().getId(), tskRun.getTaskResult());

        Task<?> tsk = tskRun.getTask();
        TaskResult result = tskRun.getTaskResult();

        int exitVal = result.getExitVal();
        DriverUtils.checkInterrupted(driverState, driverContext, "when checking the execution result.", hookContext,
            perfLogger);

        if (exitVal != 0) {
          Task<?> backupTask = tsk.getAndInitBackupTask();
          if (backupTask != null) {
            String errorMessage = getErrorMsgAndDetail(exitVal, result.getTaskError(), tsk);
            CONSOLE.printError(errorMessage);
            errorMessage = "ATTEMPT: Execute BackupTask: " + backupTask.getClass().getName();
            CONSOLE.printError(errorMessage);

            // add backup task to runnable
            if (TaskQueue.isLaunchable(backupTask)) {
              taskQueue.addToRunnable(backupTask);
            }
            continue;

          } else {
            String errorMessage = getErrorMsgAndDetail(exitVal, result.getTaskError(), tsk);
            if (taskQueue.isShutdown()) {
              errorMessage = "FAILED: Operation cancelled. " + errorMessage;
            }
            DriverUtils.invokeFailureHooks(driverContext, perfLogger, hookContext,
              errorMessage + Strings.nullToEmpty(tsk.getDiagnosticsMessage()), result.getTaskError());
            String sqlState = "08S01";

            // 08S01 (Communication error) is the default sql state.  Override the sqlstate
            // based on the ErrorMsg set in HiveException.
            if (result.getTaskError() instanceof HiveException) {
              ErrorMsg errorMsg = ((HiveException) result.getTaskError()).
                  getCanonicalErrorMsg();
              if (errorMsg != ErrorMsg.GENERIC_ERROR) {
                sqlState = errorMsg.getSQLState();
              }
            }

            CONSOLE.printError(errorMessage);
            taskQueue.shutdown();
            // in case we decided to run everything in local mode, restore the
            // the jobtracker setting to its initial value
            context.restoreOriginalTracker();
            throw DriverUtils.createProcessorException(driverContext, exitVal, errorMessage, sqlState,
                result.getTaskError());
          }
        }

        taskQueue.finished(tskRun);

        if (SessionState.get() != null) {
          SessionState.get().getHiveHistory().setTaskProperty(queryId, tsk.getId(),
              Keys.TASK_RET_CODE, String.valueOf(exitVal));
          SessionState.get().getHiveHistory().endTask(queryId, tsk);
        }

        if (tsk.getChildTasks() != null) {
          for (Task<?> child : tsk.getChildTasks()) {
            if (TaskQueue.isLaunchable(child)) {
              taskQueue.addToRunnable(child);
            }
          }
        }
      }
      perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.RUN_TASKS);

      postExecutionCacheActions();

      // in case we decided to run everything in local mode, restore the
      // the jobtracker setting to its initial value
      context.restoreOriginalTracker();

      if (taskQueue.isShutdown()) {
        String errorMessage = "FAILED: Operation cancelled";
        DriverUtils.invokeFailureHooks(driverContext, perfLogger, hookContext, errorMessage, null);
        CONSOLE.printError(errorMessage);
        throw DriverUtils.createProcessorException(driverContext, 1000, errorMessage, "HY008", null);
      }

      // remove incomplete outputs.
      // Some incomplete outputs may be added at the beginning, for eg: for dynamic partitions.
      // remove them
      HashSet<WriteEntity> remOutputs = new LinkedHashSet<WriteEntity>();
      for (WriteEntity output : driverContext.getPlan().getOutputs()) {
        if (!output.isComplete()) {
          remOutputs.add(output);
        }
      }

      for (WriteEntity output : remOutputs) {
        driverContext.getPlan().getOutputs().remove(output);
      }


      hookContext.setHookType(HookContext.HookType.POST_EXEC_HOOK);

      driverContext.getHookRunner().runPostExecHooks(hookContext);

      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setQueryProperty(queryId, Keys.QUERY_RET_CODE,
            String.valueOf(0));
        SessionState.get().getHiveHistory().printRowCount(queryId);
      }
      releasePlan(driverContext.getPlan());
    } catch (CommandProcessorException cpe) {
      executionError = true;
      throw cpe;
    } catch (Throwable e) {
      executionError = true;

      DriverUtils.checkInterrupted(driverState, driverContext, "during query execution: \n" + e.getMessage(),
          hookContext, perfLogger);

      context.restoreOriginalTracker();
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setQueryProperty(queryId, Keys.QUERY_RET_CODE,
            String.valueOf(12));
      }
      // TODO: do better with handling types of Exception here
      String errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
      if (hookContext != null) {
        try {
          DriverUtils.invokeFailureHooks(driverContext, perfLogger, hookContext, errorMessage, e);
        } catch (Exception t) {
          LOG.warn("Failed to invoke failure hook", t);
        }
      }
      CONSOLE.printError(errorMessage + "\n" + StringUtils.stringifyException(e));
      throw DriverUtils.createProcessorException(driverContext, 12, errorMessage, "08S01", e);
    } finally {
      // Trigger query hooks after query completes its execution.
      try {
        driverContext.getHookRunner().runAfterExecutionHook(queryStr, hookContext, executionError);
      } catch (Exception e) {
        LOG.warn("Failed when invoking query after execution hook", e);
      }

      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().endQuery(queryId);
      }
      if (noName) {
        driverContext.getConf().set(MRJobConfig.JOB_NAME, "");
      }
      double duration = perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.DRIVER_EXECUTE)/1000.00;

      ImmutableMap<String, Long> executionHMSTimings = Hive.dumpMetaCallTimingWithoutEx("execution");
      driverContext.getQueryDisplay().setHmsTimings(QueryDisplay.Phase.EXECUTION, executionHMSTimings);

      Map<String, MapRedStats> stats = SessionState.get().getMapRedStats();
      if (stats != null && !stats.isEmpty()) {
        long totalCpu = 0;
        long numModifiedRows = 0;
        CONSOLE.printInfo("MapReduce Jobs Launched: ");
        for (Map.Entry<String, MapRedStats> entry : stats.entrySet()) {
          CONSOLE.printInfo("Stage-" + entry.getKey() + ": " + entry.getValue());
          totalCpu += entry.getValue().getCpuMSec();

          if (numModifiedRows > -1) {
            //if overflow, then numModifiedRows is set as -1. Else update numModifiedRows with the sum.
            numModifiedRows = addWithOverflowCheck(numModifiedRows, entry.getValue().getNumModifiedRows());
          }
        }
        driverContext.getQueryState().setNumModifiedRows(numModifiedRows);
        CONSOLE.printInfo("Total MapReduce CPU Time Spent: " + Utilities.formatMsecToStr(totalCpu));
      }
      SparkSession ss = SessionState.get().getSparkSession();
      if (ss != null) {
        ss.onQueryCompletion(queryId);
      }
      driverState.lock();
      try {
        driverState.executionFinished(executionError);
      } finally {
        driverState.unlock();
      }
      if (driverState.isAborted()) {
        LOG.info("Executing command(queryId=" + queryId + ") has been interrupted after " + duration + " seconds");
      } else {
        LOG.info("Completed executing command(queryId=" + queryId + "); Time taken: " + duration + " seconds");
      }
    }
  }

  private long addWithOverflowCheck(long val1, long val2) {
    try {
      return Math.addExact(val1, val2);
    } catch (ArithmeticException e) {
      return -1;
    }
  }

  private void releasePlan(QueryPlan plan) {
    // Plan maybe null if Driver.close is called in another thread for the same Driver object
    driverState.lock();
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
      driverState.unlock();
    }
  }

  private void setQueryDisplays(List<Task<?>> tasks) {
    if (tasks != null) {
      Set<Task<?>> visited = new HashSet<Task<?>>();
      while (!tasks.isEmpty()) {
        tasks = setQueryDisplays(tasks, visited);
      }
    }
  }

  private List<Task<?>> setQueryDisplays(
          List<Task<?>> tasks,
          Set<Task<?>> visited) {
    List<Task<?>> childTasks = new ArrayList<>();
    for (Task<?> task : tasks) {
      if (visited.contains(task)) {
        continue;
      }
      task.setQueryDisplay(driverContext.getQueryDisplay());
      if (task.getDependentTasks() != null) {
        childTasks.addAll(task.getDependentTasks());
      }
      visited.add(task);
    }
    return childTasks;
  }

  private void logMrWarning(int mrJobs) {
    if (mrJobs <= 0 || !("mr".equals(HiveConf.getVar(driverContext.getConf(), ConfVars.HIVE_EXECUTION_ENGINE)))) {
      return;
    }
    String warning = HiveConf.generateMrDeprecationWarning();
    LOG.warn(warning);
  }

  private String getErrorMsgAndDetail(int exitVal, Throwable downstreamError, Task tsk) {
    String errorMessage = "FAILED: Execution Error, return code " + exitVal + " from " + tsk.getClass().getName();
    if (downstreamError != null) {
      //here we assume that upstream code may have parametrized the msg from ErrorMsg
      //so we want to keep it
      if (downstreamError.getMessage() != null) {
        errorMessage += ". " + downstreamError.getMessage();
      } else {
        errorMessage += ". " + StringUtils.stringifyException(downstreamError);
      }
    }
    else {
      ErrorMsg em = ErrorMsg.getErrorMsg(exitVal);
      if (em != null) {
        errorMessage += ". " +  em.getMsg();
      }
    }

    return errorMessage;
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
   * @param taskQueue
   *          the task queue
   */
  private TaskRunner launchTask(Task<?> tsk, String queryId, boolean noName,
      String jobname, int jobs, TaskQueue taskQueue) throws HiveException {
    if (SessionState.get() != null) {
      SessionState.get().getHiveHistory().startTask(queryId, tsk, tsk.getClass().getName());
    }
    if (tsk.isMapRedTask() && !(tsk instanceof ConditionalTask)) {
      if (noName) {
        driverContext.getConf().set(MRJobConfig.JOB_NAME, jobname + " (" + tsk.getId() + ")");
      }
      driverContext.getConf().set(DagUtils.MAPREDUCE_WORKFLOW_NODE_NAME, tsk.getId());
      Utilities.setWorkflowAdjacencies(driverContext.getConf(), driverContext.getPlan());
      taskQueue.incCurJobNo(1);
      CONSOLE.printInfo("Launching Job " + taskQueue.getCurJobNo() + " out of " + jobs);
    }
    tsk.initialize(driverContext.getQueryState(), driverContext.getPlan(), taskQueue, context);
    TaskRunner tskRun = new TaskRunner(tsk, taskQueue);

    taskQueue.launching(tskRun);
    // Launch Task
    if (HiveConf.getBoolVar(tsk.getConf(), HiveConf.ConfVars.EXECPARALLEL) && tsk.canExecuteInParallel()) {
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

    if (resStream == null) {
      resStream = context.getStream();
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
        CONSOLE.printError("FAILED: Unexpected IO exception : " + e.getMessage());
        return false;
      }

      if (ss == Utilities.StreamStatus.EOF) {
        resStream = context.getStream();
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
      resStream = null;
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

  @Override
  public QueryDisplay getQueryDisplay() {
    return driverContext.getQueryDisplay();
  }

  /**
   * Set the HS2 operation handle's guid string
   * @param opId base64 encoded guid string
   */
  @Override
  public void setOperationId(String opId) {
    this.operationId = opId;
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
