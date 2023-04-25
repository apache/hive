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

package org.apache.hive.service.cli.operation;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.io.SessionStream;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.common.MetricsScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.QueryDisplay;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.server.ThreadWithGarbageCleanup;

import static org.apache.hadoop.hive.shims.HadoopShims.USER_ID;

/**
 * SQLOperation.
 */
public class SQLOperation extends ExecuteStatementOperation {
  private IDriver driver = null;
  private Optional<TableSchema> resultSchema;
  private AbstractSerDe serde = null;
  private boolean fetchStarted = false;
  private volatile MetricsScope currentSQLStateScope;
  private final QueryInfo queryInfo;
  private final long queryTimeout;
  private ScheduledExecutorService timeoutExecutor;
  private final boolean runAsync;
  private final long operationLogCleanupDelayMs;
  private final ArrayList<Object> convey = new ArrayList<>();

  /**
   * A map to track query count running by each user
   */
  private static final Map<String, AtomicInteger> USER_QUERIES = new ConcurrentHashMap<>();
  private static final String ACTIVE_SQL_USER = MetricsConstant.SQL_OPERATION_PREFIX + "active_user";
  private final Optional<MetricsScope> submittedQryScp;

  public SQLOperation(HiveSession parentSession, String statement, Map<String, String> confOverlay,
                      boolean runInBackground, long queryTimeout) {
    this(parentSession, statement, confOverlay, runInBackground, queryTimeout, false);
  }

  public SQLOperation(HiveSession parentSession, String statement, Map<String, String> confOverlay,
      boolean runInBackground, long queryTimeout, boolean embedded) {
    // TODO: call setRemoteUser in ExecuteStatementOperation or higher.
    super(parentSession, statement, confOverlay, runInBackground, embedded);
    this.runAsync = runInBackground;
    this.resultSchema = Optional.empty();

    final long timeout =
        HiveConf.getTimeVar(queryState.getConf(), HiveConf.ConfVars.HIVE_QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    if (timeout > 0 && (queryTimeout <= 0 || timeout < queryTimeout)) {
      this.queryTimeout = timeout;
    } else {
      this.queryTimeout = queryTimeout;
    }

    this.operationLogCleanupDelayMs = HiveConf.getTimeVar(queryState.getConf(),
      HiveConf.ConfVars.HIVE_SERVER2_OPERATION_LOG_CLEANUP_DELAY, TimeUnit.MILLISECONDS);

    setupSessionIO(parentSession.getSessionState());

    queryInfo = new QueryInfo(getState().toString(), getParentSession().getUserName(),
        getExecutionEngine(), getParentSession().getSessionHandle().getHandleIdentifier().toString(),
        getHandle().getHandleIdentifier().toString());

    final Metrics metrics = MetricsFactory.getInstance();
    this.submittedQryScp =
        (metrics == null) ? Optional.empty() : Optional.of(metrics.createScope(MetricsConstant.HS2_SUBMITTED_QURIES));
  }

  @Override
  public boolean shouldRunAsync() {
    return runAsync;
  }

  private void setupSessionIO(SessionState sessionState) {
    try {
      sessionState.in = null; // hive server's session input stream is not used
      sessionState.out =
          new SessionStream(System.out, true, StandardCharsets.UTF_8.name());
      sessionState.info =
          new SessionStream(System.err, true, StandardCharsets.UTF_8.name());
      sessionState.err =
          new SessionStream(System.err, true, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      log.error("Error creating PrintStream", e);
        sessionState.out = null;
        sessionState.info = null;
        sessionState.err = null;
      }
    }

  /**
   * Compile the query and extract metadata
   *
   * @throws HiveSQLException
   */
  private void prepare(QueryState queryState) throws HiveSQLException {
    setState(OperationState.RUNNING);
    try {
      driver = DriverFactory.newDriver(queryState, queryInfo);

      // Start the timer thread for canceling the query when query timeout is reached
      // queryTimeout == 0 means no timeout
      if (queryTimeout > 0L) {
        timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
        timeoutExecutor.schedule(() -> {
          try {
            final String queryId = queryState.getQueryId();
            log.info("Query timed out after: {} seconds. Cancelling the execution now: {}", queryTimeout, queryId);
            SQLOperation.this.cancel(OperationState.TIMEDOUT);
          } catch (HiveSQLException e) {
            log.error("Error cancelling the query after timeout: {} seconds", queryTimeout, e);
          }
          return null;
        }, queryTimeout, TimeUnit.SECONDS);
      }

      queryInfo.setQueryDisplay(driver.getQueryDisplay());
      if (operationLog != null) {
        queryInfo.setOperationLogLocation(operationLog.toString());
      }

      // set the operation handle information in Driver, so that thrift API users
      // can use the operation handle they receive, to lookup query information in
      // Yarn ATS, also used in logging so remove padding for better display
      String guid64 = Base64.getUrlEncoder().withoutPadding()
          .encodeToString(getHandle().getHandleIdentifier().toTHandleIdentifier().getGuid());
      driver.setOperationId(guid64);

      // In Hive server mode, we are not able to retry in the FetchTask
      // case, when calling fetch queries since execute() has returned.
      // For now, we disable the test attempts.
      driver.compileAndRespond(statement);
      if (queryState.getQueryTag() != null && queryState.getQueryId() != null) {
        parentSession.updateQueryTag(queryState.getQueryId(), queryState.getQueryTag());
      }
      setHasResultSet(driver.hasResultSet());
    } catch (CommandProcessorException e) {
      setState(OperationState.ERROR);
      throw toSQLException("Error while compiling statement", e);
    } catch (Throwable e) {
      setState(OperationState.ERROR);
      if (e instanceof OutOfMemoryError) {
        throw e;
      }
      throw new HiveSQLException("Error running query", e);
    }
  }

  private void runQuery() throws HiveSQLException {
    try {
      OperationState opState = getState();
      // Operation may have been cancelled by another thread
      if (opState.isTerminal()) {
        log.info("Not running the query. Operation is already in terminal state: " + opState
            + ", perhaps cancelled due to query timeout or by another thread.");
        return;
      }
      // In Hive server mode, we are not able to retry in the FetchTask
      // case, when calling fetch queries since execute() has returned.
      // For now, we disable the test attempts.
      driver.run();
    } catch (Throwable e) {
      /**
       * If the operation was cancelled by another thread, or the execution timed out, Driver#run
       * may return a non-zero response code. We will simply return if the operation state is
       * CANCELED, TIMEDOUT, CLOSED or FINISHED, otherwise throw an exception
       */
      if (getState().isTerminal()) {
        log.warn("Ignore exception in terminal state: {}", getState(), e);
        return;
      }
      setState(OperationState.ERROR);
      if (e instanceof CommandProcessorException) {
        throw toSQLException("Error while compiling statement", (CommandProcessorException)e);
      } else if (e instanceof HiveSQLException) {
        throw (HiveSQLException) e;
      } else if (e instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) e;
      } else {
        throw new HiveSQLException("Error running query", e);
      }
    }
    setState(OperationState.FINISHED);
  }

  @Override
  public void runInternal() throws HiveSQLException {
    setState(OperationState.PENDING);

    final boolean doRunAsync = shouldRunAsync();
    final boolean asyncPrepare = doRunAsync
      && HiveConf.getBoolVar(queryState.getConf(),
        HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_ASYNC_COMPILE);
    if (!asyncPrepare) {
      prepare(queryState);
    }
    if (!doRunAsync) {
      runQuery();
    } else {
      // We'll pass ThreadLocals in the background thread from the foreground (handler) thread.
      // 1) ThreadLocal Hive object needs to be set in background thread
      // 2) The metastore client in Hive is associated with right user.
      // 3) Current UGI will get used by metastore when metastore is in embedded mode
      Runnable work =
          new BackgroundWork(getCurrentUGI(), parentSession.getSessionHive(), SessionState.get(),
              asyncPrepare);

      try {
        // This submit blocks if no background threads are available to run this operation
        Future<?> backgroundHandle = getParentSession().submitBackgroundOperation(work);
        setBackgroundHandle(backgroundHandle);
      } catch (RejectedExecutionException rejected) {
        setState(OperationState.ERROR);
        throw new HiveSQLException("The background threadpool cannot accept" +
            " new task for execution, please retry the operation", rejected);
      }
    }
  }


  private final class BackgroundWork implements Runnable {
    private final UserGroupInformation currentUGI;
    private final Hive parentHive;
    private final SessionState parentSessionState;
    private final boolean asyncPrepare;

    private BackgroundWork(UserGroupInformation currentUGI,
        Hive parentHive,
        SessionState parentSessionState, boolean asyncPrepare) {
      // Note: parentHive can be shared by multiple threads and so it should be protected from any
      // thread closing metastore connections when some other thread still accessing it. So, it is
      // expected that allowClose flag in parentHive is set to false by caller and it will be caller's
      // responsibility to close it explicitly with forceClose flag as true.
      // Shall refer to sessionHive in HiveSessionImpl.java for the usage.
      this.currentUGI = currentUGI;
      this.parentHive = parentHive;
      this.parentSessionState = parentSessionState;
      this.asyncPrepare = asyncPrepare;
    }

    @Override
    public void run() {
      PrivilegedExceptionAction<Object> doAsAction = new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws HiveSQLException {
          assert (!parentHive.allowClose());
          Hive.set(parentHive);
          // TODO: can this result in cross-thread reuse of session state?
          SessionState.setCurrentSessionState(parentSessionState);
          PerfLogger.setPerfLogger(SessionState.getPerfLogger());
          if (!embedded) {
            LogUtils.registerLoggingContext(queryState.getConf());
          }
          ShimLoader.getHadoopShims()
              .setHadoopQueryContext(String.format(USER_ID, queryState.getQueryId(), parentSessionState.getUserName()));

          try {
            if (asyncPrepare) {
              prepare(queryState);
            }
            runQuery();
          } catch (HiveSQLException e) {
            // TODO: why do we invent our own error path op top of the one from Future.get?
            setOperationException(e);
            log.error("Error running hive query", e);
          } finally {
            if (!embedded) {
              LogUtils.unregisterLoggingContext();
            }

            // If new hive object is created  by the child thread, then we need to close it as it might
            // have created a hms connection. Call Hive.closeCurrent() that closes the HMS connection, causes
            // HMS connection leaks otherwise.
            Hive.closeCurrent();
          }
          return null;
        }
      };

      try {
        currentUGI.doAs(doAsAction);
      } catch (Exception e) {
        setOperationException(new HiveSQLException(e));
        log.error("Error running hive query as user : {}", currentUGI.getShortUserName(), e);
      } finally {
        /**
         * We'll cache the ThreadLocal RawStore object for this background thread for an orderly cleanup
         * when this thread is garbage collected later.
         * @see org.apache.hive.service.server.ThreadWithGarbageCleanup#finalize()
         */
        if (ThreadWithGarbageCleanup.currentThread() instanceof ThreadWithGarbageCleanup) {
          ThreadWithGarbageCleanup currentThread =
              (ThreadWithGarbageCleanup) ThreadWithGarbageCleanup.currentThread();
          currentThread.cacheThreadLocalRawStore();
        }
      }
    }
  }

  /**
   * Returns the current UGI on the stack
   *
   * @return UserGroupInformation
   *
   * @throws HiveSQLException
   */
  private UserGroupInformation getCurrentUGI() throws HiveSQLException {
    try {
      return Utils.getUGI();
    } catch (Exception e) {
      throw new HiveSQLException("Unable to get current user", e);
    }
  }

  private synchronized void cleanup(OperationState state) throws HiveSQLException {
    setState(state);

    //Need shut down background thread gracefully, driver.close will inform background thread
    //a cancel request is sent.
    if (shouldRunAsync() && state != OperationState.CANCELED && state != OperationState.TIMEDOUT) {
      Future<?> backgroundHandle = getBackgroundHandle();
      if (backgroundHandle != null) {
        boolean success = backgroundHandle.cancel(true);
        String queryId = queryState.getQueryId();
        if (success) {
          log.info("The running operation has been successfully interrupted: {}", queryId);
        } else if (log.isDebugEnabled()) {
          log.debug("The running operation could not be cancelled, typically because it has already completed normally: {}", queryId);
        }
      }
    }

    if (driver != null) {
      driver.close();
      driver.destroy();
    }
    driver = null;

    SessionState ss = SessionState.get();
    if (ss == null) {
      log.warn("Operation seems to be in invalid state, SessionState is null");
    } else {
      ss.deleteTmpOutputFile();
      ss.deleteTmpErrOutputFile();
    }

    // Shutdown the timeout thread if any, while closing this operation
    if ((timeoutExecutor != null) && (state != OperationState.TIMEDOUT) && (state.isTerminal())) {
      timeoutExecutor.shutdownNow();
    }
  }

  @Override
  public void cancel(OperationState stateAfterCancel) throws HiveSQLException {
    String queryId = null;
    if (stateAfterCancel == OperationState.CANCELED) {
      queryId = queryState.getQueryId();
      log.info("Cancelling the query execution: {}", queryId);
    }
    cleanup(stateAfterCancel);
    cleanupOperationLog(operationLogCleanupDelayMs);
    if (stateAfterCancel == OperationState.CANCELED) {
      log.info("Successfully cancelled the query: {}", queryId);
    }
  }

  @Override
  public void close() throws HiveSQLException {
    if (!embedded) {
      cleanup(OperationState.CLOSED);
      cleanupOperationLog(0);
    }
  }

  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    // Since compilation is always a blocking RPC call, and schema is ready after compilation,
    // we can return when are in the RUNNING state.
    assertState(Arrays.asList(OperationState.RUNNING, OperationState.FINISHED));
    if (!resultSchema.isPresent()) {
      resultSchema = Optional.of(new TableSchema(driver.getSchema()));
    }
    return resultSchema.get();
  }

  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows)
    throws HiveSQLException {

    validateDefaultFetchOrientation(orientation);
    assertState(Collections.singleton(OperationState.FINISHED));

    FetchTask fetchTask = driver.getFetchTask();
    boolean isBlobBased = false;

    if (fetchTask != null && fetchTask.getWork().isUsingThriftJDBCBinarySerDe()) {
      // Just fetch one blob if we've serialized thrift objects in final tasks
      maxRows = 1;
      isBlobBased = true;
    }
    RowSet rowSet = RowSetFactory.create(getResultSetSchema(), getProtocolVersion(), isBlobBased);
    try {
      /* if client is requesting fetch-from-start and its not the first time reading from this operation
       * then reset the fetch position to beginning
       */
      if (orientation.equals(FetchOrientation.FETCH_FIRST) && fetchStarted) {
        driver.resetFetch();
      }
      fetchStarted = true;

      final int capacity = Math.toIntExact(maxRows);
      convey.ensureCapacity(capacity);
      driver.setMaxRows(capacity);
      if (driver.getResults(convey)) {
        if (convey.size() == capacity) {
          log.info("Result set buffer filled to capacity [{}]", capacity);
        }
        return decode(convey, rowSet);
      }
      return rowSet;
    } catch (Exception e) {
      throw new HiveSQLException("Unable to get the next row set with exception: " + e.getMessage(), e);
    } finally {
      convey.clear();
    }
  }

  @Override
  public String getTaskStatus() throws HiveSQLException {
    if (driver != null) {
      List<QueryDisplay.TaskDisplay> statuses = driver.getQueryDisplay().getTaskDisplays();
      if (statuses != null) {
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
          QueryDisplay.OBJECT_MAPPER.writeValue(out, statuses);
          return out.toString(StandardCharsets.UTF_8.name());
        } catch (Exception e) {
          throw new HiveSQLException(e);
        }
      }
    }
    // Driver not initialized
    return null;
  }

  private RowSet decode(final List<Object> rows, final RowSet rowSet) throws Exception {
    return (driver.isFetchingTable()) ? prepareFromRow(rows, rowSet) : decodeFromString(rows, rowSet);
  }

  // already encoded to thrift-able object in ThriftFormatter
  private RowSet prepareFromRow(final List<Object> rows, final RowSet rowSet) throws Exception {
    rows.forEach(row -> rowSet.addRow((Object[]) row));
    return rowSet;
  }

  private RowSet decodeFromString(List<Object> rows, RowSet rowSet)
      throws SQLException, SerDeException {
    getSerDe();
    StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();
    List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();

    Object[] deserializedFields = new Object[fieldRefs.size()];
    ObjectInspector fieldOI;

    int protocol = getProtocolVersion().getValue();
    for (Object rowString : rows) {
      final Object rowObj = serde.deserialize(new BytesWritable(
          ((String) rowString).getBytes(StandardCharsets.UTF_8)));
      for (int i = 0; i < fieldRefs.size(); i++) {
        StructField fieldRef = fieldRefs.get(i);
        fieldOI = fieldRef.getFieldObjectInspector();
        Object fieldData = soi.getStructFieldData(rowObj, fieldRef);
        deserializedFields[i] = SerDeUtils.toThriftPayload(fieldData, fieldOI, protocol);
      }
      rowSet.addRow(deserializedFields);
    }
    return rowSet;
  }

  private AbstractSerDe getSerDe() throws SQLException {
    if (serde == null) {
      try {
        this.serde = new LazySimpleSerDe();

        final Schema mResultSchema = driver.getSchema();
        final List<FieldSchema> fieldSchemas = mResultSchema.getFieldSchemas();
        final Properties props = new Properties();

        if (!fieldSchemas.isEmpty()) {

          final String names = fieldSchemas.stream().map(i -> i.getName()).collect(Collectors.joining(","));
          final String types = fieldSchemas.stream().map(i -> i.getType()).collect(Collectors.joining(","));

          log.debug("Column names: {}", names);
          log.debug("Column types: {}", types);

          props.setProperty(serdeConstants.LIST_COLUMNS, names);
          props.setProperty(serdeConstants.LIST_COLUMN_TYPES, types);
        }

        serde.initialize(queryState.getConf(), props, null);
      } catch (Exception ex) {
        throw new SQLException("Could not create ResultSet: " + ex.getMessage(), ex);
      }
    }
    return serde;
  }

  /**
   * Get summary information of this SQLOperation for display in WebUI.
   */
  public QueryInfo getQueryInfo() {
    return queryInfo;
  }

  @Override
  protected void onNewState(final OperationState state, final OperationState prevState) {
    super.onNewState(state, prevState);

    currentSQLStateScope = updateOperationStateMetrics(currentSQLStateScope,
        MetricsConstant.SQL_OPERATION_PREFIX,
        MetricsConstant.COMPLETED_SQL_OPERATION_PREFIX, state);

    final Optional<Metrics> metrics = Optional.ofNullable(MetricsFactory.getInstance());
    if (metrics.isPresent()) {
      // New state is changed to running from something else (user is active)
      if (state == OperationState.RUNNING && prevState != state) {
        incrementUserQueries(metrics.get());
      }
      // New state is not running (user not active) any more
      if (prevState == OperationState.RUNNING && prevState != state) {
        decrementUserQueries(metrics.get());
      }
    }

    switch (state) {
    case CANCELED:
      queryInfo.setRuntime(getOperationComplete() - getOperationStart());
      if (metrics.isPresent() && submittedQryScp.isPresent()) {
        metrics.get().endScope(submittedQryScp.get());
      }
      queryInfo.updateState(state.toString());
      break;
    case CLOSED:
      queryInfo.setEndTime();
      break;
    case ERROR:
      queryInfo.setRuntime(getOperationComplete() - getOperationStart());
      if (metrics.isPresent() && submittedQryScp.isPresent()) {
        metrics.get().endScope(submittedQryScp.get());
      }
      markQueryMetric(MetricsFactory.getInstance(), MetricsConstant.HS2_FAILED_QUERIES);
      queryInfo.updateState(state.toString());
      break;
    case FINISHED:
      queryInfo.setRuntime(getOperationComplete() - getOperationStart());
      if (metrics.isPresent() && submittedQryScp.isPresent()) {
        metrics.get().endScope(submittedQryScp.get());
      }
      markQueryMetric(MetricsFactory.getInstance(), MetricsConstant.HS2_SUCCEEDED_QUERIES);
      queryInfo.updateState(state.toString());
      break;
    case INITIALIZED:
      /* fall through */
    case PENDING:
      /* fall through */
    case RUNNING:
      /* fall through */
    case TIMEDOUT:
      /* fall through */
    case UNKNOWN:
      /* fall through */
    default:
      queryInfo.updateState(state.toString());
      break;
    }
  }

  private void incrementUserQueries(final Metrics metrics) {
    final String username = parentSession.getUserName();
    if (StringUtils.isNotBlank(username)) {
      USER_QUERIES.compute(username, (key, value) -> {
        if (value == null) {
          metrics.incrementCounter(ACTIVE_SQL_USER);
          return new AtomicInteger(1);
        } else {
          value.incrementAndGet();
          return value;
        }
      });
    }
  }

  private void decrementUserQueries(final Metrics metrics) {
    final String username = parentSession.getUserName();
    if (StringUtils.isNotBlank(username)) {
      USER_QUERIES.compute(username, (key, value) -> {
        if (value == null) {
          return null;
        } else {
          final int newValue = value.decrementAndGet();
          if (newValue == 0) {
            metrics.decrementCounter(ACTIVE_SQL_USER);
            return null;
          }
          return value;
        }
      });
    }
  }

  private void markQueryMetric(Metrics metric, String name) {
    if (metric != null) {
      metric.markMeter(name);
    }
  }

  public String getExecutionEngine() {
    return queryState.getConf().getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
  }

}
