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
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.CharEncoding;
import org.apache.hadoop.hive.common.LogUtils;
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
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
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
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * SQLOperation.
 *
 */
public class SQLOperation extends ExecuteStatementOperation {
  private IDriver driver = null;
  private CommandProcessorResponse response;
  private TableSchema resultSchema;
  private AbstractSerDe serde = null;
  private boolean fetchStarted = false;
  private volatile MetricsScope currentSQLStateScope;
  private QueryInfo queryInfo;
  private long queryTimeout;
  private ScheduledExecutorService timeoutExecutor;
  private final boolean runAsync;
  private final long operationLogCleanupDelayMs;

  /**
   * A map to track query count running by each user
   */
  private static Map<String, AtomicInteger> userQueries = new HashMap<String, AtomicInteger>();
  private static final String ACTIVE_SQL_USER = MetricsConstant.SQL_OPERATION_PREFIX + "active_user";
  private MetricsScope submittedQryScp;

  public SQLOperation(HiveSession parentSession, String statement, Map<String, String> confOverlay,
      boolean runInBackground, long queryTimeout) {
    // TODO: call setRemoteUser in ExecuteStatementOperation or higher.
    super(parentSession, statement, confOverlay, runInBackground);
    this.runAsync = runInBackground;
    this.queryTimeout = queryTimeout;
    long timeout = HiveConf.getTimeVar(queryState.getConf(),
        HiveConf.ConfVars.HIVE_QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    if (timeout > 0 && (queryTimeout <= 0 || timeout < queryTimeout)) {
      this.queryTimeout = timeout;
    }
    this.operationLogCleanupDelayMs = HiveConf.getTimeVar(queryState.getConf(),
      HiveConf.ConfVars.HIVE_SERVER2_OPERATION_LOG_CLEANUP_DELAY, TimeUnit.MILLISECONDS);

    setupSessionIO(parentSession.getSessionState());

    queryInfo = new QueryInfo(getState().toString(), getParentSession().getUserName(),
            getExecutionEngine(), getHandle().getHandleIdentifier().toString());

    Metrics metrics = MetricsFactory.getInstance();
    if (metrics != null) {
      submittedQryScp = metrics.createScope(MetricsConstant.HS2_SUBMITTED_QURIES);
    }
  }

  @Override
  public boolean shouldRunAsync() {
    return runAsync;
  }

  private void setupSessionIO(SessionState sessionState) {
    try {
      sessionState.in = null; // hive server's session input stream is not used
      sessionState.out = new PrintStream(System.out, true, CharEncoding.UTF_8);
      sessionState.info = new PrintStream(System.err, true, CharEncoding.UTF_8);
      sessionState.err = new PrintStream(System.err, true, CharEncoding.UTF_8);
    } catch (UnsupportedEncodingException e) {
        LOG.error("Error creating PrintStream", e);
        e.printStackTrace();
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
  public void prepare(QueryState queryState) throws HiveSQLException {
    setState(OperationState.RUNNING);
    try {
      driver = DriverFactory.newDriver(queryState, getParentSession().getUserName(), queryInfo);

      // Start the timer thread for cancelling the query when query timeout is reached
      // queryTimeout == 0 means no timeout
      if (queryTimeout > 0) {
        timeoutExecutor = new ScheduledThreadPoolExecutor(1);
        Runnable timeoutTask = new Runnable() {
          @Override
          public void run() {
            try {
              String queryId = queryState.getQueryId();
              LOG.info("Query timed out after: " + queryTimeout
                  + " seconds. Cancelling the execution now: " + queryId);
              SQLOperation.this.cancel(OperationState.TIMEDOUT);
            } catch (HiveSQLException e) {
              LOG.error("Error cancelling the query after timeout: " + queryTimeout + " seconds", e);
            } finally {
              // Stop
              timeoutExecutor.shutdown();
            }
          }
        };
        timeoutExecutor.schedule(timeoutTask, queryTimeout, TimeUnit.SECONDS);
      }

      queryInfo.setQueryDisplay(driver.getQueryDisplay());

      // set the operation handle information in Driver, so that thrift API users
      // can use the operation handle they receive, to lookup query information in
      // Yarn ATS
      String guid64 = Base64.encodeBase64URLSafeString(getHandle().getHandleIdentifier()
          .toTHandleIdentifier().getGuid()).trim();
      driver.setOperationId(guid64);

      // In Hive server mode, we are not able to retry in the FetchTask
      // case, when calling fetch queries since execute() has returned.
      // For now, we disable the test attempts.
      response = driver.compileAndRespond(statement);
      if (0 != response.getResponseCode()) {
        throw toSQLException("Error while compiling statement", response);
      }

      setHasResultSet(driver.hasResultSet());
    } catch (HiveSQLException e) {
      setState(OperationState.ERROR);
      throw e;
    } catch (Throwable e) {
      setState(OperationState.ERROR);
      throw new HiveSQLException("Error running query: " + e.toString(), e);
    }
  }

  private void runQuery() throws HiveSQLException {
    try {
      OperationState opState = getStatus().getState();
      // Operation may have been cancelled by another thread
      if (opState.isTerminal()) {
        LOG.info("Not running the query. Operation is already in terminal state: " + opState
            + ", perhaps cancelled due to query timeout or by another thread.");
        return;
      }
      // In Hive server mode, we are not able to retry in the FetchTask
      // case, when calling fetch queries since execute() has returned.
      // For now, we disable the test attempts.
      response = driver.run();
      if (0 != response.getResponseCode()) {
        throw toSQLException("Error while processing statement", response);
      }
    } catch (Throwable e) {
      /**
       * If the operation was cancelled by another thread, or the execution timed out, Driver#run
       * may return a non-zero response code. We will simply return if the operation state is
       * CANCELED, TIMEDOUT, CLOSED or FINISHED, otherwise throw an exception
       */
      if ((getStatus().getState() == OperationState.CANCELED)
          || (getStatus().getState() == OperationState.TIMEDOUT)
          || (getStatus().getState() == OperationState.CLOSED)
          || (getStatus().getState() == OperationState.FINISHED)) {
        LOG.warn("Ignore exception in terminal state", e);
        return;
      }
      setState(OperationState.ERROR);
      if (e instanceof HiveSQLException) {
        throw (HiveSQLException) e;
      } else {
        throw new HiveSQLException("Error running query: " + e.toString(), e);
      }
    }
    setState(OperationState.FINISHED);
  }

  @Override
  public void runInternal() throws HiveSQLException {
    setState(OperationState.PENDING);

    boolean runAsync = shouldRunAsync();
    final boolean asyncPrepare = runAsync
      && HiveConf.getBoolVar(queryState.getConf(),
        HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_ASYNC_COMPILE);
    if (!asyncPrepare) {
      prepare(queryState);
    }
    if (!runAsync) {
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
          Hive.set(parentHive);
          // TODO: can this result in cross-thread reuse of session state?
          SessionState.setCurrentSessionState(parentSessionState);
          PerfLogger.setPerfLogger(SessionState.getPerfLogger());
          LogUtils.registerLoggingContext(queryState.getConf());
          try {
            if (asyncPrepare) {
              prepare(queryState);
            }
            runQuery();
          } catch (HiveSQLException e) {
            // TODO: why do we invent our own error path op top of the one from Future.get?
            setOperationException(e);
            LOG.error("Error running hive query: ", e);
          } finally {
            LogUtils.unregisterLoggingContext();
          }
          return null;
        }
      };

      try {
        currentUGI.doAs(doAsAction);
      } catch (Exception e) {
        setOperationException(new HiveSQLException(e));
        LOG.error("Error running hive query as user : " + currentUGI.getShortUserName(), e);
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
          LOG.info("The running operation has been successfully interrupted: " + queryId);
        } else if (state == OperationState.CANCELED) {
          LOG.info("The running operation could not be cancelled, typically because it has already completed normally: " + queryId);
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
      LOG.warn("Operation seems to be in invalid state, SessionState is null");
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
      LOG.info("Cancelling the query execution: " + queryId);
    }
    cleanup(stateAfterCancel);
    cleanupOperationLog(operationLogCleanupDelayMs);
    if (stateAfterCancel == OperationState.CANCELED) {
      LOG.info("Successfully cancelled the query: " + queryId);
    }
  }

  @Override
  public void close() throws HiveSQLException {
    cleanup(OperationState.CLOSED);
    cleanupOperationLog(0);
  }

  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    // Since compilation is always a blocking RPC call, and schema is ready after compilation,
    // we can return when are in the RUNNING state.
    assertState(Arrays.asList(OperationState.RUNNING, OperationState.FINISHED));
    if (resultSchema == null) {
      resultSchema = new TableSchema(driver.getSchema());
    }
    return resultSchema;
  }

  private transient final List<Object> convey = new ArrayList<Object>();

  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows)
    throws HiveSQLException {

    validateDefaultFetchOrientation(orientation);
    assertState(new ArrayList<OperationState>(Arrays.asList(OperationState.FINISHED)));

    FetchTask fetchTask = driver.getFetchTask();
    boolean isBlobBased = false;

    if (fetchTask != null && fetchTask.getWork().isUsingThriftJDBCBinarySerDe()) {
      // Just fetch one blob if we've serialized thrift objects in final tasks
      maxRows = 1;
      isBlobBased = true;
    }
    driver.setMaxRows((int) maxRows);
    RowSet rowSet = RowSetFactory.create(getResultSetSchema(), getProtocolVersion(), isBlobBased);
    try {
      /* if client is requesting fetch-from-start and its not the first time reading from this operation
       * then reset the fetch position to beginning
       */
      if (orientation.equals(FetchOrientation.FETCH_FIRST) && fetchStarted) {
        driver.resetFetch();
      }
      fetchStarted = true;
      driver.setMaxRows((int) maxRows);
      if (driver.getResults(convey)) {
        return decode(convey, rowSet);
      }
      return rowSet;
    } catch (IOException e) {
      throw new HiveSQLException(e);
    } catch (Exception e) {
      throw new HiveSQLException(e);
    } finally {
      convey.clear();
    }
  }

  @Override
  public String getTaskStatus() throws HiveSQLException {
    if (driver != null) {
      List<QueryDisplay.TaskDisplay> statuses = driver.getQueryDisplay().getTaskDisplays();
      if (statuses != null) {
        ByteArrayOutputStream out = null;
        try {
          ObjectMapper mapper = new ObjectMapper();
          out = new ByteArrayOutputStream();
          mapper.writeValue(out, statuses);
          return out.toString("UTF-8");
        } catch (JsonGenerationException e) {
          throw new HiveSQLException(e);
        } catch (JsonMappingException e) {
          throw new HiveSQLException(e);
        } catch (IOException e) {
          throw new HiveSQLException(e);
        } finally {
          if (out != null) {
            try {
              out.close();
            } catch (IOException e) {
              throw new HiveSQLException(e);
            }
          }
        }
      }
    }
    // Driver not initialized
    return null;
  }

  private RowSet decode(List<Object> rows, RowSet rowSet) throws Exception {
    if (driver.isFetchingTable()) {
      return prepareFromRow(rows, rowSet);
    }
    return decodeFromString(rows, rowSet);
  }

  // already encoded to thrift-able object in ThriftFormatter
  private RowSet prepareFromRow(List<Object> rows, RowSet rowSet) throws Exception {
    for (Object row : rows) {
      rowSet.addRow((Object[]) row);
    }
    return rowSet;
  }

  private RowSet decodeFromString(List<Object> rows, RowSet rowSet)
      throws SQLException, SerDeException {
    getSerDe();
    StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();
    List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();

    Object[] deserializedFields = new Object[fieldRefs.size()];
    Object rowObj;
    ObjectInspector fieldOI;

    int protocol = getProtocolVersion().getValue();
    for (Object rowString : rows) {
      try {
        rowObj = serde.deserialize(new BytesWritable(((String)rowString).getBytes("UTF-8")));
      } catch (UnsupportedEncodingException e) {
        throw new SerDeException(e);
      }
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
    if (serde != null) {
      return serde;
    }
    try {
      Schema mResultSchema = driver.getSchema();

      List<FieldSchema> fieldSchemas = mResultSchema.getFieldSchemas();
      StringBuilder namesSb = new StringBuilder();
      StringBuilder typesSb = new StringBuilder();

      if (fieldSchemas != null && !fieldSchemas.isEmpty()) {
        for (int pos = 0; pos < fieldSchemas.size(); pos++) {
          if (pos != 0) {
            namesSb.append(",");
            typesSb.append(",");
          }
          namesSb.append(fieldSchemas.get(pos).getName());
          typesSb.append(fieldSchemas.get(pos).getType());
        }
      }
      String names = namesSb.toString();
      String types = typesSb.toString();

      serde = new LazySimpleSerDe();
      Properties props = new Properties();
      if (names.length() > 0) {
        LOG.debug("Column names: " + names);
        props.setProperty(serdeConstants.LIST_COLUMNS, names);
      }
      if (types.length() > 0) {
        LOG.debug("Column types: " + types);
        props.setProperty(serdeConstants.LIST_COLUMN_TYPES, types);
      }
      SerDeUtils.initializeSerDe(serde, queryState.getConf(), props, null);

    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Could not create ResultSet: " + ex.getMessage(), ex);
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
  protected void onNewState(OperationState state, OperationState prevState) {

    super.onNewState(state, prevState);
    currentSQLStateScope = updateOperationStateMetrics(currentSQLStateScope,
        MetricsConstant.SQL_OPERATION_PREFIX,
        MetricsConstant.COMPLETED_SQL_OPERATION_PREFIX, state);

    Metrics metrics = MetricsFactory.getInstance();
    if (metrics != null) {
      // New state is changed to running from something else (user is active)
      if (state == OperationState.RUNNING && prevState != state) {
        incrementUserQueries(metrics);
      }
      // New state is not running (user not active) any more
      if (prevState == OperationState.RUNNING && prevState != state) {
        decrementUserQueries(metrics);
      }
    }

    if (state == OperationState.FINISHED || state == OperationState.CANCELED || state == OperationState.ERROR) {
      //update runtime
      queryInfo.setRuntime(getOperationComplete() - getOperationStart());
      if (metrics != null && submittedQryScp != null) {
        metrics.endScope(submittedQryScp);
      }
    }

    if (state == OperationState.CLOSED) {
      queryInfo.setEndTime();
    } else {
      //CLOSED state not interesting, state before (FINISHED, ERROR) is.
      queryInfo.updateState(state.toString());
    }

    if (state == OperationState.ERROR) {
      markQueryMetric(MetricsFactory.getInstance(), MetricsConstant.HS2_FAILED_QUERIES);
    }
    if (state == OperationState.FINISHED) {
      markQueryMetric(MetricsFactory.getInstance(), MetricsConstant.HS2_SUCCEEDED_QUERIES);
    }
  }

  private void incrementUserQueries(Metrics metrics) {
    String username = parentSession.getUserName();
    if (username != null) {
      synchronized (userQueries) {
        AtomicInteger count = userQueries.get(username);
        if (count == null) {
          count = new AtomicInteger(0);
          AtomicInteger prev = userQueries.put(username, count);
          if (prev == null) {
            metrics.incrementCounter(ACTIVE_SQL_USER);
          } else {
            count = prev;
          }
        }
        count.incrementAndGet();
      }
    }
  }

  private void decrementUserQueries(Metrics metrics) {
    String username = parentSession.getUserName();
    if (username != null) {
      synchronized (userQueries) {
        AtomicInteger count = userQueries.get(username);
        if (count != null && count.decrementAndGet() <= 0) {
          metrics.decrementCounter(ACTIVE_SQL_USER);
          userQueries.remove(username);
        }
      }
    }
  }

  private void markQueryMetric(Metrics metric, String name) {
    if(metric != null) {
      metric.markMeter(name);
    }
  }

  public String getExecutionEngine() {
    return queryState.getConf().getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
  }

}
