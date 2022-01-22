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

package org.apache.hive.jdbc;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.logs.InPlaceUpdateStream;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCancelOperationReq;
import org.apache.hive.service.rpc.thrift.TCancelOperationResp;
import org.apache.hive.service.rpc.thrift.TCloseOperationReq;
import org.apache.hive.service.rpc.thrift.TCloseOperationResp;
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq;
import org.apache.hive.service.rpc.thrift.TExecuteStatementResp;
import org.apache.hive.service.rpc.thrift.TFetchOrientation;
import org.apache.hive.service.rpc.thrift.TFetchResultsReq;
import org.apache.hive.service.rpc.thrift.TFetchResultsResp;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusReq;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusResp;
import org.apache.hive.service.rpc.thrift.TGetQueryIdReq;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.hadoop.hive.ql.ErrorMsg.CLIENT_POLLING_OPSTATUS_INTERRUPTED;

/**
 * The object used for executing a static SQL statement and returning the
 * results it produces.
 */
public class HiveStatement implements java.sql.Statement {

  private static final Logger LOG = LoggerFactory.getLogger(HiveStatement.class);

  public static final String QUERY_CANCELLED_MESSAGE = "Query was cancelled.";
  private static final int DEFAULT_FETCH_SIZE =
      HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE.defaultIntVal;

  private final HiveConnection connection;
  private TCLIService.Iface client;
  private Optional<TOperationHandle> stmtHandle;
  private final TSessionHandle sessHandle;
  Map<String, String> sessConf = new HashMap<>();
  private int fetchSize;
  private final int defaultFetchSize;
  private final boolean isScrollableResultset;
  private boolean isOperationComplete = false;
  private boolean closeOnResultSetCompletion = false;
  /**
   * We need to keep a reference to the result set to support the following:
   * <code>
   * statement.execute(String sql);
   * statement.getResultSet();
   * </code>.
   */
  private ResultSet resultSet = null;

  /**
   * Sets the limit for the maximum number of rows that any ResultSet object produced by this
   * Statement can contain to the given number. If the limit is exceeded, the excess rows
   * are silently dropped. The value must be >= 0, and 0 means there is not limit.
   */
  private int maxRows = 0;

  /**
   * Add SQLWarnings to the warningChain if needed.
   */
  private SQLWarning warningChain = null;

  /**
   * Keep state so we can fail certain calls made after close().
   */
  private boolean isClosed = false;

  /**
   * Keep state so we can fail certain calls made after cancel().
   */
  private boolean isCancelled = false;

  /**
   * Keep this state so we can know whether the query in this statement is closed.
   */
  private boolean isQueryClosed = false;

  /**
   * Keep this state so we can know whether the query logs are being generated in HS2.
   */
  private boolean isLogBeingGenerated = true;

  private int queryTimeout = 0;

  private Optional<InPlaceUpdateStream> inPlaceUpdateStream;

  public HiveStatement(HiveConnection connection, TCLIService.Iface client,
      TSessionHandle sessHandle) {
    this(connection, client, sessHandle, false, 0, DEFAULT_FETCH_SIZE);
  }

  public HiveStatement(HiveConnection connection, TCLIService.Iface client, TSessionHandle sessHandle,
      boolean isScrollableResultset, int initFetchSize, int defaultFetchSize) {
    this.connection = Objects.requireNonNull(connection);
    this.client = Objects.requireNonNull(client);
    this.sessHandle = Objects.requireNonNull(sessHandle);

    if (initFetchSize < 0 || defaultFetchSize <= 0) {
      throw new IllegalArgumentException();
    }

    this.isScrollableResultset = isScrollableResultset;
    this.defaultFetchSize = defaultFetchSize;
    this.fetchSize = (initFetchSize == 0) ? defaultFetchSize : initFetchSize;
    this.inPlaceUpdateStream = Optional.empty();
    this.stmtHandle = Optional.empty();
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void cancel() throws SQLException {
    checkConnection("cancel");
    if (isCancelled) {
      return;
    }

    try {
      if (stmtHandle.isPresent()) {
        TCancelOperationReq cancelReq = new TCancelOperationReq(stmtHandle.get());
        TCancelOperationResp cancelResp = client.CancelOperation(cancelReq);
        Utils.verifySuccessWithInfo(cancelResp.getStatus());
      }
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException("Failed to cancel statement", "08S01", e);
    }
    isCancelled = true;
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  /**
   * Closes the statement if there is one running. Do not change the the flags.
   * @throws SQLException If there is an error closing the statement
   */
  private void closeStatementIfNeeded() throws SQLException {
    try {
      if (stmtHandle.isPresent()) {
        TCloseOperationReq closeReq = new TCloseOperationReq(stmtHandle.get());
        TCloseOperationResp closeResp = client.CloseOperation(closeReq);
        if (!checkInvalidOperationHandle(closeResp)) {
          Utils.verifySuccessWithInfo(closeResp.getStatus());
        }
      }
    } catch (SQLException e) {
      throw e;
    } catch (TApplicationException tae) {
      String errorMsg = "Failed to close statement";
      if (tae.getType() == TApplicationException.BAD_SEQUENCE_ID) {
        errorMsg = "Failed to close statement. Mismatch thrift sequence id. A previous call to the Thrift library"
            + " failed and now position within the input stream is lost. Please enable verbose error logging and"
            + " check the status of previous calls.";
      }
      throw new SQLException(errorMsg, "08S01", tae);
    } catch (Exception e) {
      throw new SQLException("Failed to close statement", "08S01", e);
    } finally {
      stmtHandle = Optional.empty();
    }
  }

  /**
   * Invalid OperationHandle is a special case in HS2, which sometimes could be considered as safe to ignore.
   * For instance: if the client retried due to HIVE-24786, and the retried operation happened to be the
   * closeOperation, we don't care as the query might have already been removed from HS2's scope.
   * @return true, if the response from server contains "Invalid OperationHandle"
   */
  private boolean checkInvalidOperationHandle(TCloseOperationResp closeResp) {
    List<String> messages = closeResp.getStatus().getInfoMessages();
    if (messages != null && messages.size() > 0) {
      /*
       * Here we need to handle 2 different cases, which can happen in CLIService.closeOperation, which actually does:
       * sessionManager.getOperationManager().getOperation(opHandle).getParentSession().closeOperation(opHandle);
       */
      String message = messages.get(0);
      if (message.contains("Invalid OperationHandle")) {
        /*
         * This happens when the first request properly removes the operation handle, then second request arrives, calls
         * sessionManager.getOperationManager().getOperation(opHandle), and it doesn't find the handle.
         */
        LOG.warn("'Invalid OperationHandle' on server side (messages: " + messages + ")");
        return true;
      } else if (message.contains("Operation does not exist")) {
        /*
         * This is an extremely rare case, which represents a race condition when the first and second request
         * arrives almost at the same time, both can get the OperationHandle instance
         * from sessionManager's OperationManager, but the second fails, because it cannot get it again from the
         * session's OperationManager, because it has been already removed in the meantime.
         */
        LOG.warn("'Operation does not exist' on server side (messages: " + messages + ")");
        return true;
      }
    }

    return false;
  }

  void closeClientOperation() throws SQLException {
    try {
      closeStatementIfNeeded();
    } finally {
      isQueryClosed = true;
    }
  }

  void closeOnResultSetCompletion() throws SQLException {
    if (closeOnResultSetCompletion) {
      resultSet = null;
      close();
    }
  }

  @Override
  public void close() throws SQLException {
    if (isClosed) {
      return;
    }
    closeClientOperation();
    client = null;
    if (resultSet != null) {
      if (!resultSet.isClosed()){
        resultSet.close();
      }
      resultSet = null;
    }
    isClosed = true;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    closeOnResultSetCompletion = true;
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    runAsyncOnServer(sql);
    TGetOperationStatusResp status = waitForOperationToComplete();

    // The query should be completed by now
    if (!status.isHasResultSet() && stmtHandle.isPresent() && !stmtHandle.get().isHasResultSet()) {
      return false;
    }
    resultSet = new HiveQueryResultSet.Builder(this).setClient(client)
        .setStmtHandle(stmtHandle.get()).setMaxRows(maxRows).setFetchSize(fetchSize)
        .setScrollable(isScrollableResultset)
        .build();
    return true;
  }

  /**
   * Starts the query execution asynchronously on the server, and immediately returns to the client.
   * The client subsequently blocks on ResultSet#next or Statement#getUpdateCount, depending on the
   * query type. Users should call ResultSet.next or Statement#getUpdateCount (depending on whether
   * query returns results) to ensure that query completes successfully. Calling another execute*
   * method, or close before query completion would result in the async query getting killed if it
   * is not already finished.
   * Note: This method is an API for limited usage outside of Hive by applications like Apache Ambari,
   * although it is not part of the interface java.sql.Statement.
   *
   * @param sql
   * @return true if the first result is a ResultSet object; false if it is an update count or there
   *         are no results
   * @throws SQLException
   */
  public boolean executeAsync(String sql) throws SQLException {
    runAsyncOnServer(sql);
    TGetOperationStatusResp status = waitForResultSetStatus();
    if (!status.isHasResultSet()) {
      return false;
    }
    resultSet =
        new HiveQueryResultSet.Builder(this).setClient(client)
            .setStmtHandle(stmtHandle.get()).setMaxRows(maxRows)
            .setFetchSize(fetchSize).setScrollable(isScrollableResultset)
            .build();
    return true;
  }

  private void runAsyncOnServer(String sql) throws SQLException {
    checkConnection("execute");

    reInitState();

    TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, sql);
    /**
     * Run asynchronously whenever possible
     * Currently only a SQLOperation can be run asynchronously,
     * in a background operation thread
     * Compilation can run asynchronously or synchronously and execution run asynchronously
     */
    execReq.setRunAsync(true);
    execReq.setConfOverlay(sessConf);
    execReq.setQueryTimeout(queryTimeout);
    try {
      LOG.debug("Submitting statement [{}]: {}", sessHandle, sql);
      TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
      Utils.verifySuccessWithInfo(execResp.getStatus());
      List<String> infoMessages = execResp.getStatus().getInfoMessages();
      if (infoMessages != null) {
        for (String message : infoMessages) {
          LOG.info(message);
        }
      }
      stmtHandle = Optional.of(execResp.getOperationHandle());
      LOG.debug("Running with statement handle: {}", stmtHandle.get());
    } catch (SQLException eS) {
      isLogBeingGenerated = false;
      throw eS;
    } catch (Exception ex) {
      isLogBeingGenerated = false;
      throw new SQLException("Failed to run async statement", "08S01", ex);
    }
  }

  /**
   * Poll the result set status by checking if isSetHasResultSet is set
   * @return
   * @throws SQLException
   */
  private TGetOperationStatusResp waitForResultSetStatus() throws SQLException {
    TGetOperationStatusReq statusReq = new TGetOperationStatusReq(stmtHandle.get());
    TGetOperationStatusResp statusResp = null;

    while (statusResp == null || !statusResp.isSetHasResultSet()) {
      try {
        statusResp = client.GetOperationStatus(statusReq);
      } catch (TException e) {
        isLogBeingGenerated = false;
        throw new SQLException("Failed to wait for result set status", "08S01", e);
      }
    }

    return statusResp;
  }

  TGetOperationStatusResp waitForOperationToComplete() throws SQLException {
    TGetOperationStatusResp statusResp = null;

    final TGetOperationStatusReq statusReq = new TGetOperationStatusReq(stmtHandle.get());
    statusReq.setGetProgressUpdate(inPlaceUpdateStream.isPresent());

    // Progress bar is completed if there is nothing to request
    if (inPlaceUpdateStream.isPresent()) {
      inPlaceUpdateStream.get().getEventNotifier().progressBarCompleted();
    }

    LOG.debug("Waiting on operation to complete: Polling operation status");

    // Poll on the operation status, till the operation is complete
    do {
      try {
        if (Thread.currentThread().isInterrupted()) {
          throw new SQLException(CLIENT_POLLING_OPSTATUS_INTERRUPTED.getMsg(),
              CLIENT_POLLING_OPSTATUS_INTERRUPTED.getSQLState());
        }
        /**
         * For an async SQLOperation, GetOperationStatus will use the long polling approach It will
         * essentially return after the HIVE_SERVER2_LONG_POLLING_TIMEOUT (a server config) expires
         */
        statusResp = client.GetOperationStatus(statusReq);
        LOG.debug("Status response: {}", statusResp);
        if (!isOperationComplete && inPlaceUpdateStream.isPresent()) {
          inPlaceUpdateStream.get().update(statusResp.getProgressUpdateResponse());
        }
        Utils.verifySuccessWithInfo(statusResp.getStatus());
        if (statusResp.isSetOperationState()) {
          switch (statusResp.getOperationState()) {
          case CLOSED_STATE:
          case FINISHED_STATE:
            isOperationComplete = true;
            isLogBeingGenerated = false;
            break;
          case CANCELED_STATE:
            // 01000 -> warning
            final String errMsg = statusResp.getErrorMessage();
            final String fullErrMsg =
                (errMsg == null || errMsg.isEmpty()) ? QUERY_CANCELLED_MESSAGE : QUERY_CANCELLED_MESSAGE + " " + errMsg;
            throw new SQLException(fullErrMsg, "01000");
          case TIMEDOUT_STATE:
            throw new SQLTimeoutException("Query timed out after " + queryTimeout + " seconds");
          case ERROR_STATE:
            // Get the error details from the underlying exception
            throw new SQLException(statusResp.getErrorMessage(), statusResp.getSqlState(),
                statusResp.getErrorCode());
          case UKNOWN_STATE:
            throw new SQLException("Unknown query", "HY000");
          case INITIALIZED_STATE:
          case PENDING_STATE:
          case RUNNING_STATE:
            break;
          }
        }
      } catch (SQLException e) {
        isLogBeingGenerated = false;
        throw e;
      } catch (Exception e) {
        isLogBeingGenerated = false;
        throw new SQLException("Failed to wait for operation to complete", "08S01", e);
      }
    } while (!isOperationComplete);

    // set progress bar to be completed when hive query execution has completed
    if (inPlaceUpdateStream.isPresent()) {
      inPlaceUpdateStream.get().getEventNotifier().progressBarCompleted();
    }
    return statusResp;
  }

  private void checkConnection(String action) throws SQLException {
    if (isClosed) {
      throw new SQLException("Cannot " + action + " after statement has been closed");
    }
  }

  /**
   * Close statement if needed, and reset the flags.
   * @throws SQLException
   */
  private void reInitState() throws SQLException {
    try {
      closeStatementIfNeeded();
    } finally {
      isCancelled = false;
      isQueryClosed = false;
      isLogBeingGenerated = true;
      isOperationComplete = false;
    }
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    if (!execute(sql)) {
      throw new SQLException("The query did not generate a result set");
    }
    return resultSet;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    execute(sql);
    return getUpdateCount();
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Connection getConnection() throws SQLException {
    checkConnection("getConnection");
    return this.connection;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    checkConnection("getFetchDirection");
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public int getFetchSize() throws SQLException {
    checkConnection("getFetchSize");
    return fetchSize;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxRows() throws SQLException {
    checkConnection("getMaxRows");
    return maxRows;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    checkConnection("getQueryTimeout");
    return this.queryTimeout;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    checkConnection("getResultSet");
    return resultSet;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getResultSetType() throws SQLException {
    checkConnection("getResultSetType");
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    checkConnection("getUpdateCount");
    /**
     * Poll on the operation status, till the operation is complete. We want to ensure that since a
     * client might end up using executeAsync and then call this to check if the query run is
     * finished.
     */
    long numModifiedRows = -1L;
    TGetOperationStatusResp resp = waitForOperationToComplete();
    if (resp != null) {
      numModifiedRows = resp.getNumModifiedRows();
    }
    if (numModifiedRows == -1L || numModifiedRows > Integer.MAX_VALUE) {
      LOG.warn("Invalid number of updated rows: {}", numModifiedRows);
      return -1;
    }
    return (int) numModifiedRows;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    checkConnection("getWarnings");
    return warningChain;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  public boolean isQueryClosed() throws SQLException {
    return isQueryClosed;
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return closeOnResultSetCompletion;
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    if (enable) {
      throw new SQLFeatureNotSupportedException("Method not supported");
    }
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    checkConnection("setFetchDirection");
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SQLException("Not supported direction: " + direction);
    }
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    checkConnection("setFetchSize");
    if (rows > 0) {
      this.fetchSize = rows;
    } else if (rows == 0) {
      this.fetchSize = this.defaultFetchSize;
    } else {
      throw new SQLException("Fetch size must be greater or equal to 0");
    }
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    checkConnection("setMaxRows");
    if (max < 0) {
      throw new SQLException("Maximum number of rows must be >= 0");
    }
    maxRows = max;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    this.queryTimeout = seconds;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException("Cannot unwrap to " + iface);
  }

  /**
   * Check whether query execution might be producing more logs to be fetched.
   * This method is a public API for usage outside of Hive, although it is not part of the
   * interface java.sql.Statement.
   * @return true if query execution might be producing more logs. It does not indicate if last
   *         log lines have been fetched by getQueryLog.
   */
  public boolean hasMoreLogs() {
    return isLogBeingGenerated;
  }

  /**
   * Get the execution logs of the given SQL statement.
   * This method is a public API for usage outside of Hive, although it is not part of the
   * interface java.sql.Statement.
   * This method gets the incremental logs during SQL execution, and uses fetchSize holden by
   * HiveStatement object.
   * @return a list of logs. It can be empty if there are no new logs to be retrieved at that time.
   * @throws SQLException
   * @throws ClosedOrCancelledStatementException if statement has been cancelled or closed
   */
  public List<String> getQueryLog() throws SQLException, ClosedOrCancelledStatementException {
    return getQueryLog(true, fetchSize);
  }

  /**
   * Get the execution logs of the given SQL statement.
   * This method is a public API for usage outside of Hive, although it is not part of the
   * interface java.sql.Statement.
   * @param incremental indicate getting logs either incrementally or from the beginning,
   *                    when it is true or false.
   * @param fetchSize the number of lines to fetch
   * @return a list of logs. It can be empty if there are no new logs to be retrieved at that time.
   * @throws SQLException
   * @throws ClosedOrCancelledStatementException if statement has been cancelled or closed
   */
  public List<String> getQueryLog(boolean incremental, int fetchSize)
      throws SQLException, ClosedOrCancelledStatementException {
    checkConnection("getQueryLog");
    if (isCancelled) {
      throw new ClosedOrCancelledStatementException("Method getQueryLog() failed. The " +
          "statement has been closed or cancelled.");
    }

    TFetchResultsResp tFetchResultsResp = null;
    try {
      if (stmtHandle.isPresent()) {
        TFetchResultsReq tFetchResultsReq = new TFetchResultsReq(stmtHandle.get(),
            getFetchOrientation(incremental), fetchSize);
        tFetchResultsReq.setFetchType((short)1);
        tFetchResultsResp = client.FetchResults(tFetchResultsReq);
        Utils.verifySuccessWithInfo(tFetchResultsResp.getStatus());
      } else {
        if (isQueryClosed) {
          throw new ClosedOrCancelledStatementException("Method getQueryLog() failed. The " +
              "statement has been closed or cancelled.");
        } else {
          return Collections.emptyList();
        }
      }
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException("Error when getting query log", e);
    }

    final List<String> logs = new ArrayList<>();
    try {
      final RowSet rowSet = RowSetFactory.create(tFetchResultsResp.getResults(), connection.getProtocol());
      for (Object[] row : rowSet) {
        logs.add(String.valueOf(row[0]));
      }
    } catch (TException e) {
      throw new SQLException("Error building result set for query log", e);
    }

    return Collections.unmodifiableList(logs);
  }

  private TFetchOrientation getFetchOrientation(boolean incremental) {
    return (incremental) ? TFetchOrientation.FETCH_NEXT : TFetchOrientation.FETCH_FIRST;
  }

  /**
   * Returns the Yarn ATS GUID.
   * This method is a public API for usage outside of Hive, although it is not part of the
   * interface java.sql.Statement.
   * @return Yarn ATS GUID or null if it hasn't been created yet.
   */
  public String getYarnATSGuid() {
    // Set on the server side.
    // @see org.apache.hive.service.cli.operation.SQLOperation#prepare
    return (stmtHandle.isPresent())
        ? Base64.getUrlEncoder().withoutPadding().encodeToString(stmtHandle.get().getOperationId().getGuid())
        : null;
  }

  /**
   * This is only used by the beeline client to set the stream on which in place
   * progress updates are to be shown.
   */
  public void setInPlaceUpdateStream(InPlaceUpdateStream stream) {
    this.inPlaceUpdateStream = Optional.ofNullable(stream);
  }

  /**
   * Returns the Query ID if it is running. This method is a public API for
   * usage outside of Hive, although it is not part of the interface
   * java.sql.Statement.
   *
   * @return Valid query ID if it is running else returns NULL.
   * @throws SQLException If any internal failures.
   */
  @LimitedPrivate(value={"Hive and closely related projects."})
  public String getQueryId() throws SQLException {
    // Storing it in temp variable as this method is not thread-safe and concurrent thread can
    // close this handle and set it to null after checking for null.
    TOperationHandle stmtHandleTmp = stmtHandle.orElse(null);
    if (stmtHandleTmp == null) {
      // If query is not running or already closed.
      return null;
    }
    try {
      final String queryId = client.GetQueryId(new TGetQueryIdReq(stmtHandleTmp)).getQueryId();

      // queryId can be empty string if query was already closed. Need to return null in such case.
      return StringUtils.isBlank(queryId) ? null : queryId;
    } catch (TException e) {
      throw new SQLException(e);
    }
  }
}
