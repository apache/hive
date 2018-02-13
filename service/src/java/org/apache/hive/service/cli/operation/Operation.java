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

import java.io.File;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.common.MetricsScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.log.LogDivertAppender;
import org.apache.hadoop.hive.ql.log.LogDivertAppenderForTest;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public abstract class Operation {
  protected final HiveSession parentSession;
  private volatile OperationState state = OperationState.INITIALIZED;
  private volatile MetricsScope currentStateScope;
  private final OperationHandle opHandle;
  public static final FetchOrientation DEFAULT_FETCH_ORIENTATION = FetchOrientation.FETCH_NEXT;
  public static final Logger LOG = LoggerFactory.getLogger(Operation.class.getName());
  protected boolean hasResultSet;
  protected volatile HiveSQLException operationException;
  protected volatile Future<?> backgroundHandle;
  protected OperationLog operationLog;
  protected boolean isOperationLogEnabled;

  private long operationTimeout;
  private volatile long lastAccessTime;
  private final long beginTime;

  protected long operationStart;
  protected long operationComplete;

  protected final QueryState queryState;

  protected static final EnumSet<FetchOrientation> DEFAULT_FETCH_ORIENTATION_SET =
      EnumSet.of(FetchOrientation.FETCH_NEXT,FetchOrientation.FETCH_FIRST);


  protected Operation(HiveSession parentSession, OperationType opType) {
    this(parentSession, null, opType);
  }

  protected Operation(HiveSession parentSession,
      Map<String, String> confOverlay, OperationType opType) {
    this.parentSession = parentSession;
    this.opHandle = new OperationHandle(opType, parentSession.getProtocolVersion());
    beginTime = System.currentTimeMillis();
    lastAccessTime = beginTime;
    operationTimeout = HiveConf.getTimeVar(parentSession.getHiveConf(),
        HiveConf.ConfVars.HIVE_SERVER2_IDLE_OPERATION_TIMEOUT, TimeUnit.MILLISECONDS);

    currentStateScope = updateOperationStateMetrics(null, MetricsConstant.OPERATION_PREFIX,
        MetricsConstant.COMPLETED_OPERATION_PREFIX, state);
    queryState = new QueryState.Builder()
                     .withConfOverlay(confOverlay)
                     .withGenerateNewQueryId(true)
                     .withHiveConf(parentSession.getHiveConf())
                     .build();
  }

  public Future<?> getBackgroundHandle() {
    return backgroundHandle;
  }

  protected void setBackgroundHandle(Future<?> backgroundHandle) {
    this.backgroundHandle = backgroundHandle;
  }

  public boolean shouldRunAsync() {
    return false; // Most operations cannot run asynchronously.
  }

  public HiveSession getParentSession() {
    return parentSession;
  }

  public OperationHandle getHandle() {
    return opHandle;
  }

  public TProtocolVersion getProtocolVersion() {
    return opHandle.getProtocolVersion();
  }

  public OperationType getType() {
    return opHandle.getOperationType();
  }

  public OperationStatus getStatus() {
    String taskStatus = null;
    try {
      taskStatus = getTaskStatus();
    } catch (HiveSQLException sqlException) {
      LOG.error("Error getting task status for " + opHandle.toString(), sqlException);
    }
    return new OperationStatus(state, taskStatus, operationStart, operationComplete, hasResultSet, operationException);
  }

  public boolean hasResultSet() {
    return hasResultSet;
  }

  protected void setHasResultSet(boolean hasResultSet) {
    this.hasResultSet = hasResultSet;
    opHandle.setHasResultSet(hasResultSet);
  }

  public OperationLog getOperationLog() {
    return operationLog;
  }

  protected final OperationState setState(OperationState newState) throws HiveSQLException {
    state.validateTransition(newState);
    OperationState prevState = state;
    this.state = newState;
    currentStateScope = updateOperationStateMetrics(currentStateScope, MetricsConstant.OPERATION_PREFIX,
        MetricsConstant.COMPLETED_OPERATION_PREFIX, state);
    onNewState(state, prevState);
    this.lastAccessTime = System.currentTimeMillis();
    return this.state;
  }

  public boolean isTimedOut(long current) {
    if (operationTimeout == 0) {
      return false;
    }
    if (operationTimeout > 0) {
      // check only when it's in terminal state
      return state.isTerminal() && lastAccessTime + operationTimeout <= current;
    }
    return lastAccessTime + -operationTimeout <= current;
  }

  public long getLastAccessTime() {
    return lastAccessTime;
  }

  public long getOperationTimeout() {
    return operationTimeout;
  }

  public void setOperationTimeout(long operationTimeout) {
    this.operationTimeout = operationTimeout;
  }

  protected void setOperationException(HiveSQLException operationException) {
    this.operationException = operationException;
  }

  protected final void assertState(List<OperationState> states) throws HiveSQLException {
    if (!states.contains(state)) {
      throw new HiveSQLException("Expected states: " + states.toString() + ", but found "
          + this.state);
    }
    this.lastAccessTime = System.currentTimeMillis();
  }

  public boolean isDone() {
    return state.isTerminal();
  }

  protected void createOperationLog() {
    if (parentSession.isOperationLogEnabled()) {
      File operationLogFile = new File(parentSession.getOperationLogSessionDir(), queryState.getQueryId());
      isOperationLogEnabled = true;

      // create OperationLog object with above log file
      operationLog = new OperationLog(opHandle.toString(), operationLogFile, parentSession.getHiveConf());
    }
  }

  /**
   * Invoked before runInternal().
   * Set up some preconditions, or configurations.
   */
  protected void beforeRun() {
    createOperationLog();
    LogUtils.registerLoggingContext(queryState.getConf());
  }

  /**
   * Invoked after runInternal(), even if an exception is thrown in runInternal().
   * Clean up resources, which was set up in beforeRun().
   */
  protected void afterRun() {
    LogUtils.unregisterLoggingContext();
  }

  /**
   * Implemented by subclass of Operation class to execute specific behaviors.
   * @throws HiveSQLException
   */
  protected abstract void runInternal() throws HiveSQLException;

  public void run() throws HiveSQLException {
    beforeRun();
    try {
      Metrics metrics = MetricsFactory.getInstance();
      if (metrics != null) {
        metrics.incrementCounter(MetricsConstant.OPEN_OPERATIONS);
      }
      runInternal();
    } finally {
      afterRun();
    }
  }

  protected synchronized void cleanupOperationLog() {
    if (isOperationLogEnabled) {
      if (opHandle == null) {
        LOG.warn("Operation seems to be in invalid state, opHandle is null");
        return;
      }
      if (operationLog == null) {
        LOG.warn("Operation [ " + opHandle.getHandleIdentifier() + " ] " + "logging is enabled, "
            + "but its OperationLog object cannot be found. "
            + "Perhaps the operation has already terminated.");
      } else {
        operationLog.close();
      }
      // stop the appenders for the operation log
      String queryId = queryState.getQueryId();
      LogUtils.stopQueryAppender(LogDivertAppender.QUERY_ROUTING_APPENDER, queryId);
      LogUtils.stopQueryAppender(LogDivertAppenderForTest.TEST_QUERY_ROUTING_APPENDER, queryId);
    }
  }

  public abstract void cancel(OperationState stateAfterCancel) throws HiveSQLException;

  public abstract void close() throws HiveSQLException;

  public abstract TableSchema getResultSetSchema() throws HiveSQLException;

  public abstract RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException;

  public String getTaskStatus() throws HiveSQLException {
    return null;
  }

  /**
   * Verify if the given fetch orientation is part of the default orientation types.
   * @param orientation
   * @throws HiveSQLException
   */
  protected void validateDefaultFetchOrientation(FetchOrientation orientation)
      throws HiveSQLException {
    validateFetchOrientation(orientation, DEFAULT_FETCH_ORIENTATION_SET);
  }

  /**
   * Verify if the given fetch orientation is part of the supported orientation types.
   * @param orientation
   * @param supportedOrientations
   * @throws HiveSQLException
   */
  protected void validateFetchOrientation(FetchOrientation orientation,
      EnumSet<FetchOrientation> supportedOrientations) throws HiveSQLException {
    if (!supportedOrientations.contains(orientation)) {
      throw new HiveSQLException("The fetch type " + orientation.toString() +
          " is not supported for this resultset", "HY106");
    }
  }

  protected HiveSQLException toSQLException(String prefix, CommandProcessorResponse response) {
    HiveSQLException ex = new HiveSQLException(prefix + ": " + response.getErrorMessage(),
        response.getSQLState(), response.getResponseCode());
    if (response.getException() != null) {
      ex.initCause(response.getException());
    }
    return ex;
  }

  //list of operation states to measure duration of.
  protected static Set<OperationState> scopeStates = Sets.immutableEnumSet(
    OperationState.INITIALIZED,
    OperationState.PENDING,
    OperationState.RUNNING
  );

  //list of terminal operation states.  We measure only completed counts for operations in these states.
  protected static Set<OperationState> terminalStates = Sets.immutableEnumSet(
    OperationState.CLOSED,
    OperationState.CANCELED,
    OperationState.FINISHED,
    OperationState.ERROR,
    OperationState.UNKNOWN
  );

  protected final MetricsScope updateOperationStateMetrics(MetricsScope stateScope, String operationPrefix,
      String completedOperationPrefix, OperationState state) {
    Metrics metrics = MetricsFactory.getInstance();
    if (metrics != null) {
      if (stateScope != null) {
        metrics.endScope(stateScope);
        stateScope = null;
      }
      if (scopeStates.contains(state)) {
        stateScope = metrics.createScope(MetricsConstant.API_PREFIX + operationPrefix + state);
      }
      if (terminalStates.contains(state)) {
        metrics.incrementCounter(completedOperationPrefix + state);
      }
    }
    return stateScope;
  }

  public long getBeginTime() {
    return beginTime;
  }

  protected OperationState getState() {
    return state;
  }

  protected void onNewState(OperationState state, OperationState prevState) {
    switch(state) {
      case RUNNING:
        markOperationStartTime();
        break;
      case ERROR:
      case FINISHED:
      case CANCELED:
        markOperationCompletedTime();
        break;
    }
  }

  public long getOperationComplete() {
    return operationComplete;
  }

  public long getOperationStart() {
    return operationStart;
  }

  protected void markOperationStartTime() {
    operationStart = System.currentTimeMillis();
  }

  protected void markOperationCompletedTime() {
    operationComplete = System.currentTimeMillis();
  }
}
