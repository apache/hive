/**
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;



public abstract class Operation {
  protected final HiveSession parentSession;
  private OperationState state = OperationState.INITIALIZED;
  private final OperationHandle opHandle;
  private HiveConf configuration;
  public static final Log LOG = LogFactory.getLog(Operation.class.getName());
  public static final long DEFAULT_FETCH_MAX_ROWS = 100;
  protected boolean hasResultSet;

  protected Operation(HiveSession parentSession, OperationType opType) {
    super();
    this.parentSession = parentSession;
    opHandle = new OperationHandle(opType);
  }

  public void setConfiguration(HiveConf configuration) {
    this.configuration = new HiveConf(configuration);
  }

  public HiveConf getConfiguration() {
    return new HiveConf(configuration);
  }

  public HiveSession getParentSession() {
    return parentSession;
  }

  public OperationHandle getHandle() {
    return opHandle;
  }

  public OperationType getType() {
    return opHandle.getOperationType();
  }

  public OperationState getState() {
    return state;
  }

  public boolean hasResultSet() {
    return hasResultSet;
  }

  protected void setHasResultSet(boolean hasResultSet) {
    this.hasResultSet = hasResultSet;
    opHandle.setHasResultSet(hasResultSet);
  }

  protected final OperationState setState(OperationState newState) throws HiveSQLException {
    state.validateTransition(newState);
    this.state = newState;
    return this.state;
  }

  protected final void assertState(OperationState state) throws HiveSQLException {
    if (this.state != state) {
      throw new HiveSQLException("Expected state " + state + ", but found " + this.state);
    }
  }

  public boolean isRunning() {
    return OperationState.RUNNING.equals(getState());
  }

  public boolean isFinished() {
    return OperationState.FINISHED.equals(getState());
  }

  public boolean isCanceled() {
    return OperationState.CANCELED.equals(getState());
  }

  public boolean isFailed() {
    return OperationState.ERROR.equals(getState());
  }

  public abstract void run() throws HiveSQLException;

  // TODO: make this abstract and implement in subclasses.
  public void cancel() throws HiveSQLException {
    setState(OperationState.CANCELED);
    throw new UnsupportedOperationException("SQLOperation.cancel()");
  }

  public abstract void close() throws HiveSQLException;

  public abstract TableSchema getResultSetSchema() throws HiveSQLException;

  public abstract RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException;

  public RowSet getNextRowSet() throws HiveSQLException {
    return getNextRowSet(FetchOrientation.FETCH_NEXT, DEFAULT_FETCH_MAX_ROWS);
  }
}
