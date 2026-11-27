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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.rpc.thrift.TOperationState;

/**
 * The class is synchronized, as WebUI may access information about a running query.
 */
public class QueryInfo {

  private final String userName;
  private final String executionEngine;
  private final Long beginTime;
  private final String sessionId;
  private final String operationId;
  private Long runtime;  // tracks only running portion of the query.
  private Long endTime;
  private TOperationState state;
  private QueryDisplay queryDisplay;

  private String operationLogLocation;

  public QueryInfo(TOperationState state, String userName, String executionEngine, String sessionId,
      String operationId) {
    this.state = state;
    this.userName = userName;
    this.executionEngine = executionEngine;
    this.beginTime = System.currentTimeMillis();
    this.sessionId = sessionId;
    this.operationId = operationId;
  }

  public static QueryInfo getFromConf(HiveConf conf) {
    return new QueryInfo(TOperationState.INITIALIZED_STATE, conf.get(DriverContext.DEFAULT_USER_NAME_PROP),
        conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE), HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SESSION_ID),
        conf.get(DriverContext.DEFAULT_OPERATION_ID_PROP));
  }

  public synchronized long getElapsedTime() {
    if (isRunning()) {
      return System.currentTimeMillis() - beginTime;
    } else {
      return endTime - beginTime;
    }
  }

  public synchronized boolean isRunning() {
    return endTime == null;
  }

  public synchronized QueryDisplay getQueryDisplay() {
    return queryDisplay;
  }

  public synchronized void setQueryDisplay(QueryDisplay queryDisplay) {
    this.queryDisplay = queryDisplay;
  }

  public String getUserName() {
    return userName;
  }

  public String getExecutionEngine() {
    return executionEngine;
  }

  public synchronized String getState() {
    return getDisplayState(state);
  }

  public synchronized TOperationState getOperationState() {
    return state;
  }

  public long getBeginTime() {
    return beginTime;
  }

  public synchronized Long getEndTime() {
    return endTime;
  }

  public synchronized void updateState(TOperationState state) {
    this.state = state;
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getOperationId() {
    return operationId;
  }

  public synchronized void setEndTime() {
    this.endTime = System.currentTimeMillis();
  }

  public synchronized void setRuntime(long runtime) {
    this.runtime = runtime;
  }

  public synchronized Long getRuntime() {
    return runtime;
  }

  public String getOperationLogLocation() {
    return operationLogLocation;
  }

  public void setOperationLogLocation(String operationLogLocation) {
    this.operationLogLocation = operationLogLocation;
  }

  private static String getDisplayState(TOperationState state) {
    if (state == null) {
      return "UNKNOWN";
    }

    switch (state) {
    case INITIALIZED_STATE:
      return "INITIALIZED";
    case RUNNING_STATE:
      return "RUNNING";
    case FINISHED_STATE:
      return "FINISHED";
    case CANCELED_STATE:
      return "CANCELED";
    case CLOSED_STATE:
      return "CLOSED";
    case ERROR_STATE:
      return "ERROR";
    case UKNOWN_STATE:
      return "UNKNOWN";
    case PENDING_STATE:
      return "PENDING";
    case TIMEDOUT_STATE:
      return "TIMEDOUT";
    default:
      return "UNKNOWN";
    }
  }
}
