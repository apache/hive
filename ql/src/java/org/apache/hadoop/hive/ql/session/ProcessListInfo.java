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
package org.apache.hadoop.hive.ql.session;

/**
 * The class to store query and associated session level info to be used for ProcessListProcessor.
 */
public class ProcessListInfo {
  private final String sessionId;
  private final String userName;
  private final String ipAddr;
  private final long sessionActiveTime;
  private final long sessionIdleTime;
  private final String executionEngine;
  private final String queryId;
  private final String beginTime;
  private final String runtime;  // tracks only running portion of the query.
  private final long elapsedTime;
  private final String state;
  private final long txnId;

  private ProcessListInfo(String userName, String ipAddr, String sessionId, long sessionActiveTime,
      long sessionIdleTime, String queryId, String executionEngine, String beginTime,
      String runtime, long elapsedTime, String state, long txnId) {
    this.userName = userName;
    this.ipAddr = ipAddr;
    this.sessionId = sessionId;
    this.sessionActiveTime = sessionActiveTime;
    this.sessionIdleTime = sessionIdleTime;
    this.queryId = queryId;
    this.executionEngine = executionEngine;
    this.beginTime = beginTime;
    this.runtime = runtime;
    this.elapsedTime = elapsedTime;
    this.state = state;
    this.txnId = txnId;
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getUserName() {
    return userName;
  }

  public String getIpAddr() {
    return ipAddr;
  }

  public long getSessionActiveTime() {
    return sessionActiveTime;
  }

  public long getSessionIdleTime() {
    return sessionIdleTime;
  }

  public String getExecutionEngine() {
    return executionEngine;
  }

  public String getBeginTime() {
    return beginTime;
  }

  public String getQueryId() {
    return queryId;
  }

  public String getRuntime() {
    return runtime;
  }

  public long getElapsedTime() {
    return elapsedTime;
  }

  public String getState() {
    return state;
  }

  public Long getTxnId() {
    return txnId;
  }

  public static class Builder {
    private String userName;
    private String ipAddr;
    private String sessionId;
    private long sessionActiveTime;
    private long sessionIdleTime;
    private String executionEngine;
    private String beginTime;
    private String queryId;
    private String runtime;
    private long elapsedTime;
    private String state;
    private long txnId;

    public Builder setSessionId(String sessionId) {
      this.sessionId = sessionId;
      return this;
    }

    public Builder setUserName(String userName) {
      this.userName = userName;
      return this;
    }

    public Builder setIpAddr(String ipAddr) {
      this.ipAddr = ipAddr;
      return this;
    }

    public Builder setSessionActiveTime(long sessionActiveTime) {
      this.sessionActiveTime = sessionActiveTime;
      return this;
    }

    public Builder setSessionIdleTime(long sessionIdleTime) {
      this.sessionIdleTime = sessionIdleTime;
      return this;
    }

    public Builder setExecutionEngine(String executionEngine) {
      this.executionEngine = executionEngine;
      return this;
    }

    public Builder setBeginTime(String beginTime) {
      this.beginTime = beginTime;
      return this;
    }

    public Builder setQueryId(String queryId) {
      this.queryId = queryId;
      return this;
    }

    public Builder setRuntime(String runtime) {
      this.runtime = runtime;
      return this;
    }

    public Builder setElapsedTime(long elapsedTime) {
      this.elapsedTime = elapsedTime;
      return this;
    }

    public Builder setState(String state) {
      this.state = state;
      return this;
    }

    public Builder setTxnId(long txnId) {
      this.txnId = txnId;
      return this;
    }

    public ProcessListInfo build() {
      ProcessListInfo processListInfo = new ProcessListInfo(userName, ipAddr, sessionId, sessionActiveTime,
          sessionIdleTime, queryId, executionEngine, beginTime, runtime,
          elapsedTime, state, txnId);
      return processListInfo;
    }
  }
}
