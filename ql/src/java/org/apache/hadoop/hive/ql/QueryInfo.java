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

import java.util.concurrent.TimeUnit;

/**
 * Provide WebUI information about a running query. Class is thread safe so that
 * multiple browser sessions can access the data simultaneously.
 */
public class QueryInfo {

  private final String userName;
  private final String executionEngine;
  private final String operationId;

  private String state;
  private QueryDisplay queryDisplay;

  /*
   * Times are stored internally with nanosecond precision.
   */
  private final long beginTime;
  private long runtime;
  private long endTime;

  private String operationLogLocation;

  public QueryInfo(String state, String userName, String executionEngine, String operationId) {
    this.state = state;
    this.userName = userName;
    this.executionEngine = executionEngine;
    this.beginTime = System.nanoTime();
    this.operationId = operationId;
    this.endTime = -1L;
  }

  /**
   * Get query running time in milliseconds.
   *
   * @return the current running time
   */
  public long getElapsedTime() {
    final long maxTime = isRunning() ? System.nanoTime() : endTime;
    return TimeUnit.NANOSECONDS.toMillis(maxTime - beginTime);
  }

  public boolean isRunning() {
    return endTime == -1L;
  }

  public QueryDisplay getQueryDisplay() {
    return queryDisplay;
  }

  public void setQueryDisplay(QueryDisplay queryDisplay) {
    this.queryDisplay = queryDisplay;
  }

  public String getUserName() {
    return userName;
  }

  public String getExecutionEngine() {
    return executionEngine;
  }

  public String getState() {
    return state;
  }

  /**
   * The time the query began in milliseconds.
   *
   * @return The time the query began
   */
  public long getBeginTime() {
    return TimeUnit.NANOSECONDS.toMillis(beginTime);
  }

  /**
   * Get the end time in milliseconds. Only valid if {@link #isRunning()}
   * returns false.
   *
   * @return Query end time
   */
  public long getEndTime() {
    return TimeUnit.NANOSECONDS.toMillis(endTime);
  }

  public void updateState(String state) {
    this.state = state;
  }

  public String getOperationId() {
    return operationId;
  }

  public void setEndTime() {
    this.endTime = System.nanoTime();
  }

  /**
   * Set the amount of time the query spent actually running in milliseconds.
   *
   * @param runtime The amount of time this query spent running
   */
  public void setRuntime(long runtime) {
    this.runtime = TimeUnit.MILLISECONDS.toNanos(runtime);
  }

  /**
   * Get the amount of time the query spent actually running in milliseconds.
   *
   * @return The amount of time this query spent running
   */
  public long getRuntime() {
    return TimeUnit.NANOSECONDS.toMillis(runtime);
  }

  public String getOperationLogLocation() {
    return operationLogLocation;
  }

  public void setOperationLogLocation(String operationLogLocation) {
    this.operationLogLocation = operationLogLocation;
  }
}
