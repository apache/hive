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

package org.apache.hadoop.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.QueryState;

/**
 * Represents a scheduled execution.
 */
public class MScheduledExecution {

  private int scheduledExecutionId;
  private MScheduledQuery scheduledQuery;
  private String executorQueryId;
  private String state;
  private Integer startTime;
  private Integer endTime;
  private String errorMessage;
  private Integer lastUpdateTime;

  public MScheduledExecution() {
  }

  @Override
  public String toString() {
    return String.format("state: %s, scheduledQuery: %s, execId: %d", state, scheduledQuery.getScheduleName(),
        executorQueryId);
  }

  public int getScheduledExecutionId() {
    return scheduledExecutionId;
  }

  public MScheduledQuery getScheduledQuery() {
    return scheduledQuery;
  }

  public String getExecutorQueryId() {
    return executorQueryId;
  }

  public QueryState getState() {
    return QueryState.valueOf(state);
  }

  public Integer getStartTime() {
    return startTime;
  }

  public Integer getEndTime() {
    return endTime;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public Integer getLastUpdateTime() {
    return lastUpdateTime;
  }

  public void setState(QueryState state) {
    this.state = state.name();
  }

  public void setExecutorQueryId(String executorQueryId) {
    this.executorQueryId = executorQueryId;
  }

  public void setLastUpdateTime(Integer newUpdateTime) {
    lastUpdateTime = newUpdateTime;
  }

  public void setEndTime(Integer endTime) {
    this.endTime = endTime;
  }

  public void setScheduledQuery(MScheduledQuery scheduledQuery) {
    this.scheduledQuery = scheduledQuery;
  }

  public void setStartTime(Integer startTime) {
    this.startTime = startTime;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }
}
