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

import org.apache.hadoop.hive.metastore.api.ScheduledQuery;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;

public class MScheduledExecution {

  public MScheduledExecution(MScheduledQuery schq, int now) {
    scheduledQuery = schq;
    startTime = now;
    state = "INITED";
  }

  private int scheduledExecutionId;
  private MScheduledQuery scheduledQuery;
  private String executorQueryId;
  private String state;
  private Integer startTime;
  private Integer endTime;
  private String errorString;
  // FIXME rename to lastUpdateTime
  private Integer nextDeadline;

  @Override
  public String toString() {
    return String.format("state: %s, scheduledQuery: %s, execId: %d", state, scheduledQuery.getScheduleName(),
        executorQueryId);
  }

  //  public MScheduledQuery2(ScheduledQuery s) {
  //    scheduleName = s.getScheduleKey().getScheduleName();
  //    enabled = s.isEnabled();
  //    clusterNamespace = s.getScheduleKey().getClusterNamespace();
  //    schedule = s.getSchedule();
  //    user = s.getUser();
  //    query = s.getQuery();
  //    nextExecution = s.getNextExecution();
  //  }
  //
  //  public static MScheduledQuery2 fromThrift(ScheduledQuery s) {
  //    return new MScheduledQuery2(s);
  //  }
  //
  //  public static ScheduledQuery toThrift(MScheduledQuery2 s) {
  //    ScheduledQuery ret = new ScheduledQuery();
  //    ret.setScheduleKey(new ScheduledQueryKey(s.scheduleName, s.clusterNamespace));
  //    ret.setEnabled(s.enabled);
  //    ret.setSchedule(s.schedule);
  //    ret.setUser(s.user);
  //    ret.setQuery(s.query);
  //    ret.setNextExecution(s.nextExecution);
  //    return ret;
  //  }
  //
  //  public ScheduledQuery toThrift() {
  //    return toThrift(this);
  //  }
  //
  //  public void doUpdate(MScheduledQuery2 schq) {
  //    // may not change scheduleName
  //    enabled = schq.enabled;
  //    clusterNamespace = schq.clusterNamespace;
  //    schedule = schq.schedule;
  //    user = schq.user;
  //    query = schq.query;
  //    // may not change nextExecution
  //  }
  //
  //  public String getSchedule() {
  //    return schedule;
  //  }
  //
  //  public Integer getNextExecution() {
  //    return nextExecution;
  //  }
  //
  //  public void setNextExecution(Integer nextExec) {
  //    nextExecution = nextExec;
  //  }
  //
  //  public String getQuery() {
  //    return query;
  //  }
  //
  //  public String getScheduleName() {
  //    return scheduleName;
  //  }
  //
  //  public ScheduledQueryKey getScheduleKey() {
  //    return new ScheduledQueryKey(scheduleName, clusterNamespace);
  //  }
  //

  public int getScheduledExecutionId() {
    return scheduledExecutionId;
  }

  public MScheduledQuery getScheduledQuery() {
    return scheduledQuery;
  }

  public String getExecutorQueryId() {
    return executorQueryId;
  }

  public String getState() {
    return state;
  }

  public Integer getStartTime() {
    return startTime;
  }

  public Integer getEndTime() {
    return endTime;
  }

  public String getErrorString() {
    return errorString;
  }

  public Integer getNextDeadline() {
    return nextDeadline;
  }

  public void setState(String state) {
    this.state = state;
  }

  public void setExecutorQueryId(String executorQueryId) {
    this.executorQueryId = executorQueryId;
  }

  public void setDeadline(Integer newScheduledQueryDeadline) {
    nextDeadline = newScheduledQueryDeadline;

  }

}
