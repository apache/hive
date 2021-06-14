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

import java.util.Set;

import org.apache.hadoop.hive.metastore.api.ScheduledQuery;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;

/**
 * Describes a scheduled query.
 */
public class MScheduledQuery {

  private String clusterNamespace;
  private String scheduleName;
  private boolean enabled;
  private String schedule;
  private String user;
  private String query;
  private Integer nextExecution;
  private MScheduledExecution activeExecution;
  private Set<MScheduledExecution> executions;

  public MScheduledQuery(ScheduledQuery s) {
    clusterNamespace = s.getScheduleKey().getClusterNamespace();
    scheduleName = s.getScheduleKey().getScheduleName();
    enabled = s.isEnabled();
    schedule = s.getSchedule();
    user = s.getUser();
    query = s.getQuery();
    nextExecution = s.getNextExecution();
  }

  public static MScheduledQuery fromThrift(ScheduledQuery s) {
    return new MScheduledQuery(s);
  }

  public static ScheduledQuery toThrift(MScheduledQuery s) {
    ScheduledQuery ret = new ScheduledQuery();
    ret.setScheduleKey(new ScheduledQueryKey(s.scheduleName, s.clusterNamespace));
    ret.setEnabled(s.enabled);
    ret.setSchedule(s.schedule);
    ret.setUser(s.user);
    ret.setQuery(s.query);
    ret.setNextExecution(s.nextExecution);
    return ret;
  }

  public ScheduledQuery toThrift() {
    return toThrift(this);
  }

  public void doUpdate(MScheduledQuery schq) {
    // may not change scheduleName
    enabled = schq.enabled;
    clusterNamespace = schq.clusterNamespace;
    schedule = schq.schedule;
    user = schq.user;
    query = schq.query;
    // may not change nextExecution
  }

  public String getSchedule() {
    return schedule;
  }

  public Integer getNextExecution() {
    return nextExecution;
  }

  public void setNextExecution(Integer nextExec) {
    nextExecution = nextExec;
  }

  public String getQuery() {
    return query;
  }

  public String getScheduleName() {
    return scheduleName;
  }

  public ScheduledQueryKey getScheduleKey() {
    return new ScheduledQueryKey(scheduleName, clusterNamespace);
  }

  public Set<MScheduledExecution>  getExecutions() {
    return executions;
  }

  public void setExecutions(Set<MScheduledExecution>  e) {
    executions=e;
  }

  public String getUser() {
    return user;
  }

  public void setActiveExecution(MScheduledExecution execution) {
    activeExecution = execution;
  }

  public MScheduledExecution getActiveExecution() {
    return activeExecution;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }
}
