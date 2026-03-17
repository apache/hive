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

package org.apache.hadoop.hive.ql.exec.tez;

import org.apache.hadoop.hive.ql.exec.tez.monitoring.TezJobMonitor;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounters;

/**
 * TezRuntimeContext is a class used by mainly TezTask to store runtime information.
 */
public class TezRuntimeContext {
  //package protected: it's fine to be visible within the tez related package
  TezCounters counters;

  // dag id of the running dag
  private String dagId;
  // tez application id
  private String appId;
  // tez session id
  private String sessionId;
  // address (host:port) of the AM
  private String amAddress;
  private TezJobMonitor monitor;
  // llap/container
  private String executionMode;

  public void init(TezSessionState sessionState) {
    this.amAddress = sessionState.getAppMasterUri();
  }

  public TezCounters getCounters() {
    return counters;
  }

  public void setCounters(TezCounters counters) {
    this.counters = counters;
  }

  public String getDagId() {
    return dagId;
  }

  public void setDagId(String dagId) {
    this.dagId = dagId;
  }

  public String getSessionId() {
    return sessionId;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public String getApplicationId() {
    return appId;
  }

  public void setApplicationId(String appId) {
    this.appId = appId;
  }

  public String getExecutionMode() {
    return executionMode;
  }

  public void setExecutionMode(String executionMode) {
    this.executionMode = executionMode;
  }

  public String getAmAddress() {
    return amAddress;
  }

  public TezJobMonitor getMonitor() {
    return monitor;
  }

  public void setMonitor(TezJobMonitor monitor) {
    this.monitor = monitor;
  }

  public long getCounter(String groupName, String counterName) {
    CounterGroup group = counters == null ? null : counters.getGroup(groupName);
    if (group == null) {
      return 0;
    }
    return group.findCounter(counterName, true).getValue();
  }
}
