/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.wm;

import static org.apache.hadoop.hive.ql.exec.tez.monitoring.Constants.SEPARATOR;

import java.text.DecimalFormat;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.management.MXBean;

import org.apache.hadoop.hive.ql.exec.tez.WmEvent;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.PrintSummary;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Some context information that are required for rule evaluation.
 */
@MXBean
public class WmContext implements PrintSummary {
  private static final Logger LOG = LoggerFactory.getLogger(WmContext.class);
  @JsonProperty("queryId")
  private String queryId;
  @JsonProperty("queryStartTime")
  private long queryStartTime;
  @JsonProperty("queryEndTime")
  private long queryEndTime;
  @JsonProperty("queryCompleted")
  private boolean queryCompleted;
  @JsonProperty("queryWmEvents")
  private final List<WmEvent> queryWmEvents = new LinkedList<>();
  @JsonProperty("appliedTriggers")
  private Set<Trigger> appliedTriggers = new HashSet<>();
  @JsonProperty("subscribedCounters")
  private Set<String> subscribedCounters = new HashSet<>();
  @JsonProperty("currentCounters")
  private Map<String, Long> currentCounters = new HashMap<>();
  @JsonIgnore // explictly ignoring as Getter visibility is ANY for auto-json serialization of Trigger based on getters
  private Future<Boolean> returnEventFuture;

  public WmContext(final long queryStartTime, final String queryId) {
    this.queryStartTime = queryStartTime;
    this.queryId = queryId;
    this.queryCompleted = false;
  }

  public Set<Trigger> getAppliedTriggers() {
    return appliedTriggers;
  }

  public void addTriggers(final List<Trigger> triggers) {
    if (triggers != null) {
      this.appliedTriggers.addAll(triggers);
      // reset and add counters. This can happen during start of query or a session being moved to another pool with its
      // own set of triggers
      Set<String> counters = new HashSet<>();
      for (Trigger trigger : triggers) {
        counters.add(trigger.getExpression().getCounterLimit().getName());
      }
      addSubscribedCounters(counters);
    }
  }

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(final String queryId) {
    this.queryId = queryId;
  }

  public Set<String> getSubscribedCounters() {
    return subscribedCounters;
  }

  public void setSubscribedCounters(final Set<String> subscribedCounters) {
    this.subscribedCounters = subscribedCounters;
  }

  public void addSubscribedCounters(final Set<String> moreCounters) {
    if (subscribedCounters == null) {
      subscribedCounters = new HashSet<>();
    }
    subscribedCounters.addAll(moreCounters);
  }

  public Map<String, Long> getCurrentCounters() {
    return currentCounters;
  }

  public void setCurrentCounters(final Map<String, Long> currentCounters) {
    this.currentCounters = currentCounters;
  }

  public long getElapsedTime() {
    return System.currentTimeMillis() - queryStartTime;
  }

  public boolean isQueryCompleted() {
    return queryCompleted;
  }

  public void setQueryCompleted(final boolean queryCompleted) {
    this.queryCompleted = queryCompleted;
    this.queryEndTime = System.currentTimeMillis();
  }

  public void addWMEvent(WmEvent wmEvent) {
    queryWmEvents.add(wmEvent);
  }

  public long getQueryStartTime() {
    return queryStartTime;
  }

  public long getQueryEndTime() {
    return queryEndTime;
  }

  List<WmEvent> getQueryWmEvents() {
    return queryWmEvents;
  }

  Future<Boolean> getReturnEventFuture() {
    return returnEventFuture;
  }

  public void setReturnEventFuture(final Future<Boolean> returnEventFuture) {
    this.returnEventFuture = returnEventFuture;
  }

  private static final String WM_EVENTS_HEADER_FORMAT = "%7s %24s %24s %11s %9s %13s";
  private static final String WM_EVENTS_TITLE = "Workload Manager Events Summary";
  private static final String WM_EVENTS_TABLE_HEADER = String.format(WM_EVENTS_HEADER_FORMAT,
    "EVENT", "START_TIMESTAMP", "END_TIMESTAMP", "ELAPSED_MS", "CLUSTER %", "POOL");
  private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#0.00");

  @Override
  public void print(final SessionState.LogHelper console) {
    try {
      waitForReturnSessionEvent();
      boolean first = false;
      console.printInfo("");
      console.printInfo(WM_EVENTS_TITLE);

      for (final WmEvent wmEvent : queryWmEvents) {
        if (!first) {
          console.printInfo("");
          console.printInfo("QueryId: " + queryId);
          console.printInfo("SessionId: " + queryWmEvents.get(0).getWmTezSessionInfo().getSessionId());
          console.printInfo("Applied Triggers: " + getAppliedTriggers());
          console.printInfo(SEPARATOR);
          console.printInfo(WM_EVENTS_TABLE_HEADER);
          console.printInfo(SEPARATOR);
          first = true;
        }
        WmEvent.WmTezSessionInfo wmTezSessionInfo = wmEvent.getWmTezSessionInfo();
        String row = String.format(WM_EVENTS_HEADER_FORMAT,
          wmEvent.getEventType(),
          Instant.ofEpochMilli(wmEvent.getEventStartTimestamp()).toString(),
          Instant.ofEpochMilli(wmEvent.getEventEndTimestamp()).toString(),
          wmEvent.getElapsedTime(),
          DECIMAL_FORMAT.format(wmTezSessionInfo.getClusterPercent()),
          wmTezSessionInfo.getPoolName());
        console.printInfo(row);
      }
      console.printInfo(SEPARATOR);
      console.printInfo("");
    } catch (Exception e) {
      LOG.warn("Unable to print WM events summary", e);
    }
  }

  // TODO: expose all WMContext's via /jmx to use in UI
  public void printJson(final SessionState.LogHelper console) {
    try {
      waitForReturnSessionEvent();
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
      // serialize json based on field annotations only
      objectMapper.setVisibilityChecker(objectMapper.getSerializationConfig().getDefaultVisibilityChecker()
        .withSetterVisibility(JsonAutoDetect.Visibility.NONE));
      String wmContextJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
      console.printInfo("");
      console.printInfo(WM_EVENTS_TITLE);
      console.printInfo(SEPARATOR);
      console.printInfo(wmContextJson);
      console.printInfo(SEPARATOR);
      console.printInfo("");
    } catch (Exception e) {
      LOG.warn("Unable to serialize WMContext to json.", e);
    }
  }

  private void waitForReturnSessionEvent() throws ExecutionException, InterruptedException {
    if (getReturnEventFuture() != null && !Thread.currentThread().isInterrupted()) {
      getReturnEventFuture().get();
    }
  }

  // prints short events information that are safe for consistent testing
  public void shortPrint(final SessionState.LogHelper console) throws ExecutionException, InterruptedException {
    waitForReturnSessionEvent();
    console.printInfo(WmContext.WM_EVENTS_TITLE, false);
    for (WmEvent wmEvent : getQueryWmEvents()) {
      console.printInfo("Event: " + wmEvent.getEventType() +
        " Pool: " + wmEvent.getWmTezSessionInfo().getPoolName() +
        " Cluster %: " + WmContext.DECIMAL_FORMAT.format(wmEvent.getWmTezSessionInfo().getClusterPercent()));
    }
  }

  public void updateElapsedTimeCounter() {
    if (subscribedCounters.contains(TimeCounterLimit.TimeCounter.ELAPSED_TIME.name())) {
      currentCounters.put(TimeCounterLimit.TimeCounter.ELAPSED_TIME.name(), getElapsedTime());
    }
  }
}
