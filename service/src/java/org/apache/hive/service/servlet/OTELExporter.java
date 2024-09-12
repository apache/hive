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

package org.apache.hive.service.servlet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.internal.AttributesMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.ql.QueryDisplay;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.session.SessionManager;

public class OTELExporter extends Thread {

  private static final String INSTRUMENTATION_NAME = OTELExporter.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(OTELExporter.class);
  private final Tracer tracer;
  private final OperationManager operationManager;
  private final LongGauge liveQueryGauge;
  private Set<String> historicalQueryId;
  private final long frequency;

  public OTELExporter(OpenTelemetry openTelemetry, SessionManager sessionManager, long frequency) {
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_NAME);
    this.operationManager = sessionManager.getOperationManager();
    this.historicalQueryId = new HashSet<>();
    this.frequency = frequency;
    liveQueryGauge = openTelemetry.getMeter(INSTRUMENTATION_NAME).gaugeBuilder("LiveQueries")
        .setDescription("Number of Live Queries").ofLongs().build();
  }

  @Override
  public void run() {
    while (true) {
      exposeMetricsToOTEL();
      try {
        Thread.sleep(frequency);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void exposeMetricsToOTEL() {
    List<QueryInfo> liveQueries = operationManager.getLiveQueryInfos();
    List<QueryInfo> historicalQueries = operationManager.getHistoricalQueryInfos();

    LOG.debug("Found {} liveQueries and {} historicalQueries", liveQueries.size(), historicalQueries.size());

    liveQueryGauge.set(liveQueries.size());
    HashSet<String> currentHistoricalQueries = new HashSet<>();
    for (QueryInfo hQuery : historicalQueries) {
      currentHistoricalQueries.add(hQuery.getQueryDisplay().getQueryId());

      if (!historicalQueryId.contains(hQuery.getQueryDisplay().getQueryId())) {
        Span span = tracer.spanBuilder(hQuery.getQueryDisplay().getQueryId()).startSpan()
            .setAllAttributes(getAttributes(hQuery));

        for (QueryDisplay.TaskDisplay taskDisplay : hQuery.getQueryDisplay().getTaskDisplays()) {
          span.addEvent(taskDisplay.getName(), getTaskAttributes(taskDisplay), taskDisplay.getBeginTime(),
              TimeUnit.MILLISECONDS);
        }
        span.end(hQuery.getEndTime(), TimeUnit.MILLISECONDS);
      }
    }
    historicalQueryId = currentHistoricalQueries;
  }

  private AttributesMap getTaskAttributes(QueryDisplay.TaskDisplay taskDisplay) {
    AttributesMap attributes = AttributesMap.create(Long.MAX_VALUE, Integer.MAX_VALUE);
    attributes.put(AttributeKey.stringKey("TaskId"), taskDisplay.getTaskId());
    attributes.put(AttributeKey.stringKey("Name"), taskDisplay.getName());
    attributes.put(AttributeKey.stringKey("TaskType"), taskDisplay.getTaskType().toString());
    attributes.put(AttributeKey.longKey("Status"), taskDisplay.getStatus());
    attributes.put(AttributeKey.longKey("StatusMessage"), taskDisplay.getStatusMessage());
    attributes.put(AttributeKey.stringKey("ExternalHandle"), taskDisplay.getExternalHandle());
    attributes.put(AttributeKey.stringKey("ErrorMsg"), taskDisplay.getErrorMsg());
    attributes.put(AttributeKey.longKey("ReturnValue"), taskDisplay.getReturnValue().longValue());
    attributes.put(AttributeKey.longKey("BeginTime"), taskDisplay.getBeginTime());
    attributes.put(AttributeKey.longKey("ElapsedTime"), taskDisplay.getElapsedTime());
    attributes.put(AttributeKey.longKey("EndTime"), taskDisplay.getEndTime());
    return attributes;
  }

  public Attributes getAttributes(QueryInfo queryInfo) {
    AttributesMap attributes = AttributesMap.create(Long.MAX_VALUE, Integer.MAX_VALUE);
    attributes.put(AttributeKey.longKey("BeginTime"), queryInfo.getBeginTime());
    attributes.put(AttributeKey.longKey("ElapsedTime"), queryInfo.getElapsedTime());
    attributes.put(AttributeKey.longKey("EndTime"), queryInfo.getEndTime());
    attributes.put(AttributeKey.stringKey("ExecutionEngine"), queryInfo.getExecutionEngine());
    attributes.put(AttributeKey.stringKey("OperationId"), queryInfo.getOperationId());
    attributes.put(AttributeKey.stringKey("OperationLogLocation"), queryInfo.getOperationLogLocation());
    attributes.put(AttributeKey.stringKey("ErrorMessage"), queryInfo.getQueryDisplay().getErrorMessage());
    attributes.put(AttributeKey.stringKey("ExplainPlan"), queryInfo.getQueryDisplay().getExplainPlan());
    attributes.put(AttributeKey.stringKey("FullLogLocation"), queryInfo.getQueryDisplay().getFullLogLocation());
    attributes.put(AttributeKey.stringKey("QueryId"), queryInfo.getQueryDisplay().getQueryId());
    attributes.put(AttributeKey.longKey("QueryStartTime"), queryInfo.getQueryDisplay().getQueryStartTime());
    attributes.put(AttributeKey.stringKey("QueryString"), queryInfo.getQueryDisplay().getQueryString());
    attributes.put(AttributeKey.longKey("Running"), queryInfo.getRuntime());
    attributes.put(AttributeKey.stringKey("State"), queryInfo.getState());
    attributes.put(AttributeKey.stringKey("SessionId"), queryInfo.getSessionId());
    return attributes;
  }
}
