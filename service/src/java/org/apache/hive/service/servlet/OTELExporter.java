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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.internal.AttributesMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.OTELJavaMetrics;
import org.apache.hadoop.hive.ql.QueryDisplay;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.session.SessionManager;

public class OTELExporter extends Thread {
  private static final String QUERY_SCOPE = OTELExporter.class.getName();
  private static final String JVM_SCOPE = OTELJavaMetrics.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(OTELExporter.class);
  private final OperationManager operationManager;
  private final Set<String> historicalQueryId;
  private final long frequency;
  private final Tracer tracer;
  private final Map<String, Span> queryIdToSpanMap;
  private final Map<String, Set<String>> queryIdToTasksMap;
  private final OTELJavaMetrics jvmMetrics;

  public OTELExporter(OpenTelemetry openTelemetry, SessionManager sessionManager, long frequency, String addr) {
    this.tracer = openTelemetry.getTracer(QUERY_SCOPE + ":" + addr);
    this.jvmMetrics = new OTELJavaMetrics(openTelemetry.getMeter(JVM_SCOPE + ":" + addr));
    this.operationManager = sessionManager.getOperationManager();
    this.historicalQueryId = new HashSet<>();
    this.frequency = frequency;
    this.queryIdToSpanMap = new HashMap<>();
    this.queryIdToTasksMap = new HashMap<>();
  }

  @Override
  public void run() {
    while (true) {
      try {
        jvmMetrics.setJvmMetrics();
        exposeMetricsToOTEL();
      } catch (Throwable e) {
        LOG.error("Exception occurred in OTELExporter thread ", e);
      }
      
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

    for (QueryInfo lQuery : liveQueries) {
      if (lQuery.getQueryDisplay() == null || StringUtils.isEmpty(lQuery.getQueryDisplay().getQueryId())) {
        continue;
      }
      String queryID = lQuery.getQueryDisplay().getQueryId();
      Span rootspan = queryIdToSpanMap.get(queryID);

      //In case of live query previously encountered in past loops
      if (rootspan != null) {
        for (QueryDisplay.TaskDisplay task : lQuery.getQueryDisplay().getTaskDisplays()) {
          if (task.getReturnValue() != null && task.getEndTime() != null
                  && queryIdToTasksMap.get(queryID).add(task.getTaskId())) {
            Context parentContext = Context.current().with(rootspan);
            tracer.spanBuilder(queryID + " - " + task.getTaskId())
                    .setParent(parentContext).setAllAttributes(addTaskAttributes(task))
                    .setStartTimestamp(task.getBeginTime(), TimeUnit.MILLISECONDS).startSpan()
                    .end(task.getEndTime(), TimeUnit.MILLISECONDS);
          }
        }
      } else {
        // In case of live queries being seen for first time and has initialized its queryDisplay
        rootspan = tracer.spanBuilder(queryID)
                .setStartTimestamp(lQuery.getBeginTime(), TimeUnit.MILLISECONDS).startSpan();
        Set<String> completedTasks = new HashSet<>();
        Context parentContext = Context.current().with(rootspan);

        Span initSpan = tracer.spanBuilder(queryID).setParent(parentContext)
                .setStartTimestamp(lQuery.getBeginTime(), TimeUnit.MILLISECONDS).startSpan()
                .setAttribute("QueryId", queryID)
                .setAttribute("QueryString", lQuery.getQueryDisplay().getQueryString())
                .setAttribute("UserName", lQuery.getUserName())
                .setAttribute("ExecutionEngine", lQuery.getExecutionEngine());
        if (lQuery.getQueryDisplay().getErrorMessage() != null) {
          initSpan.setAttribute("ErrorMessage", lQuery.getQueryDisplay().getErrorMessage());
        }
        initSpan.end(lQuery.getBeginTime(), TimeUnit.MILLISECONDS);

        for (QueryDisplay.TaskDisplay task : lQuery.getQueryDisplay().getTaskDisplays()) {
          if (task.getReturnValue() != null && task.getEndTime() != null) {
            completedTasks.add(task.getTaskId());
            parentContext = Context.current().with(rootspan);
            tracer.spanBuilder(queryID + " - " + task.getTaskId())
                    .setParent(parentContext).setAllAttributes(addTaskAttributes(task))
                    .setStartTimestamp(task.getBeginTime(), TimeUnit.MILLISECONDS).startSpan()
                    .end(task.getEndTime(), TimeUnit.MILLISECONDS);
          }
        }
        
        queryIdToSpanMap.put(queryID, rootspan);
        queryIdToTasksMap.put(queryID, completedTasks);
      }
    }

    Set<String> historicalQueryIDs = new HashSet<>();
    for (QueryInfo hQuery : historicalQueries) {
      if (hQuery.getEndTime() != null) {
        String hQueryId = hQuery.getQueryDisplay().getQueryId();
        historicalQueryIDs.add(hQueryId);
        Span rootspan = queryIdToSpanMap.remove(hQueryId);
        Set<String> completedTasks = queryIdToTasksMap.remove(hQueryId);

        //For queries that were live till last loop but have ended before start of this loop
        if (rootspan != null) {
          for (QueryDisplay.TaskDisplay task : hQuery.getQueryDisplay().getTaskDisplays()) {
            if (!completedTasks.contains(task.getTaskId())) {
              Context parentContext = Context.current().with(rootspan);
              tracer.spanBuilder(hQueryId + " - " + task.getTaskId())
                      .setParent(parentContext).setAllAttributes(addTaskAttributes(task))
                      .setStartTimestamp(task.getBeginTime(), TimeUnit.MILLISECONDS).startSpan()
                      .end(task.getEndTime(), TimeUnit.MILLISECONDS);
            }
          }
  
          //Update the rootSpan name & attributes before ending it
          rootspan.updateName(hQueryId + " - completed").setAllAttributes(addQueryAttributes(hQuery))
                  .end(hQuery.getEndTime(), TimeUnit.MILLISECONDS);
          historicalQueryId.add(hQueryId);
        }

        //For queries that already ended either before OTEL service started or in between OTEL loops
        if (historicalQueryId.add(hQueryId)) {
          rootspan = tracer.spanBuilder(hQueryId + " - completed")
                  .setStartTimestamp(hQuery.getBeginTime(), TimeUnit.MILLISECONDS).startSpan();
          Context parentContext = Context.current().with(rootspan);
          
          Span initSpan = tracer.spanBuilder(hQueryId).setParent(parentContext)
                  .setStartTimestamp(hQuery.getBeginTime(), TimeUnit.MILLISECONDS).startSpan()
                  .setAttribute("QueryId", hQueryId)
                  .setAttribute("QueryString", hQuery.getQueryDisplay().getQueryString())
                  .setAttribute("UserName", hQuery.getUserName())
                  .setAttribute("ExecutionEngine", hQuery.getExecutionEngine());
          if (hQuery.getQueryDisplay().getErrorMessage() != null) {
            initSpan.setAttribute("ErrorMessage", hQuery.getQueryDisplay().getErrorMessage());
          }
          initSpan.end(hQuery.getBeginTime(), TimeUnit.MILLISECONDS);

          for (QueryDisplay.TaskDisplay task : hQuery.getQueryDisplay().getTaskDisplays()) {
            parentContext = Context.current().with(rootspan);
            tracer.spanBuilder(hQueryId + " - " + task.getTaskId())
                    .setParent(parentContext).setAllAttributes(addTaskAttributes(task))
                    .setStartTimestamp(task.getBeginTime(), TimeUnit.MILLISECONDS).startSpan()
                    .end(task.getEndTime(), TimeUnit.MILLISECONDS);
          }

          rootspan.setAllAttributes(addQueryAttributes(hQuery)).end(hQuery.getEndTime(), TimeUnit.MILLISECONDS);
        }
      }
    }
    
    historicalQueryId.retainAll(historicalQueryIDs);
  }

  private AttributesMap addQueryAttributes(QueryInfo query){
    AttributesMap attributes = AttributesMap.create(Long.MAX_VALUE, Integer.MAX_VALUE);
    attributes.put(AttributeKey.stringKey("QueryId"), query.getQueryDisplay().getQueryId());
    attributes.put(AttributeKey.longKey("QueryStartTime"), query.getQueryDisplay().getQueryStartTime());
    attributes.put(AttributeKey.longKey("EndTime"), query.getEndTime());
    attributes.put(AttributeKey.stringKey("OperationId"), query.getOperationId());
    attributes.put(AttributeKey.stringKey("OperationLogLocation"), query.getOperationLogLocation());
    attributes.put(AttributeKey.stringKey("ErrorMessage"), query.getQueryDisplay().getErrorMessage());
    attributes.put(AttributeKey.stringKey("ExplainPlan"), query.getQueryDisplay().getExplainPlan());
    attributes.put(AttributeKey.stringKey("FullLogLocation"), query.getQueryDisplay().getFullLogLocation());
    attributes.put(AttributeKey.stringKey("Running"), String.valueOf(query.isRunning()));
    attributes.put(AttributeKey.longKey("Runtime"), query.getRuntime());
    attributes.put(AttributeKey.stringKey("UserName"), query.getUserName());
    attributes.put(AttributeKey.stringKey("State"), query.getState());
    attributes.put(AttributeKey.stringKey("SessionId"), query.getSessionId());
    attributes.put(AttributeKey.stringKey("FScounters"), query.getCounters().toString());
    return attributes;
  }

  private AttributesMap addTaskAttributes(QueryDisplay.TaskDisplay taskDisplay) {
    AttributesMap attributes = AttributesMap.create(Long.MAX_VALUE, Integer.MAX_VALUE);
    attributes.put(AttributeKey.stringKey("TaskId"), taskDisplay.getTaskId());
    attributes.put(AttributeKey.stringKey("Name"), taskDisplay.getName());
    if(taskDisplay.getTaskType() != null){
      attributes.put(AttributeKey.stringKey("TaskType"), taskDisplay.getTaskType().toString());
    }
    attributes.put(AttributeKey.stringKey("Status"), taskDisplay.getStatus());
    attributes.put(AttributeKey.stringKey("StatusMessage"), taskDisplay.getStatusMessage());
    attributes.put(AttributeKey.stringKey("ExternalHandle"), taskDisplay.getExternalHandle());
    attributes.put(AttributeKey.stringKey("ErrorMsg"), taskDisplay.getErrorMsg());
    if(taskDisplay.getReturnValue() != null ){
      attributes.put(AttributeKey.longKey("ReturnValue"), taskDisplay.getReturnValue().longValue());
    }
    attributes.put(AttributeKey.longKey("BeginTime"), taskDisplay.getBeginTime());
    attributes.put(AttributeKey.longKey("ElapsedTime"), taskDisplay.getElapsedTime());
    attributes.put(AttributeKey.longKey("EndTime"), taskDisplay.getEndTime());
    return attributes;
  }
}
