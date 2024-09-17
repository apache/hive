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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
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
  private final OperationManager operationManager;
  private Set<String> historicalQueryId;
  private final long frequency;
  private final Tracer tracer;
  private Map<String, Span> queryIdToSpanMap;
  private Map<String, List<String>> queryIdToTasksMap;

  public OTELExporter(OpenTelemetry openTelemetry, SessionManager sessionManager, long frequency) {
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_NAME);
    this.operationManager = sessionManager.getOperationManager();
    this.historicalQueryId = new HashSet<>();
    this.frequency = frequency;
    this.queryIdToSpanMap = new HashMap<>();
    this.queryIdToTasksMap = new HashMap<>();
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

    for (QueryInfo lQuery: liveQueries){

      while (lQuery.getQueryDisplay() == null) {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      String queryID = lQuery.getQueryDisplay().getQueryId();

      //In case of live query previously encountered in past loops
      if(queryIdToSpanMap.containsKey(queryID)){
        Span rootspan = queryIdToSpanMap.get(queryID);

        for (QueryDisplay.TaskDisplay task : lQuery.getQueryDisplay().getTaskDisplays()) {
          if(task.getReturnValue() != null && (queryIdToTasksMap.get(queryID) == null || !queryIdToTasksMap.get(queryID).contains(task.getTaskId()))){
            queryIdToTasksMap.get(queryID).add(task.getTaskId());
            Context parentContext = Context.current().with(rootspan);
            Span currSpan = tracer.spanBuilder(queryID+ " - " + task.getTaskId() + " - live").setParent(parentContext).setAllAttributes(addTaskAttributes(task))
                    .setStartTimestamp(task.getBeginTime(), TimeUnit.MILLISECONDS).startSpan();
            while (task.getEndTime() == null){
              try {
                Thread.sleep(500);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            }
            currSpan.end(task.getEndTime(), TimeUnit.MILLISECONDS);
          }
        }
      } else {
        // In case of live queries being seen for first time
        Span rootspan = tracer.spanBuilder(queryID + " - live")
                .startSpan();
        List<String> completedTasks = new ArrayList<>();
        Context parentContext = Context.current().with(rootspan);
        Span initSpan = tracer.spanBuilder(queryID + " - live").setParent(parentContext).startSpan()
                .setAttribute("queryID", lQuery.getQueryDisplay().getQueryId())
                .setAttribute("queryString", lQuery.getQueryDisplay().getQueryString())
                .setAttribute("Begin Time", lQuery.getBeginTime());
        if(lQuery.getQueryDisplay().getErrorMessage() != null){
          initSpan.setAttribute("Error Message", lQuery.getQueryDisplay().getErrorMessage());
        }
        initSpan.end();
        for (QueryDisplay.TaskDisplay task : lQuery.getQueryDisplay().getTaskDisplays()) {
          if (task.getReturnValue() != null) {
            completedTasks.add(task.getTaskId());
            parentContext = Context.current().with(rootspan);
            Span currSpan = tracer.spanBuilder(queryID + " - " + task.getTaskId() + " - live").setParent(parentContext).setAllAttributes(addTaskAttributes(task))
                    .setStartTimestamp(task.getBeginTime(), TimeUnit.MILLISECONDS).startSpan();

            while (task.getEndTime() == null) {
              try {
                Thread.sleep(500);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            }
            currSpan.end(task.getEndTime(), TimeUnit.MILLISECONDS);
          }
        }

        if(rootspan != null){
          queryIdToSpanMap.put(queryID,rootspan);
          queryIdToTasksMap.put(queryID,completedTasks);
        }

      }
    }


    for (QueryInfo hQuery : historicalQueries) {

      //For queries that were live till last loop but have ended before start of this loop
      if(queryIdToSpanMap.containsKey(hQuery.getQueryDisplay().getQueryId())){
        String hQueryId = hQuery.getQueryDisplay().getQueryId();
        Span rootspan = queryIdToSpanMap.get(hQueryId);
        for (QueryDisplay.TaskDisplay task : hQuery.getQueryDisplay().getTaskDisplays()) {
          if(queryIdToTasksMap.get(hQueryId) == null || !queryIdToTasksMap.get(hQueryId).contains(task.getTaskId())){
            queryIdToTasksMap.get(hQueryId).add(task.getTaskId());
            Context parentContext = Context.current().with(rootspan);
            Span currSpan = tracer.spanBuilder(hQueryId+ " - " + task.getTaskId() + " - completed").setParent(parentContext).setAllAttributes(addTaskAttributes(task))
                    .setStartTimestamp(task.getBeginTime(), TimeUnit.MILLISECONDS).startSpan();
            currSpan.end(task.getEndTime(), TimeUnit.MILLISECONDS);
          }
        }

        //Update the rootSpan name & attributes before ending it
        queryIdToSpanMap.get(hQueryId).updateName(hQueryId + " - completed");
        queryIdToSpanMap.get(hQueryId).setAllAttributes(addQueryAttributes(hQuery));
        queryIdToSpanMap.get(hQueryId).end();

        queryIdToSpanMap.remove(hQueryId);
        queryIdToTasksMap.remove(hQueryId);

        historicalQueryId.add(hQueryId);
      }

      //For queries that were already either before OTEL service started or in between OTEL loops
      if (!historicalQueryId.contains(hQuery.getQueryDisplay().getQueryId())) {
        historicalQueryId.add(hQuery.getQueryDisplay().getQueryId());
        Span rootSpan = tracer.spanBuilder(hQuery.getQueryDisplay().getQueryId() + " - completed")
                .startSpan();
        Context parentContext = Context.current().with(rootSpan);
        Span initSpan = tracer.spanBuilder(hQuery.getQueryDisplay().getQueryId() + " - completed").setParent(parentContext).startSpan()
                .setAttribute("queryID", hQuery.getQueryDisplay().getQueryId())
                .setAttribute("queryString", hQuery.getQueryDisplay().getQueryString())
                .setAttribute("Begin Time", hQuery.getBeginTime());
        if(hQuery.getQueryDisplay().getErrorMessage() != null){
          initSpan.setAttribute("Error Message", hQuery.getQueryDisplay().getErrorMessage());
        }
        initSpan.end();
        for (QueryDisplay.TaskDisplay task : hQuery.getQueryDisplay().getTaskDisplays()) {
          parentContext = Context.current().with(rootSpan);
          Span currSpan = tracer.spanBuilder(hQuery.getQueryDisplay().getQueryId()+ " - " + task.getTaskId() + " - completed").setParent(parentContext).setAllAttributes(addTaskAttributes(task))
                  .setStartTimestamp(task.getBeginTime(), TimeUnit.MILLISECONDS).startSpan();
          currSpan.end(task.getEndTime(), TimeUnit.MILLISECONDS);
        }
        rootSpan.setAllAttributes(addQueryAttributes(hQuery)).end();
      }

    }
  }

  private AttributesMap addQueryAttributes(QueryInfo query){
    AttributesMap attributes = AttributesMap.create(Long.MAX_VALUE, Integer.MAX_VALUE);
    attributes.put(AttributeKey.stringKey("queryId"), query.getQueryDisplay().getQueryId());
    attributes.put(AttributeKey.stringKey("QueryString"), query.getQueryDisplay().getQueryString());
    attributes.put(AttributeKey.longKey("QueryStartTime"), query.getQueryDisplay().getQueryStartTime());
    attributes.put(AttributeKey.longKey("End Time"), query.getEndTime());
    attributes.put(AttributeKey.stringKey("Operation Id"), query.getOperationId());
    attributes.put(AttributeKey.stringKey("Operation Log Location"), query.getOperationLogLocation());
    attributes.put(AttributeKey.stringKey("Error Message"), query.getQueryDisplay().getErrorMessage());
    attributes.put(AttributeKey.stringKey("explainPlan"), query.getQueryDisplay().getExplainPlan());
    attributes.put(AttributeKey.stringKey("fullLogLocation"), query.getQueryDisplay().getFullLogLocation());
    attributes.put(AttributeKey.stringKey("taskDisplays"), Joiner.on("\t").join(query.getQueryDisplay().getTaskDisplays()));
    attributes.put(AttributeKey.stringKey("Running"), String.valueOf(query.isRunning()));
    attributes.put(AttributeKey.longKey("Runtime"), query.getRuntime());
    attributes.put(AttributeKey.stringKey("User Name"), query.getUserName());
    attributes.put(AttributeKey.stringKey("State"), query.getState());
    attributes.put(AttributeKey.stringKey("Session Id"), query.getSessionId());
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
