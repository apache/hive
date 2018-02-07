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

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskResult;
import org.apache.hadoop.hive.ql.plan.api.StageType;

import java.io.Serializable;
import java.util.*;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonWriteNullProperties;
import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * Some limited query information to save for WebUI.
 *
 * The class is synchronized, as WebUI may access information about a running query.
 */
public class QueryDisplay {

  // Member variables
  private String queryStr;
  private String explainPlan;
  private String errorMessage;
  private String queryId;
  private long queryStartTime = System.currentTimeMillis();

  private final Map<Phase, Map<String, Long>> hmsTimingMap = new HashMap<Phase, Map<String, Long>>();
  private final Map<Phase, Map<String, Long>> perfLogStartMap = new HashMap<Phase, Map<String, Long>>();
  private final Map<Phase, Map<String, Long>> perfLogEndMap = new HashMap<Phase, Map<String, Long>>();

  private final LinkedHashMap<String, TaskDisplay> tasks = new LinkedHashMap<String, TaskDisplay>();

  public synchronized <T extends Serializable> void updateTaskStatus(Task<T> tTask) {
    if (!tasks.containsKey(tTask.getId())) {
      tasks.put(tTask.getId(), new TaskDisplay(tTask));
    }
    tasks.get(tTask.getId()).updateStatus(tTask);
  }

  //Inner classes
  public enum Phase {
    COMPILATION,
    EXECUTION,
  }

  @JsonWriteNullProperties(false)
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class TaskDisplay {

    private Integer returnValue;  //if set, determines that task is complete.
    private String errorMsg;

    private Long beginTime;
    private Long endTime;

    private String taskId;
    private String externalHandle;

    public Task.TaskState taskState;
    private StageType taskType;
    private String name;
    private boolean requireLock;
    private String statusMessage;

    // required for jackson
    public TaskDisplay() {

    }
    public TaskDisplay(Task task) {
      taskId = task.getId();
      externalHandle = task.getExternalHandle();
      taskType = task.getType();
      name = task.getName();
      requireLock = task.requireLock();
    }
    @JsonIgnore
    public synchronized String getStatus() {
      if (returnValue == null) {
        return "Running";
      } else if (returnValue == 0) {
        return "Success, ReturnVal 0";
      } else {
        return "Failure, ReturnVal " + String.valueOf(returnValue);
      }
    }

    public synchronized Long getElapsedTime() {
      if (endTime == null) {
        if (beginTime == null) {
          return null;
        }
        return System.currentTimeMillis() - beginTime;
      } else {
        return endTime - beginTime;
      }
    }

    public synchronized Integer getReturnValue() {
      return returnValue;
    }

    public synchronized String getErrorMsg() {
      return errorMsg;
    }

    public synchronized Long getBeginTime() {
      return beginTime;
    }

    public synchronized Long getEndTime() {
      return endTime;
    }

    public synchronized String getTaskId() {
      return taskId;
    }

    public synchronized StageType getTaskType() {
      return taskType;
    }

    public synchronized String getName() {
      return name;
    }
    @JsonIgnore
    public synchronized boolean isRequireLock() {
      return requireLock;
    }

    public synchronized String getExternalHandle() {
      return externalHandle;
    }

    public synchronized <T extends Serializable> void updateStatus(Task<T> tTask) {
      this.taskState = tTask.getTaskState();
      if (externalHandle == null && tTask.getExternalHandle() != null) {
        this.externalHandle = tTask.getExternalHandle();
      }
      setStatusMessage(tTask.getStatusMessage());
      switch (taskState) {
        case RUNNING:
          if (beginTime == null) {
            beginTime = System.currentTimeMillis();
          }
          break;
        case FINISHED:
          if (endTime == null) {
            endTime = System.currentTimeMillis();
          }
          break;
      }
    }

    public synchronized String getStatusMessage() {
      return statusMessage;
    }

    public synchronized void setStatusMessage(String statusMessage) {
      this.statusMessage = statusMessage;
    }
  }
  public synchronized void setTaskResult(String taskId, TaskResult result) {
    TaskDisplay taskDisplay = tasks.get(taskId);
    if (taskDisplay != null) {
      taskDisplay.returnValue = result.getExitVal();
      if (result.getTaskError() != null) {
        taskDisplay.errorMsg = result.getTaskError().toString();
      }
    }
  }
  public synchronized List<TaskDisplay> getTaskDisplays() {
    List<TaskDisplay> taskDisplays = new ArrayList<TaskDisplay>();
    taskDisplays.addAll(tasks.values());
    return taskDisplays;
  }

  public synchronized void setQueryStr(String queryStr) {
    this.queryStr = queryStr;
  }

  public synchronized String getQueryString() {
    return returnStringOrUnknown(queryStr);
  }

  public synchronized String getExplainPlan() {
    return returnStringOrUnknown(explainPlan);
  }

  public synchronized void setExplainPlan(String explainPlan) {
    this.explainPlan = explainPlan;
  }

  /**
   * @param phase phase of query
   * @return map of HMS Client method-calls and duration in miliseconds, during given phase.
   */
  public synchronized Map<String, Long> getHmsTimings(Phase phase) {
    return hmsTimingMap.get(phase);
  }

  /**
   * @param phase phase of query
   * @param hmsTimings map of HMS Client method-calls and duration in miliseconds, during given phase.
   */
  public synchronized void setHmsTimings(Phase phase, ImmutableMap<String, Long> hmsTimings) {
    hmsTimingMap.put(phase, hmsTimings);
  }

  /**
   * @param phase phase of query
   * @return map of PerfLogger call-trace name and start time in miliseconds, during given phase.
   */
  public synchronized Map<String, Long> getPerfLogStarts(Phase phase) {
    return perfLogStartMap.get(phase);
  }

  /**
   * @param phase phase of query
   * @param perfLogStarts map of PerfLogger call-trace name and start time in miliseconds, during given phase.
   */
  public synchronized void setPerfLogStarts(Phase phase, ImmutableMap<String, Long> perfLogStarts) {
    perfLogStartMap.put(phase, perfLogStarts);
  }

  /**
   * @param phase phase of query
   * @return map of PerfLogger call-trace name and end time in miliseconds, during given phase.
   */
  public synchronized Map<String, Long> getPerfLogEnds(Phase phase) {
    return perfLogEndMap.get(phase);
  }

  /**
   * @param phase phase of query
   * @param perfLogEnds map of PerfLogger call-trace name and end time in miliseconds, during given phase.
   */
   public synchronized void setPerfLogEnds(Phase phase, ImmutableMap<String, Long> perfLogEnds) {
    perfLogEndMap.put(phase, perfLogEnds);
  }

  /**
   * @param phase phase of query
   * @return map of PerfLogger call-trace name and duration in miliseconds, during given phase.
   */
  public synchronized Map<String, Long> getPerfLogTimes(Phase phase) {
    Map<String, Long> times = new HashMap<>();
    Map<String, Long> startTimes = perfLogStartMap.get(phase);
    Map<String, Long> endTimes = perfLogEndMap.get(phase);
    if (endTimes != null && startTimes != null) {
      for (String timeKey : endTimes.keySet()) {
        Long endTime = endTimes.get(timeKey);
        Long startTime = startTimes.get(timeKey);
        if (startTime != null) {
          times.put(timeKey, endTime - startTime);
        }
      }
    }
    return times;
  }

  public synchronized String getErrorMessage() {
    return errorMessage;
  }

  public synchronized void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public synchronized String getQueryId() {
    return returnStringOrUnknown(queryId);
  }

  public synchronized void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  private String returnStringOrUnknown(String s) {
    return s == null ? "UNKNOWN" : s;
  }

  public long getQueryStartTime() {
    return queryStartTime;
  }
}
