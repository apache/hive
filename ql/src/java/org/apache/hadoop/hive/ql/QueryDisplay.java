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

import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskResult;
import org.apache.hadoop.hive.ql.plan.api.StageType;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.RunningJob;
import org.json.JSONException;
import org.json.JSONObject;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Some limited query information to save for WebUI.
 *
 * The class is synchronized, as WebUI may access information about a running query.
 */
@JsonAutoDetect(fieldVisibility = Visibility.ANY, getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class QueryDisplay {

  /**
   * Preferred objectMapper for this class.
   *
   * It must be used to have things work in shaded environment (and its also more performant).
   */
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

  public synchronized void updateTaskStatus(Task<?> tTask) {
    if (!tasks.containsKey(tTask.getId())) {
      tasks.put(tTask.getId(), new TaskDisplay(tTask));
    }
    tasks.get(tTask.getId()).updateStatus(tTask);
  }

  public synchronized void updateTaskStatistics(MapRedStats mapRedStats,
      RunningJob rj, String taskId) throws IOException, JSONException {
    if (tasks.containsKey(taskId)) {
      tasks.get(taskId).updateMapRedStatsJson(mapRedStats, rj);
    }
  }

  //Inner classes
  public enum Phase {
    COMPILATION,
    EXECUTION,
  }

  @JsonIgnore
  public String getFullLogLocation() {
    return LogUtils.getLogFilePath();
  }

  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonAutoDetect(fieldVisibility = Visibility.ANY, getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
  public static class TaskDisplay {

    public static final String NUMBER_OF_MAPPERS = "Number of Mappers";
    public static final String NUMBER_OF_REDUCERS = "Number of Reducers";
    public static final String COUNTERS = "Counters";
    public static final String JOB_ID = "Job Id";
    public static final String JOB_FILE = "Job File";
    public static final String TRACKING_URL = "Tracking URL";
    public static final String MAP_PROGRESS = "Map Progress (%)";
    public static final String REDUCE_PROGRESS = "Reduce Progress (%)";
    public static final String CLEANUP_PROGRESS = "Cleanup Progress (%)";
    public static final String SETUP_PROGRESS = "Setup Progress (%)";
    public static final String COMPLETE = "Complete";
    public static final String SUCCESSFUL = "Successful";

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
    private JSONObject statsJSON;

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

    private void updateMapRedStatsJson(MapRedStats stats, RunningJob rj) throws IOException, JSONException {
      if (statsJSON == null) {
        statsJSON = new JSONObject();
      }
      if (stats != null) {
        if (stats.getNumMap() >= 0) {
          statsJSON.put(NUMBER_OF_MAPPERS, stats.getNumMap());
        }
        if (stats.getNumReduce() >= 0) {
          statsJSON.put(NUMBER_OF_REDUCERS, stats.getNumReduce());
        }
        if (stats.getCounters() != null) {
          statsJSON.put(COUNTERS, getCountersJson(stats.getCounters()));
        }
      }
      if (rj != null) {
        statsJSON.put(JOB_ID, rj.getID().toString());
        statsJSON.put(JOB_FILE, rj.getJobFile());
        statsJSON.put(TRACKING_URL, rj.getTrackingURL());
        statsJSON.put(MAP_PROGRESS, Math.round(rj.mapProgress() * 100));
        statsJSON.put(REDUCE_PROGRESS, Math.round(rj.reduceProgress() * 100));
        statsJSON.put(CLEANUP_PROGRESS, Math.round(rj.cleanupProgress() * 100));
        statsJSON.put(SETUP_PROGRESS, Math.round(rj.setupProgress() * 100));
        statsJSON.put(COMPLETE, rj.isComplete());
        statsJSON.put(SUCCESSFUL, rj.isSuccessful());
      }
    }

    public synchronized String getStatsJsonString() {
      if (statsJSON != null) {
        return statsJSON.toString();
      }
      return null;
    }

    private JSONObject getCountersJson(Counters ctrs) throws JSONException {
      JSONObject countersJson = new JSONObject();
      Iterator<Counters.Group> iterator = ctrs.iterator();
      while(iterator.hasNext()) {
        Counters.Group group = iterator.next();
        Iterator<Counters.Counter> groupIterator = group.iterator();
        JSONObject groupJson = new JSONObject();
        while(groupIterator.hasNext()) {
          Counters.Counter counter = groupIterator.next();
          groupJson.put(counter.getDisplayName(), counter.getCounter());
        }
        countersJson.put(group.getDisplayName(), groupJson);
      }
      return countersJson;
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

    public void updateStatus(Task<?> tTask) {
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

  @JsonIgnore
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
   * @return map of HMS Client method-calls and duration in milliseconds, during given phase.
   */
  public synchronized Map<String, Long> getHmsTimings(Phase phase) {
    return hmsTimingMap.get(phase);
  }

  /**
   * @param phase phase of query
   * @param hmsTimings map of HMS Client method-calls and duration in milliseconds, during given phase.
   */
  public synchronized void setHmsTimings(Phase phase, Map<String, Long> hmsTimings) {
    hmsTimingMap.put(phase, hmsTimings);
  }

  /**
   * @param phase phase of query
   * @return map of PerfLogger call-trace name and start time in milliseconds, during given phase.
   */
  public synchronized Map<String, Long> getPerfLogStarts(Phase phase) {
    return perfLogStartMap.get(phase);
  }

  /**
   * @param phase phase of query
   * @param perfLogStarts map of PerfLogger call-trace name and start time in milliseconds, during given phase.
   */
  public synchronized void setPerfLogStarts(Phase phase, Map<String, Long> perfLogStarts) {
    perfLogStartMap.put(phase, perfLogStarts);
  }

  /**
   * @param phase phase of query
   * @return map of PerfLogger call-trace name and end time in milliseconds, during given phase.
   */
  public synchronized Map<String, Long> getPerfLogEnds(Phase phase) {
    return perfLogEndMap.get(phase);
  }

  /**
   * @param phase phase of query
   * @param perfLogEnds map of PerfLogger call-trace name and end time in milliseconds, during given phase.
   */
  public synchronized void setPerfLogEnds(Phase phase, Map<String, Long> perfLogEnds) {
    perfLogEndMap.put(phase, perfLogEnds);
  }

  /**
   * @param phase phase of query
   * @return map of PerfLogger call-trace name and duration in milliseconds, during given phase.
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

  private static String returnStringOrUnknown(String s) {
    return s == null ? "UNKNOWN" : s;
  }

  public synchronized long getQueryStartTime() {
    return queryStartTime;
  }
}
