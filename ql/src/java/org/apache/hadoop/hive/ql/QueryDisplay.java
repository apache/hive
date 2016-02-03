/**
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

  private final Map<Phase, Map<String, Long>> hmsTimingMap = new HashMap();
  private final Map<Phase, Map<String, Long>> perfLogStartMap = new HashMap();
  private final Map<Phase, Map<String, Long>> perfLogEndMap = new HashMap();

  private final LinkedHashMap<String, TaskInfo> tasks = new LinkedHashMap<String, TaskInfo>();

  //Inner classes
  public static enum Phase {
    COMPILATION,
    EXECUTION,
  }

  public static class TaskInfo {
    private Integer returnVal;  //if set, determines that task is complete.
    private String errorMsg;
    private long endTime;

    final long beginTime;
    final String taskId;
    final StageType taskType;
    final String name;
    final boolean requireLock;
    final boolean retryIfFail;

    public TaskInfo (Task task) {
      beginTime = System.currentTimeMillis();
      taskId = task.getId();
      taskType = task.getType();
      name = task.getName();
      requireLock = task.requireLock();
      retryIfFail = task.ifRetryCmdWhenFail();
    }

    public synchronized String getStatus() {
      if (returnVal == null) {
        return "Running";
      } else if (returnVal == 0) {
        return "Success, ReturnVal 0";
      } else {
        return "Failure, ReturnVal " + String.valueOf(returnVal);
      }
    }

    public synchronized long getElapsedTime() {
      if (endTime == 0) {
        return System.currentTimeMillis() - beginTime;
      } else {
        return endTime - beginTime;
      }
    }

    public synchronized String getErrorMsg() {
      return errorMsg;
    }

    public synchronized long getEndTime() {
      return endTime;
    }

    //Following methods do not need to be synchronized, because they are final fields.
    public long getBeginTime() {
      return beginTime;
    }

    public String getTaskId() {
      return taskId;
    }

    public StageType getTaskType() {
      return taskType;
    }

    public String getName() {
      return name;
    }

    public boolean isRequireLock() {
      return requireLock;
    }

    public boolean isRetryIfFail() {
      return retryIfFail;
    }
  }

  public synchronized void addTask(Task task) {
    tasks.put(task.getId(), new TaskInfo(task));
  }

  public synchronized void setTaskCompleted(String taskId, TaskResult result) {
    TaskInfo taskInfo = tasks.get(taskId);
    if (taskInfo != null) {
      taskInfo.returnVal = result.getExitVal();
      if (result.getTaskError() != null) {
        taskInfo.errorMsg = result.getTaskError().toString();
      }
      taskInfo.endTime = System.currentTimeMillis();
    }
  }

  public synchronized List<TaskInfo> getTaskInfos() {
    List<TaskInfo> taskInfos = new ArrayList<TaskInfo>();
    taskInfos.addAll(tasks.values());
    return taskInfos;
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
}
