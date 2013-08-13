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

package org.apache.hadoop.hive.ql.history;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.mapred.Counters;

/**
 * HiveHistory. Logs information such as query, query plan, runtime statistics
 * into a file.
 * Each session uses a new object, which creates a new file.
 */
public interface HiveHistory {

  /**
   * RecordTypes.
   *
   */
  public static enum RecordTypes {
    QueryStart,
    QueryEnd,
    TaskStart,
    TaskEnd,
    TaskProgress,
    SessionStart,
    SessionEnd,
    Counters
  };

  /**
   * Keys.
   *
   */
  public static enum Keys {
    SESSION_ID,
    QUERY_ID,
    TASK_ID,
    QUERY_RET_CODE,
    QUERY_NUM_TASKS,
    QUERY_STRING,
    TIME,
    TASK_RET_CODE,
    TASK_NAME,
    TASK_HADOOP_ID,
    TASK_HADOOP_PROGRESS,
    TASK_COUNTERS,
    TASK_NUM_MAPPERS,
    TASK_NUM_REDUCERS,
    ROWS_INSERTED
  };

  /**
   * Listener interface.
   * Parser will call handle function for each history record row, specifying
   * the record type and its values
   *
   */
  public static interface Listener {

    void handle(RecordTypes recType, Map<String, String> values) throws IOException;
  }

  /**
   * Info.
   *
   */
  public static class Info {

  }

  /**
   * SessionInfo.
   *
   */
  public static class SessionInfo extends Info {
    public String sessionId;
  };

  /**
   * QueryInfo.
   *
   */
  public static class QueryInfo extends Info {
    public Map<String, String> hm = new HashMap<String, String>();
    public Map<String, Long> rowCountMap = new HashMap<String, Long>();
  };

  /**
   * TaskInfo.
   *
   */
  public static class TaskInfo extends Info {
    public Map<String, String> hm = new HashMap<String, String>();

  };


  /**
   * @return historyFileName
   */
  public String getHistFileName();

  /**
   * Called at the start of query execution in Driver.execute().
   */
  public void startQuery(String cmd, String id);

  /**
   * Used to set query status and other attributes of a query
   *
   * @param queryId
   * @param propName
   * @param propValue
   */
  public void setQueryProperty(String queryId, Keys propName, String propValue);

  /**
   * Used to set task properties.
   *
   * @param taskId
   * @param propName
   * @param propValue
   */
  public void setTaskProperty(String queryId, String taskId, Keys propName,
      String propValue);

  /**
   * Serialize the task counters and set as a task property.
   *
   * @param queryId
   * @param taskId
   * @param ctrs
   */
  public void setTaskCounters(String queryId, String taskId, Counters ctrs);

  public void printRowCount(String queryId);

  /**
   * Called at the end of a query
   *
   * @param queryId
   */
  public void endQuery(String queryId);

  /**
   * Called at the start of a task. Called by Driver.run() A query can have
   * multiple tasks. Tasks will have multiple operator.
   *
   * @param task
   */
  public void startTask(String queryId, Task<? extends Serializable> task,
      String taskName);

  /**
   * Called at the end of a task.
   *
   * @param task
   */
  public void endTask(String queryId, Task<? extends Serializable> task);

  /**
   * Logs progress of a task if ConfVars.HIVE_LOG_INCREMENTAL_PLAN_PROGRESS is
   * set to true
   *
   * @param task
   */
  public void progressTask(String queryId, Task<? extends Serializable> task);


  /**
   * Logs the current plan state
   * @param plan
   * @throws IOException
   */
  public void logPlanProgress(QueryPlan plan) throws IOException;


  /**
   * Set the id to table name map
   *
   * @param map
   */
  public void setIdToTableMap(Map<String, String> map);

  /**
   * Close the log file stream
   */
  public void closeStream();



}
