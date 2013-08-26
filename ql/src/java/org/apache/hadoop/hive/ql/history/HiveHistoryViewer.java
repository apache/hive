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

package org.apache.hadoop.hive.ql.history;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.history.HiveHistory.Listener;
import org.apache.hadoop.hive.ql.history.HiveHistory.QueryInfo;
import org.apache.hadoop.hive.ql.history.HiveHistory.RecordTypes;
import org.apache.hadoop.hive.ql.history.HiveHistory.TaskInfo;

/**
 * HiveHistoryViewer.
 *
 */
public class HiveHistoryViewer implements Listener {

  String historyFile;
  String sessionId;
  private static final Log LOG = LogFactory.getLog(HiveHistoryViewer.class);

  // Job Hash Map
  private final HashMap<String, QueryInfo> jobInfoMap = new HashMap<String, QueryInfo>();

  // Task Hash Map
  private final HashMap<String, TaskInfo> taskInfoMap = new HashMap<String, TaskInfo>();

  public HiveHistoryViewer(String path) {
    historyFile = path;
    init();
  }

  public String getSessionId() {
    return sessionId;
  }

  public Map<String, QueryInfo> getJobInfoMap() {
    return jobInfoMap;
  }

  public Map<String, TaskInfo> getTaskInfoMap() {
    return taskInfoMap;
  }

  /**
   * Parse history files.
   */
  void init() {
    try {
      HiveHistoryUtil.parseHiveHistory(historyFile, this);
    } catch (IOException e) {
      // TODO pass on this exception
      e.printStackTrace();
      LOG.error("Error parsing hive history log file", e);
    }
  }

  /**
   * Implementation Listener interface function.
   *
   * @see org.apache.hadoop.hive.ql.history.HiveHistory.Listener#handle(org.apache.hadoop.hive.ql.history.HiveHistory.RecordTypes,
   *      java.util.Map)
   */
  public void handle(RecordTypes recType, Map<String, String> values) {

    if (recType == RecordTypes.SessionStart) {
      sessionId = values.get(Keys.SESSION_ID.name());
    } else if (recType == RecordTypes.QueryStart
        || recType == RecordTypes.QueryEnd) {
      String key = values.get(Keys.QUERY_ID.name());
      QueryInfo ji;
      if (jobInfoMap.containsKey(key)) {
        ji = jobInfoMap.get(key);

        ji.hm.putAll(values);

      } else {
        ji = new QueryInfo();
        ji.hm = new HashMap<String, String>();
        ji.hm.putAll(values);

        jobInfoMap.put(key, ji);

      }
    } else if (recType == RecordTypes.TaskStart
        || recType == RecordTypes.TaskEnd
        || recType == RecordTypes.TaskProgress) {

      String jobid = values.get(Keys.QUERY_ID.name());
      String taskid = values.get(Keys.TASK_ID.name());
      String key = jobid + ":" + taskid;
      TaskInfo ti;
      if (taskInfoMap.containsKey(key)) {
        ti = taskInfoMap.get(key);
        ti.hm.putAll(values);
      } else {
        ti = new TaskInfo();
        ti.hm = new HashMap<String, String>();
        ti.hm.putAll(values);
        taskInfoMap.put(key, ti);

      }

    }

  }

}
