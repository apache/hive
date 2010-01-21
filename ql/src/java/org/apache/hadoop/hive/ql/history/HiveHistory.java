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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;

public class HiveHistory {

  PrintWriter histStream; // History File stream

  String histFileName; // History file name

  static final private Log LOG = LogFactory.getLog("hive.ql.exec.HiveHistory");

  private LogHelper console;

  private Map<String, String> idToTableMap = null;

  // Job Hash Map
  private final HashMap<String, QueryInfo> queryInfoMap = new HashMap<String, QueryInfo>();

  // Task Hash Map
  private final HashMap<String, TaskInfo> taskInfoMap = new HashMap<String, TaskInfo>();

  private static final String DELIMITER = " ";

  public static enum RecordTypes {
    QueryStart, QueryEnd, TaskStart, TaskEnd, TaskProgress, SessionStart, SessionEnd, Counters
  };

  public static enum Keys {
    SESSION_ID, QUERY_ID, TASK_ID, QUERY_RET_CODE, QUERY_NUM_TASKS, QUERY_STRING, TIME, TASK_RET_CODE, TASK_NAME, TASK_HADOOP_ID, TASK_HADOOP_PROGRESS, TASK_COUNTERS, TASK_NUM_REDUCERS, ROWS_INSERTED
  };

  private static final String KEY = "(\\w+)";
  private static final String VALUE = "[[^\"]?]+"; // anything but a " in ""
  private static final String ROW_COUNT_PATTERN = "TABLE_ID_(\\d+)_ROWCOUNT";

  private static final Pattern pattern = Pattern.compile(KEY + "=" + "\""
      + VALUE + "\"");

  private static final Pattern rowCountPattern = Pattern
      .compile(ROW_COUNT_PATTERN);

  // temp buffer for parsed dataa
  private static Map<String, String> parseBuffer = new HashMap<String, String>();

  /**
   * Listner interface Parser will call handle function for each record type
   */
  public static interface Listener {

    public void handle(RecordTypes recType, Map<String, String> values)
        throws IOException;
  }

  /**
   * Parses history file and calls call back functions
   * 
   * @param path
   * @param l
   * @throws IOException
   */
  public static void parseHiveHistory(String path, Listener l)
      throws IOException {
    FileInputStream fi = new FileInputStream(path);
    BufferedReader reader = new BufferedReader(new InputStreamReader(fi));
    try {
      String line = null;
      StringBuffer buf = new StringBuffer();
      while ((line = reader.readLine()) != null) {
        buf.append(line);
        // if it does not end with " then it is line continuation
        if (!line.trim().endsWith("\"")) {
          continue;
        }
        parseLine(buf.toString(), l);
        buf = new StringBuffer();
      }
    } finally {
      try {
        reader.close();
      } catch (IOException ex) {
      }
    }
  }

  /**
   * Parse a single line of history.
   * 
   * @param line
   * @param l
   * @throws IOException
   */
  private static void parseLine(String line, Listener l) throws IOException {
    // extract the record type
    int idx = line.indexOf(' ');
    String recType = line.substring(0, idx);
    String data = line.substring(idx + 1, line.length());

    Matcher matcher = pattern.matcher(data);

    while (matcher.find()) {
      String tuple = matcher.group(0);
      String[] parts = tuple.split("=");

      parseBuffer.put(parts[0], parts[1].substring(1, parts[1].length() - 1));
    }

    l.handle(RecordTypes.valueOf(recType), parseBuffer);

    parseBuffer.clear();
  }

  public static class Info {

  }

  public static class SessionInfo extends Info {
    public String sessionId;
  };

  public static class QueryInfo extends Info {
    public Map<String, String> hm = new HashMap<String, String>();
    public Map<String, Long> rowCountMap = new HashMap<String, Long>();
  };

  public static class TaskInfo extends Info {
    public Map<String, String> hm = new HashMap<String, String>();

  };

  /**
   * Construct HiveHistory object an open history log file.
   * 
   * @param ss
   */
  public HiveHistory(SessionState ss) {

    try {
      console = new LogHelper(LOG);
      String conf_file_loc = ss.getConf().getVar(
          HiveConf.ConfVars.HIVEHISTORYFILELOC);
      if ((conf_file_loc == null) || conf_file_loc.length() == 0) {
        console.printError("No history file location given");
        return;
      }

      // Create directory
      File f = new File(conf_file_loc);
      if (!f.exists()) {
        if (!f.mkdir()) {
          console.printError("Unable to create log directory " + conf_file_loc);
          return;
        }
      }
      Random randGen = new Random();
      histFileName = conf_file_loc + "/hive_job_log_" + ss.getSessionId() + "_"
          + Math.abs(randGen.nextInt()) + ".txt";
      console.printInfo("Hive history file=" + histFileName);
      histStream = new PrintWriter(histFileName);

      HashMap<String, String> hm = new HashMap<String, String>();
      hm.put(Keys.SESSION_ID.name(), ss.getSessionId());
      log(RecordTypes.SessionStart, hm);
    } catch (FileNotFoundException e) {
      console.printError("FAILED: Failed to open Query Log : " + histFileName
          + " " + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
    }

  }

  /**
   * @return historyFileName
   */
  public String getHistFileName() {
    return histFileName;
  }

  /**
   * Write the a history record to history file
   * 
   * @param rt
   * @param keyValMap
   */
  void log(RecordTypes rt, Map<String, String> keyValMap) {

    if (histStream == null) {
      return;
    }

    StringBuffer sb = new StringBuffer();
    sb.append(rt.name());

    for (Map.Entry<String, String> ent : keyValMap.entrySet()) {

      sb.append(DELIMITER);
      String key = ent.getKey();
      String val = ent.getValue();
      val = val.replace('\n', ' ');
      sb.append(key + "=\"" + val + "\"");

    }
    sb.append(DELIMITER);
    sb.append(Keys.TIME.name() + "=\"" + System.currentTimeMillis() + "\"");
    histStream.println(sb);
    histStream.flush();

  }

  /**
   * Called at the start of job Driver.run()
   */
  public void startQuery(String cmd, String id) {
    SessionState ss = SessionState.get();
    if (ss == null) {
      return;
    }
    QueryInfo ji = new QueryInfo();

    ji.hm.put(Keys.QUERY_ID.name(), id);
    ji.hm.put(Keys.QUERY_STRING.name(), cmd);

    queryInfoMap.put(id, ji);

    log(RecordTypes.QueryStart, ji.hm);

  }

  /**
   * Used to set job status and other attributes of a job
   * 
   * @param queryId
   * @param propName
   * @param propValue
   */
  public void setQueryProperty(String queryId, Keys propName, String propValue) {
    QueryInfo ji = queryInfoMap.get(queryId);
    if (ji == null) {
      return;
    }
    ji.hm.put(propName.name(), propValue);
  }

  /**
   * Used to set task properties.
   * 
   * @param taskId
   * @param propName
   * @param propValue
   */
  public void setTaskProperty(String queryId, String taskId, Keys propName,
      String propValue) {
    String id = queryId + ":" + taskId;
    TaskInfo ti = taskInfoMap.get(id);
    if (ti == null) {
      return;
    }
    ti.hm.put(propName.name(), propValue);
  }

  /**
   * Serialize the task counters and set as a task property.
   * 
   * @param taskId
   * @param rj
   */
  public void setTaskCounters(String queryId, String taskId, RunningJob rj) {
    String id = queryId + ":" + taskId;
    QueryInfo ji = queryInfoMap.get(queryId);
    StringBuilder sb1 = new StringBuilder("");
    TaskInfo ti = taskInfoMap.get(id);
    if (ti == null) {
      return;
    }
    StringBuilder sb = new StringBuilder("");
    try {

      boolean first = true;
      for (Group group : rj.getCounters()) {
        for (Counter counter : group) {
          if (first) {
            first = false;
          } else {
            sb.append(',');
          }
          sb.append(group.getDisplayName());
          sb.append('.');
          sb.append(counter.getDisplayName());
          sb.append(':');
          sb.append(counter.getCounter());
          String tab = getRowCountTableName(counter.getDisplayName());
          if (tab != null) {
            if (sb1.length() > 0) {
              sb1.append(",");
            }
            sb1.append(tab);
            sb1.append('~');
            sb1.append(counter.getCounter());
            ji.rowCountMap.put(tab, counter.getCounter());

          }
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    if (sb1.length() > 0) {
      taskInfoMap.get(id).hm.put(Keys.ROWS_INSERTED.name(), sb1.toString());
      queryInfoMap.get(queryId).hm.put(Keys.ROWS_INSERTED.name(), sb1
          .toString());
    }
    if (sb.length() > 0) {
      taskInfoMap.get(id).hm.put(Keys.TASK_COUNTERS.name(), sb.toString());
    }
  }

  public void printRowCount(String queryId) {
    QueryInfo ji = queryInfoMap.get(queryId);
    for (String tab : ji.rowCountMap.keySet()) {
      console.printInfo(ji.rowCountMap.get(tab) + " Rows loaded to " + tab);
    }
  }

  /**
   * Called at the end of Job. A Job is sql query.
   * 
   * @param queryId
   */
  public void endQuery(String queryId) {

    QueryInfo ji = queryInfoMap.get(queryId);
    if (ji == null) {
      return;
    }
    log(RecordTypes.QueryEnd, ji.hm);
  }

  /**
   * Called at the start of a task. Called by Driver.run() A Job can have
   * multiple tasks. Tasks will have multiple operator.
   * 
   * @param task
   */
  public void startTask(String queryId, Task<? extends Serializable> task,
      String taskName) {
    SessionState ss = SessionState.get();
    if (ss == null) {
      return;
    }
    TaskInfo ti = new TaskInfo();

    ti.hm.put(Keys.QUERY_ID.name(), ss.getQueryId());
    ti.hm.put(Keys.TASK_ID.name(), task.getId());
    ti.hm.put(Keys.TASK_NAME.name(), taskName);

    String id = queryId + ":" + task.getId();
    taskInfoMap.put(id, ti);

    log(RecordTypes.TaskStart, ti.hm);

  }

  /**
   * Called at the end of a task.
   * 
   * @param task
   */
  public void endTask(String queryId, Task<? extends Serializable> task) {
    String id = queryId + ":" + task.getId();
    TaskInfo ti = taskInfoMap.get(id);

    if (ti == null) {
      return;
    }
    log(RecordTypes.TaskEnd, ti.hm);
  }

  /**
   * Called at the end of a task.
   * 
   * @param task
   */
  public void progressTask(String queryId, Task<? extends Serializable> task) {
    String id = queryId + ":" + task.getId();
    TaskInfo ti = taskInfoMap.get(id);
    if (ti == null) {
      return;
    }
    log(RecordTypes.TaskProgress, ti.hm);

  }

  /**
   * write out counters
   */
  static Map<String, String> ctrmap = null;

  public void logPlanProgress(QueryPlan plan) throws IOException {
    if (ctrmap == null) {
      ctrmap = new HashMap<String, String>();
    }
    ctrmap.put("plan", plan.toString());
    log(RecordTypes.Counters, ctrmap);
  }

  /**
   * Set the table to id map
   * 
   * @param map
   */
  public void setIdToTableMap(Map<String, String> map) {
    idToTableMap = map;
  }

  /**
   * Returns table name for the counter name
   * 
   * @param name
   * @return tableName
   */
  String getRowCountTableName(String name) {
    if (idToTableMap == null) {
      return null;
    }
    Matcher m = rowCountPattern.matcher(name);

    if (m.find()) {
      String tuple = m.group(1);
      return idToTableMap.get(tuple);
    }
    return null;

  }

}
