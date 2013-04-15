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
package org.apache.hadoop.hive.ql.profiler;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorHookContext;
import org.apache.hadoop.hive.ql.exec.Utilities;

public class HiveProfilerStats {
  public final class Columns {
    public static final String QUERY_ID = "queryId";
    public static final String OPERATOR_NAME = "operatorName";
    public static final String OPERATOR_ID = "operatorId";
    public static final String PARENT_OPERATOR_ID = "parentOperatorId";
    public static final String PARENT_OPERATOR_NAME = "parentOperatorName";
    public static final String EXCL_TIME = "exclTime";
    public static final String LEVEL_ANNO_NAME = "levelAnnotatedName";
    public static final String INCL_TIME = "inclTime";
    public static final String CALL_COUNT = "callCount";
    public static final String TASK_ID = "taskId";
  }

  public static final String[] COLUMN_NAMES= new String[] {
    Columns.QUERY_ID,
    Columns.TASK_ID,
    Columns.OPERATOR_NAME,
    Columns.OPERATOR_ID,
    Columns.PARENT_OPERATOR_ID,
    Columns.PARENT_OPERATOR_NAME,
    Columns.LEVEL_ANNO_NAME,
    Columns.INCL_TIME,
    Columns.CALL_COUNT
  };

  private final Map<String, String> stats = new HashMap<String, String>();

  long callCount;
  long inclTime;
  String taskId;

  protected HiveProfilerStats(
    OperatorHookContext opHookContext,
    long callCount, long wallTime, Configuration conf) {
    this.callCount = callCount;
    this.inclTime = wallTime;
    this.taskId = Utilities.getTaskId(conf);
    populateStatsMap(opHookContext, conf);
  }

  private void populateStatsMap(OperatorHookContext opHookContext,
    Configuration conf) {
    String queryId =
      conf == null ? "no conf" : HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYID);
    stats.put(Columns.QUERY_ID, queryId);
    String opName = opHookContext.getOperatorName();
    stats.put(
      Columns.OPERATOR_NAME, opName);
    stats.put(
      Columns.OPERATOR_ID, opHookContext.getOperatorId());

    Operator parent = opHookContext.getParentOperator();
    String parentOpName = parent == null ? "" : parent.getName();
    stats.put(Columns.PARENT_OPERATOR_NAME, parentOpName);

    String parentOpId = parent == null ? "-1" : parent.getIdentifier();
    stats.put(Columns.PARENT_OPERATOR_ID, parentOpId);

    stats.put(Columns.LEVEL_ANNO_NAME, HiveProfilerUtils.getLevelAnnotatedName(opHookContext));

  }

  public void updateStats(long wallTime, long count) {
    this.inclTime += wallTime;
    this.callCount += count;
  }

  public Map<String, String> getStatsMap() {
    stats.put(Columns.TASK_ID, taskId);
    stats.put(Columns.INCL_TIME, String.valueOf(inclTime));
    stats.put(Columns.CALL_COUNT, String.valueOf(callCount));
    return stats;
  }

  @Override
  public String toString() {
    return stats.toString();
  }
}

