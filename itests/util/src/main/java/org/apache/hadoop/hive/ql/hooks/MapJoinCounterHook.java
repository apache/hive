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
package org.apache.hadoop.hive.ql.hooks;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

public class MapJoinCounterHook implements ExecuteWithHookContext {

  public void run(HookContext hookContext) {
    HiveConf conf = hookContext.getConf();
    boolean enableConvert = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVECONVERTJOIN);
    if (!enableConvert) {
      return;
    }

    QueryPlan plan = hookContext.getQueryPlan();
    String queryID = plan.getQueryId();
    // String query = SessionState.get().getCmd();

    int commonJoin = 0;
    int hintedMapJoin = 0;
    int convertedMapJoin = 0;
    int hintedMapJoinLocal = 0;
    int convertedMapJoinLocal = 0;
    int backupCommonJoin = 0;

    List<TaskRunner> list = hookContext.getCompleteTaskList();
    for (TaskRunner tskRunner : list) {
      Task tsk = tskRunner.getTask();
      int tag = tsk.getTaskTag();
      switch (tag) {
      case Task.COMMON_JOIN:
        commonJoin++;
        break;
      case Task.HINTED_MAPJOIN:
        hintedMapJoin++;
        break;
      case Task.HINTED_MAPJOIN_LOCAL:
        hintedMapJoinLocal++;
        break;
      case Task.CONVERTED_MAPJOIN:
        convertedMapJoin++;
        break;
      case Task.CONVERTED_MAPJOIN_LOCAL:
        convertedMapJoinLocal++;
        break;
      case Task.BACKUP_COMMON_JOIN:
        backupCommonJoin++;
        break;
      }
    }
    LogHelper console = SessionState.getConsole();
    console.printError("[MapJoinCounter PostHook] COMMON_JOIN: " + commonJoin
        + " HINTED_MAPJOIN: " + hintedMapJoin + " HINTED_MAPJOIN_LOCAL: " + hintedMapJoinLocal
        + " CONVERTED_MAPJOIN: " + convertedMapJoin + " CONVERTED_MAPJOIN_LOCAL: " + convertedMapJoinLocal
        + " BACKUP_COMMON_JOIN: " + backupCommonJoin);
  }
}
