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

package org.apache.hadoop.hive.ql.hooks;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Implementation of a post execute hook that checks whether a partition is
 * archived or not and also sets that query time for the partition.
 */
public class AuditLocalModeHook implements ExecuteWithHookContext {
  static final private Log LOG = LogFactory
      .getLog("hive.ql.hooks.AuditLocalModeHook");
  final static String MYSQL_TIER_VAR_NAME = "fbhive.audit.mysql.tier";
  final static String HOST_DATABASE_VAR_NAME = "fbhive.audit.mysql";

  ConnectionUrlFactory urlFactory = null;

  public AuditLocalModeHook() throws Exception {
    HiveConf conf = new HiveConf(AuditLocalModeHook.class);
    urlFactory = HookUtils.getUrlFactory(conf,
        FBHiveConf.CONNECTION_FACTORY,
        FBHiveConf.AUDIT_CONNECTION_FACTORY,
        FBHiveConf.AUDIT_MYSQL_TIER_VAR_NAME,
        FBHiveConf.AUDIT_HOST_DATABASE_VAR_NAME);
  }

  public void run(HookContext hookContext) throws Exception {
    HiveConf conf = hookContext.getConf();
    String command = StringEscapeUtils.escapeJava(SessionState.get().getCmd());
    QueryPlan plan = hookContext.getQueryPlan();
    String queryID = StringEscapeUtils.escapeJava(plan.getQueryId());
    int numLocalModeTask = 0;
    int numMapRedTask = 0;
    int numTask = 0;
    List<TaskRunner> list = hookContext.getCompleteTaskList();
    numTask = list.size();
    for (TaskRunner tskRunner : list) {
      Task tsk = tskRunner.getTask();
      if(tsk.isMapRedTask()){
        if(tsk.isLocalMode()) {
            numLocalModeTask++;
           }
        numMapRedTask++;
      }
    }
    if(numLocalModeTask == 0){
      return ;
    }
    ArrayList<Object> sqlParams = new ArrayList<Object>();
    sqlParams.add(StringEscapeUtils.escapeJava(command));
    sqlParams.add(StringEscapeUtils.escapeJava(queryID));
    sqlParams.add(new Integer(numTask));
    sqlParams.add(new Integer(numMapRedTask));
    sqlParams.add(new Integer(numLocalModeTask));

    String sql = "insert into audit_local set " +
                  " command = ?, query_id = ?, num_tasks = ?, num_mapred_tasks = ?," +
    " num_local_mapred_tasks = ?";
    HookUtils.runInsert(conf, urlFactory, sql, sqlParams, HookUtils
                        .getSqlNumRetry(conf));
  }
}
