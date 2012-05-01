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
public class AuditJoinHook implements ExecuteWithHookContext {
  static final private Log LOG = LogFactory
      .getLog("hive.ql.hooks.AuditJoinHook");

  ConnectionUrlFactory urlFactory = null;

  public AuditJoinHook() throws Exception {
    HiveConf conf = new HiveConf(AuditJoinHook.class);
    urlFactory = HookUtils.getUrlFactory(conf,
      FBHiveConf.CONNECTION_FACTORY,
      FBHiveConf.AUDIT_CONNECTION_FACTORY,
      FBHiveConf.AUDIT_MYSQL_TIER_VAR_NAME,
      FBHiveConf.AUDIT_HOST_DATABASE_VAR_NAME);
  }

  public void run(HookContext hookContext) throws Exception {
    HiveConf conf = hookContext.getConf();
    boolean enableConvert = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.HIVECONVERTJOIN);
    if (!enableConvert) {
      return;
    }
    String command = StringEscapeUtils.escapeJava(SessionState.get()
        .getCmd());
    QueryPlan plan = hookContext.getQueryPlan();
    String queryID = StringEscapeUtils.escapeJava(plan.getQueryId());
    // String query = SessionState.get().getCmd();

    int convertedMapJoin = 0;
    int commonJoin = 0;
    int backupCommonJoin = 0;
    int convertedLocalMapJoin = 0;
    int localMapJoin = 0;

    List<TaskRunner> list = hookContext.getCompleteTaskList();
    for (TaskRunner tskRunner : list) {
      Task tsk = tskRunner.getTask();
      int tag = tsk.getTaskTag();
      switch (tag) {
      case Task.COMMON_JOIN:
        commonJoin++;
        break;
      case Task.CONVERTED_LOCAL_MAPJOIN:
        convertedLocalMapJoin++;
        break;
      case Task.CONVERTED_MAPJOIN:
        convertedMapJoin++;
        break;
      case Task.BACKUP_COMMON_JOIN:
        backupCommonJoin++;
        break;
      case Task.LOCAL_MAPJOIN:
        localMapJoin++;
        break;
      }
    }

    // nothing to do
    if ((convertedMapJoin == 0) &&
         (commonJoin == 0) &&
         (backupCommonJoin == 0) &&
         (convertedLocalMapJoin == 0)
        && (localMapJoin == 0)) {
      return;
    }

    ArrayList<Object> sqlParams = new ArrayList<Object>();
    sqlParams.add(StringEscapeUtils.escapeJava(command));
    sqlParams.add(StringEscapeUtils.escapeJava(queryID));
    sqlParams.add(new Integer(convertedLocalMapJoin));
    sqlParams.add(new Integer(convertedMapJoin));
    sqlParams.add(new Integer(localMapJoin));
    sqlParams.add(new Integer(commonJoin));
    sqlParams.add(new Integer(backupCommonJoin));

    String sql = "insert into audit_join set " +
        " command = ?, query_id = ?, converted_local_mapjoin = ?, converted_map_join = ?," +
        " local_mapjoin = ?, common_join = ?, backup_common_join = ?";

    if (urlFactory == null) {
      urlFactory = HookUtils.getUrlFactory(
          conf,
          FBHiveConf.CONNECTION_FACTORY,
          FBHiveConf.AUDIT_CONNECTION_FACTORY,
          FBHiveConf.AUDIT_MYSQL_TIER_VAR_NAME,
          FBHiveConf.AUDIT_HOST_DATABASE_VAR_NAME);
      if (urlFactory == null) {
        throw new RuntimeException("DB parameters not set!");
      }
    }

    HookUtils.runInsert(conf,
                        urlFactory, sql, sqlParams, HookUtils
                        .getSqlNumRetry(conf));
  }
}
