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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.json.JSONObject;

/**
 * A hook which populates the query_plan_log MySQL table with
 * the query plan for the query. The query plan is recorded as a json string.
 * the stats through it as well.
 */
public class QueryPlanHook implements ExecuteWithHookContext {

  private ConnectionUrlFactory urlFactory = null;
  private HiveConf conf;

  public static ConnectionUrlFactory getQueryPlanUrlFactory(HiveConf conf) {
    return HookUtils.getUrlFactory(
        conf,
        FBHiveConf.CONNECTION_FACTORY,
        FBHiveConf.QUERYPLAN_CONNECTION_FACTORY,
        FBHiveConf.QUERYPLAN_MYSQL_TIER_VAR_NAME,
        FBHiveConf.QUERYPLAN_HOST_DATABASE_VAR_NAME);
  }

  public QueryPlanHook() throws Exception {
    conf = new HiveConf(QueryPlanHook.class);
  }

  @Override
  public void run(HookContext hookContext) throws Exception {

    assert(hookContext.getHookType() == HookContext.HookType.POST_EXEC_HOOK);

    String queryId = "";
    SessionState sess = SessionState.get();

    if (sess != null) {
      conf = sess.getConf();
      queryId = conf.getVar(HiveConf.ConfVars.HIVEQUERYID);
      HiveOperation op = sess.getHiveOperation();

      // No need to log for DDLs
      if ((op == null) ||
          ((!op.equals(HiveOperation.CREATETABLE_AS_SELECT)) &&
           (!op.equals(HiveOperation.CREATEVIEW)) &&
           (!op.equals(HiveOperation.LOAD)) &&
           (!op.equals(HiveOperation.QUERY)))) {
        return;
      }
    }
    // QueryId not present - nothing to do
    else {
      return;
    }

    // Get the list of root tasks
    List<Task<? extends Serializable>> rootTasks = hookContext.getQueryPlan().getRootTasks();
    if ((rootTasks == null) || (rootTasks.isEmpty())) {
      return;
    }

    ExplainWork explainWork = new ExplainWork(null, rootTasks, null, false, true);
    JSONObject queryPlan = ExplainTask.getJSONPlan(null, explainWork);

    List<Object> sqlParams = new ArrayList<Object>();
    sqlParams.add(StringEscapeUtils.escapeJava(queryId));
    sqlParams.add(StringEscapeUtils.escapeJava(queryPlan.toString()));

    // Assertion at beginning of method guarantees this string will not remain empty
    String sql = "insert into query_plan_log set queryId = ?, queryPlan = ?";
    if (urlFactory == null) {
      urlFactory = getQueryPlanUrlFactory(conf);
      if (urlFactory == null) {
        throw new RuntimeException("DB parameters not set!");
      }
    }

    HookUtils.runInsert(conf, urlFactory, sql, sqlParams, HookUtils
        .getSqlNumRetry(conf));
  }
}
