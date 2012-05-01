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
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 *  A post-execute hook that will log the overridden Hive Config
 *  for this query to the audit_log database.
 */
public class HiveConfigLoggingHook  implements ExecuteWithHookContext{

  protected ConnectionUrlFactory urlFactory = null;

  public static ConnectionUrlFactory getQueryConfigUrlFactory(HiveConf conf) {
    // Writes to the same database as BaseReplicationHook.
    return HookUtils.getUrlFactory(conf,
        FBHiveConf.CONNECTION_FACTORY,
        FBHiveConf.REPLICATION_CONNECTION_FACTORY,
        FBHiveConf.REPLICATION_MYSQL_TIER_VAR_NAME,
        FBHiveConf.REPLICATION_HOST_DATABASE_VAR_NAME);
  }

   @Override
   public void run(HookContext hookContext) throws Exception {
     SessionState ss = SessionState.get();
     if (ss == null) {
       // QueryId not present. Nothing to do.
       return;
     }
     HiveConf conf = ss.getConf();
     String queryId = conf.getVar(HiveConf.ConfVars.HIVEQUERYID);
     HiveOperation op = ss.getHiveOperation();

     if ((op == null) ||
         (!op.equals(HiveOperation.CREATETABLE_AS_SELECT) &&
             !op.equals(HiveOperation.LOAD) &&
             !op.equals(HiveOperation.QUERY))) {
       return;
     }

     Map<String, String> overriddenConfig = ss.getOverriddenConfigurations();
     String sql = "insert into query_config_log set queryId = ?, config_key = ?, config_value = ?";
     List<Object> sqlParams = new ArrayList<Object>();
     urlFactory = getQueryConfigUrlFactory(conf);
     if (urlFactory == null) {
       throw new RuntimeException("DB parameters not set!");
     }

      for (Map.Entry<String, String> e : overriddenConfig.entrySet()) {
        String key = e.getKey();
        String val = conf.get(key);
        if (val != null) {
          sqlParams.clear();
          sqlParams.add(StringEscapeUtils.escapeJava(queryId));
          sqlParams.add(StringEscapeUtils.escapeJava(key));
          sqlParams.add(StringEscapeUtils.escapeJava(val));
          HookUtils.runInsert(conf, urlFactory, sql, sqlParams, HookUtils
              .getSqlNumRetry(conf));
        }
      }
   }
}
