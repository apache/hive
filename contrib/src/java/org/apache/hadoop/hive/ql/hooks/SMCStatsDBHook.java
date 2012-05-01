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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;

public class SMCStatsDBHook extends AbstractSemanticAnalyzerHook {

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast) {
    HiveConf conf;
    try {
      conf = (HiveConf) context.getConf();
    } catch (ClassCastException e) {
      // Statistics won't be collected for this query,
      // warning about it will be supplied later, by JDBCStatsPublisher
      return ast;
    }
    ConnectionUrlFactory urlFactory =
        HookUtils.getUrlFactory(conf,
            FBHiveConf.CONNECTION_FACTORY,
            FBHiveConf.STATS_CONNECTION_FACTORY,
            FBHiveConf.STATS_MYSQL_TIER_VAR_NAME,
            FBHiveConf.STATS_HOST_DATABASE_VAR_NAME);
    String databaseHostName;

    try {
      databaseHostName = urlFactory.getUrl();
    } catch (Exception e) {
      // Statistics won't be collected for this query,
      // warning about it will be supplied later, by JDBCStatsPublisher
      return ast;
    }

    conf.setVar(
        HiveConf.ConfVars.HIVESTATSDBCONNECTIONSTRING,
        getUpdatedConnectionString(conf.getVar(HiveConf.ConfVars.HIVESTATSDBCONNECTIONSTRING),
            databaseHostName));
    return ast;
  }

  // default visibility for the sake of TestSMCStatsDBHook
  String getUpdatedConnectionString(String initialConnectionString, String addressFromSMC) {
    return initialConnectionString.replaceAll("jdbc.*\\?", addressFromSMC);
  }
}
