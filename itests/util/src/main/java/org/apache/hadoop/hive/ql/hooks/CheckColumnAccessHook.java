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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

import org.apache.hadoop.hive.ql.parse.ColumnAccessInfo;

/*
 * This hook is used for verifying the column access information
 * that is generated and maintained in the QueryPlan object by the
 * ColumnAccessAnalyzer. All the hook does is print out the columns
 * accessed from each table as recorded in the ColumnAccessInfo
 * in the QueryPlan.
 */
public class CheckColumnAccessHook implements ExecuteWithHookContext {

  @Override
  public void run(HookContext hookContext) {

    HiveConf conf = hookContext.getConf();
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_STATS_COLLECT_SCANCOLS) == false) {
      return;
    }

    QueryPlan plan = hookContext.getQueryPlan();
    if (plan == null) {
      return;
    }

    ColumnAccessInfo columnAccessInfo = hookContext.getQueryPlan().getColumnAccessInfo();
    if (columnAccessInfo == null) {
      return;
    }

    LogHelper console = SessionState.getConsole();
    Map<String, List<String>> tableToColumnAccessMap =
      columnAccessInfo.getTableToColumnAccessMap();

    // Must be deterministic order map for consistent test output across Java versions
    Map<String, String> outputOrderedMap = new LinkedHashMap<String, String>();

    for (Map.Entry<String, List<String>> tableAccess : tableToColumnAccessMap.entrySet()) {
      StringBuilder perTableInfo = new StringBuilder();
      perTableInfo.append("Table:").append(tableAccess.getKey()).append("\n");
      // Sort columns to make output deterministic
      String[] columns = new String[tableAccess.getValue().size()];
      tableAccess.getValue().toArray(columns);
      Arrays.sort(columns);
      perTableInfo.append("Columns:").append(StringUtils.join(columns, ','))
        .append("\n");
      outputOrderedMap.put(tableAccess.getKey(), perTableInfo.toString());
    }

    for (String perOperatorInfo : outputOrderedMap.values()) {
      console.printError(perOperatorInfo);
    }
  }
}
