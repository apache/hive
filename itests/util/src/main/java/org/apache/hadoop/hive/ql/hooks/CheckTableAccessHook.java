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

import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.parse.TableAccessInfo;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/*
 * This hook is used for verifying the table access key information
 * that is generated and maintained in the QueryPlan object by the
 * TableAccessAnalyer. All the hook does is print out the table/keys
 * per operator recorded in the TableAccessInfo in the QueryPlan.
 */
public class CheckTableAccessHook implements ExecuteWithHookContext {

  public void run(HookContext hookContext) {
    HiveConf conf = hookContext.getConf();
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_STATS_COLLECT_TABLEKEYS) == false) {
      return;
    }

    QueryPlan plan = hookContext.getQueryPlan();
    if (plan == null) {
      return;
    }

    TableAccessInfo tableAccessInfo = hookContext.getQueryPlan().getTableAccessInfo();
    if (tableAccessInfo == null ||
        tableAccessInfo.getOperatorToTableAccessMap() == null ||
        tableAccessInfo.getOperatorToTableAccessMap().isEmpty()) {
      return;
    }

    LogHelper console = SessionState.getConsole();
    Map<Operator<? extends OperatorDesc>, Map<String, List<String>>> operatorToTableAccessMap =
      tableAccessInfo.getOperatorToTableAccessMap();

    // Must be deterministic order map for consistent q-test output across Java versions
    Map<String, String> outputOrderedMap = new LinkedHashMap<String, String>();

    for (Map.Entry<Operator<? extends OperatorDesc>, Map<String, List<String>>> tableAccess:
        operatorToTableAccessMap.entrySet()) {
      StringBuilder perOperatorInfo = new StringBuilder();
      perOperatorInfo.append("Operator:").append(tableAccess.getKey().getOperatorId())
        .append("\n");
      for (Map.Entry<String, List<String>> entry: tableAccess.getValue().entrySet()) {
        perOperatorInfo.append("Table:").append(entry.getKey()).append("\n");
        perOperatorInfo.append("Keys:").append(StringUtils.join(entry.getValue(), ','))
          .append("\n");
      }
      outputOrderedMap.put(tableAccess.getKey().getOperatorId(), perOperatorInfo.toString());
    }

    for (String perOperatorInfo: outputOrderedMap.values()) {
        console.printError(perOperatorInfo);
    }
  }
}
