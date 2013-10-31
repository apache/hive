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

import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

/**
 *
 * CheckQueryPropertiesHook.
 *
 * This hook prints the values in the QueryProperties object contained in the QueryPlan
 * in the HookContext passed to the hook.
 */
public class CheckQueryPropertiesHook implements ExecuteWithHookContext {

  public void run(HookContext hookContext) {
    LogHelper console = SessionState.getConsole();

    if (console == null) {
      return;
    }

    QueryProperties queryProps = hookContext.getQueryPlan().getQueryProperties();

    if (queryProps != null) {
      console.printError("Has Join: " + queryProps.hasJoin());
      console.printError("Has Group By: " + queryProps.hasGroupBy());
      console.printError("Has Sort By: " + queryProps.hasSortBy());
      console.printError("Has Order By: " + queryProps.hasOrderBy());
      console.printError("Has Group By After Join: " + queryProps.hasJoinFollowedByGroupBy());
      console.printError("Uses Script: " + queryProps.usesScript());
      console.printError("Has Distribute By: " + queryProps.hasDistributeBy());
      console.printError("Has Cluster By: " + queryProps.hasClusterBy());
    }
  }
}