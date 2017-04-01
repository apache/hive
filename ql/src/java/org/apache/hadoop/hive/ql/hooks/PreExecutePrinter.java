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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.common.io.FetchConverter;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Implementation of a pre execute hook that simply prints out its parameters to
 * standard output.
 */
public class PreExecutePrinter implements ExecuteWithHookContext {

  @Override
  public void run(HookContext hookContext) throws Exception {
    assert(hookContext.getHookType() == HookType.PRE_EXEC_HOOK);
    SessionState ss = SessionState.get();
    QueryState queryState = hookContext.getQueryState();
    if (ss != null && ss.out instanceof FetchConverter) {
      boolean foundQuery = queryState.getHiveOperation() == HiveOperation.QUERY &&
              !hookContext.getQueryPlan().isForExplain();
      ((FetchConverter)ss.out).foundQuery(foundQuery);
    }

    Set<ReadEntity> inputs = hookContext.getInputs();
    Set<WriteEntity> outputs = hookContext.getOutputs();
    UserGroupInformation ugi = hookContext.getUgi();
    this.run(queryState,inputs,outputs,ugi);
  }

  public void run(QueryState queryState, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, UserGroupInformation ugi)
    throws Exception {

    LogHelper console = SessionState.getConsole();

    if (console == null) {
      return;
    }

    if (queryState != null) {
      console.printInfo("PREHOOK: query: " + queryState.getQueryString().trim(), false);
      console.printInfo("PREHOOK: type: " + queryState.getCommandType(), false);
    }

    printEntities(console, inputs, "PREHOOK: Input: ");
    printEntities(console, outputs, "PREHOOK: Output: ");
  }

  static void printEntities(LogHelper console, Set<?> entities, String prefix) {
    List<String> strings = new ArrayList<String>();
    for (Object o : entities) {
      strings.add(o.toString());
    }
    Collections.sort(strings);
    for (String s : strings) {
      console.printInfo(prefix + s, false);
    }
  }
}
