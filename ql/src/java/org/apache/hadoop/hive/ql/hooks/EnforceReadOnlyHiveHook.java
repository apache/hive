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

import com.google.common.collect.Sets;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

import java.util.Set;

/**
 * EnforceReadOnlyHiveHook is a hook that enforces that the query is read-only.
 * It's enabled when "hive.exec.pre.hooks" has "org.apache.hadoop.hive.ql.hooks.EnforceReadOnlyHiveHook" and
 * "hive.enforce.readonly" is set to true.
 */
public class EnforceReadOnlyHiveHook implements ExecuteWithHookContext {
  @Override
  public void run(HookContext hookContext) throws Exception {
    assert(hookContext.getHookType() == HookContext.HookType.PRE_EXEC_HOOK);
    hookContext.getConf().getBoolVar(HiveConf.ConfVars.HIVE_ENFORCE_READONLY);
    QueryState queryState = hookContext.getQueryState();
    HiveOperation hiveOperation = queryState.getHiveOperation();

    // Allow statements that don't change any data/metadata only.
    Set<String> allowedPrefixes = Sets.newHashSet("SHOW", "DESCRIBE", "SET", "DESC", "EXPORT",
        "ROLLBACK", "KILL", "ABORT", "SWITCHDATABASE");
    if (allowedPrefixes.stream().anyMatch(hiveOperation.name()::startsWith)) {
      return;
    }

    // Allow EXPLAIN, SELECT queries, not INSERT, UPDATE, DELETE queries.
    if (hiveOperation == HiveOperation.QUERY) {
      if (hookContext.getQueryPlan().isExplain()) {
        return;
      }
      final String upper = queryState.getQueryString().trim().toUpperCase();
      if (upper.startsWith("SELECT")) {
        return;
      }
      throw new HiveException("Only SELECT queries are allowed in read-only mode.");
    }

    throw new HiveException("Statement " + hiveOperation.name() + " is not allowed in read-only mode");
  }
}
