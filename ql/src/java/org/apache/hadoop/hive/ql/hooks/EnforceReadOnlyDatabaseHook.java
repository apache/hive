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
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.database.alter.poperties.AlterDatabaseSetPropertiesDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * EnforceReadOnlyDatabaseHook is a hook that disallows write operations on read-only databases.
 * It's enforced when "hive.exec.pre.hooks" has "org.apache.hadoop.hive.ql.hooks.EnforceReadOnlyDatabaseHook" and
 * a database has 'readonly'='true' property.
 */
public class EnforceReadOnlyDatabaseHook implements ExecuteWithHookContext {
  public static final String READONLY = "readonly";
  private static final Logger LOG = LoggerFactory.getLogger(EnforceReadOnlyDatabaseHook.class);

  @Override
  public void run(HookContext hookContext) throws Exception {
    assert(hookContext.getHookType() == HookContext.HookType.PRE_EXEC_HOOK);
    final QueryState queryState = hookContext.getQueryState();
    final HiveOperation hiveOperation = queryState.getHiveOperation();

    // Allow read-only type operations, excluding query.
    // A query can be EXPLAIN, SELECT, INSERT, UPDATE, or DELETE.
    if (isReadOnlyOperation(hiveOperation)) {
      return;
    }

    // Allow EXPLAIN or SELECT query operations, disallow INSERT, UPDATE, and DELETE.
    if (isExplainOrSelectQuery(hookContext)) {
      return;
    }

    // Allow ALTERDATABASE operations to make writable.
    // It's a special allowed case to get out from readonly mode.
    if (isAlterDbWritable(hookContext)) {
      return;
    }
    // The second exception is to load a dumped database.
    if (hiveOperation == HiveOperation.REPLLOAD) {
      return;
    }

    // Now the remaining operation is a write operation, as a read operation is already allowed.

    // Disallow write operations on a read-only database.
    checkReadOnlyDbAsOutput(hookContext);

    // Allow write operations on writable databases.
  }

  private static void checkReadOnlyDbAsOutput(HookContext hookContext) throws HiveException {
    LOG.debug(hookContext.getQueryPlan().getOutputs().toString());
    // If it has a data/metadata change operation on a read-only database, throw an exception.
    final Set<WriteEntity> outputs = hookContext.getQueryPlan().getOutputs();
    for (WriteEntity output: outputs) {
      // Get the database.
      final Database database;
      final Hive hive = SessionState.get().getHiveDb();
      if (output.getDatabase() == null) {
        // For a table, get its database.
        database = hive.getDatabase(output.getTable().getDbName());
      } else {
        // For a database, allow new one, since it's not read-only yet.
        database = output.getDatabase();
        final boolean exists = hive.databaseExists(database.getName());
        LOG.debug("database exists: {}", exists);
        if (!exists) {
          continue;
        }
      }

      // Read the database property.
      final Map<String, String> parameters = database.getParameters();
      LOG.debug("database name: " + database.getName() + " param: " + database.getParameters());

      // If it's a read-only database, disallow it.
      if (parameters == null) {
        continue;
      }
      if (parameters.isEmpty()) {
        continue;
      }
      if (!parameters.containsKey(READONLY)) {
        continue;
      }
      if ("true".equalsIgnoreCase(parameters.get(READONLY))) {
        throw new SemanticException(ErrorMsg.READ_ONLY_DATABASE, database.getName());
      }
    }
  }

  private static boolean isExplainOrSelectQuery(HookContext hookContext) {
    final QueryState queryState = hookContext.getQueryState();
    final HiveOperation hiveOperation = queryState.getHiveOperation();

    // Allow EXPLAIN, SELECT queries, not INSERT, UPDATE, DELETE queries.
    if (hiveOperation == HiveOperation.QUERY) {
      if (hookContext.getQueryPlan().isExplain()) {
        return true;
      }
      final String upper = queryState.getQueryString().trim().toUpperCase();
      if (upper.startsWith("SELECT")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks whether it's an ALTERDATABASE statement without {"readonly":"true"} property.
   * 1. The properties are removed.
   * 2. "readonly" is removed from the properties.
   * 3. "readonly" becomes non-true value.
   */
  private static boolean isAlterDbWritable(HookContext hookContext) {
    final HiveOperation hiveOperation = hookContext.getQueryState().getHiveOperation();
    if (hiveOperation == HiveOperation.ALTERDATABASE) {
      final List<Task<?>> rootTasks = hookContext.getQueryPlan().getRootTasks();
      if (rootTasks.size() == 1) {
        final Serializable rootWork = rootTasks.get(0).getWork();
        if (rootWork instanceof DDLWork) {
          final DDLWork ddlWork = (DDLWork) rootWork;
          final DDLDesc ddlDesc = ddlWork.getDDLDesc();
          if (ddlDesc instanceof AlterDatabaseSetPropertiesDesc) {
            final AlterDatabaseSetPropertiesDesc alterDatabaseDesc = (AlterDatabaseSetPropertiesDesc) ddlDesc;
            final Map<String, String> properties = alterDatabaseDesc.getDatabaseProperties();
            if (properties == null) {
              // The properties are removed.
              return true;
            }
            if (!properties.containsKey(READONLY)) {
              // "readonly" is removed from the properties.
              return true;
            }
            if (!"true".equalsIgnoreCase(properties.get(READONLY))) {
              // "readonly" becomes non-true value.
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  private static boolean isReadOnlyOperation(HiveOperation hiveOperation) {
    switch (hiveOperation) {
    case EXPLAIN:
    case SWITCHDATABASE:
    case REPLDUMP:
    case REPLSTATUS:
    case EXPORT:
    case KILL_QUERY:
      return true;
    }
    if (Sets.newHashSet("SHOW", "DESC").stream().anyMatch(hiveOperation.name()::startsWith)) {
      return true;
    }
    return false;
  }
}
