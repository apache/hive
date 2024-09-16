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
package org.apache.hadoop.hive.metastore.tools.schematool.hms;

import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTask;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTaskProvider;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * This {@link SchemaToolTaskProvider} implementation provides tasks which can be executed in both Liquibase or Hive contexts.
 * (in other words: the scripts can be executed both using {@link sqlline.SqlLine} or Beeline)
 */
public class EmbeddedTaskProvider implements SchemaToolTaskProvider {

  /**
   * The map contains {@link Supplier} lambdas, so only the required {@link SchemaToolTask}s are instantiated.
   */
  private static final Map<TaskType, Supplier<SchemaToolTask>> TASK_SUPPLIERS = new HashMap<>();

  static {
    TASK_SUPPLIERS.put(TaskType.INFO, SchemaToolTaskInfo::new);
    TASK_SUPPLIERS.put(TaskType.ALTER_CATALOG, SchemaToolTaskAlterCatalog::new);
    TASK_SUPPLIERS.put(TaskType.CREATE_CATALOG, SchemaToolTaskCreateCatalog::new);
    TASK_SUPPLIERS.put(TaskType.MERGE_CATALOG, SchemaToolTaskMergeCatalog::new);
    TASK_SUPPLIERS.put(TaskType.MOVE_DATABASE, SchemaToolTaskMoveDatabase::new);
    TASK_SUPPLIERS.put(TaskType.MOVE_TABLE, SchemaToolTaskMoveTable::new);
    TASK_SUPPLIERS.put(TaskType.CREATE_LOGS_TABLE, SchemaToolTaskCreateLogsTable::new);
    TASK_SUPPLIERS.put(TaskType.CREATE_USER, SchemaToolTaskCreateUser::new);
  }

  @Override
  public SchemaToolTask getTask(TaskType taskType) {
    return TASK_SUPPLIERS.getOrDefault(taskType, () -> null).get();
  }

  @Override
  public Set<String> getSupportedDatabases() {
    return new HashSet<>(Arrays.asList(HiveSchemaHelper.DB_DERBY, HiveSchemaHelper.DB_MSSQL, HiveSchemaHelper.DB_MYSQL,
        HiveSchemaHelper.DB_ORACLE, HiveSchemaHelper.DB_POSTGRES, HiveSchemaHelper.DB_HIVE));
  }

}
