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
package org.apache.hive.beeline.schematool.tasks;

import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTask;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTaskProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class HiveTaskProvider implements SchemaToolTaskProvider {

  /**
   * The map contains {@link Supplier} lambdas, so only the required {@link SchemaToolTask}s are instantiated.
   */
  private final Map<TaskType, Supplier<SchemaToolTask>> taskSuppliers = new HashMap<>();

  @Override
  public SchemaToolTask getTask(TaskType taskType) {
    return taskSuppliers.getOrDefault(taskType, () -> null).get();
  }

  @Override
  public Set<String> getSupportedDatabases() {
    return new HashSet<>(Collections.singletonList(HiveSchemaHelper.DB_HIVE));
  }

  public HiveTaskProvider(SchemaToolTaskProvider embeddedHmsTaskProvider) {
    taskSuppliers.put(TaskType.INIT_SCHEMA, () -> new HiveContextTask().addChild(new HiveUpdateTask().addChild(embeddedHmsTaskProvider.getTask(TaskType.INFO))));
    taskSuppliers.put(TaskType.INIT_SCHEMA_TO, () -> new HiveContextTask().addChild(new HiveUpdateTask().addChild(embeddedHmsTaskProvider.getTask(TaskType.INFO))));
    taskSuppliers.put(TaskType.UPGRADE_SCHEMA, () -> new HiveContextTask().addChild(new HiveUpdateTask().addChild(embeddedHmsTaskProvider.getTask(TaskType.INFO))));
    taskSuppliers.put(TaskType.UPGRADE_SCHEMA_FROM, () -> new HiveContextTask().addChild(new HiveUpdateTask().addChild(embeddedHmsTaskProvider.getTask(TaskType.INFO))));
    taskSuppliers.put(TaskType.INIT_OR_UPGRADE_SCHEMA, () -> new HiveContextTask().addChild(new HiveUpdateTask().addChild(embeddedHmsTaskProvider.getTask(TaskType.INFO))));
    taskSuppliers.put(TaskType.DROP_ALL_DATABASES, () -> new HiveContextTask().addChild(new SchemaToolTaskDrop()));

    for(TaskType taskType : new TaskType[] {TaskType.INFO, TaskType.ALTER_CATALOG, TaskType.CREATE_CATALOG, TaskType.MERGE_CATALOG,
        TaskType.MOVE_DATABASE, TaskType.MOVE_TABLE, TaskType.CREATE_LOGS_TABLE, TaskType.CREATE_USER}) {
      taskSuppliers.put(taskType, () -> new HiveContextTask().addChild(embeddedHmsTaskProvider.getTask(taskType)));
    }
  }
}