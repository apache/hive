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
package org.apache.hadoop.hive.metastore.tools.schematool.task;

import java.util.Set;

/**
 * Responsible for creating the necessary {@link SchemaToolTask} instances
 */
public interface SchemaToolTaskProvider {

  enum TaskType {

    INIT_SCHEMA("initSchema"),
    INIT_SCHEMA_TO("initSchemaTo"),
    UPGRADE_SCHEMA("upgradeSchema"),
    UPGRADE_SCHEMA_FROM("upgradeSchemaFrom"),
    INIT_OR_UPGRADE_SCHEMA("initOrUpgradeSchema"),
    VALIDATE("validate"),
    INFO("info"),
    ALTER_CATALOG("alterCatalog"),
    CREATE_CATALOG("createCatalog"),
    MERGE_CATALOG("mergeCatalog"),
    MOVE_DATABASE("moveDatabase"),
    MOVE_TABLE("moveTable"),
    CREATE_LOGS_TABLE("createLogsTable"),
    CREATE_USER("createUser"),
    DROP_ALL_DATABASES("dropAllDatabases");
    
    private final String command;

    public String getCommand() {
      return command;      
    }

    TaskType(String command) {
      this.command = command;
    }
    
    public static TaskType fromCommand(String command) {
      for(TaskType taskType : TaskType.values()) {
        if (command.equalsIgnoreCase(taskType.command)) {
          return taskType;
        }
      }
      return null;
    }

  }

  /**
   * Creates a new {@link SchemaToolTask} instance associated with the given command
   * @param taskType The schema tool command parsed from the arguments
   * @return Returns with a new instance of the {@link SchemaToolTask} associated with the command, or null if there
   *  is nothing registered for it.
   */
  SchemaToolTask getTask(TaskType taskType);

  /**
   * @return Returns the list of databases which are supported by the {@link SchemaToolTask} instances returned by this
   * {@link SchemaToolTaskProvider}.
   */
  Set<String> getSupportedDatabases();

}
