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
package org.apache.hadoop.hive.metastore.tools.schematool.liquibase;

import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTask;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTaskProvider;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class LiquibaseTaskProvider implements SchemaToolTaskProvider {

  /**
   * The map contains {@link Supplier} lambdas, so only the required {@link SchemaToolTask}s are instantiated.
   */
  private final Map<String, Supplier<SchemaToolTask>> taskSuppliers = new HashMap<>();

  @Override
  public SchemaToolTask getTask(String command) {
    return taskSuppliers.getOrDefault(command, () -> null).get();
  }

  @Override
  public Set<String> getSupportedDatabases() {
    return new HashSet<>(Arrays.asList(HiveSchemaHelper.DB_DERBY, HiveSchemaHelper.DB_MSSQL, HiveSchemaHelper.DB_MYSQL,
        HiveSchemaHelper.DB_ORACLE, HiveSchemaHelper.DB_POSTGRACE));
  }

  public LiquibaseTaskProvider(SchemaToolTaskProvider embeddedHmsTaskProvider) {
    taskSuppliers.put("initSchema", () -> new LiquibaseContextTask()
        .addChild(new LiquibaseValidationTask().addChild(new LiquibaseUpdateTask())));
    taskSuppliers.put("initSchemaTo", () -> new LiquibaseContextTask()
        .addChild(new LiquibaseValidationTask().addChild(new LiquibaseUpdateToTask())));
    taskSuppliers.put("upgradeSchema", () -> new LiquibaseContextTask()
        .addChild(new LiquibaseValidationTask().addChild(new LiquibaseSyncTask(false).addChild(new LiquibaseUpdateTask()))));
    // added only for limited backward-compatiblity. Will behave the same as 'upgradeSchema'
    taskSuppliers.put("upgradeSchemaFrom", () -> new LiquibaseContextTask()
        .addChild(new LiquibaseValidationTask().addChild(new LiquibaseSyncTask(false).addChild(new LiquibaseUpdateTask()))));
    taskSuppliers.put("initOrUpgradeSchema", () -> new LiquibaseContextTask()
        .addChild(new LiquibaseValidationTask().addChild(new LiquibaseSyncTask(true).addChild(new LiquibaseUpdateTask()))));
    taskSuppliers.put("validate", () -> new LiquibaseContextTask().addChild(new LiquibaseValidationTask()).addChild(new MetastoreValidationTask(new ScriptScannerFactory())));
    for(String command : new String[] {"info", "alterCatalog", "createCatalog", "dropAllDatabases", "mergeCatalog",
        "moveDatabase", "moveTable", "createLogsTable", "createUser"}) {
      taskSuppliers.put(command, () -> new LiquibaseContextTask().addChild(embeddedHmsTaskProvider.getTask(command)));
    }
  }
}
