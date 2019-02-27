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
package org.apache.hadoop.hive.metastore.tools.schematool;

import java.io.IOException;

import org.apache.hadoop.hive.metastore.HiveMetaException;

/**
 * Initialize the metastore schema.
 */
class SchemaToolTaskInit extends SchemaToolTask {
  private boolean validate = true;
  private String toVersion;

  @Override
  void setCommandLineArguments(SchemaToolCommandLine cl) {
    if (cl.hasOption("initSchemaTo")) {
      this.toVersion = cl.getOptionValue("initSchemaTo");
      this.validate = false;
    }
  }

  private void ensureToVersion() throws HiveMetaException {
    if (toVersion != null) {
      return;
    }

    // If null then current hive version is used
    toVersion = schemaTool.getMetaStoreSchemaInfo().getHiveSchemaVersion();
    System.out.println("Initializing the schema to: " + toVersion);
  }

  @Override
  void execute() throws HiveMetaException {
    ensureToVersion();

    schemaTool.testConnectionToMetastore();
    System.out.println("Starting metastore schema initialization to " + toVersion);

    String initScriptDir = schemaTool.getMetaStoreSchemaInfo().getMetaStoreScriptDir();
    String initScriptFile = schemaTool.getMetaStoreSchemaInfo().generateInitFileName(toVersion);

    try {
      System.out.println("Initialization script " + initScriptFile);
      if (!schemaTool.isDryRun()) {
        schemaTool.execSql(initScriptDir, initScriptFile);
        System.out.println("Initialization script completed");
      }
    } catch (IOException e) {
      throw new HiveMetaException("Schema initialization FAILED! Metastore state would be inconsistent!", e);
    }

    if (validate) {
      schemaTool.verifySchemaVersion();
    }
  }
}
