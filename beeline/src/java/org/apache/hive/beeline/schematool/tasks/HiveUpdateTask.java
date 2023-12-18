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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.tools.schematool.SchemaToolCommandLine;
import org.apache.hadoop.hive.metastore.SchemaInfo;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTask;
import org.apache.hadoop.hive.metastore.tools.schematool.task.TaskContext;
import org.apache.hive.beeline.schematool.HiveSchemaTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

class HiveUpdateTask extends SchemaToolTask {

  private static final Logger LOG = LoggerFactory.getLogger(HiveUpdateTask.class);
  
  private static final String VERSION_UPGRADE_SCRIPT = 
      "USE SYS\n" +
      "DROP TABLE IF EXISTS `VERSION`\n" +
      "%s\n" +
      "USE INFORMATION_SCHEMA\n" +
      "DROP TABLE IF EXISTS `VERSION`\n" +
      "%s\n" +
      "SELECT 'Finished upgrading MetaStore schema to %s'\n";

  @Override
  protected Set<String> usedCommandLineArguments() {
    return null;
  }

  @Override
  protected void execute(TaskContext context) throws HiveMetaException {
    String fromVersion = getFromVersion(context);

    SchemaInfo schemaInfo = context.getSchemaInfo();
    String toVersion = context.getCommandLine().getOptionValue("initSchemaTo");
    if (StringUtils.isBlank(toVersion)) {
      toVersion = SchemaInfo.getRequiredHiveSchemaVersion();
    }

    if (toVersion.equals(fromVersion)) {
      System.out.println("No schema upgrade required from version " + fromVersion);
      return;
    }

    if (fromVersion.equals(HiveSchemaInfo.INITIAL_VERSION)) {
      System.out.println("Initializing schema");
    } else {
      System.out.println("Starting upgrade metastore schema from version " + fromVersion + " to " + toVersion);
    }

    // Find the list of scripts to execute for this upgrade
    List<String> upgradeScripts = schemaInfo.getUnappliedScripts();

    String scriptDir = schemaInfo.getMetaStoreScriptDir();
    try {
      for (String scriptFile : upgradeScripts) {
        System.out.println("Upgrade script " + scriptFile);
        if (!context.getCommandLine().hasOption("dryRun")) {
          context.getScriptExecutor().execSql(scriptDir, scriptFile);
          System.out.println("Completed " + scriptFile);
        }
      }
      
      //Update schema version
      File scriptFile = File.createTempFile("hiveVesionScript", "sql");
      scriptFile.deleteOnExit();
      
      String script = String.format(
          VERSION_UPGRADE_SCRIPT,
          String.format(HiveSchemaTool.VERSION_SCRIPT, toVersion, toVersion),
          String.format(HiveSchemaTool.VERSION_SCRIPT, toVersion, toVersion), 
          toVersion);
      
      FileUtils.write(scriptFile, script, StandardCharsets.UTF_8);
      context.getScriptExecutor().execSql(scriptFile.getAbsolutePath());
    } catch (IOException e) {
      throw new HiveMetaException("Upgrade FAILED! Metastore state would be inconsistent !!", e);
    }
  }

  private String getFromVersion(TaskContext context) throws HiveMetaException {
    String dbVersion = null;
    try {
      dbVersion = context.getSchemaInfo().getSchemaVersion();
    } catch (HiveMetaException e) {
      if (!suppressErrorOnEmptySchema(context)) {
        throw new HiveMetaException("Schema version not stored in the metastore. " +
            "Metastore schema is too old or corrupt. Try specifying the version manually");
      }
      LOG.info("Exception getting db version, schema may not exist: " + e.getMessage());
      LOG.info("Try to initialize db schema");
    }

    String fromVersion = context.getCommandLine().getOptionValue("upgradeSchemaFrom");
    if (StringUtils.isNotBlank(fromVersion)) {
      if (StringUtils.isNotBlank(dbVersion) && !fromVersion.equals(dbVersion)) {
        throw new RuntimeException("The upgradeSchemaFrom version " + fromVersion + " and Metastore schema version " +
            dbVersion + " are different.");
      }
    } else {
      fromVersion = dbVersion;
    }

    return StringUtils.isNotBlank(fromVersion) ? fromVersion : HiveSchemaInfo.INITIAL_VERSION;
  }

  private boolean suppressErrorOnEmptySchema(TaskContext context) {
    SchemaToolCommandLine commandLine = context.getCommandLine();
    return commandLine.hasOption("initSchema") || commandLine.hasOption("initSchemaTo") || commandLine.hasOption("initOrUpgradeSchema");
  }

}
