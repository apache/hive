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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.MetaStoreConnectionInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Perform metastore schema upgrade.
 */
class SchemaToolTaskUpgrade extends SchemaToolTask {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaToolTaskUpgrade.class);
  private String fromVersion;

  @Override
  void setCommandLineArguments(SchemaToolCommandLine cl) {
    if (cl.hasOption("upgradeSchemaFrom")) {
      this.fromVersion = cl.getOptionValue("upgradeSchemaFrom");
    }
  }

  private void ensureFromVersion() throws HiveMetaException {
    MetaStoreConnectionInfo connectionInfo = schemaTool.getConnectionInfo(false);
    String dbVersion = null;
    try {
      dbVersion = schemaTool.getMetaStoreSchemaInfo().getMetaStoreSchemaVersion(connectionInfo);
    } catch (HiveMetaException e) {
      LOG.info("Exception getting db version:" + e.getMessage());
      LOG.info("Try to initialize db schema");
    }

    if (fromVersion != null) {
      if (dbVersion != null && !fromVersion.equals(dbVersion)) {
        throw new RuntimeException("The upgradeSchemaFrom version " + fromVersion + " and Metastore schema version " +
                dbVersion + " are different.");
      }
      System.out.println("Upgrading from the user input version " + fromVersion);
      return;
    }
    // fromVersion is null
    if (dbVersion != null) {
      fromVersion = dbVersion;
    } else {
      // both fromVersion and dbVersion are null
      throw new HiveMetaException("Schema version not stored in the metastore. " +
          "Metastore schema is too old or corrupt. Try specifying the version manually");
    }
    System.out.println("Upgrading from the version " + fromVersion);
  }

  @Override
  void execute() throws HiveMetaException {
    ensureFromVersion();

    if (schemaTool.getMetaStoreSchemaInfo().getHiveSchemaVersion().equals(fromVersion)) {
      System.out.println("No schema upgrade required from version " + fromVersion);
      return;
    }

    // Find the list of scripts to execute for this upgrade
    List<String> upgradeScripts = schemaTool.getMetaStoreSchemaInfo().getUpgradeScripts(fromVersion);
    schemaTool.testConnectionToMetastore();
    System.out.println("Starting upgrade metastore schema from version " + fromVersion + " to " +
        schemaTool.getMetaStoreSchemaInfo().getHiveSchemaVersion());
    String scriptDir = schemaTool.getMetaStoreSchemaInfo().getMetaStoreScriptDir();
    try {
      for (String scriptFile : upgradeScripts) {
        System.out.println("Upgrade script " + scriptFile);
        if (!schemaTool.isDryRun()) {
          runPreUpgrade(scriptDir, scriptFile);
          schemaTool.execSql(scriptDir, scriptFile);
          System.out.println("Completed " + scriptFile);
        }
      }
    } catch (IOException e) {
      throw new HiveMetaException("Upgrade FAILED! Metastore state would be inconsistent !!", e);
    }

    // Revalidated the new version after upgrade
    schemaTool.verifySchemaVersion();
  }

  /**
   *  Run pre-upgrade scripts corresponding to a given upgrade script,
   *  if any exist. The errors from pre-upgrade are ignored.
   *  Pre-upgrade scripts typically contain setup statements which
   *  may fail on some database versions and failure is ignorable.
   *
   *  @param scriptDir upgrade script directory name
   *  @param scriptFile upgrade script file name
   */
  private void runPreUpgrade(String scriptDir, String scriptFile) {
    for (int i = 0;; i++) {
      String preUpgradeScript = schemaTool.getMetaStoreSchemaInfo().getPreUpgradeScriptName(i, scriptFile);
      File preUpgradeScriptFile = new File(scriptDir, preUpgradeScript);
      if (!preUpgradeScriptFile.isFile()) {
        break;
      }

      try {
        schemaTool.execSql(scriptDir, preUpgradeScript);
        System.out.println("Completed " + preUpgradeScript);
      } catch (Exception e) {
        // Ignore the pre-upgrade script errors
        System.err.println("Warning in pre-upgrade script " + preUpgradeScript + ": " + e.getMessage());
        if (schemaTool.isVerbose()) {
          e.printStackTrace();
        }
      }
    }
  }
}
