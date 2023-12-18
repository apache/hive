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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.SchemaInfo;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

class HiveSchemaInfo extends SchemaInfo {

  private static final Logger LOG = LoggerFactory.getLogger(HiveSchemaInfo.class);
  private static final String UPGRADE_FILE_PREFIX = "upgrade-";
  protected static final String VERSION_UPGRADE_LIST = "upgrade.order";
  protected static final String INITIAL_VERSION = "0.0.0";

  String[] hiveSchemaVersions;

  @Override
  public List<String> getUnappliedScripts() throws HiveMetaException {
    String schemaVersion = INITIAL_VERSION;
    try {
      schemaVersion = getSchemaVersion();
    } catch (HiveMetaException e) {
      if (e.getMessage().startsWith("Failed to get schema version")) {
        LOG.warn("Unable to get schema version, assuming it's an empty schema.");
      } else throw e;
    }
    List <String> upgradeScriptList = new ArrayList<>();

    // check if we are already at the required schema level
    if (isVersionCompatible(getRequiredHiveSchemaVersion(), schemaVersion)) {
      return upgradeScriptList;
    }

    // Find the list of scripts to execute for this upgrade
    int firstScript = -1;
    for (int i=0; i < hiveSchemaVersions.length; i++) {
      if (hiveSchemaVersions[i].startsWith(schemaVersion + "-to-")) {
        firstScript = i;
      }
    }
    if (firstScript == -1) {
      throw new HiveMetaException("Unknown version specified for upgrade " + schemaVersion + " Metastore schema may be too old or newer");
    }

    for (int i=firstScript; i < hiveSchemaVersions.length; i++) {
      String scriptFile = generateUpgradeFileName(hiveSchemaVersions[i]);
      upgradeScriptList.add(scriptFile);
    }
    return upgradeScriptList;
  }

  @Override
  public List<String> getAppliedScripts() throws HiveMetaException {
    List <String> upgradeScriptList = new ArrayList<>();
    String schemaVersion;
    try {
      schemaVersion = getSchemaVersion();
    } catch (HiveMetaException e) {
      if (e.getMessage().startsWith("Failed to get schema version")) {
        LOG.warn("Unable to get schema version, assuming it's an empty schema.");
        return upgradeScriptList;
      } else throw e;
    }

    // Find the list of scripts to execute for this upgrade
    for (String hiveSchemaVersion : hiveSchemaVersions) {
      if (hiveSchemaVersion.startsWith(schemaVersion + "-to-")) {
        break;
      } else {
        String scriptFile = generateUpgradeFileName(hiveSchemaVersion);
        upgradeScriptList.add(scriptFile);
      }
    }
    return upgradeScriptList;
  }

  @Override
  public String getSchemaVersion() throws HiveMetaException {
    try (Connection metastoreDbConnection = HiveSchemaHelper.getConnectionToMetastore(connectionInfo, conf, "SYS");
         Statement stmt = metastoreDbConnection.createStatement()) {
      ResultSet res = stmt.executeQuery("select t.SCHEMA_VERSION from VERSION t");
      if (!res.next()) {
        throw new HiveMetaException("Could not find version info in Hive VERSION table.");
      }
      String currentSchemaVersion = res.getString(1);
      if (res.next()) {
        throw new HiveMetaException("Multiple versions were found in version table.");
      }
      return currentSchemaVersion;
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to get schema version, Cause:" + e.getMessage());
    }
  }

  @Override
  public String getCreateUserScript() throws HiveMetaException {
    String createScript = CREATE_USER_PREFIX + "." + connectionInfo + SQL_FILE_EXTENSION;
    File scriptFile = new File(getMetaStoreScriptDir() + File.separatorChar + createScript);
    // check if the file exists
    if (!scriptFile.exists()) {
      throw new HiveMetaException("Unable to find create user file, expected: " + scriptFile.getAbsolutePath());
    }
    return createScript;
  }

  // format the upgrade script name eg upgrade-x-y-dbType.sql
  private String generateUpgradeFileName(String fileVersion) {
    return UPGRADE_FILE_PREFIX +  fileVersion + "." + connectionInfo.getDbType() + SQL_FILE_EXTENSION;
  }

  private void loadAllUpgradeScripts() throws HiveMetaException {
    // load upgrade order for the given dbType
    List<String> upgradeOrderList = new ArrayList<>();
    String upgradeListFile = getMetaStoreScriptDir() + File.separator + VERSION_UPGRADE_LIST + "." + connectionInfo.getDbType();
    try (FileReader fr = new FileReader(upgradeListFile);
         BufferedReader bfReader = new BufferedReader(fr)) {
      String currSchemaVersion;
      while ((currSchemaVersion = bfReader.readLine()) != null) {
        upgradeOrderList.add(currSchemaVersion.trim());
      }
    } catch (FileNotFoundException e) {
      throw new HiveMetaException("File " + upgradeListFile + " not found ", e);
    } catch (IOException e) {
      throw new HiveMetaException("Error reading " + upgradeListFile, e);
    }
    hiveSchemaVersions = upgradeOrderList.toArray(new String[0]);
  }


  public HiveSchemaInfo(String metastoreHome, HiveSchemaHelper.MetaStoreConnectionInfo connectionInfo, Configuration conf) throws HiveMetaException {
    super(metastoreHome, connectionInfo, conf);
    loadAllUpgradeScripts();
  }
}
