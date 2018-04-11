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
package org.apache.hadoop.hive.metastore;

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
import java.util.Map;

import org.apache.hadoop.hive.metastore.tools.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.HiveSchemaHelper.MetaStoreConnectionInfo;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.utils.MetastoreVersionInfo;


public class MetaStoreSchemaInfo implements IMetaStoreSchemaInfo {
  protected static final String UPGRADE_FILE_PREFIX = "upgrade-";
  protected static final String INIT_FILE_PREFIX = "hive-schema-";
  protected static final String VERSION_UPGRADE_LIST = "upgrade.order";
  protected static final String PRE_UPGRADE_PREFIX = "pre-";
  protected static final String CREATE_USER_PREFIX = "create-user";

  private String[] hiveSchemaVersions;
  private final String metastoreHome;
  protected final String dbType;

  // Some version upgrades often don't change schema. So they are equivalent to
  // a version
  // that has a corresponding schema. eg "0.13.1" is equivalent to "0.13.0"
  private static final Map<String, String> EQUIVALENT_VERSIONS =
      ImmutableMap.of("0.13.1", "0.13.0",
          "1.0.0", "0.14.0",
          "1.0.1", "1.0.0",
          "1.1.1", "1.1.0",
          "1.2.1", "1.2.0"
      );

  public MetaStoreSchemaInfo(String metastoreHome, String dbType) throws HiveMetaException {
    this.metastoreHome = metastoreHome;
    this.dbType = dbType;
  }

  private void loadAllUpgradeScripts(String dbType) throws HiveMetaException {
    // load upgrade order for the given dbType
    List<String> upgradeOrderList = new ArrayList<>();
    String upgradeListFile = getMetaStoreScriptDir() + File.separator +
        VERSION_UPGRADE_LIST + "." + dbType;
    try (FileReader fr = new FileReader(upgradeListFile);
        BufferedReader bfReader = new BufferedReader(fr)) {
      String currSchemaVersion;
      while ((currSchemaVersion = bfReader.readLine()) != null) {
        upgradeOrderList.add(currSchemaVersion.trim());
      }
    } catch (FileNotFoundException e) {
      throw new HiveMetaException("File " + upgradeListFile + "not found ", e);
    } catch (IOException e) {
      throw new HiveMetaException("Error reading " + upgradeListFile, e);
    }
    hiveSchemaVersions = upgradeOrderList.toArray(new String[0]);
  }

  /***
   * Get the list of sql scripts required to upgrade from the give version to current
   * @param fromVersion
   * @return
   * @throws HiveMetaException
   */
  @Override
  public List<String> getUpgradeScripts(String fromVersion)
      throws HiveMetaException {
    List <String> upgradeScriptList = new ArrayList<>();

    // check if we are already at current schema level
    if (getHiveSchemaVersion().equals(fromVersion)) {
      return upgradeScriptList;
    }
    loadAllUpgradeScripts(dbType);
    // Find the list of scripts to execute for this upgrade
    int firstScript = hiveSchemaVersions.length;
    for (int i=0; i < hiveSchemaVersions.length; i++) {
      if (hiveSchemaVersions[i].startsWith(fromVersion)) {
        firstScript = i;
      }
    }
    if (firstScript == hiveSchemaVersions.length) {
      throw new HiveMetaException("Unknown version specified for upgrade " +
              fromVersion + " Metastore schema may be too old or newer");
    }

    for (int i=firstScript; i < hiveSchemaVersions.length; i++) {
      String scriptFile = generateUpgradeFileName(hiveSchemaVersions[i]);
      upgradeScriptList.add(scriptFile);
    }
    return upgradeScriptList;
  }

  /***
   * Get the name of the script to initialize the schema for given version
   * @param toVersion Target version. If it's null, then the current server version is used
   * @return
   * @throws HiveMetaException
   */
  @Override
  public String generateInitFileName(String toVersion) throws HiveMetaException {
    if (toVersion == null) {
      toVersion = getHiveSchemaVersion();
    }
    String initScriptName = INIT_FILE_PREFIX + toVersion + "." +
        dbType + SQL_FILE_EXTENSION;
    // check if the file exists
    if (!(new File(getMetaStoreScriptDir() + File.separatorChar +
          initScriptName).exists())) {
      throw new HiveMetaException("Unknown version specified for initialization: " + toVersion);
    }
    return initScriptName;
  }

  @Override
  public String getCreateUserScript() throws HiveMetaException {
    String createScript = CREATE_USER_PREFIX + "." + dbType + SQL_FILE_EXTENSION;
    // check if the file exists
    if (!(new File(getMetaStoreScriptDir() + File.separatorChar +
        createScript).exists())) {
      throw new HiveMetaException("Unable to find create user file, expected: " + createScript);
    }
    return createScript;
  }

  /**
   * Find the directory of metastore scripts
   * @return
   */
  @Override
  public String getMetaStoreScriptDir() {
    return  metastoreHome + File.separatorChar +
     "scripts" + File.separatorChar + "metastore" +
    File.separatorChar + "upgrade" + File.separatorChar + dbType;
  }

  // format the upgrade script name eg upgrade-x-y-dbType.sql
  private String generateUpgradeFileName(String fileVersion) {
    return UPGRADE_FILE_PREFIX +  fileVersion + "." + dbType + SQL_FILE_EXTENSION;
  }

  @Override
  public String getPreUpgradeScriptName(int index, String upgradeScriptName) {
    return PRE_UPGRADE_PREFIX + index + "-" + upgradeScriptName;
  }

  @Override
  public String getHiveSchemaVersion() {
    String hiveVersion = MetastoreVersionInfo.getShortVersion();
    return getEquivalentVersion(hiveVersion);
  }

  private static String getEquivalentVersion(String hiveVersion) {
    // if there is an equivalent version, return that, else return this version
    String equivalentVersion = EQUIVALENT_VERSIONS.get(hiveVersion);
    if (equivalentVersion != null) {
      return equivalentVersion;
    } else {
      return hiveVersion;
    }
  }

  @Override
  public boolean isVersionCompatible(String hiveVersion, String dbVersion) {
    hiveVersion = getEquivalentVersion(hiveVersion);
    dbVersion = getEquivalentVersion(dbVersion);
    if (hiveVersion.equals(dbVersion)) {
      return true;
    }
    String[] hiveVerParts = hiveVersion.split("\\.");
    String[] dbVerParts = dbVersion.split("\\.");
    if (hiveVerParts.length != 3 || dbVerParts.length != 3) {
      // these are non standard version numbers. can't perform the
      // comparison on these, so assume that they are incompatible
      return false;
    }

    for (int i = 0; i < dbVerParts.length; i++) {
      int dbVerPart = Integer.parseInt(dbVerParts[i]);
      int hiveVerPart = Integer.parseInt(hiveVerParts[i]);
      if (dbVerPart > hiveVerPart) {
        return true;
      } else if (dbVerPart < hiveVerPart) {
        return false;
      } else {
        continue; // compare next part
      }
    }

    return true;
  }

  @Override
  public String getMetaStoreSchemaVersion(MetaStoreConnectionInfo connectionInfo) throws HiveMetaException {
    String versionQuery;
    boolean needsQuotedIdentifier = HiveSchemaHelper.getDbCommandParser(connectionInfo.getDbType(),
        connectionInfo.getMetaDbType(), false).needsQuotedIdentifier();
    if (needsQuotedIdentifier) {
      versionQuery = "select t.\"SCHEMA_VERSION\" from \"VERSION\" t";
    } else {
      versionQuery = "select t.SCHEMA_VERSION from VERSION t";
    }
    String schema = ( HiveSchemaHelper.DB_HIVE.equals(connectionInfo.getDbType()) ? "SYS" : null );
    try (Connection metastoreDbConnection = HiveSchemaHelper.getConnectionToMetastore(connectionInfo, schema);
        Statement stmt = metastoreDbConnection.createStatement()) {
      ResultSet res = stmt.executeQuery(versionQuery);
      if (!res.next()) {
        throw new HiveMetaException("Could not find version info in metastore VERSION table.");
      }
      String currentSchemaVersion = res.getString(1);
      if (res.next()) {
        throw new HiveMetaException("Multiple versions were found in metastore.");
      }
      return currentSchemaVersion;
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to get schema version, Cause:" + e.getMessage());
    }
  }
}
