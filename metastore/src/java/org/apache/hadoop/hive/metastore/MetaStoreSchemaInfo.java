/**
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.common.util.HiveVersionInfo;

import com.google.common.collect.ImmutableMap;


public class MetaStoreSchemaInfo {
  private static String SQL_FILE_EXTENSION=".sql";
  private static String UPGRADE_FILE_PREFIX="upgrade-";
  private static String INIT_FILE_PREFIX="hive-schema-";
  private static String VERSION_UPGRADE_LIST = "upgrade.order";
  private static String PRE_UPGRADE_PREFIX = "pre-";
  private final String dbType;
  private final String hiveSchemaVersions[];
  private final HiveConf hiveConf;
  private final String hiveHome;

  // Some version upgrades often don't change schema. So they are equivalent to
  // a version
  // that has a corresponding schema. eg "0.13.1" is equivalent to "0.13.0"
  private static final Map<String, String> EQUIVALENT_VERSIONS =
      ImmutableMap.of("0.13.1", "0.13.0",
          "1.0.0", "0.14.0");

  public MetaStoreSchemaInfo(String hiveHome, HiveConf hiveConf, String dbType) throws HiveMetaException {
    this.hiveHome = hiveHome;
    this.dbType = dbType;
    this.hiveConf = hiveConf;
    // load upgrade order for the given dbType
    List<String> upgradeOrderList = new ArrayList<String>();
    String upgradeListFile = getMetaStoreScriptDir() + File.separator +
        VERSION_UPGRADE_LIST + "." + dbType;
    try {
      BufferedReader bfReader =
        new BufferedReader(new FileReader(upgradeListFile));
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
  public List<String> getUpgradeScripts(String fromVersion)
      throws HiveMetaException {
    List <String> upgradeScriptList = new ArrayList<String>();

    // check if we are already at current schema level
    if (getHiveSchemaVersion().equals(fromVersion)) {
      return upgradeScriptList;
    }
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

  /**
   * Find the directory of metastore scripts
   * @return
   */
  public String getMetaStoreScriptDir() {
    return  hiveHome + File.separatorChar +
     "scripts" + File.separatorChar + "metastore" +
    File.separatorChar + "upgrade" + File.separatorChar + dbType;
  }

  // format the upgrade script name eg upgrade-x-y-dbType.sql
  private String generateUpgradeFileName(String fileVersion) {
    return UPGRADE_FILE_PREFIX +  fileVersion + "." + dbType + SQL_FILE_EXTENSION;
  }

  public static String getPreUpgradeScriptName(int index, String upgradeScriptName) {
    return PRE_UPGRADE_PREFIX + index + "-" + upgradeScriptName;
  }

  public static String getHiveSchemaVersion() {
    String hiveVersion = HiveVersionInfo.getShortVersion();
    // if there is an equivalent version, return that, else return this version
    String equivalentVersion = EQUIVALENT_VERSIONS.get(hiveVersion);
    if (equivalentVersion != null) {
      return equivalentVersion;
    } else {
      return hiveVersion;
    }
  }

}