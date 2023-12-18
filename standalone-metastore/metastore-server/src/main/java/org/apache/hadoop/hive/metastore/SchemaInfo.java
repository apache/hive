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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;

import java.io.File;
import java.util.List;

/**
 * Provides information about the HMS or Hive schema
 */
public abstract class SchemaInfo {

  /**
   * This must be updated accordingly in case of breaking schema changes. 
   * For example: adding a new column to an HMS table.
   */
  private static final String MIN_HMS_SCHEMA_VERSION = "4.0.0";
  /**
   * This must be updated accordingly in case of breaking schema changes. 
   * For example: adding a new column to a Hive view.
   */
  public static final String MIN_HIVE_SCHEMA_VERSION = "4.0.0";
  
  protected static final String SQL_FILE_EXTENSION = ".sql";
  protected static final String CREATE_USER_PREFIX = "create-user";

  private final String metastoreHome;
  protected final Configuration conf;
  protected final HiveSchemaHelper.MetaStoreConnectionInfo connectionInfo;

  /***
   * Get the list of sql scripts not yet applied to the schema.
   *
   * @return Returns paths of the unapplied script files.
   * @throws HiveMetaException Thrown when the scripts are not present, or schema version cannot be determined.
   */
  public abstract List<String> getUnappliedScripts() throws HiveMetaException;

  /***
   * Get the list of sql scripts already applied to the schema.
   *
   * @return Returns paths of the applied script files.
   * @throws HiveMetaException Thrown when the scripts are not present, or schema version cannot be determined.
   */
  public abstract List<String> getAppliedScripts() throws HiveMetaException;

  /**
   * Get the version of the database schema. <b>Please note that while this value is accurate for Hive schema,
   * for liquibase this function simply returns the version of the changeset with the
   * highest id. Due to possible unapplied out-of-order migrations, this version cannot guarantee that all former migration
   * sctipts are applied. Because of this, please consider well, where to use this value!</b>
   * @return returns the schema version obtained from the database
   * @throws HiveMetaException Thrown if the schema version could not be obtained from the database
   */
  public abstract String getSchemaVersion() throws HiveMetaException;

  /**
   * Get SQL script that will create the user and database for Metastore to use.
   *
   * @return filename
   * @throws HiveMetaException if something goes wrong.
   */
  public String getCreateUserScript() throws HiveMetaException {
    String createScript = CREATE_USER_PREFIX + "." + connectionInfo.getDbType() + SQL_FILE_EXTENSION;
    File scriptFile = new File(getMetaStoreScriptDir() + File.separatorChar + createScript);
    // check if the file exists
    if (!scriptFile.exists()) {
      throw new HiveMetaException("Unable to find create user file, expected: " + scriptFile.getAbsolutePath());
    }
    return createScript;
  }

  /**
   * Find the directory of metastore scripts
   * @return
   */
  public String getMetaStoreScriptDir() {
    return metastoreHome + File.separatorChar +
        "scripts" + File.separatorChar + "metastore" +
        File.separatorChar + "upgrade" + File.separatorChar + connectionInfo.getDbType();
  }

  /**
   * Get the required minimum HMS schema version by the current Hive distribution. Schematool uses this version to compare with the
   * version found in metastore database to determine the list of upgrade or initialization scripts
   * @return Required HMS schema version
   */
  public static String getRequiredHmsSchemaVersion() {
    return MIN_HMS_SCHEMA_VERSION;
  }

  /**
   * Get the required minimum Hive schema version by the current Hive distribution. Schematool uses this version to compare with the
   * version found in Hive database to determine the list of upgrade or initialization scripts
   * @return Required Hive schema version
   */
  public static String getRequiredHiveSchemaVersion() {
    return MIN_HIVE_SCHEMA_VERSION;
  }

  /**
   * Checks if the minimum version and the DB versions are compatible. The versions are compatible if they are differing only 
   * in the incremental version, and the DB version is higer or equal as the minimum version, or the minimum version has more or equal 
   * version parts (4.0.1-alpha-1 has the following parts:4,0,1,alpha,1) than the DB version. Some examples:
   * <ul>
   *   <li>minimum version, DB version</li>
   *   <li>4.0.1 and 4.0.1 are compatible</li>
   *   <li>4.0.1 and 4.0.9 are compatible</li>
   *   <li>4.0.9 and 4.0.1 are not compatible</li>
   *   <li><b>4.0.1-alpha-1 and 4.0.0 are compatible</b></li>
   *   <li>4.1.1 and 4.0.1 are not compatible</li>
   *   <li>4.0.1 and 4.1.1 are not compatible</li>
   *   <li><b>4.0.0 and 4.0.1-alpha-1 are not compatible</b></li>
   * </ul>
   * @param minimumVersion The minimum required version
   * @param dbVersion The DB version read from the schema tracking table
   * @return returns true if the two versions are compatible, false otherwise
   */
  public static boolean isVersionCompatible(String minimumVersion, String dbVersion) {
    if (minimumVersion.equals(dbVersion)) {
      return true;
    }
    String[] verParts = minimumVersion.split("\\.");
    String[] dbVerParts = dbVersion.split("\\.");
    if (verParts.length != 3 || dbVerParts.length != 3) {
      // these are non-standard version numbers. can't perform the
      // comparison on these, so assume that they are incompatible
      return false;
    }

    verParts = minimumVersion.split("\\.|-");
    dbVerParts = dbVersion.split("\\.|-");
    for (int i = 0; i < Math.min(verParts.length, dbVerParts.length); i++) {
      int compare = compareVersion(dbVerParts[i], verParts[i]);
      if (compare != 0 && i < 2) { //major or minor version difference
        return false;
      }
      if (compare < 0) {
        return false;
      }
    }
    return verParts.length >= dbVerParts.length;
  }

  private static int compareVersion(String dbVerPart, String hiveVerPart) {
    if (dbVerPart.equals(hiveVerPart)) {
      return 0;
    }
    boolean isDbVerNum = StringUtils.isNumeric(dbVerPart);
    boolean isHiveVerNum = StringUtils.isNumeric(hiveVerPart);
    if (isDbVerNum && isHiveVerNum) {
      return Integer.parseInt(dbVerPart) - Integer.parseInt(hiveVerPart);
    } else if (!isDbVerNum && !isHiveVerNum) {
      return dbVerPart.compareTo(hiveVerPart);
    }
    // return -1 for one is a number but the other is a string
    return -1;
  }

  public SchemaInfo(String metastoreHome, HiveSchemaHelper.MetaStoreConnectionInfo connectionInfo, Configuration conf) {
    this.metastoreHome = metastoreHome;
    this.connectionInfo = connectionInfo;
    this.conf = conf;
  }
}
