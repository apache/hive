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


import java.sql.Connection;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;

/**
 * Defines the method which must be implemented to be used using schema tool to support metastore
 * schema upgrades. The configuration hive.metastore.schema.info.class is used to create instances
 * of this type by SchemaTool.
 *
 * Instances of this interface should be created using MetaStoreSchemaInfoFactory class which uses
 * two Strings argument constructor to instantiate the implementations of this interface
 */
@InterfaceAudience.Private
public interface IMetaStoreSchemaInfo {
  String SQL_FILE_EXTENSION = ".sql";

  /***
   * Get the list of sql scripts required to upgrade from the give version to current.
   *
   * @param fromVersion
   * @return
   * @throws HiveMetaException
   */
  List<String> getUpgradeScripts(String fromVersion) throws HiveMetaException;

  /***
   * Get the name of the script to initialize the schema for given version
   *
   * @param toVersion Target version. If it's null, then the current server version is used
   * @return
   * @throws HiveMetaException
   */
  String generateInitFileName(String toVersion) throws HiveMetaException;

  /**
   * Get SQL script that will create the user and database for Metastore to use.
   * @return filename
   * @throws HiveMetaException if something goes wrong.
   */
  String getCreateUserScript() throws HiveMetaException;

  /**
   * Find the directory of metastore scripts
   *
   * @return the path of directory where the sql scripts are
   */
  String getMetaStoreScriptDir();

  /**
   * Get the pre-upgrade script for a given script name. Schema tool runs the pre-upgrade scripts
   * returned by this method before running any upgrade scripts. These scripts could contain setup
   * statements may fail on some database versions and failure is ignorable.
   *
   * @param index - index number of the file. The preupgrade script name is derived using the given
   *          index
   * @param scriptName - upgrade script name
   * @return name of the pre-upgrade script to be run before running upgrade script
   */
  String getPreUpgradeScriptName(int index, String scriptName);

  /**
   * Get hive distribution schema version. Schematool uses this version to identify
   * the Hive version. It compares this version with the version found in metastore database
   * to determine the upgrade or initialization scripts
   * @return Hive schema version
   */
  String getHiveSchemaVersion();

  /**
   * Get the schema version from the backend database. This version is used by SchemaTool to to
   * compare the version returned by getHiveSchemaVersion and determine the upgrade order and
   * scripts needed to upgrade the metastore schema
   * 
   * @param metastoreDbConnectionInfo Connection information needed to connect to the backend
   *          database
   * @return
   * @throws HiveMetaException when unable to fetch the schema version
   */
  String getMetaStoreSchemaVersion(
      HiveSchemaHelper.MetaStoreConnectionInfo metastoreDbConnectionInfo) throws HiveMetaException;
  /**
   * A dbVersion is compatible with hive version if it is greater or equal to the hive version. This
   * is result of the db schema upgrade design principles followed in hive project. The state where
   * db schema version is ahead of hive software version is often seen when a 'rolling upgrade' or
   * 'rolling downgrade' is happening. This is a state where hive is functional and returning non
   * zero status for it is misleading.
   *
   * @param productVersion version of hive software
   * @param dbVersion version of metastore rdbms schema
   * @return true if versions are compatible
   */
  boolean isVersionCompatible(String productVersion, String dbVersion);
}
