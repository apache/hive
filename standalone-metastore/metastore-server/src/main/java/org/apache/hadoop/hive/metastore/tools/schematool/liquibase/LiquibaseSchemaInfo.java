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

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.changelog.ChangeSetStatus;
import liquibase.exception.LiquibaseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.SchemaInfo;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LiquibaseSchemaInfo extends SchemaInfo {

  private static final Map<String, String[]> VERSION_QUERIES = new HashMap<>(5);

  static {
    VERSION_QUERIES.put(HiveSchemaHelper.DB_POSTGRES, new String[]{
        "select t.\"labels\" from \"databasechangelog\" t where CAST(t.\"id\" AS INTEGER) = (select MAX(CAST(t2.\"id\" as INTEGER)) from \"databasechangelog\" t2)",
        "select t.\"SCHEMA_VERSION\" from \"VERSION\" t" });
    VERSION_QUERIES.put(HiveSchemaHelper.DB_DERBY, new String[] {
        "select t.labels from DATABASECHANGELOG t where CAST(t.id as INT) = (select MAX(CAST(t2.id AS INT)) from DATABASECHANGELOG t2)",
        "select t.SCHEMA_VERSION from VERSION t" });
    VERSION_QUERIES.put(HiveSchemaHelper.DB_MYSQL, new String[] {
        "select t.labels from DATABASECHANGELOG t where CAST(t.id as UNSIGNED) = (select MAX(CAST(t2.id AS UNSIGNED)) from DATABASECHANGELOG t2)",
        "select t.SCHEMA_VERSION from VERSION t" });
    VERSION_QUERIES.put(HiveSchemaHelper.DB_ORACLE, new String[] {
        "select t.labels from DATABASECHANGELOG t where CAST(t.id as INTEGER) = (select MAX(CAST(t2.id AS INTEGER)) from DATABASECHANGELOG t2)",
        "select t.SCHEMA_VERSION from VERSION t" });
    VERSION_QUERIES.put(HiveSchemaHelper.DB_MSSQL, new String[] {
        "select t.labels from DATABASECHANGELOG t where CAST(t.id as INT) = (select MAX(CAST(t2.id AS INT)) from DATABASECHANGELOG t2)",
        "select t.SCHEMA_VERSION from VERSION t" });
  }

  private final Liquibase liquibase;
  private final Contexts liquibaseContexts;

  @Override
  public List<String> getUnappliedScripts() throws HiveMetaException {
    return getScripPaths(false);
  }

  @Override
  public List<String> getAppliedScripts() throws HiveMetaException {
    return getScripPaths(true);
  }

  @Override
  public String getSchemaVersion() throws HiveMetaException {
    String schema = ( HiveSchemaHelper.DB_HIVE.equals(connectionInfo.getDbType()) ? "SYS" : null );

    Exception lastException = null;
    for(String versionQuery : VERSION_QUERIES.get(connectionInfo.getDbType().toLowerCase())) {
      try (Connection metastoreDbConnection = HiveSchemaHelper.getConnectionToMetastore(connectionInfo, conf, schema);
           Statement stmt = metastoreDbConnection.createStatement()) {
        ResultSet res = stmt.executeQuery(versionQuery);
        if (!res.next()) {
          throw new HiveMetaException("Could not find version info in metastore!");
        }
        String currentSchemaVersion = res.getString(1);
        if (res.next()) {
          throw new HiveMetaException("Multiple versions were found in metastore.");
        }
        return currentSchemaVersion;
      } catch (SQLException e) {
        lastException = e;
        //advance to next query
      }
    }
    throw new HiveMetaException("Failed to get schema version, Cause: " + (lastException == null ? "Unknown" : lastException.getMessage()));
  }

  private List<String> getScripPaths(boolean applied) throws HiveMetaException {
    try {
      List<ChangeSetStatus> appliedChanges = liquibase
          .getChangeSetStatuses(liquibaseContexts, null, false)
          .stream()
          .filter(css -> css.getPreviouslyRan() == applied)
          .collect(Collectors.toList());
      List<String> paths = new ArrayList<>();
      for(ChangeSetStatus css : appliedChanges) {
        paths.add(liquibase.getResourceAccessor().get(css.getChangeSet().getFilePath()).getUri().getPath());
      }
      return paths;
    } catch (LiquibaseException e) {
      throw new HiveMetaException("Liquibase failed to collect the script files.", e);
    } catch (IOException e) {
      throw new HiveMetaException("Liquibase failed to locate the script files.", e);
    }
  }

  public LiquibaseSchemaInfo(String metastoreHome, HiveSchemaHelper.MetaStoreConnectionInfo connectionInfo,
                             Configuration conf, Liquibase liquibase, Contexts liquibaseContexts) {
    super(metastoreHome, connectionInfo, conf);
    this.liquibase = liquibase;
    this.liquibaseContexts = liquibaseContexts;
  }

}
