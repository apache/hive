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

package org.apache.hadoop.hive.ql.ddl.database.create;

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.DatabaseType;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import javax.xml.crypto.Data;

/**
 * DDL task description for CREATE DATABASE commands.
 */
@Explain(displayName = "Create Database", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateDatabaseDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private final String databaseName;
  private final String comment;
  private final String locationUri;
  private final String managedLocationUri;
  private final boolean ifNotExists;
  private final DatabaseType dbType;
  private final String connectorName;
  private final String remoteDbName;
  private final Map<String, String> dbProperties;

  public CreateDatabaseDesc(String databaseName, String comment, String locationUri, String managedLocationUri,
      boolean ifNotExists, Map<String, String> dbProperties) {
    this(databaseName, comment, locationUri, managedLocationUri, ifNotExists, dbProperties, "NATIVE", null, null);
  }

  public CreateDatabaseDesc(String databaseName, String comment, String locationUri, String managedLocationUri,
      boolean ifNotExists, Map<String, String> dbProperties, String dbtype, String connectorName, String remoteDbName) {
    this.databaseName = databaseName;
    this.comment = comment;
    if (dbtype != null && dbtype.equalsIgnoreCase("REMOTE")) {
      this.dbType = DatabaseType.REMOTE;
      this.connectorName = connectorName;
      this.remoteDbName = remoteDbName;
      this.locationUri = null;
      this.managedLocationUri = null;
    } else {
      this.dbType = DatabaseType.NATIVE;
      this.locationUri = locationUri;
      this.managedLocationUri = managedLocationUri;
      this.connectorName = null;
      this.remoteDbName = null;
    }
    this.ifNotExists = ifNotExists;
    this.dbProperties = dbProperties;
  }

  @Explain(displayName="if not exists", displayOnlyOnTrue = true)
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public Map<String, String> getDatabaseProperties() {
    return dbProperties;
  }

  @Explain(displayName="name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getName() {
    return databaseName;
  }

  @Explain(displayName="comment")
  public String getComment() {
    return comment;
  }

  @Explain(displayName="locationUri")
  public String getLocationUri() {
    return locationUri;
  }

  @Explain(displayName="managed location uri")
  public String getManagedLocationUri() {
    return managedLocationUri;
  }

  @Explain(displayName="database type")
  public DatabaseType getDatabaseType() {
    return dbType;
  }

  @Explain(displayName="connector name")
  public String getConnectorName() {
    return connectorName;
  }

  @Explain(displayName="remote database name")
  public String getRemoteDbName() {
    return remoteDbName;
  }
}
