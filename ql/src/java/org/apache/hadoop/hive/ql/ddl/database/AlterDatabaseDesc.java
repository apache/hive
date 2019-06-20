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

package org.apache.hadoop.hive.ql.ddl.database;

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.PrincipalDesc;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER DATABASE commands.
 */
@Explain(displayName = "Alter Database", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterDatabaseDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * Supported type of alter db commands.
   * Only altering the database property and owner is currently supported
   */
  public enum AlterDbType {
    ALTER_PROPERTY, ALTER_OWNER, ALTER_LOCATION
  };

  private final AlterDbType alterType;
  private final String databaseName;
  private final Map<String, String> dbProperties;
  private final ReplicationSpec replicationSpec;
  private final PrincipalDesc ownerPrincipal;
  private final String location;

  public AlterDatabaseDesc(String databaseName, Map<String, String> dbProperties, ReplicationSpec replicationSpec) {
    this.alterType = AlterDbType.ALTER_PROPERTY;
    this.databaseName = databaseName;
    this.dbProperties = dbProperties;
    this.replicationSpec = replicationSpec;
    this.ownerPrincipal = null;
    this.location = null;
  }

  public AlterDatabaseDesc(String databaseName, PrincipalDesc ownerPrincipal, ReplicationSpec replicationSpec) {
    this.alterType = AlterDbType.ALTER_OWNER;
    this.databaseName = databaseName;
    this.dbProperties = null;
    this.replicationSpec = replicationSpec;
    this.ownerPrincipal = ownerPrincipal;
    this.location = null;
  }

  public AlterDatabaseDesc(String databaseName, String location) {
    this.alterType = AlterDbType.ALTER_LOCATION;
    this.databaseName = databaseName;
    this.dbProperties = null;
    this.replicationSpec = null;
    this.ownerPrincipal = null;
    this.location = location;
  }

  public AlterDbType getAlterType() {
    return alterType;
  }

  @Explain(displayName="name", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDatabaseName() {
    return databaseName;
  }

  @Explain(displayName="properties")
  public Map<String, String> getDatabaseProperties() {
    return dbProperties;
  }

  /**
   * @return what kind of replication scope this alter is running under.
   * This can result in a "ALTER IF NEWER THAN" kind of semantic
   */
  public ReplicationSpec getReplicationSpec() {
    return this.replicationSpec;
  }

  @Explain(displayName="owner")
  public PrincipalDesc getOwnerPrincipal() {
    return ownerPrincipal;
  }

  @Explain(displayName="location")
  public String getLocation() {
    return location;
  }
}
