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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * AlterDatabaseDesc.
 *
 */
@Explain(displayName = "Alter Database", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterDatabaseDesc extends DDLDesc implements Serializable {

  private static final long serialVersionUID = 1L;

  // Only altering the database property and owner is currently supported
  public static enum ALTER_DB_TYPES {
    ALTER_PROPERTY, ALTER_OWNER, ALTER_LOCATION
  };

  ALTER_DB_TYPES alterType;
  String databaseName;
  Map<String, String> dbProperties;
  PrincipalDesc ownerPrincipal;
  ReplicationSpec replicationSpec;
  String location;

  /**
   * For serialization only.
   */
  public AlterDatabaseDesc() {
  }

  public AlterDatabaseDesc(String databaseName, Map<String, String> dbProps,
                           ReplicationSpec replicationSpec) {
    super();
    this.databaseName = databaseName;
    this.replicationSpec = replicationSpec;
    this.setDatabaseProperties(dbProps);
    this.setAlterType(ALTER_DB_TYPES.ALTER_PROPERTY);
  }

  public AlterDatabaseDesc(String databaseName, PrincipalDesc ownerPrincipal,
                           ReplicationSpec replicationSpec) {
    this.databaseName = databaseName;
    this.replicationSpec = replicationSpec;
    this.setOwnerPrincipal(ownerPrincipal);
    this.setAlterType(ALTER_DB_TYPES.ALTER_OWNER);
  }

  public AlterDatabaseDesc(String databaseName, String newLocation) {
    this.databaseName = databaseName;
    this.setLocation(newLocation);
    this.setAlterType(ALTER_DB_TYPES.ALTER_LOCATION);
  }

  @Explain(displayName="properties")
  public Map<String, String> getDatabaseProperties() {
    return dbProperties;
  }

  public void setDatabaseProperties(Map<String, String> dbProps) {
    this.dbProperties = dbProps;
  }

  @Explain(displayName="name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  @Explain(displayName="owner")
  public PrincipalDesc getOwnerPrincipal() {
    return ownerPrincipal;
  }

  public void setOwnerPrincipal(PrincipalDesc ownerPrincipal) {
    this.ownerPrincipal = ownerPrincipal;
  }

  @Explain(displayName="location")
  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }
  public ALTER_DB_TYPES getAlterType() {
    return alterType;
  }

  public void setAlterType(ALTER_DB_TYPES alterType) {
    this.alterType = alterType;
  }

  /**
   * @return what kind of replication scope this alter is running under.
   * This can result in a "ALTER IF NEWER THAN" kind of semantic
   */
  public ReplicationSpec getReplicationSpec() {
    return this.replicationSpec;
  }
}
