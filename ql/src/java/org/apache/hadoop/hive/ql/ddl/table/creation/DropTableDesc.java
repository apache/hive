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

package org.apache.hadoop.hive.ql.ddl.table.creation;

import java.io.Serializable;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for DROP TABLE commands.
 */
@Explain(displayName = "Drop Table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DropTableDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private final String tableName;
  private final TableType expectedType;
  private final boolean ifExists;
  private final boolean ifPurge;
  private final ReplicationSpec replicationSpec;
  private final boolean validationRequired;

  public DropTableDesc(String tableName, TableType expectedType, boolean ifExists, boolean ifPurge,
      ReplicationSpec replicationSpec) {
    this(tableName, expectedType, ifExists, ifPurge, replicationSpec, true);
  }

  public DropTableDesc(String tableName, TableType expectedType, boolean ifExists, boolean ifPurge,
      ReplicationSpec replicationSpec, boolean validationRequired) {
    this.tableName = tableName;
    this.expectedType = expectedType;
    this.ifExists = ifExists;
    this.ifPurge = ifPurge;
    this.replicationSpec = replicationSpec == null ? new ReplicationSpec() : replicationSpec;
    this.validationRequired = validationRequired;
  }

  @Explain(displayName = "table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  public boolean getExpectView() {
    return expectedType != null && expectedType == TableType.VIRTUAL_VIEW;
  }

  public boolean getExpectMaterializedView() {
    return expectedType != null && expectedType == TableType.MATERIALIZED_VIEW;
  }

  public boolean getIfExists() {
    return ifExists;
  }

  public boolean getIfPurge() {
    return ifPurge;
  }

  /**
   * @return what kind of replication scope this drop is running under.
   * This can result in a "DROP IF OLDER THAN" kind of semantic
   */
  public ReplicationSpec getReplicationSpec(){
    return this.replicationSpec;
  }

  public boolean getValidationRequired(){
    return this.validationRequired;
  }
}
