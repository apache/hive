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

package org.apache.hadoop.hive.ql.ddl.database.drop;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for DROP DATABASE commands.
 */
@Explain(displayName = "Drop Database", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DropDatabaseDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private final String databaseName;
  private final boolean ifExists;
  private final boolean cascade;
  private final ReplicationSpec replicationSpec;
  
  private boolean deleteData = true;

  public DropDatabaseDesc(String databaseName, boolean ifExists, ReplicationSpec replicationSpec) {
    this(databaseName, ifExists, false, replicationSpec);
  }

  public DropDatabaseDesc(String databaseName, boolean ifExists, boolean cascade, ReplicationSpec replicationSpec) {
    this.databaseName = databaseName;
    this.ifExists = ifExists;
    this.cascade = cascade;
    this.replicationSpec = replicationSpec;
  }

  public DropDatabaseDesc(String databaseName, boolean ifExists, boolean cascade, boolean deleteData) {
    this(databaseName, ifExists, cascade, null);
    this.deleteData = deleteData;
  }

  @Explain(displayName = "database", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDatabaseName() {
    return databaseName;
  }

  @Explain(displayName = "if exists")
  public boolean getIfExists() {
    return ifExists;
  }

  public boolean isCasdade() {
    return cascade;
  }

  public ReplicationSpec getReplicationSpec() {
    return replicationSpec;
  }
  
  public boolean isDeleteData() {
    return deleteData;
  }
}
