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

package org.apache.hadoop.hive.ql.ddl.table.constraint.drop;

import java.io.Serializable;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... DROP CONSTRAINT ... commands.
 */
@Explain(displayName = "Drop Constraint", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableDropConstraintDesc implements DDLDescWithWriteId, Serializable {
  private static final long serialVersionUID = 1L;

  private final TableName tableName;
  private final ReplicationSpec replicationSpec;
  private final String constraintName;
  private Long writeId;

  public AlterTableDropConstraintDesc(TableName tableName, ReplicationSpec replicationSpec, String constraintName)
      throws SemanticException  {
    this.tableName = tableName;
    this.replicationSpec = replicationSpec;
    this.constraintName = constraintName;
  }

  public TableName getTableName() {
    return tableName;
  }

  @Explain(displayName = "table name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDbTableName() {
    return tableName.getNotEmptyDbTable();
  }

  public ReplicationSpec getReplicationSpec() {
    return replicationSpec;
  }

  @Explain(displayName = "constraint name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getConstraintName() {
    return constraintName;
  }

  @Override
  public String getFullTableName() {
    return tableName.getNotEmptyDbTable();
  }

  @Override
  public void setWriteId(long writeId) {
    this.writeId = writeId;
  }

  public Long getWriteId() {
    return writeId;
  }

  @Override
  public boolean mayNeedWriteId() {
    return true;
  }
}
