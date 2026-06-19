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

package org.apache.hadoop.hive.ql.ddl.table.partition.rename;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... PARTITION ... RENAME TO PARTITION ... commands.
 */
@Explain(displayName = "Rename Partition", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableRenamePartitionDesc implements DDLDescWithWriteId, Serializable {
  private static final long serialVersionUID = 1L;

  private final TableName tableName;
  private final Map<String, String> oldPartSpec;
  private final Map<String, String> newPartSpec;
  private final ReplicationSpec replicationSpec;

  private long writeId;

  public AlterTableRenamePartitionDesc(TableName tableName, Map<String, String> oldPartSpec,
      Map<String, String> newPartSpec, ReplicationSpec replicationSpec, Table table) {
    this.tableName = tableName;
    this.oldPartSpec = new LinkedHashMap<String, String>(oldPartSpec);
    this.newPartSpec = new LinkedHashMap<String, String>(newPartSpec);
    this.replicationSpec = replicationSpec;
  }

  @Explain(displayName = "table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName.getNotEmptyDbTable();
  }

  @Explain(displayName = "old partitions", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<String, String> getOldPartSpec() {
    return oldPartSpec;
  }

  @Explain(displayName = "new partitions", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<String, String> getNewPartSpec() {
    return newPartSpec;
  }

  /**
   * @return what kind of replication scope this rename is running under.
   * This can result in a "RENAME IF NEWER THAN" kind of semantic
   */
  @Explain(displayName = "replication", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public ReplicationSpec getReplicationSpec() {
    return this.replicationSpec;
  }

  @Override
  public void setWriteId(long writeId) {
    this.writeId = writeId;
  }

  public long getWriteId() {
    return writeId;
  }

  @Override
  public String getFullTableName() {
    return tableName.getNotEmptyDbTable();
  }

  @Override
  public boolean mayNeedWriteId() {
    return true; // The check is done when setting this as the ACID DDLDesc.
  }
}
