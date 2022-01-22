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

package org.apache.hadoop.hive.ql.ddl.table.partition.drop;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... DROP PARTITION ... commands.
 */
@Explain(displayName = "Drop Partition", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableDropPartitionDesc implements DDLDescWithWriteId, Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * Partition description.
   */
  public static class PartitionDesc implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ExprNodeGenericFuncDesc partSpec;
    // TODO: see if we can get rid of this... used in one place to distinguish archived parts
    private final int prefixLength;

    public PartitionDesc(ExprNodeGenericFuncDesc partSpec, int prefixLength) {
      this.partSpec = partSpec;
      this.prefixLength = prefixLength;
    }

    public ExprNodeGenericFuncDesc getPartSpec() {
      return partSpec;
    }

    public int getPrefixLength() {
      return prefixLength;
    }
  }

  private final TableName tableName;
  private final ArrayList<PartitionDesc> partSpecs;
  private final boolean ifPurge;
  private final ReplicationSpec replicationSpec;
  private final boolean deleteData;
  private final boolean isTransactional;

  private long writeId = 0;

  public AlterTableDropPartitionDesc(TableName tableName, Map<Integer, List<ExprNodeGenericFuncDesc>> partSpecs,
      boolean ifPurge, ReplicationSpec replicationSpec) {
    this(tableName, partSpecs, ifPurge, replicationSpec, true, null);
  }

  public AlterTableDropPartitionDesc(TableName tableName, Map<Integer, List<ExprNodeGenericFuncDesc>> partSpecs,
      boolean ifPurge, ReplicationSpec replicationSpec, boolean deleteData, Table table) {
    this.tableName = tableName;
    this.partSpecs = new ArrayList<PartitionDesc>(partSpecs.size());
    for (Map.Entry<Integer, List<ExprNodeGenericFuncDesc>> partSpec : partSpecs.entrySet()) {
      int prefixLength = partSpec.getKey();
      for (ExprNodeGenericFuncDesc expr : partSpec.getValue()) {
        this.partSpecs.add(new PartitionDesc(expr, prefixLength));
      }
    }
    this.ifPurge = ifPurge;
    this.replicationSpec = replicationSpec == null ? new ReplicationSpec() : replicationSpec;
    this.isTransactional = AcidUtils.isTransactionalTable(table);
    this.deleteData = deleteData;
  }

  @Explain(displayName = "table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName.getNotEmptyDbTable();
  }

  public ArrayList<PartitionDesc> getPartSpecs() {
    return partSpecs;
  }

  public boolean getIfPurge() {
    return ifPurge;
  }

  /**
   * @return what kind of replication scope this drop is running under.
   * This can result in a "DROP IF OLDER THAN" kind of semantic
   */
  public ReplicationSpec getReplicationSpec() {
    return replicationSpec;
  }

  @Override
  public void setWriteId(long writeId) {
    this.writeId = writeId;
  }

  @Override
  public boolean mayNeedWriteId() {
    return isTransactional;
  }

  public long getWriteId() {
    return writeId;
  }

  public boolean getDeleteData() {
    return deleteData;
  }

  @Override
  public String getFullTableName() {
    return getTableName();
  }

}
