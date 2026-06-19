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

package org.apache.hadoop.hive.ql.ddl.table;

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * Abstract ancestor of all ALTER TABLE descriptors that are handled by the AlterTableWithWriteIdOperations framework.
 */
public abstract class AbstractAlterTableDesc implements DDLDescWithWriteId, Serializable {
  private static final long serialVersionUID = 1L;

  private final AlterTableType type;
  private final TableName tableName;
  private final Map<String, String> partitionSpec;
  private final ReplicationSpec replicationSpec;
  private final boolean isCascade;
  private final boolean expectView;
  private final Map<String, String> props;

  private Long writeId;

  public AbstractAlterTableDesc(AlterTableType type, TableName tableName, Map<String, String> partitionSpec,
      ReplicationSpec replicationSpec, boolean isCascade, boolean expectView, Map<String, String> props)
      throws SemanticException {
    this.type = type;
    this.tableName = tableName;
    this.partitionSpec = partitionSpec;
    this.replicationSpec = replicationSpec;
    this.isCascade = isCascade;
    this.expectView = expectView;
    this.props = props;
  }

  public AlterTableType getType() {
    return type;
  }

  @Explain(displayName = "table name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDbTableName() {
    return tableName.getNotEmptyDbTable();
  }

  @Explain(displayName = "partition", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<String, String> getPartitionSpec() {
    return partitionSpec;
  }

  public ReplicationSpec getReplicationSpec() {
    return replicationSpec;
  }

  @Explain(displayName = "cascade", displayOnlyOnTrue = true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isCascade() {
    return isCascade;
  }

  public boolean expectView() {
    return expectView;
  }

  @Explain(displayName = "properties", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<String, String> getProps() {
    return props;
  }

  public EnvironmentContext getEnvironmentContext() {
    return null;
  };

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
}
