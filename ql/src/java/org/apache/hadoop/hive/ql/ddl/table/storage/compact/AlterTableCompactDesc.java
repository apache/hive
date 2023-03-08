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

package org.apache.hadoop.hive.ql.ddl.table.storage.compact;

import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... [PARTITION ... ] COMPACT 'major|minor' [POOL 'poolname'] [WITH OVERWRITE TBLPROPERTIES ...]
 * commands.
 */
@Explain(displayName = "Compact", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableCompactDesc extends AbstractAlterTableDesc implements DDLDescWithWriteId {
  private final String tableName;
  private final Map<String, String> partitionSpec;
  private final String compactionType;
  private final boolean isBlocking;
  private final String poolName;
  private final int numberOfBuckets;
  private final Map<String, String> properties;
  private final String orderByClause;
  private Long writeId;

  public AlterTableCompactDesc(TableName tableName, Map<String, String> partitionSpec, String compactionType,
      boolean isBlocking, String poolName, int numberOfBuckets, Map<String, String> properties, String orderByClause)
      throws SemanticException{
    super(AlterTableType.COMPACT, tableName, partitionSpec, null, false, false, properties);
    this.tableName = tableName.getNotEmptyDbTable();
    this.partitionSpec = partitionSpec;
    this.compactionType = compactionType;
    this.isBlocking = isBlocking;
    this.poolName = poolName;
    this.numberOfBuckets = numberOfBuckets;
    this.properties = properties;
    this.orderByClause = orderByClause;
  }

  @Explain(displayName = "table name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  @Explain(displayName = "partition spec", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<String, String> getPartitionSpec() {
    return partitionSpec;
  }

  @Explain(displayName = "compaction type", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getCompactionType() {
    return compactionType;
  }

  @Explain(displayName = "blocking", displayOnlyOnTrue = true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isBlocking() {
    return isBlocking;
  }

  @Explain(displayName = "pool", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPoolName() {
    return poolName;
  }

  @Explain(displayName = "numberOfBuckets", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public int getNumberOfBuckets() {
    return numberOfBuckets;
  }

  @Explain(displayName = "properties", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<String, String> getProperties() {
    return properties;
  }

  @Explain(displayName = "order by", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getOrderByClause() {
    return orderByClause;
  }

  @Override
  public void setWriteId(long writeId) {
    this.writeId = writeId;
  }

  @Override
  public String getFullTableName() {
    return tableName;
  }

  @Override
  public boolean mayNeedWriteId() {
    return true;
  }
}
