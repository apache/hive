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

package org.apache.hadoop.hive.ql.ddl.table.partition.alter;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... PARTITION COLUMN ... commands.
 */
@Explain(displayName = "Alter Partition", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableAlterPartitionDesc implements DDLDescWithWriteId {
  public static final long serialVersionUID = 1;

  private final String fqTableName;
  private final FieldSchema partKeySpec;

  public AlterTableAlterPartitionDesc(String fqTableName, FieldSchema partKeySpec) {
    this.fqTableName = fqTableName;
    this.partKeySpec = partKeySpec;
  }

  @Explain(displayName = "table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return fqTableName;
  }

  public FieldSchema getPartKeySpec() {
    return partKeySpec;
  }

  @Explain(displayName = "partition key name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPartKeyName() {
    return partKeySpec.getName();
  }

  @Explain(displayName = "partition key type", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPartKeyType() {
    return partKeySpec.getType();
  }

  @Override
  public void setWriteId(long writeId) {
    // We don't actually need the write id, but by implementing DDLDescWithWriteId it ensures that it is allocated
  }

  @Override
  public String getFullTableName() {
    return fqTableName;
  }

  @Override
  public boolean mayNeedWriteId() {
    return true; // Checked before setting as the acid desc.
  }
}
