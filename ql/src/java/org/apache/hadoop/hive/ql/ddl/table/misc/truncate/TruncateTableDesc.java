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

package org.apache.hadoop.hive.ql.ddl.table.misc.truncate;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for TRUNCATE TABLE commands.
 */
@Explain(displayName = "Truncate Table or Partition", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class TruncateTableDesc implements DDLDescWithWriteId, Serializable {
  private static final long serialVersionUID = 1L;

  private final TableName tableName;
  private final Map<String, String> partSpec;
  private final ReplicationSpec replicationSpec;
  private final boolean isTransactional;
  private final List<Integer> columnIndexes;
  private final Path inputDir;
  private final Path outputDir;
  private final ListBucketingCtx lbCtx;

  private long writeId = 0;

  public TruncateTableDesc(TableName tableName, Map<String, String> partSpec, ReplicationSpec replicationSpec) {
    this(tableName, partSpec, replicationSpec, null);
  }

  public TruncateTableDesc(TableName tableName, Map<String, String> partSpec, ReplicationSpec replicationSpec,
      Table table) {
    this(tableName, partSpec, replicationSpec, table, null, null, null, null);
  }

  public TruncateTableDesc(TableName tableName, Map<String, String> partSpec, ReplicationSpec replicationSpec,
      Table table, List<Integer> columnIndexes, Path inputDir, Path outputDir, ListBucketingCtx lbCtx) {
    this.tableName = tableName;
    this.partSpec = partSpec;
    this.replicationSpec = replicationSpec;
    this.isTransactional = AcidUtils.isTransactionalTable(table);
    this.columnIndexes = columnIndexes;
    this.inputDir = inputDir;
    this.outputDir = outputDir;
    this.lbCtx = lbCtx;
  }

  @Explain(displayName = "table name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName.getNotEmptyDbTable();
  }

  @Override
  public String getFullTableName() {
    return tableName.getNotEmptyDbTable();
  }

  @Explain(displayName = "partition spec", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<String, String> getPartSpec() {
    return partSpec;
  }

  /**
   * @return what kind of replication scope this truncate is running under.
   * This can result in a "TRUNCATE IF NEWER THAN" kind of semantic
   */
  public ReplicationSpec getReplicationSpec() {
    return replicationSpec;
  }

  @Explain(displayName = "column indexes")
  public List<Integer> getColumnIndexes() {
    return columnIndexes;
  }

  public Path getInputDir() {
    return inputDir;
  }

  public Path getOutputDir() {
    return outputDir;
  }

  public ListBucketingCtx getLbCtx() {
    return lbCtx;
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

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " for " + getFullTableName();
  }
}
