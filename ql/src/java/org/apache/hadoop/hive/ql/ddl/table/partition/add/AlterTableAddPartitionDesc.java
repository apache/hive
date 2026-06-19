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

package org.apache.hadoop.hive.ql.ddl.table.partition.add;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... ADD PARTITION ... commands.
 */
@Explain(displayName = "Add Partition", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableAddPartitionDesc implements DDLDescWithWriteId, Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * Description of a partition to add.
   */
  @Explain(displayName = "Partition", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public static class PartitionDesc {
    private final Map<String, String> partitionSpec;
    private String location; // TODO: make location final too
    private final Map<String, String> params;
    private final String inputFormat;
    private final String outputFormat;
    private final int numBuckets;
    private final List<FieldSchema> columns;
    private final String serializationLib;
    private final Map<String, String> serdeParams;
    private final List<String> bucketColumns;
    private final List<Order> sortColumns;
    private final ColumnStatistics columnStats;
    private final long writeId;

    public PartitionDesc(Map<String, String> partitionSpec, String location, Map<String, String> params) {
      this(partitionSpec, location, params, null, null, -1, null, null, null, null, null, null, -1);
    }

    public PartitionDesc(Map<String, String> partitionSpec, String location, Map<String, String> params,
        String inputFormat, String outputFormat, int numBuckets, List<FieldSchema> columns, String serializationLib,
        Map<String, String> serdeParams, List<String> bucketColumns, List<Order> sortColumns,
        ColumnStatistics columnStats, long writeId) {
      this.partitionSpec = partitionSpec;
      this.location = location;
      this.params = params;
      this.inputFormat = inputFormat;
      this.outputFormat = outputFormat;
      this.numBuckets = numBuckets;
      this.columns = columns;
      this.serializationLib = serializationLib;
      this.serdeParams = serdeParams;
      this.bucketColumns = bucketColumns;
      this.sortColumns = sortColumns;
      this.columnStats = columnStats;
      this.writeId = writeId;
    }

    public Map<String, String> getPartSpec() {
      return partitionSpec;
    }

    @Explain(displayName = "partition spec", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
    public String getPartSpecForExplain() {
      return partitionSpec.toString();
    }

    /**
     * @return location of partition in relation to table
     */
    @Explain(displayName = "location", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
    public String getLocation() {
      return location;
    }

    public void setLocation(String location) {
      this.location = location;
    }

    public Map<String, String> getPartParams() {
      return params;
    }

    public void addPartParams(Map<String, String> partParams) {
      if (params != null) {
        params.putAll(partParams);
      }
    }

    @Explain(displayName = "params", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
    public String getPartParamsForExplain() {
      return params.toString();
    }

    @Explain(displayName = "input format", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
    public String getInputFormat() {
      return inputFormat;
    }

    @Explain(displayName = "output format", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
    public String getOutputFormat() {
      return outputFormat;
    }

    public int getNumBuckets() {
      return numBuckets;
    }

    @Explain(displayName = "num buckets", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
    public Integer getNumBucketsExplain() {
      return numBuckets == -1 ? null : numBuckets;
    }

    @Explain(displayName = "columns", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
    public List<FieldSchema> getCols() {
      return columns;
    }

    @Explain(displayName = "serialization lib", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
    public String getSerializationLib() {
      return serializationLib;
    }

    @Explain(displayName = "serde params", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
    public Map<String, String> getSerdeParams() {
      return serdeParams;
    }

    @Explain(displayName = "bucket columns", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
    public List<String> getBucketCols() {
      return bucketColumns;
    }

    @Explain(displayName = "sort columns", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
    public List<Order> getSortCols() {
      return sortColumns;
    }

    @Explain(displayName = "column stats", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
    public ColumnStatistics getColStats() {
      return columnStats;
    }

    public long getWriteId() {
      return writeId;
    }
  }

  private final String dbName;
  private final String tableName;
  private final boolean ifNotExists;
  private final List<PartitionDesc> partitions;
  private Long writeId;

  private ReplicationSpec replicationSpec = null; // TODO: make replicationSpec final too

  public AlterTableAddPartitionDesc(String dbName, String tableName, boolean ifNotExists,
      List<PartitionDesc> partitions) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.ifNotExists = ifNotExists;
    this.partitions = partitions;
  }

  @Explain(displayName = "db name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDbName() {
    return dbName;
  }

  @Explain(displayName = "table name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  @Explain(displayName = "if not exists", displayOnlyOnTrue = true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isIfNotExists() {
    return ifNotExists;
  }

  @Explain(displayName = "partitions", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<PartitionDesc> getPartitions() {
    return partitions;
  }

  /**
   * @param replicationSpec Sets the replication spec governing this create.
   * This parameter will have meaningful values only for creates happening as a result of a replication.
   */
  public void setReplicationSpec(ReplicationSpec replicationSpec) {
    this.replicationSpec = replicationSpec;
  }

  /**
   * @return what kind of replication scope this drop is running under.
   * This can result in a "CREATE/REPLACE IF NEWER THAN" kind of semantic
   */
  public ReplicationSpec getReplicationSpec() {
    if (replicationSpec == null) {
      this.replicationSpec = new ReplicationSpec();
    }
    return replicationSpec;
  }

  @Override
  public void setWriteId(long writeId) {
    this.writeId = writeId;
  }

  @Override
  public String getFullTableName() {
    return AcidUtils.getFullTableName(dbName, tableName);
  }

  @Override
  public boolean mayNeedWriteId() {
    return true;
  }

}
