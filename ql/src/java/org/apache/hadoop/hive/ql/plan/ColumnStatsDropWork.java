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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * ColumnStatsDropWork implementation. ColumnStatsDropWork will drop the
 * colStats from the metastore. Work corresponds to statements like:
 * ALTER TABLE src_stat DROP STATISTICS for column key;
 * ALTER TABLE src_stat_part PARTITION(partitionId=100) DROP STATISTICS for column value;
 */
@Explain(displayName = "Column Stats Drop Work", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ColumnStatsDropWork implements Serializable, DDLDescWithWriteId {
  private static final long serialVersionUID = 1L;
  private final String partName;
  private final String dbName;
  private final String tableName;
  private final String colName;
  private final String colType;
  private final ColumnStatistics colStats;
  private long writeId;
  private boolean isReplication;
  private String dumpDirectory;
  private transient ReplicationMetricCollector metricCollector;

  public ColumnStatsDropWork(String partName,
      String dbName,
      String tableName,
      String colName,
      String colType) {
    this.partName = partName;
    this.dbName = dbName;
    this.tableName = tableName;
    this.colName = colName;
    this.colType = colType;
    this.colStats = null;
  }

  public ColumnStatsDropWork(ColumnStatistics colStats) {
    this.colStats = colStats;
    this.partName = null;
    this.dbName = null;
    this.tableName = null;
    this.colName = null;
    this.colType = null;
  }

  public ColumnStatsDropWork(ColumnStatistics colStats, String dumpRoot, ReplicationMetricCollector metricCollector,
                               boolean isReplication) {
    this.colStats = colStats;
    this.partName = null;
    this.dbName = null;
    this.tableName = null;
    this.colName = null;
    this.colType = null;
    this.dumpDirectory = dumpRoot;
    this.metricCollector = metricCollector;
    this.isReplication = true;
  }

  @Override
  public String toString() {
    return null;
  }

  public String getDumpDirectory() {
    return dumpDirectory;
  }

  public boolean isReplication() {
    return isReplication;
  }

  public String getPartName() {
    return partName;
  }

  public String dbName() {
    return dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getColName() {
    return colName;
  }

  public String getColType() {
    return colType;
  }

  public ColumnStatistics getColStats() { return colStats; }

  public ReplicationMetricCollector getMetricCollector() {
    return metricCollector;
  }


  @Override
  public void setWriteId(long writeId) {
    this.writeId = writeId;
  }

  public long getWriteId() { return writeId; }

  @Override
  public String getFullTableName() {
    return dbName + "." + tableName;
  }

  @Override
  public boolean mayNeedWriteId() {
    return true; // Checked at setup time; if this is called, the table is transactional.
  }
}
