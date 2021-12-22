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

package org.apache.hadoop.hive.metastore.cache;

import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints;

import java.util.List;

/*
 * Holder class for table objects like partitions, statistics, constraints etc.
 */
public class TableCacheObjects {
  private SQLAllTableConstraints tableConstraints;
  private ColumnStatistics tableColStats;
  private List<Partition> partitions;
  private List<ColumnStatistics> partitionColStats;
  private AggrStats aggrStatsAllPartitions;
  private AggrStats aggrStatsAllButDefaultPartition;

  public ColumnStatistics getTableColStats() {
    return tableColStats;
  }

  public void setTableColStats(ColumnStatistics tableColStats) {
    this.tableColStats = tableColStats;
  }

  public List<Partition> getPartitions() {
    return partitions;
  }

  public void setPartitions(List<Partition> partitions) {
    this.partitions = partitions;
  }

  public List<ColumnStatistics> getPartitionColStats() {
    return partitionColStats;
  }

  public void setPartitionColStats(List<ColumnStatistics> partitionColStats) {
    this.partitionColStats = partitionColStats;
  }

  public AggrStats getAggrStatsAllPartitions() {
    return aggrStatsAllPartitions;
  }

  public void setAggrStatsAllPartitions(AggrStats aggrStatsAllPartitions) {
    this.aggrStatsAllPartitions = aggrStatsAllPartitions;
  }

  public AggrStats getAggrStatsAllButDefaultPartition() {
    return aggrStatsAllButDefaultPartition;
  }

  public void setAggrStatsAllButDefaultPartition(AggrStats aggrStatsAllButDefaultPartition) {
    this.aggrStatsAllButDefaultPartition = aggrStatsAllButDefaultPartition;
  }

  public SQLAllTableConstraints getTableConstraints() {
    return tableConstraints;
  }

  public void setTableConstraints(SQLAllTableConstraints tableConstraints) {
    this.tableConstraints = tableConstraints;
  }
}
