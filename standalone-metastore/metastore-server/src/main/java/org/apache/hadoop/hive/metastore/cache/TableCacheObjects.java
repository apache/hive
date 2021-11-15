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
