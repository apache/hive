package org.apache.hadoop.hive.metastore.cache;

import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;

import java.util.List;

/*
 * Holder class for table objects like partitions, statistics, constraints etc.
 */
public class TableCacheObjects {
  private List<SQLPrimaryKey> primaryKeys;
  private List<SQLForeignKey> foreignKeys;
  private List<SQLNotNullConstraint> notNullConstraints;
  private List<SQLUniqueConstraint> uniqueConstraints;
  private ColumnStatistics tableColStats;
  private List<Partition> partitions;
  private List<ColumnStatistics> partitionColStats;
  private AggrStats aggrStatsAllPartitions;
  private AggrStats aggrStatsAllButDefaultPartition;

  public List<SQLPrimaryKey> getPrimaryKeys() {
    return primaryKeys;
  }

  public void setPrimaryKeys(List<SQLPrimaryKey> primaryKeys) {
    this.primaryKeys = primaryKeys;
  }

  public List<SQLForeignKey> getForeignKeys() {
    return foreignKeys;
  }

  public void setForeignKeys(List<SQLForeignKey> foreignKeys) {
    this.foreignKeys = foreignKeys;
  }

  public List<SQLNotNullConstraint> getNotNullConstraints() {
    return notNullConstraints;
  }

  public void setNotNullConstraints(List<SQLNotNullConstraint> notNullConstraints) {
    this.notNullConstraints = notNullConstraints;
  }

  public List<SQLUniqueConstraint> getUniqueConstraints() {
    return uniqueConstraints;
  }

  public void setUniqueConstraints(List<SQLUniqueConstraint> uniqueConstraints) {
    this.uniqueConstraints = uniqueConstraints;
  }

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
}
