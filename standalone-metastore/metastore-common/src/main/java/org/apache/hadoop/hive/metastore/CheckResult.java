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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Result class used by the HiveMetaStoreChecker.
 */
public class CheckResult {

  // tree sets to preserve ordering in qfile tests
  private Set<String> tablesNotOnFs = new TreeSet<>();
  private Set<String> tablesNotInMs = new TreeSet<>();
  private Set<PartitionResult> partitionsNotOnFs = new TreeSet<>();
  private Set<PartitionResult> partitionsNotInMs = new TreeSet<>();
  private Set<PartitionResult> expiredPartitions = new TreeSet<>();
  private Set<PartitionResult> correctPartitions = new TreeSet<>();
  private long maxWriteId;
  private long maxTxnId;

  /**
   * @return a list of tables not found on the filesystem.
   */
  public Set<String> getTablesNotOnFs() {
    return tablesNotOnFs;
  }

  /**
   * @param tablesNotOnFs
   *          a list of tables not found on the filesystem.
   */
  public void setTablesNotOnFs(Set<String> tablesNotOnFs) {
    this.tablesNotOnFs = tablesNotOnFs;
  }

  /**
   * @return a list of tables not found in the metastore.
   */
  public Set<String> getTablesNotInMs() {
    return tablesNotInMs;
  }

  /**
   * @param tablesNotInMs
   *          a list of tables not found in the metastore.
   */
  public void setTablesNotInMs(Set<String> tablesNotInMs) {
    this.tablesNotInMs = tablesNotInMs;
  }

  /**
   * @return a list of partitions not found on the fs
   */
  public Set<PartitionResult> getPartitionsNotOnFs() {
    return partitionsNotOnFs;
  }

  /**
   * @param partitionsNotOnFs
   *          a list of partitions not found on the fs
   */
  public void setPartitionsNotOnFs(Set<PartitionResult> partitionsNotOnFs) {
    this.partitionsNotOnFs = partitionsNotOnFs;
  }

  /**
   * @return a list of partitions not found in the metastore
   */
  public Set<PartitionResult> getPartitionsNotInMs() {
    return partitionsNotInMs;
  }

  /**
   * @param partitionsNotInMs
   *          a list of partitions not found in the metastore
   */
  public void setPartitionsNotInMs(Set<PartitionResult> partitionsNotInMs) {
    this.partitionsNotInMs = partitionsNotInMs;
  }

  public Set<PartitionResult> getExpiredPartitions() {
    return expiredPartitions;
  }

  public void setExpiredPartitions(final Set<PartitionResult> expiredPartitions) {
    this.expiredPartitions = expiredPartitions;
  }

  public Set<PartitionResult> getCorrectPartitions() {
    return this.correctPartitions;
  }

  public void setCorrectPartitions(final Set<PartitionResult> correctPartitions) {
    this.correctPartitions = correctPartitions;
  }

  public long getMaxWriteId() {
    return maxWriteId;
  }

  public void setMaxWriteId(long maxWriteId) {
    this.maxWriteId = maxWriteId;
  }

  public long getMaxTxnId() {
    return maxTxnId;
  }

  public void setMaxTxnId(long maxTxnId) {
    this.maxTxnId = maxTxnId;
  }

  /**
   * A basic description of a partition that is missing from either the fs or
   * the ms.
   */
  public static class PartitionResult implements Comparable<PartitionResult> {
    private String partitionName;
    private String tableName;
    private Path path;
    private long maxWriteId;
    private long maxTxnId;

    /**
     * @return name of partition
     */
    public String getPartitionName() {
      return partitionName;
    }

    /**
     * @param partitionName
     *          name of partition
     */
    public void setPartitionName(String partitionName) {
      this.partitionName = partitionName;
    }

    /**
     * @return table name
     */
    public String getTableName() {
      return tableName;
    }

    /**
     * @param tableName
     *          table name
     */
    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public void setPath(Path path) {
      this.path = path;
    }

    public Path getLocation(Path tablePath, Map<String, String> partSpec) throws MetaException {
      if (this.path == null) {
        return new Path(tablePath, Warehouse.makePartPath(partSpec));
      }

      return this.path;
    }

    public long getMaxWriteId() {
      return maxWriteId;
    }

    public void setMaxWriteId(long maxWriteId) {
      this.maxWriteId = maxWriteId;
    }

    public long getMaxTxnId() {
      return maxTxnId;
    }

    public void setMaxTxnId(long maxTxnId) {
      this.maxTxnId = maxTxnId;
    }

    @Override
    public String toString() {
      return tableName + ":" + partitionName;
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof PartitionResult) {
        if (0 == compareTo((PartitionResult)other)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    public int compareTo(PartitionResult o) {
      int ret = tableName.compareTo(o.tableName);
      return ret != 0 ? ret : partitionName.compareTo(o.partitionName);
    }
  }

}
