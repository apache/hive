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
package org.apache.hadoop.hive.ql.metadata;

import java.util.Set;
import java.util.TreeSet;

/**
 * Result class used by the HiveMetaStoreChecker.
 */
public class CheckResult {

  private Set<String> tablesNotOnFs = new TreeSet<String>();
  private Set<String> tablesNotInMs = new TreeSet<String>();
  private Set<PartitionResult> partitionsNotOnFs = new TreeSet<PartitionResult>();
  private Set<PartitionResult> partitionsNotInMs = new TreeSet<PartitionResult>();

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

  /**
   * A basic description of a partition that is missing from either the fs or
   * the ms.
   */
  public static class PartitionResult implements Comparable<PartitionResult> {
    private String partitionName;
    private String tableName;

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

    @Override
    public String toString() {
      return tableName + ":" + partitionName;
    }

    public int compareTo(PartitionResult o) {
      int ret = tableName.compareTo(o.tableName);
      return ret != 0 ? ret : partitionName.compareTo(o.partitionName);
    }
  }

}
