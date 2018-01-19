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

package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * The list of pruned partitions.
 */
public class PrunedPartitionList {

  /** Key to identify this partition list. */
  private final String ppListKey;

  /** Source table. */
  private final Table source;

  /** Partitions that either satisfy the partition criteria, or may satisfy it. */
  private Set<Partition> partitions;

  /** partition columns referred by pruner expr */
  private List<String> referred;

  /** Whether there are partitions in the list that may or may not satisfy the criteria. */
  private boolean hasUnknowns;

  public PrunedPartitionList(Table source, Set<Partition> partitions,
      List<String> referred, boolean hasUnknowns) {
    this.source = source;
    this.ppListKey = null;
    this.referred = referred;
    this.partitions = partitions;
    this.hasUnknowns = hasUnknowns;
  }

  public PrunedPartitionList(Table source, String key, Set<Partition> partitions,
      List<String> referred, boolean hasUnknowns) {
    this.source = source;
    this.ppListKey = key;
    this.referred = referred;
    this.partitions = partitions;
    this.hasUnknowns = hasUnknowns;
  }

  public Table getSourceTable() {
    return source;
  }

  public String getKey() {
    return ppListKey;
  }

  /**
   * @return partitions
   */
  public Set<Partition> getPartitions() {
    return partitions;
  }


  /**
   * @return all partitions.
   */
  public List<Partition> getNotDeniedPartns() {
    return new ArrayList<Partition>(partitions);
  }

  /**
   * @return Whether there are unknown partitions in {@link #getPartitions()} result.
   */
  public boolean hasUnknownPartitions() {
    return hasUnknowns;
  }

  public List<String> getReferredPartCols() {
    return referred;
  }
}
