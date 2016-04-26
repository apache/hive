/**
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

package org.apache.hadoop.hive.metastore.events;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class AddPartitionEvent extends ListenerEvent {

  private final Table table;
  private final List<Partition> partitions;
  private PartitionSpecProxy partitionSpecProxy;

  public AddPartitionEvent(Table table, List<Partition> partitions, boolean status, HMSHandler handler) {
    super(status, handler);
    this.table = table;
    this.partitions = partitions;
    this.partitionSpecProxy = null;
  }

  public AddPartitionEvent(Table table, Partition partition, boolean status, HMSHandler handler) {
    this(table, Arrays.asList(partition), status, handler);
  }

  /**
   * Alternative constructor to use PartitionSpec APIs.
   */
  public AddPartitionEvent(Table table, PartitionSpecProxy partitionSpec, boolean status, HMSHandler handler) {
    super(status, handler);
    this.table = table;
    this.partitions = null;
    this.partitionSpecProxy = partitionSpec;
  }

  /**
   * @return The table.
   */
  public Table getTable() {
    return table;
  }

  /**
   * @return List of partitions.
   * CDH-ONLY FIX
   * The method was removed upstream in Hive 1.2 in HIVE-9609.
   * And replaced by getPartitionIterator() for memory efficiency.
   * Additionally, the new method fixes a bug for the case where
   * we can end up returning null when AddPartitionEvent was
   * initialized on a PartitionSpec rather than a List<Partition>.
   * Because we cannot change this public API in CDH5, we are
   * adding the bug fix in the old API.
   * When we move to CDH6, we no longer needed this CDH-ONLY fix
   * since at that point, it is ok to break API compatibility.
   * Sentry and possibly customer code extending MetaStoreEventListener
   * is vulnerable to this change.
   */
  public List<Partition> getPartitions() {
    if (partitions != null) {
      return partitions;
    } else  if (partitionSpecProxy != null){
      return ImmutableList.copyOf(partitionSpecProxy.getPartitionIterator());
    } else {
      return null;
    }
  }

  /**
   * @return Iterator for partitions.
   */
  public Iterator<Partition> getPartitionIterator() {
    return partitionSpecProxy == null ? null : partitionSpecProxy.getPartitionIterator();
  }

}
