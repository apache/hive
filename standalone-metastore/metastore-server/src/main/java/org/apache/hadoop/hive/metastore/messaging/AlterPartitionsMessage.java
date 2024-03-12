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
package org.apache.hadoop.hive.metastore.messaging;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

public abstract class AlterPartitionsMessage extends EventMessage {

  protected AlterPartitionsMessage() {
    super(EventType.ALTER_PARTITIONS);
  }

  public abstract String getTable();

  public abstract String getTableType();

  public abstract Table getTableObj() throws Exception;

  public abstract boolean getIsTruncateOp();

  public abstract Long getWriteId();

  /**
   * Getter for list of partitions.
   * @return List of maps, where each map identifies values for each partition-key, for every altered partition.
   */
  public abstract List<Map<String, String>> getPartitions();

  public abstract Iterable<Partition> getPartitionObjs() throws Exception;

  @Override
  public EventMessage checkValid() {
    if (getTable() == null)
      throw new IllegalStateException("Table name unset.");
    if (getPartitions() == null)
      throw new IllegalStateException("Partition-list unset.");

    try {
      getPartitionObjs().forEach(partition -> {
        if (getWriteId() != partition.getWriteId()) {
          throw new IllegalStateException("Different write id in the same event");
        }
      });
    } catch (Exception e) {
      throw new IllegalStateException("Unable to get the partition objects");
    }

    return super.checkValid();
  }
}
