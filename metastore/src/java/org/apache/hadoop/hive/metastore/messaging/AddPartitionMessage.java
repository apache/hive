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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.messaging;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Map;

public abstract class AddPartitionMessage extends EventMessage {

  protected AddPartitionMessage() {
    super(EventType.ADD_PARTITION);
  }

  /**
   * Getter for name of table (where partitions are added).
   * @return Table-name (String).
   */
  public abstract String getTable();

  public abstract Table getTableObj() throws Exception;

  /**
   * Getter for list of partitions added.
   * @return List of maps, where each map identifies values for each partition-key, for every added partition.
   */
  public abstract List<Map<String, String>> getPartitions ();

  public abstract Iterable<Partition> getPartitionObjs() throws Exception;

  @Override
  public EventMessage checkValid() {
    if (getTable() == null)
      throw new IllegalStateException("Table name unset.");
    if (getPartitions() == null)
      throw new IllegalStateException("Partition-list unset.");
    return super.checkValid();
  }

  /**
   * Get iterable of partition name and file lists created as a result of this DDL operation
   *
   * @return The iterable of partition PartitionFiles
   */
  public abstract Iterable<PartitionFiles> getPartitionFilesIter();

}
