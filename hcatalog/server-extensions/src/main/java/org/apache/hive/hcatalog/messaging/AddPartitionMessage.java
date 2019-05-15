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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.messaging;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;

import java.util.List;
import java.util.Map;

/**
 * The HCat message sent when partition(s) are added to a table.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class AddPartitionMessage extends HCatEventMessage {

  protected AddPartitionMessage() {
    super(EventType.ADD_PARTITION);
  }

  /**
   * Getter for name of table (where partitions are added).
   * @return Table-name (String).
   */
  public abstract String getTable();

  public abstract String getTableType();

  /**
   * Getter for list of partitions added.
   * @return List of maps, where each map identifies values for each partition-key, for every added partition.
   */
  public abstract List<Map<String, String>> getPartitions ();

  @Override
  public HCatEventMessage checkValid() {
    if (getTable() == null)
      throw new IllegalStateException("Table name unset.");
    if (getPartitions() == null)
      throw new IllegalStateException("Partition-list unset.");
    return super.checkValid();
  }
}
