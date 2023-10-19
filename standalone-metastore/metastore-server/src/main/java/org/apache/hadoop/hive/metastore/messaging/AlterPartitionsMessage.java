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

import org.apache.hadoop.hive.metastore.api.Partition;

public abstract class AlterPartitionsMessage extends EventMessage {

  protected AlterPartitionsMessage() {
    super(EventType.ALTER_PARTITIONS);
  }

  public abstract String getTable();

  public abstract String getTableType();

  public abstract boolean getIsTruncateOp();

  public abstract Long getWriteId();

  public abstract List<String> getPartitionKeys();

  public abstract List<List<String>> getPartitionValues();

  public abstract List<Partition> getPartitionsAfter();

  @Override
  public EventMessage checkValid() {
    if (getTable() == null) {
      throw new IllegalStateException("Table name unset.");
    }
    if (getPartitionKeys() == null || getPartitionKeys().isEmpty()) {
      throw new IllegalStateException("Partition keys unset");
    }
    if (getPartitionValues() == null || getPartitionValues().isEmpty()) {
      new IllegalStateException("Partition values unset.");
    }

    getPartitionsAfter().forEach(partition -> {
      if (getWriteId() != partition.getWriteId()) {
        new IllegalStateException("Different write id in the same event");
      }
    });

    return super.checkValid();
  }
}
