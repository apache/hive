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

package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class AlterPartitionEvent extends ListenerEvent {

  private final Partition oldPart;
  private final Partition newPart;
  private final Table table;
  private final boolean isTruncateOp;
  private Long writeId;

  public AlterPartitionEvent(Partition oldPart, Partition newPart, Table table, boolean isTruncateOp,
                             boolean status, Long writeId, IHMSHandler handler) {
    super(status, handler);
    this.oldPart = oldPart;
    this.newPart = newPart;
    this.table = table;
    this.isTruncateOp = isTruncateOp;
    this.writeId = writeId;
  }

  /**
   * @return the old partition
   */
  public Partition getOldPartition() {
    return oldPart;
  }

  /**
   *
   * @return the new partition
   */
  public Partition getNewPartition() {
    return newPart;
  }

  /**
   * Get the table this partition is in
   * @return
   */
  public Table getTable() {
    return table;
  }

  /**
   * Get the truncate table flag
   * @return
   */
  public boolean getIsTruncateOp() {
    return isTruncateOp;
  }

  public Long getWriteId() {
    return writeId;
  }
}
