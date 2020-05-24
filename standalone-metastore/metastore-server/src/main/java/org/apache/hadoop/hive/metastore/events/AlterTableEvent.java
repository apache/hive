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
import org.apache.hadoop.hive.metastore.api.Table;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class AlterTableEvent extends ListenerEvent {

  private final Table newTable;
  private final Table oldTable;
  private final boolean isTruncateOp;
  private Long writeId;
  private final boolean isReplicated;

  public AlterTableEvent(Table oldTable, Table newTable, boolean isTruncateOp, boolean status,
                         Long writeId, IHMSHandler handler, boolean isReplicated) {
    super (status, handler);
    this.oldTable = oldTable;
    this.newTable = newTable;
    this.isTruncateOp = isTruncateOp;
    this.writeId = writeId;
    this.isReplicated = isReplicated;
  }

  /**
   * @return the old table
   */
  public Table getOldTable() {
    return oldTable;
  }

  /**
   * @return the new table
   */
  public Table getNewTable() {
    return newTable;
  }

  /**
   * @return the flag for truncate
   */
  public boolean getIsTruncateOp() {
    return isTruncateOp;
  }

  public Long getWriteId() {
    return writeId;
  }

  public boolean isReplicated() { return isReplicated; }
}