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

import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.Table;

public class DropTableEvent extends ListenerEvent {

  private final Table table;
  private final boolean deleteData;

  public DropTableEvent(Table table, boolean status, boolean deleteData, HMSHandler handler) {
    super(status, handler);
    this.table = table;
    // In HiveMetaStore, the deleteData flag indicates whether DFS data should be
    // removed on a drop.
    this.deleteData = deleteData;
  }

  /**
   * @return the table
   */
  public Table getTable() {
    return table;
  }

  /**
   * @return the deleteData flag
   */
  public boolean getDeleteData() {
    return deleteData;
  }
}
