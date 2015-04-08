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
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class InsertEvent extends ListenerEvent {

  // Note that this event is fired from the client, so rather than having full metastore objects
  // we have just the string names, but that's fine for what we need.
  private final String db;
  private final String table;
  private final List<String> partVals;
  private final List<String> files;

  /**
   *
   * @param db name of the database the table is in
   * @param table name of the table being inserted into
   * @param partitions list of partition values, can be null
   * @param status status of insert, true = success, false = failure
   * @param handler handler that is firing the event
   */
  public InsertEvent(String db, String table, List<String> partitions, List<String> files,
                     boolean status, HMSHandler handler) {
    super(status, handler);
    this.db = db;
    this.table = table;
    this.partVals = partitions;
    this.files = files;
  }

  public String getDb() {
    return db;
  }
  /**
   * @return The table.
   */
  public String getTable() {
    return table;
  }

  /**
   * @return List of partitions.
   */
  public List<String> getPartitions() {
    return partVals;
  }

  /**
   * Get list of files created as a result of this DML operation
   * @return list of new files
   */
  public List<String> getFiles() {
    return files;
  }
}
