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

import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.Partition;

public class PreAlterPartitionEvent extends PreEventContext {

  private final String dbName;
  private final String tableName;
  private final List<String> oldPartVals;
  private final Partition newPart;

  public PreAlterPartitionEvent(String dbName, String tableName, List<String> oldPartVals,
      Partition newPart, HMSHandler handler) {
    super(PreEventType.ALTER_PARTITION, handler);
    this.dbName = dbName;
    this.tableName = tableName;
    this.oldPartVals = oldPartVals;
    this.newPart = newPart;
  }

  public String getDbName() {
    return dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public List<String> getOldPartVals() {
    return oldPartVals;
  }

  /**
   *
   * @return the new partition
   */
  public Partition getNewPartition() {
    return newPart;
  }
}
