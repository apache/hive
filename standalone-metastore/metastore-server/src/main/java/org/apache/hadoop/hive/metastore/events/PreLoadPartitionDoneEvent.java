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

import java.util.Map;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class PreLoadPartitionDoneEvent extends PreEventContext {

  private final String catName;
  private final String dbName;
  private final String tableName;
  private final Map<String,String> partSpec;

  public PreLoadPartitionDoneEvent(String catName, String dbName, String tableName,
      Map<String, String> partSpec, IHMSHandler handler) {
    super(PreEventType.LOAD_PARTITION_DONE, handler);
    this.catName = catName;
    this.dbName = dbName;
    this.tableName = tableName;
    this.partSpec = partSpec;
  }

  public String getCatName() {
    return catName;
  }

  public String getDbName() {
    return dbName;
  }

  public String getTableName() {
    return tableName;
  }

  /**
   * @return the partition Name
   */
  public Map<String,String> getPartitionName() {
    return partSpec;
  }

}
