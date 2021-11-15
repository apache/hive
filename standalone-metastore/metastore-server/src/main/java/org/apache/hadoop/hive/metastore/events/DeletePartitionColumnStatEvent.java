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

import java.util.List;

/**
 * DeletePartitionColumnStatEvent
 * Event generated for partition column stat delete event.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DeletePartitionColumnStatEvent extends ListenerEvent {
  private String catName, dbName, tableName, colName, partName, engine;

  private List<String> partVals;

  /**
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param partName partition column name
   * @param partVals partition value
   * @param colName column name
   * @param engine engine
   * @param handler handler that is firing the event
   */
  public DeletePartitionColumnStatEvent(String catName, String dbName, String tableName, String partName,
      List<String> partVals, String colName, String engine, IHMSHandler handler) {
    super(true, handler);
    this.catName = catName;
    this.dbName = dbName;
    this.tableName = tableName;
    this.colName = colName;
    this.partName = partName;
    this.partVals = partVals;
    this.engine = engine;
  }

  public String getCatName() {
    return catName;
  }

  public String getDBName() {
    return dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getColName() {
    return colName;
  }

  public String getPartName() {
    return partName;
  }

  public List<String> getPartVals() {
    return partVals;
  }

  public String getEngine() {
    return engine;
  }
}
