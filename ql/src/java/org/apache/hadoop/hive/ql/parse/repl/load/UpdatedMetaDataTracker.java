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
package org.apache.hadoop.hive.ql.parse.repl.load;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;

/**
 * Utility class to help track and return the metadata which are updated by repl load
 */
public class UpdatedMetaDataTracker {
  private String replState;
  private String dbName;
  private String tableName;
  private List<Map <String, String>> partitionsList;

  public UpdatedMetaDataTracker() {
    this.replState = null;
    this.dbName = null;
    this.tableName = null;
    this.partitionsList = new ArrayList<>();
  }

  public void copyUpdatedMetadata(UpdatedMetaDataTracker other) {
    this.replState = other.replState;
    this.dbName = other.dbName;
    this.tableName = other.tableName;
    this.partitionsList = other.getPartitions();
  }

  public void set(String replState, String dbName, String tableName, Map <String, String> partSpec) {
    this.replState = replState;
    this.dbName = dbName;
    this.tableName = tableName;
    if (partSpec != null) {
      addPartition(partSpec);
    }
  }

  public void addPartition(Map <String, String> partSpec) {
    partitionsList.add(partSpec);
  }

  public String getReplicationState() {
    return replState;
  }

  public String getDatabase() {
    return dbName;
  }

  public String getTable() {
    return tableName;
  }

  public List<Map <String, String>> getPartitions() {
    return partitionsList;
  }

}
