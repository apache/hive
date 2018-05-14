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

import org.apache.hive.common.util.HiveStringUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

/**
 * Utility class to help track and return the metadata which are updated by repl load
 */
public class UpdatedMetaDataTracker {

  /**
   * Utility class to store replication state of a table.
   */
  public static class UpdateMetaData {
    private String replState;
    private String dbName;
    private String tableName;
    private List<Map <String, String>> partitionsList;

    UpdateMetaData(String replState, String dbName, String tableName, Map <String, String> partSpec) {
      this.replState = replState;
      this.dbName = dbName;
      this.tableName = tableName;
      this.partitionsList = new ArrayList<>();
      if (partSpec != null) {
        this.partitionsList.add(partSpec);
      }
    }

    public String getReplState() {
      return replState;
    }

    public String getDbName() {
      return dbName;
    }

    public String getTableName() {
      return tableName;
    }

    public List<Map <String, String>> getPartitionsList() {
      return partitionsList;
    }

    public void addPartition(Map<String, String> partSpec) {
      this.partitionsList.add(partSpec);
    }
  }

  private List<UpdateMetaData> updateMetaDataList;
  private Map<String, Integer> updateMetaDataMap;

  public UpdatedMetaDataTracker() {
    updateMetaDataList = new ArrayList<>();
    updateMetaDataMap = new HashMap<>();
  }

  public void copyUpdatedMetadata(UpdatedMetaDataTracker other) {
    int size = updateMetaDataList.size();
    for (UpdateMetaData updateMetaData : other.updateMetaDataList) {
      updateMetaDataList.add(updateMetaData);
      String key = getKey(normalizeIdentifier(updateMetaData.getDbName()),
                            normalizeIdentifier(updateMetaData.getTableName()));
      updateMetaDataMap.put(key, size++);
    }
  }

  public void set(String replState, String dbName, String tableName, Map <String, String> partSpec) {
    updateMetaDataList.add(new UpdateMetaData(replState, dbName, tableName, partSpec));
    String key = getKey(normalizeIdentifier(dbName), normalizeIdentifier(tableName));
    updateMetaDataMap.put(key, updateMetaDataList.size() - 1);
  }

  public void addPartition(String dbName, String tableName, Map <String, String> partSpec) {
    String key = getKey(normalizeIdentifier(dbName), normalizeIdentifier(tableName));
    updateMetaDataList.get(updateMetaDataMap.get(key)).addPartition(partSpec);
  }

  public List<UpdateMetaData> getUpdateMetaDataList() {
    return updateMetaDataList;
  }

  private String getKey(String dbName, String tableName) {
    if (tableName == null) {
      return dbName + ".*";
    }
    return dbName + "." + tableName;
  }

  private String normalizeIdentifier(String name) {
    return name == null ? null : HiveStringUtils.normalizeIdentifier(name);
  }

}
