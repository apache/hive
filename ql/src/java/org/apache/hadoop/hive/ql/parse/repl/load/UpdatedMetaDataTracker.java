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

import org.apache.hadoop.hive.ql.parse.SemanticException;
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
  private boolean needCommitTxn = false;

  public UpdatedMetaDataTracker() {
    updateMetaDataList = new ArrayList<>();
    updateMetaDataMap = new HashMap<>();
  }

  public void setNeedCommitTxn(boolean needCommitTxn) {
    this.needCommitTxn = needCommitTxn;
  }

  public boolean isNeedCommitTxn() {
    return needCommitTxn;
  }

  public void copyUpdatedMetadata(UpdatedMetaDataTracker other) {
    int size = updateMetaDataList.size();
    for (UpdateMetaData updateMetaDataOther : other.updateMetaDataList) {
      String key = getKey(normalizeIdentifier(updateMetaDataOther.getDbName()),
              normalizeIdentifier(updateMetaDataOther.getTableName()));
      Integer idx = updateMetaDataMap.get(key);
      if (idx == null) {
        updateMetaDataList.add(updateMetaDataOther);
        updateMetaDataMap.put(key, size++);
      } else if (updateMetaDataOther.partitionsList != null && updateMetaDataOther.partitionsList.size() != 0) {
        UpdateMetaData updateMetaData = updateMetaDataList.get(idx);
        for (Map<String, String> partSpec : updateMetaDataOther.partitionsList) {
          updateMetaData.addPartition(partSpec);
        }
      }
    }
    this.needCommitTxn = other.needCommitTxn;
  }

  public void set(String replState, String dbName, String tableName, Map <String, String> partSpec)
          throws SemanticException {
    if (dbName == null) {
      throw new SemanticException("db name can not be null");
    }
    if (dbName.contains(".") || ((tableName != null)  && tableName.contains("."))) {
      throw new SemanticException("Invalid DB or table name." + dbName + "::"
              + ((tableName != null) ? tableName : ""));
    }
    String key = getKey(normalizeIdentifier(dbName), normalizeIdentifier(tableName));
    Integer idx = updateMetaDataMap.get(key);
    if (idx == null) {
      updateMetaDataList.add(new UpdateMetaData(replState, dbName, tableName, partSpec));
      updateMetaDataMap.put(key, updateMetaDataList.size() - 1);
    } else {
      updateMetaDataList.get(idx).addPartition(partSpec);
    }
  }

  public void addPartition(String dbName, String tableName, Map <String, String> partSpec) throws SemanticException {
    if (dbName == null) {
      throw new SemanticException("db name can not be null");
    }
    String key = getKey(normalizeIdentifier(dbName), normalizeIdentifier(tableName));
    Integer idx = updateMetaDataMap.get(key);
    if (idx == null) {
      throw new SemanticException("add partition to metadata map failed as list is not yet set for table : " + key);
    }
    updateMetaDataList.get(idx).addPartition(partSpec);
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
