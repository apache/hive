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
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogRequest;
import org.apache.hadoop.hive.metastore.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * AcidWriteEvent
 * Event generated for acid write operations
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BatchAcidWriteEvent extends ListenerEvent {
  private final List<WriteNotificationLogRequest> writeNotificationLogRequestList = new ArrayList<>();
  private final List<String> partitionList = new ArrayList<>();
  private final List<Table> tableObjList = new ArrayList<>();
  private final List<Partition> partitionObjList = new ArrayList<>();

  public BatchAcidWriteEvent(String partition, Table tableObj, Partition partitionObj,
                             WriteNotificationLogRequest writeNotificationLogRequest) {
    super(true, null);
    addNotification(partition, tableObj, partitionObj, writeNotificationLogRequest);
  }

  public BatchAcidWriteEvent() {
    super(true, null);
  }

  public void addNotification(String partition, Table tableObj, Partition partitionObj,
                              WriteNotificationLogRequest writeNotificationLogRequest) {
    this.writeNotificationLogRequestList.add(writeNotificationLogRequest);
    this.partitionList.add(partition);
    this.tableObjList.add(tableObj);
    this.partitionObjList.add(partitionObj);
  }

  public Long getTxnId(int idx) {
    return writeNotificationLogRequestList.get(idx).getTxnId();
  }

  public List<String> getFiles(int idx) {
    return writeNotificationLogRequestList.get(idx).getFileInfo().getFilesAdded();
  }

  public List<String> getChecksums(int idx) {
    return writeNotificationLogRequestList.get(idx).getFileInfo().getFilesAddedChecksum();
  }

  public String getDatabase(int idx) {
    return StringUtils.normalizeIdentifier(writeNotificationLogRequestList.get(idx).getDb());
  }

  public String getTable(int idx) {
    return StringUtils.normalizeIdentifier(writeNotificationLogRequestList.get(idx).getTable());
  }

  public String getPartition(int idx) {
    return partitionList.get(idx); //Don't normalize partition value, as its case sensitive.
  }

  public Long getWriteId(int idx) {
    return writeNotificationLogRequestList.get(idx).getWriteId();
  }

  public Table getTableObj(int idx) {
    return tableObjList.get(idx);
  }

  public Partition getPartitionObj(int idx) {
    return partitionObjList.get(idx);
  }

  public List<String> getSubDirs(int idx) {
    return writeNotificationLogRequestList.get(idx).getFileInfo().getSubDirectoryList();
  }

  public WriteNotificationLogRequest getNotificationRequest(int idx) {
    return writeNotificationLogRequestList.get(idx);
  }

  public int getNumRequest() {
    return writeNotificationLogRequestList.size();
  }
}

