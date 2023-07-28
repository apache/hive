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

import java.util.List;

/**
 * AcidWriteEvent
 * Event generated for acid write operations
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class AcidWriteEvent extends ListenerEvent {
  private final WriteNotificationLogRequest writeNotificationLogRequest;
  private final String partition;
  private final Table tableObj;
  private final Partition partitionObj;

  public AcidWriteEvent(String partition, Table tableObj, Partition partitionObj,
                        WriteNotificationLogRequest writeNotificationLogRequest) {
    super(true, null);
    this.writeNotificationLogRequest = writeNotificationLogRequest;
    this.partition = partition;
    this.tableObj = tableObj;
    this.partitionObj = partitionObj;
  }

  public Long getTxnId() {
    return writeNotificationLogRequest.getTxnId();
  }

  public List<String> getFiles() {
    return writeNotificationLogRequest.getFileInfo().getFilesAdded();
  }

  public List<String> getChecksums() {
    return writeNotificationLogRequest.getFileInfo().getFilesAddedChecksum();
  }

  public String getDatabase() {
    return StringUtils.normalizeIdentifier(writeNotificationLogRequest.getDb());
  }

  public String getTable() {
    return StringUtils.normalizeIdentifier(writeNotificationLogRequest.getTable());
  }

  public String getPartition() {
    return partition; //Don't normalize partition value, as its case sensitive.
  }

  public Long getWriteId() {
    return writeNotificationLogRequest.getWriteId();
  }

  public Table getTableObj() {
    return tableObj;
  }

  public Partition getPartitionObj() {
    return partitionObj;
  }

  public List<String> getSubDirs() {
    return writeNotificationLogRequest.getFileInfo().getSubDirectoryList();
  }
}

