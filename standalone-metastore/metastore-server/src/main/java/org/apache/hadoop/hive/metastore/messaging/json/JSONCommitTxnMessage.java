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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.messaging.json;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.messaging.CommitTxnMessage;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;

import java.util.List;

/**
 * JSON implementation of CommitTxnMessage
 */
public class JSONCommitTxnMessage extends CommitTxnMessage {

  @JsonProperty
  private Long txnid;

  @JsonProperty
  private Long timestamp;

  @JsonProperty
  private String server;

  @JsonProperty
  private String servicePrincipal;

  @JsonProperty
  private List<Long> writeIds;

  @JsonProperty
  private List<String> databases, tables, partitions, tableObjs, partitionObjs, files;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONCommitTxnMessage() {
  }

  public JSONCommitTxnMessage(String server, String servicePrincipal, Long txnid, Long timestamp) {
    this.timestamp = timestamp;
    this.txnid = txnid;
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.databases = null;
    this.tables = null;
    this.writeIds = null;
    this.partitions = null;
    this.tableObjs = null;
    this.partitionObjs = null;
    this.files = null;
  }

  @Override
  public Long getTxnId() {
    return txnid;
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public String getDB() {
    return null;
  }

  @Override
  public String getServicePrincipal() {
    return servicePrincipal;
  }

  @Override
  public String getServer() {
    return server;
  }

  @Override
  public List<Long> getWriteIds() {
    return writeIds;
  }

  @Override
  public List<String> getDatabases() {
    return databases;
  }

  @Override
  public List<String> getTables() {
    return tables;
  }

  @Override
  public List<String> getPartitions() {
    return partitions;
  }

  @Override
  public Table getTableObj(int idx) throws Exception {
    return tableObjs == null ? null :  (Table) MessageBuilder.getTObj(tableObjs.get(idx), Table.class);
  }

  @Override
  public Partition getPartitionObj(int idx) throws Exception {
    return (partitionObjs == null ? null : (partitionObjs.get(idx) == null ? null :
            (Partition) MessageBuilder.getTObj(partitionObjs.get(idx), Partition.class)));
  }

  @Override
  public String getFiles(int idx) {
    return files == null ? null : files.get(idx);
  }

  @Override
  public List<String> getFilesList() {
    return files;
  }

  @Override
  public void addWriteEventInfo(List<WriteEventInfo> writeEventInfoList) {
    if (this.databases == null) {
      this.databases = Lists.newArrayList();
    }
    if (this.tables == null) {
      this.tables = Lists.newArrayList();
    }
    if (this.writeIds == null) {
      this.writeIds = Lists.newArrayList();
    }
    if (this.tableObjs == null) {
      this.tableObjs = Lists.newArrayList();
    }
    if (this.partitions == null) {
      this.partitions = Lists.newArrayList();
    }
    if (this.partitionObjs == null) {
      this.partitionObjs = Lists.newArrayList();
    }
    if (this.files == null) {
      this.files = Lists.newArrayList();
    }

    for (WriteEventInfo writeEventInfo : writeEventInfoList) {
      this.databases.add(writeEventInfo.getDatabase());
      this.tables.add(writeEventInfo.getTable());
      this.writeIds.add(writeEventInfo.getWriteId());
      this.partitions.add(writeEventInfo.getPartition());
      this.tableObjs.add(writeEventInfo.getTableObj());
      this.partitionObjs.add(writeEventInfo.getPartitionObj());
      this.files.add(writeEventInfo.getFiles());
    }
  }

  @Override
  public String toString() {
    try {
      return JSONMessageDeserializer.mapper.writeValueAsString(this);
    } catch (Exception exception) {
      throw new IllegalArgumentException("Could not serialize: ", exception);
    }
  }
}
