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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AcidWriteEvent;
import org.apache.hadoop.hive.metastore.messaging.AcidWriteMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.thrift.TException;
import java.util.Iterator;
import java.util.List;

/**
 * JSON implementation of AcidWriteMessage
 */
public class JSONAcidWriteMessage extends AcidWriteMessage {

  @JsonProperty
  private Long txnid, writeId, timestamp;

  @JsonProperty
  private String server, servicePrincipal, database, table, partition, tableObjJson, partitionObjJson;

  @JsonProperty
  private List<String> files;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONAcidWriteMessage() {
  }

  public JSONAcidWriteMessage(String server, String servicePrincipal, Long timestamp, AcidWriteEvent acidWriteEvent,
                              Iterator<String> files) {
    this.timestamp = timestamp;
    this.txnid = acidWriteEvent.getTxnId();
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.database = acidWriteEvent.getDatabase();
    this.table = acidWriteEvent.getTable();
    this.writeId = acidWriteEvent.getWriteId();
    this.partition = acidWriteEvent.getPartition();
    try {
      this.tableObjJson = MessageBuilder.createTableObjJson(acidWriteEvent.getTableObj());
      if (acidWriteEvent.getPartitionObj() != null) {
        this.partitionObjJson = MessageBuilder.createPartitionObjJson(acidWriteEvent.getPartitionObj());
      } else {
        this.partitionObjJson = null;
      }
    } catch (TException e) {
      throw new IllegalArgumentException("Could not serialize JSONAcidWriteMessage : ", e);
    }
    this.files = Lists.newArrayList(files);
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
    return database;
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
  public String getTable() {
    return table;
  }

  @Override
  public Long getWriteId() {
    return writeId;
  }

  @Override
  public String getPartition() {
    return partition;
  }

  @Override
  public List<String> getFiles() {
    return files;
  }

  @Override
  public Table getTableObj() throws Exception {
    return (tableObjJson == null) ? null : (Table) MessageBuilder.getTObj(tableObjJson, Table.class);
  }

  @Override
  public Partition getPartitionObj() throws Exception {
    return ((partitionObjJson == null) ? null :
            (Partition) MessageBuilder.getTObj(partitionObjJson, Partition.class));
  }

  @Override
  public String getTableObjStr() {
    return tableObjJson;
  }

  @Override
  public String getPartitionObjStr() {
    return partitionObjJson;
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

