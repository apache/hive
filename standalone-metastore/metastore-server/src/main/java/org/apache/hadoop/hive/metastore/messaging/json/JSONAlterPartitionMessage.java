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

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.thrift.TException;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * JSON alter table message
 */
public class JSONAlterPartitionMessage extends AlterPartitionMessage {

  @JsonProperty
  String server, servicePrincipal, db, table, tableType, tableObjJson;

  @JsonProperty
  String isTruncateOp;

  @JsonProperty
  Long timestamp, writeId;

  @JsonProperty
  Map<String, String> keyValues;

  @JsonProperty
  String partitionObjBeforeJson, partitionObjAfterJson;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONAlterPartitionMessage() {
  }

  public JSONAlterPartitionMessage(String server, String servicePrincipal, Table tableObj,
      Partition partitionObjBefore, Partition partitionObjAfter, boolean isTruncateOp, Long writeId, Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.db = tableObj.getDbName();
    this.table = tableObj.getTableName();
    this.tableType = tableObj.getTableType();
    this.isTruncateOp = Boolean.toString(isTruncateOp);
    this.timestamp = timestamp;
    this.keyValues = MessageBuilder.getPartitionKeyValues(tableObj, partitionObjBefore);
    this.writeId = writeId;
    try {
      this.tableObjJson = MessageBuilder.createTableObjJson(tableObj);
      this.partitionObjBeforeJson = MessageBuilder.createPartitionObjJson(partitionObjBefore);
      this.partitionObjAfterJson = MessageBuilder.createPartitionObjJson(partitionObjAfter);
    } catch (TException e) {
      throw new IllegalArgumentException("Could not serialize: ", e);
    }
    checkValid();
  }

  @Override
  public String getServer() {
    return server;
  }

  @Override
  public String getServicePrincipal() {
    return servicePrincipal;
  }

  @Override
  public String getDB() {
    return db;
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public String getTable() {
    return table;
  }

  @Override
  public String getTableType() {
    if (tableType != null) {
      return tableType;
    } else {
      return "";
    }
  }

  @Override
  public boolean getIsTruncateOp() { return Boolean.parseBoolean(isTruncateOp); }

  @Override
  public Map<String, String> getKeyValues() {
    return keyValues;
  }

  @Override
  public Table getTableObj() throws Exception {
    return (Table) MessageBuilder.getTObj(tableObjJson,Table.class);
  }

  @Override
  public Partition getPtnObjBefore() throws Exception {
    return (Partition) MessageBuilder.getTObj(partitionObjBeforeJson, Partition.class);
  }

  @Override
  public Partition getPtnObjAfter() throws Exception {
    return (Partition) MessageBuilder.getTObj(partitionObjAfterJson, Partition.class);
  }

  public String getTableObjJson() {
    return tableObjJson;
  }

  public String getPartitionObjBeforeJson() {
    return partitionObjBeforeJson;
  }

  public String getPartitionObjAfterJson() {
    return partitionObjAfterJson;
  }

  @Override
  public Long getWriteId() {
    return writeId == null ? 0 : writeId;
  }

  @Override
  public String toString() {
    try {
      return JSONMessageDeserializer.mapper.writeValueAsString(this);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not serialize: ", e);
    }
  }
}
