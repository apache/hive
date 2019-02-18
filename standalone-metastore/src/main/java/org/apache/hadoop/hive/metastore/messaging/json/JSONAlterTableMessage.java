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

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.thrift.TException;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * JSON alter table message
 */
public class JSONAlterTableMessage extends AlterTableMessage {

  @JsonProperty
  String server, servicePrincipal, db, table, tableType, tableObjBeforeJson, tableObjAfterJson;

  @JsonProperty
  String isTruncateOp;

  @JsonProperty
  Long timestamp, writeId;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONAlterTableMessage() {
  }

  public JSONAlterTableMessage(String server, String servicePrincipal, Table tableObjBefore, Table tableObjAfter,
      boolean isTruncateOp, Long writeId, Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.db = tableObjBefore.getDbName();
    this.table = tableObjBefore.getTableName();
    this.tableType = tableObjBefore.getTableType();
    this.isTruncateOp = Boolean.toString(isTruncateOp);
    this.timestamp = timestamp;
    this.writeId = writeId;
    try {
      this.tableObjBeforeJson = MessageBuilder.createTableObjJson(tableObjBefore);
      this.tableObjAfterJson = MessageBuilder.createTableObjJson(tableObjAfter);
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
  public Table getTableObjBefore() throws Exception {
    return (Table) MessageBuilder.getTObj(tableObjBeforeJson,Table.class);
  }

  @Override
  public Table getTableObjAfter() throws Exception {
    return (Table) MessageBuilder.getTObj(tableObjAfterJson,Table.class);
  }

  public String getTableObjBeforeJson() {
    return tableObjBeforeJson;
  }

  public String getTableObjAfterJson() {
    return tableObjAfterJson ;
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
