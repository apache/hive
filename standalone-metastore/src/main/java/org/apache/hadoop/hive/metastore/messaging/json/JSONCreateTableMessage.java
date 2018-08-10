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

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.thrift.TException;
import org.codehaus.jackson.annotate.JsonProperty;

import com.google.common.collect.Lists;

/**
 * JSON implementation of CreateTableMessage.
 */
public class JSONCreateTableMessage extends CreateTableMessage {

  @JsonProperty
  String server, servicePrincipal, db, table, tableType, tableObjJson;
  @JsonProperty
  Long timestamp;
  @JsonProperty
  List<String> files;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONCreateTableMessage() {
  }

  public JSONCreateTableMessage(String server, String servicePrincipal, String db, String table,
      String tableType, Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.db = db;
    this.table = table;
    this.tableType = tableType;
    this.timestamp = timestamp;
    checkValid();
  }

  public JSONCreateTableMessage(String server, String servicePrincipal, String db, String table,
      Long timestamp) {
    this(server, servicePrincipal, db, table, null, timestamp);
  }

  public JSONCreateTableMessage(String server, String servicePrincipal, Table tableObj,
      Iterator<String> fileIter, Long timestamp) {
    this(server, servicePrincipal, tableObj.getDbName(), tableObj.getTableName(),
        tableObj.getTableType(), timestamp);
    try {
      this.tableObjJson = JSONMessageFactory.createTableObjJson(tableObj);
    } catch (TException e) {
      throw new IllegalArgumentException("Could not serialize: ", e);
    }
    this.files = (fileIter != null) ? Lists.newArrayList(fileIter) : Lists.newArrayList();
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
    if (tableType != null) return tableType; else return "";
  }

  @Override
  public Table getTableObj() throws Exception {
    return (Table) JSONMessageFactory.getTObj(tableObjJson,Table.class);
  }

  public String getTableObjJson() {
    return tableObjJson;
  }

  @Override
  public String toString() {
    try {
      return JSONMessageDeserializer.mapper.writeValueAsString(this);
    } catch (Exception exception) {
      throw new IllegalArgumentException("Could not serialize: ", exception);
    }
  }

  @Override
  public Iterable<String> getFiles() {
    return files;
  }
}
