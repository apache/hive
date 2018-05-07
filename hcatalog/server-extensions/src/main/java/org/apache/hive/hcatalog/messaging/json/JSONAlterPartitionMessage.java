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
package org.apache.hive.hcatalog.messaging.json;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.messaging.AlterPartitionMessage;
import org.apache.hive.hcatalog.messaging.AlterTableMessage;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * JSON alter table message
 */
public class JSONAlterPartitionMessage extends AlterPartitionMessage {

  @JsonProperty
  String server, servicePrincipal, db, table, tableType;

  @JsonProperty
  Long timestamp;

  @JsonProperty
  Map<String,String> keyValues;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONAlterPartitionMessage() {}

  public JSONAlterPartitionMessage(String server,
                                   String servicePrincipal,
                                   String db,
                                   String table,
                                   Map<String,String> keyValues,
                                   Long timestamp) {
    this(server, servicePrincipal, db, table, null, keyValues, timestamp);
  }

  public JSONAlterPartitionMessage(String server,
                                   String servicePrincipal,
                                   String db,
                                   String table,
                                   String tableType,
                                   Map<String,String> keyValues,
                                   Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.db = db;
    this.table = table;
    this.tableType = tableType;
    this.timestamp = timestamp;
    this.keyValues = keyValues;
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
    if (tableType != null) return tableType; else return "";
  }

  @Override
  public Map<String,String> getKeyValues() {
    return keyValues;
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
