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
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.UpdateTableColumnStatMessage;
import org.apache.thrift.TException;
import java.util.Map;

/**
 * JSON implementation of JSONUpdateTableColumnStatMessage
 */
public class JSONUpdateTableColumnStatMessage extends UpdateTableColumnStatMessage {

  @JsonProperty
  private Long writeId, timestamp;

  @JsonProperty
  private String server, servicePrincipal, database;

  @JsonProperty
  private String colStatsJson;

  @JsonProperty
  Map<String, String> parameters;

  @JsonProperty
  private String tableObjJson;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONUpdateTableColumnStatMessage() {
  }

  public JSONUpdateTableColumnStatMessage(String server, String servicePrincipal, Long timestamp,
                      ColumnStatistics colStats, Table tableObj, Map<String, String> parameters,
                                           long writeId) {
    this.timestamp = timestamp;
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.writeId = writeId;
    this.database = colStats.getStatsDesc().getDbName();
    try {
      this.colStatsJson = MessageBuilder.createTableColumnStatJson(colStats);
      this.tableObjJson = MessageBuilder.createTableObjJson(tableObj);
    } catch (TException e) {
      throw new IllegalArgumentException("Could not serialize JSONUpdateTableColumnStatMessage : ", e);
    }
    this.parameters = parameters;
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
  public ColumnStatistics getColumnStatistics() {
    try {
      return  (ColumnStatistics) MessageBuilder.getTObj(colStatsJson, ColumnStatistics.class);
    } catch (Exception e) {
      throw new RuntimeException("failed to get the ColumnStatistics object ", e);
    }
  }

  @Override
  public Table getTableObject() throws Exception {
    return (Table) MessageBuilder.getTObj(tableObjJson, Table.class);
  }

  @Override
  public Long getWriteId() {
    return writeId;
  }

  @Override
  public Map<String, String> getParameters() {
    return parameters;
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

