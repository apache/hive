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
import org.apache.hadoop.hive.metastore.messaging.DeleteTableColumnStatMessage;

/**
 * JSON implementation of JSONDeleteTableColumnStatMessage
 */
public class JSONDeleteTableColumnStatMessage extends DeleteTableColumnStatMessage {

  @JsonProperty
  private Long timestamp;

  @JsonProperty
  private String server, servicePrincipal, database, colName;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONDeleteTableColumnStatMessage() {
  }

  public JSONDeleteTableColumnStatMessage(String server, String servicePrincipal, Long timestamp,
                                          String dbName, String colName) {
    this.timestamp = timestamp;
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.database = dbName;
    this.colName = colName;
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
  public String getColName() {
    return colName;
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

