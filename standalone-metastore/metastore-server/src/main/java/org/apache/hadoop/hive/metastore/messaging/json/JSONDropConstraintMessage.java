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

import org.apache.hadoop.hive.metastore.messaging.DropConstraintMessage;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * JSON implementation of DropConstraintMessage
 */
public class JSONDropConstraintMessage extends DropConstraintMessage {

  @JsonProperty
  String server, servicePrincipal, dbName, tableName, constraintName;

  @JsonProperty
  Long timestamp;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONDropConstraintMessage() {
  }

  public JSONDropConstraintMessage(String server, String servicePrincipal, String dbName,
      String tableName, String constraintName, Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.timestamp = timestamp;
    this.dbName = dbName;
    this.tableName = tableName;
    this.constraintName = constraintName;
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
    return dbName;
  }

  @Override
  public String getTable() {
    return tableName;
  }

  @Override
  public String getConstraint() {
    return constraintName;
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
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
