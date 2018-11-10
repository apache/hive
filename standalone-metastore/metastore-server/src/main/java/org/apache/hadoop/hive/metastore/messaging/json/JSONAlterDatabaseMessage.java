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

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.messaging.AlterDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.thrift.TException;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * JSON alter database message.
 */
public class JSONAlterDatabaseMessage extends AlterDatabaseMessage {

  @JsonProperty
  String server, servicePrincipal, db, dbObjBeforeJson, dbObjAfterJson;

  @JsonProperty
  Long timestamp;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONAlterDatabaseMessage() {
  }

  public JSONAlterDatabaseMessage(String server, String servicePrincipal,
                                  Database dbObjBefore, Database dbObjAfter, Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.db = dbObjBefore.getName();
    this.timestamp = timestamp;
    try {
      this.dbObjBeforeJson = MessageBuilder.createDatabaseObjJson(dbObjBefore);
      this.dbObjAfterJson = MessageBuilder.createDatabaseObjJson(dbObjAfter);
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
  public Database getDbObjBefore() throws Exception {
    return (Database) MessageBuilder.getTObj(dbObjBeforeJson, Database.class);
  }

  @Override
  public Database getDbObjAfter() throws Exception {
    return (Database) MessageBuilder.getTObj(dbObjAfterJson, Database.class);
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
