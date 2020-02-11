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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.messaging.AddForeignKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.thrift.TException;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * JSON implementation of AddForeignKeyMessage
 */
public class JSONAddForeignKeyMessage extends AddForeignKeyMessage {

  @JsonProperty
  String server, servicePrincipal;

  @JsonProperty
  Long timestamp;

  @JsonProperty
  List<String> foreignKeyListJson;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONAddForeignKeyMessage() {
  }

  public JSONAddForeignKeyMessage(String server, String servicePrincipal, List<SQLForeignKey> fks,
      Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.timestamp = timestamp;
    this.foreignKeyListJson = new ArrayList<>();
    try {
      for (SQLForeignKey pk : fks) {
        foreignKeyListJson.add(MessageBuilder.createForeignKeyObjJson(pk));
      }
    } catch (TException e) {
      throw new IllegalArgumentException("Could not serialize: ", e);
    }
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
    return null;
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public List<SQLForeignKey> getForeignKeys() throws Exception {
    List<SQLForeignKey> fks = new ArrayList<>();
    for (String pkJson : foreignKeyListJson) {
      fks.add((SQLForeignKey) MessageBuilder.getTObj(pkJson, SQLForeignKey.class));
    }
    return fks;
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
