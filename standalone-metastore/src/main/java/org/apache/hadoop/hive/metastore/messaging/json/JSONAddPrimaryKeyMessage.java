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

import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.messaging.AddPrimaryKeyMessage;
import org.apache.thrift.TException;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * JSON implementation of AddPrimaryKeyMessage
 */
public class JSONAddPrimaryKeyMessage extends AddPrimaryKeyMessage {

  @JsonProperty
  String server, servicePrincipal;

  @JsonProperty
  Long timestamp;

  @JsonProperty
  List<String> primaryKeyListJson;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONAddPrimaryKeyMessage() {
  }

  public JSONAddPrimaryKeyMessage(String server, String servicePrincipal, List<SQLPrimaryKey> pks,
      Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.timestamp = timestamp;
    this.primaryKeyListJson = new ArrayList<>();
    try {
      for (SQLPrimaryKey pk : pks) {
        primaryKeyListJson.add(JSONMessageFactory.createPrimaryKeyObjJson(pk));
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
  public List<SQLPrimaryKey> getPrimaryKeys() throws Exception {
    List<SQLPrimaryKey> pks = new ArrayList<>();
    for (String pkJson : primaryKeyListJson) {
      pks.add((SQLPrimaryKey)JSONMessageFactory.getTObj(pkJson, SQLPrimaryKey.class));
    }
    return pks;
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
