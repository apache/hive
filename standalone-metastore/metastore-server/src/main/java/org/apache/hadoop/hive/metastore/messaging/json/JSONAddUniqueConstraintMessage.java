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

import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.messaging.AddUniqueConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.thrift.TException;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JSONAddUniqueConstraintMessage extends AddUniqueConstraintMessage {
  @JsonProperty
  String server, servicePrincipal;

  @JsonProperty
  Long timestamp;

  @JsonProperty
  List<String> uniqueConstraintListJson;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONAddUniqueConstraintMessage() {
  }

  public JSONAddUniqueConstraintMessage(String server, String servicePrincipal, List<SQLUniqueConstraint> uks,
      Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.timestamp = timestamp;
    this.uniqueConstraintListJson = new ArrayList<>();
    try {
      for (SQLUniqueConstraint uk : uks) {
        uniqueConstraintListJson.add(MessageBuilder.createUniqueConstraintObjJson(uk));
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
  public List<SQLUniqueConstraint> getUniqueConstraints() throws Exception {
    List<SQLUniqueConstraint> uks = new ArrayList<>();
    for (String pkJson : uniqueConstraintListJson) {
      uks.add((SQLUniqueConstraint) MessageBuilder.getTObj(pkJson, SQLUniqueConstraint.class));
    }
    return uks;
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
