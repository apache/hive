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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore.messaging.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.messaging.AddDefaultConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;

public class JSONAddDefaultConstraintMessage extends AddDefaultConstraintMessage {
  @JsonProperty
  String server, servicePrincipal;

  @JsonProperty
  Long timestamp;

  @JsonProperty
  List<String> defaultConstraintListJson;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONAddDefaultConstraintMessage() {
  }

  public JSONAddDefaultConstraintMessage(String server, String servicePrincipal, List<SQLDefaultConstraint> dcs,
                                         Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.timestamp = timestamp;
    this.defaultConstraintListJson = new ArrayList<>();
    try {
      for (SQLDefaultConstraint dc : dcs) {
        defaultConstraintListJson.add(MessageBuilder.createDefaultConstraintObjJson(dc));
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
  public List<SQLDefaultConstraint> getDefaultConstraints() throws Exception {
    List<SQLDefaultConstraint> dcs = new ArrayList<>();
    for (String ddJson : defaultConstraintListJson) {
      dcs.add((SQLDefaultConstraint) MessageBuilder.getTObj(ddJson, SQLDefaultConstraint.class));
    }
    return dcs;
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
