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
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.messaging.AddCheckConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;

public class JSONAddCheckConstraintMessage extends AddCheckConstraintMessage {
  @JsonProperty
  String server, servicePrincipal;

  @JsonProperty
  Long timestamp;

  @JsonProperty
  List<String> checkConstraintListJson;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONAddCheckConstraintMessage() {
  }

  public JSONAddCheckConstraintMessage(String server, String servicePrincipal, List<SQLCheckConstraint> ccs,
                                         Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.timestamp = timestamp;
    this.checkConstraintListJson = new ArrayList<>();
    try {
      for (SQLCheckConstraint cc : ccs) {
        checkConstraintListJson.add(MessageBuilder.createCheckConstraintObjJson(cc));
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
  public List<SQLCheckConstraint> getCheckConstraints() throws Exception {
    List<SQLCheckConstraint> ccs = new ArrayList<>();
    for (String ccJson : checkConstraintListJson) {
      ccs.add((SQLCheckConstraint) MessageBuilder.getTObj(ccJson, SQLCheckConstraint.class));
    }
    return ccs;
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
