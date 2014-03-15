/**
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

package org.apache.hcatalog.messaging.json;

import org.apache.hcatalog.messaging.DropTableMessage;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * JSON implementation of DropTableMessage.
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.messaging.json.JSONDropTableMessage} instead
 */
public class JSONDropTableMessage extends DropTableMessage {

  @JsonProperty
  String server, servicePrincipal, db, table;

  @JsonProperty
  Long timestamp;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONDropTableMessage() {}

  public JSONDropTableMessage(String server, String servicePrincipal, String db, String table, Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.db = db;
    this.table = table;
    this.timestamp = timestamp;
    checkValid();
  }


  @Override
  public String getTable() { return table; }

  @Override
  public String getServer() { return server; }

  @Override
  public String getServicePrincipal() { return servicePrincipal; }

  @Override
  public String getDB() { return db; }

  @Override
  public Long getTimestamp() { return timestamp; }

  @Override
  public String toString() {
    try {
      return JSONMessageDeserializer.mapper.writeValueAsString(this);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not serialize: ", exception);
    }
  }

}
