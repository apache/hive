/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.messaging.json;

import org.apache.hadoop.hive.metastore.messaging.DropCatalogMessage;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JSONDropCatalogMessage extends DropCatalogMessage {

  @JsonProperty
  String server, servicePrincipal, catalog;

  @JsonProperty
  Long timestamp;

  public JSONDropCatalogMessage() {

  }

  public JSONDropCatalogMessage(String server, String servicePrincipal, String catalog,
                                Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.catalog = catalog;
    this.timestamp = timestamp;
  }

  @Override
  public String getDB() {
    return null;
  }

  @Override
  public String getServer() {
    return server;
  }

  @Override
  public String getServicePrincipal() {
    return servicePrincipal;
  }

  public String getCatalog() {
    return catalog;
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
  }
}
