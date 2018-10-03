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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.messaging.AlterCatalogMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.thrift.TException;

public class JSONAlterCatalogMessage extends AlterCatalogMessage {
  @JsonProperty
  String server, servicePrincipal, catObjBeforeJson, catObjAfterJson;

  @JsonProperty
  Long timestamp;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONAlterCatalogMessage() {
  }

  public JSONAlterCatalogMessage(String server, String servicePrincipal,
                                 Catalog catObjBefore, Catalog catObjAfter, Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.timestamp = timestamp;
    try {
      this.catObjBeforeJson = MessageBuilder.createCatalogObjJson(catObjBefore);
      this.catObjAfterJson = MessageBuilder.createCatalogObjJson(catObjAfter);
    } catch (TException e) {
      throw new IllegalArgumentException("Could not serialize: ", e);
    }
    checkValid();
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

  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public Catalog getCatObjBefore() throws Exception {
    return (Catalog) MessageBuilder.getTObj(catObjBeforeJson, Catalog.class);
  }

  @Override
  public Catalog getCatObjAfter() throws Exception {
    return (Catalog) MessageBuilder.getTObj(catObjAfterJson, Catalog.class);
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
