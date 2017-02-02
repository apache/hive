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

package org.apache.hadoop.hive.metastore.messaging.json;

import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.messaging.AlterIndexMessage;
import org.apache.thrift.TException;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * JSON Implementation of AlterIndexMessage.
 */
public class JSONAlterIndexMessage extends AlterIndexMessage {

  @JsonProperty
  String server, servicePrincipal, db, beforeIndexObjJson, afterIndexObjJson;

  @JsonProperty
  Long timestamp;

  /**
   * Default constructor, required for Jackson.
   */
  public JSONAlterIndexMessage() {}

  public JSONAlterIndexMessage(String server, String servicePrincipal, Index before, Index after,
                               Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.db = after.getDbName();
    this.timestamp = timestamp;
    try {
      this.beforeIndexObjJson = JSONMessageFactory.createIndexObjJson(before);
      this.afterIndexObjJson = JSONMessageFactory.createIndexObjJson(after);
    } catch (TException ex) {
      throw new IllegalArgumentException("Could not serialize Index object", ex);
    }

    checkValid();
  }

  @Override
  public String getDB() { return db; }

  @Override
  public String getServer() { return server; }

  @Override
  public String getServicePrincipal() { return servicePrincipal; }

  @Override
  public Long getTimestamp() { return timestamp; }

  public String getBeforeIndexObjJson() {
    return beforeIndexObjJson;
  }

  public String getAfterIndexObjJson() {
    return afterIndexObjJson;
  }

  @Override
  public Index getIndexObjBefore() throws Exception {
    return (Index)  JSONMessageFactory.getTObj(beforeIndexObjJson, Index.class);
  }

  @Override
  public Index getIndexObjAfter() throws Exception {
    return (Index)  JSONMessageFactory.getTObj(afterIndexObjJson, Index.class);
  }

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