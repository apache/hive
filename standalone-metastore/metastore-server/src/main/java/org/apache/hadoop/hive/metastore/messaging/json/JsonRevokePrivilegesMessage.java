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

import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.RevokePrivilegesMessage;
import org.apache.thrift.TException;
import org.codehaus.jackson.annotate.JsonProperty;

public class JsonRevokePrivilegesMessage extends RevokePrivilegesMessage {
  @JsonProperty
  private String server, servicePrincipal, privilegesObj;

  @JsonProperty
  private boolean grantOption;

  @JsonProperty
  private Long timestamp;

  public JsonRevokePrivilegesMessage() {}

  public JsonRevokePrivilegesMessage(String server, String servicePrincipal, PrivilegeBag privileges, boolean grantOption, Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.grantOption = grantOption;
    this.timestamp = timestamp;
    try {
      this.privilegesObj = MessageBuilder.createPrivilegesObjJson(privileges);
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
    return null;
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  public PrivilegeBag getPrivileges() throws Exception {
    return (PrivilegeBag) MessageBuilder.getTObj(privilegesObj, PrivilegeBag.class);
  }

  public boolean isGrantOption() {
    return grantOption;
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
