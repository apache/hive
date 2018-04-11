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
import org.apache.hadoop.hive.metastore.messaging.CommitTxnMessage;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * JSON implementation of CommitTxnMessage
 */
public class JSONCommitTxnMessage extends CommitTxnMessage {

  @JsonProperty
  private Long txnid;

  @JsonProperty
  private Long timestamp;

  @JsonProperty
  private String server;

  @JsonProperty
  private String servicePrincipal;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONCommitTxnMessage() {
  }

  public JSONCommitTxnMessage(String server, String servicePrincipal, Long txnid, Long timestamp) {
    this.timestamp = timestamp;
    this.txnid = txnid;
    this.server = server;
    this.servicePrincipal = servicePrincipal;
  }

  @Override
  public Long getTxnId() {
    return txnid;
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public String getDB() {
    return null;
  }

  @Override
  public String getServicePrincipal() {
    return servicePrincipal;
  }

  @Override
  public String getServer() {
    return server;
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
