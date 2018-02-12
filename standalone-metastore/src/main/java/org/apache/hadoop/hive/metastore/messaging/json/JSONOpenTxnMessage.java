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
import org.apache.hadoop.hive.metastore.messaging.OpenTxnMessage;
import org.apache.thrift.TException;
import org.codehaus.jackson.annotate.JsonProperty;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

/**
 * JSON implementation of InsertMessage
 */
public class JSONOpenTxnMessage extends OpenTxnMessage {

  @JsonProperty
  Long txnid;

  Long timestamp;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONOpenTxnMessage() {
  }

  public JSONOpenTxnMessage(Long txnid, Long timestamp) {
    this.timestamp = timestamp;
    this.txnid = txnid;
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
    return null;
  }

  @Override
  public String getServer() {
    return null;
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