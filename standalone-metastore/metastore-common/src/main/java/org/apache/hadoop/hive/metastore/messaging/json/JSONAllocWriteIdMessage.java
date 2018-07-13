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

import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.messaging.AllocWriteIdMessage;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * JSON implementation of AllocWriteId
 */
public class JSONAllocWriteIdMessage extends AllocWriteIdMessage {

  @JsonProperty
  private String server, servicePrincipal, dbName, tableName;

  @JsonProperty
  private List<Long> txnIdList, writeIdList;

  @JsonProperty
  private Long timestamp;

  private List<TxnToWriteId> txnToWriteIdList;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONAllocWriteIdMessage() {
  }

  public JSONAllocWriteIdMessage(String server, String servicePrincipal,
                                 List<TxnToWriteId> txnToWriteIdList, String dbName, String tableName, Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.timestamp = timestamp;
    this.txnIdList = new ArrayList<>();
    this.writeIdList = new ArrayList<>();
    for (TxnToWriteId txnToWriteId : txnToWriteIdList) {
      this.txnIdList.add(txnToWriteId.getTxnId());
      this.writeIdList.add(txnToWriteId.getWriteId());
    }
    this.tableName = tableName;
    this.dbName = dbName;
    this.txnToWriteIdList = txnToWriteIdList;
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
    return dbName;
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public List<TxnToWriteId> getTxnToWriteIdList() {
    // after deserialization, need to recreate the txnToWriteIdList as its not under JsonProperty.
    if (txnToWriteIdList == null) {
      txnToWriteIdList = new ArrayList<>();
      for (int idx = 0; idx < txnIdList.size(); idx++) {
        txnToWriteIdList.add(new TxnToWriteId(txnIdList.get(idx), writeIdList.get(idx)));
      }
    }
    return txnToWriteIdList;
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
