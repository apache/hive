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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.events.CommitCompactionEvent;
import org.apache.hadoop.hive.metastore.messaging.CommitCompactionMessage;

/**
 * JSON implementation of CommitCompactionMessage
 */
public class JSONCommitCompactionMessage extends CommitCompactionMessage {

  @JsonProperty
  private Long txnid;

  @JsonProperty
  private Long timestamp;

  @JsonProperty
  private String server;

  @JsonProperty
  private String servicePrincipal;

  @JsonProperty
  private Long compactionId;

  @JsonProperty
  private CompactionType type;

  @JsonProperty
  private String dbname;

  @JsonProperty
  private String tableName;

  @JsonProperty
  private String partName;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONCommitCompactionMessage() {
  }

  public JSONCommitCompactionMessage(String server, String servicePrincipal, long timestamp,
      CommitCompactionEvent event) {
    this.timestamp = timestamp;
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.txnid = event.getTxnId();
    this.compactionId = event.getCompactionId();
    this.type = event.getType();
    this.dbname = event.getDbname();
    this.tableName = event.getTableName();
    this.partName = event.getPartName();
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
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
  public String getDB() {
    return dbname;
  }

  @Override
  public Long getTxnId() {
    return txnid;
  }

  @Override
  public Long getCompactionId() {
    return compactionId;
  }

  @Override
  public CompactionType getType() {
    return type;
  }

  @Override
  public String getDbname() {
    return dbname;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public String getPartName() {
    return partName;
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
