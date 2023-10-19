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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionsMessage;

public class JSONAlterPartitionsMessage extends AlterPartitionsMessage {

  @JsonProperty
  String server, servicePrincipal, db, table, tableType;

  @JsonProperty
  String isTruncateOp;

  @JsonProperty
  Long timestamp, writeId;

  @JsonProperty
  List<String> partitionKeys;

  @JsonProperty
  List<List<String>> partitionValues;

  List<Partition> partitionsAfter;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONAlterPartitionsMessage() {
  }

  public JSONAlterPartitionsMessage(String server, String servicePrincipal, Table tableObj,
      List<Partition> partitionsAfter, boolean isTruncateOp, Long writeId, Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.db = tableObj.getDbName();
    this.table = tableObj.getTableName();
    this.tableType = tableObj.getTableType();
    this.partitionsAfter = partitionsAfter;
    this.partitionKeys = tableObj.getPartitionKeys().stream()
        .map(column -> column.getName()).collect(Collectors.toList());
    this.partitionValues = new ArrayList<>(partitionsAfter.size());
    partitionsAfter.forEach(partition -> partitionValues.add(new ArrayList<>(partition.getValues())));
    this.isTruncateOp = Boolean.toString(isTruncateOp);
    this.timestamp = timestamp;
    this.writeId = writeId;
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
    return db;
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public String getTable() {
    return table;
  }

  @Override
  public String getTableType() {
    return tableType != null ? tableType : "";
  }

  @Override
  public boolean getIsTruncateOp() {
    return Boolean.parseBoolean(isTruncateOp);
  }

  @Override
  public Long getWriteId() {
    return writeId == null ? 0 : writeId;
  }

  @Override
  public List<String> getPartitionKeys() {
    return partitionKeys;
  }

  @Override
  public List<List<String>> getPartitionValues() {
    return partitionValues;
  }

  @Override
  public List<Partition> getPartitionsAfter() {
    return partitionsAfter;
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
