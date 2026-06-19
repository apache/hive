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
import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionsMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.thrift.TException;

public class JSONAlterPartitionsMessage extends AlterPartitionsMessage {

  @JsonProperty
  String server, servicePrincipal, db, table, tableType, tableObjJson;

  @JsonProperty
  String isTruncateOp;

  @JsonProperty
  Long timestamp, writeId;

  @JsonProperty
  List<Map<String, String>> partitions;

  @JsonProperty
  List<String> partitionListJson;

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
    this.isTruncateOp = Boolean.toString(isTruncateOp);
    this.timestamp = timestamp;
    this.writeId = writeId;
    this.partitions = new ArrayList<>();
    this.partitionListJson = new ArrayList<>();
    try {
      this.tableObjJson = MessageBuilder.createTableObjJson(tableObj);
      Iterator<Partition> iterator = partitionsAfter.iterator();
      while (iterator.hasNext()) {
        Partition partitionObj = iterator.next();
        partitions.add(MessageBuilder.getPartitionKeyValues(tableObj, partitionObj));
        partitionListJson.add(MessageBuilder.createPartitionObjJson(partitionObj));
      }
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
  public Table getTableObj() throws Exception {
    return (Table) MessageBuilder.getTObj(tableObjJson,Table.class);
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
  public List<Map<String, String>> getPartitions() {
    return partitions;
  }

  @Override
  public Iterable<Partition> getPartitionObjs() throws Exception {
    // glorified cast from Iterable<TBase> to Iterable<Partition>
    return Iterables.transform(
        MessageBuilder.getTObjs(partitionListJson, Partition.class),
        new Function<Object, Partition>() {
          @Nullable
          @Override
          public Partition apply(@Nullable Object input) {
            return (Partition) input;
          }
        });
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
