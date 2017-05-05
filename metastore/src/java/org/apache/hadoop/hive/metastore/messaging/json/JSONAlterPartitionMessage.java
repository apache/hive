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

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.thrift.TException;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * JSON alter table message
 */
public class JSONAlterPartitionMessage extends AlterPartitionMessage {

  @JsonProperty
  String server, servicePrincipal, db, table, tableObjJson;

  @JsonProperty
  String isTruncateOp;

  @JsonProperty
  Long timestamp;

  @JsonProperty
  Map<String, String> keyValues;

  @JsonProperty
  String partitionObjBeforeJson, partitionObjAfterJson;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONAlterPartitionMessage() {
  }

  public JSONAlterPartitionMessage(String server, String servicePrincipal, Table tableObj,
      Partition partitionObjBefore, Partition partitionObjAfter, boolean isTruncateOp, Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.db = tableObj.getDbName();
    this.table = tableObj.getTableName();
    this.isTruncateOp = Boolean.toString(isTruncateOp);
    this.timestamp = timestamp;
    this.keyValues = JSONMessageFactory.getPartitionKeyValues(tableObj, partitionObjBefore);
    try {
      this.tableObjJson = JSONMessageFactory.createTableObjJson(tableObj);
      this.partitionObjBeforeJson = JSONMessageFactory.createPartitionObjJson(partitionObjBefore);
      this.partitionObjAfterJson = JSONMessageFactory.createPartitionObjJson(partitionObjAfter);
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
  public boolean getIsTruncateOp() { return Boolean.parseBoolean(isTruncateOp); }

  @Override
  public Map<String, String> getKeyValues() {
    return keyValues;
  }

  @Override
  public Table getTableObj() throws Exception {
    return (Table) JSONMessageFactory.getTObj(tableObjJson,Table.class);
  }

  @Override
  public Partition getPtnObjBefore() throws Exception {
    return (Partition) JSONMessageFactory.getTObj(partitionObjBeforeJson, Partition.class);
  }

  @Override
  public Partition getPtnObjAfter() throws Exception {
    return (Partition) JSONMessageFactory.getTObj(partitionObjAfterJson, Partition.class);
  }

  public String getTableObjJson() {
    return tableObjJson;
  }

  public String getPartitionObjBeforeJson() {
    return partitionObjBeforeJson;
  }

  public String getPartitionObjAfterJson() {
    return partitionObjAfterJson;
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
