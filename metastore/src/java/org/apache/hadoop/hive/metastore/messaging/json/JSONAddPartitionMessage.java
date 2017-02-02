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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.PartitionFiles;
import org.apache.thrift.TException;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * JSON implementation of AddPartitionMessage.
 */
public class JSONAddPartitionMessage extends AddPartitionMessage {

  @JsonProperty
  String server, servicePrincipal, db, table, tableObjJson;

  @JsonProperty
  Long timestamp;

  @JsonProperty
  List<Map<String, String>> partitions;

  @JsonProperty
  List<String> partitionListJson;

  @JsonProperty
  List<PartitionFiles> partitionFiles;

  /**
   * Default Constructor. Required for Jackson.
   */
  public JSONAddPartitionMessage() {
  }

  /**
   * Note that we get an Iterator rather than an Iterable here: so we can only walk thru the list once
   */
  public JSONAddPartitionMessage(String server, String servicePrincipal, Table tableObj,
      Iterator<Partition> partitionsIterator, Iterator<PartitionFiles> partitionFileIter,
      Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.db = tableObj.getDbName();
    this.table = tableObj.getTableName();
    this.timestamp = timestamp;
    partitions = new ArrayList<Map<String, String>>();
    partitionListJson = new ArrayList<String>();
    Partition partitionObj;
    try {
      this.tableObjJson = JSONMessageFactory.createTableObjJson(tableObj);
      while (partitionsIterator.hasNext()) {
        partitionObj = partitionsIterator.next();
        partitions.add(JSONMessageFactory.getPartitionKeyValues(tableObj, partitionObj));
        partitionListJson.add(JSONMessageFactory.createPartitionObjJson(partitionObj));
      }
    } catch (TException e) {
      throw new IllegalArgumentException("Could not serialize: ", e);
    }
    this.partitionFiles = Lists.newArrayList(partitionFileIter);
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
  public String getTable() {
    return table;
  }

  @Override
  public Table getTableObj() throws Exception {
    return (Table) JSONMessageFactory.getTObj(tableObjJson,Table.class);
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public List<Map<String, String>> getPartitions() {
    return partitions;
  }

  @Override
  public Iterable<Partition> getPartitionObjs() throws Exception {
    // glorified cast from Iterable<TBase> to Iterable<Partition>
    return Iterables.transform(
        JSONMessageFactory.getTObjs(partitionListJson,Partition.class),
        new Function<Object, Partition>() {
      @Nullable
      @Override
      public Partition apply(@Nullable Object input) {
        return (Partition) input;
      }
    });
  }

  public String getTableObjJson() {
    return tableObjJson;
  }

  public List<String> getPartitionListJson() {
    return partitionListJson;
  }

  @Override
  public String toString() {
    try {
      return JSONMessageDeserializer.mapper.writeValueAsString(this);
    } catch (Exception exception) {
      throw new IllegalArgumentException("Could not serialize: ", exception);
    }
  }

  @Override
  public Iterable<PartitionFiles> getPartitionFilesIter() {
    return partitionFiles;
  }

}
