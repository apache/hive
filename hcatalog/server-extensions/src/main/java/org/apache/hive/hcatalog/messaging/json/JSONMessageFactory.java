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

package org.apache.hive.hcatalog.messaging.json;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hive.hcatalog.messaging.AlterIndexMessage;
import org.apache.hive.hcatalog.messaging.CreateFunctionMessage;
import org.apache.hive.hcatalog.messaging.CreateIndexMessage;
import org.apache.hive.hcatalog.messaging.DropFunctionMessage;
import org.apache.hive.hcatalog.messaging.DropIndexMessage;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.hcatalog.messaging.AddPartitionMessage;
import org.apache.hive.hcatalog.messaging.AlterPartitionMessage;
import org.apache.hive.hcatalog.messaging.AlterTableMessage;
import org.apache.hive.hcatalog.messaging.CreateDatabaseMessage;
import org.apache.hive.hcatalog.messaging.CreateTableMessage;
import org.apache.hive.hcatalog.messaging.DropDatabaseMessage;
import org.apache.hive.hcatalog.messaging.DropPartitionMessage;
import org.apache.hive.hcatalog.messaging.DropTableMessage;
import org.apache.hive.hcatalog.messaging.InsertMessage;
import org.apache.hive.hcatalog.messaging.MessageDeserializer;
import org.apache.hive.hcatalog.messaging.MessageFactory;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The JSON implementation of the MessageFactory. Constructs JSON implementations of
 * each message-type.
 */
public class JSONMessageFactory extends MessageFactory {

  private static final Logger LOG = LoggerFactory.getLogger(JSONMessageFactory.class.getName());

  private static JSONMessageDeserializer deserializer = new JSONMessageDeserializer();

  @Override
  public MessageDeserializer getDeserializer() {
    return deserializer;
  }

  @Override
  public String getVersion() {
    return "0.1";
  }

  @Override
  public String getMessageFormat() {
    return "json";
  }

  @Override
  public CreateDatabaseMessage buildCreateDatabaseMessage(Database db) {
    return new JSONCreateDatabaseMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, db.getName(),
        now());
  }

  @Override
  public DropDatabaseMessage buildDropDatabaseMessage(Database db) {
    return new JSONDropDatabaseMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, db.getName(),
        now());
  }

  @Override
  public CreateTableMessage buildCreateTableMessage(Table table) {
    return new JSONCreateTableMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(),
        table.getTableName(), table.getTableType(), now());
  }

  @Override
  public AlterTableMessage buildAlterTableMessage(Table before, Table after) {
    return new JSONAlterTableMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, before.getDbName(),
        before.getTableName(), before.getTableType(), now());
  }

  @Override
  public DropTableMessage buildDropTableMessage(Table table) {
    return new JSONDropTableMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(),
        table.getTableName(), table.getTableType(), now());
  }

  @Override
  public AddPartitionMessage buildAddPartitionMessage(Table table, Iterator<Partition> partitionsIterator) {
    return new JSONAddPartitionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(),
        table.getTableName(), table.getTableType(),
        getPartitionKeyValues(table, partitionsIterator), now());
  }

  @Override
  public AlterPartitionMessage buildAlterPartitionMessage(Table table, Partition before, Partition after) {
    return new JSONAlterPartitionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL,
        before.getDbName(), before.getTableName(), table.getTableType(),
        getPartitionKeyValues(table,before),now());
  }

  @Override
  public DropPartitionMessage buildDropPartitionMessage(Table table, Iterator<Partition> partitions) {
    return new JSONDropPartitionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(),
        table.getTableName(), table.getTableType(),
        getPartitionKeyValues(table, partitions), now());
  }

  @Override
  public CreateFunctionMessage buildCreateFunctionMessage(Function fn) {
    return new JSONCreateFunctionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, fn,
        now());
  }

  @Override
  public DropFunctionMessage buildDropFunctionMessage(Function fn) {
    return new JSONDropFunctionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, fn,
        now());
  }

  @Override
  public CreateIndexMessage buildCreateIndexMessage(Index idx) {
    return new JSONCreateIndexMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, idx,
        now());
  }

  @Override
  public DropIndexMessage buildDropIndexMessage(Index idx) {
    return new JSONDropIndexMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, idx,
        now());
  }

  @Override
  public AlterIndexMessage buildAlterIndexMessage(Index before, Index after) {
    return new JSONAlterIndexMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, before, after,
        now());
  }

  @Override
  public InsertMessage buildInsertMessage(String db, String table, Map<String,String> partKeyVals,
                                          List<String> files) {
    return new JSONInsertMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, db, table, null,
        partKeyVals, files, now());
  }

  @Override
  public InsertMessage buildInsertMessage(String db, Table table, Map<String,String> partKeyVals,
      List<String> files) {
    return new JSONInsertMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(),
        table.getTableName(), table.getTableType(), partKeyVals, files, now());
  }

  private long now() {
    return System.currentTimeMillis() / 1000;
  }

  private static Map<String, String> getPartitionKeyValues(Table table, Partition partition) {
    Map<String, String> partitionKeys = new LinkedHashMap<String, String>();
    for (int i=0; i<table.getPartitionKeysSize(); ++i)
      partitionKeys.put(table.getPartitionKeys().get(i).getName(),
          partition.getValues().get(i));
    return partitionKeys;
  }

  private static List<Map<String, String>> getPartitionKeyValues(final Table table, Iterator<Partition> iterator) {
    return Lists.newArrayList(Iterators.transform(iterator, new com.google.common.base.Function<Partition, Map<String, String>>() {
      @Override
      public Map<String, String> apply(@Nullable Partition partition) {
        return getPartitionKeyValues(table, partition);
      }
    }));
  }

  static String createFunctionObjJson(Function functionObj) throws TException {
    TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
    return serializer.toString(functionObj, "UTF-8");
  }

  static String createIndexObjJson(Index indexObj) throws TException {
    TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
    return serializer.toString(indexObj, "UTF-8");
  }
}