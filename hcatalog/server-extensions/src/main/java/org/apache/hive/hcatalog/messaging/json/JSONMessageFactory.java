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

package org.apache.hive.hcatalog.messaging.json;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hive.hcatalog.messaging.AlterIndexMessage;
import org.apache.hive.hcatalog.messaging.CreateFunctionMessage;
import org.apache.hive.hcatalog.messaging.CreateIndexMessage;
import org.apache.hive.hcatalog.messaging.DropFunctionMessage;
import org.apache.hive.hcatalog.messaging.DropIndexMessage;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The JSON implementation of the MessageFactory. Constructs JSON implementations of
 * each message-type.
 */
public class JSONMessageFactory extends MessageFactory {

  private static final Log LOG = LogFactory.getLog(JSONMessageFactory.class.getName());

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
        table.getTableName(), now());
  }

  @Override
  public AlterTableMessage buildAlterTableMessage(Table before, Table after) {
    return new JSONAlterTableMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, before.getDbName(),
        before.getTableName(), now());
  }

  @Override
  public DropTableMessage buildDropTableMessage(Table table) {
    return new JSONDropTableMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(), table.getTableName(),
        now());
  }

  @Override
  public AddPartitionMessage buildAddPartitionMessage(Table table, List<Partition> partitions) {
    return new JSONAddPartitionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(),
        table.getTableName(), getPartitionKeyValues(table, partitions), now());
  }

  @Override
  @InterfaceAudience.LimitedPrivate({"Hive"})
  @InterfaceStability.Evolving
  public AddPartitionMessage buildAddPartitionMessage(Table table, PartitionSpecProxy partitionSpec) {
    return new JSONAddPartitionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(),
        table.getTableName(), getPartitionKeyValues(table, partitionSpec), now());
  }

  @Override
  public AlterPartitionMessage buildAlterPartitionMessage(Table table, Partition before, Partition after) {
    return new JSONAlterPartitionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL,
        before.getDbName(), before.getTableName(), getPartitionKeyValues(table,before),now());
  }

  @Override
  public DropPartitionMessage buildDropPartitionMessage(Table table, Partition partition) {
    return new JSONDropPartitionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, partition.getDbName(),
        partition.getTableName(), Arrays.asList(getPartitionKeyValues(table, partition)), now());
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
    return new JSONInsertMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, db, table, partKeyVals,
        files, now());
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

  private static List<Map<String, String>> getPartitionKeyValues(Table table, List<Partition> partitions) {
    List<Map<String, String>> partitionList = new ArrayList<Map<String, String>>(partitions.size());
    for (Partition partition : partitions)
      partitionList.add(getPartitionKeyValues(table, partition));
    return partitionList;
  }

  @InterfaceAudience.LimitedPrivate({"Hive"})
  @InterfaceStability.Evolving
  private static List<Map<String, String>> getPartitionKeyValues(Table table, PartitionSpecProxy partitionSpec) {
    List<Map<String, String>> partitionList = new ArrayList<Map<String, String>>();
    PartitionSpecProxy.PartitionIterator iterator = partitionSpec.getPartitionIterator();
    while (iterator.hasNext()) {
      Partition partition = iterator.next();
      partitionList.add(getPartitionKeyValues(table, partition));
    }
    return partitionList;
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