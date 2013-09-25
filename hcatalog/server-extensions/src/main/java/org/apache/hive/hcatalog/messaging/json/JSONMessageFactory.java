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

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.hcatalog.messaging.AddPartitionMessage;
import org.apache.hive.hcatalog.messaging.CreateDatabaseMessage;
import org.apache.hive.hcatalog.messaging.CreateTableMessage;
import org.apache.hive.hcatalog.messaging.DropDatabaseMessage;
import org.apache.hive.hcatalog.messaging.DropPartitionMessage;
import org.apache.hive.hcatalog.messaging.DropTableMessage;
import org.apache.hive.hcatalog.messaging.MessageDeserializer;
import org.apache.hive.hcatalog.messaging.MessageFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The JSON implementation of the MessageFactory. Constructs JSON implementations of
 * each message-type.
 */
public class JSONMessageFactory extends MessageFactory {

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
        System.currentTimeMillis() / 1000);
  }

  @Override
  public DropDatabaseMessage buildDropDatabaseMessage(Database db) {
    return new JSONDropDatabaseMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, db.getName(),
        System.currentTimeMillis() / 1000);
  }

  @Override
  public CreateTableMessage buildCreateTableMessage(Table table) {
    return new JSONCreateTableMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(),
        table.getTableName(), System.currentTimeMillis()/1000);
  }

  @Override
  public DropTableMessage buildDropTableMessage(Table table) {
    return new JSONDropTableMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(), table.getTableName(),
        System.currentTimeMillis()/1000);
  }

  @Override
  public AddPartitionMessage buildAddPartitionMessage(Table table, Partition partition) {
    return new JSONAddPartitionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, partition.getDbName(),
        partition.getTableName(), Arrays.asList(getPartitionKeyValues(table, partition)),
        System.currentTimeMillis()/1000);
  }

  @Override
  public DropPartitionMessage buildDropPartitionMessage(Table table, Partition partition) {
    return new JSONDropPartitionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, partition.getDbName(),
        partition.getTableName(), Arrays.asList(getPartitionKeyValues(table, partition)),
        System.currentTimeMillis()/1000);
  }

  private static Map<String, String> getPartitionKeyValues(Table table, Partition partition) {
    Map<String, String> partitionKeys = new LinkedHashMap<String, String>();
    for (int i=0; i<table.getPartitionKeysSize(); ++i)
      partitionKeys.put(table.getPartitionKeys().get(i).getName(),
          partition.getValues().get(i));
    return partitionKeys;
  }
}
