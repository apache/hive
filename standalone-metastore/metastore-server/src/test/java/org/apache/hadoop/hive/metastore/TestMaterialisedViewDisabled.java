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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SourceTable;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Category(MetastoreUnitTest.class)
public class TestMaterialisedViewDisabled {
  private Configuration conf;
  private HiveMetaStoreClient client;

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.MATERIALIZED_VIEW_ENABLED, false);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    int port = MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.EXECUTE_SET_UGI, false);
    client = new HiveMetaStoreClient(conf);
  }

  @Test
  public void testMaterialisedViewDisabled() throws TException {
    String dbName = "db";
    List<String> tableNames = Arrays.asList("table1", "table2", "table3", "table4", "table5");

    Set<Table> tablesUsed = new HashSet<>();
    new DatabaseBuilder().setName(dbName).create(client, conf);
    for (String tableName : tableNames) {
      tablesUsed.add(createTable(dbName, tableName));
    }

    Assert.assertThrows("Creation of MATERIALIZED_VIEW is disabled via metastore.materialized.view.enabled",
        MetaException.class, () -> createMaterializedView(dbName, "mv1", tablesUsed));
  }

   Table createTable(String dbName, String tableName) throws TException {
    return new TableBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .addCol("foo", "string")
        .addCol("bar", "string")
        .create(client, conf);
  }

  private void createMaterializedView(String dbName, String tableName, Set<Table> tablesUsed)
      throws TException {
    Set<SourceTable> sourceTables = new HashSet<>(tablesUsed.size());
    for (Table table : tablesUsed) {
      sourceTables.add(createSourceTable(table));
    }

    new TableBuilder().setDbName(dbName)
        .setTableName(tableName)
        .setType(TableType.MATERIALIZED_VIEW.name())
        .addMaterializedViewReferencedTables(sourceTables)
        .addCol("foo", "string")
        .addCol("bar", "string")
        .create(client, conf);
  }

  public static SourceTable createSourceTable(Table table) {
    SourceTable sourceTable = new SourceTable();
    sourceTable.setTable(table);
    sourceTable.setInsertedCount(0L);
    sourceTable.setUpdatedCount(0L);
    sourceTable.setDeletedCount(0L);
    return sourceTable;
  }
}
