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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.client.MetaStoreClientTest;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestTablesListByFilter extends MetaStoreClientTest {
  private static final String DEFAULT_DATABASE = "test_tables_list_by_filter";
  private AbstractMetaStoreService metaStore;
  private HiveMetaStoreClient client;
  private Table[] testTables = new Table[6];

  public TestTablesListByFilter(String name, AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  @Before
  public void setupDb() throws Exception {
    client = metaStore.getClient();
    // Clean up the database
    client.dropDatabase(DEFAULT_DATABASE, true, true, true);
    Configuration conf = metaStore.getConf();
    new DatabaseBuilder().setName(DEFAULT_DATABASE).create(client, conf);

    testTables[0] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_0")
            .addCol("test_col", "int")
            .setOwner("Owner1")
            .setLastAccessTime(1000)
            .addTableParam("param1", "value1")
            .create(client, conf);

    testTables[1] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_1")
            .addCol("test_col", "int")
            .setOwner("Owner1")
            .setLastAccessTime(2000)
            .addTableParam("param1", "value2")
            .create(client, conf);

    testTables[2] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_2")
            .addCol("test_col", "int")
            .setOwner("Owner2")
            .setLastAccessTime(1000)
            .addTableParam("param1", "value2")
            .create(client, conf);

    testTables[3] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_3")
            .addCol("test_col", "int")
            .setOwner("Owner3")
            .setLastAccessTime(3000)
            .addTableParam("param1", "value2")
            .create(client, conf);

    testTables[4] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_4")
            .addCol("test_col", "int")
            .setOwner("Tester")
            .setLastAccessTime(2500)
            .addTableParam("param1", "value4")
            .create(client, conf);

    testTables[5] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_5")
            .addCol("test_col", "int")
            .create(client, conf);

    // Reload tables from the MetaStore
    for(int i=0; i < testTables.length; i++) {
      testTables[i] = client.getTable(testTables[i].getCatName(), testTables[i].getDbName(),
          testTables[i].getTableName());
    }
  }

  @After
  public void tearDownDb() throws Exception {
    try {
      if (client != null) {
        // Clean up the database
        client.dropDatabase(DEFAULT_DATABASE, true, true, true);
      }
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testListTableNamesByFilterCheckParameter() throws Exception {
    String filter = hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS + "param1=\"value2\"";
    List<String> tableNames = client.listTableNamesByFilter(DEFAULT_DATABASE, filter, (short)-1);
    Assert.assertEquals("Found tables", 3, tableNames.size());
    Assert.assertTrue(tableNames.contains(testTables[1].getTableName()));
    Assert.assertTrue(tableNames.contains(testTables[2].getTableName()));
    Assert.assertTrue(tableNames.contains(testTables[3].getTableName()));
  }

  @Test
  public void testListTableNamesByFilterCheckNotEquals() throws Exception {
    String filter = hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS + "param1<>\"value2\"";
    List<String> tableNames = client.listTableNamesByFilter(DEFAULT_DATABASE, filter, (short)-1);
    filter = hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS + "param1 != \"value2\"";
    List<String> tableNames1 = client.listTableNamesByFilter(DEFAULT_DATABASE, filter, (short) -1);
    Assert.assertEquals(tableNames, tableNames1);
    Assert.assertEquals("Found tables", 2, tableNames.size());
    Assert.assertTrue(tableNames.contains(testTables[0].getTableName()));
    Assert.assertTrue(tableNames.contains(testTables[4].getTableName()));
  }

  @Test
  public void testListTableNamesByFilterCheckCombined() throws Exception {
    // Combined: last_access<=3000 and (Owner="Tester" or param1="param2")
    String filter = hive_metastoreConstants.HIVE_FILTER_FIELD_LAST_ACCESS + "<3000 and ("
        + hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER + "=\"Tester\" or "
        + hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS + "param1=\"value2\")";
    List<String> tableNames = client.listTableNamesByFilter(DEFAULT_DATABASE, filter, (short)-1);
    Assert.assertEquals("Found tables", 3, tableNames.size());
    Assert.assertTrue(tableNames.contains(testTables[1].getTableName()));
    Assert.assertTrue(tableNames.contains(testTables[2].getTableName()));
    Assert.assertTrue(tableNames.contains(testTables[4].getTableName()));
  }

}
