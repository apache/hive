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

package org.apache.hadoop.hive.metastore.client;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test class for IMetaStoreClient API. Testing the Table related functions for metadata
 * querying like getting one, or multiple tables, and table name lists.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestTablesList {
  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should remove our own copy
  private static Set<AbstractMetaStoreService> metaStoreServices = null;
  private static final String DEFAULT_DATABASE = "default";
  private static final String OTHER_DATABASE = "dummy";
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;
  private Table[] testTables = new Table[7];

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getMetaStoreToTest() throws Exception {
    List<Object[]> result = MetaStoreFactoryForTests.getMetaStores();
    metaStoreServices = result.stream()
        .map(test -> (AbstractMetaStoreService)test[1])
        .collect(Collectors.toSet());
    return result;
  }

  public TestTablesList(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
    this.metaStore.start();
  }

  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should move this to @AfterParam
  @AfterClass
  public static void stopMetaStores() throws Exception {
    for(AbstractMetaStoreService metaStoreService : metaStoreServices) {
      metaStoreService.stop();
    }
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

    // Clean up the database
    client.dropDatabase(OTHER_DATABASE, true, true, true);
    // Drop every table in the default database
    for(String tableName : client.getAllTables(DEFAULT_DATABASE)) {
      client.dropTable(DEFAULT_DATABASE, tableName, true, true, true);
    }

    // Clean up trash
    metaStore.cleanWarehouseDirs();

    testTables[0] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_0")
            .addCol("test_col", "int")
            .setOwner("Owner1")
            .setLastAccessTime(1000)
            .addTableParam("param1", "value1")
            .build();

    testTables[1] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_1")
            .addCol("test_col", "int")
            .setOwner("Owner1")
            .setLastAccessTime(2000)
            .addTableParam("param1", "value2")
            .build();

    testTables[2] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_2")
            .addCol("test_col", "int")
            .setOwner("Owner2")
            .setLastAccessTime(1000)
            .addTableParam("param1", "value2")
            .build();

    testTables[3] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_3")
            .addCol("test_col", "int")
            .setOwner("Owner3")
            .setLastAccessTime(3000)
            .addTableParam("param1", "value2")
            .build();

    testTables[4] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_4")
            .addCol("test_col", "int")
            .setOwner("Tester")
            .setLastAccessTime(2500)
            .addTableParam("param1", "value4")
            .build();

    testTables[5] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_5")
            .addCol("test_col", "int")
            .build();

    client.createDatabase(new DatabaseBuilder().setName(OTHER_DATABASE).build());

    testTables[6] =
        new TableBuilder()
            .setDbName(OTHER_DATABASE)
            .setTableName("filter_test_table_0")
            .addCol("test_col", "int")
            .setOwner("Owner1")
            .setLastAccessTime(1000)
            .addTableParam("param1", "value1")
            .build();

    // Create the tables in the MetaStore
    for(int i=0; i < testTables.length; i++) {
      client.createTable(testTables[i]);
    }

    // Reload tables from the MetaStore
    for(int i=0; i < testTables.length; i++) {
      testTables[i] = client.getTable(testTables[i].getDbName(), testTables[i].getTableName());
    }
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (client != null) {
        client.close();
      }
    } finally {
      client = null;
    }
  }

  @Test
  public void testListTableNamesByFilterCheckOwner() throws Exception {
    String filter = hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER + "=\"Owner1\"";
    List<String> tableNames = client.listTableNamesByFilter(DEFAULT_DATABASE, filter, (short) -1);
    Assert.assertEquals("Found tables", 2, tableNames.size());
    Assert.assertTrue(tableNames.contains(testTables[0].getTableName()));
    Assert.assertTrue(tableNames.contains(testTables[1].getTableName()));
  }

  @Test
  public void testListTableNamesByFilterCheckLastAccess() throws Exception {
    String filter = hive_metastoreConstants.HIVE_FILTER_FIELD_LAST_ACCESS + "=1000";
    List<String> tableNames = client.listTableNamesByFilter(DEFAULT_DATABASE, filter, (short)-1);
    Assert.assertEquals("Found tables", 2, tableNames.size());
    Assert.assertTrue(tableNames.contains(testTables[0].getTableName()));
    Assert.assertTrue(tableNames.contains(testTables[2].getTableName()));
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
  public void testListTableNamesByFilterCheckLike() throws Exception {
    String filter = hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER + " LIKE \"Owner.*\"";
    List<String> tableNames = client.listTableNamesByFilter(DEFAULT_DATABASE, filter, (short)-1);
    Assert.assertEquals("Found tables", 4, tableNames.size());
    Assert.assertTrue(tableNames.contains(testTables[0].getTableName()));
    Assert.assertTrue(tableNames.contains(testTables[1].getTableName()));
    Assert.assertTrue(tableNames.contains(testTables[2].getTableName()));
    Assert.assertTrue(tableNames.contains(testTables[3].getTableName()));
  }

  @Test
  public void testListTableNamesByFilterCheckLessOrEquals() throws Exception {
    String filter = hive_metastoreConstants.HIVE_FILTER_FIELD_LAST_ACCESS + "<=2000";
    List<String> tableNames = client.listTableNamesByFilter(DEFAULT_DATABASE, filter, (short)-1);
    Assert.assertEquals("Found tables", 3, tableNames.size());
    Assert.assertTrue(tableNames.contains(testTables[0].getTableName()));
    Assert.assertTrue(tableNames.contains(testTables[1].getTableName()));
    Assert.assertTrue(tableNames.contains(testTables[2].getTableName()));
  }

  @Test
  public void testListTableNamesByFilterCheckNotEquals() throws Exception {
    String filter = hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS + "param1<>\"value2\"";
    List<String> tableNames = client.listTableNamesByFilter(DEFAULT_DATABASE, filter, (short)-1);
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

  @Test
  public void testListTableNamesByFilterCheckLimit() throws Exception {
    // Check the limit
    String filter = hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER + " LIKE \"Owner.*\"";
    List<String> tableNames = client.listTableNamesByFilter(DEFAULT_DATABASE, filter, (short)1);
    Assert.assertEquals("Found tables", 1, tableNames.size());
    Assert.assertTrue(tableNames.contains(testTables[0].getTableName())
                          || tableNames.contains(testTables[1].getTableName())
                          || tableNames.contains(testTables[2].getTableName())
                          || tableNames.contains(testTables[3].getTableName()));
  }

  @Test
  public void testListTableNamesByFilterCheckNoSuchDatabase() throws Exception {
    // No such database
    List<String> tableNames = client.listTableNamesByFilter("no_such_database",
        hive_metastoreConstants.HIVE_FILTER_FIELD_LAST_ACCESS + ">2000", (short)-1);
    Assert.assertEquals("Found tables", 0, tableNames.size());
  }

  @Test(expected = UnknownDBException.class)
  public void testListTableNamesByFilterNullDatabase() throws Exception {
    client.listTableNamesByFilter(null,
        hive_metastoreConstants.HIVE_FILTER_FIELD_LAST_ACCESS + ">2000", (short)-1);
  }

  @Test(expected = InvalidOperationException.class)
  public void testListTableNamesByFilterNullFilter() throws Exception {
    client.listTableNamesByFilter(DEFAULT_DATABASE, null, (short) -1);
  }

  @Test(expected = MetaException.class)
  public void testListTableNamesByFilterInvalidFilter() throws Exception {
    client.listTableNamesByFilter(DEFAULT_DATABASE, "invalid filter", (short)-1);
  }
}
