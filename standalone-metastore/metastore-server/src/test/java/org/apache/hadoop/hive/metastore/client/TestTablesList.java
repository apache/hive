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

import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

/**
 * Test class for IMetaStoreClient API. Testing the Table related functions for metadata
 * querying like getting one, or multiple tables, and table name lists.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestTablesList extends MetaStoreClientTest {
  private static final String DEFAULT_DATABASE = "default";
  private static final String OTHER_DATABASE = "dummy";
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;
  private Table[] testTables = new Table[7];

  public TestTablesList(String name, AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
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
            .create(client, metaStore.getConf());

    testTables[1] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_1")
            .addCol("test_col", "int")
            .setOwner("Owner1")
            .setLastAccessTime(2000)
            .addTableParam("param1", "value2")
            .create(client, metaStore.getConf());

    testTables[2] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_2")
            .addCol("test_col", "int")
            .setOwner("Owner2")
            .setLastAccessTime(1000)
            .addTableParam("param1", "value2")
            .create(client, metaStore.getConf());

    testTables[3] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_3")
            .addCol("test_col", "int")
            .setOwner("Owner3")
            .setLastAccessTime(3000)
            .addTableParam("param1", "value2")
            .create(client, metaStore.getConf());

    testTables[4] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_4")
            .addCol("test_col", "int")
            .setOwner("Tester")
            .setLastAccessTime(2500)
            .addTableParam("param1", "value4")
            .create(client, metaStore.getConf());

    testTables[5] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("filter_test_table_5")
            .addCol("test_col", "int")
            .create(client, metaStore.getConf());

    new DatabaseBuilder().setName(OTHER_DATABASE).create(client, metaStore.getConf());

    testTables[6] =
        new TableBuilder()
            .setDbName(OTHER_DATABASE)
            .setTableName("filter_test_table_0")
            .addCol("test_col", "int")
            .setOwner("Owner1")
            .setLastAccessTime(1000)
            .addTableParam("param1", "value1")
            .create(client, metaStore.getConf());

    // Reload tables from the MetaStore
    for(int i=0; i < testTables.length; i++) {
      testTables[i] = client.getTable(testTables[i].getCatName(), testTables[i].getDbName(),
          testTables[i].getTableName());
    }
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (client != null) {
        try {
          client.close();
        } catch (Exception e) {
          // HIVE-19729: Shallow the exceptions based on the discussion in the Jira
        }
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

  @Test
  public void otherCatalogs() throws TException {
    String catName = "list_tables_in_other_catalogs";
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .build();
    client.createCatalog(cat);

    String dbName = "db_in_other_catalog";
    // For this one don't specify a location to make sure it gets put in the catalog directory
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .setCatalogName(catName)
        .create(client, metaStore.getConf());

    String[] tableNames = new String[4];
    for (int i = 0; i < tableNames.length; i++) {
      tableNames[i] = "table_in_other_catalog_" + i;
      TableBuilder builder = new TableBuilder()
          .inDb(db)
          .setTableName(tableNames[i])
          .addCol("col1_" + i, ColumnType.STRING_TYPE_NAME)
          .addCol("col2_" + i, ColumnType.INT_TYPE_NAME);
      if (i == 0) builder.addTableParam("the_key", "the_value");
      builder.create(client, metaStore.getConf());
    }

    String filter = hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS + "the_key=\"the_value\"";
    List<String> fetchedNames = client.listTableNamesByFilter(catName, dbName, filter, (short)-1);
    Assert.assertEquals(1, fetchedNames.size());
    Assert.assertEquals(tableNames[0], fetchedNames.get(0));
  }

  @Test(expected = UnknownDBException.class)
  public void listTablesBogusCatalog() throws TException {
    String filter = hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS + "the_key=\"the_value\"";
    List<String> fetchedNames = client.listTableNamesByFilter("", DEFAULT_DATABASE_NAME,
        filter, (short)-1);
  }
}
