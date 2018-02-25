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
public class TestTablesGetExists {
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

  public TestTablesGetExists(String name, AbstractMetaStoreService metaStore) throws Exception {
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
            .setTableName("test_table")
            .addCol("test_col", "int")
            .build();

    testTables[1] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("test_view")
            .addCol("test_col", "int")
            .setType("VIEW")
            .build();

    testTables[2] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("test_table_to_find_1")
            .addCol("test_col", "int")
            .build();

    testTables[3] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("test_table_to_find_2")
            .addCol("test_col", "int")
            .setType("VIEW")
            .build();

    testTables[4] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("test_table_hidden_1")
            .addCol("test_col", "int")
            .build();

    client.createDatabase(new DatabaseBuilder().setName(OTHER_DATABASE).build());

    testTables[5] =
        new TableBuilder()
            .setDbName(OTHER_DATABASE)
            .setTableName("test_table")
            .addCol("test_col", "int")
            .build();

    testTables[6] =
        new TableBuilder()
            .setDbName(OTHER_DATABASE)
            .setTableName("test_table_to_find_3")
            .addCol("test_col", "int")
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
  public void testGetTableCaseInsensitive() throws Exception {
    Table table = testTables[0];

    // Test in upper case
    Table resultUpper = client.getTable(table.getDbName().toUpperCase(),
        table.getTableName().toUpperCase());
    Assert.assertEquals("Comparing tables", table, resultUpper);

    // Test in mixed case
    Table resultMix = client.getTable("DeFaUlt", "tEsT_TabLE");
    Assert.assertEquals("Comparing tables", table, resultMix);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetTableNoSuchDatabase() throws Exception {
    Table table = testTables[2];

    client.getTable("no_such_database", table.getTableName());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetTableNoSuchTable() throws Exception {
    Table table = testTables[2];

    client.getTable(table.getDbName(), "no_such_table");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetTableNoSuchTableInTheDatabase() throws Exception {
    Table table = testTables[2];

    client.getTable(OTHER_DATABASE, table.getTableName());
  }

  @Test
  public void testGetTableNullDatabase() throws Exception {
    try {
      client.getTable(null, OTHER_DATABASE);
      // TODO: Should be checked on server side. On Embedded metastore it throws MetaException,
      // on Remote metastore it throws TProtocolException
      Assert.fail("Expected an MetaException or TProtocolException to be thrown");
    } catch (MetaException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TProtocolException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testGetTableNullTableName() throws Exception {
    try {
      client.getTable(DEFAULT_DATABASE, null);
      // TODO: Should be checked on server side. On Embedded metastore it throws MetaException,
      // on Remote metastore it throws TProtocolException
      Assert.fail("Expected an MetaException or TProtocolException to be thrown");
    } catch (MetaException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TProtocolException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testGetAllTables() throws Exception {
    List<String> tables = client.getAllTables(DEFAULT_DATABASE);
    Assert.assertEquals("All tables size", 5, tables.size());
    for(Table table : testTables) {
      if (table.getDbName().equals(DEFAULT_DATABASE)) {
        Assert.assertTrue("Checking table names", tables.contains(table.getTableName()));
      }
    }

    // Drop one table, see what remains
    client.dropTable(testTables[1].getDbName(), testTables[1].getTableName());
    tables = client.getAllTables(DEFAULT_DATABASE);
    Assert.assertEquals("All tables size", 4, tables.size());
    for(Table table : testTables) {
      if (table.getDbName().equals(DEFAULT_DATABASE)
              && !table.getTableName().equals(testTables[1].getTableName())) {
        Assert.assertTrue("Checking table names", tables.contains(table.getTableName()));
      }
    }

    // No such database
    tables = client.getAllTables("no_such_database");
    Assert.assertEquals("All tables size", 0, tables.size());
  }

  @Test(expected = MetaException.class)
  public void testGetAllTablesInvalidData() throws Exception {
    client.getAllTables(null);
  }

  @Test
  public void testGetAllTablesCaseInsensitive() throws Exception {
    // Check case insensitive search
    List<String> tables = client.getAllTables("dEFauLt");
    Assert.assertEquals("Found tables size", 5, tables.size());
  }

  @Test
  public void testGetTables() throws Exception {
    // Find tables which name contains _to_find_ in the default database
    List<String> tables = client.getTables(DEFAULT_DATABASE, "*_to_find_*");
    Assert.assertEquals("All tables size", 2, tables.size());
    Assert.assertTrue("Comparing tablenames", tables.contains(testTables[2].getTableName()));
    Assert.assertTrue("Comparing tablenames", tables.contains(testTables[3].getTableName()));

    // Find tables which name contains _to_find_ or _hidden_ in the default database
    tables = client.getTables(DEFAULT_DATABASE, "*_to_find_*|*_hidden_*");
    Assert.assertEquals("All tables size", 3, tables.size());
    Assert.assertTrue("Comparing tablenames", tables.contains(testTables[2].getTableName()));
    Assert.assertTrue("Comparing tablenames", tables.contains(testTables[3].getTableName()));
    Assert.assertTrue("Comparing tablenames", tables.contains(testTables[4].getTableName()));

    // Find table which name contains _to_find_ in the dummy database
    tables = client.getTables(OTHER_DATABASE, "*_to_find_*");
    Assert.assertEquals("Found functions size", 1, tables.size());
    Assert.assertTrue("Comparing tablenames", tables.contains(testTables[6].getTableName()));

    // Look for tables but do not find any
    tables = client.getTables(DEFAULT_DATABASE, "*_not_such_function_*");
    Assert.assertEquals("No such table size", 0, tables.size());

    // Look for tables without pattern
    tables = client.getTables(DEFAULT_DATABASE, null);
    Assert.assertEquals("No such functions size", 5, tables.size());

    // Look for tables with empty pattern
    tables = client.getTables(DEFAULT_DATABASE, "");
    Assert.assertEquals("No such functions size", 0, tables.size());

    // No such database
    tables = client.getTables("no_such_database", OTHER_DATABASE);
    Assert.assertEquals("No such table size", 0, tables.size());
  }

  @Test
  public void testGetTablesCaseInsensitive() throws Exception {
    // Check case insensitive search
    List<String> tables = client.getTables(DEFAULT_DATABASE, "*_tO_FiND*");
    Assert.assertEquals("Found tables size", 2, tables.size());
    Assert.assertTrue("Comparing tablenames", tables.contains(testTables[2].getTableName()));
    Assert.assertTrue("Comparing tablenames", tables.contains(testTables[3].getTableName()));
  }

  @Test(expected = MetaException.class)
  public void testGetTablesNullDatabase() throws Exception {
    client.getTables(null, "*_tO_FiND*");
  }

  @Test
  public void testTableExists() throws Exception {
    // Using the second table, since a table called "test_table" exists in both databases
    Table table = testTables[1];

    Assert.assertTrue("Table exists", client.tableExists(table.getDbName(), table.getTableName()));
    Assert.assertFalse("Table not exists", client.tableExists(table.getDbName(),
        "non_existing_table"));

    // No such database
    Assert.assertFalse("Table not exists", client.tableExists("no_such_database",
        table.getTableName()));

    // No such table in the given database
    Assert.assertFalse("Table not exists", client.tableExists(OTHER_DATABASE,
        table.getTableName()));
  }

  @Test
  public void testTableExistsCaseInsensitive() throws Exception {
    Table table = testTables[0];

    // Test in upper case
    Assert.assertTrue("Table exists", client.tableExists(table.getDbName().toUpperCase(),
        table.getTableName().toUpperCase()));

    // Test in mixed case
    Assert.assertTrue("Table exists", client.tableExists("DeFaUlt", "tEsT_TabLE"));
  }

  @Test
  public void testTableExistsNullDatabase() throws Exception {
    try {
      client.tableExists(null, OTHER_DATABASE);
      // TODO: Should be checked on server side. On Embedded metastore it throws MetaException,
      // on Remote metastore it throws TProtocolException
      Assert.fail("Expected an MetaException or TProtocolException to be thrown");
    } catch (MetaException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TProtocolException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testTableExistsNullTableName() throws Exception {
    try {
      client.tableExists(DEFAULT_DATABASE, null);
      // TODO: Should be checked on server side. On Embedded metastore it throws MetaException,
      // on Remote metastore it throws TProtocolException
      Assert.fail("Expected an MetaException or TProtocolException to be thrown");
    } catch (MetaException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TProtocolException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testGetTableObjectsByName() throws Exception {
    List<String> tableNames = new ArrayList<String>();
    tableNames.add(testTables[0].getTableName());
    tableNames.add(testTables[1].getTableName());
    List<Table> tables = client.getTableObjectsByName(DEFAULT_DATABASE, tableNames);
    Assert.assertEquals("Found tables", 2, tables.size());
    for(Table table : tables) {
      if (table.getTableName().equals(testTables[0].getTableName())) {
        Assert.assertEquals("Comparing tables", testTables[0], table);
      } else {
        Assert.assertEquals("Comparing tables", testTables[1], table);
      }
    }

    // Test with empty array
    tables = client.getTableObjectsByName(DEFAULT_DATABASE, new ArrayList<String>());
    Assert.assertEquals("Found tables", 0, tables.size());

    // Test with table name which does not exists
    tableNames = new ArrayList<String>();
    tableNames.add("no_such_table");
    client.getTableObjectsByName(testTables[0].getDbName(), tableNames);
    Assert.assertEquals("Found tables", 0, tables.size());

    // Test with table name which does not exists in the given database
    tableNames = new ArrayList<String>();
    tableNames.add(testTables[0].getTableName());
    client.getTableObjectsByName(OTHER_DATABASE, tableNames);
    Assert.assertEquals("Found tables", 0, tables.size());

  }

  @Test
  public void testGetTableObjectsByNameCaseInsensitive() throws Exception {
    Table table = testTables[0];

    // Test in upper case
    List<String> tableNames = new ArrayList<String>();
    tableNames.add(testTables[0].getTableName().toUpperCase());
    List<Table> tables = client.getTableObjectsByName(table.getDbName().toUpperCase(), tableNames);
    Assert.assertEquals("Found tables", 1, tables.size());
    Assert.assertEquals("Comparing tables", table, tables.get(0));

    // Test in mixed case
    tableNames = new ArrayList<String>();
    tableNames.add("tEsT_TabLE");
    tables = client.getTableObjectsByName("DeFaUlt", tableNames);
    Assert.assertEquals("Found tables", 1, tables.size());
    Assert.assertEquals("Comparing tables", table, tables.get(0));
  }

  @Test(expected = UnknownDBException.class)
  public void testGetTableObjectsByNameNoSuchDatabase() throws Exception {
    List<String> tableNames = new ArrayList<String>();
    tableNames.add(testTables[0].getTableName());

    client.getTableObjectsByName("no_such_database", tableNames);
  }

  @Test
  public void testGetTableObjectsByNameNullDatabase() throws Exception {
    try {
      List<String> tableNames = new ArrayList<String>();
      tableNames.add(OTHER_DATABASE);

      client.getTableObjectsByName(null, tableNames);
      // TODO: Should be checked on server side. On Embedded metastore it throws MetaException,
      // on Remote metastore it throws TProtocolException
      Assert.fail("Expected an UnknownDBException or TProtocolException to be thrown");
    } catch (UnknownDBException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TProtocolException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testGetTableObjectsByNameNullTableNameList() throws Exception {
    try {
      client.getTableObjectsByName(DEFAULT_DATABASE, null);
      // TODO: Should be checked on server side. On Embedded metastore it throws MetaException,
      // on Remote metastore it throws TTransportException
      Assert.fail("Expected an InvalidOperationException to be thrown");
    } catch (InvalidOperationException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }
}
