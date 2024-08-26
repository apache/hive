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
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.GetTableProjectionsSpecBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

/**
 * Test class for IMetaStoreClient API. Testing the Table related functions for metadata
 * querying like getting one, or multiple tables, and table name lists.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestTablesGetExists extends MetaStoreClientTest {
  private static final String DEFAULT_DATABASE = "default";
  private static final String OTHER_DATABASE = "dummy";
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;
  private Table[] testTables = new Table[7];

  public TestTablesGetExists(String name, AbstractMetaStoreService metaStore) {
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
            .setTableName("test_table")
            .addCol("test_col", "int")
            .create(client, metaStore.getConf());

    testTables[1] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("test_view")
            .addCol("test_col", "int")
            .setType(TableType.VIRTUAL_VIEW.name())
            .create(client, metaStore.getConf());

    testTables[2] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("test_table_to_find_1")
            .addCol("test_col", "int")
            .create(client, metaStore.getConf());

    testTables[3] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("test_table_to_find_2")
            .addCol("test_col", "int")
            .setType(TableType.VIRTUAL_VIEW.name())
            .create(client, metaStore.getConf());

    testTables[4] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("test_table_hidden_1")
            .addCol("test_col", "int")
            .create(client, metaStore.getConf());

    new DatabaseBuilder().setName(OTHER_DATABASE).create(client, metaStore.getConf());

    testTables[5] =
        new TableBuilder()
            .setDbName(OTHER_DATABASE)
            .setTableName("test_table")
            .addCol("test_col", "int")
            .create(client, metaStore.getConf());

    testTables[6] =
        new TableBuilder()
            .setDbName(OTHER_DATABASE)
            .setTableName("test_table_to_find_3")
            .addCol("test_col", "int")
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
  public void testGetTableCaseInsensitive() throws Exception {
    Table table = testTables[0];

    // Test in upper case
    Table resultUpper = client.getTable(table.getCatName().toUpperCase(),
        table.getDbName().toUpperCase(), table.getTableName().toUpperCase());
    Assert.assertEquals("Comparing tables", table, resultUpper);

    // Test in mixed case
    Table resultMix = client.getTable("hIvE", "DeFaUlt", "tEsT_TabLE");
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
    client.dropTable(testTables[1].getCatName(), testTables[1].getDbName(), testTables[1] .getTableName());
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

    // Find tables by using the wildcard sign "*"
    tables = client.getTables(DEFAULT_DATABASE, "*");
    Assert.assertEquals("All tables size", 5, tables.size());
    Assert.assertTrue("Comparing tablenames", tables.contains(testTables[0].getTableName()));
    Assert.assertTrue("Comparing tablenames", tables.contains(testTables[1].getTableName()));
    Assert.assertTrue("Comparing tablenames", tables.contains(testTables[2].getTableName()));
    Assert.assertTrue("Comparing tablenames", tables.contains(testTables[3].getTableName()));
    Assert.assertTrue("Comparing tablenames", tables.contains(testTables[4].getTableName()));

    tables = client.getTables(OTHER_DATABASE, "*");
    Assert.assertEquals("All tables size", 2, tables.size());
    Assert.assertTrue("Comparing tablenames", tables.contains(testTables[5].getTableName()));
    Assert.assertTrue("Comparing tablenames", tables.contains(testTables[6].getTableName()));

    tables = client.getTables("*", "*");
    Assert.assertEquals("All tables size", 7, tables.size());
    tables = client.getTables("d*", "*");
    Assert.assertEquals("All tables size", 7, tables.size());
    tables = client.getTables("def*", "*");
    Assert.assertEquals("All tables size", 5, tables.size());

    // Look for tables but do not find any
    tables = client.getTables(DEFAULT_DATABASE, "*_not_such_function_*");
    Assert.assertEquals("No such table size", 0, tables.size());

    // Look for tables without pattern
    tables = client.getTables(DEFAULT_DATABASE, (String)null);
    Assert.assertEquals("No such functions size", 5, tables.size());

    // Look for tables with empty pattern
    tables = client.getTables(DEFAULT_DATABASE, "");
    Assert.assertEquals("No such functions size", 0, tables.size());

    // No such database
    try {
      tables = client.getTables("no_such_database", OTHER_DATABASE);
    }catch (MetaException exception) {
      // Ignoring Expected exception
    }
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

    Assert.assertTrue("Table exists", client.tableExists(table.getCatName(), table.getDbName(),
        table.getTableName()));
    Assert.assertFalse("Table not exists", client.tableExists(table.getCatName(), table.getDbName(),
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
    Assert.assertTrue("Table exists", client.tableExists(table.getCatName().toUpperCase(),
        table.getDbName().toUpperCase(), table.getTableName().toUpperCase()));

    // Test in mixed case
    Assert.assertTrue("Table exists", client.tableExists("hIVe", "DeFaUlt", "tEsT_TabLE"));
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
    List<String> tableNames = new ArrayList<>();
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
    tables = client.getTableObjectsByName(DEFAULT_DATABASE, new ArrayList<>());
    Assert.assertEquals("Found tables", 0, tables.size());

    // Test with table name which does not exists
    tableNames = new ArrayList<>();
    tableNames.add("no_such_table");
    client.getTableObjectsByName(testTables[0].getCatName(), testTables[0].getDbName(), tableNames);
    Assert.assertEquals("Found tables", 0, tables.size());

    // Test with table name which does not exists in the given database
    tableNames = new ArrayList<>();
    tableNames.add(testTables[0].getTableName());
    client.getTableObjectsByName(OTHER_DATABASE, tableNames);
    Assert.assertEquals("Found tables", 0, tables.size());

  }

  @Test
  public void testGetTableObjectsWithProjectionOfSingleField() throws Exception {
    List<String> tableNames = new ArrayList<>();
    tableNames.add(testTables[0].getTableName());
    tableNames.add(testTables[1].getTableName());

    GetProjectionsSpec projectSpec = (new GetTableProjectionsSpecBuilder()).includeSdLocation().build();

    List<Table> tables = client.getTables(null, DEFAULT_DATABASE, tableNames, projectSpec);
    Assert.assertEquals("Found tables", 2, tables.size());

    for(Table table : tables) {
      Assert.assertFalse(table.isSetDbName());
      Assert.assertFalse(table.isSetCatName());
      Assert.assertFalse(table.isSetTableName());
      Assert.assertTrue(table.isSetSd());
    }
  }

  @Test
  public void testGetTableObjectsWithNullProjectionSpec() throws Exception {
    List<String> tableNames = new ArrayList<>();
    tableNames.add(testTables[0].getTableName());
    tableNames.add(testTables[1].getTableName());

    List<Table> tables = client.getTables(null, DEFAULT_DATABASE, tableNames, null);

    Assert.assertEquals("Found tables", 2, tables.size());
  }

  @Test
  public void testGetTableObjectsWithIncludePattern() throws Exception {
    List<String> tableNames = new ArrayList<>();
    tableNames.add(testTables[0].getTableName());
    tableNames.add(testTables[1].getTableName());

    GetProjectionsSpec projectSpec = (new GetTableProjectionsSpecBuilder()).setExcludeColumnPattern("foo").build();

    Assert.assertThrows(Exception.class, ()->client.getTables(null, DEFAULT_DATABASE, tableNames, projectSpec));
  }

  @Test
  public void testGetTableObjectsWithNonExistentColumn() throws Exception {
    List<String> tableNames = new ArrayList<>();
    tableNames.add(testTables[0].getTableName());
    tableNames.add(testTables[1].getTableName());

    GetProjectionsSpec projectSpec = (new GetTableProjectionsSpecBuilder()).setColumnList(Arrays.asList("Invalid1"))
            .build();

    Assert.assertThrows(Exception.class, ()->client.getTables(null, DEFAULT_DATABASE, tableNames, projectSpec));
  }


  @Test
  public void testGetTableObjectsWithNonExistentColumns() throws Exception {
    List<String> tableNames = new ArrayList<>();
    tableNames.add(testTables[0].getTableName());
    tableNames.add(testTables[1].getTableName());

    GetProjectionsSpec projectSpec = (new GetTableProjectionsSpecBuilder())
            .setColumnList(Arrays.asList("Invalid1", "Invalid2")).build();

    Assert.assertThrows(Exception.class, ()->client.getTables(null, DEFAULT_DATABASE, tableNames, projectSpec));
  }

  @Test
  public void testGetTableObjectsWithEmptyProjection() throws Exception {
    List<String> tableNames = new ArrayList<>();
    tableNames.add(testTables[0].getTableName());
    tableNames.add(testTables[1].getTableName());

    GetProjectionsSpec projectSpec = (new GetTableProjectionsSpecBuilder()).build();

    List<Table> tables = client.getTables(null, DEFAULT_DATABASE, tableNames, projectSpec);

    Assert.assertEquals("Found tables", 2, tables.size());
  }

  @Test
  public void testGetTableObjectsWithProjectionOfMultipleField_1() throws Exception {
    List<String> tableNames = new ArrayList<>();
    tableNames.add(testTables[0].getTableName());
    tableNames.add(testTables[1].getTableName());

    GetProjectionsSpec projectSpec = (new GetTableProjectionsSpecBuilder()).includeDatabase().includeTableName()
            .includeCreateTime().includeLastAccessTime().build();

    List<Table> tables = client.getTables(null, DEFAULT_DATABASE, tableNames, projectSpec);

    Assert.assertEquals("Found tables", 2, tables.size());

    for(Table table : tables) {
      Assert.assertTrue(table.isSetDbName());
      Assert.assertTrue(table.isSetTableName());
      Assert.assertTrue(table.isSetCreateTime());
      Assert.assertFalse(table.isSetSd());
      Assert.assertFalse(table.isSetOwner());
    }
  }

  @Test
  public void testGetTableObjectsWithProjectionOfMultipleField_2() throws Exception {
    List<String> tableNames = new ArrayList<>();
    tableNames.add(testTables[0].getTableName());
    tableNames.add(testTables[1].getTableName());

    GetProjectionsSpec projectSpec = (new GetTableProjectionsSpecBuilder()).includeOwner().includeOwnerType().
            includeSdLocation().includeTableType().build();

    List<Table> tables = client.getTables(null, DEFAULT_DATABASE, tableNames, projectSpec);

    Assert.assertEquals("Found tables", 2, tables.size());

    for(Table table : tables) {
      Assert.assertFalse(table.isSetDbName());
      Assert.assertFalse(table.isSetTableName());
      Assert.assertTrue(table.isSetCreateTime());
      Assert.assertTrue(table.isSetSd());
      Assert.assertTrue(table.isSetOwnerType());
      Assert.assertTrue(table.isSetOwner());
      StorageDescriptor sd = table.getSd();
      if (TableType.VIRTUAL_VIEW.toString().equals(table.getTableType())) {
        Assert.assertFalse(sd.isSetLocation());
      } else {
        Assert.assertTrue(sd.isSetLocation());
      }
    }
  }

  @Test
  public void testGetTableObjectsWithProjectionOfSerDeInfoSingleValuedFields() throws Exception {
    List<String> tableNames = new ArrayList<>();
    tableNames.add(testTables[0].getTableName());
    tableNames.add(testTables[1].getTableName());

    GetProjectionsSpec projectSpec = (new GetTableProjectionsSpecBuilder()).includeSdSerDeInfoSerializationLib()
            .build();

    List<Table> tables = client.getTables(null, DEFAULT_DATABASE, tableNames, projectSpec);

    Assert.assertEquals("Found tables", 2, tables.size());

    for(Table table : tables) {
      Assert.assertFalse(table.isSetDbName());
      Assert.assertTrue(table.isSetSd());
      StorageDescriptor sd = table.getSd();
      Assert.assertFalse(sd.isSetCols());
      Assert.assertTrue(sd.isSetSerdeInfo());
      SerDeInfo serDeInfo = sd.getSerdeInfo();
      Assert.assertTrue(serDeInfo.isSetSerializationLib());
    }
  }

  @Test
  public void testGetTableObjectsWithProjectionOfMultiValuedFields() throws Exception {
    List<String> tableNames = new ArrayList<>();
    tableNames.add(testTables[0].getTableName());
    tableNames.add(testTables[1].getTableName());

    GetProjectionsSpec projectSpec = (new GetTableProjectionsSpecBuilder()).includeSdLocation().includeSdSerDeInfoName()
            .includeSdSerDeInfoSerializationLib().includeSdSerDeInfoParameters().build();

    List<Table> tables = client.getTables(null, DEFAULT_DATABASE, tableNames, projectSpec);

    Assert.assertEquals("Found tables", 2, tables.size());

    for(Table table : tables) {
      Assert.assertTrue(table.isSetDbName());
      Assert.assertTrue(table.isSetCatName());
      Assert.assertTrue(table.isSetTableName());
      Assert.assertTrue(table.isSetLastAccessTime());
      Assert.assertTrue(table.isSetSd());
      StorageDescriptor sd = table.getSd();
      Assert.assertTrue(sd.isSetCols());
      Assert.assertTrue(sd.isSetSerdeInfo());
      Assert.assertTrue(sd.isSetBucketCols());
      Assert.assertTrue(sd.isSetCompressed());
      Assert.assertTrue(sd.isSetInputFormat());
    }
  }

  @Test
  public void testGetTableProjectionSpecification() throws Exception {
    List<String> tableNames = new ArrayList<>();
    tableNames.add(testTables[0].getTableName());
    tableNames.add(testTables[1].getTableName());

    GetProjectionsSpec projectSpec = (new GetTableProjectionsSpecBuilder())
            .includeTableName()
            .includeDatabase()
            .includeSdCdColsName()
            .includeSdCdColsType()
            .includeSdCdColsComment()
            .includeSdLocation()
            .includeSdInputFormat()
            .includeSdOutputFormat()
            .includeSdIsCompressed()
            .includeSdNumBuckets()
            .includeSdSerDeInfoName()
            .includeSdSerDeInfoSerializationLib()
            .includeSdSerDeInfoParameters()
            .includeSdSerDeInfoDescription()
            .includeSdSerDeInfoSerializerClass()
            .includeSdSerDeInfoDeserializerClass()
            .includeSdSerDeInfoSerdeType()
            .includeSdBucketCols()
            .includeSdSortColsCol()
            .includeSdSortColsOrder()
            .includeSdparameters()
            .includeSdSkewedColNames()
            .includeSdSkewedColValues()
            .includeSdSkewedColValueLocationMaps()
            .includeSdIsStoredAsSubDirectories()
            .includeOwner()
            .includeOwnerType()
            .includeCreateTime()
            .includeLastAccessTime()
            .includeRetention()
            .includePartitionKeysName()
            .includePartitionKeysType()
            .includePartitionKeysComment()
            .includeParameters()
            .includeViewOriginalText()
            .includeRewriteEnabled()
            .includeTableType()
            .build();

    List<Table> tables = client.getTables(null, DEFAULT_DATABASE, tableNames, projectSpec);

    Assert.assertEquals("Found tables", 2, tables.size());

    for(Table table : tables) {
      Assert.assertTrue(table.isSetDbName());
      Assert.assertTrue(table.isSetCatName());
      Assert.assertTrue(table.isSetTableName());
      Assert.assertTrue(table.isSetLastAccessTime());
      Assert.assertTrue(table.isSetSd());
      StorageDescriptor sd = table.getSd();
      Assert.assertTrue(sd.isSetCols());
      Assert.assertTrue(sd.isSetSerdeInfo());
      Assert.assertTrue(sd.isSetBucketCols());
      Assert.assertTrue(sd.isSetCompressed());
      Assert.assertTrue(sd.isSetInputFormat());
      Assert.assertTrue(sd.isSetSerdeInfo());
      SerDeInfo serDeInfo = sd.getSerdeInfo();
      Assert.assertTrue(serDeInfo.isSetSerializationLib());
    }
  }

  @Test
  public void testGetTableObjectsByNameCaseInsensitive() throws Exception {
    Table table = testTables[0];

    // Test in upper case
    List<String> tableNames = new ArrayList<>();
    tableNames.add(testTables[0].getTableName().toUpperCase());
    List<Table> tables = client.getTableObjectsByName(table.getCatName().toUpperCase(),
        table.getDbName().toUpperCase(), tableNames);
    Assert.assertEquals("Found tables", 1, tables.size());
    Assert.assertEquals("Comparing tables", table, tables.get(0));

    // Test in mixed case
    tableNames = new ArrayList<>();
    tableNames.add("tEsT_TabLE");
    tables = client.getTableObjectsByName("HiVe", "DeFaUlt", tableNames);
    Assert.assertEquals("Found tables", 1, tables.size());
    Assert.assertEquals("Comparing tables", table, tables.get(0));
  }

  @Test(expected = UnknownDBException.class)
  public void testGetTableObjectsByNameNoSuchDatabase() throws Exception {
    List<String> tableNames = new ArrayList<>();
    tableNames.add(testTables[0].getTableName());

    client.getTableObjectsByName("no_such_database", tableNames);
  }

  @Test
  public void testGetTableObjectsByNameNullDatabase() throws Exception {
    try {
      List<String> tableNames = new ArrayList<>();
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

  // Tests for getTable in other catalogs are covered in TestTablesCreateDropAlterTruncate.
  @Test
  public void otherCatalog() throws TException {
    String catName = "get_exists_tables_in_other_catalogs";
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
      new TableBuilder()
          .inDb(db)
          .setTableName(tableNames[i])
          .addCol("col1_" + i, ColumnType.STRING_TYPE_NAME)
          .addCol("col2_" + i, ColumnType.INT_TYPE_NAME)
          .create(client, metaStore.getConf());
    }

    Set<String> tables = new HashSet<>(client.getTables(catName, dbName, "*e_in_other_*"));
    Assert.assertEquals(4, tables.size());
    for (String tableName : tableNames) Assert.assertTrue(tables.contains(tableName));

    List<String> fetchedNames = client.getTables(catName, dbName, "*_3");
    Assert.assertEquals(1, fetchedNames.size());
    Assert.assertEquals(tableNames[3], fetchedNames.get(0));

    Assert.assertTrue("Table exists", client.tableExists(catName, dbName, tableNames[0]));
    Assert.assertFalse("Table not exists", client.tableExists(catName, dbName, "non_existing_table"));
  }

  @Test(expected = UnknownDBException.class)
  public void getTablesBogusCatalog() throws TException {
    client.getTables("nosuch", DEFAULT_DATABASE_NAME, "*_to_find_*");
  }

  @Test
  public void tableExistsBogusCatalog() throws TException {
    Assert.assertFalse(client.tableExists("nosuch", testTables[0].getDbName(),
        testTables[0].getTableName()));
  }
}
