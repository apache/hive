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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test class for IMetaStoreClient API. Testing the Table related functions for metadata
 * manipulation, like creating, dropping and altering tables.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestTablesCreateDropAlterTruncate {
  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should remove our own copy
  private static Set<AbstractMetaStoreService> metaStoreServices = null;
  private static final String DEFAULT_DATABASE = "default";
  private static final String OTHER_DATABASE = "dummy";
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;
  private Table[] testTables = new Table[6];
  private Table partitionedTable = null;
  private Table externalTable = null;

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getMetaStoreToTest() throws Exception {
    List<Object[]> result = MetaStoreFactoryForTests.getMetaStores();
    metaStoreServices = result.stream()
        .map(test -> (AbstractMetaStoreService)test[1])
        .collect(Collectors.toSet());
    return result;
  }

  public TestTablesCreateDropAlterTruncate(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
    Map<MetastoreConf.ConfVars, String> msConf = new HashMap<MetastoreConf.ConfVars, String>();
    // Enable trash, so it can be tested
    Map<String, String> extraConf = new HashMap<String, String>();
    extraConf.put("fs.trash.checkpoint.interval", "30");  // FS_TRASH_CHECKPOINT_INTERVAL_KEY
    extraConf.put("fs.trash.interval", "30");             // FS_TRASH_INTERVAL_KEY (hadoop-2)

    this.metaStore.start(msConf, extraConf);
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
            .setType("VIRTUAL_VIEW")
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
            .setTableName("test_partitioned_table")
            .addCol("test_col1", "int")
            .addCol("test_col2", "int")
            .addPartCol("test_part_col", "int")
            .build();

    testTables[4] =
        new TableBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setTableName("external_table_for_test")
            .addCol("test_col", "int")
            .setLocation(metaStore.getWarehouseRoot() + "/external/table_dir")
            .addTableParam("EXTERNAL", "TRUE")
            .setType("EXTERNAL_TABLE")
            .build();


    client.createDatabase(new DatabaseBuilder().setName(OTHER_DATABASE).build());

    testTables[5] =
        new TableBuilder()
            .setDbName(OTHER_DATABASE)
            .setTableName("test_table")
            .addCol("test_col", "int")
            .build();

    // Create the tables in the MetaStore
    for(int i=0; i < testTables.length; i++) {
      client.createTable(testTables[i]);
    }

    // Create partitions for the partitioned table
    for(int i=0; i < 3; i++) {
      Partition partition =
          new PartitionBuilder()
              .fromTable(testTables[3])
              .addValue("a" + i)
              .build();
      client.add_partition(partition);
    }
    // Add data files to the partitioned table
    List<Partition> partitions =
        client.listPartitions(testTables[3].getDbName(), testTables[3].getTableName(), (short)-1);
    for(Partition partition : partitions) {
      Path dataFile = new Path(partition.getSd().getLocation().toString() + "/dataFile");
      metaStore.createFile(dataFile, "100");
    }

    // Reload tables from the MetaStore, and create data files
    for(int i=0; i < testTables.length; i++) {
      testTables[i] = client.getTable(testTables[i].getDbName(), testTables[i].getTableName());
      if (testTables[i].getPartitionKeys().isEmpty()) {
        if (testTables[i].getSd().getLocation() != null) {
          Path dataFile = new Path(testTables[i].getSd().getLocation().toString() + "/dataFile");
          metaStore.createFile(dataFile, "100");
        }
      }
    }
    partitionedTable = testTables[3];
    externalTable = testTables[4];
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

  /**
   * This test creates and queries a table and then drops it. Good for testing the happy path
   * @throws Exception
   */
  @Test
  public void testCreateGetDeleteTable() throws Exception {
    // Try to create a table with all of the parameters set
    Table table = getTableWithAllParametersSet();
    client.createTable(table);
    Table createdTable = client.getTable(table.getDbName(), table.getTableName());
    // The createTime will be set on the server side, so the comparison should skip it
    table.setCreateTime(createdTable.getCreateTime());
    // The extra parameters will be added on server side, so check that the required ones are
    // present
    for(String key: table.getParameters().keySet()) {
      Assert.assertEquals("parameters are the same",
          table.getParameters().get(key), createdTable.getParameters().get(key));
    }
    // Reset the parameters, so we can compare
    table.setParameters(createdTable.getParameters());
    table.setCreationMetadata(createdTable.getCreationMetadata());
    Assert.assertEquals("create/get table data", table, createdTable);

    // Check that the directory is created
    Assert.assertTrue("The directory should not be created",
        metaStore.isPathExists(new Path(createdTable.getSd().getLocation())));

    client.dropTable(table.getDbName(), table.getTableName(), true, false);
    try {
      client.getTable(table.getDbName(), table.getTableName());
      Assert.fail("Expected a NoSuchObjectException to be thrown");
    } catch (NoSuchObjectException exception) {
      // Expected exception
    }
  }

  @Test
  public void testCreateTableDefaultValues() throws Exception {
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = new ArrayList<FieldSchema>();

    table.setDbName(DEFAULT_DATABASE);
    table.setTableName("test_table_2");
    cols.add(new FieldSchema("column_name", "int", null));
    sd.setCols(cols);
    sd.setSerdeInfo(new SerDeInfo());
    table.setSd(sd);

    client.createTable(table);
    Table createdTable = client.getTable(table.getDbName(), table.getTableName());

    Assert.assertNull("Comparing OwnerName", createdTable.getOwner());
    Assert.assertNotEquals("Comparing CreateTime", 0, createdTable.getCreateTime());
    Assert.assertEquals("Comparing LastAccessTime", 0, createdTable.getLastAccessTime());
    Assert.assertEquals("Comparing Retention", 0, createdTable.getRetention());
    Assert.assertEquals("Comparing PartitionKeys", 0, createdTable.getPartitionKeys().size());
    // TODO: If this test method is the first to run, then the parameters does not contain totalSize
    // and numFiles, if this runs after other tests (setUp/dropDatabase is successful), then the
    // totalSize and the numFiles are set.
    Assert.assertEquals("Comparing Parameters length", 1, createdTable.getParameters().size());
    Assert.assertNotEquals("Comparing Parameters(transient_lastDdlTime)", "0",
        createdTable.getParameters().get("transient_lastDdlTime"));
//    Assert.assertEquals("Comparing Parameters(totalSize)", "0",
//        createdTable.getParameters().get("totalSize"));
//    Assert.assertEquals("Comparing Parameters(numFiles)", "0",
//        createdTable.getParameters().get("numFiles"));
    Assert.assertNull("Comparing ViewOriginalText", createdTable.getViewOriginalText());
    Assert.assertNull("Comparing ViewExpandedText", createdTable.getViewExpandedText());
    Assert.assertEquals("Comparing TableType", "MANAGED_TABLE", createdTable.getTableType());
    Assert.assertTrue("Creation metadata should be empty", createdTable.getCreationMetadata() == null);

    // Storage Descriptor data
    StorageDescriptor createdSd = createdTable.getSd();
    Assert.assertEquals("Storage descriptor cols", 1, createdSd.getCols().size());
    Assert.assertNull("Storage descriptor cols[0].comment",
        createdSd.getCols().get(0).getComment());
    Assert.assertEquals("Storage descriptor location", metaStore.getWarehouseRoot()
        + "/" + table.getTableName(), createdSd.getLocation());
    Assert.assertTrue("Table path should be created",
        metaStore.isPathExists(new Path(createdSd.getLocation())));
    // TODO: Embedded MetaStore changes the table object when client.createTable is called
    //Assert.assertNull("Original table storage descriptor location should be null",
    //    table.getSd().getLocation());

    Assert.assertNull("Storage descriptor input format", createdSd.getInputFormat());
    Assert.assertNull("Storage descriptor output format", createdSd.getOutputFormat());
    Assert.assertFalse("Storage descriptor compressed", createdSd.isCompressed());
    Assert.assertEquals("Storage descriptor num buckets", 0, createdSd.getNumBuckets());
    Assert.assertEquals("Storage descriptor bucket cols", 0, createdSd.getBucketCols().size());
    Assert.assertEquals("Storage descriptor sort cols", 0, createdSd.getSortCols().size());
    Assert.assertEquals("Storage descriptor parameters", 0, createdSd.getParameters().size());
    Assert.assertFalse("Storage descriptor stored as subdir", createdSd.isStoredAsSubDirectories());

    // Serde info
    SerDeInfo serDeInfo = createdSd.getSerdeInfo();
    Assert.assertNull("SerDeInfo name", serDeInfo.getName());
    Assert.assertNull("SerDeInfo serialization lib", serDeInfo.getSerializationLib());
    Assert.assertEquals("SerDeInfo parameters", 0, serDeInfo.getParameters().size());

    // Skewed info
    SkewedInfo skewedInfo = createdSd.getSkewedInfo();
    Assert.assertEquals("Skewed info col names", 0, skewedInfo.getSkewedColNames().size());
    Assert.assertEquals("Skewed info col values", 0, skewedInfo.getSkewedColValues().size());
    Assert.assertEquals("Skewed info col value maps", 0,
        skewedInfo.getSkewedColValueLocationMaps().size());
  }

  @Test
  public void testCreateTableDefaultLocationInSpecificDatabase() throws Exception {
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = new ArrayList<FieldSchema>();

    table.setDbName(OTHER_DATABASE);
    table.setTableName("test_table_2");
    cols.add(new FieldSchema("column_name", "int", null));
    sd.setCols(cols);
    sd.setSerdeInfo(new SerDeInfo());
    table.setSd(sd);

    client.createTable(table);
    Table createdTable = client.getTable(table.getDbName(), table.getTableName());
    Assert.assertEquals("Storage descriptor location", metaStore.getWarehouseRoot()
        + "/" + table.getDbName() + ".db/" + table.getTableName(),
        createdTable.getSd().getLocation());
  }

  @Test
  public void testCreateTableDefaultValuesView() throws Exception {
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = new ArrayList<FieldSchema>();

    table.setDbName(DEFAULT_DATABASE);
    table.setTableName("test_table_2");
    table.setTableType("VIRTUAL_VIEW");
    cols.add(new FieldSchema("column_name", "int", null));
    sd.setCols(cols);
    sd.setSerdeInfo(new SerDeInfo());
    table.setSd(sd);

    client.createTable(table);
    Table createdTable = client.getTable(table.getDbName(), table.getTableName());

    // No location should be created for views
    StorageDescriptor createdSd = createdTable.getSd();
    Assert.assertNull("Storage descriptor location should be null",
        createdTable.getSd().getLocation());
  }

  @Test(expected = MetaException.class)
  public void testCreateTableNullDatabase() throws Exception {
    Table table = testTables[0];
    table.setDbName(null);

    client.createTable(table);
  }

  @Test(expected = MetaException.class)
  public void testCreateTableNullTableName() throws Exception {
    Table table = testTables[0];
    table.setTableName(null);

    client.createTable(table);
  }

  @Test(expected = InvalidObjectException.class)
  public void testCreateTableInvalidTableName() throws Exception {
    Table table = testTables[0];
    table.setTableName("test_table;");

    client.createTable(table);
  }

  @Test(expected = InvalidObjectException.class)
  public void testCreateTableEmptyName() throws Exception {
    Table table = testTables[0];
    table.setTableName("");

    client.createTable(table);
  }

  @Test(expected = MetaException.class)
  public void testCreateTableNullStorageDescriptor() throws Exception {
    Table table = testTables[0];
    table.setSd(null);

    client.createTable(table);
  }

  private Table getNewTable() throws MetaException {
    return new TableBuilder()
               .setDbName(DEFAULT_DATABASE)
               .setTableName("test_table_with_invalid_sd")
               .addCol("test_col", "int")
               .build();
  }

  @Test(expected = MetaException.class)
  public void testCreateTableInvalidStorageDescriptorNullColumns() throws Exception {
    Table table = getNewTable();
    table.getSd().setCols(null);
    client.createTable(table);
  }

  @Test(expected = MetaException.class)
  public void testCreateTableInvalidStorageDescriptorNullSerdeInfo() throws Exception {
    Table table = getNewTable();
    table.getSd().setSerdeInfo(null);

    client.createTable(table);
  }

  @Test(expected = MetaException.class)
  public void testCreateTableInvalidStorageDescriptorNullColumnType() throws Exception {
    Table table = getNewTable();
    table.getSd().getCols().get(0).setType(null);

    client.createTable(table);
  }

  @Test(expected = InvalidObjectException.class)
  public void testCreateTableInvalidStorageDescriptorInvalidColumnType() throws Exception {
    Table table = getNewTable();
    table.getSd().getCols().get(0).setType("xyz");

    client.createTable(table);
  }

  @Test(expected = InvalidObjectException.class)
  public void testCreateTableNoSuchDatabase() throws Exception {
    Table table = testTables[0];
    table.setDbName("no_such_database");

    client.createTable(table);
  }

  @Test(expected = AlreadyExistsException.class)
  public void testCreateTableAlreadyExists() throws Exception {
    Table table = testTables[0];

    client.createTable(table);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropTableNoSuchDatabase() throws Exception {
    Table table = testTables[2];

    client.dropTable("no_such_database", table.getTableName(), true, false);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropTableNoSuchTable() throws Exception {
    Table table = testTables[2];

    client.dropTable(table.getDbName(), "no_such_table", true, false);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropTableNoSuchTableInTheDatabase() throws Exception {
    Table table = testTables[2];

    client.dropTable(OTHER_DATABASE, table.getTableName(), true, false);
  }

  @Test
  public void testDropTableNullDatabase() throws Exception {
    // Missing database in the query
    try {
      client.dropTable(null, OTHER_DATABASE, true, false);
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
  public void testDropTableNullTableName() throws Exception {
    try {
      client.dropTable(DEFAULT_DATABASE, null, true, false);
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
  public void testDropTableCaseInsensitive() throws Exception {
    Table table = testTables[0];

    // Test in upper case
    client.dropTable(table.getDbName().toUpperCase(), table.getTableName().toUpperCase());
    try {
      client.getTable(table.getDbName(), table.getTableName());
      Assert.fail("Expected a NoSuchObjectException to be thrown");
    } catch (NoSuchObjectException exception) {
      // Expected exception
    }

    // Test in mixed case
    client.createTable(table);
    client.dropTable("DeFaUlt", "TeST_tAbLE");
    try {
      client.getTable(table.getDbName(), table.getTableName());
      Assert.fail("Expected a NoSuchObjectException to be thrown");
    } catch (NoSuchObjectException exception) {
      // Expected exception
    }
  }

  @Test
  public void testDropTableDeleteDir() throws Exception {
    Table table = testTables[0];

    client.dropTable(table.getDbName(), table.getTableName(), true, false);

    Assert.assertFalse("Table path should be removed",
        metaStore.isPathExists(new Path(table.getSd().getLocation())));

    client.createTable(table);
    client.dropTable(table.getDbName(), table.getTableName(), false, false);

    Assert.assertTrue("Table path should be kept",
        metaStore.isPathExists(new Path(table.getSd().getLocation())));

    // Drop table with partitions
    client.dropTable(partitionedTable.getDbName(), partitionedTable.getTableName(), true, false);

    Assert.assertFalse("Table path should be removed",
        metaStore.isPathExists(new Path(partitionedTable.getSd().getLocation())));
  }

  @Test
  public void testDropTableIgnoreUnknown() throws Exception {
    Table table = testTables[0];

    // Check what happens, when we ignore these errors
    client.dropTable("no_such_database", table.getTableName(), true, true);
    client.dropTable(table.getDbName(), "no_such_table", false, true);
    client.dropTable(OTHER_DATABASE, table.getTableName(), true, true);

    // TODO: Strangely the default parametrization is to ignore missing tables
    client.dropTable("no_such_database", table.getTableName());
    client.dropTable(table.getDbName(), "no_such_table");
    client.dropTable(OTHER_DATABASE, table.getTableName());
  }

  @Test
  public void testDropTableWithPurge() throws Exception {
    Table table = testTables[0];

    client.dropTable(table.getDbName(), table.getTableName(), true, true, true);

    Assert.assertFalse("Table path should be removed",
        metaStore.isPathExists(new Path(table.getSd().getLocation())));
    Assert.assertFalse("Table path should not be in trash",
        metaStore.isPathExistsInTrash(new Path(table.getSd().getLocation())));
  }

  @Test
  public void testDropTableWithoutPurge() throws Exception {
    Table table = testTables[0];

    client.dropTable(table.getDbName(), table.getTableName(), true, true, false);

    Assert.assertFalse("Table path should be removed",
        metaStore.isPathExists(new Path(table.getSd().getLocation())));
    Assert.assertTrue("Table path should be in trash",
        metaStore.isPathExistsInTrash(new Path(table.getSd().getLocation())));
  }

  @Test
  public void testDropTableExternalWithPurge() throws Exception {
    Table table = externalTable;

    client.dropTable(table.getDbName(), table.getTableName(), true, true, true);

    Assert.assertTrue("Table path should not be removed",
        metaStore.isPathExists(new Path(table.getSd().getLocation())));
    Assert.assertFalse("Table path should not be in trash",
        metaStore.isPathExistsInTrash(new Path(table.getSd().getLocation())));
  }

  @Test
  public void testDropTableExternalWithoutPurge() throws Exception {
    Table table = externalTable;

    client.dropTable(table.getDbName(), table.getTableName(), true, true, false);

    Assert.assertTrue("Table path should not be removed",
        metaStore.isPathExists(new Path(table.getSd().getLocation())));
    Assert.assertFalse("Table path should be in trash",
        metaStore.isPathExistsInTrash(new Path(table.getSd().getLocation())));
  }

  @Test
  public void testTruncateTableUnpartitioned() throws Exception {
    // Unpartitioned table
    Path dataFile = new Path(testTables[0].getSd().getLocation().toString() + "/dataFile");
    client.truncateTable(testTables[0].getDbName(), testTables[0].getTableName(), null);
    Assert.assertTrue("Location should exist",
        metaStore.isPathExists(new Path(testTables[0].getSd().getLocation())));
    Assert.assertFalse("DataFile should be removed", metaStore.isPathExists(dataFile));

  }

  @Test
  public void testTruncateTablePartitioned() throws Exception {
    // Partitioned table - delete specific partitions a0, a2
    List<String> partitionsToDelete = new ArrayList<String>();
    partitionsToDelete.add("test_part_col=a0");
    partitionsToDelete.add("test_part_col=a2");
    client.truncateTable(partitionedTable.getDbName(), partitionedTable.getTableName(),
        partitionsToDelete);
    Assert.assertTrue("Location should exist",
        metaStore.isPathExists(new Path(testTables[0].getSd().getLocation())));
    List<Partition> partitions =
        client.listPartitions(partitionedTable.getDbName(), partitionedTable.getTableName(),
            (short)-1);
    for(Partition partition : partitions) {
      Path dataFile = new Path(partition.getSd().getLocation().toString() + "/dataFile");
      if (partition.getValues().contains("a0") || partition.getValues().contains("a2")) {
        // a0, a2 should be empty
        Assert.assertFalse("DataFile should be removed", metaStore.isPathExists(dataFile));
      } else {
        // Others (a1) should be kept
        Assert.assertTrue("DataFile should not be removed", metaStore.isPathExists(dataFile));
      }
    }

  }

  @Test
  public void testTruncateTablePartitionedDeleteAll() throws Exception {
    // Partitioned table - delete all
    client.truncateTable(partitionedTable.getDbName(), partitionedTable.getTableName(), null);
    Assert.assertTrue("Location should exist",
        metaStore.isPathExists(new Path(testTables[0].getSd().getLocation())));
    List<Partition> partitions =
        client.listPartitions(partitionedTable.getDbName(), partitionedTable.getTableName(),
            (short)-1);
    for(Partition partition : partitions) {
      Path dataFile = new Path(partition.getSd().getLocation().toString() + "/dataFile");
      Assert.assertFalse("Every dataFile should be removed", metaStore.isPathExists(dataFile));
    }
  }

  @Test
  public void testAlterTable() throws Exception {
    Table originalTable = testTables[2];
    String originalTableName = originalTable.getTableName();
    String originalDatabase = originalTable.getDbName();

    Table newTable = getTableWithAllParametersSet();
    newTable.setTableName(originalTableName);
    newTable.setDbName(originalDatabase);
    // Partition keys can not be set, but getTableWithAllParametersSet is added one, so remove for
    // this test
    newTable.setPartitionKeys(originalTable.getPartitionKeys());
    client.alter_table(originalDatabase, originalTableName, newTable);
    Table alteredTable = client.getTable(originalDatabase, originalTableName);

    // The extra parameters will be added on server side, so check that the required ones are
    // present
    for(String key: newTable.getParameters().keySet()) {
      Assert.assertEquals("parameters are present", newTable.getParameters().get(key),
          alteredTable.getParameters().get(key));
    }
    // The parameters are checked manually, so do not check them
    newTable.setParameters(alteredTable.getParameters());

    // Some of the data is set on the server side, so reset those
    newTable.setCreateTime(alteredTable.getCreateTime());
    newTable.setCreationMetadata(alteredTable.getCreationMetadata());
    Assert.assertEquals("The table data should be the same", newTable, alteredTable);
  }

  @Test
  public void testAlterTableRename() throws Exception {
    Table originalTable = testTables[2];
    String originalTableName = originalTable.getTableName();
    String originalDatabase = originalTable.getDbName();

    Table newTable = originalTable.deepCopy();
    // Do not change the location, so it is tested that the location will be changed even if the
    // location is not set to null, just remain the same
    newTable.setTableName("new_table");
    client.alter_table(originalDatabase, originalTableName, newTable);
    List<String> tableNames = client.getTables(originalDatabase, originalTableName);
    Assert.assertEquals("Original table should be removed", 0, tableNames.size());
    Assert.assertFalse("Original table directory should be removed",
        metaStore.isPathExists(new Path(originalTable.getSd().getLocation())));
    Table alteredTable = client.getTable(newTable.getDbName(), newTable.getTableName());
    Assert.assertTrue("New table directory should exist",
        metaStore.isPathExists(new Path(alteredTable.getSd().getLocation())));
    Assert.assertEquals("New directory should be set", new Path(metaStore.getWarehouseRoot()
        + "/" + alteredTable.getTableName()), new Path(alteredTable.getSd().getLocation()));

    Path dataFile = new Path(alteredTable.getSd().getLocation().toString() + "/dataFile");
    Assert.assertTrue("New directory should contain data", metaStore.isPathExists(dataFile));

    // The following data should be changed
    newTable.getSd().setLocation(alteredTable.getSd().getLocation());
    Assert.assertEquals("The table data should be the same", newTable, alteredTable);
  }

  @Test
  public void testAlterTableChangingDatabase() throws Exception {
    Table originalTable = testTables[2];
    String originalTableName = originalTable.getTableName();
    String originalDatabase = originalTable.getDbName();

    Table newTable = originalTable.deepCopy();
    newTable.setDbName(OTHER_DATABASE);
    client.alter_table(originalDatabase, originalTableName, newTable);
    List<String> tableNames = client.getTables(originalDatabase, originalTableName);
    Assert.assertEquals("Original table should be removed", 0, tableNames.size());
    Assert.assertFalse("Original table directory should be removed",
        metaStore.isPathExists(new Path(originalTable.getSd().getLocation())));
    Table alteredTable = client.getTable(newTable.getDbName(), newTable.getTableName());
    Assert.assertTrue("New table directory should exist",
        metaStore.isPathExists(new Path(alteredTable.getSd().getLocation())));
    Assert.assertEquals("New directory should be set", new Path(metaStore.getWarehouseRoot()
        + "/" + alteredTable.getDbName() + ".db/" + alteredTable.getTableName()),
        new Path(alteredTable.getSd().getLocation()));
    Path dataFile = new Path(alteredTable.getSd().getLocation().toString() + "/dataFile");
    Assert.assertTrue("New directory should contain data", metaStore.isPathExists(dataFile));

    // The following data should be changed, other data should be the same
    newTable.getSd().setLocation(alteredTable.getSd().getLocation());
    Assert.assertEquals("The table data should be the same", newTable, alteredTable);
  }

  @Test
  public void testAlterTableExternalTable() throws Exception {
    Table originalTable = externalTable;
    String originalTableName = originalTable.getTableName();
    String originalDatabase = originalTable.getDbName();

    Table newTable = originalTable.deepCopy();
    newTable.setTableName("new_external_table_for_test");
    client.alter_table(originalDatabase, originalTableName, newTable);
    List<String> tableNames = client.getTables(originalDatabase, originalTableName);
    Assert.assertEquals("Original table should be removed", 0, tableNames.size());
    Assert.assertTrue("Original table directory should be kept",
        metaStore.isPathExists(new Path(originalTable.getSd().getLocation())));
    Table alteredTable = client.getTable(newTable.getDbName(), newTable.getTableName());
    Assert.assertEquals("New location should be the same", originalTable.getSd().getLocation(),
        alteredTable.getSd().getLocation());
    Path dataFile = new Path(alteredTable.getSd().getLocation().toString() + "/dataFile");
    Assert.assertTrue("The location should contain data", metaStore.isPathExists(dataFile));

    // The extra parameters will be added on server side, so check that the required ones are
    // present
    for(String key: newTable.getParameters().keySet()) {
      Assert.assertEquals("parameters are present", newTable.getParameters().get(key),
          alteredTable.getParameters().get(key));
    }
    // The parameters are checked manually, so do not check them
    newTable.setParameters(alteredTable.getParameters());
    Assert.assertEquals("The table data should be the same", newTable, alteredTable);
  }

  @Test
  public void testAlterTableExternalTableChangeLocation() throws Exception {
    Table originalTable = externalTable;

    // Change the location, and see the results
    Table newTable = originalTable.deepCopy();
    newTable.getSd().setLocation(newTable.getSd().getLocation() + "_modified");
    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
    Table alteredTable = client.getTable(newTable.getDbName(), newTable.getTableName());
    Assert.assertTrue("Original table directory should be kept",
        metaStore.isPathExists(new Path(originalTable.getSd().getLocation())));
    Assert.assertEquals("New location should be the new one", newTable.getSd().getLocation(),
        alteredTable.getSd().getLocation());
    Path dataFile = new Path(alteredTable.getSd().getLocation().toString() + "/dataFile");
    Assert.assertFalse("The location should not contain data", metaStore.isPathExists(dataFile));

    // The extra parameters will be added on server side, so check that the required ones are
    // present
    for(String key: newTable.getParameters().keySet()) {
      Assert.assertEquals("parameters are present", newTable.getParameters().get(key),
          alteredTable.getParameters().get(key));
    }
    // The parameters are checked manually, so do not check them
    newTable.setParameters(alteredTable.getParameters());

    // The following data should be changed, other data should be the same
    newTable.getSd().setLocation(alteredTable.getSd().getLocation());
    Assert.assertEquals("The table data should be the same", newTable, alteredTable);
  }

  @Test
  public void testAlterTableChangeCols() throws Exception {
    Table originalTable = partitionedTable;

    Table newTable = originalTable.deepCopy();

    List<FieldSchema> cols = newTable.getSd().getCols();
    // Change a column
    cols.get(0).setName("modified_col");
    // Remove a column
    cols.remove(1);
    // Add a new column
    cols.add(new FieldSchema("new_col", "int", null));
    // Store the changes
    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
    Table alteredTable = client.getTable(newTable.getDbName(), newTable.getTableName());
    Assert.assertTrue("Original table directory should be kept",
        metaStore.isPathExists(new Path(originalTable.getSd().getLocation())));

    // The following data might be changed
    alteredTable.setParameters(newTable.getParameters());
    Assert.assertEquals("The table data should be the same", newTable, alteredTable);

    // Modify partition column type, and comment
    newTable.getPartitionKeys().get(0).setType("string");
    newTable.getPartitionKeys().get(0).setComment("changed comment");

    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
    alteredTable = client.getTable(newTable.getDbName(), newTable.getTableName());
    // The following data might be changed
    alteredTable.setParameters(newTable.getParameters());
    Assert.assertEquals("The table data should be the same", newTable, alteredTable);
  }

  @Test
  public void testAlterTableCascade() throws Exception {
    Table originalTable = partitionedTable;

    Table newTable = originalTable.deepCopy();
    List<FieldSchema> cols = newTable.getSd().getCols();
    cols.add(new FieldSchema("new_col_1", "int", null));

    // Run without cascade
    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable, false);
    Table alteredTable = client.getTable(newTable.getDbName(), newTable.getTableName());
    Assert.assertEquals("The table data should be changed", newTable, alteredTable);

    List<Partition> partitions =
        client.listPartitions(originalTable.getDbName(), originalTable.getTableName(), (short)-1);
    for(Partition partition : partitions) {
      Assert.assertEquals("Partition columns should not be changed", 2,
          partition.getSd().getCols().size());
    }

    // Run with cascade
    cols.add(new FieldSchema("new_col_2", "int", null));
    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable, true);
    alteredTable = client.getTable(newTable.getDbName(), newTable.getTableName());
    Assert.assertEquals("The table data should be changed", newTable, alteredTable);

    partitions =
        client.listPartitions(originalTable.getDbName(), originalTable.getTableName(), (short)-1);
    for(Partition partition : partitions) {
      Assert.assertEquals("Partition columns should be changed", 4,
          partition.getSd().getCols().size());
    }

    // Run using environment context with cascade
    cols.add(new FieldSchema("new_col_3", "int", null));
    EnvironmentContext context = new EnvironmentContext();
    context.putToProperties(StatsSetupConst.CASCADE, "true");
    client.alter_table_with_environmentContext(originalTable.getDbName(),
        originalTable.getTableName(), newTable, context);
    alteredTable = client.getTable(newTable.getDbName(), newTable.getTableName());
    Assert.assertEquals("The table data should be changed", newTable, alteredTable);

    partitions =
        client.listPartitions(originalTable.getDbName(), originalTable.getTableName(), (short)-1);
    for(Partition partition : partitions) {
      Assert.assertEquals("Partition columns should be changed", 5,
          partition.getSd().getCols().size());
    }
  }

  @Test(expected = MetaException.class)
  public void testAlterTableNullDatabaseInNew() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();
    newTable.setDbName(null);

    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
  }

  @Test(expected = MetaException.class)
  public void testAlterTableNullTableNameInNew() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();
    newTable.setTableName(null);

    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterTableInvalidTableNameInNew() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();
    newTable.setTableName("test_table;");
    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterTableEmptyTableNameInNew() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();
    newTable.setTableName("");

    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
  }

  @Test(expected = MetaException.class)
  public void testAlterTableNullStorageDescriptorInNew() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();
    newTable.setSd(null);

    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
  }

  @Test(expected = MetaException.class)
  public void testAlterTableNullDatabase() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();

    client.alter_table(null, originalTable.getTableName(), newTable);
  }

  @Test(expected = MetaException.class)
  public void testAlterTableNullTableName() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();

    client.alter_table(originalTable.getDbName(), null, newTable);
  }

  @Test
  public void testAlterTableNullNewTable() throws Exception {
    Table originalTable = testTables[0];
    try {
      client.alter_table(originalTable.getDbName(), originalTable.getTableName(), null);
      // TODO: Should be checked on server side. On Embedded metastore it throws
      // NullPointerException, on Remote metastore it throws TTransportException
      Assert.fail("Expected a NullPointerException or TTransportException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test(expected = MetaException.class)
  public void testAlterTableInvalidStorageDescriptorNullCols() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();
    newTable.getSd().setCols(null);

    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
  }

  @Test(expected = MetaException.class)
  public void testAlterTableInvalidStorageDescriptorNullSerdeInfo() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();
    newTable.getSd().setSerdeInfo(null);

    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
  }

  @Test(expected = MetaException.class)
  public void testAlterTableInvalidStorageDescriptorNullColumnType() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();
    newTable.getSd().getCols().get(0).setType(null);

    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
  }

  @Test(expected = MetaException.class)
  public void testAlterTableInvalidStorageDescriptorNullLocation() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();
    newTable.getSd().setLocation(null);

    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterTableInvalidStorageDescriptorInvalidColumnType() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();
    newTable.getSd().getCols().get(0).setType("xyz");

    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterTableInvalidStorageDescriptorAddPartitionColumns() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();
    newTable.addToPartitionKeys(new FieldSchema("new_part", "int", "comment"));

    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterTableInvalidStorageDescriptorAlterPartitionColumnName() throws Exception {
    Table originalTable = partitionedTable;
    Table newTable = originalTable.deepCopy();
    newTable.getPartitionKeys().get(0).setName("altered_name");

    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterTableInvalidStorageDescriptorRemovePartitionColumn() throws Exception {
    Table originalTable = partitionedTable;
    Table newTable = originalTable.deepCopy();
    newTable.getPartitionKeys().remove(0);
    client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterTableNoSuchDatabase() throws Exception {
    Table originalTable = testTables[2];
    Table newTable = originalTable.deepCopy();

    client.alter_table("no_such_database", originalTable.getTableName(), newTable);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterTableNoSuchTable() throws Exception {
    Table originalTable = testTables[2];
    Table newTable = originalTable.deepCopy();

    client.alter_table(originalTable.getDbName(), "no_such_table_name", newTable);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterTableNoSuchTableInThisDatabase() throws Exception {
    Table originalTable = testTables[2];
    Table newTable = originalTable.deepCopy();

    client.alter_table(OTHER_DATABASE, originalTable.getTableName(), newTable);
  }

  @Test
  public void testAlterTableAlreadyExists() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();

    newTable.setTableName(testTables[2].getTableName());
    try {
      // Already existing table
      client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
      // TODO: Maybe throw AlreadyExistsException.
      Assert.fail("Expected an InvalidOperationException to be thrown");
    } catch (InvalidOperationException exception) {
      // Expected exception
    }
  }

  /**
   * Creates a Table with all of the parameters set. The temporary table is available only on HS2
   * server, so do not use it.
   * @return The Table object
   */
  private Table getTableWithAllParametersSet() throws MetaException {
    return new TableBuilder()
               .setDbName(DEFAULT_DATABASE)
               .setTableName("test_table_with_all_parameters_set")
               .setCreateTime(100)
               .setOwner("owner")
               .setLastAccessTime(200)
               .addPartCol("part_col", "int", "part col comment")
               .addCol("test_col", "int", "test col comment")
               .addCol("test_bucket_col", "int", "test bucket col comment")
               .addCol("test_skewed_col", "int", "test skewed col comment")
               .addCol("test_sort_col", "int", "test sort col comment")
               .addBucketCol("test_bucket_col")
               .addSkewedColName("test_skewed_col")
               .addSortCol("test_sort_col", 1)
               .setCompressed(true)
               .setInputFormat("inputFormat")
               .setInputFormat("outputFormat")
               .setLocation(metaStore.getWarehouseRoot() + "/location")
               .setNumBuckets(4)
               .setRetention(30000)
               .setRewriteEnabled(true)
               .setType("VIEW")
               .setViewExpandedText("viewExplainedText")
               .setViewOriginalText("viewOriginalText")
               .setSerdeLib("serdelib")
               .setSerdeName("serdename")
               .setStoredAsSubDirectories(true)
               .addSerdeParam("serdeParam", "serdeParamValue")
               .addTableParam("tableParam", "tableParamValue")
               .addStorageDescriptorParam("sdParam", "sdParamValue")
               .build();
  }
}
