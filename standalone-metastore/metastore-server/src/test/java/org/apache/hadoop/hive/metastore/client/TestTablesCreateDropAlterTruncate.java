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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.SourceTable;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.hadoop.hive.metastore.TestHiveMetaStore.createSourceTable;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Test class for IMetaStoreClient API. Testing the Table related functions for metadata
 * manipulation, like creating, dropping and altering tables.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestTablesCreateDropAlterTruncate extends MetaStoreClientTest {
  private static final String DEFAULT_DATABASE = "default";
  private static final String OTHER_DATABASE = "dummy";
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;
  private Table[] testTables = new Table[6];
  private Table partitionedTable = null;
  private Table externalTable = null;

  public TestTablesCreateDropAlterTruncate(String name, AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  @BeforeClass
  public static void startMetaStores() {
    Map<MetastoreConf.ConfVars, String> msConf = new HashMap<MetastoreConf.ConfVars, String>();
    // Enable trash, so it can be tested
    Map<String, String> extraConf = new HashMap<>();
    extraConf.put("fs.trash.checkpoint.interval", "30");  // FS_TRASH_CHECKPOINT_INTERVAL_KEY
    extraConf.put("fs.trash.interval", "30");             // FS_TRASH_INTERVAL_KEY (hadoop-2)
    extraConf.put(ConfVars.HIVE_IN_TEST.getVarname(), "true");
    extraConf.put(ConfVars.METASTORE_METADATA_TRANSFORMER_CLASS.getVarname(), " ");
    extraConf.put(ConfVars.AUTHORIZATION_STORAGE_AUTH_CHECKS.getVarname(), "true");

    startMetaStores(msConf, extraConf);
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
            .setTableName("test_table")
            .addCol("test_col", "int")
            .create(client, metaStore.getConf());

    testTables[1] =
        new TableBuilder()
            .setTableName("test_view")
            .addCol("test_col", "int")
            .setType("VIRTUAL_VIEW")
            .create(client, metaStore.getConf());

    testTables[2] =
        new TableBuilder()
            .setTableName("test_table_to_find_1")
            .addCol("test_col", "int")
            .create(client, metaStore.getConf());

    testTables[3] =
        new TableBuilder()
            .setTableName("test_partitioned_table")
            .addCol("test_col1", "int")
            .addCol("test_col2", "int")
            .addPartCol("test_part_col", "int")
            .create(client, metaStore.getConf());

    testTables[4] =
        new TableBuilder()
            .setTableName("external_table_for_test")
            .addCol("test_col", "int")
            .setLocation(metaStore.getExternalWarehouseRoot() + "/external/table_dir")
            .addTableParam("EXTERNAL", "TRUE")
            .setType("EXTERNAL_TABLE")
            .create(client, metaStore.getConf());


    new DatabaseBuilder().setName(OTHER_DATABASE).create(client, metaStore.getConf());

    testTables[5] =
        new TableBuilder()
            .setDbName(OTHER_DATABASE)
            .setTableName("test_table")
            .addCol("test_col", "int")
            .create(client, metaStore.getConf());

    // Create partitions for the partitioned table
    for(int i=0; i < 2; i++) {
      new PartitionBuilder()
              .inTable(testTables[3])
              .addValue("a" + i)
              .addToTable(client, metaStore.getConf());
    }
    // Add an external partition too
    new PartitionBuilder()
        .inTable(testTables[3])
        .addValue("a2")
        .setLocation(metaStore.getWarehouseRoot() + "/external/a2")
        .addToTable(client, metaStore.getConf());

    // Add data files to the partitioned table
    List<Partition> partitions =
        client.listPartitions(testTables[3].getDbName(), testTables[3].getTableName(), (short)-1);
    for(Partition partition : partitions) {
      Path dataFile = new Path(partition.getSd().getLocation() + "/dataFile");
      metaStore.createFile(dataFile, "100");
    }

    // Reload tables from the MetaStore, and create data files
    for(int i=0; i < testTables.length; i++) {
      testTables[i] = client.getTable(testTables[i].getDbName(), testTables[i].getTableName());
      if (testTables[i].getPartitionKeys().isEmpty()) {
        if (testTables[i].getSd().getLocation() != null) {
          Path dataFile = new Path(testTables[i].getSd().getLocation() + "/dataFile");
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

  /**
   * This test creates and queries a table and then drops it. Good for testing the happy path
   */
  @Test
  public void testCreateGetDeleteTable() throws Exception {
    // Try to create a table with all of the parameters set
    Table table = getTableWithAllParametersSet();
    client.createTable(table);
    table.unsetId();

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
    table.setWriteId(createdTable.getWriteId());

    Assert.assertTrue(createdTable.isSetId());
    createdTable.unsetId();
    Assert.assertEquals("create/get table data", table, createdTable);

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
    List<FieldSchema> cols = new ArrayList<>();

    table.setDbName(DEFAULT_DATABASE);
    table.setTableName("test_table_2");
    cols.add(new FieldSchema("column_name", "int", null));
    sd.setCols(cols);
    sd.setSerdeInfo(new SerDeInfo());
    table.setSd(sd);

    client.createTable(table);
    Table createdTable = client.getTable(table.getDbName(), table.getTableName());

    Assert.assertEquals("Comparing OwnerType", PrincipalType.USER, createdTable.getOwnerType());
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
    List<FieldSchema> cols = new ArrayList<>();

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
  public void testCreateTableRooPathLocationInSpecificDatabase() {
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = new ArrayList<>();
    sd.setLocation("hdfs://localhost:8020");
    table.setDbName(DEFAULT_DATABASE);
    table.setTableName("test_table_2_with_root_path");
    cols.add(new FieldSchema("column_name", "int", null));
    sd.setCols(cols);
    sd.setSerdeInfo(new SerDeInfo());
    table.setSd(sd);

    Exception exception = assertThrows(InvalidObjectException.class, () -> client.createTable(table));
    Assert.assertEquals("Storage descriptor location",
            table.getTableName() + " location must not be root path",
            exception.getMessage());
  }

  @Test
  public void testCreateTableDefaultValuesView() throws Exception {
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = new ArrayList<>();

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
    table.setTableName("test§table;");

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
    table.setTableName("NullStorageT");
    table.setSd(null);

    client.createTable(table);
  }

  private Table getNewTable() throws MetaException {
    return new TableBuilder()
               .setTableName("test_table_with_invalid_sd")
               .addCol("test_col", "int")
               .build(metaStore.getConf());
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
    table.unsetId();

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

    table.unsetId();
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
    Partition externalPartition = client.getPartition(partitionedTable.getDbName(),
        partitionedTable.getTableName(), "test_part_col=a2");

    client.dropTable(table.getDbName(), table.getTableName(), true, false);

    Assert.assertFalse("Table path should be removed",
        metaStore.isPathExists(new Path(table.getSd().getLocation())));

    table.unsetId();
    client.createTable(table);
    client.dropTable(table.getDbName(), table.getTableName(), false, false);

    Assert.assertTrue("Table path should be kept",
        metaStore.isPathExists(new Path(table.getSd().getLocation())));

    // Drop table with partitions
    client.dropTable(partitionedTable.getDbName(), partitionedTable.getTableName(), true, false);

    Assert.assertFalse("Table path should be removed",
        metaStore.isPathExists(new Path(partitionedTable.getSd().getLocation())));

    Assert.assertFalse("Extra partition path should be removed",
        metaStore.isPathExists(new Path(externalPartition.getSd().getLocation())));
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

    Table newTable = table.deepCopy();
    newTable.setTableName("external_table_for_purge");
    newTable.getSd().setLocation(metaStore.getWarehouseRoot() + "/external/purge_table_dir");
    newTable.getParameters().put("external.table.purge", "true");

    client.createTable(newTable);
    client.dropTable(newTable.getDbName(), newTable.getTableName(), true, true, true);

    Assert.assertFalse("Table path should be removed",
        metaStore.isPathExists(new Path(newTable.getSd().getLocation())));
    Assert.assertFalse("Table path should not be in trash",
        metaStore.isPathExistsInTrash(new Path(newTable.getSd().getLocation())));

    newTable.getParameters().put("skip.trash", "true");

    client.createTable(newTable);
    client.dropTable(newTable.getDbName(), newTable.getTableName(), true, true, false);

    Assert.assertFalse("Table path should be removed",
        metaStore.isPathExists(new Path(newTable.getSd().getLocation())));
    Assert.assertFalse("Table path should not be in trash",
        metaStore.isPathExistsInTrash(new Path(newTable.getSd().getLocation())));
  }

  @Test
  public void testDropTableExternalWithoutPurge() throws Exception {
    Table table = externalTable;

    client.dropTable(table.getDbName(), table.getTableName(), true, true, false);

    Assert.assertTrue("Table path should not be removed",
        metaStore.isPathExists(new Path(table.getSd().getLocation())));
    Assert.assertFalse("Table path should not be in trash",
        metaStore.isPathExistsInTrash(new Path(table.getSd().getLocation())));

    Table newTable = table.deepCopy();
    newTable.setTableName("external_table_for_purge");
    newTable.getSd().setLocation(metaStore.getWarehouseRoot() + "/external/purge_table_dir");
    newTable.getParameters().put("external.table.purge", "true");

    client.createTable(newTable);
    client.dropTable(newTable.getDbName(), newTable.getTableName(), true, true, false);

    Assert.assertFalse("Table path should be removed",
        metaStore.isPathExists(new Path(newTable.getSd().getLocation())));
    Assert.assertTrue("Table path should be in trash",
        metaStore.isPathExistsInTrash(new Path(newTable.getSd().getLocation())));
  }

  @Test
  public void testTruncateTableUnpartitioned() throws Exception {
    // Unpartitioned table
    Path dataFile = new Path(testTables[0].getSd().getLocation() + "/dataFile");
    client.truncateTable(testTables[0].getDbName(), testTables[0].getTableName(), null);
    Assert.assertTrue("Location should exist",
        metaStore.isPathExists(new Path(testTables[0].getSd().getLocation())));
    Assert.assertFalse("DataFile should be removed", metaStore.isPathExists(dataFile));

  }

  @Test
  public void testTruncateTablePartitioned() throws Exception {
    // Partitioned table - delete specific partitions a0, a2
    List<String> partitionsToDelete = new ArrayList<>();
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
      Path dataFile = new Path(partition.getSd().getLocation() + "/dataFile");
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
      Path dataFile = new Path(partition.getSd().getLocation() + "/dataFile");
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
    newTable.setWriteId(alteredTable.getWriteId());

    Assert.assertTrue(alteredTable.isSetId());
    alteredTable.unsetId();
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

    Path dataFile = new Path(alteredTable.getSd().getLocation() + "/dataFile");
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
    Path dataFile = new Path(alteredTable.getSd().getLocation() + "/dataFile");
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
    Path dataFile = new Path(alteredTable.getSd().getLocation() + "/dataFile");
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
    Path dataFile = new Path(alteredTable.getSd().getLocation() + "/dataFile");
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

  @SuppressWarnings("deprecation")
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

  @Test
  public void testAlterTableNullTableNameInNew() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();
    newTable.setTableName(null);

    try {
      client.alter_table(originalTable.getDbName(), originalTable.getTableName(), newTable);
      Assert.fail("Expected exception");
    } catch (MetaException | TProtocolException ex) {
      // Expected.
    }
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterTableInvalidTableNameInNew() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();
    newTable.setTableName("test§table;");
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

  @Test
  public void testAlterTableNullDatabase() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();
    try {
      client.alter_table(null, originalTable.getTableName(), newTable);
      Assert.fail("Expected exception");
    } catch (MetaException | TProtocolException ex) {
    }
  }

  @Test
  public void testAlterTableNullTableName() throws Exception {
    Table originalTable = testTables[0];
    Table newTable = originalTable.deepCopy();

    try {
      client.alter_table(originalTable.getDbName(), null, newTable);
      Assert.fail("Expected exception");
    } catch (MetaException | TProtocolException ex) {
      // Expected.
    }
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
    } catch (TProtocolException exception) {
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

  @Test
  public void testAlterTableExpectedPropertyMatch() throws Exception {
    Assume.assumeTrue(MetastoreConf.getBoolVar(metaStore.getConf(), ConfVars.TRY_DIRECT_SQL));
    Assume.assumeTrue(MetastoreConf.getBoolVar(metaStore.getConf(), ConfVars.TRY_DIRECT_SQL_DDL));
    Table originalTable = testTables[0];

    EnvironmentContext context = new EnvironmentContext();
    context.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_KEY, "transient_lastDdlTime");
    context.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_VALUE,
            originalTable.getParameters().get("transient_lastDdlTime"));

    client.alter_table(originalTable.getCatName(), originalTable.getDbName(), originalTable.getTableName(),
            originalTable, context);
  }

  @Test(expected = MetaException.class)
  public void testAlterTableExpectedPropertyDifferent() throws Exception {
    Assume.assumeTrue(MetastoreConf.getBoolVar(metaStore.getConf(), ConfVars.TRY_DIRECT_SQL));
    Assume.assumeTrue(MetastoreConf.getBoolVar(metaStore.getConf(), ConfVars.TRY_DIRECT_SQL_DDL));
    Table originalTable = testTables[0];

    EnvironmentContext context = new EnvironmentContext();
    context.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_KEY, "transient_lastDdlTime");
    context.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_VALUE, "alma");

    client.alter_table(originalTable.getCatName(), originalTable.getDbName(), originalTable.getTableName(),
            originalTable, context);
  }

  /**
   * This tests ensures that concurrent Iceberg commits will fail. Acceptable as a first sanity check.
   * <p>
   * I have not found a good way to check that HMS side database commits are parallel in the
   * {@link org.apache.hadoop.hive.metastore.HiveAlterHandler#alterTable(RawStore, Warehouse, String, String, String, Table, EnvironmentContext, IHMSHandler, String)}
   * call, but this test could be used to manually ensure that using breakpoints.
   */
  @Test
  public void testAlterTableExpectedPropertyConcurrent() throws Exception {
    Assume.assumeTrue(MetastoreConf.getBoolVar(metaStore.getConf(), ConfVars.TRY_DIRECT_SQL));
    Assume.assumeTrue(MetastoreConf.getBoolVar(metaStore.getConf(), ConfVars.TRY_DIRECT_SQL_DDL));
    Table originalTable = testTables[0];

    originalTable.getParameters().put("snapshot", "0");
    client.alter_table(originalTable.getCatName(), originalTable.getDbName(), originalTable.getTableName(),
            originalTable, null);

    ExecutorService threads = null;
    try {
      threads = Executors.newFixedThreadPool(2);
      for (int i = 0; i < 3; i++) {
        EnvironmentContext context = new EnvironmentContext();
        context.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_KEY, "snapshot");
        context.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_VALUE, String.valueOf(i));

        Table newTable = originalTable.deepCopy();
        newTable.getParameters().put("snapshot", String.valueOf(i + 1));

        IMetaStoreClient client1 = metaStore.getClient();
        IMetaStoreClient client2 = metaStore.getClient();

        Collection<Callable<Boolean>> concurrentTasks = new ArrayList<>(2);
        concurrentTasks.add(alterTask(client1, newTable, context));
        concurrentTasks.add(alterTask(client2, newTable, context));

        Collection<Future<Boolean>> results = threads.invokeAll(concurrentTasks);

        boolean foundSuccess = false;
        boolean foundFailure = false;

        for (Future<Boolean> result : results) {
          if (result.get()) {
            foundSuccess = true;
          } else {
            foundFailure = true;
          }
        }

        assertTrue("At least one success is expected", foundSuccess);
        assertTrue("At least one failure is expected", foundFailure);
      }
    } finally {
      if (threads != null) {
        threads.shutdown();
      }
    }
  }

  private Callable<Boolean> alterTask(IMetaStoreClient hmsClient, Table newTable, EnvironmentContext context) {
    return () -> {
      try {
        hmsClient.alter_table(newTable.getCatName(), newTable.getDbName(), newTable.getTableName(),
                newTable, context);
      } catch (Throwable e) {
        return false;
      }
      return true;
    };
  }

  @Test
  public void tablesInOtherCatalogs() throws TException, URISyntaxException {
    String catName = "create_etc_tables_in_other_catalogs";
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

    Table table = new TableBuilder()
        .inDb(db)
        .setTableName("mvSource")
        .addCol("col1_1", ColumnType.STRING_TYPE_NAME)
        .addCol("col2_2", ColumnType.INT_TYPE_NAME).build(metaStore.getConf());
    client.createTable(table);
    SourceTable sourceTable = createSourceTable(table);

    String[] tableNames = new String[4];
    for (int i = 0; i < tableNames.length; i++) {
      tableNames[i] = "table_in_other_catalog_" + i;
      TableBuilder builder = new TableBuilder()
          .inDb(db)
          .setTableName(tableNames[i])
          .addCol("col1_" + i, ColumnType.STRING_TYPE_NAME)
          .addCol("col2_" + i, ColumnType.INT_TYPE_NAME);
      // Make one have a non-standard location
      if (i == 0) {
        builder.setLocation(MetaStoreTestUtils.getTestWarehouseDir(tableNames[i]));
      }
      // Make one partitioned
      if (i == 2) {
        builder.addPartCol("pcol1", ColumnType.STRING_TYPE_NAME);
      }
      // Make one a materialized view
      if (i == 3) {
        builder.setType(TableType.MATERIALIZED_VIEW.name())
            .setRewriteEnabled(true)
            .addMaterializedViewReferencedTable(sourceTable);
      }
      client.createTable(builder.build(metaStore.getConf()));
    }

    // Add partitions for the partitioned table
    String[] partVals = new String[3];
    Table partitionedTable = client.getTable(catName, dbName, tableNames[2]);
    for (int i = 0; i < partVals.length; i++) {
      partVals[i] = "part" + i;
      new PartitionBuilder()
          .inTable(partitionedTable)
          .addValue(partVals[i])
          .addToTable(client, metaStore.getConf());
    }

    // Get tables, make sure the locations are correct
    for (int i = 0; i < tableNames.length; i++) {
      Table t = client.getTable(catName, dbName, tableNames[i]);
      Assert.assertEquals(catName, t.getCatName());
      String expectedLocation = (i < 1) ?
        new File(MetaStoreTestUtils.getTestWarehouseDir(tableNames[i])).toURI().toString()
        :
        new File(cat.getLocationUri() + File.separatorChar + dbName + ".db",
            tableNames[i]).toURI().toString();

      Assert.assertEquals(expectedLocation, t.getSd().getLocation() + "/");
      File dir = new File(new URI(t.getSd().getLocation()).getPath());
      Assert.assertTrue(dir.exists() && dir.isDirectory());

    }

    // Make sure getting table in the wrong catalog does not work
    try {
      Table t = client.getTable(DEFAULT_DATABASE_NAME, tableNames[0]);
      Assert.fail();
    } catch (NoSuchObjectException e) {
      // NOP
    }

    // test getAllTables
    Set<String> fetchedNames = new HashSet<>(client.getAllTables(catName, dbName));
    Assert.assertEquals(tableNames.length + 1, fetchedNames.size());
    for (String tableName : tableNames) {
      Assert.assertTrue(fetchedNames.contains(tableName));
    }

    fetchedNames = new HashSet<>(client.getAllTables(DEFAULT_DATABASE_NAME));
    for (String tableName : tableNames) {
      Assert.assertFalse(fetchedNames.contains(tableName));
    }

    // test getMaterializedViewsForRewriting
    List<String> materializedViews = client.getMaterializedViewsForRewriting(catName, dbName);
    Assert.assertEquals(1, materializedViews.size());
    Assert.assertEquals(tableNames[3], materializedViews.get(0));

    fetchedNames = new HashSet<>(client.getMaterializedViewsForRewriting(DEFAULT_DATABASE_NAME));
    Assert.assertFalse(fetchedNames.contains(tableNames[3]));

    // test getTableObjectsByName
    List<Table> fetchedTables = client.getTableObjectsByName(catName, dbName,
        Arrays.asList(tableNames[0], tableNames[1]));
    Assert.assertEquals(2, fetchedTables.size());
    Collections.sort(fetchedTables);
    Assert.assertEquals(tableNames[0], fetchedTables.get(0).getTableName());
    Assert.assertEquals(tableNames[1], fetchedTables.get(1).getTableName());

    fetchedTables = client.getTableObjectsByName(DEFAULT_DATABASE_NAME,
        Arrays.asList(tableNames[0], tableNames[1]));
    Assert.assertEquals(0, fetchedTables.size());

    // Test altering the table
    Table t = client.getTable(catName, dbName, tableNames[0]).deepCopy();
    t.getParameters().put("test", "test");
    client.alter_table(catName, dbName, tableNames[0], t);
    t = client.getTable(catName, dbName, tableNames[0]).deepCopy();
    Assert.assertEquals("test", t.getParameters().get("test"));

    // Alter a table in the wrong catalog
    try {
      client.alter_table(DEFAULT_DATABASE_NAME, tableNames[0], t);
      Assert.fail();
    } catch (InvalidOperationException e) {
      // NOP
    }

    // Update the metadata for the materialized view
    CreationMetadata cm = client.getTable(catName, dbName, tableNames[3]).getCreationMetadata();
    Table table1 = new TableBuilder()
        .inDb(db)
        .setTableName("mvSource2")
        .addCol("col1_1", ColumnType.STRING_TYPE_NAME)
        .addCol("col2_2", ColumnType.INT_TYPE_NAME).build(metaStore.getConf());
    client.createTable(table1);
    sourceTable = createSourceTable(table1);
    cm.addToTablesUsed(
            TableName.getDbTable(sourceTable.getTable().getDbName(), sourceTable.getTable().getTableName()));
    cm.addToSourceTables(sourceTable);
    cm.unsetMaterializationTime();
    client.updateCreationMetadata(catName, dbName, tableNames[3], cm);

    List<String> partNames = new ArrayList<>();
    for (String partVal : partVals) {
      partNames.add("pcol1=" + partVal);
    }
    // Truncate a table
    client.truncateTable(catName, dbName, tableNames[0], partNames);

    // Truncate a table in the wrong catalog
    try {
      client.truncateTable(DEFAULT_DATABASE_NAME, tableNames[0], partNames);
      Assert.fail();
    } catch (NoSuchObjectException|TApplicationException e) {
      // NOP
    }

    // Drop a table from the wrong catalog
    try {
      client.dropTable(DEFAULT_DATABASE_NAME, tableNames[0], true, false);
      Assert.fail();
    } catch (NoSuchObjectException|TApplicationException e) {
      // NOP
    }

    // Should ignore the failure
    client.dropTable(DEFAULT_DATABASE_NAME, tableNames[0], false, true);

    // Have to do this in reverse order so that we drop the materialized view first.
    for (int i = tableNames.length - 1; i >= 0; i--) {
      t = client.getTable(catName, dbName, tableNames[i]);
      File tableDir = new File(new URI(t.getSd().getLocation()).getPath());
      Assert.assertTrue(tableDir.exists() && tableDir.isDirectory());

      if (tableNames[i].equalsIgnoreCase(tableNames[0])) {
        client.dropTable(catName, dbName, tableNames[i], false, false);
        Assert.assertTrue(tableDir.exists() && tableDir.isDirectory());
      } else {
        client.dropTable(catName, dbName, tableNames[i]);
        Assert.assertFalse(tableDir.exists());
      }
    }

    client.dropTable(table.getCatName(), table.getDbName(), table.getTableName());
    client.dropTable(table1.getCatName(), table1.getDbName(), table1.getTableName());

    Assert.assertEquals(0, client.getAllTables(catName, dbName).size());
  }

  @Test(expected = InvalidObjectException.class)
  public void createTableInBogusCatalog() throws TException {
    new TableBuilder()
        .setCatName("nosuch")
        .setTableName("doomed")
        .addCol("col1", ColumnType.STRING_TYPE_NAME)
        .addCol("col2", ColumnType.INT_TYPE_NAME)
        .create(client, metaStore.getConf());
  }

  @Test(expected = NoSuchObjectException.class)
  public void getTableInBogusCatalog() throws TException {
    client.getTable("nosuch", testTables[0].getDbName(), testTables[0].getTableName());
  }

  @Test
  public void getAllTablesInBogusCatalog() throws TException {
    List<String> names = client.getAllTables("nosuch", testTables[0].getDbName());
    Assert.assertTrue(names.isEmpty());
  }

  @Test(expected = UnknownDBException.class)
  public void getTableObjectsByNameBogusCatalog() throws TException {
    client.getTableObjectsByName("nosuch", testTables[0].getDbName(),
        Arrays.asList(testTables[0].getTableName(), testTables[1].getTableName()));
  }

  @Test
  public void getMaterializedViewsInBogusCatalog() throws TException {
    List<String> names = client.getMaterializedViewsForRewriting("nosuch", DEFAULT_DATABASE_NAME);
    Assert.assertTrue(names.isEmpty());
  }

  @Test(expected = InvalidOperationException.class)
  public void alterTableBogusCatalog() throws TException {
    Table t = testTables[0].deepCopy();
    t.getParameters().put("a", "b");
    client.alter_table("nosuch", t.getDbName(), t.getTableName(), t);
  }

  @Test(expected = InvalidOperationException.class)
  public void moveTablesBetweenCatalogsOnAlter() throws TException {
    String catName = "move_table_between_catalogs_on_alter";
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .build();
    client.createCatalog(cat);

    String dbName = "a_db";
    // For this one don't specify a location to make sure it gets put in the catalog directory
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .setCatalogName(catName)
        .create(client, metaStore.getConf());

    String tableName = "non_movable_table";
    Table before = new TableBuilder()
        .inDb(db)
        .setTableName(tableName)
        .addCol("col1", ColumnType.STRING_TYPE_NAME)
        .addCol("col2", ColumnType.INT_TYPE_NAME)
        .create(client, metaStore.getConf());
    Table after = before.deepCopy();
    after.setCatName(DEFAULT_CATALOG_NAME);
    client.alter_table(catName, dbName, tableName, after);

  }

  @Test
  public void truncateTableBogusCatalog() throws TException {
    try {
      List<String> partNames = client.listPartitionNames(partitionedTable.getDbName(),
          partitionedTable.getTableName(), (short) -1);
      client.truncateTable("nosuch", partitionedTable.getDbName(), partitionedTable.getTableName(),
          partNames);
      Assert.fail(); // For reasons I don't understand and am too lazy to debug at the moment the
      // NoSuchObjectException gets swallowed by a TApplicationException in remote mode.
    } catch (TApplicationException|NoSuchObjectException e) {
      //NOP
    }
  }

  @Test(expected = NoSuchObjectException.class)
  public void dropTableBogusCatalog() throws TException {
    client.dropTable("nosuch", testTables[0].getDbName(), testTables[0].getTableName(), true, false);
  }

  @Test(expected = MetaException.class)
  public void testDropManagedTableWithoutStoragePermission() throws TException, IOException {
    String dbName = testTables[0].getDbName();
    String tblName = testTables[0].getTableName();
    Table table = client.getTable(dbName, tblName);
    Path tablePath = new Path(table.getSd().getLocation());
    FileSystem fs = Warehouse.getFs(tablePath, new Configuration());
    fs.setPermission(tablePath.getParent(), new FsPermission((short) 0555));

    try {
      client.dropTable(dbName, tblName);
    } finally {
      // recover write permission so that file can be cleaned.
      fs.setPermission(tablePath.getParent(), new FsPermission((short) 0755));
    }
  }

  @Test
  public void testDropExternalTableWithoutStoragePermission() throws TException, IOException {
    // external table
    String dbName = testTables[4].getDbName();
    String tblName = testTables[4].getTableName();
    Table table = client.getTable(dbName, tblName);
    Path tablePath = new Path(table.getSd().getLocation());
    FileSystem fs = Warehouse.getFs(tablePath, new Configuration());
    fs.setPermission(tablePath.getParent(), new FsPermission((short) 0555));

    try {
      client.dropTable(dbName, tblName);
    } finally {
      // recover write permission so that file can be cleaned.
      fs.setPermission(tablePath.getParent(), new FsPermission((short) 0755));
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
               .setOwnerType(PrincipalType.ROLE)
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
               .setType(TableType.VIRTUAL_VIEW.name())
               .setViewExpandedText("viewExplainedText")
               .setViewOriginalText("viewOriginalText")
               .setSerdeLib("serdelib")
               .setSerdeName("serdename")
               .setStoredAsSubDirectories(true)
               .addSerdeParam("serdeParam", "serdeParamValue")
               .addTableParam("tableParam", "tableParamValue")
               .addStorageDescriptorParam("sdParam", "sdParamValue")
               .build(metaStore.getConf());
  }
}
