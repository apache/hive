/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.FunctionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLCheckConstraintBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLDefaultConstraintBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLForeignKeyBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLNotNullConstraintBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLPrimaryKeyBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLUniqueConstraintBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

public abstract class NonCatCallsWithCatalog {

  private static final String OTHER_DATABASE = "non_cat_other_db";
  private Table[] testTables = new Table[6];
  private static final String TEST_FUNCTION_CLASS =
      "org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper";

  protected Configuration conf;
  protected IMetaStoreClient client;

  protected abstract IMetaStoreClient getClient() throws Exception;
  protected abstract String expectedCatalog();
  protected abstract String expectedBaseDir() throws MetaException;
  protected abstract String expectedExtBaseDir() throws MetaException;

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(this.conf, ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, ConfVars.METASTORE_METADATA_TRANSFORMER_CLASS, " ");
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    // Get new client
    client = getClient();

    List<String> databases = client.getAllDatabases();
    for (String db : databases) {
      if (!DEFAULT_DATABASE_NAME.equals(db)) {
        client.dropDatabase(db, true, true, true);
      }
    }
    // Drop every table in the default database
    for(String tableName : client.getAllTables(DEFAULT_DATABASE_NAME)) {
      client.dropTable(DEFAULT_DATABASE_NAME, tableName, true, true, true);
    }

    Database db = new DatabaseBuilder().setName(OTHER_DATABASE).build(conf);
    db.unsetCatalogName();
    client.createDatabase(db);


    testTables[0] =
        new TableBuilder()
            .setTableName("test_table")
            .addCol("test_col1", "int")
            .addCol("test_col2", "int")
            .addCol("test_col3", "int")
            .build(conf);

    testTables[1] =
        new TableBuilder()
            .setTableName("test_view")
            .addCol("test_col1", "int")
            .addCol("test_col2", "int")
            .addCol("test_col3", "int")
            .setType("VIRTUAL_VIEW")
            .build(conf);

    testTables[2] =
        new TableBuilder()
            .setTableName("test_table_to_find_1")
            .addCol("test_col1", "int")
            .addCol("test_col2", "int")
            .addCol("test_col3", "int")
            .build(conf);

    testTables[3] =
        new TableBuilder()
            .setTableName("test_partitioned_table")
            .addCol("test_col1", "int")
            .addCol("test_col2", "int")
            .addPartCol("test_part_col", "int")
            .build(conf);

    testTables[4] =
        new TableBuilder()
            .setTableName("external_table_for_test")
            .addCol("test_col", "int")
            .setLocation(MetaStoreTestUtils.getTestWarehouseDir("/external/table_dir"))
            .addTableParam("EXTERNAL", "TRUE")
            .setType("EXTERNAL_TABLE")
            .build(conf);

    testTables[5] =
        new TableBuilder()
            .setDbName(OTHER_DATABASE)
            .setTableName("test_table")
            .addCol("test_col", "int")
            .build(conf);

    for (Table t : testTables) {
      t.unsetCatName();
      client.createTable(t);
    }

    // Create partitions for the partitioned table
    for(int i=0; i < 3; i++) {
      Partition p = new PartitionBuilder()
          .inTable(testTables[3])
          .addValue("a" + i)
          .build(conf);
      p.unsetCatName();
      client.add_partition(p);
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
  public void databases() throws TException, URISyntaxException {
    String[] dbNames = {"db1", "db9"};
    Database[] dbs = new Database[2];
    // For this one don't specify a location to make sure it gets put in the catalog directory
    dbs[0] = new DatabaseBuilder()
        .setName(dbNames[0])
        .build(conf);

    // For the second one, explicitly set a location to make sure it ends up in the specified place.
    String db1Location = MetaStoreTestUtils.getTestWarehouseDir(dbNames[1]);
    dbs[1] = new DatabaseBuilder()
        .setName(dbNames[1])
        .setLocation(db1Location)
        .build(conf);

    for (Database db : dbs) {
      db.unsetCatalogName();
      client.createDatabase(db);
    }

    Database fetched = client.getDatabase(dbNames[0]);
    String expectedLocation = new File(expectedExtBaseDir(), dbNames[0] + ".db").toURI().toString();
    Assert.assertEquals(expectedCatalog(), fetched.getCatalogName());
    Assert.assertEquals(expectedLocation, fetched.getLocationUri() + "/");
    String db0Location = new URI(fetched.getLocationUri()).getPath();
    File dir = new File(db0Location);
    Assert.assertTrue(dir.exists() && dir.isDirectory());
    Assert.assertEquals(expectedCatalog(), fetched.getCatalogName());

    fetched = client.getDatabase(dbNames[1]);
    Assert.assertEquals(new File(db1Location).toURI().toString(), fetched.getLocationUri() + "/");
    dir = new File(new URI(fetched.getLocationUri()).getPath());
    Assert.assertTrue(dir.exists() && dir.isDirectory());
    Assert.assertEquals(expectedCatalog(), fetched.getCatalogName());

    Set<String> fetchedDbs = new HashSet<>(client.getAllDatabases());
    for (String dbName : dbNames) {
      Assert.assertTrue(fetchedDbs.contains(dbName));
    }

    fetchedDbs = new HashSet<>(client.getDatabases("db*"));
    Assert.assertEquals(2, fetchedDbs.size());
    for (String dbName : dbNames) {
      Assert.assertTrue(fetchedDbs.contains(dbName));
    }

    client.dropDatabase(dbNames[0], true, false, false);
    dir = new File(db0Location);
    Assert.assertFalse(dir.exists());

    client.dropDatabase(dbNames[1], true, false, false);
    dir = new File(db1Location);
    Assert.assertFalse(dir.exists());

    fetchedDbs = new HashSet<>(client.getAllDatabases());
    for (String dbName : dbNames) {
      Assert.assertFalse(fetchedDbs.contains(dbName));
    }
  }

  @Test
  public void tablesCreateDropAlterTruncate() throws TException, URISyntaxException {
    String dbName = "db_in_other_catalog";
    // For this one don't specify a location to make sure it gets put in the catalog directory
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .build(conf);
    db.unsetCatalogName();
    client.createDatabase(db);

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
      /*
      // TODO HIVE-18991
      if (i == 3) {
        builder.setType(TableType.MATERIALIZED_VIEW.name())
            .setRewriteEnabled(true)
            .addMaterializedViewReferencedTable(dbName + "." + tableNames[0]);
      }
      */
      Table t = builder.build(conf);
      t.unsetCatName();
      client.createTable(t);
    }

    // Add partitions for the partitioned table
    String[] partVals = new String[3];
    Table partitionedTable = client.getTable(dbName, tableNames[2]);
    for (int i = 0; i < partVals.length; i++) {
      partVals[i] = "part" + i;
      Partition p = new PartitionBuilder()
          .inTable(partitionedTable)
          .addValue(partVals[i])
          .build(conf);
      p.unsetCatName();
      client.add_partition(p);
    }

    // Get tables, make sure the locations are correct
    for (int i = 0; i < tableNames.length; i++) {
      Table t = client.getTable(dbName, tableNames[i]);
      Assert.assertEquals(expectedCatalog(), t.getCatName());
      String expectedLocation = (i < 1) ?
          new File(MetaStoreTestUtils.getTestWarehouseDir(tableNames[i])).toURI().toString()
          :
          new File(expectedBaseDir() + File.separatorChar + dbName + ".db",
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
    Set<String> fetchedNames = new HashSet<>(client.getAllTables(dbName));
    Assert.assertEquals(tableNames.length, fetchedNames.size());
    for (String tableName : tableNames) {
      Assert.assertTrue(fetchedNames.contains(tableName));
    }

    fetchedNames = new HashSet<>(client.getAllTables(DEFAULT_DATABASE_NAME));
    for (String tableName : tableNames) {
      Assert.assertFalse(fetchedNames.contains(tableName));
    }

    // test getMaterializedViewsForRewriting
    /* TODO HIVE-18991
    List<String> materializedViews = client.getMaterializedViewsForRewriting(dbName);
    Assert.assertEquals(1, materializedViews.size());
    Assert.assertEquals(tableNames[3], materializedViews.get(0));
    */

    fetchedNames = new HashSet<>(client.getMaterializedViewsForRewriting(DEFAULT_DATABASE_NAME));
    Assert.assertFalse(fetchedNames.contains(tableNames[3]));

    // test getTableObjectsByName
    List<Table> fetchedTables = client.getTableObjectsByName(dbName,
        Arrays.asList(tableNames[0], tableNames[1]));
    Assert.assertEquals(2, fetchedTables.size());
    Collections.sort(fetchedTables);
    Assert.assertEquals(tableNames[0], fetchedTables.get(0).getTableName());
    Assert.assertEquals(tableNames[1], fetchedTables.get(1).getTableName());

    fetchedTables = client.getTableObjectsByName(DEFAULT_DATABASE_NAME,
        Arrays.asList(tableNames[0], tableNames[1]));
    Assert.assertEquals(0, fetchedTables.size());

    // Test altering the table
    Table t = client.getTable(dbName, tableNames[0]).deepCopy();
    t.getParameters().put("test", "test");
    client.alter_table(dbName, tableNames[0], t);
    t = client.getTable(dbName, tableNames[0]).deepCopy();
    Assert.assertEquals("test", t.getParameters().get("test"));

    // Alter a table in the wrong catalog
    try {
      client.alter_table(DEFAULT_DATABASE_NAME, tableNames[0], t);
      Assert.fail();
    } catch (InvalidOperationException e) {
      // NOP
    }

    // Update the metadata for the materialized view
    /* TODO HIVE-18991
    CreationMetadata cm = client.getTable(dbName, tableNames[3]).getCreationMetadata();
    cm.addToTablesUsed(dbName + "." + tableNames[1]);
    client.updateCreationMetadata(dbName, tableNames[3], cm);
    */

    List<String> partNames = new ArrayList<>();
    for (String partVal : partVals) {
      partNames.add("pcol1=" + partVal);
    }
    // Truncate a table
    client.truncateTable(dbName, tableNames[0], partNames);

    // Have to do this in reverse order so that we drop the materialized view first.
    for (int i = tableNames.length - 1; i >= 0; i--) {
      t = client.getTable(dbName, tableNames[i]);
      File tableDir = new File(new URI(t.getSd().getLocation()).getPath());
      Assert.assertTrue(tableDir.exists() && tableDir.isDirectory());

      if (tableNames[i].equalsIgnoreCase(tableNames[0])) {
        client.dropTable(dbName, tableNames[i], false, false);
        Assert.assertTrue(tableDir.exists() && tableDir.isDirectory());
      } else {
        client.dropTable(dbName, tableNames[i]);
        Assert.assertFalse(tableDir.exists());
      }
    }
    Assert.assertEquals(0, client.getAllTables(dbName).size());
  }

  @Test
  public void tablesGetExists() throws TException {
    String dbName = "db_in_other_catalog";
    // For this one don't specify a location to make sure it gets put in the catalog directory
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .build(conf);
    db.unsetCatalogName();
    client.createDatabase(db);

    String[] tableNames = new String[4];
    for (int i = 0; i < tableNames.length; i++) {
      tableNames[i] = "table_in_other_catalog_" + i;
      Table table = new TableBuilder()
          .inDb(db)
          .setTableName(tableNames[i])
          .addCol("col1_" + i, ColumnType.STRING_TYPE_NAME)
          .addCol("col2_" + i, ColumnType.INT_TYPE_NAME)
          .build(conf);
      table.unsetCatName();
      client.createTable(table);
    }

    Set<String> tables = new HashSet<>(client.getTables(dbName, "*e_in_other_*"));
    Assert.assertEquals(4, tables.size());
    for (String tableName : tableNames) {
      Assert.assertTrue(tables.contains(tableName));
    }

    List<String> fetchedNames = client.getTables(dbName, "*_3");
    Assert.assertEquals(1, fetchedNames.size());
    Assert.assertEquals(tableNames[3], fetchedNames.get(0));

    Assert.assertTrue("Table exists", client.tableExists(dbName, tableNames[0]));
    Assert.assertFalse("Table not exists", client.tableExists(dbName, "non_existing_table"));
  }

  @Test
  public void tablesList() throws TException {
    String dbName = "db_in_other_catalog";
    // For this one don't specify a location to make sure it gets put in the catalog directory
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .build(conf);
    db.unsetCatalogName();
    client.createDatabase(db);

    String[] tableNames = new String[4];
    for (int i = 0; i < tableNames.length; i++) {
      tableNames[i] = "table_in_other_catalog_" + i;
      TableBuilder builder = new TableBuilder()
          .inDb(db)
          .setTableName(tableNames[i])
          .addCol("col1_" + i, ColumnType.STRING_TYPE_NAME)
          .addCol("col2_" + i, ColumnType.INT_TYPE_NAME);
      if (i == 0) {
        builder.addTableParam("the_key", "the_value");
      }
      Table table = builder.build(conf);
      table.unsetCatName();
      client.createTable(table);
    }

    String filter = hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS + "the_key=\"the_value\"";
    List<String> fetchedNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
    Assert.assertEquals(1, fetchedNames.size());
    Assert.assertEquals(tableNames[0], fetchedNames.get(0));
  }

  @Test
  public void getTableMeta() throws TException {
    String dbName = "db9";
    // For this one don't specify a location to make sure it gets put in the catalog directory
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .build(conf);
    db.unsetCatalogName();
    client.createDatabase(db);

    String[] tableNames = {"table_in_other_catalog_1", "table_in_other_catalog_2", "random_name"};
    List<TableMeta> expected = new ArrayList<>(tableNames.length);
    for (int i = 0; i < tableNames.length; i++) {
      Table table = new TableBuilder()
          .inDb(db)
          .setTableName(tableNames[i])
          .addCol("id", "int")
          .addCol("name", "string")
          .build(conf);
      table.unsetCatName();
      client.createTable(table);
      TableMeta tableMeta = new TableMeta(dbName, tableNames[i], TableType.MANAGED_TABLE.name());
      tableMeta.setOwnerName(table.getOwner());
      tableMeta.setOwnerType(table.getOwnerType());

      tableMeta.setCatName(expectedCatalog());
      expected.add(tableMeta);
    }

    List<String> types = Collections.singletonList(TableType.MANAGED_TABLE.name());
    List<TableMeta> actual = client.getTableMeta(dbName, "*", types);
    Assert.assertEquals(new TreeSet<>(expected), new TreeSet<>(actual));

    actual = client.getTableMeta("*", "table_*", types);
    Assert.assertEquals(expected.subList(0, 2), actual.subList(0, 2));

  }

  @Test
  public void addPartitions() throws TException {
    String dbName = "add_partition_database_in_other_catalog";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .build(conf);
    db.unsetCatalogName();
    client.createDatabase(db);

    String tableName = "table_in_other_catalog";
    Table table = new TableBuilder()
        .inDb(db)
        .setTableName(tableName)
        .addCol("id", "int")
        .addCol("name", "string")
        .addPartCol("partcol", "string")
        .build(conf);
    table.unsetCatName();
    client.createTable(table);

    Partition[] parts = new Partition[5];
    for (int i = 0; i < parts.length; i++) {
      parts[i] = new PartitionBuilder()
          .inTable(table)
          .addValue("a" + i)
          .build(conf);
      parts[i].unsetCatName();
    }
    client.add_partition(parts[0]);
    Assert.assertEquals(2, client.add_partitions(Arrays.asList(parts[1], parts[2])));
    client.add_partitions(Arrays.asList(parts[3], parts[4]), true, false);

    for (int i = 0; i < parts.length; i++) {
      Partition fetched = client.getPartition(dbName, tableName,
          Collections.singletonList("a" + i));
      Assert.assertEquals(dbName, fetched.getDbName());
      Assert.assertEquals(tableName, fetched.getTableName());
      Assert.assertEquals(expectedCatalog(), fetched.getCatName());
    }

    client.dropDatabase(dbName, true, true, true);
  }

  @Test
  public void getPartitions() throws TException {
    String dbName = "get_partition_database_in_other_catalog";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .build(conf);
    db.unsetCatalogName();
    client.createDatabase(db);

    String tableName = "table_in_other_catalog";
    Table table = new TableBuilder()
        .inDb(db)
        .setTableName(tableName)
        .addCol("id", "int")
        .addCol("name", "string")
        .addPartCol("partcol", "string")
        .addTableParam("PARTITION_LEVEL_PRIVILEGE", "true")
        .build(conf);
    table.unsetCatName();
    client.createTable(table);

    Partition[] parts = new Partition[5];
    for (int i = 0; i < parts.length; i++) {
      parts[i] = new PartitionBuilder()
          .inTable(table)
          .addValue("a" + i)
          .build(conf);
      parts[i].unsetCatName();
    }
    client.add_partitions(Arrays.asList(parts));

    Partition fetched = client.getPartition(dbName, tableName,
        Collections.singletonList("a0"));
    Assert.assertEquals(expectedCatalog(), fetched.getCatName());
    Assert.assertEquals("a0", fetched.getValues().get(0));

    fetched = client.getPartition(dbName, tableName, "partcol=a0");
    Assert.assertEquals(expectedCatalog(), fetched.getCatName());
    Assert.assertEquals("a0", fetched.getValues().get(0));

    List<Partition> fetchedParts = client.getPartitionsByNames(dbName, tableName,
        Arrays.asList("partcol=a0", "partcol=a1"));
    Assert.assertEquals(2, fetchedParts.size());
    Set<String> vals = new HashSet<>(fetchedParts.size());
    for (Partition part : fetchedParts) {
      vals.add(part.getValues().get(0));
    }
    Assert.assertTrue(vals.contains("a0"));
    Assert.assertTrue(vals.contains("a1"));

  }

  @Test
  public void listPartitions() throws TException {
    String dbName = "list_partition_database_in_other_catalog";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .build(conf);
    db.unsetCatalogName();
    client.createDatabase(db);

    String tableName = "table_in_other_catalog";
    Table table = new TableBuilder()
        .inDb(db)
        .setTableName(tableName)
        .addCol("id", "int")
        .addCol("name", "string")
        .addPartCol("partcol", "string")
        .build(conf);
    table.unsetCatName();
    client.createTable(table);

    Partition[] parts = new Partition[5];
    for (int i = 0; i < parts.length; i++) {
      parts[i] = new PartitionBuilder()
          .inTable(table)
          .addValue("a" + i)
          .build(conf);
      parts[i].unsetCatName();
    }
    client.add_partitions(Arrays.asList(parts));

    List<Partition> fetched = client.listPartitions(dbName, tableName, (short)-1);
    Assert.assertEquals(parts.length, fetched.size());
    Assert.assertEquals(expectedCatalog(), fetched.get(0).getCatName());

    fetched = client.listPartitions(dbName, tableName,
        Collections.singletonList("a0"), (short)-1);
    Assert.assertEquals(1, fetched.size());
    Assert.assertEquals(expectedCatalog(), fetched.get(0).getCatName());

    PartitionSpecProxy proxy = client.listPartitionSpecs(dbName, tableName, -1);
    Assert.assertEquals(parts.length, proxy.size());
    Assert.assertEquals(expectedCatalog(), proxy.getCatName());

    fetched = client.listPartitionsByFilter(dbName, tableName, "partcol=\"a0\"", (short)-1);
    Assert.assertEquals(1, fetched.size());
    Assert.assertEquals(expectedCatalog(), fetched.get(0).getCatName());

    proxy = client.listPartitionSpecsByFilter(dbName, tableName, "partcol=\"a0\"", -1);
    Assert.assertEquals(1, proxy.size());
    Assert.assertEquals(expectedCatalog(), proxy.getCatName());

    Assert.assertEquals(1, client.getNumPartitionsByFilter(dbName, tableName,
        "partcol=\"a0\""));

    List<String> names = client.listPartitionNames(dbName, tableName, (short)57);
    Assert.assertEquals(parts.length, names.size());

    names = client.listPartitionNames(dbName, tableName, Collections.singletonList("a0"),
        Short.MAX_VALUE);
    Assert.assertEquals(1, names.size());

    PartitionValuesRequest rqst = new PartitionValuesRequest(dbName,
        tableName, Lists.newArrayList(new FieldSchema("partcol", "string", "")));
    PartitionValuesResponse rsp = client.listPartitionValues(rqst);
    Assert.assertEquals(5, rsp.getPartitionValuesSize());
  }

  @Test
  public void alterPartitions() throws TException {
    String dbName = "alter_partition_database_in_other_catalog";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .build(conf);
    db.unsetCatalogName();
    client.createDatabase(db);

    String tableName = "table_in_other_catalog";
    Table table = new TableBuilder()
        .inDb(db)
        .setTableName(tableName)
        .addCol("id", "int")
        .addCol("name", "string")
        .addPartCol("partcol", "string")
        .build(conf);
    table.unsetCatName();
    client.createTable(table);

    Partition[] parts = new Partition[5];
    for (int i = 0; i < 5; i++) {
      parts[i] = new PartitionBuilder()
          .inTable(table)
          .addValue("a" + i)
          .setLocation(MetaStoreTestUtils.getTestWarehouseDir("b" + i))
          .build(conf);
      parts[i].unsetCatName();
    }
    client.add_partitions(Arrays.asList(parts));

    Partition newPart =
        client.getPartition(dbName, tableName, Collections.singletonList("a0"));
    newPart.getParameters().put("test_key", "test_value");
    client.alter_partition(dbName, tableName, newPart);

    Partition fetched =
        client.getPartition(dbName, tableName, Collections.singletonList("a0"));
    Assert.assertEquals("test_value", fetched.getParameters().get("test_key"));

    newPart =
        client.getPartition(dbName, tableName, Collections.singletonList("a1"));
    newPart.setLastAccessTime(3);
    Partition newPart1 =
        client.getPartition(dbName, tableName, Collections.singletonList("a2"));
    newPart1.getSd().setLocation(MetaStoreTestUtils.getTestWarehouseDir("somewhere"));
    client.alter_partitions(dbName, tableName, Arrays.asList(newPart, newPart1));
    fetched =
        client.getPartition(dbName, tableName, Collections.singletonList("a1"));
    Assert.assertEquals(3L, fetched.getLastAccessTime());
    fetched =
        client.getPartition(dbName, tableName, Collections.singletonList("a2"));
    Assert.assertTrue(fetched.getSd().getLocation().contains("somewhere"));

    newPart =
        client.getPartition(dbName, tableName, Collections.singletonList("a4"));
    newPart.getParameters().put("test_key", "test_value");
    EnvironmentContext ec = new EnvironmentContext();
    ec.setProperties(Collections.singletonMap("a", "b"));
    client.alter_partition(dbName, tableName, newPart, ec);
    fetched =
        client.getPartition(dbName, tableName, Collections.singletonList("a4"));
    Assert.assertEquals("test_value", fetched.getParameters().get("test_key"));


    client.dropDatabase(dbName, true, true, true);
  }

  @Test
  public void dropPartitions() throws TException {
    String dbName = "drop_partition_database_in_other_catalog";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .build(conf);
    db.unsetCatalogName();
    client.createDatabase(db);

    String tableName = "table_in_other_catalog";
    Table table = new TableBuilder()
        .inDb(db)
        .setTableName(tableName)
        .addCol("id", "int")
        .addCol("name", "string")
        .addPartCol("partcol", "string")
        .build(conf);
    table.unsetCatName();
    client.createTable(table);

    Partition[] parts = new Partition[2];
    for (int i = 0; i < parts.length; i++) {
      parts[i] = new PartitionBuilder()
          .inTable(table)
          .addValue("a" + i)
          .build(conf);
      parts[i].unsetCatName();
    }
    client.add_partitions(Arrays.asList(parts));
    List<Partition> fetched = client.listPartitions(dbName, tableName, (short)-1);
    Assert.assertEquals(parts.length, fetched.size());

    Assert.assertTrue(client.dropPartition(dbName, tableName,
        Collections.singletonList("a0"), PartitionDropOptions.instance().ifExists(false)));
    try {
      client.getPartition(dbName, tableName, Collections.singletonList("a0"));
      Assert.fail();
    } catch (NoSuchObjectException e) {
      // NOP
    }

    Assert.assertTrue(client.dropPartition(dbName, tableName, "partcol=a1", true));
    try {
      client.getPartition(dbName, tableName, Collections.singletonList("a1"));
      Assert.fail();
    } catch (NoSuchObjectException e) {
      // NOP
    }
  }

  @Test
  public void primaryKeyAndForeignKey() throws TException {
    Table parentTable = testTables[2];
    Table table = testTables[3];
    String constraintName = "othercatfk";

    // Single column unnamed primary key in default catalog and database
    List<SQLPrimaryKey> pk = new SQLPrimaryKeyBuilder()
        .onTable(parentTable)
        .addColumn("test_col1")
        .build(conf);
    client.addPrimaryKey(pk);

    List<SQLForeignKey> fk = new SQLForeignKeyBuilder()
        .fromPrimaryKey(pk)
        .onTable(table)
        .addColumn("test_col1")
        .setConstraintName(constraintName)
        .build(conf);
    client.addForeignKey(fk);

    PrimaryKeysRequest pkRqst = new PrimaryKeysRequest(parentTable.getDbName(),
        parentTable.getTableName());
    pkRqst.setCatName(parentTable.getCatName());
    List<SQLPrimaryKey> pkFetched = client.getPrimaryKeys(pkRqst);
    Assert.assertEquals(1, pkFetched.size());
    Assert.assertEquals(expectedCatalog(), pkFetched.get(0).getCatName());
    Assert.assertEquals(parentTable.getDbName(), pkFetched.get(0).getTable_db());
    Assert.assertEquals(parentTable.getTableName(), pkFetched.get(0).getTable_name());
    Assert.assertEquals("test_col1", pkFetched.get(0).getColumn_name());
    Assert.assertEquals(1, pkFetched.get(0).getKey_seq());
    Assert.assertTrue(pkFetched.get(0).isEnable_cstr());
    Assert.assertFalse(pkFetched.get(0).isValidate_cstr());
    Assert.assertFalse(pkFetched.get(0).isRely_cstr());
    Assert.assertEquals(parentTable.getCatName(), pkFetched.get(0).getCatName());

    ForeignKeysRequest rqst = new ForeignKeysRequest(parentTable.getDbName(),
        parentTable.getTableName(), table.getDbName(), table.getTableName());
    rqst.setCatName(table.getCatName());
    List<SQLForeignKey> fetched = client.getForeignKeys(rqst);
    Assert.assertEquals(1, fetched.size());
    Assert.assertEquals(table.getDbName(), fetched.get(0).getFktable_db());
    Assert.assertEquals(table.getTableName(), fetched.get(0).getFktable_name());
    Assert.assertEquals(expectedCatalog(), fetched.get(0).getCatName());
    Assert.assertEquals("test_col1", fetched.get(0).getFkcolumn_name());
    Assert.assertEquals(parentTable.getDbName(), fetched.get(0).getPktable_db());
    Assert.assertEquals(parentTable.getTableName(), fetched.get(0).getPktable_name());
    Assert.assertEquals("test_col1", fetched.get(0).getFkcolumn_name());
    Assert.assertEquals(1, fetched.get(0).getKey_seq());
    Assert.assertEquals(parentTable.getTableName() + "_primary_key", fetched.get(0).getPk_name());
    Assert.assertEquals(constraintName, fetched.get(0).getFk_name());
    String table0FkName = fetched.get(0).getFk_name();
    Assert.assertTrue(fetched.get(0).isEnable_cstr());
    Assert.assertFalse(fetched.get(0).isValidate_cstr());
    Assert.assertFalse(fetched.get(0).isRely_cstr());
    Assert.assertEquals(table.getCatName(), fetched.get(0).getCatName());

    // Drop a foreign key
    client.dropConstraint(table.getDbName(), table.getTableName(), table0FkName);
    rqst = new ForeignKeysRequest(parentTable.getDbName(), parentTable.getTableName(),
        table.getDbName(), table.getTableName());
    rqst.setCatName(table.getCatName());
    fetched = client.getForeignKeys(rqst);
    Assert.assertTrue(fetched.isEmpty());
  }

  @Test
  public void notNullConstraint() throws TException {
    String constraintName = "ocuc";
    // Table in non 'hive' catalog
    List<SQLNotNullConstraint> nn = new SQLNotNullConstraintBuilder()
        .onTable(testTables[2])
        .addColumn("test_col1")
        .setConstraintName(constraintName)
        .build(conf);
    client.addNotNullConstraint(nn);

    NotNullConstraintsRequest rqst = new NotNullConstraintsRequest(testTables[2].getCatName(),
        testTables[2].getDbName(), testTables[2].getTableName());
    List<SQLNotNullConstraint> fetched = client.getNotNullConstraints(rqst);
    Assert.assertEquals(1, fetched.size());
    Assert.assertEquals(testTables[2].getDbName(), fetched.get(0).getTable_db());
    Assert.assertEquals(testTables[2].getTableName(), fetched.get(0).getTable_name());
    Assert.assertEquals("test_col1", fetched.get(0).getColumn_name());
    Assert.assertEquals(constraintName, fetched.get(0).getNn_name());
    Assert.assertTrue(fetched.get(0).isEnable_cstr());
    Assert.assertFalse(fetched.get(0).isValidate_cstr());
    Assert.assertFalse(fetched.get(0).isRely_cstr());
    Assert.assertEquals(testTables[2].getCatName(), fetched.get(0).getCatName());

    client.dropConstraint(testTables[2].getDbName(), testTables[2].getTableName(), constraintName);
    rqst = new NotNullConstraintsRequest(testTables[2].getCatName(), testTables[2].getDbName(),
        testTables[2].getTableName());
    fetched = client.getNotNullConstraints(rqst);
    Assert.assertTrue(fetched.isEmpty());
  }

  @Test
  public void uniqueConstraint() throws TException {
    String constraintName = "ocuc";
    // Table in non 'hive' catalog
    List<SQLUniqueConstraint> uc = new SQLUniqueConstraintBuilder()
        .onTable(testTables[2])
        .addColumn("test_col1")
        .setConstraintName(constraintName)
        .build(conf);
    client.addUniqueConstraint(uc);

    UniqueConstraintsRequest rqst = new UniqueConstraintsRequest(testTables[2].getCatName(),
        testTables[2].getDbName(), testTables[2].getTableName());
    List<SQLUniqueConstraint> fetched = client.getUniqueConstraints(rqst);
    Assert.assertEquals(1, fetched.size());
    Assert.assertEquals(testTables[2].getDbName(), fetched.get(0).getTable_db());
    Assert.assertEquals(testTables[2].getTableName(), fetched.get(0).getTable_name());
    Assert.assertEquals("test_col1", fetched.get(0).getColumn_name());
    Assert.assertEquals(1, fetched.get(0).getKey_seq());
    Assert.assertEquals(constraintName, fetched.get(0).getUk_name());
    Assert.assertTrue(fetched.get(0).isEnable_cstr());
    Assert.assertFalse(fetched.get(0).isValidate_cstr());
    Assert.assertFalse(fetched.get(0).isRely_cstr());
    Assert.assertEquals(testTables[2].getCatName(), fetched.get(0).getCatName());
    Assert.assertEquals(expectedCatalog(), fetched.get(0).getCatName());

    client.dropConstraint(testTables[2].getDbName(), testTables[2].getTableName(), constraintName);
    rqst = new UniqueConstraintsRequest(testTables[2].getCatName(), testTables[2].getDbName(),
        testTables[2].getTableName());
    fetched = client.getUniqueConstraints(rqst);
    Assert.assertTrue(fetched.isEmpty());
  }

  @Test
  public void defaultConstraints() throws TException {
    String constraintName = "ocdv";
    // Table in non 'hive' catalog
    List<SQLDefaultConstraint> dv = new SQLDefaultConstraintBuilder()
        .onTable(testTables[2])
        .addColumn("test_col1")
        .setConstraintName(constraintName)
        .setDefaultVal("empty")
        .build(conf);
    client.addDefaultConstraint(dv);

    DefaultConstraintsRequest rqst = new DefaultConstraintsRequest(testTables[2].getCatName(),
        testTables[2].getDbName(), testTables[2].getTableName());
    List<SQLDefaultConstraint> fetched = client.getDefaultConstraints(rqst);
    Assert.assertEquals(1, fetched.size());
    Assert.assertEquals(expectedCatalog(), fetched.get(0).getCatName());
    Assert.assertEquals(testTables[2].getDbName(), fetched.get(0).getTable_db());
    Assert.assertEquals(testTables[2].getTableName(), fetched.get(0).getTable_name());
    Assert.assertEquals("test_col1", fetched.get(0).getColumn_name());
    Assert.assertEquals("empty", fetched.get(0).getDefault_value());
    Assert.assertEquals(constraintName, fetched.get(0).getDc_name());
    Assert.assertTrue(fetched.get(0).isEnable_cstr());
    Assert.assertFalse(fetched.get(0).isValidate_cstr());
    Assert.assertFalse(fetched.get(0).isRely_cstr());
    Assert.assertEquals(testTables[2].getCatName(), fetched.get(0).getCatName());

    client.dropConstraint(testTables[2].getDbName(), testTables[2].getTableName(), constraintName);
    rqst = new DefaultConstraintsRequest(testTables[2].getCatName(), testTables[2].getDbName(),
        testTables[2].getTableName());
    fetched = client.getDefaultConstraints(rqst);
    Assert.assertTrue(fetched.isEmpty());
  }

  @Test
  public void createTableWithConstraints() throws TException {
    Table parentTable = testTables[2];


    Table table = new TableBuilder()
        .setTableName("table_in_other_catalog_with_constraints")
        .addCol("col1", "int")
        .addCol("col2", "varchar(32)")
        .addCol("col3", "int")
        .addCol("col4", "int")
        .addCol("col5", "int")
        .addCol("col6", "int")
        .build(conf);
    table.unsetCatName();

    List<SQLPrimaryKey> parentPk = new SQLPrimaryKeyBuilder()
        .onTable(parentTable)
        .addColumn("test_col1")
        .build(conf);
    for (SQLPrimaryKey pkcol : parentPk) {
      pkcol.unsetCatName();
    }
    client.addPrimaryKey(parentPk);

    List<SQLPrimaryKey> pk = new SQLPrimaryKeyBuilder()
        .onTable(table)
        .addColumn("col2")
        .build(conf);
    for (SQLPrimaryKey pkcol : pk) {
      pkcol.unsetCatName();
    }

    List<SQLForeignKey> fk = new SQLForeignKeyBuilder()
        .fromPrimaryKey(parentPk)
        .onTable(table)
        .addColumn("col1")
        .build(conf);
    for (SQLForeignKey fkcol : fk) {
      fkcol.unsetCatName();
    }

    List<SQLDefaultConstraint> dv = new SQLDefaultConstraintBuilder()
        .onTable(table)
        .addColumn("col3")
        .setDefaultVal(0)
        .build(conf);
    for (SQLDefaultConstraint dccol : dv) {
      dccol.unsetCatName();
    }

    List<SQLNotNullConstraint> nn = new SQLNotNullConstraintBuilder()
        .onTable(table)
        .addColumn("col4")
        .build(conf);
    for (SQLNotNullConstraint nncol : nn) {
      nncol.unsetCatName();
    }

    List<SQLUniqueConstraint> uc = new SQLUniqueConstraintBuilder()
        .onTable(table)
        .addColumn("col5")
        .build(conf);
    for (SQLUniqueConstraint uccol : uc) {
      uccol.unsetCatName();
    }

    List<SQLCheckConstraint> cc = new SQLCheckConstraintBuilder()
        .onTable(table)
        .addColumn("col6")
        .setCheckExpression("> 0")
        .build(conf);
    for (SQLCheckConstraint cccol : cc) {
      cccol.unsetCatName();
    }

    client.createTableWithConstraints(table, pk, fk, uc, nn, dv, cc);

    PrimaryKeysRequest pkRqst = new PrimaryKeysRequest(parentTable.getDbName(),
        parentTable.getTableName());
    pkRqst.setCatName(parentTable.getCatName());
    List<SQLPrimaryKey> pkFetched = client.getPrimaryKeys(pkRqst);
    Assert.assertEquals(1, pkFetched.size());
    Assert.assertEquals(expectedCatalog(), pkFetched.get(0).getCatName());
    Assert.assertEquals(parentTable.getDbName(), pkFetched.get(0).getTable_db());
    Assert.assertEquals(parentTable.getTableName(), pkFetched.get(0).getTable_name());
    Assert.assertEquals("test_col1", pkFetched.get(0).getColumn_name());
    Assert.assertEquals(1, pkFetched.get(0).getKey_seq());
    Assert.assertTrue(pkFetched.get(0).isEnable_cstr());
    Assert.assertFalse(pkFetched.get(0).isValidate_cstr());
    Assert.assertFalse(pkFetched.get(0).isRely_cstr());
    Assert.assertEquals(parentTable.getCatName(), pkFetched.get(0).getCatName());

    ForeignKeysRequest fkRqst = new ForeignKeysRequest(parentTable.getDbName(), parentTable
        .getTableName(),
        table.getDbName(), table.getTableName());
    fkRqst.setCatName(table.getCatName());
    List<SQLForeignKey> fkFetched = client.getForeignKeys(fkRqst);
    Assert.assertEquals(1, fkFetched.size());
    Assert.assertEquals(expectedCatalog(), fkFetched.get(0).getCatName());
    Assert.assertEquals(table.getDbName(), fkFetched.get(0).getFktable_db());
    Assert.assertEquals(table.getTableName(), fkFetched.get(0).getFktable_name());
    Assert.assertEquals("col1", fkFetched.get(0).getFkcolumn_name());
    Assert.assertEquals(parentTable.getDbName(), fkFetched.get(0).getPktable_db());
    Assert.assertEquals(parentTable.getTableName(), fkFetched.get(0).getPktable_name());
    Assert.assertEquals(1, fkFetched.get(0).getKey_seq());
    Assert.assertEquals(parentTable.getTableName() + "_primary_key", fkFetched.get(0).getPk_name());
    Assert.assertTrue(fkFetched.get(0).isEnable_cstr());
    Assert.assertFalse(fkFetched.get(0).isValidate_cstr());
    Assert.assertFalse(fkFetched.get(0).isRely_cstr());
    Assert.assertEquals(table.getCatName(), fkFetched.get(0).getCatName());

    NotNullConstraintsRequest nnRqst = new NotNullConstraintsRequest(table.getCatName(),
        table.getDbName(), table.getTableName());
    List<SQLNotNullConstraint> nnFetched = client.getNotNullConstraints(nnRqst);
    Assert.assertEquals(1, nnFetched.size());
    Assert.assertEquals(table.getDbName(), nnFetched.get(0).getTable_db());
    Assert.assertEquals(table.getTableName(), nnFetched.get(0).getTable_name());
    Assert.assertEquals("col4", nnFetched.get(0).getColumn_name());
    Assert.assertEquals(table.getTableName() + "_not_null_constraint", nnFetched.get(0).getNn_name());
    Assert.assertTrue(nnFetched.get(0).isEnable_cstr());
    Assert.assertFalse(nnFetched.get(0).isValidate_cstr());
    Assert.assertFalse(nnFetched.get(0).isRely_cstr());
    Assert.assertEquals(table.getCatName(), nnFetched.get(0).getCatName());

    UniqueConstraintsRequest ucRqst = new UniqueConstraintsRequest(table.getCatName(), table
        .getDbName(), table.getTableName());
    List<SQLUniqueConstraint> ucFetched = client.getUniqueConstraints(ucRqst);
    Assert.assertEquals(1, ucFetched.size());
    Assert.assertEquals(table.getDbName(), ucFetched.get(0).getTable_db());
    Assert.assertEquals(table.getTableName(), ucFetched.get(0).getTable_name());
    Assert.assertEquals("col5", ucFetched.get(0).getColumn_name());
    Assert.assertEquals(1, ucFetched.get(0).getKey_seq());
    Assert.assertEquals(table.getTableName() + "_unique_constraint", ucFetched.get(0).getUk_name());
    Assert.assertTrue(ucFetched.get(0).isEnable_cstr());
    Assert.assertFalse(ucFetched.get(0).isValidate_cstr());
    Assert.assertFalse(ucFetched.get(0).isRely_cstr());
    Assert.assertEquals(table.getCatName(), ucFetched.get(0).getCatName());

    DefaultConstraintsRequest dcRqst = new DefaultConstraintsRequest(table.getCatName(), table
        .getDbName(), table.getTableName());
    List<SQLDefaultConstraint> dcFetched = client.getDefaultConstraints(dcRqst);
    Assert.assertEquals(1, dcFetched.size());
    Assert.assertEquals(expectedCatalog(), dcFetched.get(0).getCatName());
    Assert.assertEquals(table.getDbName(), dcFetched.get(0).getTable_db());
    Assert.assertEquals(table.getTableName(), dcFetched.get(0).getTable_name());
    Assert.assertEquals("col3", dcFetched.get(0).getColumn_name());
    Assert.assertEquals("0", dcFetched.get(0).getDefault_value());
    Assert.assertEquals(table.getTableName() + "_default_value", dcFetched.get(0).getDc_name());
    Assert.assertTrue(dcFetched.get(0).isEnable_cstr());
    Assert.assertFalse(dcFetched.get(0).isValidate_cstr());
    Assert.assertFalse(dcFetched.get(0).isRely_cstr());
    Assert.assertEquals(table.getCatName(), dcFetched.get(0).getCatName());

    CheckConstraintsRequest ccRqst = new CheckConstraintsRequest(table.getCatName(), table
        .getDbName(), table.getTableName());
    List<SQLCheckConstraint> ccFetched = client.getCheckConstraints(ccRqst);
    Assert.assertEquals(1, ccFetched.size());
    Assert.assertEquals(expectedCatalog(), ccFetched.get(0).getCatName());
    Assert.assertEquals(table.getDbName(), ccFetched.get(0).getTable_db());
    Assert.assertEquals(table.getTableName(), ccFetched.get(0).getTable_name());
    Assert.assertEquals("col6", ccFetched.get(0).getColumn_name());
    Assert.assertEquals("> 0", ccFetched.get(0).getCheck_expression());
    Assert.assertEquals(table.getTableName() + "_check_constraint", ccFetched.get(0).getDc_name());
    Assert.assertTrue(ccFetched.get(0).isEnable_cstr());
    Assert.assertFalse(ccFetched.get(0).isValidate_cstr());
    Assert.assertFalse(ccFetched.get(0).isRely_cstr());
    Assert.assertEquals(table.getCatName(), ccFetched.get(0).getCatName());
  }

  @Test
  public void functions() throws TException {
    String dbName = "functions_other_catalog_db";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .build(conf);
    db.unsetCatalogName();
    client.createDatabase(db);

    String functionName = "test_function";
    Function function =
        new FunctionBuilder()
            .inDb(db)
            .setName(functionName)
            .setClass(TEST_FUNCTION_CLASS)
            .setFunctionType(FunctionType.JAVA)
            .setOwnerType(PrincipalType.ROLE)
            .setOwner("owner")
            .addResourceUri(new ResourceUri(ResourceType.JAR, "hdfs:///tmp/jar1.jar"))
            .addResourceUri(new ResourceUri(ResourceType.FILE, "hdfs:///tmp/file1.txt"))
            .addResourceUri(new ResourceUri(ResourceType.ARCHIVE, "hdfs:///tmp/archive1.tgz"))
            .build(conf);
    function.unsetCatName();
    client.createFunction(function);

    Function createdFunction = client.getFunction(dbName, functionName);
    // Creation time will be set by server and not us.
    Assert.assertEquals(function.getFunctionName(), createdFunction.getFunctionName());
    Assert.assertEquals(function.getDbName(), createdFunction.getDbName());
    Assert.assertEquals(expectedCatalog(), createdFunction.getCatName());
    Assert.assertEquals(function.getClassName(), createdFunction.getClassName());
    Assert.assertEquals(function.getOwnerName(), createdFunction.getOwnerName());
    Assert.assertEquals(function.getOwnerType(), createdFunction.getOwnerType());
    Assert.assertEquals(function.getFunctionType(), createdFunction.getFunctionType());
    Assert.assertEquals(function.getResourceUris(), createdFunction.getResourceUris());

    String f2Name = "testy_function2";
    Function f2 = new FunctionBuilder()
        .inDb(db)
        .setName(f2Name)
        .setClass(TEST_FUNCTION_CLASS)
        .build(conf);
    f2.unsetCatName();
    client.createFunction(f2);

    Set<String> functions = new HashSet<>(client.getFunctions(dbName, "test*"));
    Assert.assertEquals(2, functions.size());
    Assert.assertTrue(functions.contains(functionName));
    Assert.assertTrue(functions.contains(f2Name));

    functions = new HashSet<>(client.getFunctions(dbName, "test_*"));
    Assert.assertEquals(1, functions.size());
    Assert.assertTrue(functions.contains(functionName));
    Assert.assertFalse(functions.contains(f2Name));

    client.dropFunction(function.getDbName(), function.getFunctionName());
    try {
      client.getFunction(function.getDbName(), function.getFunctionName());
      Assert.fail("Expected a NoSuchObjectException to be thrown");
    } catch (NoSuchObjectException exception) {
      // Expected exception
    }
  }
}
