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

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(MetastoreUnitTest.class)
public class TestPartitionManagement {
  private IMetaStoreClient client;
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    conf.setClass(MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS.getVarname(),
      MsckPartitionExpressionProxy.class, PartitionExpressionProxy.class);
    MetastoreConf.setVar(conf, ConfVars.METASTORE_METADATA_TRANSFORMER_CLASS, " ");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    conf.setBoolean(ConfVars.MULTITHREADED.getVarname(), false);
    conf.setBoolean(ConfVars.HIVE_IN_TEST.getVarname(), true);
    MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
    TestTxnDbUtil.setConfValues(conf);
    TestTxnDbUtil.prepDb(conf);
    client = new HiveMetaStoreClient(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (client != null) {
      // Drop any left over catalogs
      List<String> catalogs = client.getCatalogs();
      for (String catName : catalogs) {
        if (!catName.equalsIgnoreCase(DEFAULT_CATALOG_NAME)) {
          // First drop any databases in catalog
          List<String> databases = client.getAllDatabases(catName);
          for (String db : databases) {
            client.dropDatabase(catName, db, true, false, true);
          }
          client.dropCatalog(catName);
        } else {
          List<String> databases = client.getAllDatabases(catName);
          for (String db : databases) {
            if (!db.equalsIgnoreCase(Warehouse.DEFAULT_DATABASE_NAME)) {
              client.dropDatabase(catName, db, true, false, true);
            }
          }
        }
      }
    }
    try {
      if (client != null) {
        client.close();
      }
    } finally {
      client = null;
    }
  }

  private Map<String, Column> buildAllColumns() {
    Map<String, Column> colMap = new HashMap<>(6);
    Column[] cols = {new Column("b", "binary"), new Column("bo", "boolean"),
      new Column("d", "date"), new Column("do", "double"), new Column("l", "bigint"),
      new Column("s", "string")};
    for (Column c : cols) {
      colMap.put(c.colName, c);
    }
    return colMap;
  }

  private List<String> createMetadata(String catName, String dbName, String tableName,
    List<String> partKeys, List<String> partKeyTypes, List<List<String>> partVals,
    Map<String, Column> colMap, boolean isOrc)
    throws TException {
    if (!DEFAULT_CATALOG_NAME.equals(catName)) {
      Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .build();
      client.createCatalog(cat);
    }

    Database db;
    if (!DEFAULT_DATABASE_NAME.equals(dbName)) {
      DatabaseBuilder dbBuilder = new DatabaseBuilder()
        .setName(dbName);
      dbBuilder.setCatalogName(catName);
      db = dbBuilder.create(client, conf);
    } else {
      db = client.getDatabase(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME);
    }

    TableBuilder tb = new TableBuilder()
      .inDb(db)
      .setTableName(tableName);

    if (isOrc) {
      tb.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
        .setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
    }

    for (Column col : colMap.values()) {
      tb.addCol(col.colName, col.colType);
    }

    if (partKeys != null) {
      if (partKeyTypes == null) {
        throw new IllegalArgumentException("partKeyTypes cannot be null when partKeys is non-null");
      }
      if (partKeys.size() != partKeyTypes.size()) {
        throw new IllegalArgumentException("partKeys and partKeyTypes size should be same");
      }
      if (partVals.isEmpty()) {
        throw new IllegalArgumentException("partVals cannot be empty for patitioned table");
      }
      for (int i = 0; i < partKeys.size(); i++) {
        tb.addPartCol(partKeys.get(i), partKeyTypes.get(i));
      }
    }
    Table table = tb.create(client, conf);

    if (partKeys != null) {
      for (List<String> partVal : partVals) {
        new PartitionBuilder()
          .inTable(table)
          .setValues(partVal)
          .addToTable(client, conf);
      }
    }

    List<String> partNames = new ArrayList<>();
    if (partKeys != null) {
      for (int i = 0; i < partKeys.size(); i++) {
        String partKey = partKeys.get(i);
        for (String partVal : partVals.get(i)) {
          String partName = partKey + "=" + partVal;
          partNames.add(partName);
        }
      }
    }
    client.flushCache();
    return partNames;
  }

  @Test
  public void testPartitionDiscoveryDisabledByDefault() throws TException, IOException {
    String dbName = "db1";
    String tableName = "tbl1";
    Map<String, Column> colMap = buildAllColumns();
    List<String> partKeys = Lists.newArrayList("state", "dt");
    List<String> partKeyTypes = Lists.newArrayList("string", "date");
    List<List<String>> partVals = Lists.newArrayList(
      Lists.newArrayList("__HIVE_DEFAULT_PARTITION__", "1990-01-01"),
      Lists.newArrayList("CA", "1986-04-28"),
      Lists.newArrayList("MN", "2018-11-31"));
    createMetadata(DEFAULT_CATALOG_NAME, dbName, tableName, partKeys, partKeyTypes, partVals, colMap, false);
    Table table = client.getTable(dbName, tableName);
    List<Partition> partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());
    String tableLocation = table.getSd().getLocation();
    URI location = URI.create(tableLocation);
    Path tablePath = new Path(location);
    FileSystem fs = FileSystem.get(location, conf);
    fs.mkdirs(new Path(tablePath, "state=WA/dt=2018-12-01"));
    fs.mkdirs(new Path(tablePath, "state=UT/dt=2018-12-02"));
    assertEquals(5, fs.listStatus(tablePath).length);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());

    // partition discovery is not enabled via table property, so nothing should change on this table
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());

    // table property is set to false, so no change expected
    table.getParameters().put(PartitionManagementTask.DISCOVER_PARTITIONS_TBLPROPERTY, "false");
    client.alter_table(dbName, tableName, table);
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());
  }

  @Test
  public void testPartitionDiscoveryEnabledBothTableTypes() throws TException, IOException {
    String dbName = "db2";
    String tableName = "tbl2";
    Map<String, Column> colMap = buildAllColumns();
    List<String> partKeys = Lists.newArrayList("state", "dt");
    List<String> partKeyTypes = Lists.newArrayList("string", "date");
    List<List<String>> partVals = Lists.newArrayList(
      Lists.newArrayList("__HIVE_DEFAULT_PARTITION__", "1990-01-01"),
      Lists.newArrayList("CA", "1986-04-28"),
      Lists.newArrayList("MN", "2018-11-31"));
    createMetadata(DEFAULT_CATALOG_NAME, dbName, tableName, partKeys, partKeyTypes, partVals, colMap, false);
    Table table = client.getTable(dbName, tableName);
    List<Partition> partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());
    String tableLocation = table.getSd().getLocation();
    URI location = URI.create(tableLocation);
    Path tablePath = new Path(location);
    FileSystem fs = FileSystem.get(location, conf);
    Path newPart1 = new Path(tablePath, "state=WA/dt=2018-12-01");
    Path newPart2 = new Path(tablePath, "state=UT/dt=2018-12-02");
    fs.mkdirs(newPart1);
    fs.mkdirs(newPart2);
    assertEquals(5, fs.listStatus(tablePath).length);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());

    // table property is set to true, we expect 5 partitions
    table.getParameters().put(PartitionManagementTask.DISCOVER_PARTITIONS_TBLPROPERTY, "true");
    client.alter_table(dbName, tableName, table);
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(5, partitions.size());

    // change table type to external, delete a partition directory and make sure partition discovery works
    table.getParameters().put("EXTERNAL", "true");
    table.setTableType(TableType.EXTERNAL_TABLE.name());
    client.alter_table(dbName, tableName, table);
    boolean deleted = fs.delete(newPart1.getParent(), true);
    assertTrue(deleted);
    assertEquals(4, fs.listStatus(tablePath).length);
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(4, partitions.size());

    // remove external tables from partition discovery and expect no changes even after partition is deleted
    conf.set(MetastoreConf.ConfVars.PARTITION_MANAGEMENT_TABLE_TYPES.getVarname(), TableType.MANAGED_TABLE.name());
    deleted = fs.delete(newPart2.getParent(), true);
    assertTrue(deleted);
    assertEquals(3, fs.listStatus(tablePath).length);
    // this doesn't remove partition because table is still external and we have remove external table type from
    // partition discovery
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(4, partitions.size());

    // no table types specified, msck will not select any tables
    conf.set(MetastoreConf.ConfVars.PARTITION_MANAGEMENT_TABLE_TYPES.getVarname(), "");
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(4, partitions.size());

    // only EXTERNAL table type, msck should drop a partition now
    conf.set(MetastoreConf.ConfVars.PARTITION_MANAGEMENT_TABLE_TYPES.getVarname(), TableType.EXTERNAL_TABLE.name());
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());

    // only MANAGED table type
    conf.set(MetastoreConf.ConfVars.PARTITION_MANAGEMENT_TABLE_TYPES.getVarname(), TableType.MANAGED_TABLE.name());
    table.getParameters().remove("EXTERNAL");
    table.setTableType(TableType.MANAGED_TABLE.name());
    client.alter_table(dbName, tableName, table);
    Assert.assertTrue(fs.mkdirs(newPart1));
    Assert.assertTrue(fs.mkdirs(newPart2));
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(5, partitions.size());
    Assert.assertTrue(fs.delete(newPart1, true));
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(4, partitions.size());
  }

  @Test
  public void testPartitionDiscoveryNonDefaultCatalog() throws TException, IOException {
    String catName = "cat3";
    String dbName = "db3";
    String tableName = "tbl3";
    Map<String, Column> colMap = buildAllColumns();
    List<String> partKeys = Lists.newArrayList("state", "dt");
    List<String> partKeyTypes = Lists.newArrayList("string", "date");
    List<List<String>> partVals = Lists.newArrayList(
      Lists.newArrayList("__HIVE_DEFAULT_PARTITION__", "1990-01-01"),
      Lists.newArrayList("CA", "1986-04-28"),
      Lists.newArrayList("MN", "2018-11-31"));
    createMetadata(catName, dbName, tableName, partKeys, partKeyTypes, partVals, colMap, false);
    Table table = client.getTable(catName, dbName, tableName);
    List<Partition> partitions = client.listPartitions(catName, dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());
    String tableLocation = table.getSd().getLocation();
    URI location = URI.create(tableLocation);
    Path tablePath = new Path(location);
    FileSystem fs = FileSystem.get(location, conf);
    Path newPart1 = new Path(tablePath, "state=WA/dt=2018-12-01");
    Path newPart2 = new Path(tablePath, "state=UT/dt=2018-12-02");
    fs.mkdirs(newPart1);
    fs.mkdirs(newPart2);
    assertEquals(5, fs.listStatus(tablePath).length);
    table.getParameters().put(PartitionManagementTask.DISCOVER_PARTITIONS_TBLPROPERTY, "true");
    client.alter_table(catName, dbName, tableName, table);
    // default catalog in conf is 'hive' but we are using 'cat3' as catName for this test, so msck should not fix
    // anything for this one
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(catName, dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());

    // using the correct catalog name, we expect msck to fix partitions
    conf.set(MetastoreConf.ConfVars.PARTITION_MANAGEMENT_CATALOG_NAME.getVarname(), catName);
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(catName, dbName, tableName, (short) -1);
    assertEquals(5, partitions.size());
  }

  @Test
  public void testPartitionDiscoveryDBPattern() throws TException, IOException {
    String dbName = "db4";
    String tableName = "tbl4";
    Map<String, Column> colMap = buildAllColumns();
    List<String> partKeys = Lists.newArrayList("state", "dt");
    List<String> partKeyTypes = Lists.newArrayList("string", "date");
    List<List<String>> partVals = Lists.newArrayList(
      Lists.newArrayList("__HIVE_DEFAULT_PARTITION__", "1990-01-01"),
      Lists.newArrayList("CA", "1986-04-28"),
      Lists.newArrayList("MN", "2018-11-31"));
    createMetadata(DEFAULT_CATALOG_NAME, dbName, tableName, partKeys, partKeyTypes, partVals, colMap, false);
    Table table = client.getTable(dbName, tableName);
    List<Partition> partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());
    String tableLocation = table.getSd().getLocation();
    URI location = URI.create(tableLocation);
    Path tablePath = new Path(location);
    FileSystem fs = FileSystem.get(location, conf);
    Path newPart1 = new Path(tablePath, "state=WA/dt=2018-12-01");
    Path newPart2 = new Path(tablePath, "state=UT/dt=2018-12-02");
    fs.mkdirs(newPart1);
    fs.mkdirs(newPart2);
    assertEquals(5, fs.listStatus(tablePath).length);
    table.getParameters().put(PartitionManagementTask.DISCOVER_PARTITIONS_TBLPROPERTY, "true");
    client.alter_table(dbName, tableName, table);
    // no match for this db pattern, so we will see only 3 partitions
    conf.set(MetastoreConf.ConfVars.PARTITION_MANAGEMENT_DATABASE_PATTERN.getVarname(), "*dbfoo*");
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());

    // matching db pattern, we will see all 5 partitions now
    conf.set(MetastoreConf.ConfVars.PARTITION_MANAGEMENT_DATABASE_PATTERN.getVarname(), "*db4*");
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(5, partitions.size());

    fs.mkdirs(new Path(tablePath, "state=MG/dt=2021-28-05"));
    assertEquals(6, fs.listStatus(tablePath).length);
    Database db = client.getDatabase(table.getDbName());
    //PartitionManagementTask would not run for the database which is being failed over.
    db.putToParameters(ReplConst.REPL_FAILOVER_ENDPOINT, MetaStoreUtils.FailoverEndpoint.SOURCE.toString());
    client.alterDatabase(dbName, db);
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(5, partitions.size());
  }

  @Test
  public void testPartitionDiscoveryTablePattern() throws TException, IOException {
    String dbName = "db5";
    String tableName = "tbl5";
    Map<String, Column> colMap = buildAllColumns();
    List<String> partKeys = Lists.newArrayList("state", "dt");
    List<String> partKeyTypes = Lists.newArrayList("string", "date");
    List<List<String>> partVals = Lists.newArrayList(
      Lists.newArrayList("__HIVE_DEFAULT_PARTITION__", "1990-01-01"),
      Lists.newArrayList("CA", "1986-04-28"),
      Lists.newArrayList("MN", "2018-11-31"));
    createMetadata(DEFAULT_CATALOG_NAME, dbName, tableName, partKeys, partKeyTypes, partVals, colMap, false);
    Table table = client.getTable(dbName, tableName);
    List<Partition> partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());
    String tableLocation = table.getSd().getLocation();
    URI location = URI.create(tableLocation);
    Path tablePath = new Path(location);
    FileSystem fs = FileSystem.get(location, conf);
    Path newPart1 = new Path(tablePath, "state=WA/dt=2018-12-01");
    Path newPart2 = new Path(tablePath, "state=UT/dt=2018-12-02");
    fs.mkdirs(newPart1);
    fs.mkdirs(newPart2);
    assertEquals(5, fs.listStatus(tablePath).length);
    table.getParameters().put(PartitionManagementTask.DISCOVER_PARTITIONS_TBLPROPERTY, "true");
    client.alter_table(dbName, tableName, table);
    // no match for this table pattern, so we will see only 3 partitions
    conf.set(MetastoreConf.ConfVars.PARTITION_MANAGEMENT_TABLE_PATTERN.getVarname(), "*tblfoo*");
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());

    // matching table pattern, we will see all 5 partitions now
    conf.set(MetastoreConf.ConfVars.PARTITION_MANAGEMENT_TABLE_PATTERN.getVarname(), "tbl5*");
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(5, partitions.size());
  }

  @Test
  public void testPartitionDiscoveryTransactionalTable()
    throws TException, IOException, InterruptedException, ExecutionException {
    String dbName = "db6";
    String tableName = "tbl6";
    Map<String, Column> colMap = buildAllColumns();
    List<String> partKeys = Lists.newArrayList("state", "dt");
    List<String> partKeyTypes = Lists.newArrayList("string", "date");
    List<List<String>> partVals = Lists.newArrayList(
      Lists.newArrayList("__HIVE_DEFAULT_PARTITION__", "1990-01-01"),
      Lists.newArrayList("CA", "1986-04-28"),
      Lists.newArrayList("MN", "2018-11-31"));
    createMetadata(DEFAULT_CATALOG_NAME, dbName, tableName, partKeys, partKeyTypes, partVals, colMap, true);
    Table table = client.getTable(dbName, tableName);
    List<Partition> partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());
    String tableLocation = table.getSd().getLocation();
    URI location = URI.create(tableLocation);
    Path tablePath = new Path(location);
    FileSystem fs = FileSystem.get(location, conf);
    Path newPart1 = new Path(tablePath, "state=WA/dt=2018-12-01");
    Path newPart2 = new Path(tablePath, "state=UT/dt=2018-12-02");
    fs.mkdirs(newPart1);
    fs.mkdirs(newPart2);
    assertEquals(5, fs.listStatus(tablePath).length);
    table.getParameters().put(PartitionManagementTask.DISCOVER_PARTITIONS_TBLPROPERTY, "true");
    table.getParameters().put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
    table.getParameters().put(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES,
      TransactionalValidationListener.INSERTONLY_TRANSACTIONAL_PROPERTY);
    client.alter_table(dbName, tableName, table);

    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(5, partitions.size());

    // only one partition discovery task is running, there will be no skipped attempts
    assertEquals(0, PartitionManagementTask.getSkippedAttempts());

    // delete a partition from fs, and submit 3 tasks at the same time each of them trying to acquire X lock on the
    // same table, only one of them will run other attempts will be skipped
    boolean deleted = fs.delete(newPart1.getParent(), true);
    assertTrue(deleted);
    assertEquals(4, fs.listStatus(tablePath).length);

    // 3 tasks are submitted at the same time, only one will eventually lock the table and only one get to run at a time
    // This is to simulate, skipping partition discovery task attempt when previous attempt is still incomplete
    PartitionManagementTask partitionDiscoveryTask1 = new PartitionManagementTask();
    partitionDiscoveryTask1.setConf(conf);
    PartitionManagementTask partitionDiscoveryTask2 = new PartitionManagementTask();
    partitionDiscoveryTask2.setConf(conf);
    PartitionManagementTask partitionDiscoveryTask3 = new PartitionManagementTask();
    partitionDiscoveryTask3.setConf(conf);
    List<PartitionManagementTask> tasks = Lists
      .newArrayList(partitionDiscoveryTask1, partitionDiscoveryTask2, partitionDiscoveryTask3);
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    int successBefore = PartitionManagementTask.getCompletedAttempts();
    int skippedBefore = PartitionManagementTask.getSkippedAttempts();
    List<Future<?>> futures = new ArrayList<>();
    for (PartitionManagementTask task : tasks) {
      futures.add(executorService.submit(task));
    }
    for (Future<?> future : futures) {
      future.get();
    }
    int successAfter = PartitionManagementTask.getCompletedAttempts();
    int skippedAfter = PartitionManagementTask.getSkippedAttempts();
    assertEquals(1, successAfter - successBefore);
    assertEquals(2, skippedAfter - skippedBefore);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(4, partitions.size());
  }

  @Test
  public void testPartitionRetention() throws TException, IOException, InterruptedException {
    String dbName = "db7";
    String tableName = "tbl7";
    Map<String, Column> colMap = buildAllColumns();
    List<String> partKeys = Lists.newArrayList("state", "dt");
    List<String> partKeyTypes = Lists.newArrayList("string", "date");
    List<List<String>> partVals = Lists.newArrayList(
      Lists.newArrayList("__HIVE_DEFAULT_PARTITION__", "1990-01-01"),
      Lists.newArrayList("CA", "1986-04-28"),
      Lists.newArrayList("MN", "2018-11-31"));
    createMetadata(DEFAULT_CATALOG_NAME, dbName, tableName, partKeys, partKeyTypes, partVals, colMap, false);
    Table table = client.getTable(dbName, tableName);
    List<Partition> partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());
    String tableLocation = table.getSd().getLocation();
    URI location = URI.create(tableLocation);
    Path tablePath = new Path(location);
    FileSystem fs = FileSystem.get(location, conf);
    Path newPart1 = new Path(tablePath, "state=WA/dt=2018-12-01");
    Path newPart2 = new Path(tablePath, "state=UT/dt=2018-12-02");
    fs.mkdirs(newPart1);
    fs.mkdirs(newPart2);
    assertEquals(5, fs.listStatus(tablePath).length);
    table.getParameters().put(PartitionManagementTask.DISCOVER_PARTITIONS_TBLPROPERTY, "true");
    table.getParameters().put(PartitionManagementTask.PARTITION_RETENTION_PERIOD_TBLPROPERTY, "20000ms");
    client.alter_table(dbName, tableName, table);

    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(5, partitions.size());

    Database db = client.getDatabase(table.getDbName());
    db.putToParameters(ReplConst.REPL_FAILOVER_ENDPOINT, MetaStoreUtils.FailoverEndpoint.SOURCE.toString());
    client.alterDatabase(table.getDbName(), db);
    // PartitionManagementTask would not do anything because the db is being failed over.
    Thread.sleep(30 * 1000);
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(5, partitions.size());

    db.putToParameters(ReplConst.REPL_FAILOVER_ENDPOINT, "");
    client.alterDatabase(table.getDbName(), db);

    // after 30s all partitions should have been gone
    Thread.sleep(30 * 1000);
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(0, partitions.size());
  }

  @Test
  public void testPartitionDiscoverySkipInvalidPath() throws TException, IOException, InterruptedException {
    String dbName = "db8";
    String tableName = "tbl8";
    Map<String, Column> colMap = buildAllColumns();
    List<String> partKeys = Lists.newArrayList("state", "dt");
    List<String> partKeyTypes = Lists.newArrayList("string", "date");
    List<List<String>> partVals = Lists.newArrayList(
      Lists.newArrayList("__HIVE_DEFAULT_PARTITION__", "1990-01-01"),
      Lists.newArrayList("CA", "1986-04-28"),
      Lists.newArrayList("MN", "2018-11-31"));
    createMetadata(DEFAULT_CATALOG_NAME, dbName, tableName, partKeys, partKeyTypes, partVals, colMap, false);
    Table table = client.getTable(dbName, tableName);
    List<Partition> partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());
    String tableLocation = table.getSd().getLocation();
    URI location = URI.create(tableLocation);
    Path tablePath = new Path(location);
    FileSystem fs = FileSystem.get(location, conf);
    Path newPart1 = new Path(tablePath, "state=WA/dt=2018-12-01");
    Path newPart2 = new Path(tablePath, "state=UT/dt=");
    fs.mkdirs(newPart1);
    fs.mkdirs(newPart2);
    assertEquals(5, fs.listStatus(tablePath).length);
    table.getParameters().put(PartitionManagementTask.DISCOVER_PARTITIONS_TBLPROPERTY, "true");
    // empty retention period basically means disabled
    table.getParameters().put(PartitionManagementTask.PARTITION_RETENTION_PERIOD_TBLPROPERTY, "");
    client.alter_table(dbName, tableName, table);

    // there is one partition with invalid path which will get skipped
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(4, partitions.size());
  }

  @Test
  public void testNoPartitionDiscoveryForReplTable() throws Exception {
    String dbName = "db_repl1";
    String tableName = "tbl_repl1";
    Map<String, Column> colMap = buildAllColumns();
    List<String> partKeys = Lists.newArrayList("state", "dt");
    List<String> partKeyTypes = Lists.newArrayList("string", "date");
    List<List<String>> partVals = Lists.newArrayList(
            Lists.newArrayList("__HIVE_DEFAULT_PARTITION__", "1990-01-01"),
            Lists.newArrayList("CA", "1986-04-28"),
            Lists.newArrayList("MN", "2018-11-31"));
    createMetadata(DEFAULT_CATALOG_NAME, dbName, tableName, partKeys, partKeyTypes, partVals, colMap, false);
    Table table = client.getTable(dbName, tableName);
    List<Partition> partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());
    String tableLocation = table.getSd().getLocation();
    URI location = URI.create(tableLocation);
    Path tablePath = new Path(location);
    FileSystem fs = FileSystem.get(location, conf);
    Path newPart1 = new Path(tablePath, "state=WA/dt=2018-12-01");
    Path newPart2 = new Path(tablePath, "state=UT/dt=2018-12-02");
    fs.mkdirs(newPart1);
    fs.mkdirs(newPart2);
    assertEquals(5, fs.listStatus(tablePath).length);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());

    // table property is set to true, but the table is marked as replication target. The new
    // partitions should not be created
    table.getParameters().put(PartitionManagementTask.DISCOVER_PARTITIONS_TBLPROPERTY, "true");
    Database db = client.getDatabase(table.getDbName());
    db.putToParameters(ReplConst.TARGET_OF_REPLICATION, "true");
    client.alterDatabase(table.getDbName(), db);
    client.alter_table(dbName, tableName, table);
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());

    // change table type to external, delete a partition directory and make sure partition discovery works
    table.getParameters().put("EXTERNAL", "true");
    table.setTableType(TableType.EXTERNAL_TABLE.name());
    client.alter_table(dbName, tableName, table);
    // Delete location of one of the partitions. The partition discovery task should not drop
    // that partition.
    boolean deleted = fs.delete((new Path(URI.create(partitions.get(0).getSd().getLocation()))).getParent(),
                    true);
    assertTrue(deleted);
    assertEquals(4, fs.listStatus(tablePath).length);
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());

    //Check that partition Discovery works for database with repl.background.enable as true.
    db = client.getDatabase(table.getDbName());
    db.putToParameters(ReplConst.REPL_ENABLE_BACKGROUND_THREAD, ReplConst.TRUE);
    client.alterDatabase(table.getDbName(), db);
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(4, partitions.size());
  }

  @Test
  public void testNoPartitionRetentionForReplTarget() throws TException, InterruptedException {
    String dbName = "db_repl2";
    String tableName = "tbl_repl2";
    Map<String, Column> colMap = buildAllColumns();
    List<String> partKeys = Lists.newArrayList("state", "dt");
    List<String> partKeyTypes = Lists.newArrayList("string", "date");
    List<List<String>> partVals = Lists.newArrayList(
            Lists.newArrayList("__HIVE_DEFAULT_PARTITION__", "1990-01-01"),
            Lists.newArrayList("CA", "1986-04-28"),
            Lists.newArrayList("MN", "2018-11-31"));
    // Check for the existence of partitions 10 seconds after the partition retention period has
    // elapsed. Gives enough time for the partition retention task to work.
    long partitionRetentionPeriodMs = 20000;
    long waitingPeriodForTest = partitionRetentionPeriodMs + 10 * 1000;
    createMetadata(DEFAULT_CATALOG_NAME, dbName, tableName, partKeys, partKeyTypes, partVals, colMap, false);
    Table table = client.getTable(dbName, tableName);
    List<Partition> partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());

    table.getParameters().put(PartitionManagementTask.DISCOVER_PARTITIONS_TBLPROPERTY, "true");
    table.getParameters().put(PartitionManagementTask.PARTITION_RETENTION_PERIOD_TBLPROPERTY,
            partitionRetentionPeriodMs + "ms");
    client.alter_table(dbName, tableName, table);
    Database db = client.getDatabase(table.getDbName());
    db.putToParameters(ReplConst.TARGET_OF_REPLICATION, "true");
    client.alterDatabase(table.getDbName(), db);

    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());

    // after 30s all partitions should remain in-tact for a table which is target of replication.
    Thread.sleep(waitingPeriodForTest);
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());

    //Check that partition retention works for database with repl.background.enable as true.
    db = client.getDatabase(table.getDbName());
    db.putToParameters(ReplConst.REPL_ENABLE_BACKGROUND_THREAD, ReplConst.TRUE);
    client.alterDatabase(table.getDbName(), db);

    Thread.sleep(waitingPeriodForTest);
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(0, partitions.size());
  }

  @Test
  public void testPartitionExprFilter() throws TException, IOException {
    String dbName = "db10";
    String tableName = "tbl10";
    Map<String, Column> colMap = buildAllColumns();
    List<String> partKeys = Lists.newArrayList("state", "dt", "modts");
    List<String> partKeyTypes = Lists.newArrayList("string", "date", "timestamp");

    List<List<String>> partVals = Lists.newArrayList(
        Lists.newArrayList("__HIVE_DEFAULT_PARTITION__", "1990-01-01", "__HIVE_DEFAULT_PARTITION__"),
        Lists.newArrayList("CA", "1986-04-28", "2020-02-21 08:30:01"),
        Lists.newArrayList("MN", "2018-11-31", "2020-02-21 08:19:01"));
    createMetadata(DEFAULT_CATALOG_NAME, dbName, tableName, partKeys, partKeyTypes, partVals, colMap, false);
    Table table = client.getTable(dbName, tableName);

    table.getParameters().put(PartitionManagementTask.DISCOVER_PARTITIONS_TBLPROPERTY, "true");
    table.getParameters().put("EXTERNAL", "true");
    table.setTableType(TableType.EXTERNAL_TABLE.name());
    client.alter_table(dbName, tableName, table);

    List<Partition> partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(3, partitions.size());

    String tableLocation = table.getSd().getLocation();
    URI location = URI.create(tableLocation);
    Path tablePath = new Path(location);
    FileSystem fs = FileSystem.get(location, conf);
    String partPath = partitions.get(1).getSd().getLocation();
    Path newPart1 = new Path(tablePath, partPath);
    fs.delete(newPart1);

    conf.set(MetastoreConf.ConfVars.PARTITION_MANAGEMENT_DATABASE_PATTERN.getVarname(), "*db10*");
    conf.set(ConfVars.PARTITION_MANAGEMENT_TABLE_TYPES.getVarname(), TableType.EXTERNAL_TABLE.name());
    runPartitionManagementTask(conf);
    partitions = client.listPartitions(dbName, tableName, (short) -1);
    assertEquals(2, partitions.size());
  }

  private void runPartitionManagementTask(Configuration conf) {
    PartitionManagementTask task = new PartitionManagementTask();
    task.setConf(conf);
    task.run();
  }

  private static class Column {
    private String colName;
    private String colType;

    public Column(final String colName, final String colType) {
      this.colName = colName;
      this.colType = colType;
    }
  }


}
