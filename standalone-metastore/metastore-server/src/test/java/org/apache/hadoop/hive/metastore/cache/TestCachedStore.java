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
package org.apache.hadoop.hive.metastore.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.columnstats.ColStatsBuilder;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.assertEqualStatistics;
import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.createColumnStatistics;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

/**
 * Unit tests for CachedStore
 */
@Category(MetastoreCheckinTest.class) public class TestCachedStore {
  // cs_db1
  Database db1;
  // cs_db2
  Database db2;
  // cs_db1_unptntbl1
  Table db1Utbl1;
  // cs_db1_ptntbl1
  Table db1Ptbl1;
  // cs_db2_unptntbl1
  Table db2Utbl1;
  // cs_db2_ptntbl1
  Table db2Ptbl1;
  // Partitions for cs_db1_ptntbl1 (a/1, a/2 ... e/4, e/5)
  List<Partition> db1Ptbl1Ptns;
  List<String> db1Ptbl1PtnNames;
  // Partitions for cs_db2_ptntbl1 (a/1, a/2 ... e/4, e/5)
  List<Partition> db2Ptbl1Ptns;
  List<String> db2Ptbl1PtnNames;

  @Before public void setUp() throws Exception {
    Deadline.registerIfNot(10000000);
    Deadline.startTimer("");
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    ObjectStore objectStore = new ObjectStore();
    objectStore.setConf(conf);
    // Create the 'hive' catalog
    HMSHandler.createDefaultCatalog(objectStore, new Warehouse(conf));
    // Create 2 database objects
    db1 = createDatabaseObject("cs_db1", "user1");
    objectStore.createDatabase(db1);
    db2 = createDatabaseObject("cs_db2", "user1");
    objectStore.createDatabase(db2);
    // For each database object, create one partitioned and one unpartitioned table
    db1Utbl1 = createUnpartitionedTableObject(db1);
    objectStore.createTable(db1Utbl1);
    db1Ptbl1 = createPartitionedTableObject(db1);
    objectStore.createTable(db1Ptbl1);
    db2Utbl1 = createUnpartitionedTableObject(db2);
    objectStore.createTable(db2Utbl1);
    db2Ptbl1 = createPartitionedTableObject(db2);
    objectStore.createTable(db2Ptbl1);
    // Create partitions for cs_db1's partitioned table
    db1Ptbl1Ptns = createPartitionObjects(db1Ptbl1).getPartitions();
    db1Ptbl1PtnNames = createPartitionObjects(db1Ptbl1).getPartitionNames();
    objectStore.addPartitions(db1Ptbl1.getCatName(), db1Ptbl1.getDbName(), db1Ptbl1.getTableName(), db1Ptbl1Ptns);
    // Create partitions for cs_db2's partitioned table
    db2Ptbl1Ptns = createPartitionObjects(db2Ptbl1).getPartitions();
    db2Ptbl1PtnNames = createPartitionObjects(db2Ptbl1).getPartitionNames();
    objectStore.addPartitions(db2Ptbl1.getCatName(), db2Ptbl1.getDbName(), db2Ptbl1.getTableName(), db2Ptbl1Ptns);
    objectStore.shutdown();
  }

  @After public void teardown() throws Exception {
    Deadline.startTimer("");
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    ObjectStore objectStore = new ObjectStore();
    objectStore.setConf(conf);
    for (String clg : objectStore.getCatalogs()) {
      for (String db : objectStore.getAllDatabases(clg)) {
        for (String tbl : objectStore.getAllTables(clg, db)) {
          List<String> pts = objectStore.listPartitionNames(clg, db, tbl, Short.MAX_VALUE);
          objectStore.dropPartitions(clg, db, tbl, pts);
          objectStore.dropTable(clg, db, tbl);
        }
        objectStore.dropDatabase(clg, db);
      }
      objectStore.dropCatalog(clg);
    }
    objectStore.shutdown();
    CachedStore.clearSharedCache();
  }

  /**********************************************************************************************
   * Methods that test CachedStore
   *********************************************************************************************/

  @Test public void testPrewarm() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(objectStore);
    List<String> allDatabases = cachedStore.getAllDatabases(DEFAULT_CATALOG_NAME);
    Assert.assertEquals(2, allDatabases.size());
    Assert.assertTrue(allDatabases.contains(db1.getName()));
    Assert.assertTrue(allDatabases.contains(db2.getName()));
    List<String> db1Tables = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db1.getName());
    Assert.assertEquals(2, db1Tables.size());
    Assert.assertTrue(db1Tables.contains(db1Utbl1.getTableName()));
    Assert.assertTrue(db1Tables.contains(db1Ptbl1.getTableName()));
    List<String> db2Tables = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(2, db2Tables.size());
    Assert.assertTrue(db2Tables.contains(db2Utbl1.getTableName()));
    Assert.assertTrue(db2Tables.contains(db2Ptbl1.getTableName()));
    // cs_db1_ptntbl1
    List<Partition> db1Ptbl1Partitions =
        cachedStore.getPartitions(DEFAULT_CATALOG_NAME, db1.getName(), db1Ptbl1.getTableName(), -1);
    Assert.assertEquals(25, db1Ptbl1Partitions.size());
    Deadline.startTimer("");
    List<Partition> db1Ptbl1PartitionsOS =
        objectStore.getPartitions(DEFAULT_CATALOG_NAME, db2.getName(), db1Ptbl1.getTableName(), -1);
    Assert.assertTrue(db1Ptbl1Partitions.containsAll(db1Ptbl1PartitionsOS));
    // cs_db2_ptntbl1
    List<Partition> db2Ptbl1Partitions =
        cachedStore.getPartitions(DEFAULT_CATALOG_NAME, db2.getName(), db2Ptbl1.getTableName(), -1);
    Assert.assertEquals(25, db2Ptbl1Partitions.size());
    Deadline.startTimer("");
    List<Partition> db2Ptbl1PartitionsOS =
        objectStore.getPartitions(DEFAULT_CATALOG_NAME, db2.getName(), db2Ptbl1.getTableName(), -1);
    Assert.assertTrue(db2Ptbl1Partitions.containsAll(db2Ptbl1PartitionsOS));
    cachedStore.shutdown();
  }

  @Test public void testPrewarmBlackList() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    // Don't cache tables from hive.cs_db2
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_CACHED_OBJECTS_BLACKLIST, "hive.cs_db2.*");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(objectStore);
    SharedCache sharedCache = CachedStore.getSharedCache();
    // cachedStore.getAllTables falls back to objectStore when whitelist/blacklist is set
    List<String> db1Tables = sharedCache.listCachedTableNames(DEFAULT_CATALOG_NAME, db1.getName());
    Assert.assertEquals(2, db1Tables.size());
    List<String> db2Tables = sharedCache.listCachedTableNames(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(0, db2Tables.size());
    cachedStore.shutdown();
  }

  @Test public void testPrewarmWhiteList() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    // Only cache tables from hive.cs_db1
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_CACHED_OBJECTS_WHITELIST, "hive.cs_db1.*");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(objectStore);
    SharedCache sharedCache = CachedStore.getSharedCache();
    // cachedStore.getAllTables falls back to objectStore when whitelist/blacklist is set
    List<String> db1Tables = sharedCache.listCachedTableNames(DEFAULT_CATALOG_NAME, db1.getName());
    Assert.assertEquals(2, db1Tables.size());
    List<String> db2Tables = sharedCache.listCachedTableNames(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(0, db2Tables.size());
    cachedStore.shutdown();
  }

  //@Test
  // Note: the 44Kb approximation has been determined based on trial/error.
  // If this starts failing on different env, might need another look.
  public void testPrewarmMemoryEstimation() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "44Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(objectStore);
    SharedCache sharedCache = CachedStore.getSharedCache();
    List<String> db1Tables = sharedCache.listCachedTableNames(DEFAULT_CATALOG_NAME, db1.getName());
    Assert.assertEquals(2, db1Tables.size());
    List<String> db2Tables = sharedCache.listCachedTableNames(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(0, db2Tables.size());
    cachedStore.shutdown();
  }

  @Test public void testCacheUpdate() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(objectStore);
    // Drop basedb1's unpartitioned table
    objectStore.dropTable(DEFAULT_CATALOG_NAME, db1Utbl1.getDbName(), db1Utbl1.getTableName());
    Deadline.startTimer("");
    // Drop a partitions of basedb1's partitioned table
    objectStore.dropPartitions(DEFAULT_CATALOG_NAME, db1Ptbl1.getDbName(), db1Ptbl1.getTableName(), db1Ptbl1PtnNames);
    // Update SharedCache
    updateCache(cachedStore);
    List<String> allDatabases = cachedStore.getAllDatabases(DEFAULT_CATALOG_NAME);
    Assert.assertEquals(2, allDatabases.size());
    Assert.assertTrue(allDatabases.contains(db1.getName()));
    Assert.assertTrue(allDatabases.contains(db2.getName()));
    // cs_db1_ptntbl1
    List<String> db1Tbls = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db1.getName());
    Assert.assertEquals(1, db1Tbls.size());
    Assert.assertTrue(db1Tbls.contains(db1Ptbl1.getTableName()));
    List<Partition> db1Ptns =
        cachedStore.getPartitions(DEFAULT_CATALOG_NAME, db1.getName(), db1Ptbl1.getTableName(), -1);
    Assert.assertEquals(0, db1Ptns.size());
    // cs_db2_ptntbl1
    List<String> db2Tbls = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(2, db2Tbls.size());
    Assert.assertTrue(db2Tbls.contains(db2Utbl1.getTableName()));
    Assert.assertTrue(db2Tbls.contains(db2Ptbl1.getTableName()));
    List<Partition> db2Ptns =
        cachedStore.getPartitions(DEFAULT_CATALOG_NAME, db2.getName(), db2Ptbl1.getTableName(), -1);
    Assert.assertEquals(25, db2Ptns.size());
    Deadline.startTimer("");
    List<Partition> db2PtnsOS =
        objectStore.getPartitions(DEFAULT_CATALOG_NAME, db2.getName(), db2Ptbl1.getTableName(), -1);
    Assert.assertTrue(db2Ptns.containsAll(db2PtnsOS));
    // Create a new unpartitioned table under basedb1
    Table db1Utbl2 = createUnpartitionedTableObject(db1);
    db1Utbl2.setTableName(db1.getName() + "_unptntbl2");
    objectStore.createTable(db1Utbl2);
    // Add a new partition to db1PartitionedTable
    // Create partitions for cs_db1's partitioned table
    db1Ptbl1Ptns = createPartitionObjects(db1Ptbl1).getPartitions();
    Deadline.startTimer("");
    objectStore.addPartition(db1Ptbl1Ptns.get(0));
    objectStore.addPartition(db1Ptbl1Ptns.get(1));
    objectStore.addPartition(db1Ptbl1Ptns.get(2));
    objectStore.addPartition(db1Ptbl1Ptns.get(3));
    objectStore.addPartition(db1Ptbl1Ptns.get(4));
    updateCache(cachedStore);
    allDatabases = cachedStore.getAllDatabases(DEFAULT_CATALOG_NAME);
    Assert.assertEquals(2, allDatabases.size());
    Assert.assertTrue(allDatabases.contains(db1.getName()));
    Assert.assertTrue(allDatabases.contains(db2.getName()));
    db1Tbls = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db1.getName());
    Assert.assertEquals(2, db1Tbls.size());
    Assert.assertTrue(db1Tbls.contains(db1Ptbl1.getTableName()));
    Assert.assertTrue(db1Tbls.contains(db1Utbl2.getTableName()));
    db2Tbls = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(2, db2Tbls.size());
    Assert.assertTrue(db2Tbls.contains(db2Utbl1.getTableName()));
    Assert.assertTrue(db2Tbls.contains(db2Ptbl1.getTableName()));
    // cs_db1_ptntbl1
    db1Ptns = cachedStore.getPartitions(DEFAULT_CATALOG_NAME, db1.getName(), db1Ptbl1.getTableName(), -1);
    Assert.assertEquals(5, db1Ptns.size());
    // cs_db2_ptntbl1
    db2Ptns = cachedStore.getPartitions(DEFAULT_CATALOG_NAME, db2.getName(), db2Ptbl1.getTableName(), -1);
    Assert.assertEquals(25, db2Ptns.size());
    Deadline.startTimer("");
    db2PtnsOS = objectStore.getPartitions(DEFAULT_CATALOG_NAME, db2.getName(), db2Ptbl1.getTableName(), -1);
    Assert.assertTrue(db2Ptns.containsAll(db2PtnsOS));
    // Clean up
    objectStore.dropTable(DEFAULT_CATALOG_NAME, db1Utbl2.getDbName(), db1Utbl2.getTableName());
    cachedStore.shutdown();
  }

  @Test public void testCacheUpdatePartitionColStats() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    ObjectStore objectStore = new ObjectStore();
    objectStore.setConf(conf);

    Database tpcdsdb = createDatabaseObject("tpcdsdb", "user1");
    objectStore.createDatabase(tpcdsdb);
    FieldSchema soldDateCol = new FieldSchema("ss_sold_date_sk", "int", "");
    FieldSchema customerCol = new FieldSchema("ss_customer_sk", "int", "");
    List<FieldSchema> columns = Arrays.asList(soldDateCol, customerCol);
    List<FieldSchema> partitionsColumns = Collections.singletonList(soldDateCol);
    Table salesTable =
        createTable(tpcdsdb.getName(), "store_sales", createStorageDescriptor(columns), partitionsColumns);
    objectStore.createTable(salesTable);
    MTable salesMTable = objectStore.ensureGetMTable(salesTable.getCatName(), salesTable.getDbName(), salesTable.getTableName());

    Map<String, ColumnStatisticsData> partitionStats = new HashMap<>();
    ColumnStatisticsData data1 = new ColStatsBuilder<>(long.class).numNulls(100).numDVs(50)
        .hll(1, 2, 3).kll(1, 2, 3).build();
    partitionStats.put("1999", data1);
    ColumnStatisticsData data2 = new ColStatsBuilder<>(long.class).numNulls(200).numDVs(100)
        .hll(1, 2, 3, 4).kll(1, 2, 3, 4).build();
    partitionStats.put("2000", data2);

    List<String> partNames = new ArrayList<>();
    for (Map.Entry<String, ColumnStatisticsData> pStat : partitionStats.entrySet()) {
      List<String> partitionValue = Collections.singletonList(pStat.getKey());
      Partition p = createPartition(salesTable, partitionValue);
      objectStore.addPartition(p);
      String pName = FileUtils.makePartName(Collections.singletonList(soldDateCol.getName()), partitionValue);
      partNames.add(pName);

      ColumnStatistics stats = createColumnStatistics(pStat.getValue(), salesTable, soldDateCol, pName);
      objectStore.updatePartitionColumnStatistics(salesTable, salesMTable, stats, partitionValue, null, -1);
    }

    List<ColumnStatistics> rawStats = objectStore
        .getPartitionColumnStatistics(DEFAULT_CATALOG_NAME, salesTable.getDbName(), salesTable.getTableName(), partNames,
            Collections.singletonList(soldDateCol.getName()), CacheUtils.HIVE_ENGINE);
    objectStore.shutdown();

    CachedStore cachedStore = new CachedStore();
    cachedStore.setConfForTest(conf);

    updateCache(cachedStore);

    List<ColumnStatistics> cachedStats = CachedStore.getSharedCache()
        .getPartitionColStatsListFromCache(DEFAULT_CATALOG_NAME, salesTable.getDbName(), salesTable.getTableName(), partNames,
            Collections.singletonList(soldDateCol.getName()), null, true);
    Assert.assertNotNull(rawStats);
    Assert.assertNotNull(cachedStats);
    Assert.assertEquals(rawStats.size(), cachedStats.size());
    for (int i = 0; i < rawStats.size(); i++) {
      assertLongStatsEquals(rawStats.get(i), cachedStats.get(i));
    }
    cachedStore.shutdown();
  }

  @Test public void testCreateAndGetDatabase() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    // Add a db via ObjectStore
    String dbName = "testCreateAndGetDatabase";
    String dbOwner = "user1";
    Database db = createDatabaseObject(dbName, dbOwner);
    objectStore.createDatabase(db);
    db = objectStore.getDatabase(DEFAULT_CATALOG_NAME, dbName);
    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(objectStore);
    // Read database via CachedStore
    Database dbRead = cachedStore.getDatabase(DEFAULT_CATALOG_NAME, dbName);
    Assert.assertEquals(db, dbRead);
    List<String> allDatabases = cachedStore.getAllDatabases(DEFAULT_CATALOG_NAME);
    Assert.assertEquals(3, allDatabases.size());
    // Add another db via CachedStore
    String dbName1 = "testCreateAndGetDatabase1";
    Database localDb1 = createDatabaseObject(dbName1, dbOwner);
    cachedStore.createDatabase(localDb1);
    localDb1 = cachedStore.getDatabase(DEFAULT_CATALOG_NAME, dbName1);
    // Read db via ObjectStore
    dbRead = objectStore.getDatabase(DEFAULT_CATALOG_NAME, dbName1);
    Assert.assertEquals(localDb1, dbRead);
    allDatabases = cachedStore.getAllDatabases(DEFAULT_CATALOG_NAME);
    Assert.assertEquals(4, allDatabases.size());
    // Clean up
    objectStore.dropDatabase(DEFAULT_CATALOG_NAME, dbName);
    objectStore.dropDatabase(DEFAULT_CATALOG_NAME, dbName1);
    cachedStore.shutdown();
  }

  @Test public void testDropDatabase() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    // Add a db via ObjectStore
    String dbName = "testDropDatabase";
    String dbOwner = "user1";
    Database db = createDatabaseObject(dbName, dbOwner);
    objectStore.createDatabase(db);
    db = objectStore.getDatabase(DEFAULT_CATALOG_NAME, dbName);
    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(objectStore);
    // Read database via CachedStore
    Database dbRead = cachedStore.getDatabase(DEFAULT_CATALOG_NAME, dbName);
    Assert.assertEquals(db, dbRead);
    List<String> allDatabases = cachedStore.getAllDatabases(DEFAULT_CATALOG_NAME);
    Assert.assertEquals(3, allDatabases.size());
    // Drop db via CachedStore
    cachedStore.dropDatabase(DEFAULT_CATALOG_NAME, dbName);
    // Read via ObjectStore
    allDatabases = objectStore.getAllDatabases(DEFAULT_CATALOG_NAME);
    Assert.assertEquals(2, allDatabases.size());
    // Create another db via CachedStore and drop via ObjectStore
    String dbName1 = "testDropDatabase1";
    Database localDb1 = createDatabaseObject(dbName1, dbOwner);
    cachedStore.createDatabase(localDb1);
    localDb1 = cachedStore.getDatabase(DEFAULT_CATALOG_NAME, dbName1);
    // Read db via ObjectStore
    dbRead = objectStore.getDatabase(DEFAULT_CATALOG_NAME, dbName1);
    Assert.assertEquals(localDb1, dbRead);
    allDatabases = cachedStore.getAllDatabases(DEFAULT_CATALOG_NAME);
    Assert.assertEquals(3, allDatabases.size());
    objectStore.dropDatabase(DEFAULT_CATALOG_NAME, dbName1);
    updateCache(cachedStore);
    updateCache(cachedStore);
    allDatabases = cachedStore.getAllDatabases(DEFAULT_CATALOG_NAME);
    Assert.assertEquals(2, allDatabases.size());
    cachedStore.shutdown();
  }

  @Test public void testAlterDatabase() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(objectStore);
    // Read database via CachedStore
    List<String> allDatabases = cachedStore.getAllDatabases(DEFAULT_CATALOG_NAME);
    Assert.assertEquals(2, allDatabases.size());
    // Alter the db via CachedStore (can only alter owner or parameters)
    String dbOwner = "user2";
    Database db = new Database(db1);
    db.setOwnerName(dbOwner);
    String dbName = db1.getName();
    cachedStore.alterDatabase(DEFAULT_CATALOG_NAME, dbName, db);
    db = cachedStore.getDatabase(DEFAULT_CATALOG_NAME, dbName);
    // Read db via ObjectStore
    Database dbRead = objectStore.getDatabase(DEFAULT_CATALOG_NAME, dbName);
    Assert.assertEquals(db, dbRead);
    // Alter db via ObjectStore
    dbOwner = "user3";
    db = new Database(db1);
    db.setOwnerName(dbOwner);
    objectStore.alterDatabase(DEFAULT_CATALOG_NAME, dbName, db);
    db = objectStore.getDatabase(DEFAULT_CATALOG_NAME, dbName);
    updateCache(cachedStore);
    updateCache(cachedStore);
    // Read db via CachedStore
    dbRead = cachedStore.getDatabase(DEFAULT_CATALOG_NAME, dbName);
    Assert.assertEquals(db, dbRead);
    cachedStore.shutdown();
  }

  @Test public void testCreateAndGetTable() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(objectStore);
    // Read database via CachedStore
    List<String> allDatabases = cachedStore.getAllDatabases(DEFAULT_CATALOG_NAME);
    Assert.assertEquals(2, allDatabases.size());
    List<String> db1Tables = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db1.getName());
    Assert.assertEquals(2, db1Tables.size());
    List<String> db2Tables = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(2, db2Tables.size());
    // Add a new table to db1 via CachedStore
    // Create a new unpartitioned table under db1
    Table db1Utbl2 = createUnpartitionedTableObject(db1);
    db1Utbl2.setTableName(db1.getName() + "_unptntbl2");
    cachedStore.createTable(db1Utbl2);
    db1Tables = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db1.getName());
    Assert.assertEquals(3, db1Tables.size());
    db1Utbl2 = cachedStore.getTable(DEFAULT_CATALOG_NAME, db1Utbl2.getDbName(), db1Utbl2.getTableName());
    Table tblRead = objectStore.getTable(DEFAULT_CATALOG_NAME, db1Utbl2.getDbName(), db1Utbl2.getTableName());
    Assert.assertEquals(db1Utbl2, tblRead);
    // Create a new unpartitioned table under basedb2 via ObjectStore
    Table db2Utbl2 = createUnpartitionedTableObject(db2);
    db2Utbl2.setTableName(db2.getName() + "_unptntbl2");
    objectStore.createTable(db2Utbl2);
    db2Utbl2 = objectStore.getTable(DEFAULT_CATALOG_NAME, db2Utbl2.getDbName(), db2Utbl2.getTableName());
    updateCache(cachedStore);
    db2Tables = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(3, db2Tables.size());
    tblRead = cachedStore.getTable(DEFAULT_CATALOG_NAME, db2Utbl2.getDbName(), db2Utbl2.getTableName());
    Assert.assertEquals(db2Utbl2, tblRead);
    // Clean up
    objectStore.dropTable(DEFAULT_CATALOG_NAME, db1Utbl2.getDbName(), db1Utbl2.getTableName());
    db1Utbl2 = cachedStore.getTable(DEFAULT_CATALOG_NAME, db1Utbl2.getDbName(), db1Utbl2.getTableName());
    objectStore.dropTable(DEFAULT_CATALOG_NAME, db2Utbl2.getDbName(), db2Utbl2.getTableName());
    cachedStore.shutdown();
  }

  // Note: the 44Kb approximation has been determined based on trial/error.
  // If this starts failing on different env, might need another look.
  public void testGetAllTablesPrewarmMemoryLimit() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "44Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(objectStore);
    SharedCache sharedCache = CachedStore.getSharedCache();
    List<String> db1Tables = sharedCache.listCachedTableNames(DEFAULT_CATALOG_NAME, db1.getName());
    Assert.assertEquals(2, db1Tables.size());
    List<String> db2Tables = sharedCache.listCachedTableNames(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(0, db2Tables.size());
    // The CachedStore call should fall back to ObjectStore in case of partial metadata caching
    List<String> db2TablesCS = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(2, db2TablesCS.size());
    cachedStore.shutdown();
  }

  @Test public void testGetAllTablesBlacklist() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    // Don't cache tables from hive.cs_db2
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_CACHED_OBJECTS_BLACKLIST, "hive.cs_db2.*");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(objectStore);
    SharedCache sharedCache = CachedStore.getSharedCache();
    // cachedStore.getAllTables falls back to objectStore when whitelist/blacklist is set
    List<String> db1Tables = sharedCache.listCachedTableNames(DEFAULT_CATALOG_NAME, db1.getName());
    Assert.assertEquals(2, db1Tables.size());
    List<String> db2Tables = sharedCache.listCachedTableNames(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(0, db2Tables.size());
    List<String> db2TablesCS = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(2, db2TablesCS.size());
    cachedStore.shutdown();
  }

  @Test public void testGetAllTablesWhitelist() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    // Only cache tables from hive.cs_db1
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_CACHED_OBJECTS_WHITELIST, "hive.cs_db1.*");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(objectStore);
    SharedCache sharedCache = CachedStore.getSharedCache();
    // cachedStore.getAllTables falls back to objectStore when whitelist/blacklist is set
    List<String> db1Tables = sharedCache.listCachedTableNames(DEFAULT_CATALOG_NAME, db1.getName());
    Assert.assertEquals(2, db1Tables.size());
    List<String> db2Tables = sharedCache.listCachedTableNames(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(0, db2Tables.size());
    List<String> db2TablesCS = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(2, db2TablesCS.size());
    cachedStore.shutdown();
  }

  @Test public void testGetTableByPattern() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(objectStore);
    List<String> db1Tables = cachedStore.getTables(DEFAULT_CATALOG_NAME, db1.getName(), "cs_db1.*");
    Assert.assertEquals(2, db1Tables.size());
    db1Tables = cachedStore.getTables(DEFAULT_CATALOG_NAME, db1.getName(), "cs_db1.un*");
    Assert.assertEquals(1, db1Tables.size());
    db1Tables = cachedStore.getTables(DEFAULT_CATALOG_NAME, db1.getName(), ".*tbl1");
    Assert.assertEquals(2, db1Tables.size());
    db1Tables = cachedStore.getTables(DEFAULT_CATALOG_NAME, db1.getName(), ".*tbl1", TableType.MANAGED_TABLE, 1);
    Assert.assertEquals(1, db1Tables.size());
    db1Tables = cachedStore.getTables(DEFAULT_CATALOG_NAME, db1.getName(), ".*tbl1", TableType.MANAGED_TABLE, -1);
    Assert.assertEquals(2, db1Tables.size());
    cachedStore.shutdown();
  }

  @Test public void testAlterTable() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(objectStore);
    List<String> db1Tables = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db1.getName());
    Assert.assertEquals(2, db1Tables.size());
    List<String> db2Tables = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(2, db2Tables.size());
    // Alter table db1Utbl1 via CachedStore and read via ObjectStore
    Table db1Utbl1Read = cachedStore.getTable(DEFAULT_CATALOG_NAME, db1Utbl1.getDbName(), db1Utbl1.getTableName());
    String newOwner = "newOwner";
    Table db1Utbl1ReadAlt = new Table(db1Utbl1Read);
    db1Utbl1ReadAlt.setOwner(newOwner);
    cachedStore
        .alterTable(DEFAULT_CATALOG_NAME, db1Utbl1Read.getDbName(), db1Utbl1Read.getTableName(), db1Utbl1ReadAlt, "0");
    db1Utbl1Read =
        cachedStore.getTable(DEFAULT_CATALOG_NAME, db1Utbl1ReadAlt.getDbName(), db1Utbl1ReadAlt.getTableName());
    Table db1Utbl1ReadOS =
        objectStore.getTable(DEFAULT_CATALOG_NAME, db1Utbl1ReadAlt.getDbName(), db1Utbl1ReadAlt.getTableName());
    Assert.assertEquals(db1Utbl1Read, db1Utbl1ReadOS);
    // Alter table db2Utbl1 via ObjectStore and read via CachedStore
    Table db2Utbl1Read = objectStore.getTable(DEFAULT_CATALOG_NAME, db2Utbl1.getDbName(), db2Utbl1.getTableName());
    Table db2Utbl1ReadAlt = new Table(db2Utbl1Read);
    db2Utbl1ReadAlt.setOwner(newOwner);
    objectStore
        .alterTable(DEFAULT_CATALOG_NAME, db2Utbl1Read.getDbName(), db2Utbl1Read.getTableName(), db2Utbl1ReadAlt, "0");
    updateCache(cachedStore);
    db2Utbl1Read =
        objectStore.getTable(DEFAULT_CATALOG_NAME, db2Utbl1ReadAlt.getDbName(), db2Utbl1ReadAlt.getTableName());
    Table d21Utbl1ReadCS =
        cachedStore.getTable(DEFAULT_CATALOG_NAME, db2Utbl1ReadAlt.getDbName(), db2Utbl1ReadAlt.getTableName());
    Assert.assertEquals(db2Utbl1Read, d21Utbl1ReadCS);
    cachedStore.shutdown();
  }

  @Test public void testDropTable() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(objectStore);
    List<String> db1Tables = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db1.getName());
    Assert.assertEquals(2, db1Tables.size());
    List<String> db2Tables = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(2, db2Tables.size());
    // Drop table db1Utbl1 via CachedStore and read via ObjectStore
    Table db1Utbl1Read = cachedStore.getTable(DEFAULT_CATALOG_NAME, db1Utbl1.getDbName(), db1Utbl1.getTableName());
    cachedStore.dropTable(DEFAULT_CATALOG_NAME, db1Utbl1Read.getDbName(), db1Utbl1Read.getTableName());
    db1Tables = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db1.getName());
    Assert.assertEquals(1, db1Tables.size());
    Table db1Utbl1ReadOS =
        objectStore.getTable(DEFAULT_CATALOG_NAME, db1Utbl1Read.getDbName(), db1Utbl1Read.getTableName());
    Assert.assertNull(db1Utbl1ReadOS);
    // Drop table db2Utbl1 via ObjectStore and read via CachedStore
    Table db2Utbl1Read = objectStore.getTable(DEFAULT_CATALOG_NAME, db2Utbl1.getDbName(), db2Utbl1.getTableName());
    objectStore.dropTable(DEFAULT_CATALOG_NAME, db2Utbl1Read.getDbName(), db2Utbl1Read.getTableName());
    db2Tables = objectStore.getAllTables(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(1, db2Tables.size());
    updateCache(cachedStore);
    db2Tables = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(1, db2Tables.size());
    Table db2Utbl1ReadCS =
        cachedStore.getTable(DEFAULT_CATALOG_NAME, db2Utbl1Read.getDbName(), db2Utbl1Read.getTableName());
    Assert.assertNull(db2Utbl1ReadCS);
    cachedStore.shutdown();
  }

  /**********************************************************************************************
   * Methods that test SharedCache
   *********************************************************************************************/

  @Test public void testSharedStoreDb() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    SharedCache sharedCache = CachedStore.getSharedCache();

    Database localDb1 = createDatabaseObject("db1", "user1");
    Database localDb2 = createDatabaseObject("db2", "user1");
    Database localDb3 = createDatabaseObject("db3", "user1");
    Database newDb1 = createDatabaseObject("newdb1", "user1");
    sharedCache.addDatabaseToCache(localDb1);
    sharedCache.addDatabaseToCache(localDb2);
    sharedCache.addDatabaseToCache(localDb3);
    Assert.assertEquals(3, sharedCache.getCachedDatabaseCount());
    sharedCache.alterDatabaseInCache(DEFAULT_CATALOG_NAME, "db1", newDb1);
    Assert.assertEquals(3, sharedCache.getCachedDatabaseCount());
    sharedCache.removeDatabaseFromCache(DEFAULT_CATALOG_NAME, "db2");
    Assert.assertEquals(2, sharedCache.getCachedDatabaseCount());
    List<String> dbs = sharedCache.listCachedDatabases(DEFAULT_CATALOG_NAME);
    Assert.assertEquals(2, dbs.size());
    Assert.assertTrue(dbs.contains("newdb1"));
    Assert.assertTrue(dbs.contains("db3"));
    cachedStore.shutdown();
  }

  @Test public void testSharedStoreTable() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    SharedCache sharedCache = CachedStore.getSharedCache();

    Table tbl1 = new Table();
    StorageDescriptor sd1 = new StorageDescriptor();
    List<FieldSchema> cols1 = new ArrayList<>();
    cols1.add(new FieldSchema("col1", "int", ""));
    Map<String, String> params1 = new HashMap<>();
    params1.put("key", "value");
    sd1.setCols(cols1);
    sd1.setParameters(params1);
    sd1.setLocation("loc1");
    tbl1.setSd(sd1);
    tbl1.setPartitionKeys(new ArrayList<>());

    Table tbl2 = new Table();
    StorageDescriptor sd2 = new StorageDescriptor();
    List<FieldSchema> cols2 = new ArrayList<>();
    cols2.add(new FieldSchema("col1", "int", ""));
    Map<String, String> params2 = new HashMap<>();
    params2.put("key", "value");
    sd2.setCols(cols2);
    sd2.setParameters(params2);
    sd2.setLocation("loc2");
    tbl2.setSd(sd2);
    tbl2.setPartitionKeys(new ArrayList<>());

    Table tbl3 = new Table();
    StorageDescriptor sd3 = new StorageDescriptor();
    List<FieldSchema> cols3 = new ArrayList<>();
    cols3.add(new FieldSchema("col3", "int", ""));
    Map<String, String> params3 = new HashMap<>();
    params3.put("key2", "value2");
    sd3.setCols(cols3);
    sd3.setParameters(params3);
    sd3.setLocation("loc3");
    tbl3.setSd(sd3);
    tbl3.setPartitionKeys(new ArrayList<>());

    Table newTbl1 = new Table();
    newTbl1.setDbName("db2");
    newTbl1.setTableName("tbl1");
    StorageDescriptor newSd1 = new StorageDescriptor();
    List<FieldSchema> newCols1 = new ArrayList<>();
    newCols1.add(new FieldSchema("newcol1", "int", ""));
    Map<String, String> newParams1 = new HashMap<>();
    newParams1.put("key", "value");
    newSd1.setCols(newCols1);
    newSd1.setParameters(newParams1);
    newSd1.setLocation("loc1");
    newTbl1.setSd(newSd1);
    newTbl1.setPartitionKeys(new ArrayList<>());

    sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, "db1", "tbl1", tbl1);
    sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, "db1", "tbl2", tbl2);
    sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, "db1", "tbl3", tbl3);
    sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, "db2", "tbl1", tbl1);

    Assert.assertEquals(4, sharedCache.getCachedTableCount());
    Assert.assertEquals(2, sharedCache.getSdCache().size());

    Table t = sharedCache.getTableFromCache(DEFAULT_CATALOG_NAME, "db1", "tbl1");
    Assert.assertEquals("loc1", t.getSd().getLocation());

    sharedCache.removeTableFromCache(DEFAULT_CATALOG_NAME, "db1", "tbl1");
    Assert.assertEquals(3, sharedCache.getCachedTableCount());
    Assert.assertEquals(2, sharedCache.getSdCache().size());

    sharedCache.alterTableInCache(DEFAULT_CATALOG_NAME, "db2", "tbl1", newTbl1);
    Assert.assertEquals(3, sharedCache.getCachedTableCount());
    Assert.assertEquals(3, sharedCache.getSdCache().size());

    sharedCache.removeTableFromCache(DEFAULT_CATALOG_NAME, "db1", "tbl2");
    Assert.assertEquals(2, sharedCache.getCachedTableCount());
    Assert.assertEquals(2, sharedCache.getSdCache().size());
    cachedStore.shutdown();
  }

  @Test public void testSharedStorePartition() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    SharedCache sharedCache = CachedStore.getSharedCache();
    String dbName = "db1";
    String tbl1Name = "tbl1";
    String tbl2Name = "tbl2";
    String owner = "user1";
    Database db = createDatabaseObject(dbName, owner);
    sharedCache.addDatabaseToCache(db);
    FieldSchema col1 = new FieldSchema("col1", "int", "integer column");
    FieldSchema col2 = new FieldSchema("col2", "string", "string column");
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(col1);
    cols.add(col2);
    List<FieldSchema> ptnCols = new ArrayList<>();
    StorageDescriptor sd = createStorageDescriptor(cols);
    Table tbl1 = createTable(dbName, tbl1Name, sd, ptnCols);
    sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, dbName, tbl1Name, tbl1);
    Table tbl2 = createTable(dbName, tbl2Name, sd, ptnCols);
    sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, dbName, tbl2Name, tbl2);

    Partition part1 = new Partition();
    StorageDescriptor sd1 = new StorageDescriptor();
    List<FieldSchema> cols1 = new ArrayList<>();
    cols1.add(new FieldSchema("col1", "int", ""));
    Map<String, String> params1 = new HashMap<>();
    params1.put("key", "value");
    sd1.setCols(cols1);
    sd1.setParameters(params1);
    sd1.setLocation("loc1");
    part1.setSd(sd1);
    part1.setValues(Collections.singletonList("201701"));

    Partition part2 = new Partition();
    StorageDescriptor sd2 = new StorageDescriptor();
    List<FieldSchema> cols2 = new ArrayList<>();
    cols2.add(new FieldSchema("col1", "int", ""));
    Map<String, String> params2 = new HashMap<>();
    params2.put("key", "value");
    sd2.setCols(cols2);
    sd2.setParameters(params2);
    sd2.setLocation("loc2");
    part2.setSd(sd2);
    part2.setValues(Collections.singletonList("201702"));

    Partition part3 = new Partition();
    StorageDescriptor sd3 = new StorageDescriptor();
    List<FieldSchema> cols3 = new ArrayList<>();
    cols3.add(new FieldSchema("col3", "int", ""));
    Map<String, String> params3 = new HashMap<>();
    params3.put("key2", "value2");
    sd3.setCols(cols3);
    sd3.setParameters(params3);
    sd3.setLocation("loc3");
    part3.setSd(sd3);
    part3.setValues(Collections.singletonList("201703"));

    Partition newPart1 = new Partition();
    newPart1.setDbName(dbName);
    newPart1.setTableName(tbl1Name);
    StorageDescriptor newSd1 = new StorageDescriptor();
    List<FieldSchema> newCols1 = new ArrayList<>();
    newCols1.add(new FieldSchema("newcol1", "int", ""));
    Map<String, String> newParams1 = new HashMap<>();
    newParams1.put("key", "value");
    newSd1.setCols(newCols1);
    newSd1.setParameters(newParams1);
    newSd1.setLocation("loc1new");
    newPart1.setSd(newSd1);
    newPart1.setValues(Collections.singletonList("201701"));

    sharedCache.addPartitionToCache(DEFAULT_CATALOG_NAME, dbName, tbl1Name, part1);
    sharedCache.addPartitionToCache(DEFAULT_CATALOG_NAME, dbName, tbl1Name, part2);
    sharedCache.addPartitionToCache(DEFAULT_CATALOG_NAME, dbName, tbl1Name, part3);
    sharedCache.addPartitionToCache(DEFAULT_CATALOG_NAME, dbName, tbl2Name, part1);

    Partition t = sharedCache.getPartitionFromCache(DEFAULT_CATALOG_NAME, dbName, tbl1Name,
        Collections.singletonList("201701"));
    Assert.assertEquals("loc1", t.getSd().getLocation());

    sharedCache.removePartitionFromCache(DEFAULT_CATALOG_NAME, dbName, tbl2Name, Collections.singletonList("201701"));
    t = sharedCache.getPartitionFromCache(DEFAULT_CATALOG_NAME, dbName, tbl2Name, Collections.singletonList("201701"));
    Assert.assertNull(t);

    sharedCache.alterPartitionInCache(DEFAULT_CATALOG_NAME, dbName, tbl1Name, Collections.singletonList("201701"), newPart1);
    t = sharedCache.getPartitionFromCache(DEFAULT_CATALOG_NAME, dbName, tbl1Name, Collections.singletonList("201701"));
    Assert.assertEquals("loc1new", t.getSd().getLocation());
    cachedStore.shutdown();
  }

  //@Test
  public void testAggrStatsRepeatedRead() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    String dbName = "testTableColStatsOps";
    String tblName = "tbl";
    String colName = "f1";

    Database db = new DatabaseBuilder().setName(dbName).setLocation("some_location").build(conf);
    cachedStore.createDatabase(db);

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema(colName, "int", null));
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("col", "int", null));
    StorageDescriptor sd = new StorageDescriptor(cols, null, "input", "output", false, 0,
        new SerDeInfo("serde", "seriallib", new HashMap<>()), null, null, null);

    Table tbl = new Table(tblName, dbName, null, 0, 0, 0, sd, partCols, new HashMap<>(), null, null,
        TableType.MANAGED_TABLE.toString());
    tbl.setCatName(DEFAULT_CATALOG_NAME);
    cachedStore.createTable(tbl);

    List<String> partVals1 = new ArrayList<>();
    partVals1.add("1");
    List<String> partVals2 = new ArrayList<>();
    partVals2.add("2");

    Partition ptn1 = createPartition(tbl, partVals1);
    ptn1.setCatName(DEFAULT_CATALOG_NAME);
    cachedStore.addPartition(ptn1);
    Partition ptn2 = createPartition(tbl, partVals2);
    ptn2.setCatName(DEFAULT_CATALOG_NAME);
    cachedStore.addPartition(ptn2);

    ColumnStatistics stats = new ColumnStatistics();
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, dbName, tblName);
    statsDesc.setPartName("col");
    List<ColumnStatisticsObj> colStatObjs = new ArrayList<>();

    ColumnStatisticsData data = new ColumnStatisticsData();
    ColumnStatisticsObj colStats = new ColumnStatisticsObj(colName, "int", data);
    LongColumnStatsDataInspector longStats = new LongColumnStatsDataInspector();
    longStats.setLowValue(0);
    longStats.setHighValue(100);
    longStats.setNumNulls(50);
    longStats.setNumDVs(30);
    data.setLongStats(longStats);
    colStatObjs.add(colStats);

    stats.setStatsDesc(statsDesc);
    stats.setStatsObj(colStatObjs);
    stats.setEngine(CacheUtils.HIVE_ENGINE);

    cachedStore.updatePartitionColumnStatistics(stats.deepCopy(), partVals1, null, -1);
    cachedStore.updatePartitionColumnStatistics(stats.deepCopy(), partVals2, null, -1);

    List<String> colNames = new ArrayList<>();
    colNames.add(colName);
    List<String> aggrPartVals = new ArrayList<>();
    aggrPartVals.add("1");
    aggrPartVals.add("2");
    AggrStats aggrStats = cachedStore.get_aggr_stats_for(DEFAULT_CATALOG_NAME, dbName, tblName, aggrPartVals, colNames, CacheUtils.HIVE_ENGINE);
    Assert.assertEquals(100, aggrStats.getColStats().get(0).getStatsData().getLongStats().getNumNulls());
    aggrStats = cachedStore.get_aggr_stats_for(DEFAULT_CATALOG_NAME, dbName, tblName, aggrPartVals, colNames, CacheUtils.HIVE_ENGINE);
    Assert.assertEquals(100, aggrStats.getColStats().get(0).getStatsData().getLongStats().getNumNulls());

    objectStore.deletePartitionColumnStatistics(DEFAULT_CATALOG_NAME, db.getName(), tbl.getTableName(),
        Warehouse.makePartName(tbl.getPartitionKeys(), partVals1), partVals1, colName, CacheUtils.HIVE_ENGINE);
    objectStore.deletePartitionColumnStatistics(DEFAULT_CATALOG_NAME, db.getName(), tbl.getTableName(),
        Warehouse.makePartName(tbl.getPartitionKeys(), partVals2), partVals2, colName, CacheUtils.HIVE_ENGINE);
    objectStore.dropPartition(DEFAULT_CATALOG_NAME, db.getName(), tbl.getTableName(), partVals1);
    objectStore.dropPartition(DEFAULT_CATALOG_NAME, db.getName(), tbl.getTableName(), partVals2);
    objectStore.dropTable(DEFAULT_CATALOG_NAME, db.getName(), tbl.getTableName());
    objectStore.dropDatabase(DEFAULT_CATALOG_NAME, db.getName());
    cachedStore.shutdown();
  }

  @Test
  public void testPartitionAggrStats() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    String dbName = "testTableColStatsOps1";
    String tblName = "tbl1";
    String colName = "f1";

    Database db = new Database(dbName, null, "some_location", null);
    db.setCatalogName(DEFAULT_CATALOG_NAME);
    cachedStore.createDatabase(db);

    FieldSchema col = new FieldSchema(colName, "int", null);
    List<FieldSchema> cols = Collections.singletonList(col);
    List<FieldSchema> partCols = Collections.singletonList(new FieldSchema("col", "int", null));
    StorageDescriptor sd = createStorageDescriptor(cols);

    Table table = createTable(dbName, tblName, sd, partCols);
    cachedStore.createTable(table);

    List<String> partVals1 = Collections.singletonList("1");
    cachedStore.addPartition(createPartition(table, partVals1));

    ColumnStatisticsData data = new ColStatsBuilder<>(long.class).numNulls(50).numDVs(30)
        .low(0L).high(100L).build();
    ColumnStatistics stats = createColumnStatistics(data, table, col, "part1");
    cachedStore.updatePartitionColumnStatistics(stats, partVals1, null, -1);

    List<String> partVals2 = Collections.singletonList("2");
    cachedStore.addPartition(createPartition(table, partVals2));

    data = new ColStatsBuilder<>(long.class).numNulls(50).numDVs(40)
        .low(0L).high(100L).build();
    stats = createColumnStatistics(data, table, col, "part2");
    cachedStore.updatePartitionColumnStatistics(stats, partVals2, null, -1);

    List<String> colNames = Collections.singletonList(colName);
    List<String> aggrPartVals = Arrays.asList("1", "2");

    AggrStats aggrStats = cachedStore.get_aggr_stats_for(
        DEFAULT_CATALOG_NAME, dbName, tblName, aggrPartVals, colNames, CacheUtils.HIVE_ENGINE);
    ColumnStatisticsData computedData = aggrStats.getColStats().get(0).getStatsData();
    ColumnStatisticsData expectedData = new ColStatsBuilder<>(long.class).numNulls(100).numDVs(40)
        .low(0L).high(100L).build();
    Assert.assertEquals(expectedData, computedData);

    // read a second time, the result should not change
    aggrStats = cachedStore.get_aggr_stats_for(
        DEFAULT_CATALOG_NAME, dbName, tblName, aggrPartVals, colNames, CacheUtils.HIVE_ENGINE);
    computedData = aggrStats.getColStats().get(0).getStatsData();
    Assert.assertEquals(expectedData, computedData);

    cachedStore.shutdown();
  }

  @Test
  public void testPartitionAggrStatsBitVectorKll() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);

    String dbName = "testTableColStatsOps2";
    String tblName = "tbl2";
    String colName = "f1";

    Database db = new Database(dbName, null, "some_location", null);
    db.setCatalogName(DEFAULT_CATALOG_NAME);
    cachedStore.createDatabase(db);

    FieldSchema col = new FieldSchema(colName, "int", null);
    List<FieldSchema> cols = Collections.singletonList(col);
    List<FieldSchema> partCols = Collections.singletonList(new FieldSchema("col", "int", null));
    StorageDescriptor sd = createStorageDescriptor(cols);

    Table table = createTable(dbName, tblName, sd, partCols);
    cachedStore.createTable(table);

    List<String> partVals1 = Collections.singletonList("1");
    cachedStore.addPartition(createPartition(table, partVals1));

    ColumnStatisticsData data = new ColStatsBuilder<>(long.class).numNulls(50).numDVs(30)
        .low(0L).high(100L).hll(1, 2, 3).kll(1, 2, 3).build();
    ColumnStatistics stats = createColumnStatistics(data, table, col, "part1");
    cachedStore.updatePartitionColumnStatistics(stats, partVals1, null, -1);

    List<String> partVals2 = Collections.singletonList("2");
    cachedStore.addPartition(createPartition(table, partVals2));
    data = new ColStatsBuilder<>(long.class).numNulls(50).numDVs(30)
        .low(0L).high(100L).hll(1, 2, 3, 4).kll(2, 3, 4, 5).build();
    stats = createColumnStatistics(data, table, col, "part2");
    cachedStore.updatePartitionColumnStatistics(stats, partVals2, null, -1);

    List<String> colNames = Collections.singletonList(colName);
    List<String> aggrPartVals = Arrays.asList("1", "2");

    AggrStats aggrStats = cachedStore.get_aggr_stats_for(
        DEFAULT_CATALOG_NAME, dbName, tblName, aggrPartVals, colNames, CacheUtils.HIVE_ENGINE);

    ColumnStatisticsData columnStatsData = aggrStats.getColStats().get(0).getStatsData();
    ColumnStatisticsData expectedData = new ColStatsBuilder<>(long.class).numNulls(100).numDVs(4)
        .low(0L).high(100L).hll(1, 2, 3)
        .kll(1, 2, 3, 2, 3, 4, 5).build();
    assertEqualStatistics(expectedData, columnStatsData);

    aggrStats = cachedStore.get_aggr_stats_for(
        DEFAULT_CATALOG_NAME, dbName, tblName, aggrPartVals, colNames, CacheUtils.HIVE_ENGINE);
    columnStatsData = aggrStats.getColStats().get(0).getStatsData();
    assertEqualStatistics(expectedData, columnStatsData);

    cachedStore.shutdown();
  }

  @Test public void testMultiThreadedSharedCacheOps() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    SharedCache sharedCache = CachedStore.getSharedCache();

    List<String> dbNames = new ArrayList<>(Arrays.asList("db1", "db2", "db3", "db4", "db5"));
    List<Callable<Object>> tasks = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(50, r -> {
      Thread t = Executors.defaultThreadFactory().newThread(r);
      t.setDaemon(true);
      return t;
    });

    // Create 5 dbsConstraintConstraint
    for (String dbName : dbNames) {
      Callable<Object> c = () -> {
        Database db = createDatabaseObject(dbName, "user1");
        sharedCache.addDatabaseToCache(db);
        return null;
      };
      tasks.add(c);
    }
    executor.invokeAll(tasks);
    for (String dbName : dbNames) {
      Database db = sharedCache.getDatabaseFromCache(DEFAULT_CATALOG_NAME, dbName);
      Assert.assertNotNull(db);
      Assert.assertEquals(dbName, db.getName());
    }

    // Created 5 tables under "db1"
    List<String> tblNames = new ArrayList<>(Arrays.asList("tbl1", "tbl2", "tbl3", "tbl4", "tbl5"));
    tasks.clear();
    for (String tblName : tblNames) {
      FieldSchema col1 = new FieldSchema("col1", "int", "integer column");
      FieldSchema col2 = new FieldSchema("col2", "string", "string column");
      List<FieldSchema> cols = new ArrayList<>();
      cols.add(col1);
      cols.add(col2);
      FieldSchema ptnCol1 = new FieldSchema("part1", "string", "string partition column");
      List<FieldSchema> ptnCols = new ArrayList<>();
      ptnCols.add(ptnCol1);
      Callable<Object> c = () -> {
        Table tbl = createTable(dbNames.get(0), tblName, createStorageDescriptor(cols), ptnCols);
        sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, dbNames.get(0), tblName, tbl);
        return null;
      };
      tasks.add(c);
    }
    executor.invokeAll(tasks);
    for (String tblName : tblNames) {
      Table tbl = sharedCache.getTableFromCache(DEFAULT_CATALOG_NAME, dbNames.get(0), tblName);
      Assert.assertNotNull(tbl);
      Assert.assertEquals(tblName, tbl.getTableName());
    }

    // Add 5 partitions to all tables
    List<String> ptnVals = new ArrayList<>(Arrays.asList("aaa", "bbb", "ccc", "ddd", "eee"));
    tasks.clear();
    for (String tblName : tblNames) {
      Table tbl = sharedCache.getTableFromCache(DEFAULT_CATALOG_NAME, dbNames.get(0), tblName);
      for (String ptnVal : ptnVals) {
        Callable<Object> c = () -> {
          Partition ptn = createPartition(tbl, Collections.singletonList(ptnVal));
          sharedCache.addPartitionToCache(DEFAULT_CATALOG_NAME, dbNames.get(0), tblName, ptn);
          return null;
        };
        tasks.add(c);
      }
    }
    executor.invokeAll(tasks);
    for (String tblName : tblNames) {
      for (String ptnVal : ptnVals) {
        Partition ptn =
            sharedCache.getPartitionFromCache(DEFAULT_CATALOG_NAME, dbNames.get(0), tblName,
                Collections.singletonList(ptnVal));
        Assert.assertNotNull(ptn);
        Assert.assertEquals(tblName, ptn.getTableName());
        Assert.assertEquals(tblName, ptn.getTableName());
        Assert.assertEquals(Collections.singletonList(ptnVal), ptn.getValues());
      }
    }

    // Drop all partitions from "tbl1", "tbl2", "tbl3" and add 2 new partitions to "tbl4" and "tbl5"
    List<String> newPtnVals = new ArrayList<>(Arrays.asList("fff", "ggg"));
    List<String> dropPtnTblNames = new ArrayList<>(Arrays.asList("tbl1", "tbl2", "tbl3"));
    List<String> addPtnTblNames = new ArrayList<>(Arrays.asList("tbl4", "tbl5"));
    tasks.clear();
    for (String tblName : dropPtnTblNames) {
      for (String ptnVal : ptnVals) {
        Callable<Object> c = () -> {
          sharedCache.removePartitionFromCache(DEFAULT_CATALOG_NAME, dbNames.get(0), tblName,
              Collections.singletonList(ptnVal));
          return null;
        };
        tasks.add(c);
      }
    }
    for (String tblName : addPtnTblNames) {
      Table tbl = sharedCache.getTableFromCache(DEFAULT_CATALOG_NAME, dbNames.get(0), tblName);
      for (String ptnVal : newPtnVals) {
        Callable<Object> c = () -> {
          Partition ptn = createPartition(tbl, Collections.singletonList(ptnVal));
          sharedCache.addPartitionToCache(DEFAULT_CATALOG_NAME, dbNames.get(0), tblName, ptn);
          return null;
        };
        tasks.add(c);
      }
    }
    executor.invokeAll(tasks);
    for (String tblName : addPtnTblNames) {
      for (String ptnVal : newPtnVals) {
        Partition ptn =
            sharedCache.getPartitionFromCache(DEFAULT_CATALOG_NAME, dbNames.get(0), tblName,
                Collections.singletonList(ptnVal));
        Assert.assertNotNull(ptn);
        Assert.assertEquals(tblName, ptn.getTableName());
        Assert.assertEquals(tblName, ptn.getTableName());
        Assert.assertEquals(Collections.singletonList(ptnVal), ptn.getValues());
      }
    }
    for (String tblName : dropPtnTblNames) {
      List<Partition> ptns = sharedCache.listCachedPartitions(DEFAULT_CATALOG_NAME, dbNames.get(0), tblName, 100);
      Assert.assertEquals(0, ptns.size());
    }
    cachedStore.shutdown();
  }

  @Test public void testPartitionSize() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "5Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTestExceptSharedCache(conf);

    String dbName = "db1";
    String tbl1Name = "tbl1";
    String tbl2Name = "tbl2";
    String owner = "user1";
    Database db = createDatabaseObject(dbName, owner);

    FieldSchema col1 = new FieldSchema("col1", "int", "integer column");
    FieldSchema col2 = new FieldSchema("col2", "string", "string column");
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(col1);
    cols.add(col2);
    StorageDescriptor sd = createStorageDescriptor(cols);
    List<FieldSchema> ptnCols = new ArrayList<>();
    Table tbl1 = createTable(dbName, tbl1Name, sd, ptnCols);
    Table tbl2 = createTable(dbName, tbl2Name, sd, ptnCols);

    Map<String, Integer> tableSizeMap = new HashMap<>();
    String tbl1Key = CacheUtils.buildTableKey(DEFAULT_CATALOG_NAME, dbName, tbl1Name);
    String tbl2Key = CacheUtils.buildTableKey(DEFAULT_CATALOG_NAME, dbName, tbl2Name);
    tableSizeMap.put(tbl1Key, 1000);
    tableSizeMap.put(tbl2Key, 4500);

    Partition part1 = new Partition();
    StorageDescriptor sd1 = new StorageDescriptor();
    List<FieldSchema> cols1 = new ArrayList<>();
    cols1.add(new FieldSchema("col1", "int", ""));
    Map<String, String> params1 = new HashMap<>();
    params1.put("key", "value");
    sd1.setCols(cols1);
    sd1.setParameters(params1);
    sd1.setLocation("loc1");
    part1.setSd(sd1);
    part1.setValues(Collections.singletonList("201701"));

    Partition part2 = new Partition();
    StorageDescriptor sd2 = new StorageDescriptor();
    List<FieldSchema> cols2 = new ArrayList<>();
    cols2.add(new FieldSchema("col1", "int", ""));
    Map<String, String> params2 = new HashMap<>();
    params2.put("key", "value");
    sd2.setCols(cols2);
    sd2.setParameters(params2);
    sd2.setLocation("loc2");
    part2.setSd(sd2);
    part2.setValues(Collections.singletonList("201702"));

    Partition part3 = new Partition();
    StorageDescriptor sd3 = new StorageDescriptor();
    List<FieldSchema> cols3 = new ArrayList<>();
    cols3.add(new FieldSchema("col3", "int", ""));
    Map<String, String> params3 = new HashMap<>();
    params3.put("key2", "value2");
    sd3.setCols(cols3);
    sd3.setParameters(params3);
    sd3.setLocation("loc3");
    part3.setSd(sd3);
    part3.setValues(Collections.singletonList("201703"));

    Partition newPart1 = new Partition();
    newPart1.setDbName(dbName);
    newPart1.setTableName(tbl1Name);
    StorageDescriptor newSd1 = new StorageDescriptor();
    List<FieldSchema> newCols1 = new ArrayList<>();
    newCols1.add(new FieldSchema("newcol1", "int", ""));
    Map<String, String> newParams1 = new HashMap<>();
    newParams1.put("key", "value");
    newSd1.setCols(newCols1);
    newSd1.setParameters(newParams1);
    newSd1.setLocation("loc1new");
    newPart1.setSd(newSd1);
    newPart1.setValues(Collections.singletonList("201701"));

    SharedCache sharedCache = CachedStore.getSharedCache();
    sharedCache.setConcurrencyLevel(1);
    sharedCache.setTableSizeMap(tableSizeMap);
    sharedCache.initialize(conf);

    sharedCache.addDatabaseToCache(db);
    sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, dbName, tbl1Name, tbl1);
    sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, dbName, tbl2Name, tbl2);

    sharedCache.addPartitionToCache(DEFAULT_CATALOG_NAME, dbName, tbl1Name, part1);
    sharedCache.addPartitionToCache(DEFAULT_CATALOG_NAME, dbName, tbl1Name, part2);
    sharedCache.addPartitionToCache(DEFAULT_CATALOG_NAME, dbName, tbl1Name, part3);

    Partition p = sharedCache.getPartitionFromCache(DEFAULT_CATALOG_NAME, dbName, tbl1Name,
        Collections.singletonList("201701"));
    Assert.assertNull(p);

    sharedCache.addPartitionToCache(DEFAULT_CATALOG_NAME, dbName, tbl2Name, newPart1);
    p = sharedCache.getPartitionFromCache(DEFAULT_CATALOG_NAME, dbName, tbl2Name, Collections.singletonList("201701"));
    Assert.assertNotNull(p);
    cachedStore.shutdown();
  }

  @Test public void testShowTables() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "5kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();

    cachedStore.setConfForTestExceptSharedCache(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    //set up table size map
    Map<String, Integer> tableSizeMap = new HashMap<>();
    String db1Utbl1TblKey =
        CacheUtils.buildTableKey(DEFAULT_CATALOG_NAME, db1Utbl1.getDbName(), db1Utbl1.getTableName());
    String db1Ptbl1TblKey =
        CacheUtils.buildTableKey(DEFAULT_CATALOG_NAME, db1Ptbl1.getDbName(), db1Ptbl1.getTableName());
    String db2Utbl1TblKey =
        CacheUtils.buildTableKey(DEFAULT_CATALOG_NAME, db2Utbl1.getDbName(), db2Utbl1.getTableName());
    String db2Ptbl1TblKey =
        CacheUtils.buildTableKey(DEFAULT_CATALOG_NAME, db2Ptbl1.getDbName(), db2Ptbl1.getTableName());
    tableSizeMap.put(db1Utbl1TblKey, 4000);
    tableSizeMap.put(db1Ptbl1TblKey, 4000);
    tableSizeMap.put(db2Utbl1TblKey, 4000);
    tableSizeMap.put(db2Ptbl1TblKey, 4000);

    SharedCache sc = CachedStore.getSharedCache();
    sc.setConcurrencyLevel(1);
    sc.setTableSizeMap(tableSizeMap);
    sc.initialize(conf);

    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(objectStore);

    List<String> db1Tables = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db1.getName());
    Assert.assertEquals(2, db1Tables.size());
    List<String> db2Tables = cachedStore.getAllTables(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(2, db2Tables.size());

    cachedStore.shutdown();
  }

  @Test public void testTableEviction() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "5kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();

    cachedStore.setConfForTestExceptSharedCache(conf);
    ObjectStore objectStore = (ObjectStore) cachedStore.getRawStore();
    //set up table size map
    Map<String, Integer> tableSizeMap = new HashMap<>();
    String db1Utbl1TblKey =
        CacheUtils.buildTableKey(DEFAULT_CATALOG_NAME, db1Utbl1.getDbName(), db1Utbl1.getTableName());
    String db1Ptbl1TblKey =
        CacheUtils.buildTableKey(DEFAULT_CATALOG_NAME, db1Ptbl1.getDbName(), db1Ptbl1.getTableName());
    String db2Utbl1TblKey =
        CacheUtils.buildTableKey(DEFAULT_CATALOG_NAME, db2Utbl1.getDbName(), db2Utbl1.getTableName());
    String db2Ptbl1TblKey =
        CacheUtils.buildTableKey(DEFAULT_CATALOG_NAME, db2Ptbl1.getDbName(), db2Ptbl1.getTableName());
    tableSizeMap.put(db1Utbl1TblKey, 4000);
    tableSizeMap.put(db1Ptbl1TblKey, 4000);
    tableSizeMap.put(db2Utbl1TblKey, 4000);
    tableSizeMap.put(db2Ptbl1TblKey, 4000);
    Table tblDb1Utbl1 = objectStore.getTable(DEFAULT_CATALOG_NAME, db1Utbl1.getDbName(), db1Utbl1.getTableName());
    Table tblDb1Ptbl1 = objectStore.getTable(DEFAULT_CATALOG_NAME, db1Ptbl1.getDbName(), db1Ptbl1.getTableName());
    Table tblDb2Utbl1 = objectStore.getTable(DEFAULT_CATALOG_NAME, db2Utbl1.getDbName(), db2Utbl1.getTableName());
    Table tblDb2Ptbl1 = objectStore.getTable(DEFAULT_CATALOG_NAME, db2Ptbl1.getDbName(), db2Ptbl1.getTableName());

    SharedCache sc = CachedStore.getSharedCache();
    sc.setConcurrencyLevel(1);
    sc.setTableSizeMap(tableSizeMap);
    sc.initialize(conf);

    sc.addDatabaseToCache(db1);
    sc.addDatabaseToCache(db2);
    sc.addTableToCache(DEFAULT_CATALOG_NAME, db1Utbl1.getDbName(), db1Utbl1.getTableName(), tblDb1Utbl1);
    sc.addTableToCache(DEFAULT_CATALOG_NAME, db1Ptbl1.getDbName(), db1Ptbl1.getTableName(), tblDb1Ptbl1);
    sc.addTableToCache(DEFAULT_CATALOG_NAME, db2Utbl1.getDbName(), db2Utbl1.getTableName(), tblDb2Utbl1);
    sc.addTableToCache(DEFAULT_CATALOG_NAME, db2Ptbl1.getDbName(), db2Ptbl1.getTableName(), tblDb2Ptbl1);

    List<String> db1Tables = sc.listCachedTableNames(DEFAULT_CATALOG_NAME, db1.getName());
    Assert.assertEquals(0, db1Tables.size());
    List<String> db2Tables = sc.listCachedTableNames(DEFAULT_CATALOG_NAME, db2.getName());
    Assert.assertEquals(1, db2Tables.size());

    cachedStore.shutdown();
  }

  @Test
  public void testPrimaryKeys() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    SharedCache sharedCache = CachedStore.getSharedCache();

    Database db = createDatabaseObject("db", "testUser");
    Table tbl = createUnpartitionedTableObject(db);

    sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, "db", tbl.getTableName(), tbl);
    Assert.assertEquals(1, sharedCache.getCachedTableCount());

    List<SQLPrimaryKey> origKeys = createPrimaryKeys(tbl);
    SQLAllTableConstraints constraints = new SQLAllTableConstraints();
    constraints.setPrimaryKeys(origKeys);
    sharedCache.addTableConstraintsToCache(DEFAULT_CATALOG_NAME, "db", tbl.getTableName(), constraints);

    // List operation
    List<SQLPrimaryKey> cachedKeys =
        sharedCache.listCachedPrimaryKeys(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());

    Assert.assertEquals(origKeys, cachedKeys);

    SQLPrimaryKey modifiedKey = origKeys.get(0).deepCopy();
    modifiedKey.setColumn_name("col2");
    modifiedKey.setPk_name("pk_modified");

    sharedCache
        .addPrimaryKeysToCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(),
            Collections.singletonList(modifiedKey));
    constraints = new SQLAllTableConstraints();
    List<SQLPrimaryKey> list = new ArrayList<>();
    list.add(origKeys.get(0));
    list.add(modifiedKey);
    constraints.setPrimaryKeys(list);
    sharedCache
        .refreshAllTableConstraintsInCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), constraints);
    cachedKeys = sharedCache.listCachedPrimaryKeys(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(cachedKeys, constraints.getPrimaryKeys());

    sharedCache.removeConstraintFromCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), "pk1");
    constraints = new SQLAllTableConstraints();
    constraints.setPrimaryKeys(Collections.singletonList(modifiedKey));
    sharedCache
        .refreshAllTableConstraintsInCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), constraints);
    cachedKeys = sharedCache.listCachedPrimaryKeys(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(Collections.singletonList(modifiedKey), cachedKeys);

    cachedStore.shutdown();
  }

  @Test
  public void testNotNullConstraint() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    SharedCache sharedCache = CachedStore.getSharedCache();

    Database db = createDatabaseObject("db", "testUser");
    Table tbl = createUnpartitionedTableObject(db);

    sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, "db", tbl.getTableName(), tbl);
    Assert.assertEquals(1, sharedCache.getCachedTableCount());

    List<SQLNotNullConstraint> origKeys = createNotNullConstraint(tbl);
    SQLAllTableConstraints constraints = new SQLAllTableConstraints();
    constraints.setNotNullConstraints(origKeys);
    sharedCache.addTableConstraintsToCache(DEFAULT_CATALOG_NAME, "db", tbl.getTableName(), constraints);

    // List operation
    List<SQLNotNullConstraint> cachedKeys =
        sharedCache.listCachedNotNullConstraints(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());

    Assert.assertEquals(origKeys, cachedKeys);

    SQLNotNullConstraint modifiedKey = origKeys.get(0).deepCopy();
    modifiedKey.setColumn_name("col2");
    modifiedKey.setNn_name("nn_modified");

    sharedCache.addNotNullConstraintsToCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(),
        Collections.singletonList(modifiedKey));
    constraints = new SQLAllTableConstraints();
    List<SQLNotNullConstraint> list = new ArrayList<>();
    list.add(origKeys.get(0));
    list.add(modifiedKey);
    constraints.setNotNullConstraints(list);
    sharedCache
        .refreshAllTableConstraintsInCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), constraints);
    cachedKeys = sharedCache.listCachedNotNullConstraints(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(cachedKeys, constraints.getNotNullConstraints());

    sharedCache.removeConstraintFromCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), "nn1");
    constraints = new SQLAllTableConstraints();
    constraints.setNotNullConstraints(Collections.singletonList(modifiedKey));
    sharedCache
        .refreshAllTableConstraintsInCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), constraints);
    cachedKeys = sharedCache.listCachedNotNullConstraints(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(Collections.singletonList(modifiedKey), cachedKeys);

    cachedStore.shutdown();
  }

  @Test
  public void testUniqueConstraint() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    SharedCache sharedCache = CachedStore.getSharedCache();

    Database db = createDatabaseObject("db", "testUser");
    Table tbl = createUnpartitionedTableObject(db);

    sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, "db", tbl.getTableName(), tbl);
    Assert.assertEquals(1, sharedCache.getCachedTableCount());

    List<SQLUniqueConstraint> origKeys = createUniqueConstraint(tbl);
    SQLAllTableConstraints constraints = new SQLAllTableConstraints();
    constraints.setUniqueConstraints(origKeys);
    sharedCache.addTableConstraintsToCache(DEFAULT_CATALOG_NAME, "db", tbl.getTableName(), constraints);

    // List operation
    List<SQLUniqueConstraint> cachedKeys =
        sharedCache.listCachedUniqueConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());

    Assert.assertEquals(origKeys, cachedKeys);

    SQLUniqueConstraint modifiedKey = origKeys.get(0).deepCopy();
    modifiedKey.setColumn_name("col2");
    modifiedKey.setUk_name("uk_modified");

    sharedCache.addUniqueConstraintsToCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(),
        Collections.singletonList(modifiedKey));
    cachedKeys = sharedCache.listCachedUniqueConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(cachedKeys, new ArrayList<>());

    constraints = new SQLAllTableConstraints();
    List<SQLUniqueConstraint> list = new ArrayList<>();
    list.add(origKeys.get(0));
    list.add(modifiedKey);
    constraints.setUniqueConstraints(list);
    sharedCache
        .refreshAllTableConstraintsInCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), constraints);
    cachedKeys = sharedCache.listCachedUniqueConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(cachedKeys, constraints.getUniqueConstraints());

    sharedCache.removeConstraintFromCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), "uk1");
    constraints = new SQLAllTableConstraints();
    constraints.setUniqueConstraints(Collections.singletonList(modifiedKey));
    sharedCache
        .refreshAllTableConstraintsInCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), constraints);
    cachedKeys = sharedCache.listCachedUniqueConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(Collections.singletonList(modifiedKey), cachedKeys);

    cachedStore.shutdown();
  }

  @Test
  public void testForeignKeys() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    SharedCache sharedCache = CachedStore.getSharedCache();

    Database db = createDatabaseObject("db", "testUser");
    Database db1 = createDatabaseObject("db1", "testUser");
    Table tbl = createUnpartitionedTableObject(db);
    Table tbl1 = createUnpartitionedTableObject(db1);

    sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, "db", tbl.getTableName(), tbl);
    sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, "db1", tbl1.getTableName(), tbl1);

    Assert.assertEquals(2, sharedCache.getCachedTableCount());

    List<SQLForeignKey> origKeys = createForeignKeys(tbl, tbl, "fk1");

    SQLAllTableConstraints constraints = new SQLAllTableConstraints();
    constraints.setForeignKeys(origKeys);
    sharedCache.addTableConstraintsToCache(DEFAULT_CATALOG_NAME, "db", tbl.getTableName(), constraints);

    // List operation
    List<SQLForeignKey> cachedKeys = sharedCache
        .listCachedForeignKeys(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), tbl.getDbName(),
            tbl.getTableName());

    Assert.assertEquals(origKeys, cachedKeys);

    // List operation with different parent table
    cachedKeys = sharedCache.listCachedForeignKeys(
        DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), "dummyDB", "dummyTable");
    Assert.assertEquals(0, cachedKeys.size());

    SQLForeignKey modifiedKey = origKeys.get(0).deepCopy();
    modifiedKey.setFkcolumn_name("col3");
    modifiedKey.setFk_name("fk_modified");

    sharedCache
        .addForeignKeysToCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(),
            Collections.singletonList(modifiedKey));
    cachedKeys = sharedCache
        .listCachedForeignKeys(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), tbl.getDbName(),
            tbl.getTableName());

    Assert.assertEquals(cachedKeys, new ArrayList<>());

    constraints = new SQLAllTableConstraints();
    List<SQLForeignKey> list = new ArrayList<>();
    list.add(modifiedKey);
    list.add(origKeys.get(0));
    constraints.setForeignKeys(list);
    sharedCache
        .refreshAllTableConstraintsInCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), constraints);
    cachedKeys = sharedCache
        .listCachedForeignKeys(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), tbl.getDbName(),
            tbl.getTableName());
    Assert.assertEquals(cachedKeys, constraints.getForeignKeys());

    sharedCache.removeConstraintFromCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), "fk1");
    constraints = new SQLAllTableConstraints();
    constraints.setForeignKeys(Collections.singletonList(modifiedKey));
    sharedCache
        .refreshAllTableConstraintsInCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), constraints);
    cachedKeys = sharedCache
        .listCachedForeignKeys(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), tbl.getDbName(),
            tbl.getTableName());
    Assert.assertEquals(Collections.singletonList(modifiedKey), cachedKeys);

    cachedStore.shutdown();
  }

  @Test
  public void testDefaultConstraint() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    SharedCache sharedCache = CachedStore.getSharedCache();

    Database db = createDatabaseObject("db", "testUser");
    Table tbl = createUnpartitionedTableObject(db);

    sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, "db", tbl.getTableName(), tbl);

    Assert.assertEquals(1, sharedCache.getCachedTableCount());

    List<SQLDefaultConstraint> origKeys = createDefaultConstraint(tbl);
    SQLAllTableConstraints constraints = new SQLAllTableConstraints();
    constraints.setDefaultConstraints(origKeys);
    sharedCache.addTableConstraintsToCache(DEFAULT_CATALOG_NAME, "db", tbl.getTableName(), constraints);

    // List operation
    List<SQLDefaultConstraint> cachedKeys =
        sharedCache.listCachedDefaultConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(origKeys, cachedKeys);

    SQLDefaultConstraint modifiedKey = origKeys.get(0).deepCopy();
    modifiedKey.setColumn_name("col2");
    modifiedKey.setDc_name("dc_modified");

    sharedCache.addDefaultConstraintsToCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(),
        Collections.singletonList(modifiedKey));
    cachedKeys = sharedCache.listCachedDefaultConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(cachedKeys, new ArrayList<>());

    constraints = new SQLAllTableConstraints();
    List<SQLDefaultConstraint> list = new ArrayList<>();
    list.add(origKeys.get(0));
    list.add(modifiedKey);
    constraints.setDefaultConstraints(list);
    sharedCache
        .refreshAllTableConstraintsInCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), constraints);
    cachedKeys = sharedCache.listCachedDefaultConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(cachedKeys, constraints.getDefaultConstraints());

    sharedCache.removeConstraintFromCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), "dc1");
    constraints = new SQLAllTableConstraints();
    constraints.setDefaultConstraints(Collections.singletonList(modifiedKey));
    sharedCache
        .refreshAllTableConstraintsInCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), constraints);
    cachedKeys = sharedCache.listCachedDefaultConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(Collections.singletonList(modifiedKey), cachedKeys);

    cachedStore.shutdown();
  }

  @Test
  public void testCheckConstraint() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    SharedCache sharedCache = CachedStore.getSharedCache();

    Database db = createDatabaseObject("db", "testUser");
    Table tbl = createUnpartitionedTableObject(db);

    sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, "db", tbl.getTableName(), tbl);

    Assert.assertEquals(1, sharedCache.getCachedTableCount());

    List<SQLCheckConstraint> origKeys = createCheckConstraint(tbl);
    SQLAllTableConstraints constraints = new SQLAllTableConstraints();
    constraints.setCheckConstraints(origKeys);
    sharedCache.addTableConstraintsToCache(DEFAULT_CATALOG_NAME, "db", tbl.getTableName(), constraints);

    // List operation
    List<SQLCheckConstraint> cachedKeys =
        sharedCache.listCachedCheckConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());

    Assert.assertEquals(origKeys, cachedKeys);

    SQLCheckConstraint modifiedKey = origKeys.get(0).deepCopy();
    modifiedKey.setColumn_name("col2");
    modifiedKey.setDc_name("cc_modified");

    sharedCache.addCheckConstraintsToCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(),
        Collections.singletonList(modifiedKey));
    cachedKeys = sharedCache.listCachedCheckConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(cachedKeys, new ArrayList<>());

    constraints = new SQLAllTableConstraints();
    List<SQLCheckConstraint> list = new ArrayList<>();
    list.add(origKeys.get(0));
    list.add(modifiedKey);
    constraints.setCheckConstraints(list);
    sharedCache
        .refreshAllTableConstraintsInCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), constraints);
    cachedKeys = sharedCache.listCachedCheckConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(cachedKeys, constraints.getCheckConstraints());

    sharedCache.removeConstraintFromCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), "cc1");
    cachedKeys = sharedCache.listCachedCheckConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(cachedKeys, new ArrayList<>());

    constraints = new SQLAllTableConstraints();
    constraints.setCheckConstraints(Collections.singletonList(modifiedKey));
    sharedCache
        .refreshAllTableConstraintsInCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), constraints);
    cachedKeys = sharedCache.listCachedCheckConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(constraints.getCheckConstraints(), cachedKeys);

    cachedStore.shutdown();
  }

  @Test
  public void testAllTableConstraint() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    CachedStore cachedStore = new CachedStore();
    CachedStore.clearSharedCache();
    cachedStore.setConfForTest(conf);
    SharedCache sharedCache = CachedStore.getSharedCache();

    Database db = createDatabaseObject("db", "testUser");
    Table tbl = createUnpartitionedTableObject(db);

    sharedCache.addTableToCache(DEFAULT_CATALOG_NAME, "db", tbl.getTableName(), tbl);

    Assert.assertEquals(1, sharedCache.getCachedTableCount());

    SQLAllTableConstraints originalConstraints = new SQLAllTableConstraints();
    originalConstraints.setPrimaryKeys(createPrimaryKeys(tbl));
    originalConstraints.setForeignKeys(createForeignKeys(tbl, tbl, "fk1"));
    originalConstraints.setCheckConstraints(createCheckConstraint(tbl));
    originalConstraints.setDefaultConstraints(createDefaultConstraint(tbl));
    originalConstraints.setUniqueConstraints(createUniqueConstraint(tbl));
    originalConstraints.setNotNullConstraints(createNotNullConstraint(tbl));
    sharedCache
        .addTableConstraintsToCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), originalConstraints);

    // List operation
    SQLAllTableConstraints cachedConstraints =
        sharedCache.listCachedAllTableConstraints(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());

    Assert.assertEquals(originalConstraints, cachedConstraints);

    SQLCheckConstraint checkConstraint = originalConstraints.getCheckConstraints().get(0).deepCopy();
    checkConstraint.setColumn_name("col2");
    checkConstraint.setDc_name("cc_modified");
    List<SQLCheckConstraint> cc = new ArrayList<>();
    cc.add(originalConstraints.getCheckConstraints().get(0));
    cc.add(checkConstraint);
    originalConstraints.setCheckConstraints(cc);

    sharedCache.addCheckConstraintsToCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(),
        Collections.singletonList(checkConstraint));
    cachedConstraints =
        sharedCache.listCachedAllTableConstraints(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(new SQLAllTableConstraints(), cachedConstraints);

    sharedCache.refreshAllTableConstraintsInCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(),
        originalConstraints);
    cachedConstraints =
        sharedCache.listCachedAllTableConstraints(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(originalConstraints, cachedConstraints);

    sharedCache.removeConstraintFromCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), "cc1");
    originalConstraints.getCheckConstraints().remove(0);

    sharedCache.refreshAllTableConstraintsInCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(),
        originalConstraints);
    cachedConstraints =
        sharedCache.listCachedAllTableConstraints(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(originalConstraints, cachedConstraints);

    // remove constraints
    sharedCache.removeConstraintFromCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), "cc_modified");
    originalConstraints.getCheckConstraints().remove(0);

    sharedCache.refreshAllTableConstraintsInCache(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(),
        originalConstraints);
    cachedConstraints =
        sharedCache.listCachedAllTableConstraints(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName());
    Assert.assertEquals(originalConstraints, cachedConstraints);

    cachedStore.shutdown();
  }

  private List<SQLPrimaryKey> createPrimaryKeys(Table tbl) {
    SQLPrimaryKey key = new SQLPrimaryKey(tbl.getDbName(), tbl.getTableName(), "col1",
            1, "pk1",false, false, false);
    key.setCatName(DEFAULT_CATALOG_NAME);

    return Collections.singletonList(key);
  }

  private List<SQLNotNullConstraint> createNotNullConstraint(Table tbl) {
    SQLNotNullConstraint key = new SQLNotNullConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(),
            "col1", "nn1",false, false, false);

    return Collections.singletonList(key);
  }

  private List<SQLUniqueConstraint> createUniqueConstraint(Table tbl) {
    SQLUniqueConstraint key = new SQLUniqueConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(),
            "col1", 1, "uk1",false, false, false);

    return Collections.singletonList(key);
  }

  @SuppressWarnings("SameParameterValue")
  private List<SQLForeignKey> createForeignKeys(Table primaryKeytbl, Table foreignKeyTbl, String fKeyName) {
    String foreignKeyColumn;
    if (primaryKeytbl == foreignKeyTbl) {
      foreignKeyColumn = "col2";
    } else {
      foreignKeyColumn = "col1";
    }
    SQLForeignKey key = new SQLForeignKey(primaryKeytbl.getDbName(), primaryKeytbl.getTableName(), "col1",
            foreignKeyTbl.getDbName(), foreignKeyTbl.getTableName(), foreignKeyColumn,
            1,1,1, fKeyName, "pk1", false, false, false);
    key.setCatName(DEFAULT_CATALOG_NAME);

    return Collections.singletonList(key);
  }

  private List<SQLDefaultConstraint> createDefaultConstraint(Table tbl) {
    SQLDefaultConstraint dc =
        new SQLDefaultConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), "col1", "1",
            "dc1", false, false, false);
    return Collections.singletonList(dc);
  }

  private List<SQLCheckConstraint> createCheckConstraint(Table tbl) {
    SQLCheckConstraint cc = new SQLCheckConstraint(DEFAULT_CATALOG_NAME, tbl.getDbName(), tbl.getTableName(), "col1", "1",
        "cc1", false, false, false);
    return Collections.singletonList(cc);
  }

  private Table createTable(String dbName, String tblName, StorageDescriptor sd, List<FieldSchema> ptnCols) {
    sd.setStoredAsSubDirectories(false);
    Table tbl = new Table(tblName, dbName, "hive", 0, 0, 0, sd, ptnCols, Collections.emptyMap(), null, null,
        TableType.MANAGED_TABLE.toString());
    tbl.setCatName(DEFAULT_CATALOG_NAME);
    return tbl;
  }

  private StorageDescriptor createStorageDescriptor(List<FieldSchema> cols) {
    return new StorageDescriptor(cols, null, "input", "output", false, 0,
        new SerDeInfo("serde", "seriallib", new HashMap<>()), null, null, Collections.emptyMap());
  }

  private Database createDatabaseObject(String dbName, String dbOwner) {
    String dbLocation = "file:/tmp";
    Map<String, String> dbParams = new HashMap<>();
    Database db = new Database(dbName, dbName, dbLocation, dbParams);
    db.setOwnerName(dbOwner);
    db.setOwnerType(PrincipalType.USER);
    db.setCatalogName(DEFAULT_CATALOG_NAME);
    db.setCreateTime((int) (System.currentTimeMillis() / 1000));
    db.setType(DatabaseType.NATIVE);
    return db;
  }

  private Partition createPartition(Table tbl, List<String> values) {
    Partition ptn =
        new Partition(values, tbl.getDbName(), tbl.getTableName(), 0, 0, tbl.getSd(), Collections.emptyMap());
    ptn.setCatName(DEFAULT_CATALOG_NAME);
    return ptn;
  }

  /**
   * Create an unpartitoned table object for the given db.
   * The table has 9 types of columns
   * @param db the database for which the unpartitioned table is created
   * @return the unpartitioned table just created
   */
  private Table createUnpartitionedTableObject(Database db) {
    String dbName = db.getName();
    String owner = db.getName();
    String serdeLocation = "file:/tmp";
    Map<String, String> serdeParams = new HashMap<>();
    Map<String, String> tblParams = new HashMap<>();
    SerDeInfo serdeInfo = new SerDeInfo("serde", "seriallib", new HashMap<>());
    FieldSchema col1 = new FieldSchema("col1", "binary", "binary column");
    FieldSchema col2 = new FieldSchema("col2", "boolean", "boolean column");
    FieldSchema col3 = new FieldSchema("col3", "date", "date column");
    FieldSchema col4 = new FieldSchema("col4", "decimal", "decimal column");
    FieldSchema col5 = new FieldSchema("col5", "double", "double column");
    FieldSchema col6 = new FieldSchema("col6", "float", "float column");
    FieldSchema col7 = new FieldSchema("col7", "int", "int column");
    FieldSchema col8 = new FieldSchema("col8", "string", "string column");
    List<FieldSchema> cols = Arrays.asList(col1, col2, col3, col4, col5, col6, col7, col8);
    StorageDescriptor sd =
        new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0, serdeInfo, null, null, serdeParams);
    sd.setStoredAsSubDirectories(false);
    Table tbl = new Table(dbName + "_unptntbl1", dbName, owner, 0, 0, 0, sd, new ArrayList<>(), tblParams, null, null,
        TableType.MANAGED_TABLE.toString());
    tbl.setCatName(DEFAULT_CATALOG_NAME);
    tbl.setWriteId(0);
    return tbl;
  }

  /**
   * Create a partitoned table object for the given db.
   * The table has 9 types of columns.
   * The partition columns are string and integer
   * @param db the database into which the partitioned table is created
   * @return the partitioned table just created
   */
  private Table createPartitionedTableObject(Database db) {
    FieldSchema ptnCol1 = new FieldSchema("partCol1", "string", "string partition column");
    FieldSchema ptnCol2 = new FieldSchema("partCol2", "int", "integer partition column");
    List<FieldSchema> ptnCols = Arrays.asList(ptnCol1, ptnCol2);
    Table tbl = createUnpartitionedTableObject(db);
    tbl.setTableName(db.getName() + "_ptntbl1");
    tbl.setPartitionKeys(ptnCols);
    return tbl;
  }

  /**
   * Create 25 partition objects for table returned by createPartitionedTableObject
   * Partitions are: a/1, a/2, ... e/4, e/5
   * @param table the table for which the partitions are created
   * @return an object encapsulating the partition objects and names
   */
  private PartitionObjectsAndNames createPartitionObjects(Table table) {
    List<String> partColNames = new ArrayList<>();
    for (FieldSchema col : table.getPartitionKeys()) {
      partColNames.add(col.getName());
    }
    List<Partition> ptns = new ArrayList<>();
    List<String> ptnNames = new ArrayList<>();
    List<String> ptnCol1Vals = Arrays.asList("a", "b", "c", "d", "e");
    List<String> ptnCol2Vals = Arrays.asList("1", "2", "3", "4", "5");
    for (String ptnCol1Val : ptnCol1Vals) {
      for (String ptnCol2Val : ptnCol2Vals) {
        List<String> partVals = Arrays.asList(ptnCol1Val, ptnCol2Val);
        Partition ptn = createPartition(table, partVals);
        ptn.setCatName(DEFAULT_CATALOG_NAME);
        ptns.add(ptn);
        ptnNames.add(FileUtils.makePartName(partColNames, partVals));
      }
    }
    return new PartitionObjectsAndNames(ptns, ptnNames);
  }

  static class PartitionObjectsAndNames {
    private final List<Partition> ptns;
    private final List<String> ptnNames;

    PartitionObjectsAndNames(List<Partition> ptns, List<String> ptnNames) {
      this.ptns = ptns;
      this.ptnNames = ptnNames;
    }

    List<Partition> getPartitions() {
      return ptns;
    }

    List<String> getPartitionNames() {
      return ptnNames;
    }
  }

  private void assertLongStatsEquals(ColumnStatistics expected, ColumnStatistics actual) {
    Assert.assertEquals(expected.getStatsObjSize(), actual.getStatsObjSize());
    for (int i = 0; i < expected.getStatsObjSize(); i++) {
      LongColumnStatsData expectedData = expected.getStatsObj().get(i).getStatsData().getLongStats();
      LongColumnStatsData actualData = expected.getStatsObj().get(i).getStatsData().getLongStats();
      Assert.assertEquals(expectedData.getNumDVs(), actualData.getNumDVs());
      Assert.assertEquals(expectedData.getNumNulls(), actualData.getNumNulls());
      Assert.assertEquals(expectedData.getHighValue(), actualData.getHighValue());
      Assert.assertEquals(expectedData.getLowValue(), actualData.getLowValue());
      Assert.assertArrayEquals(expectedData.getBitVectors(), actualData.getBitVectors());
    }
  }

  // This method will return only after the cache has updated once
  private void updateCache(CachedStore cachedStore) throws Exception {
    int maxTries = 100;
    long updateCountBefore = cachedStore.getCacheUpdateCount();
    // Start the CachedStore update service
    CachedStore.startCacheUpdateService(cachedStore.getConf(), true, false);
    while ((cachedStore.getCacheUpdateCount() != (updateCountBefore + 1)) && (maxTries-- > 0)) {
      Thread.sleep(1000);
    }
    if (maxTries <= 0) {
      throw new Exception("Unable to update SharedCache in 100 attempts; possibly some bug");
    }
    CachedStore.stopCacheUpdateService(100);
  }
}
