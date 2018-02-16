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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.TestObjectStore.MockPartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.BasicTxnInfo;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.StringColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreCheckinTest.class)
public class TestCachedStore {

  private ObjectStore objectStore;
  private CachedStore cachedStore;
  private SharedCache sharedCache;

  @Before
  public void setUp() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    objectStore = new ObjectStore();
    objectStore.setConf(conf);
    cachedStore = new CachedStore();
    cachedStore.setConf(conf);
    // Stop the CachedStore cache update service. We'll start it explicitly to control the test
    CachedStore.stopCacheUpdateService(1);
    cachedStore.setInitializedForTest();

    // Stop the CachedStore cache update service. We'll start it explicitly to control the test
    CachedStore.stopCacheUpdateService(1);
    sharedCache = new SharedCache();
    sharedCache.getDatabaseCache().clear();
    sharedCache.getTableCache().clear();
    sharedCache.getPartitionCache().clear();
    sharedCache.getSdCache().clear();
    sharedCache.getPartitionColStatsCache().clear();
  }

  /**********************************************************************************************
   * Methods that test CachedStore
   *********************************************************************************************/

  @Test
  public void testDatabaseOps() throws Exception {
    // Add a db via ObjectStore
    String dbName = "testDatabaseOps";
    String dbDescription = "testDatabaseOps";
    String dbLocation = "file:/tmp";
    Map<String, String> dbParams = new HashMap<>();
    String dbOwner = "user1";
    Database db = new Database(dbName, dbDescription, dbLocation, dbParams);
    db.setOwnerName(dbOwner);
    db.setOwnerType(PrincipalType.USER);
    objectStore.createDatabase(db);
    db = objectStore.getDatabase(dbName);
    // Prewarm CachedStore
    CachedStore.prewarm(objectStore);

    // Read database via CachedStore
    Database dbNew = cachedStore.getDatabase(dbName);
    Assert.assertEquals(db, dbNew);

    // Add another db via CachedStore
    final String dbName1 = "testDatabaseOps1";
    final String dbDescription1 = "testDatabaseOps1";
    Database db1 = new Database(dbName1, dbDescription1, dbLocation, dbParams);
    db1.setOwnerName(dbOwner);
    db1.setOwnerType(PrincipalType.USER);
    cachedStore.createDatabase(db1);
    db1 = cachedStore.getDatabase(dbName1);

    // Read db via ObjectStore
    dbNew = objectStore.getDatabase(dbName1);
    Assert.assertEquals(db1, dbNew);

    // Alter the db via CachedStore (can only alter owner or parameters)
    db = new Database(dbName, dbDescription, dbLocation, dbParams);
    dbOwner = "user2";
    db.setOwnerName(dbOwner);
    db.setOwnerType(PrincipalType.USER);
    cachedStore.alterDatabase(dbName, db);
    db = cachedStore.getDatabase(dbName);

    // Read db via ObjectStore
    dbNew = objectStore.getDatabase(dbName);
    Assert.assertEquals(db, dbNew);

    // Add another db via ObjectStore
    final String dbName2 = "testDatabaseOps2";
    final String dbDescription2 = "testDatabaseOps2";
    Database db2 = new Database(dbName2, dbDescription2, dbLocation, dbParams);
    db2.setOwnerName(dbOwner);
    db2.setOwnerType(PrincipalType.USER);
    objectStore.createDatabase(db2);
    db2 = objectStore.getDatabase(dbName2);

    // Alter db "testDatabaseOps" via ObjectStore
    dbOwner = "user1";
    db = new Database(dbName, dbDescription, dbLocation, dbParams);
    db.setOwnerName(dbOwner);
    db.setOwnerType(PrincipalType.USER);
    objectStore.alterDatabase(dbName, db);
    db = objectStore.getDatabase(dbName);

    // Drop db "testDatabaseOps1" via ObjectStore
    objectStore.dropDatabase(dbName1);

    // We update twice to accurately detect if cache is dirty or not
    updateCache(cachedStore, 100, 500, 100);
    updateCache(cachedStore, 100, 500, 100);

    // Read the newly added db via CachedStore
    dbNew = cachedStore.getDatabase(dbName2);
    Assert.assertEquals(db2, dbNew);

    // Read the altered db via CachedStore (altered user from "user2" to "user1")
    dbNew = cachedStore.getDatabase(dbName);
    Assert.assertEquals(db, dbNew);

    // Try to read the dropped db after cache update
    try {
      dbNew = cachedStore.getDatabase(dbName1);
      Assert.fail("The database: " + dbName1
          + " should have been removed from the cache after running the update service");
    } catch (NoSuchObjectException e) {
      // Expected
    }

    // Clean up
    objectStore.dropDatabase(dbName);
    objectStore.dropDatabase(dbName2);
  }

  @Test
  public void testTableOps() throws Exception {
    // Add a db via ObjectStore
    String dbName = "testTableOps";
    String dbDescription = "testTableOps";
    String dbLocation = "file:/tmp";
    Map<String, String> dbParams = new HashMap<>();
    String dbOwner = "user1";
    Database db = new Database(dbName, dbDescription, dbLocation, dbParams);
    db.setOwnerName(dbOwner);
    db.setOwnerType(PrincipalType.USER);
    objectStore.createDatabase(db);
    db = objectStore.getDatabase(dbName);

    // Add a table via ObjectStore
    String tblName = "tbl";
    String tblOwner = "user1";
    String serdeLocation = "file:/tmp";
    FieldSchema col1 = new FieldSchema("col1", "int", "integer column");
    FieldSchema col2 = new FieldSchema("col2", "string", "string column");
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(col1);
    cols.add(col2);
    Map<String, String> serdeParams = new HashMap<>();
    Map<String, String> tblParams = new HashMap<>();
    SerDeInfo serdeInfo = new SerDeInfo("serde", "seriallib", new HashMap<>());
    StorageDescriptor sd =
        new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0, serdeInfo, null,
            null, serdeParams);
    sd.setStoredAsSubDirectories(false);
    Table tbl =
        new Table(tblName, dbName, tblOwner, 0, 0, 0, sd, new ArrayList<>(), tblParams,
            null, null, TableType.MANAGED_TABLE.toString());
    objectStore.createTable(tbl);
    tbl = objectStore.getTable(dbName, tblName);

    // Prewarm CachedStore
    CachedStore.prewarm(objectStore);

    // Read database, table via CachedStore
    Database dbNew = cachedStore.getDatabase(dbName);
    Assert.assertEquals(db, dbNew);
    Table tblNew = cachedStore.getTable(dbName, tblName);
    Assert.assertEquals(tbl, tblNew);

    // Add a new table via CachedStore
    String tblName1 = "tbl1";
    Table tbl1 =
        new Table(tblName1, dbName, tblOwner, 0, 0, 0, sd, new ArrayList<>(), tblParams,
            null, null, TableType.MANAGED_TABLE.toString());
    cachedStore.createTable(tbl1);
    tbl1 = cachedStore.getTable(dbName, tblName1);

    // Read via object store
    tblNew = objectStore.getTable(dbName, tblName1);
    Assert.assertEquals(tbl1, tblNew);

    // Add a new table via ObjectStore
    String tblName2 = "tbl2";
    Table tbl2 =
        new Table(tblName2, dbName, tblOwner, 0, 0, 0, sd, new ArrayList<>(), tblParams,
            null, null, TableType.MANAGED_TABLE.toString());
    objectStore.createTable(tbl2);
    tbl2 = objectStore.getTable(dbName, tblName2);

    // Alter table "tbl" via ObjectStore
    tblOwner = "user2";
    tbl =
        new Table(tblName, dbName, tblOwner, 0, 0, 0, sd, new ArrayList<>(), tblParams,
            null, null, TableType.MANAGED_TABLE.toString());
    objectStore.alterTable(dbName, tblName, tbl);
    tbl = objectStore.getTable(dbName, tblName);

    // Drop table "tbl1" via ObjectStore
    objectStore.dropTable(dbName, tblName1);

    // We update twice to accurately detect if cache is dirty or not
    updateCache(cachedStore, 100, 500, 100);
    updateCache(cachedStore, 100, 500, 100);

    // Read "tbl2" via CachedStore
    tblNew = cachedStore.getTable(dbName, tblName2);
    Assert.assertEquals(tbl2, tblNew);

    // Read the altered "tbl" via CachedStore
    tblNew = cachedStore.getTable(dbName, tblName);
    Assert.assertEquals(tbl, tblNew);

    // Try to read the dropped "tbl1" via CachedStore (should throw exception)
    tblNew = cachedStore.getTable(dbName, tblName1);
    Assert.assertNull(tblNew);

    // Should return "tbl" and "tbl2"
    List<String> tblNames = cachedStore.getTables(dbName, "*");
    Assert.assertTrue(tblNames.contains(tblName));
    Assert.assertTrue(!tblNames.contains(tblName1));
    Assert.assertTrue(tblNames.contains(tblName2));

    // Clean up
    objectStore.dropTable(dbName, tblName);
    objectStore.dropTable(dbName, tblName2);
    objectStore.dropDatabase(dbName);
  }

  @Test
  public void testPartitionOps() throws Exception {
    // Add a db via ObjectStore
    String dbName = "testPartitionOps";
    String dbDescription = "testPartitionOps";
    String dbLocation = "file:/tmp";
    Map<String, String> dbParams = new HashMap<>();
    String dbOwner = "user1";
    Database db = new Database(dbName, dbDescription, dbLocation, dbParams);
    db.setOwnerName(dbOwner);
    db.setOwnerType(PrincipalType.USER);
    objectStore.createDatabase(db);
    db = objectStore.getDatabase(dbName);

    // Add a table via ObjectStore
    String tblName = "tbl";
    String tblOwner = "user1";
    String serdeLocation = "file:/tmp";
    FieldSchema col1 = new FieldSchema("col1", "int", "integer column");
    FieldSchema col2 = new FieldSchema("col2", "string", "string column");
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(col1);
    cols.add(col2);
    Map<String, String> serdeParams = new HashMap<>();
    Map<String, String> tblParams = new HashMap<>();
    SerDeInfo serdeInfo = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd =
        new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0, serdeInfo, null,
            null, serdeParams);
    FieldSchema ptnCol1 = new FieldSchema("part1", "string", "string partition column");
    List<FieldSchema> ptnCols = new ArrayList<>();
    ptnCols.add(ptnCol1);
    Table tbl =
        new Table(tblName, dbName, tblOwner, 0, 0, 0, sd, ptnCols, tblParams, null, null,
            TableType.MANAGED_TABLE.toString());
    objectStore.createTable(tbl);
    tbl = objectStore.getTable(dbName, tblName);
    final String ptnColVal1 = "aaa";
    Map<String, String> partParams = new HashMap<>();
    Partition ptn1 =
        new Partition(Arrays.asList(ptnColVal1), dbName, tblName, 0, 0, sd, partParams);
    objectStore.addPartition(ptn1);
    ptn1 = objectStore.getPartition(dbName, tblName, Arrays.asList(ptnColVal1));
    final String ptnColVal2 = "bbb";
    Partition ptn2 =
        new Partition(Arrays.asList(ptnColVal2), dbName, tblName, 0, 0, sd, partParams);
    objectStore.addPartition(ptn2);
    ptn2 = objectStore.getPartition(dbName, tblName, Arrays.asList(ptnColVal2));

    // Prewarm CachedStore
    CachedStore.prewarm(objectStore);

    // Read database, table, partition via CachedStore
    Database dbNew = cachedStore.getDatabase(dbName);
    Assert.assertEquals(db, dbNew);
    Table tblNew = cachedStore.getTable(dbName, tblName);
    Assert.assertEquals(tbl, tblNew);
    Partition newPtn1 = cachedStore.getPartition(dbName, tblName, Arrays.asList(ptnColVal1));
    Assert.assertEquals(ptn1, newPtn1);
    Partition newPtn2 = cachedStore.getPartition(dbName, tblName, Arrays.asList(ptnColVal2));
    Assert.assertEquals(ptn2, newPtn2);

    // Add a new partition via ObjectStore
    final String ptnColVal3 = "ccc";
    Partition ptn3 =
        new Partition(Arrays.asList(ptnColVal3), dbName, tblName, 0, 0, sd, partParams);
    objectStore.addPartition(ptn3);
    ptn3 = objectStore.getPartition(dbName, tblName, Arrays.asList(ptnColVal3));

    // Alter an existing partition ("aaa") via ObjectStore
    final String ptnColVal1Alt = "aaaAlt";
    Partition ptn1Atl =
        new Partition(Arrays.asList(ptnColVal1Alt), dbName, tblName, 0, 0, sd, partParams);
    objectStore.alterPartition(dbName, tblName, Arrays.asList(ptnColVal1), ptn1Atl);
    ptn1Atl = objectStore.getPartition(dbName, tblName, Arrays.asList(ptnColVal1Alt));

    // Drop an existing partition ("bbb") via ObjectStore
    objectStore.dropPartition(dbName, tblName, Arrays.asList(ptnColVal2));

    // We update twice to accurately detect if cache is dirty or not
    updateCache(cachedStore, 100, 500, 100);
    updateCache(cachedStore, 100, 500, 100);

    // Read the newly added partition via CachedStore
    Partition newPtn = cachedStore.getPartition(dbName, tblName, Arrays.asList(ptnColVal3));
    Assert.assertEquals(ptn3, newPtn);

    // Read the altered partition via CachedStore
    newPtn = cachedStore.getPartition(dbName, tblName, Arrays.asList(ptnColVal1Alt));
    Assert.assertEquals(ptn1Atl, newPtn);

    // Try to read the dropped partition via CachedStore
    try {
      newPtn = cachedStore.getPartition(dbName, tblName, Arrays.asList(ptnColVal2));
      Assert.fail("The partition: " + ptnColVal2
          + " should have been removed from the cache after running the update service");
    } catch (NoSuchObjectException e) {
      // Expected
    }
  }

  //@Test
  public void testTableColStatsOps() throws Exception {
    // Add a db via ObjectStore
    String dbName = "testTableColStatsOps";
    String dbDescription = "testTableColStatsOps";
    String dbLocation = "file:/tmp";
    Map<String, String> dbParams = new HashMap<>();
    String dbOwner = "user1";
    Database db = new Database(dbName, dbDescription, dbLocation, dbParams);
    db.setOwnerName(dbOwner);
    db.setOwnerType(PrincipalType.USER);
    objectStore.createDatabase(db);
    db = objectStore.getDatabase(dbName);

    // Add a table via ObjectStore
    final String tblName = "tbl";
    final String tblOwner = "user1";
    final String serdeLocation = "file:/tmp";
    final FieldSchema col1 = new FieldSchema("col1", "int", "integer column");
    // Stats values for col1
    long col1LowVal = 5;
    long col1HighVal = 500;
    long col1Nulls = 10;
    long col1DV = 20;
    final  FieldSchema col2 = new FieldSchema("col2", "string", "string column");
    // Stats values for col2
    long col2MaxColLen = 100;
    double col2AvgColLen = 45.5;
    long col2Nulls = 5;
    long col2DV = 40;
    final FieldSchema col3 = new FieldSchema("col3", "boolean", "boolean column");
    // Stats values for col3
    long col3NumTrues = 100;
    long col3NumFalses = 30;
    long col3Nulls = 10;
    final List<FieldSchema> cols = new ArrayList<>();
    cols.add(col1);
    cols.add(col2);
    cols.add(col3);
    Map<String, String> serdeParams = new HashMap<>();
    Map<String, String> tblParams = new HashMap<>();
    final SerDeInfo serdeInfo = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd =
        new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0, serdeInfo, null,
            null, serdeParams);
    Table tbl =
        new Table(tblName, dbName, tblOwner, 0, 0, 0, sd, new ArrayList<>(), tblParams,
            null, null, TableType.MANAGED_TABLE.toString());
    objectStore.createTable(tbl);
    tbl = objectStore.getTable(dbName, tblName);

    // Add ColumnStatistics for tbl to metastore DB via ObjectStore
    ColumnStatistics stats = new ColumnStatistics();
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, dbName, tblName);
    List<ColumnStatisticsObj> colStatObjs = new ArrayList<>();

    // Col1
    ColumnStatisticsData data1 = new ColumnStatisticsData();
    ColumnStatisticsObj col1Stats = new ColumnStatisticsObj(col1.getName(), col1.getType(), data1);
    LongColumnStatsDataInspector longStats = new LongColumnStatsDataInspector();
    longStats.setLowValue(col1LowVal);
    longStats.setHighValue(col1HighVal);
    longStats.setNumNulls(col1Nulls);
    longStats.setNumDVs(col1DV);
    data1.setLongStats(longStats);
    colStatObjs.add(col1Stats);

    // Col2
    ColumnStatisticsData data2 = new ColumnStatisticsData();
    ColumnStatisticsObj col2Stats = new ColumnStatisticsObj(col2.getName(), col2.getType(), data2);
    StringColumnStatsDataInspector stringStats = new StringColumnStatsDataInspector();
    stringStats.setMaxColLen(col2MaxColLen);
    stringStats.setAvgColLen(col2AvgColLen);
    stringStats.setNumNulls(col2Nulls);
    stringStats.setNumDVs(col2DV);
    data2.setStringStats(stringStats);
    colStatObjs.add(col2Stats);

    // Col3
    ColumnStatisticsData data3 = new ColumnStatisticsData();
    ColumnStatisticsObj col3Stats = new ColumnStatisticsObj(col3.getName(), col3.getType(), data3);
    BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
    boolStats.setNumTrues(col3NumTrues);
    boolStats.setNumFalses(col3NumFalses);
    boolStats.setNumNulls(col3Nulls);
    data3.setBooleanStats(boolStats);
    colStatObjs.add(col3Stats);

    stats.setStatsDesc(statsDesc);
    stats.setStatsObj(colStatObjs);

    // Save to DB
    objectStore.updateTableColumnStatistics(stats);

    // Prewarm CachedStore
    CachedStore.prewarm(objectStore);

    // Read table stats via CachedStore
    ColumnStatistics newStats =
        cachedStore.getTableColumnStatistics(dbName, tblName,
            Arrays.asList(col1.getName(), col2.getName(), col3.getName()));
    Assert.assertEquals(stats, newStats);
  }

  private void updateCache(CachedStore cachedStore, long frequency, long sleepTime,
      long shutdownTimeout) throws InterruptedException {
    // Set cache refresh period to 100 milliseconds
    CachedStore.setCacheRefreshPeriod(100);
    // Start the CachedStore update service
    CachedStore.startCacheUpdateService(cachedStore.getConf());
    // Sleep for 500 ms so that cache update is complete
    Thread.sleep(500);
    // Stop cache update service
    CachedStore.stopCacheUpdateService(100);
  }

  /**********************************************************************************************
   * Methods that test SharedCache
   *********************************************************************************************/

  @Test
  public void testSharedStoreDb() {
    Database db1 = new Database();
    Database db2 = new Database();
    Database db3 = new Database();
    Database newDb1 = new Database();
    newDb1.setName("db1");

    sharedCache.addDatabaseToCache("db1", db1);
    sharedCache.addDatabaseToCache("db2", db2);
    sharedCache.addDatabaseToCache("db3", db3);

    Assert.assertEquals(sharedCache.getCachedDatabaseCount(), 3);

    sharedCache.alterDatabaseInCache("db1", newDb1);

    Assert.assertEquals(sharedCache.getCachedDatabaseCount(), 3);

    sharedCache.removeDatabaseFromCache("db2");

    Assert.assertEquals(sharedCache.getCachedDatabaseCount(), 2);

    List<String> dbs = sharedCache.listCachedDatabases();
    Assert.assertEquals(dbs.size(), 2);
    Assert.assertTrue(dbs.contains("db1"));
    Assert.assertTrue(dbs.contains("db3"));
  }

  @Test
  public void testSharedStoreTable() {
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
    newSd1.setParameters(params1);
    newSd1.setLocation("loc1");
    newTbl1.setSd(newSd1);
    newTbl1.setPartitionKeys(new ArrayList<>());

    sharedCache.addTableToCache("db1", "tbl1", tbl1);
    sharedCache.addTableToCache("db1", "tbl2", tbl2);
    sharedCache.addTableToCache("db1", "tbl3", tbl3);
    sharedCache.addTableToCache("db2", "tbl1", tbl1);

    Assert.assertEquals(sharedCache.getCachedTableCount(), 4);
    Assert.assertEquals(sharedCache.getSdCache().size(), 2);

    Table t = sharedCache.getTableFromCache("db1", "tbl1");
    Assert.assertEquals(t.getSd().getLocation(), "loc1");

    sharedCache.removeTableFromCache("db1", "tbl1");
    Assert.assertEquals(sharedCache.getCachedTableCount(), 3);
    Assert.assertEquals(sharedCache.getSdCache().size(), 2);

    sharedCache.alterTableInCache("db2", "tbl1", newTbl1);
    Assert.assertEquals(sharedCache.getCachedTableCount(), 3);
    Assert.assertEquals(sharedCache.getSdCache().size(), 3);

    sharedCache.removeTableFromCache("db1", "tbl2");
    Assert.assertEquals(sharedCache.getCachedTableCount(), 2);
    Assert.assertEquals(sharedCache.getSdCache().size(), 2);
  }


  @Test
  public void testSharedStorePartition() {
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
    part1.setValues(Arrays.asList("201701"));

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
    part2.setValues(Arrays.asList("201702"));

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
    part3.setValues(Arrays.asList("201703"));

    Partition newPart1 = new Partition();
    newPart1.setDbName("db1");
    newPart1.setTableName("tbl1");
    StorageDescriptor newSd1 = new StorageDescriptor();
    List<FieldSchema> newCols1 = new ArrayList<>();
    newCols1.add(new FieldSchema("newcol1", "int", ""));
    Map<String, String> newParams1 = new HashMap<>();
    newParams1.put("key", "value");
    newSd1.setCols(newCols1);
    newSd1.setParameters(params1);
    newSd1.setLocation("loc1");
    newPart1.setSd(newSd1);
    newPart1.setValues(Arrays.asList("201701"));

    sharedCache.addPartitionToCache("db1", "tbl1", part1);
    sharedCache.addPartitionToCache("db1", "tbl1", part2);
    sharedCache.addPartitionToCache("db1", "tbl1", part3);
    sharedCache.addPartitionToCache("db1", "tbl2", part1);

    Assert.assertEquals(sharedCache.getCachedPartitionCount(), 4);
    Assert.assertEquals(sharedCache.getSdCache().size(), 2);

    Partition t = sharedCache.getPartitionFromCache("db1", "tbl1", Arrays.asList("201701"));
    Assert.assertEquals(t.getSd().getLocation(), "loc1");

    sharedCache.removePartitionFromCache("db1", "tbl2", Arrays.asList("201701"));
    Assert.assertEquals(sharedCache.getCachedPartitionCount(), 3);
    Assert.assertEquals(sharedCache.getSdCache().size(), 2);

    sharedCache.alterPartitionInCache("db1", "tbl1", Arrays.asList("201701"), newPart1);
    Assert.assertEquals(sharedCache.getCachedPartitionCount(), 3);
    Assert.assertEquals(sharedCache.getSdCache().size(), 3);

    sharedCache.removePartitionFromCache("db1", "tbl1", Arrays.asList("201702"));
    Assert.assertEquals(sharedCache.getCachedPartitionCount(), 2);
    Assert.assertEquals(sharedCache.getSdCache().size(), 2);
  }

  @Test
  public void testAggrStatsRepeatedRead() throws Exception {
    String dbName = "testTableColStatsOps";
    String tblName = "tbl";
    String colName = "f1";

    Database db = new Database(dbName, null, "some_location", null);
    cachedStore.createDatabase(db);

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema(colName, "int", null));
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("col", "int", null));
    StorageDescriptor sd =
        new StorageDescriptor(cols, null, "input", "output", false, 0, new SerDeInfo("serde", "seriallib", new HashMap<>()),
            null, null, null);

    Table tbl =
        new Table(tblName, dbName, null, 0, 0, 0, sd, partCols, new HashMap<>(),
            null, null, TableType.MANAGED_TABLE.toString());
    cachedStore.createTable(tbl);

    List<String> partVals1 = new ArrayList<>();
    partVals1.add("1");
    List<String> partVals2 = new ArrayList<>();
    partVals2.add("2");

    Partition ptn1 =
        new Partition(partVals1, dbName, tblName, 0, 0, sd, new HashMap<>());
    cachedStore.addPartition(ptn1);
    Partition ptn2 =
        new Partition(partVals2, dbName, tblName, 0, 0, sd, new HashMap<>());
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

    cachedStore.updatePartitionColumnStatistics(stats.deepCopy(), partVals1);
    cachedStore.updatePartitionColumnStatistics(stats.deepCopy(), partVals2);

    List<String> colNames = new ArrayList<>();
    colNames.add(colName);
    List<String> aggrPartVals = new ArrayList<>();
    aggrPartVals.add("1");
    aggrPartVals.add("2");
    AggrStats aggrStats = cachedStore.get_aggr_stats_for(dbName, tblName, aggrPartVals, colNames);
    Assert.assertEquals(aggrStats.getColStats().get(0).getStatsData().getLongStats().getNumNulls(), 100);
    aggrStats = cachedStore.get_aggr_stats_for(dbName, tblName, aggrPartVals, colNames);
    Assert.assertEquals(aggrStats.getColStats().get(0).getStatsData().getLongStats().getNumNulls(), 100);
  }

  @Test
  public void testPartitionAggrStats() throws Exception {
    String dbName = "testTableColStatsOps1";
    String tblName = "tbl1";
    String colName = "f1";
    
    Database db = new Database(dbName, null, "some_location", null);
    cachedStore.createDatabase(db);
    
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema(colName, "int", null));
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("col", "int", null));
    StorageDescriptor sd =
        new StorageDescriptor(cols, null, "input", "output", false, 0, new SerDeInfo("serde", "seriallib", new HashMap<>()),
            null, null, null);
    
    Table tbl =
        new Table(tblName, dbName, null, 0, 0, 0, sd, partCols, new HashMap<>(),
            null, null, TableType.MANAGED_TABLE.toString());
    cachedStore.createTable(tbl);
    
    List<String> partVals1 = new ArrayList<>();
    partVals1.add("1");
    List<String> partVals2 = new ArrayList<>();
    partVals2.add("2");
    
    Partition ptn1 =
        new Partition(partVals1, dbName, tblName, 0, 0, sd, new HashMap<>());
    cachedStore.addPartition(ptn1);
    Partition ptn2 =
        new Partition(partVals2, dbName, tblName, 0, 0, sd, new HashMap<>());
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
    
    cachedStore.updatePartitionColumnStatistics(stats.deepCopy(), partVals1);
    
    longStats.setNumDVs(40);
    cachedStore.updatePartitionColumnStatistics(stats.deepCopy(), partVals2);
    
    List<String> colNames = new ArrayList<>();
    colNames.add(colName);
    List<String> aggrPartVals = new ArrayList<>();
    aggrPartVals.add("1");
    aggrPartVals.add("2");
    AggrStats aggrStats = cachedStore.get_aggr_stats_for(dbName, tblName, aggrPartVals, colNames);
    Assert.assertEquals(aggrStats.getColStats().get(0).getStatsData().getLongStats().getNumNulls(), 100);
    Assert.assertEquals(aggrStats.getColStats().get(0).getStatsData().getLongStats().getNumDVs(), 40);
    aggrStats = cachedStore.get_aggr_stats_for(dbName, tblName, aggrPartVals, colNames);
    Assert.assertEquals(aggrStats.getColStats().get(0).getStatsData().getLongStats().getNumNulls(), 100);
    Assert.assertEquals(aggrStats.getColStats().get(0).getStatsData().getLongStats().getNumDVs(), 40);
  }

  @Test
  public void testPartitionAggrStatsBitVector() throws Exception {
    String dbName = "testTableColStatsOps2";
    String tblName = "tbl2";
    String colName = "f1";
    
    Database db = new Database(dbName, null, "some_location", null);
    cachedStore.createDatabase(db);
    
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema(colName, "int", null));
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("col", "int", null));
    StorageDescriptor sd =
        new StorageDescriptor(cols, null, "input", "output", false, 0, new SerDeInfo("serde", "seriallib", new HashMap<>()),
            null, null, null);
    
    Table tbl =
        new Table(tblName, dbName, null, 0, 0, 0, sd, partCols, new HashMap<>(),
            null, null, TableType.MANAGED_TABLE.toString());
    cachedStore.createTable(tbl);
    
    List<String> partVals1 = new ArrayList<>();
    partVals1.add("1");
    List<String> partVals2 = new ArrayList<>();
    partVals2.add("2");
    
    Partition ptn1 =
        new Partition(partVals1, dbName, tblName, 0, 0, sd, new HashMap<>());
    cachedStore.addPartition(ptn1);
    Partition ptn2 =
        new Partition(partVals2, dbName, tblName, 0, 0, sd, new HashMap<>());
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
    
    HyperLogLog hll = HyperLogLog.builder().build();
    hll.addLong(1);
    hll.addLong(2);
    hll.addLong(3);
    longStats.setBitVectors(hll.serialize());
    
    data.setLongStats(longStats);
    colStatObjs.add(colStats);
    
    stats.setStatsDesc(statsDesc);
    stats.setStatsObj(colStatObjs);
    
    cachedStore.updatePartitionColumnStatistics(stats.deepCopy(), partVals1);
    
    longStats.setNumDVs(40);
    hll = HyperLogLog.builder().build();
    hll.addLong(2);
    hll.addLong(3);
    hll.addLong(4);
    hll.addLong(5);
    longStats.setBitVectors(hll.serialize());
    
    cachedStore.updatePartitionColumnStatistics(stats.deepCopy(), partVals2);
    
    List<String> colNames = new ArrayList<>();
    colNames.add(colName);
    List<String> aggrPartVals = new ArrayList<>();
    aggrPartVals.add("1");
    aggrPartVals.add("2");
    AggrStats aggrStats = cachedStore.get_aggr_stats_for(dbName, tblName, aggrPartVals, colNames);
    Assert.assertEquals(aggrStats.getColStats().get(0).getStatsData().getLongStats().getNumNulls(), 100);
    Assert.assertEquals(aggrStats.getColStats().get(0).getStatsData().getLongStats().getNumDVs(), 5);
    aggrStats = cachedStore.get_aggr_stats_for(dbName, tblName, aggrPartVals, colNames);
    Assert.assertEquals(aggrStats.getColStats().get(0).getStatsData().getLongStats().getNumNulls(), 100);
    Assert.assertEquals(aggrStats.getColStats().get(0).getStatsData().getLongStats().getNumDVs(), 5);
  }
}
