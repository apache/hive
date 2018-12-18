package org.apache.hadoop.hive.metastore.cache;

import java.util.*;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.hcatalog.listener.DbNotificationListener;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import jline.internal.Log;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

public class TestCachedStoreUpdateUsingEvents {

  private RawStore rawStore;
  private SharedCache sharedCache;
  private Configuration conf;
  private HiveMetaStore.HMSHandler hmsHandler;

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    // Disable memory estimation for this test class
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY, "-1Kb");
    MetastoreConf.setVar(conf, ConfVars.TRANSACTIONAL_EVENT_LISTENERS, DbNotificationListener.class.getName());
    MetastoreConf.setVar(conf, ConfVars.RAW_STORE_IMPL, "org.apache.hadoop.hive.metastore.cache.CachedStore");
    MetastoreConf.setBoolVar(conf, ConfVars.METASTORE_CACHE_CAN_USE_EVENT, true);
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    hmsHandler = new HiveMetaStore.HMSHandler("testCachedStore", conf, true);

    rawStore = hmsHandler.getMS();
    sharedCache = CachedStore.getSharedCache();

    // Stop the CachedStore cache update service. We'll start it explicitly to control the test
    CachedStore.stopCacheUpdateService(1);

    // Create the 'hive' catalog with new warehouse directory
    HiveMetaStore.HMSHandler.createDefaultCatalog(rawStore, new Warehouse(conf));
  }

  private Database createTestDb(String dbName, String dbOwner) {
    String dbDescription = dbName;
    String dbLocation = "file:/tmp";
    Map<String, String> dbParams = new HashMap<>();
    Database db = new Database(dbName, dbDescription, dbLocation, dbParams);
    db.setOwnerName(dbOwner);
    db.setOwnerType(PrincipalType.USER);
    db.setCatalogName(DEFAULT_CATALOG_NAME);
    return db;
  }

  private Table createTestTblParam(String dbName, String tblName, String tblOwner,
                              List<FieldSchema> cols, List<FieldSchema> ptnCols, Map<String, String> tblParams) {
    String serdeLocation = "file:/tmp";
    Map<String, String> serdeParams = new HashMap<>();
    SerDeInfo serdeInfo = new SerDeInfo("serde", "seriallib", new HashMap<>());
    StorageDescriptor sd = new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0,
            serdeInfo, null, null, serdeParams);
    sd.setStoredAsSubDirectories(false);
    Table tbl = new Table(tblName, dbName, tblOwner, 0, 0, 0, sd, ptnCols, tblParams, null, null,
            TableType.MANAGED_TABLE.toString());
    tbl.setCatName(DEFAULT_CATALOG_NAME);
    return tbl;
  }

  private Table createTestTbl(String dbName, String tblName, String tblOwner,
                              List<FieldSchema> cols, List<FieldSchema> ptnCols) {
    return createTestTblParam(dbName, tblName, tblOwner, cols, ptnCols, new HashMap<>());
  }

  private void compareTables(Table tbl1, Table tbl2) {
    Assert.assertEquals(tbl1.getDbName(), tbl2.getDbName());
    Assert.assertEquals(tbl1.getSd(), tbl2.getSd());
    Assert.assertEquals(tbl1.getParameters(), tbl2.getParameters());
    Assert.assertEquals(tbl1.getTableName(), tbl2.getTableName());
    Assert.assertEquals(tbl1.getCatName(), tbl2.getCatName());
    Assert.assertEquals(tbl1.getCreateTime(), tbl2.getCreateTime());
    Assert.assertEquals(tbl1.getCreationMetadata(), tbl2.getCreationMetadata());
    Assert.assertEquals(tbl1.getId(), tbl2.getId());
  }

  private void comparePartitions(Partition part1, Partition part2) {
    Assert.assertEquals(part1.getParameters(), part2.getParameters());
    Assert.assertEquals(part1.getCatName(), part2.getCatName());
    Assert.assertEquals(part1.getCreateTime(), part2.getCreateTime());
    Assert.assertEquals(part1.getTableName(), part2.getTableName());
    Assert.assertEquals(part1.getDbName().toLowerCase(), part2.getDbName().toLowerCase());
    Assert.assertEquals(part1.getLastAccessTime(), part2.getLastAccessTime());
  }

  @Test
  public void testDatabaseOpsForUpdateUsingEvents() throws Exception {
    RawStore rawStore = hmsHandler.getMS();

    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(rawStore);

    // Add a db via rawStore
    String dbName = "testDatabaseOps";
    String dbOwner = "user1";
    Database db = createTestDb(dbName, dbOwner);

    hmsHandler.create_database(db);
    db = rawStore.getDatabase(DEFAULT_CATALOG_NAME, dbName);

    // Read database via CachedStore
    Database dbRead = sharedCache.getDatabaseFromCache(DEFAULT_CATALOG_NAME, dbName);
    Assert.assertEquals(db, dbRead);

    // Add another db via rawStore
    final String dbName1 = "testDatabaseOps1";
    Database db1 = createTestDb(dbName1, dbOwner);
    hmsHandler.create_database(db1);
    db1 = rawStore.getDatabase(DEFAULT_CATALOG_NAME, dbName1);

    // Read database via CachedStore
    dbRead = sharedCache.getDatabaseFromCache(DEFAULT_CATALOG_NAME, dbName1);
    Assert.assertEquals(db1, dbRead);

    // Alter the db via rawStore (can only alter owner or parameters)
    dbOwner = "user2";
    Database newdb = new Database(db);
    newdb.setOwnerName(dbOwner);
    hmsHandler.alter_database(dbName, newdb);
    newdb = rawStore.getDatabase(DEFAULT_CATALOG_NAME, dbName);

    // Read db via cachedStore
    dbRead = sharedCache.getDatabaseFromCache(DEFAULT_CATALOG_NAME, dbName);
    Assert.assertEquals(newdb, dbRead);

    // Add another db via rawStore
    final String dbName2 = "testDatabaseOps2";
    Database db2 = createTestDb(dbName2, dbOwner);
    hmsHandler.create_database(db2);
    db2 = rawStore.getDatabase(DEFAULT_CATALOG_NAME, dbName2);

    // Alter db "testDatabaseOps" via rawStore
    dbOwner = "user1";
    newdb = new Database(db);
    newdb.setOwnerName(dbOwner);
    hmsHandler.alter_database(dbName, newdb);
    newdb = rawStore.getDatabase(DEFAULT_CATALOG_NAME, dbName);

    // Drop db "testDatabaseOps1" via rawStore
    Database dropDb = rawStore.getDatabase(DEFAULT_CATALOG_NAME, dbName1);
    hmsHandler.drop_database(dbName1, true, true);

    // Read the newly added db via CachedStore
    dbRead = sharedCache.getDatabaseFromCache(DEFAULT_CATALOG_NAME, dbName2);
    Assert.assertEquals(db2, dbRead);

    // Read the altered db via CachedStore (altered user from "user2" to "user1")
    dbRead = sharedCache.getDatabaseFromCache(DEFAULT_CATALOG_NAME, dbName);
    Assert.assertEquals(newdb, dbRead);

    // Try to read the dropped db after cache update
    dbRead = sharedCache.getDatabaseFromCache(DEFAULT_CATALOG_NAME, dbName1);
    Assert.assertEquals(null, dbRead);

    // Clean up
    hmsHandler.drop_database(dbName, true, true);
    hmsHandler.drop_database(dbName2, true, true);
    sharedCache.getDatabaseCache().clear();
    sharedCache.getTableCache().clear();
    sharedCache.getSdCache().clear();
  }

  @Test
  public void testTableOpsForUpdateUsingEvents() throws Exception {
    long lastEventId = -1;
    RawStore rawStore = hmsHandler.getMS();

    // Add a db via rawStore
    String dbName = "test_table_ops";
    String dbOwner = "user1";
    Database db = createTestDb(dbName, dbOwner);
    hmsHandler.create_database(db);
    db = rawStore.getDatabase(DEFAULT_CATALOG_NAME, dbName);

    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(rawStore);

    // Add a table via rawStore
    String tblName = "tbl";
    String tblOwner = "user1";
    FieldSchema col1 = new FieldSchema("col1", "int", "integer column");
    FieldSchema col2 = new FieldSchema("col2", "string", "string column");
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(col1);
    cols.add(col2);
    List<FieldSchema> ptnCols = new ArrayList<FieldSchema>();
    Table tbl = createTestTbl(dbName, tblName, tblOwner, cols, ptnCols);
    hmsHandler.create_table(tbl);
    tbl = rawStore.getTable(DEFAULT_CATALOG_NAME, dbName, tblName);

    // Read database, table via CachedStore
    Database dbRead= sharedCache.getDatabaseFromCache(DEFAULT_CATALOG_NAME, dbName);
    Assert.assertEquals(db, dbRead);
    Table tblRead = sharedCache.getTableFromCache(DEFAULT_CATALOG_NAME, dbName, tblName);
    compareTables(tblRead, tbl);

    // Add a new table via rawStore
    String tblName2 = "tbl2";
    Table tbl2 = createTestTbl(dbName, tblName2, tblOwner, cols, ptnCols);
    hmsHandler.create_table(tbl2);
    tbl2 = rawStore.getTable(DEFAULT_CATALOG_NAME, dbName, tblName2);

    // Alter table "tbl" via rawStore
    tblOwner = "role1";
    Table newTable = new Table(tbl);
    newTable.setOwner(tblOwner);
    newTable.setOwnerType(PrincipalType.ROLE);
    hmsHandler.alter_table(dbName, tblName, newTable);
    newTable = rawStore.getTable(DEFAULT_CATALOG_NAME, dbName, tblName);

    Assert.assertEquals("Owner of the table did not change.", tblOwner, newTable.getOwner());
    Assert.assertEquals("Owner type of the table did not change", PrincipalType.ROLE, newTable.getOwnerType());

    // Drop table "tbl2" via rawStore
    hmsHandler.drop_table(dbName, tblName2, true);

    // Read the altered "tbl" via CachedStore
    tblRead = sharedCache.getTableFromCache(DEFAULT_CATALOG_NAME, dbName, tblName);
    compareTables(tblRead, newTable);

    // Try to read the dropped "tbl2" via CachedStore (should throw exception)
    tblRead = sharedCache.getTableFromCache(DEFAULT_CATALOG_NAME, dbName, tblName2);
    Assert.assertNull(tblRead);

    // Clean up
    hmsHandler.drop_database(dbName, true, true);

    tblRead = sharedCache.getTableFromCache(DEFAULT_CATALOG_NAME, dbName, tblName2);
    Assert.assertNull(tblRead);

    tblRead = sharedCache.getTableFromCache(DEFAULT_CATALOG_NAME, dbName, tblName);
    Assert.assertNull(tblRead);

    sharedCache.getDatabaseCache().clear();
    sharedCache.getTableCache().clear();
    sharedCache.getSdCache().clear();
  }

  @Test
  public void testPartitionOpsForUpdateUsingEvents() throws Exception {
    long lastEventId = -1;
    RawStore rawStore = hmsHandler.getMS();

    // Add a db via rawStore
    String dbName = "test_partition_ops";
    String dbOwner = "user1";
    Database db = createTestDb(dbName, dbOwner);
    hmsHandler.create_database(db);
    db = rawStore.getDatabase(DEFAULT_CATALOG_NAME, dbName);

    // Add a table via rawStore
    String tblName = "tbl";
    String tblOwner = "user1";
    FieldSchema col1 = new FieldSchema("col1", "int", "integer column");
    FieldSchema col2 = new FieldSchema("col2", "string", "string column");
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(col1);
    cols.add(col2);
    FieldSchema ptnCol1 = new FieldSchema("part1", "string", "string partition column");
    List<FieldSchema> ptnCols = new ArrayList<FieldSchema>();
    ptnCols.add(ptnCol1);
    Table tbl = createTestTbl(dbName, tblName, tblOwner, cols, ptnCols);
    hmsHandler.create_table(tbl);
    tbl = rawStore.getTable(DEFAULT_CATALOG_NAME, dbName, tblName);

    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(rawStore);

    final String ptnColVal1 = "aaa";
    Map<String, String> partParams = new HashMap<String, String>();
    Partition ptn1 =
            new Partition(Arrays.asList(ptnColVal1), dbName, tblName, 0, 0, tbl.getSd(), partParams);
    ptn1.setCatName(DEFAULT_CATALOG_NAME);
    hmsHandler.add_partition(ptn1);
    ptn1 = rawStore.getPartition(DEFAULT_CATALOG_NAME, dbName, tblName, Arrays.asList(ptnColVal1));

    final String ptnColVal2 = "bbb";
    Partition ptn2 =
            new Partition(Arrays.asList(ptnColVal2), dbName, tblName, 0, 0, tbl.getSd(), partParams);
    ptn2.setCatName(DEFAULT_CATALOG_NAME);
    hmsHandler.add_partition(ptn2);
    ptn2 = rawStore.getPartition(DEFAULT_CATALOG_NAME, dbName, tblName, Arrays.asList(ptnColVal2));

    // Read database, table, partition via CachedStore
    Database dbRead = sharedCache.getDatabaseFromCache(DEFAULT_CATALOG_NAME.toLowerCase(), dbName.toLowerCase());
    Assert.assertEquals(db, dbRead);
    Table tblRead = sharedCache.getTableFromCache(DEFAULT_CATALOG_NAME.toLowerCase(), dbName.toLowerCase(), tblName.toLowerCase());
    compareTables(tbl, tblRead);
    Partition ptn1Read = sharedCache.getPartitionFromCache(DEFAULT_CATALOG_NAME.toLowerCase(), dbName.toLowerCase(), tblName.toLowerCase(), Arrays.asList(ptnColVal1));
    comparePartitions(ptn1, ptn1Read);
    Partition ptn2Read = sharedCache.getPartitionFromCache(DEFAULT_CATALOG_NAME.toLowerCase(), dbName.toLowerCase(), tblName.toLowerCase(), Arrays.asList(ptnColVal2));
    comparePartitions(ptn2, ptn2Read);

    // Add a new partition via rawStore
    final String ptnColVal3 = "ccc";
    Partition ptn3 =
            new Partition(Arrays.asList(ptnColVal3), dbName, tblName, 0, 0, tbl.getSd(), partParams);
    ptn3.setCatName(DEFAULT_CATALOG_NAME);
    hmsHandler.add_partition(ptn3);
    ptn3 = rawStore.getPartition(DEFAULT_CATALOG_NAME, dbName, tblName, Arrays.asList(ptnColVal3));

    // Alter an existing partition ("aaa") via rawStore
    ptn1 = rawStore.getPartition(DEFAULT_CATALOG_NAME, dbName, tblName, Arrays.asList(ptnColVal1));
    final String ptnColVal1Alt = "aaa";
    Partition ptn1Atl =
            new Partition(Arrays.asList(ptnColVal1Alt), dbName, tblName, 0, 0, tbl.getSd(), partParams);
    ptn1Atl.setCatName(DEFAULT_CATALOG_NAME);
    hmsHandler.alter_partitions(dbName, tblName, Arrays.asList(ptn1Atl));
    ptn1Atl = rawStore.getPartition(DEFAULT_CATALOG_NAME, dbName, tblName, Arrays.asList(ptnColVal1Alt));

    // Drop an existing partition ("bbb") via rawStore
    Partition ptnDrop = rawStore.getPartition(DEFAULT_CATALOG_NAME, dbName, tblName, Arrays.asList(ptnColVal2));
    hmsHandler.drop_partition(dbName, tblName, Arrays.asList(ptnColVal2), false);

    // Read the newly added partition via CachedStore
    Partition ptnRead = sharedCache.getPartitionFromCache(DEFAULT_CATALOG_NAME, dbName, tblName, Arrays.asList(ptnColVal3));
    comparePartitions(ptn3, ptnRead);

    // Read the altered partition via CachedStore
    ptnRead = sharedCache.getPartitionFromCache(DEFAULT_CATALOG_NAME, dbName, tblName, Arrays.asList(ptnColVal1Alt));
    Assert.assertEquals(ptn1Atl.getParameters(), ptnRead.getParameters());

    ptnRead = sharedCache.getPartitionFromCache(DEFAULT_CATALOG_NAME, dbName, tblName, Arrays.asList(ptnColVal2));
    Assert.assertEquals(null, ptnRead);

    // Drop table "tbl" via rawStore, it should remove the partition also
    hmsHandler.drop_table(dbName, tblName, true);

    ptnRead = sharedCache.getPartitionFromCache(DEFAULT_CATALOG_NAME, dbName, tblName, Arrays.asList(ptnColVal1Alt));
    Assert.assertEquals(null, ptnRead);

    ptnRead = sharedCache.getPartitionFromCache(DEFAULT_CATALOG_NAME, dbName, tblName, Arrays.asList(ptnColVal3));
    Assert.assertEquals(null, ptnRead);

    // Clean up
    rawStore.dropDatabase(DEFAULT_CATALOG_NAME, dbName);
    sharedCache.getDatabaseCache().clear();
    sharedCache.getTableCache().clear();
    sharedCache.getSdCache().clear();
  }

  @Test
  public void testTableColumnStatistics() throws Throwable {
    String dbName = "column_stats_test_db";
    String tblName = "tbl";
    String typeName = "person";
    String tblOwner = "testowner";
    int lastAccessed = 6796;
    String dbOwner = "user1";

    // Add a db via rawStore
    Database db = createTestDb(dbName, dbOwner);
    hmsHandler.create_database(db);
    db = rawStore.getDatabase(DEFAULT_CATALOG_NAME, dbName);

    Map<String, String> tableParams =  new HashMap<>();
    tableParams.put("test_param_1", "hi");
    tableParams.put("test_param_2", "50");

    // Add a table via rawStore
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("income", "int", "integer column"));
    cols.add(new FieldSchema("name", "string", "string column"));

    List<FieldSchema> ptnCols = new ArrayList<FieldSchema>();
    ptnCols.add(new FieldSchema("ds", "string", "string partition column"));
    ptnCols.add(new FieldSchema("hr", "int", "integer partition column"));

    Table tbl = createTestTblParam(dbName, tblName, tblOwner, cols, null, tableParams);
    hmsHandler.create_table(tbl);

    // Prewarm CachedStore
    CachedStore.setCachePrewarmedState(false);
    CachedStore.prewarm(rawStore);

    // Create a ColumnStatistics Obj
    String[] colName = new String[]{"income", "name"};
    double lowValue = 50000.21;
    double highValue = 1200000.4525;
    long numNulls = 3;
    long numDVs = 22;
    double avgColLen = 50.30;
    long maxColLen = 102;
    String[] colType = new String[] {"double", "string"};
    boolean isTblLevel = true;
    String partName = null;
    List<ColumnStatisticsObj> statsObjs = new ArrayList<>();

    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
    statsDesc.setDbName(dbName);
    statsDesc.setTableName(tblName);
    statsDesc.setIsTblLevel(isTblLevel);
    statsDesc.setPartName(partName);

    ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
    statsObj.setColName(colName[0]);
    statsObj.setColType(colType[0]);

    ColumnStatisticsData statsData = new ColumnStatisticsData();
    DoubleColumnStatsData numericStats = new DoubleColumnStatsData();
    statsData.setDoubleStats(numericStats);

    statsData.getDoubleStats().setHighValue(highValue);
    statsData.getDoubleStats().setLowValue(lowValue);
    statsData.getDoubleStats().setNumDVs(numDVs);
    statsData.getDoubleStats().setNumNulls(numNulls);

    statsObj.setStatsData(statsData);
    statsObjs.add(statsObj);

    statsObj = new ColumnStatisticsObj();
    statsObj.setColName(colName[1]);
    statsObj.setColType(colType[1]);

    statsData = new ColumnStatisticsData();
    StringColumnStatsData stringStats = new StringColumnStatsData();
    statsData.setStringStats(stringStats);
    statsData.getStringStats().setAvgColLen(avgColLen);
    statsData.getStringStats().setMaxColLen(maxColLen);
    statsData.getStringStats().setNumDVs(numDVs);
    statsData.getStringStats().setNumNulls(numNulls);

    statsObj.setStatsData(statsData);
    statsObjs.add(statsObj);

    ColumnStatistics colStats = new ColumnStatistics();
    colStats.setStatsDesc(statsDesc);
    colStats.setStatsObj(statsObjs);

    // write stats objs persistently
    hmsHandler.update_table_column_statistics(colStats);

    ColumnStatisticsObj colStatsCache = sharedCache.getTableColStatsFromCache(DEFAULT_CATALOG_NAME,
            dbName, tblName, Lists.newArrayList(colName[0])).get(0);
    Assert.assertEquals(colStatsCache.getColName(), colName[0]);
    Assert.assertEquals(colStatsCache.getStatsData().getDoubleStats().getLowValue(), lowValue, 0.01);
    Assert.assertEquals(colStatsCache.getStatsData().getDoubleStats().getHighValue(), highValue, 0.01);
    Assert.assertEquals(colStatsCache.getStatsData().getDoubleStats().getNumNulls(), numNulls);
    Assert.assertEquals(colStatsCache.getStatsData().getDoubleStats().getNumDVs(), numDVs);

    // test delete column stats; if no col name is passed all column stats associated with the
    // table is deleted
    boolean status = hmsHandler.delete_table_column_statistics(dbName, tblName, null);
    Assert.assertEquals(status, true);

    Assert.assertEquals(sharedCache.getTableColStatsFromCache(DEFAULT_CATALOG_NAME,
            dbName, tblName, Lists.newArrayList(colName[0])).isEmpty(), true);

    tblName = "tbl_part";
    cols = new ArrayList<>();
    cols.add(new FieldSchema(colName[0], "int", null));
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("col", "int", null));
    StorageDescriptor sd =
            new StorageDescriptor(cols, null, "input", "output", false,
                    0, new SerDeInfo("serde", "seriallib", new HashMap<>()),
                    null, null, null);

    tbl = new Table(tblName, dbName, null, 0, 0, 0, sd, partCols, new HashMap<>(),
                    null, null, TableType.MANAGED_TABLE.toString());
    tbl.setCatName(DEFAULT_CATALOG_NAME);

    hmsHandler.create_table(tbl);

    List<String> partVals1 = new ArrayList<>();
    partVals1.add("1");
    List<String> partVals2 = new ArrayList<>();
    partVals2.add("2");

    Partition ptn1 =
            new Partition(partVals1, dbName, tblName, 0, 0, sd, new HashMap<>());
    ptn1.setCatName(DEFAULT_CATALOG_NAME);
    hmsHandler.add_partition(ptn1);
    Partition ptn2 =
            new Partition(partVals2, dbName, tblName, 0, 0, sd, new HashMap<>());
    ptn2.setCatName(DEFAULT_CATALOG_NAME);
    hmsHandler.add_partition(ptn2);

    List<String> partitions = hmsHandler.get_partition_names(dbName, tblName, (short)-1);
    partName = partitions.get(0);
    isTblLevel = false;

    // create a new columnstatistics desc to represent partition level column stats
    statsDesc = new ColumnStatisticsDesc();
    statsDesc.setDbName(dbName);
    statsDesc.setTableName(tblName);
    statsDesc.setPartName(partName);
    statsDesc.setIsTblLevel(isTblLevel);

    colStats = new ColumnStatistics();
    colStats.setStatsDesc(statsDesc);
    colStats.setStatsObj(statsObjs);

    hmsHandler.update_partition_column_statistics(colStats);
    ColumnStatisticsObj colStats2 = sharedCache.getPartitionColStatsFromCache(DEFAULT_CATALOG_NAME, dbName, tblName,
            CachedStore.partNameToVals(partName), colName[1]);
    // compare stats obj to ensure what we get is what we wrote
    Assert.assertEquals(colStats.getStatsDesc().getPartName(), partName);
    Assert.assertEquals(colStats2.getColName(), colName[1]);
    Assert.assertEquals(colStats2.getStatsData().getStringStats().getMaxColLen(), maxColLen);
    Assert.assertEquals(colStats2.getStatsData().getStringStats().getAvgColLen(), avgColLen, 0.01);
    Assert.assertEquals(colStats2.getStatsData().getStringStats().getNumNulls(), numNulls);
    Assert.assertEquals(colStats2.getStatsData().getStringStats().getNumDVs(), numDVs);

    // test stats deletion at partition level
    hmsHandler.delete_partition_column_statistics(dbName, tblName, partName, colName[1]);

    colStats2 = sharedCache.getPartitionColStatsFromCache(DEFAULT_CATALOG_NAME, dbName, tblName,
            CachedStore.partNameToVals(partName), colName[1]);
    Assert.assertEquals(colStats2, null);
  }
}
