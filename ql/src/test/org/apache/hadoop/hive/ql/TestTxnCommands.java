/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.TxnInfo;
import org.apache.hadoop.hive.metastore.api.TxnState;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.AcidHouseKeeperService;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.lockmgr.TestDbTxnManager2;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorTestUtilities;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import static java.util.Arrays.asList;
import static org.apache.commons.collections.CollectionUtils.isEqualCollection;
import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE_PATTERN;

/**
 * The LockManager is not ready, but for no-concurrency straight-line path we can
 * test AC=true, and AC=false with commit/rollback/exception and test resulting data.
 *
 * Can also test, calling commit in AC=true mode, etc, toggling AC...
 *
 * Tests here are for multi-statement transactions (WIP) and others
 * Mostly uses bucketed tables
 */
public class TestTxnCommands extends TxnCommandsBaseForTests {
  static final private Logger LOG = LoggerFactory.getLogger(TestTxnCommands.class);
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
      File.separator + TestTxnCommands.class.getCanonicalName() + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  @Override
  protected String getTestDataDir() {
    return TEST_DATA_DIR;
  }

  @Override
  void initHiveConf() {
    super.initHiveConf();
    //TestTxnCommandsWithSplitUpdateAndVectorization has the vectorized version
    //of these tests.
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    HiveConf.setVar(hiveConf, HiveConf.ConfVars.DYNAMIC_PARTITIONING_MODE, "nonstrict");
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_DROP_PARTITION_USE_BASE, false);
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_RENAME_PARTITION_MAKE_COPY, false);
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX, false);
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_TRUNCATE_USE_BASE, false);
    
    MetastoreConf.setClass(hiveConf, MetastoreConf.ConfVars.FILTER_HOOK,
      DummyMetaStoreFilterHookImpl.class, MetaStoreFilterHook.class);

    HiveConf.setVar(hiveConf, HiveConf.ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL, 
      new Path(getWarehouseDir(), "ext").toUri().getPath());
  }

  public static class DummyMetaStoreFilterHookImpl extends DefaultMetaStoreFilterHookImpl {
    private static boolean blockResults = false;
    
    public DummyMetaStoreFilterHookImpl(Configuration conf) {
      super(conf);
    }
    @Override
    public List<String> filterTableNames(String catName, String dbName, List<String> tableList) {
      if (blockResults) {
        return new ArrayList<>();
      }
      return tableList;
    }
  }

  /**
   * tests that a failing Insert Overwrite (which creates a new base_x) is properly marked as
   * aborted.
   */
  @Test
  public void testInsertOverwrite() throws Exception {
    runStatementOnDriver("insert overwrite table " + Table.NONACIDORCTBL + " select a,b from " + Table.NONACIDORCTBL2);
    runStatementOnDriver("create table " + Table.NONACIDORCTBL2 + "3(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("insert into " + Table.ACIDTBL + " values(1,2)");
    List<String> rs = runStatementOnDriver("select a from " + Table.ACIDTBL + " where b = 2");
    Assert.assertEquals(1, rs.size());
    Assert.assertEquals("1", rs.get(0));
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("insert overwrite table " + Table.ACIDTBL + " values(3,2)");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, false);
    runStatementOnDriver("insert into " + Table.ACIDTBL + " values(5,6)");
    rs = runStatementOnDriver("select a from " + Table.ACIDTBL + " order by a");
    Assert.assertEquals(2, rs.size());
    Assert.assertEquals("1", rs.get(0));
    Assert.assertEquals("5", rs.get(1));
  }

  @Ignore("not needed but useful for testing")
  @Test
  public void testNonAcidInsert() throws Exception {
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,2)");
    List<String> rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(2,3)");
    List<String> rs1 = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
  }

  /**
   * Useful for debugging.  Dumps ORC file in JSON to CWD.
   */
  private void dumpBucketData(Table table, long writeId, int stmtId, int bucketNum) throws Exception {
    if(true) {
      return;
    }
    Path bucket = AcidUtils.createBucketFile(new Path(new Path(getWarehouseDir(), table.toString().toLowerCase()), AcidUtils.deltaSubdir(writeId, writeId, stmtId)), bucketNum);
    FileOutputStream delta = new FileOutputStream(testName.getMethodName() + "_" + bucket.getParent().getName() + "_" +  bucket.getName());
//    try {
//      FileDump.printJsonData(conf, bucket.toString(), delta);
//    }
//    catch(FileNotFoundException ex) {
//      ; //this happens if you change BUCKET_COUNT
//    }
    delta.close();
  }
  /**
   * Dump all data in the table by bucket in JSON format
   */
  private void dumpTableData(Table table, long writeId, int stmtId) throws Exception {
    for(int bucketNum = 0; bucketNum < BUCKET_COUNT; bucketNum++) {
      dumpBucketData(table, writeId, stmtId, bucketNum);
    }
  }
  @Test
  public void testSimpleAcidInsert() throws Exception {
    int[][] rows1 = {{1,2},{3,4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows1));
    //List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    //Assert.assertEquals("Data didn't match in autocommit=true (rs)", stringifyValues(rows1), rs);
    runStatementOnDriver("START TRANSACTION");
    int[][] rows2 = {{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows2));
    List<String> allData = stringifyValues(rows1);
    allData.addAll(stringifyValues(rows2));
    List<String> rs0 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Data didn't match inside tx (rs0)", allData, rs0);
    runStatementOnDriver("COMMIT WORK");
    dumpTableData(Table.ACIDTBL, 1, 0);
    dumpTableData(Table.ACIDTBL, 2, 0);
    runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    CommandProcessorException e = runStatementOnDriverNegative("COMMIT"); //txn started implicitly by previous statement
    Assert.assertEquals("Error didn't match: " + e,
        ErrorMsg.OP_NOT_ALLOWED_WITHOUT_TXN.getErrorCode(), e.getErrorCode());
    List<String> rs1 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Data didn't match inside tx (rs0)", allData, rs1);
  }

  @Test
  public void testMmExim() throws Exception {
    String tableName = "mm_table", importName = tableName + "_import";
    runStatementOnDriver("drop table if exists " + tableName);
    runStatementOnDriver(String.format("create table %s (a int, b int) stored as orc " +
        "TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')",
        tableName));

    // Regular insert: export some MM deltas, then import into a new table.
    int[][] rows1 = {{1,2},{3,4}};
    runStatementOnDriver(String.format("insert into %s (a,b) %s",
        tableName, makeValuesClause(rows1)));
    runStatementOnDriver(String.format("insert into %s (a,b) %s",
        tableName, makeValuesClause(rows1)));
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);
    org.apache.hadoop.hive.metastore.api.Table table = msClient.getTable("default", tableName);
    FileSystem fs = FileSystem.get(hiveConf);
    Path exportPath = new Path(table.getSd().getLocation() + "_export");
    fs.delete(exportPath, true);
    runStatementOnDriver(String.format("export table %s to '%s'", tableName, exportPath));
    List<String> paths = listPathsRecursive(fs, exportPath);
    verifyMmExportPaths(paths, 2);
    runStatementOnDriver(String.format("import table %s from '%s'", importName, exportPath));
    org.apache.hadoop.hive.metastore.api.Table imported = msClient.getTable("default", importName);
    Assert.assertEquals(imported.toString(), "insert_only",
        imported.getParameters().get("transactional_properties"));
    Path importPath = new Path(imported.getSd().getLocation());
    FileStatus[] stat = fs.listStatus(importPath, FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(Arrays.toString(stat), 1, stat.length);
    assertIsDelta(stat[0]);
    List<String> allData = stringifyValues(rows1);
    allData.addAll(stringifyValues(rows1));
    allData.sort(null);
    Collections.sort(allData);
    List<String> rs = runStatementOnDriver(
        String.format("select a,b from %s order by a,b", importName));
    Assert.assertEquals("After import: " + rs, allData, rs);
    runStatementOnDriver("drop table if exists " + importName);

    // Do insert overwrite to create some invalid deltas, and import into a non-MM table.
    int[][] rows2 = {{5,6},{7,8}};
    runStatementOnDriver(String.format("insert overwrite table %s %s",
        tableName, makeValuesClause(rows2)));
    fs.delete(exportPath, true);
    runStatementOnDriver(String.format("export table %s to '%s'", tableName, exportPath));
    paths = listPathsRecursive(fs, exportPath);
    verifyMmExportPaths(paths, 1);
    runStatementOnDriver(String.format("create table %s (a int, b int) stored as orc " +
        "TBLPROPERTIES ('transactional'='false')", importName));
    runStatementOnDriver(String.format("import table %s from '%s'", importName, exportPath));
    imported = msClient.getTable("default", importName);
    Assert.assertNull(imported.toString(), imported.getParameters().get("transactional"));
    Assert.assertNull(imported.toString(),
        imported.getParameters().get("transactional_properties"));
    importPath = new Path(imported.getSd().getLocation());
    stat = fs.listStatus(importPath, FileUtils.HIDDEN_FILES_PATH_FILTER);
    allData = stringifyValues(rows2);
    Collections.sort(allData);
    rs = runStatementOnDriver(String.format("select a,b from %s order by a,b", importName));
    Assert.assertEquals("After import: " + rs, allData, rs);
    runStatementOnDriver("drop table if exists " + importName);
    runStatementOnDriver("drop table if exists " + tableName);
    msClient.close();
  }

  private static final class QueryRunnable implements Runnable {
    private final CountDownLatch cdlIn, cdlOut;
    private final String query;
    private final HiveConf hiveConf;

    QueryRunnable(HiveConf hiveConf, String query, CountDownLatch cdlIn, CountDownLatch cdlOut) {
      this.query = query;
      this.cdlIn = cdlIn;
      this.cdlOut = cdlOut;
      this.hiveConf = new HiveConf(hiveConf);
    }

    @Override
    public void run() {
      SessionState ss = SessionState.start(hiveConf);
      try {
        ss.applyAuthorizationPolicy();
      } catch (HiveException e) {
        throw new RuntimeException(e);
      }
      QueryState qs = new QueryState.Builder().withHiveConf(hiveConf).nonIsolated().build();
      try (Driver d = new Driver(qs)) {
        LOG.info("Ready to run the query: " + query);
        syncThreadStart(cdlIn, cdlOut);
        try {
          try {
            d.run(query);
          } catch (CommandProcessorException e) {
            throw new RuntimeException(query + " failed: " + e);
          }
          d.getResults(new ArrayList<String>());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }


  private static void syncThreadStart(final CountDownLatch cdlIn, final CountDownLatch cdlOut) {
    cdlIn.countDown();
    try {
      cdlOut.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testParallelInsertStats() throws Exception {
    final int TASK_COUNT = 4;
    String tableName = "mm_table";
    List<ColumnStatisticsObj> stats;
    IMetaStoreClient msClient = prepareParallelTest(tableName, 0);

    String[] queries = new String[TASK_COUNT];
    for (int i = 0; i < queries.length; ++i) {
      queries[i] = String.format("insert into %s (a) values (" + i + ")", tableName);
    }

    runParallelQueries(queries);

    // Verify stats are either invalid, or valid and correct.
    stats = getTxnTableStats(msClient, tableName);
    boolean hasStats = 0 != stats.size();
    if (hasStats) {
      verifyLongStats(TASK_COUNT, 0, TASK_COUNT - 1, stats);
    }

    runStatementOnDriver(String.format("insert into %s (a) values (" + TASK_COUNT + ")", tableName));
    if (!hasStats) {
      // Stats should still be invalid if they were invalid.
      stats = getTxnTableStats(msClient, tableName);
      Assert.assertEquals(0, stats.size());
    }

    // Stats should be valid after analyze.
    runStatementOnDriver(String.format("analyze table %s compute statistics for columns", tableName));
    verifyLongStats(TASK_COUNT + 1, 0, TASK_COUNT, getTxnTableStats(msClient, tableName));
  }

  private void verifyLongStats(int dvCount, int min, int max, List<ColumnStatisticsObj> stats) {
    Assert.assertEquals(1, stats.size());
    LongColumnStatsData data = stats.get(0).getStatsData().getLongStats();
    Assert.assertEquals(min, data.getLowValue());
    Assert.assertEquals(max, data.getHighValue());
    Assert.assertEquals(dvCount, data.getNumDVs());
  }

  private void runParallelQueries(String[] queries)
      throws InterruptedException, ExecutionException {
    ExecutorService executor = Executors.newFixedThreadPool(queries.length);
    final CountDownLatch cdlIn = new CountDownLatch(queries.length), cdlOut = new CountDownLatch(1);
    Future<?>[] tasks = new Future[queries.length];
    for (int i = 0; i < tasks.length; ++i) {
      tasks[i] = executor.submit(new QueryRunnable(hiveConf, queries[i], cdlIn, cdlOut));
    }
    cdlIn.await(); // Wait for all threads to be ready.
    cdlOut.countDown(); // Release them at the same time.
    for (int i = 0; i < tasks.length; ++i) {
      tasks[i].get();
    }
  }

  private IMetaStoreClient prepareParallelTest(String tableName, int val)
      throws Exception, MetaException, TException, NoSuchObjectException {
    hiveConf.setBoolean("hive.stats.autogather", true);
    hiveConf.setBoolean("hive.stats.column.autogather", true);
    // Need to close the thread local Hive object so that configuration change is reflected to HMS.
    Hive.closeCurrent();
    runStatementOnDriver("drop table if exists " + tableName);
    runStatementOnDriver(String.format("create table %s (a int) stored as orc " +
        "TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')",
        tableName));
    runStatementOnDriver(String.format("insert into %s (a) values (" + val + ")", tableName));
    runStatementOnDriver(String.format("insert into %s (a) values (" + val + ")", tableName));
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);
    // Stats should be valid after serial inserts.
    List<ColumnStatisticsObj> stats = getTxnTableStats(msClient, tableName);
    Assert.assertEquals(1, stats.size());
    return msClient;
  }

  @Test
  public void testAddAndDropConstraintAdvancingWriteIds() throws Exception {

    String tableName = "constraints_table";
    hiveConf.setBoolean("hive.stats.autogather", true);
    hiveConf.setBoolean("hive.stats.column.autogather", true);
    // Need to close the thread local Hive object so that configuration change is reflected to HMS.
    Hive.closeCurrent();
    runStatementOnDriver("drop table if exists " + tableName);
    runStatementOnDriver(String.format("create table %s (a int, b string) stored as orc " +
        "TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')",
        tableName));
    runStatementOnDriver(String.format("insert into %s (a) values (0)", tableName));
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);
    String validWriteIds = msClient.getValidWriteIds("default." + tableName).toString();
    LOG.info("ValidWriteIds before add constraint::"+ validWriteIds);
    Assert.assertEquals("default.constraints_table:1:9223372036854775807::", validWriteIds);
    runStatementOnDriver(String.format("alter table %s  ADD CONSTRAINT a_PK PRIMARY KEY (`a`) DISABLE NOVALIDATE", tableName));
    validWriteIds  = msClient.getValidWriteIds("default." + tableName).toString();
    LOG.info("ValidWriteIds after add constraint primary key::"+ validWriteIds);
    Assert.assertEquals("default.constraints_table:2:9223372036854775807::", validWriteIds);
    runStatementOnDriver(String.format("alter table %s CHANGE COLUMN b b STRING NOT NULL", tableName));
    validWriteIds  = msClient.getValidWriteIds("default." + tableName).toString();
    LOG.info("ValidWriteIds after add constraint not null::"+ validWriteIds);
    Assert.assertEquals("default.constraints_table:3:9223372036854775807::", validWriteIds);
    runStatementOnDriver(String.format("alter table %s ADD CONSTRAINT check1 CHECK (a <= 25)", tableName));
    validWriteIds  = msClient.getValidWriteIds("default." + tableName).toString();
    LOG.info("ValidWriteIds after add constraint check::"+ validWriteIds);
    Assert.assertEquals("default.constraints_table:4:9223372036854775807::", validWriteIds);
    runStatementOnDriver(String.format("alter table %s ADD CONSTRAINT unique1 UNIQUE (a, b) DISABLE", tableName));
    validWriteIds  = msClient.getValidWriteIds("default." + tableName).toString();
    LOG.info("ValidWriteIds after add constraint unique::"+ validWriteIds);
    Assert.assertEquals("default.constraints_table:5:9223372036854775807::", validWriteIds);

    LOG.info("ValidWriteIds before drop constraint::"+ validWriteIds);
    runStatementOnDriver(String.format("alter table %s  DROP CONSTRAINT a_PK", tableName));
    validWriteIds  = msClient.getValidWriteIds("default." + tableName).toString();
    Assert.assertEquals("default.constraints_table:6:9223372036854775807::", validWriteIds);
    LOG.info("ValidWriteIds after drop constraint primary key::"+ validWriteIds);
    runStatementOnDriver(String.format("alter table %s  DROP CONSTRAINT check1", tableName));
    validWriteIds  = msClient.getValidWriteIds("default." + tableName).toString();
    Assert.assertEquals("default.constraints_table:7:9223372036854775807::", validWriteIds);
    LOG.info("ValidWriteIds after drop constraint check::"+ validWriteIds);
    runStatementOnDriver(String.format("alter table %s  DROP CONSTRAINT unique1", tableName));
    validWriteIds  = msClient.getValidWriteIds("default." + tableName).toString();
    Assert.assertEquals("default.constraints_table:8:9223372036854775807::", validWriteIds);
    LOG.info("ValidWriteIds after drop constraint unique::"+ validWriteIds);
    runStatementOnDriver(String.format("alter table %s CHANGE COLUMN b b STRING", tableName));
    validWriteIds  = msClient.getValidWriteIds("default." + tableName).toString();
    Assert.assertEquals("default.constraints_table:9:9223372036854775807::", validWriteIds);

  }

  /**
   * If you are disabling or removing this test case, it probably means now we support exchange partition for
   * transactional tables. If that is the case, we also have to make sure we advance the Write IDs during exchange
   * partition DDL for transactional tables. You can look at https://github.com/apache/hive/pull/2465 as an example.
   * @throws Exception
   */
  @Test
  public void exchangePartitionShouldNotWorkForTransactionalTables() throws Exception {
    runStatementOnDriver("create database IF NOT EXISTS db1");
    runStatementOnDriver("create database IF NOT EXISTS db2");

    runStatementOnDriver("CREATE TABLE db1.exchange_part_test1 (f1 string) PARTITIONED BY (ds STRING)");

    String tableName = "db2.exchange_part_test2";
    runStatementOnDriver(String.format("CREATE TABLE %s (f1 string) PARTITIONED BY (ds STRING) " +
    "TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')"
    ,tableName));

    runStatementOnDriver("ALTER TABLE db2.exchange_part_test2 ADD PARTITION (ds='2013-04-05')");

    try {
      runStatementOnDriver("ALTER TABLE db1.exchange_part_test1 EXCHANGE PARTITION (ds='2013-04-05') " +
              "WITH TABLE db2.exchange_part_test2");
      Assert.fail("Exchange partition should not be allowed for transaction tables" );
    }catch(Exception e) {
      Assert.assertTrue(e.getMessage().contains("Exchange partition is not allowed with transactional tables"));
    }
  }

  @Test
  public void truncateTableAdvancingWriteId() throws Exception {
    runStatementOnDriver("create database IF NOT EXISTS trunc_db");

    String tableName = "trunc_db.trunc_table";
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);

    runStatementOnDriver(String.format("CREATE TABLE %s (f1 string) PARTITIONED BY (ds STRING) " +
                    "TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')"
            , tableName));

    String validWriteIds = msClient.getValidWriteIds(tableName).toString();
    LOG.info("ValidWriteIds before truncate table::" + validWriteIds);
    Assert.assertEquals("trunc_db.trunc_table:0:9223372036854775807::", validWriteIds);

    runStatementOnDriver("TRUNCATE TABLE trunc_db.trunc_table");
    validWriteIds = msClient.getValidWriteIds(tableName).toString();
    LOG.info("ValidWriteIds after truncate table::" + validWriteIds);
    Assert.assertEquals("trunc_db.trunc_table:1:9223372036854775807::", validWriteIds);

  }

  @Test
  public void testAddAndDropPartitionAdvancingWriteIds() throws Exception {
    runStatementOnDriver("create database IF NOT EXISTS db1");

    String tableName = "db1.add_drop_partition";
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);

    runStatementOnDriver(String.format("CREATE TABLE %s (f1 string) PARTITIONED BY (ds STRING) " +
    "TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')"
    ,tableName));

    String validWriteIds = msClient.getValidWriteIds(tableName).toString();
    LOG.info("ValidWriteIds before add partition::"+ validWriteIds);
    Assert.assertEquals("db1.add_drop_partition:0:9223372036854775807::", validWriteIds);
    validWriteIds = msClient.getValidWriteIds(tableName).toString();
    runStatementOnDriver("ALTER TABLE db1.add_drop_partition ADD PARTITION (ds='2013-04-05')");
    validWriteIds = msClient.getValidWriteIds(tableName).toString();
    LOG.info("ValidWriteIds after add partition::"+ validWriteIds);
    Assert.assertEquals("db1.add_drop_partition:1:9223372036854775807::", validWriteIds);
    runStatementOnDriver("ALTER TABLE db1.add_drop_partition DROP PARTITION (ds='2013-04-05')");
    validWriteIds = msClient.getValidWriteIds(tableName).toString();
    LOG.info("ValidWriteIds after drop partition::"+ validWriteIds);
    Assert.assertEquals("db1.add_drop_partition:2:9223372036854775807::", validWriteIds);

  }

  @Test
  public void testDDLsAdvancingWriteIds() throws Exception {
    hiveConf.setBoolVar(HiveConf.ConfVars.TRANSACTIONAL_CONCATENATE_NOBLOCK, true);

    String tableName = "alter_table";
    runStatementOnDriver("drop table if exists " + tableName);
    runStatementOnDriver(String.format("create table %s (a int, b string, c BIGINT, d INT) " +
        "PARTITIONED BY (ds STRING)" +
        "TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')",
        tableName));
    runStatementOnDriver(String.format("insert into %s (a) values (0)", tableName));
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);
    String validWriteIds = msClient.getValidWriteIds("default." + tableName).toString();
    Assert.assertEquals("default.alter_table:1:9223372036854775807::", validWriteIds);

    runStatementOnDriver(String.format("alter table %s SET OWNER USER user_name", tableName));
    validWriteIds  = msClient.getValidWriteIds("default." + tableName).toString();
    Assert.assertEquals("default.alter_table:2:9223372036854775807::", validWriteIds);

    runStatementOnDriver(String.format("alter table %s CLUSTERED BY(c) SORTED BY(d) INTO 32 BUCKETS", tableName));
    validWriteIds  = msClient.getValidWriteIds("default." + tableName).toString();
    Assert.assertEquals("default.alter_table:3:9223372036854775807::", validWriteIds);

    runStatementOnDriver(String.format("ALTER TABLE %s ADD PARTITION (ds='2013-04-05')", tableName));
    validWriteIds  = msClient.getValidWriteIds("default." + tableName).toString();
    Assert.assertEquals("default.alter_table:4:9223372036854775807::", validWriteIds);

    runStatementOnDriver(String.format("ALTER TABLE %s SET SERDEPROPERTIES ('field.delim'='\\u0001')", tableName));
    validWriteIds  = msClient.getValidWriteIds("default." + tableName).toString();
    Assert.assertEquals("default.alter_table:5:9223372036854775807::", validWriteIds);

    runStatementOnDriver(String.format("ALTER TABLE %s PARTITION (ds='2013-04-05') SET FILEFORMAT PARQUET", tableName));
    validWriteIds  = msClient.getValidWriteIds("default." + tableName).toString();
    Assert.assertEquals("default.alter_table:6:9223372036854775807::", validWriteIds);

    // We should not advance the Write ID during compaction, since it affects the performance of
    // materialized views. So, below assertion ensures that we do not advance the write during compaction.
    runStatementOnDriver(String.format("ALTER TABLE %s PARTITION (ds='2013-04-05') COMPACT 'minor'", tableName));
    validWriteIds = msClient.getValidWriteIds("default." + tableName).toString();
    Assert.assertEquals("default.alter_table:6:9223372036854775807::", validWriteIds);

    //Process the compaction request because otherwise the CONCATENATE (major compaction) command on the same table and
    // partition would be refused.
    runWorker(hiveConf);
    runCleaner(hiveConf);

    runStatementOnDriver(String.format("ALTER TABLE %s PARTITION (ds='2013-04-05') CONCATENATE", tableName));
    validWriteIds = msClient.getValidWriteIds("default." + tableName).toString();
    Assert.assertEquals("default.alter_table:7:9223372036854775807::", validWriteIds);

    runStatementOnDriver(String.format("ALTER TABLE %s SKEWED BY (a) ON (1,2)", tableName));
    validWriteIds = msClient.getValidWriteIds("default." + tableName).toString();
    Assert.assertEquals("default.alter_table:8:9223372036854775807::", validWriteIds);

    runStatementOnDriver(String.format("ALTER TABLE %s SET SKEWED LOCATION (1='hdfs://127.0.0.1:8020/abcd/1')",
      tableName));
    validWriteIds = msClient.getValidWriteIds("default." + tableName).toString();
    Assert.assertEquals("default.alter_table:9:9223372036854775807::", validWriteIds);

    runStatementOnDriver(String.format("ALTER TABLE %s NOT SKEWED", tableName));
    validWriteIds = msClient.getValidWriteIds("default." + tableName).toString();
    Assert.assertEquals("default.alter_table:10:9223372036854775807::", validWriteIds);

    runStatementOnDriver(String.format("ALTER TABLE %s UNSET SERDEPROPERTIES ('field.delim')", tableName));
    validWriteIds = msClient.getValidWriteIds("default." + tableName).toString();
    Assert.assertEquals("default.alter_table:11:9223372036854775807::", validWriteIds);

  }

  @Test
  public void testParallelInsertAnalyzeStats() throws Exception {
    String tableName = "mm_table";
    List<ColumnStatisticsObj> stats;
    IMetaStoreClient msClient = prepareParallelTest(tableName, 0);

    String[] queries = {
        String.format("insert into %s (a) values (999)", tableName),
        String.format("analyze table %s compute statistics for columns", tableName)
    };
    runParallelQueries(queries);

    // Verify stats are either invalid, or valid and correct.
    stats = getTxnTableStats(msClient, tableName);
    boolean hasStats = 0 != stats.size();
    if (hasStats) {
      verifyLongStats(2, 0, 999, stats);
    }

    runStatementOnDriver(String.format("insert into %s (a) values (1000)", tableName));
    if (!hasStats) {
      // Stats should still be invalid if they were invalid.
      stats = getTxnTableStats(msClient, tableName);
      Assert.assertEquals(0, stats.size());
    }

    // Stats should be valid after analyze.
    runStatementOnDriver(String.format("analyze table %s compute statistics for columns", tableName));
    verifyLongStats(3, 0, 1000, getTxnTableStats(msClient, tableName));
  }

  @Test
  public void testParallelTruncateAnalyzeStats() throws Exception {
    String tableName = "mm_table";
    List<ColumnStatisticsObj> stats;
    IMetaStoreClient msClient = prepareParallelTest(tableName, 0);

    String[] queries = {
        String.format("truncate table %s", tableName),
        String.format("analyze table %s compute statistics for columns", tableName)
    };
    runParallelQueries(queries);

    // Verify stats are either invalid, or valid and correct.
    stats = getTxnTableStats(msClient, tableName);
    boolean hasStats = 0 != stats.size();
    if (hasStats) {
      // Either the truncate run before or the analyze
      if (stats.get(0).getStatsData().getLongStats().getNumDVs() > 0) {
        verifyLongStats(1, 0, 0, stats);
      } else {
        verifyLongStats(0, 0, 0, stats);
      }
    }

    // Stats should be valid after analyze.
    runStatementOnDriver(String.format("analyze table %s compute statistics for columns", tableName));
    verifyLongStats(0, 0, 0, getTxnTableStats(msClient, tableName));
  }


  @Test
  public void testTxnStatsOnOff() throws Exception {
    String tableName = "mm_table";
    hiveConf.setBoolean("hive.stats.autogather", true);
    hiveConf.setBoolean("hive.stats.column.autogather", true);
    // Need to close the thread local Hive object so that configuration change is reflected to HMS.
    Hive.closeCurrent();
    runStatementOnDriver("drop table if exists " + tableName);
    runStatementOnDriver(String.format("create table %s (a int) stored as orc " +
        "TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')",
        tableName));

    runStatementOnDriver(String.format("insert into %s (a) values (1)", tableName));
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);
    List<ColumnStatisticsObj> stats = getTxnTableStats(msClient, tableName);
    Assert.assertEquals(1, stats.size());
    runStatementOnDriver(String.format("insert into %s (a) values (1)", tableName));
    stats = getTxnTableStats(msClient, tableName);
    Assert.assertEquals(1, stats.size());
    msClient.close();
    hiveConf.setBoolean(MetastoreConf.ConfVars.HIVE_TXN_STATS_ENABLED.getVarname(), false);
    msClient = new HiveMetaStoreClient(hiveConf);
    // Even though the stats are valid in metastore, txn stats are disabled.
    stats = getTxnTableStats(msClient, tableName);
    Assert.assertEquals(0, stats.size());
    msClient.close();
    hiveConf.setBoolean(MetastoreConf.ConfVars.HIVE_TXN_STATS_ENABLED.getVarname(), true);
    msClient = new HiveMetaStoreClient(hiveConf);
    stats = getTxnTableStats(msClient, tableName);
    // Now the stats are visible again.
    Assert.assertEquals(1, stats.size());
    msClient.close();
    hiveConf.setBoolean(MetastoreConf.ConfVars.HIVE_TXN_STATS_ENABLED.getVarname(), false);
    // Need to close the thread local Hive object so that configuration change is reflected to HMS.
    Hive.closeCurrent();
    // Running the query with stats disabled will cause stats in metastore itself to become invalid.
    runStatementOnDriver(String.format("insert into %s (a) values (1)", tableName));
    hiveConf.setBoolean(MetastoreConf.ConfVars.HIVE_TXN_STATS_ENABLED.getVarname(), true);
    msClient = new HiveMetaStoreClient(hiveConf);
    stats = getTxnTableStats(msClient, tableName);
    Assert.assertEquals(0, stats.size());
    msClient.close();
  }

  public List<ColumnStatisticsObj> getTxnTableStats(IMetaStoreClient msClient,
      String tableName) throws TException, NoSuchObjectException, MetaException {
    String validWriteIds;
    List<ColumnStatisticsObj> stats;
    validWriteIds = msClient.getValidWriteIds("default." + tableName).toString();
    stats = msClient.getTableColumnStatistics(
        "default", tableName, Lists.newArrayList("a"), Constants.HIVE_ENGINE, validWriteIds);
    return stats;
  }

  private void assertIsDelta(FileStatus stat) {
    Assert.assertTrue(stat.toString(),
        stat.getPath().getName().startsWith(AcidUtils.DELTA_PREFIX));
  }

  private void verifyMmExportPaths(List<String> paths, int deltasOrBases) {
    // 1 file, 1 dir for each, for now. Plus export "data" dir.
    // This could be changed to a flat file list later.
    Assert.assertEquals(paths.toString(), 2 * deltasOrBases + 1, paths.size());
    // No confusing directories in export.
    for (String path : paths) {
      Assert.assertFalse(path, path.startsWith(AcidUtils.DELTA_PREFIX));
      Assert.assertFalse(path, path.startsWith(AcidUtils.BASE_PREFIX));
    }
  }

  private List<String> listPathsRecursive(FileSystem fs, Path path) throws IOException {
    List<String> paths = new ArrayList<>();
    LinkedList<Path> queue = new LinkedList<>();
    queue.add(path);
    while (!queue.isEmpty()) {
      Path next = queue.pollFirst();
      FileStatus[] stats = fs.listStatus(next, FileUtils.HIDDEN_FILES_PATH_FILTER);
      for (FileStatus stat : stats) {
        Path child = stat.getPath();
        paths.add(child.toString());
        if (stat.isDirectory()) {
          queue.add(child);
        }
      }
    }
    return paths;
  }


  /**
   * add tests for all transitions - AC=t, AC=t, AC=f, commit (for example)
   * @throws Exception
   */
  @Test
  public void testErrors() throws Exception {
    runStatementOnDriver("start transaction");
    CommandProcessorException e1 = runStatementOnDriverNegative("create table foo(x int, y int)");
    Assert.assertEquals("Expected DDL to fail in an open txn",
        ErrorMsg.OP_NOT_ALLOWED_IN_TXN.getErrorCode(), e1.getErrorCode());
    CommandProcessorException e2 = runStatementOnDriverNegative("update " + Table.ACIDTBL + " set a = 1 where b != 1");
    Assert.assertEquals("Expected update of bucket column to fail",
        "FAILED: SemanticException [Error 10302]: Updating values of bucketing columns is not supported.  Column a.",
        e2.getMessage());
    Assert.assertEquals("Expected update of bucket column to fail",
        ErrorMsg.UPDATE_CANNOT_UPDATE_BUCKET_VALUE.getErrorCode(), e2.getErrorCode());
    CommandProcessorException e3 = runStatementOnDriverNegative("commit"); //not allowed in w/o tx
    Assert.assertEquals("Error didn't match: " + e3,
        ErrorMsg.OP_NOT_ALLOWED_WITHOUT_TXN.getErrorCode(), e3.getErrorCode());
    CommandProcessorException e4 = runStatementOnDriverNegative("rollback"); //not allowed in w/o tx
    Assert.assertEquals("Error didn't match: " + e4,
        ErrorMsg.OP_NOT_ALLOWED_WITHOUT_TXN.getErrorCode(), e4.getErrorCode());
    runStatementOnDriver("start transaction");
    CommandProcessorException e5 = runStatementOnDriverNegative("start transaction"); //not allowed in a tx
    Assert.assertEquals("Expected start transaction to fail",
        ErrorMsg.OP_NOT_ALLOWED_IN_TXN.getErrorCode(), e5.getErrorCode());
    runStatementOnDriver("start transaction"); //ok since previously opened txn was killed
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(1,2)");
    List<String> rs0 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Can't see my own write", 1, rs0.size());
    runStatementOnDriver("commit work");
    rs0 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Can't see my own write", 1, rs0.size());
  }

  @Test
  public void testReadMyOwnInsert() throws Exception {
    runStatementOnDriver("START TRANSACTION");
    List<String> rs = runStatementOnDriver("select * from " + Table.ACIDTBL);
    Assert.assertEquals("Expected empty " + Table.ACIDTBL, 0, rs.size());
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(1,2)");
    List<String> rs0 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Can't see my own write", 1, rs0.size());
    runStatementOnDriver("commit");
    runStatementOnDriver("START TRANSACTION");
    List<String> rs1 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    runStatementOnDriver("rollback work");
    Assert.assertEquals("Can't see write after commit", 1, rs1.size());
  }
  @Test
  public void testImplicitRollback() throws Exception {
    runStatementOnDriver("START TRANSACTION");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(1,2)");
    List<String> rs0 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Can't see my own write", 1, rs0.size());
    //next command should produce an error
    CommandProcessorException e = runStatementOnDriverNegative("select * from no_such_table");
    Assert.assertEquals("Txn didn't fail?",
        "FAILED: SemanticException [Error 10001]: Line 1:14 Table not found 'no_such_table'",
        e.getMessage());
    runStatementOnDriver("start transaction");
    List<String> rs1 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    runStatementOnDriver("commit");
    Assert.assertEquals("Didn't rollback as expected", 0, rs1.size());
  }
  @Test
  public void testExplicitRollback() throws Exception {
    runStatementOnDriver("START TRANSACTION");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(1,2)");
    runStatementOnDriver("ROLLBACK");
    List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Rollback didn't rollback", 0, rs.size());
  }

  @Test
  public void testMultipleInserts() throws Exception {
    runStatementOnDriver("START TRANSACTION");
    int[][] rows1 = {{1,2},{3,4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows1));
    int[][] rows2 = {{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows2));
    List<String> allData = stringifyValues(rows1);
    allData.addAll(stringifyValues(rows2));
    List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Content didn't match before commit rs", allData, rs);
    runStatementOnDriver("commit");
    dumpTableData(Table.ACIDTBL, 1, 0);
    dumpTableData(Table.ACIDTBL, 1, 1);
    List<String> rs1 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Content didn't match after commit rs1", allData, rs1);
  }

  @Test
  public void testDeleteOfMultipleInserts() throws Exception {
    runStatementOnDriver("START TRANSACTION");
    int[][] rows1 = {{1,2},{3,4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows1));
    int[][] rows2 = {{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows2));
    runStatementOnDriver("commit");
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where b = 2");
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where b = 8");
    List<String> rs2 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] remain = {{3,4},{5,6}};
    Assert.assertEquals("Content didn't match after delete ", stringifyValues(remain), rs2);
  }

  @Test
  public void testDelete() throws Exception {
    int[][] rows1 = {{1,2},{3,4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows1));
    List<String> rs0 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Content didn't match rs0", stringifyValues(rows1), rs0);
    runStatementOnDriver("START TRANSACTION");
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where b = 4");
    int[][] updatedData2 = {{1,2}};
    List<String> rs3 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Wrong data after delete", stringifyValues(updatedData2), rs3);
    runStatementOnDriver("commit");
    List<String> rs4 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Wrong data after commit", stringifyValues(updatedData2), rs4);
  }

  @Test
  public void testUpdateOfInserts() throws Exception {
    int[][] rows1 = {{1,2},{3,4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows1));
    List<String> rs0 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Content didn't match rs0", stringifyValues(rows1), rs0);
    runStatementOnDriver("START TRANSACTION");
    int[][] rows2 = {{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows2));
    List<String> rs1 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    List<String> allData = stringifyValues(rows1);
    allData.addAll(stringifyValues(rows2));
    Assert.assertEquals("Content didn't match rs1", allData, rs1);
    runStatementOnDriver("update " + Table.ACIDTBL + " set b = 1 where b != 1");
    int[][] updatedData = {{1,1},{3,1},{5,1},{7,1}};
    List<String> rs2 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Wrong data after update", stringifyValues(updatedData), rs2);
    runStatementOnDriver("commit");
    List<String> rs4 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Wrong data after commit", stringifyValues(updatedData), rs4);
  }
  @Test
  public void testUpdateDeleteOfInserts() throws Exception {
    int[][] rows1 = {{1,2},{3,4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows1));
    List<String> rs0 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Content didn't match rs0", stringifyValues(rows1), rs0);
    runStatementOnDriver("START TRANSACTION");
    int[][] rows2 = {{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows2));
    List<String> rs1 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    List<String> allData = stringifyValues(rows1);
    allData.addAll(stringifyValues(rows2));
    Assert.assertEquals("Content didn't match rs1", allData, rs1);
    runStatementOnDriver("update " + Table.ACIDTBL + " set b = 1 where b != 1");
    int[][] updatedData = {{1,1},{3,1},{5,1},{7,1}};
    List<String> rs2 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Wrong data after update", stringifyValues(updatedData), rs2);
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where a = 7 and b = 1");
    dumpTableData(Table.ACIDTBL, 1, 0);
    dumpTableData(Table.ACIDTBL, 2, 0);
    dumpTableData(Table.ACIDTBL, 2, 2);
    dumpTableData(Table.ACIDTBL, 2, 4);
    int[][] updatedData2 = {{1,1},{3,1},{5,1}};
    List<String> rs3 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Wrong data after delete", stringifyValues(updatedData2), rs3);
    runStatementOnDriver("commit");
    List<String> rs4 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Wrong data after commit", stringifyValues(updatedData2), rs4);
  }
  @Test
  public void testMultipleDelete() throws Exception {
    int[][] rows1 = {{1,2},{3,4},{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows1));
    List<String> rs0 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Content didn't match rs0", stringifyValues(rows1), rs0);
    runStatementOnDriver("START TRANSACTION");
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where b = 8");
    int[][] updatedData2 = {{1,2},{3,4},{5,6}};
    List<String> rs2 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Wrong data after delete", stringifyValues(updatedData2), rs2);
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where b = 4");
    int[][] updatedData3 = {{1, 2}, {5, 6}};
    List<String> rs3 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Wrong data after delete2", stringifyValues(updatedData3), rs3);
    runStatementOnDriver("update " + Table.ACIDTBL + " set b=3");
    dumpTableData(Table.ACIDTBL, 1, 0);
    //nothing actually hashes to bucket0, so update/delete deltas don't have it
    dumpTableData(Table.ACIDTBL, 2, 0);
    dumpTableData(Table.ACIDTBL, 2, 2);
    dumpTableData(Table.ACIDTBL, 2, 4);
    List<String> rs5 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int [][] updatedData4 = {{1,3},{5,3}};
    Assert.assertEquals("Wrong data after delete", stringifyValues(updatedData4), rs5);
    runStatementOnDriver("commit");
    List<String> rs4 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Wrong data after commit", stringifyValues(updatedData4), rs4);
  }
  @Test
  public void testDeleteIn() throws Exception {
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where a IN (SELECT A.a from " +
        Table.ACIDTBL + "  A)");
    int[][] tableData = {{1,2},{3,2},{5,2},{1,3},{3,3},{5,3}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData));
    runStatementOnDriver("insert into " + Table.ACIDTBL2 + "(a,b,c) values(1,7,17),(3,7,17)");
//    runStatementOnDriver("select b from " + Table.ACIDTBL + " where a in (select b from " + Table.NONACIDORCTBL + ")");
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where a in(select a from " + Table.ACIDTBL2 + ")");
//    runStatementOnDriver("delete from " + Table.ACIDTBL + " where a in(select a from " + Table.NONACIDORCTBL + ")");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) select a,b from " + Table.ACIDTBL2);
    List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] updatedData = {{1,7},{3,7},{5,2},{5,3}};
    Assert.assertEquals("Bulk update failed", stringifyValues(updatedData), rs);
  }
  @Test
  public void testTimeOutReaper() throws Exception {
    runStatementOnDriver("start transaction");
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where a = 5");
    //make sure currently running txn is considered aborted by housekeeper
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TXN_TIMEOUT, 2, TimeUnit.MILLISECONDS);
    MetastoreTaskThread houseKeeperService = new AcidHouseKeeperService();
    houseKeeperService.setConf(hiveConf);
    //this will abort the txn
    houseKeeperService.run();
    //this should fail because txn aborted due to timeout
    CommandProcessorException e = runStatementOnDriverNegative("delete from " + Table.ACIDTBL + " where a = 5");
    Assert.assertTrue("Actual: " + e.getMessage(),
        e.getMessage().contains("Transaction manager has aborted the transaction txnid:1"));

    //now test that we don't timeout locks we should not
    //heartbeater should be running in the background every 1/2 second
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TXN_TIMEOUT, 1, TimeUnit.SECONDS);
    // Have to reset the conf when we change it so that the change takes affect
    houseKeeperService.setConf(hiveConf);
    runStatementOnDriver("start transaction");
    runStatementOnDriver("select count(*) from " + Table.ACIDTBL + " where a = 17");
    pause(750);

    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);

    //since there is txn open, we are heartbeating the txn not individual locks
    GetOpenTxnsInfoResponse txnsInfoResponse = txnHandler.getOpenTxnsInfo();
    Assert.assertEquals(2, txnsInfoResponse.getOpen_txns().size());
    TxnInfo txnInfo = null;
    for(TxnInfo ti : txnsInfoResponse.getOpen_txns()) {
      if(ti.getState() == TxnState.OPEN) {
        txnInfo = ti;
        break;
      }
    }
    Assert.assertNotNull(txnInfo);
    Assert.assertEquals(16, txnInfo.getId());
    Assert.assertEquals(TxnState.OPEN, txnInfo.getState());
    String s = TestTxnDbUtil
        .queryToString(hiveConf, "select TXN_STARTED, TXN_LAST_HEARTBEAT from TXNS where TXN_ID = " + txnInfo.getId(), false);
    String[] vals = s.split("\\s+");
    Assert.assertEquals("Didn't get expected timestamps", 2, vals.length);
    long lastHeartbeat = Long.parseLong(vals[1]);
    //these 2 values are equal when TXN entry is made.  Should never be equal after 1st heartbeat, which we
    //expect to have happened by now since HIVE_TXN_TIMEOUT=1sec
    Assert.assertNotEquals("Didn't see heartbeat happen", Long.parseLong(vals[0]), lastHeartbeat);

    ShowLocksResponse slr = txnHandler.showLocks(new ShowLocksRequest());
    TestDbTxnManager2.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", Table.ACIDTBL.name, null, slr.getLocks());
    pause(750);
    houseKeeperService.run();
    pause(750);
    slr = txnHandler.showLocks(new ShowLocksRequest());
    Assert.assertEquals("Unexpected lock count: " + slr, 1, slr.getLocks().size());
    TestDbTxnManager2.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", Table.ACIDTBL.name, null, slr.getLocks());

    pause(750);
    houseKeeperService.run();
    slr = txnHandler.showLocks(new ShowLocksRequest());
    Assert.assertEquals("Unexpected lock count: " + slr, 1, slr.getLocks().size());
    TestDbTxnManager2.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", Table.ACIDTBL.name, null, slr.getLocks());

    //should've done several heartbeats
    s = TestTxnDbUtil.queryToString(hiveConf, "select TXN_STARTED, TXN_LAST_HEARTBEAT from TXNS where TXN_ID = " + txnInfo.getId(), false);
    vals = s.split("\\s+");
    Assert.assertEquals("Didn't get expected timestamps", 2, vals.length);
    Assert.assertTrue("Heartbeat didn't progress: (old,new) (" + lastHeartbeat + "," + vals[1]+ ")",
        lastHeartbeat < Long.parseLong(vals[1]));

    runStatementOnDriver("rollback");
    slr = txnHandler.showLocks(new ShowLocksRequest());
    Assert.assertEquals("Unexpected lock count", 0, slr.getLocks().size());
  }
  private static void pause(int timeMillis) {
    try {
      Thread.sleep(timeMillis);
    }
    catch (InterruptedException e) {
    }
  }

  @Test
  public void exchangePartition() throws Exception {
    runStatementOnDriver("create database ex1");
    runStatementOnDriver("create database ex2");

    runStatementOnDriver("CREATE TABLE ex1.exchange_part_test1 (f1 string) PARTITIONED BY (ds STRING)");
    runStatementOnDriver("CREATE TABLE ex2.exchange_part_test2 (f1 string) PARTITIONED BY (ds STRING)");
    runStatementOnDriver("ALTER TABLE ex2.exchange_part_test2 ADD PARTITION (ds='2013-04-05')");
    runStatementOnDriver("ALTER TABLE ex1.exchange_part_test1 EXCHANGE PARTITION (ds='2013-04-05') WITH TABLE ex2.exchange_part_test2");
  }
  @Test
  public void testMergeNegative() throws Exception {
    CommandProcessorException e = runStatementOnDriverNegative(
        "MERGE INTO " + Table.ACIDTBL + " target\n" +
        "USING " + Table.NONACIDORCTBL + " source ON target.a = source.a\n" +
        "WHEN MATCHED THEN UPDATE set b = 1\n" +
        "WHEN MATCHED THEN DELETE\n" +
        "WHEN NOT MATCHED AND a < 1 THEN INSERT VALUES(1,2)");
    Assert.assertEquals(ErrorMsg.MERGE_PREDIACTE_REQUIRED, ((HiveException)e.getCause()).getCanonicalErrorMsg());
  }
  @Test
  public void testMergeNegative2() throws Exception {
    CommandProcessorException e = runStatementOnDriverNegative(
        "MERGE INTO "+ Table.ACIDTBL +
        " target USING " + Table.NONACIDORCTBL + "\n source ON target.pk = source.pk " +
        "\nWHEN MATCHED THEN UPDATE set b = 1 " +
        "\nWHEN MATCHED THEN UPDATE set b=a");
    Assert.assertEquals(ErrorMsg.MERGE_TOO_MANY_UPDATE, ((HiveException)e.getCause()).getCanonicalErrorMsg());
  }

  /**
   * `1` means 1 is a column name and '1' means 1 is a string literal
   * HiveConf.HIVE_QUOTEDID_SUPPORT
   * HiveConf.HIVE_SUPPORT_SPECICAL_CHARACTERS_IN_TABLE_NAMES
   * {@link TestTxnCommands#testMergeType2SCD01()}
   */
  @Test
  public void testQuotedIdentifier() throws Exception {
    String target = "`aci/d_u/ami`";
    String src = "`src/name`";
    runStatementOnDriver("drop table if exists " + target);
    runStatementOnDriver("drop table if exists " + src);
    runStatementOnDriver("create table " + target + "(i int," +
        "`d?*de e` decimal(5,2)," +
        "vc varchar(128)) clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + src + "(gh int, j decimal(5,2), k varchar(128))");
    runStatementOnDriver("merge into " + target + " as `d/8` using " + src + " as `a/b` on i=gh " +
        "\nwhen matched and i > 5 then delete " +
        "\nwhen matched then update set vc='blah' " +
        "\nwhen not matched then insert values(1,2.1,'baz')");
    runStatementOnDriver("merge into " + target + " as `d/8` using " + src + " as `a/b` on i=gh " +
        "\nwhen matched and i > 5 then delete " +
        "\nwhen matched then update set vc='blah',  `d?*de e` = current_timestamp()  " +
        "\nwhen not matched then insert values(1,2.1, concat('baz', current_timestamp()))");
    runStatementOnDriver("merge into " + target + " as `d/8` using " + src + " as `a/b` on i=gh " +
        "\nwhen matched and i > 5 then delete " +
        "\nwhen matched then update set vc='blah' " +
        "\nwhen not matched then insert values(1,2.1,'a\\b')");
    runStatementOnDriver("merge into " + target + " as `d/8` using " + src + " as `a/b` on i=gh " +
        "\nwhen matched and i > 5 then delete " +
        "\nwhen matched then update set vc='∆∋'" +
        "\nwhen not matched then insert values(`a/b`.gh,`a/b`.j,'c\\t')");
  }
  @Test
  public void testQuotedIdentifier2() throws Exception {
    String target = "`aci/d_u/ami`";
    String src = "`src/name`";
    runStatementOnDriver("drop table if exists " + target);
    runStatementOnDriver("drop table if exists " + src);
    runStatementOnDriver("create table " + target + "(i int," +
        "`d?*de e` decimal(5,2)," +
        "vc varchar(128)) clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + src + "(`g/h` int, j decimal(5,2), k varchar(128))");
    runStatementOnDriver("merge into " + target + " as `d/8` using " + src + " as `a/b` on i=`g/h`" +
        "\nwhen matched and `g/h` > 5 then delete " +
        "\nwhen matched and `g/h` < 0 then update set vc='∆∋', `d?*de e` =  `d?*de e` * j + 1" +
        "\nwhen not matched and `d?*de e` <> 0 then insert values(`a/b`.`g/h`,`a/b`.j,`a/b`.k)");
    runStatementOnDriver("merge into " + target + " as `d/8` using " + src + " as `a/b` on i=`g/h`" +
        "\nwhen matched and `g/h` > 5 then delete" +
        "\n when matched and `g/h` < 0 then update set vc='∆∋'  , `d?*de e` =  `d?*de e` * j + 1  " +
        "\n when not matched and `d?*de e` <> 0 then insert values(`a/b`.`g/h`,`a/b`.j,`a/b`.k)");
  }
  /**
   * https://www.linkedin.com/pulse/how-load-slowly-changing-dimension-type-2-using-oracle-padhy
   * also test QuotedIdentifier inside source expression
   * {@link TestTxnCommands#testQuotedIdentifier()}
   * {@link TestTxnCommands#testQuotedIdentifier2()}
   */
  @Test
  public void testMergeType2SCD01() throws Exception {
    runStatementOnDriver("drop table if exists target");
    runStatementOnDriver("drop table if exists source");
    runStatementOnDriver("drop table if exists splitTable");

    runStatementOnDriver("create table splitTable(op int)");
    runStatementOnDriver("insert into splitTable values (0),(1)");
    runStatementOnDriver("create table source (key int, data int)");
    runStatementOnDriver("create table target (key int, data int, cur int) clustered by (key) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    int[][] targetVals = {{1, 5, 1}, {2, 6, 1}, {1, 18, 0}};
    runStatementOnDriver("insert into target " + makeValuesClause(targetVals));
    int[][] sourceVals = {{1, 7}, {3, 8}};
    runStatementOnDriver("insert into source " + makeValuesClause(sourceVals));
    //augment source with a col which has 1 if it will cause an update in target, 0 otherwise
    String curMatch = "select s.*, case when t.cur is null then 0 else 1 end m from source s left outer join (select * from target where target.cur=1) t on s.key=t.key";
    //split each row (duplicate) which will cause an update into 2 rows and augment with 'op' col which has 0 to insert, 1 to update
    String teeCurMatch = "select curMatch.*, case when splitTable.op is null or splitTable.op = 0 then 0 else 1 end `o/p\\n` from (" + curMatch + ") curMatch left outer join splitTable on curMatch.m=1";
    if(false) {
      //this is just for debug
      List<String> r1 = runStatementOnDriver(curMatch);
      List<String> r2 = runStatementOnDriver(teeCurMatch);
    }
    String stmt = "merge into target t using (" + teeCurMatch + ") s on t.key=s.key and t.cur=1 and s.`o/p\\n`=1 " +
        "when matched then update set cur=0 " +
        "when not matched then insert values(s.key,s.data,1)";
    //to allow cross join from 'teeCurMatch'
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_STRICT_CHECKS_CARTESIAN, false);
    runStatementOnDriver(stmt);
    int[][] resultVals = {{1,5,0},{1,7,1},{1,18,0},{2,6,1},{3,8,1}};
    List<String> r = runStatementOnDriver("select * from target order by key,data,cur");
    Assert.assertEquals(stringifyValues(resultVals), r);
  }
  /**
   * https://www.linkedin.com/pulse/how-load-slowly-changing-dimension-type-2-using-oracle-padhy
   * Same as testMergeType2SCD01 but with a more intuitive "source" expression
   */
  @Test
  public void testMergeType2SCD02() throws Exception {
    runStatementOnDriver("drop table if exists target");
    runStatementOnDriver("drop table if exists source");
    runStatementOnDriver("create table source (key int, data int)");
    runStatementOnDriver("create table target (key int, data int, cur int) clustered by (key) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    int[][] targetVals = {{1, 5, 1}, {2, 6, 1}, {1, 18, 0}};
    runStatementOnDriver("insert into target " + makeValuesClause(targetVals));
    int[][] sourceVals = {{1, 7}, {3, 8}};
    runStatementOnDriver("insert into source " + makeValuesClause(sourceVals));

    String baseSrc =  "select source.*, 0 c from source " +
        "union all " +
        "select source.*, 1 c from source " +
        "inner join target " +
        "on source.key=target.key where target.cur=1";
    if(false) {
      //this is just for debug
      List<String> r1 = runStatementOnDriver(baseSrc);
      List<String> r2 = runStatementOnDriver(
        "select t.*, s.* from target t right outer join (" + baseSrc + ") s " +
          "\non t.key=s.key and t.cur=s.c and t.cur=1");
    }
    String stmt = "merge into target t using " +
        "(" + baseSrc + ") s " +
        "on t.key=s.key and t.cur=s.c and t.cur=1 " +
        "when matched then update set cur=0 " +
        "when not matched then insert values(s.key,s.data,1)";

    runStatementOnDriver(stmt);
    int[][] resultVals = {{1,5,0},{1,7,1},{1,18,0},{2,6,1},{3,8,1}};
    List<String> r = runStatementOnDriver("select * from target order by key,data,cur");
    Assert.assertEquals(stringifyValues(resultVals), r);
  }

  @Test
  public void testMergeOnTezEdges() throws Exception {
    String query = "merge into " + Table.ACIDTBL +
        " as t using " + Table.NONACIDORCTBL + " s ON t.a = s.a " +
        "WHEN MATCHED AND s.a > 8 THEN DELETE " +
        "WHEN MATCHED THEN UPDATE SET b = 7 " +
        "WHEN NOT MATCHED THEN INSERT VALUES(s.a, s.b) ";
    d.destroy();
    HiveConf hc = new HiveConf(hiveConf);
    hc.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    hc.setBoolVar(HiveConf.ConfVars.HIVE_EXPLAIN_USER, false);
    d = new Driver(hc);
    d.setMaxRows(10000);

    List<String> explain = runStatementOnDriver("explain " + query);
    StringBuilder sb = new StringBuilder();
    for(String s : explain) {
      sb.append(s).append('\n');
    }
    LOG.info("Explain1: " + sb);
    /*
     Edges:
     Reducer 2 <- Map 1 (SIMPLE_EDGE), Map 8 (SIMPLE_EDGE)
     Reducer 3 <- Reducer 2 (SIMPLE_EDGE)
     Reducer 4 <- Reducer 2 (SIMPLE_EDGE)
     Reducer 5 <- Reducer 2 (CUSTOM_SIMPLE_EDGE)
     Reducer 6 <- Reducer 2 (SIMPLE_EDGE)
     Reducer 7 <- Reducer 2 (CUSTOM_SIMPLE_EDGE)
     */
    for(int i = 0; i < explain.size(); i++) {
      if(explain.get(i).contains("Edges:")) {
        Assert.assertTrue("At i+1=" + (i+1) + explain.get(i + 1),
            explain.get(i + 1).contains("Reducer 2 <- Map 1 (SIMPLE_EDGE), Map 8 (SIMPLE_EDGE)"));
        Assert.assertTrue("At i+1=" + (i+2) + explain.get(i + 2),
            explain.get(i + 2).contains("Reducer 3 <- Reducer 2 (SIMPLE_EDGE)"));
        Assert.assertTrue("At i+1=" + (i+3) + explain.get(i + 3),
            explain.get(i + 3).contains("Reducer 4 <- Reducer 2 (SIMPLE_EDGE)"));
        Assert.assertTrue("At i+1=" + (i+4) + explain.get(i + 4),
            explain.get(i + 4).contains("Reducer 5 <- Reducer 2 (SIMPLE_EDGE)"));
        Assert.assertTrue("At i+1=" + (i+5) + explain.get(i + 5),
            explain.get(i + 5).contains("Reducer 6 <- Reducer 2 (SIMPLE_EDGE)"));
        Assert.assertTrue("At i+1=" + (i+6) + explain.get(i + 6),
            explain.get(i + 6).contains("Reducer 7 <- Reducer 2 (SIMPLE_EDGE)"));
        break;
      }
    }
  }
  @Test
  public void testMergeUpdateDelete() throws Exception {
    int[][] baseValsOdd = {{2,2},{4,44},{5,5},{11,11}};
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + " " + makeValuesClause(baseValsOdd));
    int[][] vals = {{2,1},{4,3},{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + " " + makeValuesClause(vals));
    String query = "merge into " + Table.ACIDTBL +
        " as t using " + Table.NONACIDORCTBL + " s ON t.a = s.a " +
        "WHEN MATCHED AND s.a < 3 THEN update set b = 0 " + //updates (2,1) -> (2,0)
        "WHEN MATCHED and t.a > 3 and t.a < 5 THEN DELETE " +//deletes (4,3)
        "WHEN NOT MATCHED THEN INSERT VALUES(s.a, s.b) "; //inserts (11,11)
    runStatementOnDriver(query);

    List<String> r = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] rExpected = {{2,0},{5,6},{7,8},{11,11}};
    Assert.assertEquals(stringifyValues(rExpected), r);
  }
  @Test
  public void testMergeUpdateDeleteNoCardCheck() throws Exception {
    d.destroy();
    HiveConf hc = new HiveConf(hiveConf);
    hc.setBoolVar(HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK, false);
    d = new Driver(hc);
    d.setMaxRows(10000);

    int[][] baseValsOdd = {{2,2},{4,44},{5,5},{11,11}};
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + " " + makeValuesClause(baseValsOdd));
    int[][] vals = {{2,1},{4,3},{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + " " + makeValuesClause(vals));
    String query = "merge into " + Table.ACIDTBL +
        " as t using " + Table.NONACIDORCTBL + " s ON t.a = s.a " +
        "WHEN MATCHED AND s.a < 3 THEN update set b = 0 " +
        "WHEN MATCHED and t.a > 3 and t.a < 5 THEN DELETE ";
    runStatementOnDriver(query);

    List<String> r = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] rExpected = {{2,0},{5,6},{7,8}};
    Assert.assertEquals(stringifyValues(rExpected), r);
  }
  @Test
  public void testMergeDeleteUpdate() throws Exception {
    int[][] sourceVals = {{2,2},{4,44},{5,5},{11,11}};
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + " " + makeValuesClause(sourceVals));
    int[][] targetVals = {{2,1},{4,3},{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + " " + makeValuesClause(targetVals));
    String query = "merge into " + Table.ACIDTBL +
        " as t using " + Table.NONACIDORCTBL + " s ON t.a = s.a " +
        "WHEN MATCHED and s.a < 5 THEN DELETE " +
        "WHEN MATCHED AND s.a < 3 THEN update set b = 0 " +
        "WHEN NOT MATCHED THEN INSERT VALUES(s.a, s.b) ";
    runStatementOnDriver(query);

    List<String> r = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] rExpected = {{5,6},{7,8},{11,11}};
    Assert.assertEquals(stringifyValues(rExpected), r);
  }

  /**
   * see https://issues.apache.org/jira/browse/HIVE-14949 for details
   * @throws Exception
   */
  @Test
  public void testMergeCardinalityViolation() throws Exception {
    int[][] sourceVals = {{2,2},{2,44},{5,5},{11,11}};
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + " " + makeValuesClause(sourceVals));
    int[][] targetVals = {{2,1},{4,3},{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + " " + makeValuesClause(targetVals));
    String query = "merge into " + Table.ACIDTBL +
        " as t using " + Table.NONACIDORCTBL + " s ON t.a = s.a " +
        "WHEN MATCHED and s.a < 5 THEN DELETE " +
        "WHEN MATCHED AND s.a < 3 THEN update set b = 0 " +
        "WHEN NOT MATCHED THEN INSERT VALUES(s.a, s.b) ";
    runStatementOnDriverNegative(query);
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p1'),(2,2,'p1'),(3,3,'p1'),(4,4,'p2')");
    query = "merge into " + Table.ACIDTBLPART +
        " as t using " + Table.NONACIDORCTBL + " s ON t.a = s.a " +
        "WHEN MATCHED and s.a < 5 THEN DELETE " +
        "WHEN MATCHED AND s.a < 3 THEN update set b = 0 " +
        "WHEN NOT MATCHED THEN INSERT VALUES(s.a, s.b, 'p1') ";
    runStatementOnDriverNegative(query);
  }
  @Test
  public void testSetClauseFakeColumn() throws Exception {
    CommandProcessorException e1 = runStatementOnDriverNegative(
        "MERGE INTO "+ Table.ACIDTBL + " target\n" +
        "USING " + Table.NONACIDORCTBL + "\n" +
        " source ON target.a = source.a\n" +
        "WHEN MATCHED THEN UPDATE set t = 1");
    Assert.assertEquals(ErrorMsg.INVALID_TARGET_COLUMN_IN_SET_CLAUSE,
        ((HiveException)e1.getCause()).getCanonicalErrorMsg());

    CommandProcessorException e2 = runStatementOnDriverNegative("update " + Table.ACIDTBL + " set t = 1");
    Assert.assertEquals(ErrorMsg.INVALID_TARGET_COLUMN_IN_SET_CLAUSE,
        ((HiveException)e2.getCause()).getCanonicalErrorMsg());
  }

  @Test
  public void testBadOnClause() throws Exception {
    CommandProcessorException e =
        runStatementOnDriverNegative(
            "merge into " + Table.ACIDTBL + " trgt\n" +
            "using (select *\n" +
            "       from " + Table.NONACIDORCTBL + " src) sub on sub.a = target.a\n" +
            "when not matched then insert values (sub.a,sub.b)");
    Assert.assertTrue("Error didn't match: " + e, e.getMessage().contains(
        "No columns from target table 'trgt' found in ON clause '`sub`.`a` = `target`.`a`' of MERGE statement."));
  }

  /**
   * Writing UTs that need multiple threads is challenging since Derby chokes on concurrent access.
   * This tests that "AND WAIT" actually blocks and responds to interrupt
   * @throws Exception
   */
  @Test
  public void testCompactionBlocking() throws Exception {
    Timer cancelCompact = new Timer("CancelCompactionTimer", false);
    final Thread threadToInterrupt= Thread.currentThread();
    cancelCompact.schedule(new TimerTask() {
      @Override
      public void run() {
        threadToInterrupt.interrupt();
      }
    }, 5000);
    long start = System.currentTimeMillis();
    runStatementOnDriver("alter table " + Table.ACIDTBL + " compact 'major' AND WAIT");
    //no Worker so it stays in initiated state
    //w/o AND WAIT the above alter table retunrs almost immediately, so the test here to check that
    //> 2 seconds pass, i.e. that the command in Driver actually blocks before cancel is fired
    Assert.assertTrue(System.currentTimeMillis() > start + 2);
  }

  @Test
  public void testMergeCase() throws Exception {
    runStatementOnDriver("create table merge_test (c1 integer, c2 integer, c3 integer) CLUSTERED BY (c1) into 2 buckets stored as orc tblproperties(\"transactional\"=\"true\")");
    runStatementOnDriver("create table if not exists e011_02 (c1 float, c2 double, c3 float)");
    runStatementOnDriver("merge into merge_test using e011_02 on (merge_test.c1 = e011_02.c1) when not matched then insert values (case when e011_02.c1 > 0 then e011_02.c1 + 1 else e011_02.c1 end, e011_02.c2 + e011_02.c3, coalesce(e011_02.c3, 1))");
  }
  /**
   * HIVE-16177
   * See also {@link TestTxnCommands2#testNonAcidToAcidConversion02()}
   */
  @Test
  public void testNonAcidToAcidConversion01() throws Exception {
    //create 1 row in a file 000001_0 (and an empty 000000_0)
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,2)");
    //create 1 row in a file 000000_0_copy1 and 1 row in a file 000001_0_copy1
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(0,12),(1,5)");

    //convert the table to Acid
    runStatementOnDriver("alter table " + Table.NONACIDORCTBL + " SET TBLPROPERTIES ('transactional'='true')");
    //create a delta directory
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,17)");

    boolean isVectorized = hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED);
    String query = "select ROW__ID, a, b" + (isVectorized ? " from  " : ", INPUT__FILE__NAME from ") +  Table.NONACIDORCTBL + " order by ROW__ID";
    String[][] expected = new String[][] {
      {"{\"writeid\":0,\"bucketid\":536936448,\"rowid\":0}\t1\t2", "nonacidorctbl/000001_0"},
      {"{\"writeid\":0,\"bucketid\":536936448,\"rowid\":1}\t0\t12", "nonacidorctbl/000001_0_copy_1"},
      {"{\"writeid\":0,\"bucketid\":536936448,\"rowid\":2}\t1\t5", "nonacidorctbl/000001_0_copy_1"},
      {"{\"writeid\":10000001,\"bucketid\":536936448,\"rowid\":0}\t1\t17", "nonacidorctbl/delta_10000001_10000001_0000/bucket_00001_0"}
    };
    checkResult(expected, query, isVectorized, "before compact", LOG);

    Assert.assertEquals(536870912,
        BucketCodec.V1.encode(new AcidOutputFormat.Options(hiveConf).bucket(0)));
    Assert.assertEquals(536936448,
        BucketCodec.V1.encode(new AcidOutputFormat.Options(hiveConf).bucket(1)));

    //run Compaction
    runStatementOnDriver("alter table " + Table.NONACIDORCTBL + " compact 'major'");
    runWorker(hiveConf);

    query = "select ROW__ID, a, b" + (isVectorized ? "" : ", INPUT__FILE__NAME") + " from "
        + Table.NONACIDORCTBL + " order by ROW__ID";
    String[][] expected2 = new String[][] {
        {"{\"writeid\":0,\"bucketid\":536936448,\"rowid\":0}\t1\t2", "nonacidorctbl/base_10000001_v0000021/bucket_00001"},
        {"{\"writeid\":0,\"bucketid\":536936448,\"rowid\":1}\t0\t12", "nonacidorctbl/base_10000001_v0000021/bucket_00001"},
        {"{\"writeid\":0,\"bucketid\":536936448,\"rowid\":2}\t1\t5", "nonacidorctbl/base_10000001_v0000021/bucket_00001"},
        {"{\"writeid\":10000001,\"bucketid\":536936448,\"rowid\":0}\t1\t17", "nonacidorctbl/base_10000001_v0000021/bucket_00001"}
    };
    checkResult(expected2, query, isVectorized, "after major compact", LOG);
    //make sure they are the same before and after compaction
  }
  //@Ignore("see bucket_num_reducers_acid.q")
  @Test
  public void testMoreBucketsThanReducers() throws Exception {
    //see bucket_num_reducers.q bucket_num_reducers2.q
    // todo: try using set VerifyNumReducersHook.num.reducers=10;
    d.destroy();
    HiveConf hc = new HiveConf(hiveConf);
    hc.setIntVar(HiveConf.ConfVars.MAX_REDUCERS, 1);
    //this is used in multiple places, SemanticAnalyzer.getBucketingSortingDest() among others
    hc.setIntVar(HiveConf.ConfVars.HADOOP_NUM_REDUCERS, 1);
    hc.setBoolVar(HiveConf.ConfVars.HIVE_EXPLAIN_USER, false);
    d = new Driver(hc);
    d.setMaxRows(10000);
    runStatementOnDriver("insert into " + Table.ACIDTBL + " values(1,1)"); //txn X write to bucket1
    runStatementOnDriver("insert into " + Table.ACIDTBL + " values(0,0),(3,3)"); // txn X + 1 write to bucket0 + bucket1
    runStatementOnDriver("update " + Table.ACIDTBL + " set b = -1");
    List<String> r = runStatementOnDriver("select * from " + Table.ACIDTBL + " order by a, b");
    int[][] expected = {{0, -1}, {1, -1}, {3, -1}};
    Assert.assertEquals(stringifyValues(expected), r);
  }
  @Ignore("Moved to Tez")
  @Test
  public void testMoreBucketsThanReducers2() throws Exception {
    //todo: try using set VerifyNumReducersHook.num.reducers=10;
    //see bucket_num_reducers.q bucket_num_reducers2.q
    d.destroy();
    HiveConf hc = new HiveConf(hiveConf);
    hc.setIntVar(HiveConf.ConfVars.MAX_REDUCERS, 2);
    //this is used in multiple places, SemanticAnalyzer.getBucketingSortingDest() among others
    hc.setIntVar(HiveConf.ConfVars.HADOOP_NUM_REDUCERS, 2);
    d = new Driver(hc);
    d.setMaxRows(10000);
    runStatementOnDriver("create table fourbuckets (a int, b int) clustered by (a) into 4 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    //below value for a is bucket id, for b - txn id (logically)
    runStatementOnDriver("insert into fourbuckets values(0,1),(1,1)"); //txn X write to b0 + b1
    runStatementOnDriver("insert into fourbuckets values(2,2),(3,2)"); // txn X + 1 write to b2 + b3
    runStatementOnDriver("insert into fourbuckets values(0,3),(1,3)"); //txn X + 2 write to b0 + b1
    runStatementOnDriver("insert into fourbuckets values(2,4),(3,4)"); //txn X + 3 write to b2 + b3
    //so with 2 FileSinks and 4 buckets, FS1 should see (0,1),(2,2),(0,3)(2,4) since data is sorted by ROW__ID where tnxid is the first component
    //FS2 should see (1,1),(3,2),(1,3),(3,4)

    runStatementOnDriver("update fourbuckets set b = -1");
    List<String> r = runStatementOnDriver("select * from fourbuckets order by a, b");
    int[][] expected = {{0, -1},{0, -1}, {1, -1}, {1, -1}, {2, -1}, {2, -1}, {3, -1}, {3, -1}};
    Assert.assertEquals(stringifyValues(expected), r);
  }
  @Test
  public void testVersioningVersionFileEnabled() throws Exception {
    acidVersionTest(true);
  }

  @Test
  public void testVersioningVersionFileDisabled() throws Exception {
    acidVersionTest(false);
  }

  private void acidVersionTest(boolean enableVersionFile) throws Exception {
    boolean originalEnableVersionFile = hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_WRITE_ACID_VERSION_FILE);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_WRITE_ACID_VERSION_FILE, enableVersionFile);

    hiveConf.set(MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID.getVarname(), "true");
    // Need to close the thread local Hive object so that configuration change is reflected to HMS.
    Hive.closeCurrent();
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T (a int, b int) stored as orc");
    int[][] data = {{1, 2}};
    //create 1 delta file bucket_00000
    runStatementOnDriver("insert into T" + makeValuesClause(data));
    runStatementOnDriver("update T set a=3 where b=2");

    FileSystem fs = FileSystem.get(hiveConf);
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(getWarehouseDir(), "t"), true);
    CompactorTestUtilities.checkAcidVersion(files, fs, enableVersionFile,
        new String[] { AcidUtils.DELTA_PREFIX, AcidUtils.DELETE_DELTA_PREFIX });

    runStatementOnDriver("alter table T compact 'minor'");
    runWorker(hiveConf);

    // Check status of compaction job
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history", 1, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0 compaction state",
        TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(0).getState());
    Assert.assertTrue(resp.getCompacts().get(0).getHadoopJobId().startsWith("job_local"));
    Assert.assertTrue(resp.getCompacts().get(0).getType().equals(CompactionType.MINOR));

    // Check the files after minor compaction
    files = fs.listFiles(new Path(getWarehouseDir(), "t"), true);
    CompactorTestUtilities.checkAcidVersion(files, fs, enableVersionFile,
        new String[] { AcidUtils.DELTA_PREFIX, AcidUtils.DELETE_DELTA_PREFIX });

    runStatementOnDriver("insert into T" + makeValuesClause(data));

    runStatementOnDriver("alter table T compact 'major'");
    runWorker(hiveConf);

    // Check status of compaction job
    txnHandler = TxnUtils.getTxnStore(hiveConf);
    resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history", 2, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 1 compaction state",
        TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(1).getState());
    Assert.assertTrue(resp.getCompacts().get(1).getHadoopJobId().startsWith("job_local"));

    // Check the files after major compaction
    files = fs.listFiles(new Path(getWarehouseDir(), "t"), true);
    CompactorTestUtilities.checkAcidVersion(files, fs, enableVersionFile,
        new String[] { AcidUtils.DELTA_PREFIX, AcidUtils.DELETE_DELTA_PREFIX, AcidUtils.BASE_PREFIX });

    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_WRITE_ACID_VERSION_FILE, originalEnableVersionFile);
  }

  @Test
  public void testTruncateWithBase() throws Exception{
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_TRUNCATE_USE_BASE, true);
    
    runStatementOnDriver("insert into " + Table.ACIDTBL + " values(1,2),(3,4)");
    runStatementOnDriver("truncate table " + Table.ACIDTBL);

    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] stat = fs.listStatus(new Path(getWarehouseDir(), Table.ACIDTBL.toString().toLowerCase()),
        AcidUtils.baseFileFilter);
    if (1 != stat.length) {
      Assert.fail("Expecting 1 base and found " + stat.length + " files " + Arrays.toString(stat));
    }
    String name = stat[0].getPath().getName();
    Assert.assertEquals("base_0000002", name);

    List<String> r = runStatementOnDriver("select * from " + Table.ACIDTBL);
    Assert.assertEquals(0, r.size());
  }

  @Test
  public void testTruncateWithBaseAllPartition() throws Exception{
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_TRUNCATE_USE_BASE, true);
    
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='a') values(1,2),(3,4)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='b') values(1,2),(3,4)");
    runStatementOnDriver("truncate table " + Table.ACIDTBLPART);

    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] stat = fs.listStatus(new Path(getWarehouseDir(), Table.ACIDTBLPART.toString().toLowerCase() + "/p=a"),
        AcidUtils.baseFileFilter);
    if (1 != stat.length) {
      Assert.fail("Expecting 1 base and found " + stat.length + " files " + Arrays.toString(stat));
    }
    String name = stat[0].getPath().getName();
    Assert.assertEquals("base_0000003", name);

    List<String> r = runStatementOnDriver("select * from " + Table.ACIDTBLPART);
    Assert.assertEquals(0, r.size());
  }

  @Test
  public void testTruncateWithBaseOnePartition() throws Exception{
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_TRUNCATE_USE_BASE, true);
    
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='a') values(1,2),(3,4)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='b') values(5,5),(4,4)");
    runStatementOnDriver("truncate table " + Table.ACIDTBLPART + " partition(p='b')");

    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] stat = fs.listStatus(new Path(getWarehouseDir(), Table.ACIDTBLPART.toString().toLowerCase() + "/p=b"),
        AcidUtils.baseFileFilter);
    if (1 != stat.length) {
      Assert.fail("Expecting 1 base and found " + stat.length + " files " + Arrays.toString(stat));
    }
    String name = stat[0].getPath().getName();
    Assert.assertEquals("base_0000003", name);
    stat = fs.listStatus(new Path(getWarehouseDir(), Table.ACIDTBLPART.toString().toLowerCase() + "/p=a"),
        AcidUtils.deltaFileFilter);
    if (1 != stat.length) {
      Assert.fail("Expecting 1 delta and found " + stat.length + " files " + Arrays.toString(stat));
    }

    List<String> r = runStatementOnDriver("select * from " + Table.ACIDTBLPART);
    Assert.assertEquals(2, r.size());
  }

  @Test
  public void testDropWithBaseAndRecreateOnePartition() throws Exception {
    dropWithBaseOnePartition(true);
  }
  @Test
  public void testDropWithBaseOnePartition() throws Exception {
    dropWithBaseOnePartition(false);
  }
  
  private void dropWithBaseOnePartition(boolean reCreate) throws Exception {
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition (p='a') values (1,2),(3,4)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition (p='b') values (5,5),(4,4)");

    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_DROP_PARTITION_USE_BASE, true);
    runStatementOnDriver("alter table " + Table.ACIDTBLPART + " drop partition (p='b')");

    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] stat = fs.listStatus(new Path(getWarehouseDir(), Table.ACIDTBLPART.toString().toLowerCase() + "/p=b"),
        AcidUtils.baseFileFilter);
    if (1 != stat.length) {
      Assert.fail("Expecting 1 base and found " + Arrays.toString(stat));
    }
    String name = stat[0].getPath().getName();
    Assert.assertEquals("base_0000003", name);
    
    List<String> r = runStatementOnDriver("select * from " + Table.ACIDTBLPART);
    Assert.assertEquals(2, r.size());
    Assert.assertTrue(isEqualCollection(r, asList("1\t2\ta", "3\t4\ta")));
    
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    
    Assert.assertEquals("Unexpected number of compactions in history", 1, resp.getCompactsSize());
    Assert.assertTrue(resp.getCompacts().stream().anyMatch(
        ci -> TxnStore.CLEANING_RESPONSE.equals(ci.getState()) && "p=b".equals(ci.getPartitionname())));
    if (reCreate) {
      runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition (p='b') values (3,3)");
    }
    runCleaner(hiveConf);
    
    stat = fs.listStatus(new Path(getWarehouseDir(), Table.ACIDTBLPART.toString().toLowerCase()),
        path -> path.getName().equals("p=b"));
    if ((reCreate ? 1 : 0) != stat.length) {
      Assert.fail("Partition data was " + (reCreate ? "" : "not") + " removed from FS");
    }
    if (reCreate) {
      r = runStatementOnDriver("select * from " + Table.ACIDTBLPART);
      Assert.assertEquals(3, r.size());
      Assert.assertTrue(isEqualCollection(r, asList("1\t2\ta", "3\t4\ta", "3\t3\tb")));
    }
  }

  @Test
  public void testDropWithBaseMultiplePartitions() throws Exception {
    runStatementOnDriver("insert into " + Table.ACIDTBLNESTEDPART + " partition (p1='a', p2='a', p3='a') values (1,1),(2,2)");
    runStatementOnDriver("insert into " + Table.ACIDTBLNESTEDPART + " partition (p1='a', p2='a', p3='b') values (3,3),(4,4)");
    runStatementOnDriver("insert into " + Table.ACIDTBLNESTEDPART + " partition (p1='a', p2='b', p3='c') values (7,7),(8,8)");
    
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_DROP_PARTITION_USE_BASE, true);
    runStatementOnDriver("alter table " + Table.ACIDTBLNESTEDPART + " drop partition (p2='a')");
    
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history", 2, resp.getCompactsSize());
    
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] stat;

    for (char p : asList('a', 'b')) {
      String partName = "p1=a/p2=a/p3=" + p;
      Assert.assertTrue(resp.getCompacts().stream().anyMatch(
          ci -> TxnStore.CLEANING_RESPONSE.equals(ci.getState()) && partName.equals(ci.getPartitionname())));
      
      stat = fs.listStatus(new Path(getWarehouseDir(), Table.ACIDTBLNESTEDPART.toString().toLowerCase() + "/" + partName),
          AcidUtils.baseFileFilter);
      if (1 != stat.length) {
        Assert.fail("Expecting 1 base and found " + stat.length + " files " + Arrays.toString(stat));
      }
      String name = stat[0].getPath().getName();
      Assert.assertEquals("base_0000004", name);
    }
    stat = fs.listStatus(new Path(getWarehouseDir(), Table.ACIDTBLNESTEDPART.toString().toLowerCase() + "/p1=a/p2=b/p3=c"),
        AcidUtils.baseFileFilter);
    if (0 != stat.length) {
      Assert.fail("Expecting no base and found " + stat.length + " files " + Arrays.toString(stat));
    }
    
    List<String> r = runStatementOnDriver("select * from " + Table.ACIDTBLNESTEDPART);
    Assert.assertEquals(2, r.size());
    
    runCleaner(hiveConf);

    for (char p : asList('a', 'b')) {
      stat = fs.listStatus(new Path(getWarehouseDir(), Table.ACIDTBLNESTEDPART.toString().toLowerCase() + "/p1=a/p2=a"),
          path -> path.getName().equals("p3=" + p));
      if (0 != stat.length) {
        Assert.fail("Partition data was not removed from FS");
      }
    }
  }
  
  @Test
  public void testDropDatabaseCascadePerTableNonBlocking() throws Exception {
    MetastoreConf.setLongVar(hiveConf, MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX, 1);
    dropDatabaseCascadeNonBlocking();
  }

  @Test
  public void testDropDatabaseCascadePerDbNonBlocking() throws Exception {
    dropDatabaseCascadeNonBlocking();
  }
  
  @Test
  public void testDropDatabaseCascadePerDbNonBlockingFilterTableNames() throws Exception {
    DummyMetaStoreFilterHookImpl.blockResults = true;
    dropDatabaseCascadeNonBlocking();
  }
  
  private void dropDatabaseCascadeNonBlocking() throws Exception {
    String database = "mydb";
    String tableName = "tab_acid";
    
    runStatementOnDriver("drop database if exists " + database + " cascade");
    runStatementOnDriver("create database " + database);
    
    // Create transactional table/materialized view with lockless-reads feature disabled
    runStatementOnDriver("create table " + database + "." + tableName + "1 (a int, b int) " +
      "partitioned by (ds string) stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into " + database + "." + tableName + "1 partition(ds) values(1,2,'foo'),(3,4,'bar')");

    runStatementOnDriver("create materialized view " + database + ".mv_" + tableName + "1 " +
      "partitioned on (ds) stored as orc TBLPROPERTIES ('transactional'='true')" +
      "as select a, ds from " + database + "." + tableName + "1 where b > 1");
    
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, true);
    // Create transactional table/materialized view with lockless-reads feature enabled
    runStatementOnDriver("create table " + database + "." + tableName + "2 (a int, b int) " +
      "partitioned by (ds string) stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into " + database + "." + tableName + "2 partition(ds) values(1,2,'foo'),(3,4,'bar')");

    runStatementOnDriver("create materialized view " + database + ".mv_" + tableName + "2 " +
      "partitioned on (ds) stored as orc TBLPROPERTIES ('transactional'='true')" +
      "as select a, ds from " + database + "." + tableName + "2 where b > 1");

    // Create external partition data
    runStatementOnDriver("drop table if exists Tstage");
    runStatementOnDriver("create table Tstage (a int, b int) stored as orc" +
      " tblproperties('transactional'='false')");
    runStatementOnDriver("insert into Tstage values(0,2),(0,4)");
    
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/1'");
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/2'");
    
    // Create external table
    runStatementOnDriver("create external table " + database + ".tab_ext (a int, b int) " +
      "partitioned by (ds string) stored as parquet");
    runStatementOnDriver("insert into " + database + ".tab_ext partition(ds) values(1,2,'foo'),(3,4,'bar')");
    // Add partition with external location
    runStatementOnDriver("alter table " + database + ".tab_ext add partition (ds='baz') location '" +getWarehouseDir() + "/1/data'");

    // Create managed table
    runStatementOnDriver("create table " + database + ".tab_nonacid (a int, b int) " +
      "partitioned by (ds string) stored as parquet");
    runStatementOnDriver("insert into " + database + ".tab_nonacid partition(ds) values(1,2,'foo'),(3,4,'bar')");
    // Add partition with external location
    runStatementOnDriver("alter table " + database + ".tab_nonacid add partition (ds='baz') location '" +getWarehouseDir() + "/2/data'");
   
    // Drop database cascade
    runStatementOnDriver("drop database " + database + " cascade");

    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] stat = fs.listStatus(new Path(getWarehouseDir(), database + ".db"),
      t -> t.getName().matches("(mv_)?" + tableName + "2" + SOFT_DELETE_TABLE_PATTERN));
    if (2 != stat.length) {
      Assert.fail("Table data was removed from FS");
    }
    stat = fs.listStatus(new Path(getWarehouseDir(), database + ".db"));
    Assert.assertEquals(2, stat.length);
    // External table under warehouse external location should be removed
    stat = fs.listStatus(new Path(getWarehouseDir(), "ext"));
    Assert.assertEquals(0, stat.length);
    // External partition for the external table should remain
    stat = fs.listStatus(new Path(getWarehouseDir(),"1"),
      t -> t.getName().equals("data"));
    Assert.assertEquals(1, stat.length);
    // External partition for managed table should be removed
    stat = fs.listStatus(new Path(getWarehouseDir(), "2"),
      t -> t.getName().equals("data"));
    Assert.assertEquals(0, stat.length);

    runCleaner(hiveConf);

    stat = fs.listStatus(new Path(getWarehouseDir(), database + ".db"),
      t -> t.getName().matches("(mv_)?" + tableName + "2" + SOFT_DELETE_TABLE_PATTERN));
    if (stat.length != 0) {
      Assert.fail("Table data was not removed from FS");
    }
  }
  
  @Test
  public void testDropTableWithSuffix() throws Exception {
    String tableName = "tab_acid";
    runStatementOnDriver("drop table if exists " + tableName);
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX, true);

    runStatementOnDriver("create table " + tableName + "(a int, b int) stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into " + tableName + " values(1,2),(3,4)");
    runStatementOnDriver("drop table " + tableName);

    int count = TestTxnDbUtil.countQueryAgent(hiveConf, 
      "select count(*) from TXN_TO_WRITE_ID where T2W_TABLE = '" + tableName + "'");
    Assert.assertEquals(1, count);
    
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] stat = fs.listStatus(new Path(getWarehouseDir()),
      t -> t.getName().matches(tableName + SOFT_DELETE_TABLE_PATTERN));
    if (1 != stat.length) {
      Assert.fail("Table data was removed from FS");
    }
    MetastoreTaskThread houseKeeperService = new AcidHouseKeeperService();
    houseKeeperService.setConf(hiveConf);
    
    houseKeeperService.run();
    count = TestTxnDbUtil.countQueryAgent(hiveConf,
      "select count(*) from TXN_TO_WRITE_ID where T2W_TABLE = '" + tableName + "'");
    Assert.assertEquals(0, count);

    try {
      runStatementOnDriver("select * from " + tableName);
    } catch (Exception ex) {
      Assert.assertTrue(ex.getMessage().contains(
        ErrorMsg.INVALID_TABLE.getMsg(StringUtils.wrap(tableName, "'"))));
    }
    // Check status of compaction job
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    
    Assert.assertEquals("Unexpected number of compactions in history", 1, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0 compaction state",
      TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(0).getState());

    runCleaner(hiveConf);
    
    FileStatus[] status = fs.listStatus(new Path(getWarehouseDir()),
      t -> t.getName().matches(tableName + SOFT_DELETE_TABLE_PATTERN));
    Assert.assertEquals(0, status.length);
  }

  @Test
  public void testDropTableWithoutSuffix() throws Exception {
    String tableName = "tab_acid";
    runStatementOnDriver("drop table if exists " + tableName);
    
    for (boolean enabled : asList(false, true)) {
      HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX, enabled);
      runStatementOnDriver("create table " + tableName + "(a int, b int) stored as orc TBLPROPERTIES ('transactional'='true')");
      runStatementOnDriver("insert into " + tableName + " values(1,2),(3,4)");
      
      HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX, !enabled);
      runStatementOnDriver("drop table " + tableName);

      int count = TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from TXN_TO_WRITE_ID where T2W_TABLE = '" + tableName + "'");
      Assert.assertEquals(0, count);

      FileSystem fs = FileSystem.get(hiveConf);
      FileStatus[] stat = fs.listStatus(new Path(getWarehouseDir()),
        t -> t.getName().equals(tableName));
      Assert.assertEquals(0, stat.length);

      try {
        runStatementOnDriver("select * from " + tableName);
      } catch (Exception ex) {
        Assert.assertTrue(ex.getMessage().contains(
          ErrorMsg.INVALID_TABLE.getMsg(StringUtils.wrap(tableName, "'"))));
      }
      // Check status of compaction job
      TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
      ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
      Assert.assertEquals("Unexpected number of compactions in history", 0, resp.getCompactsSize());
    }
  }

  @Test
  public void testDropMaterializedViewWithSuffix() throws Exception {
    String tableName = "tab_acid";
    String mviewName = "mv_" + tableName;
    runStatementOnDriver("drop materialized view if exists " + mviewName);
    runStatementOnDriver("drop table if exists " + tableName);
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX, true);

    runStatementOnDriver("create table " + tableName + "(a int, b int) stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into " + tableName + " values(1,2),(3,4)");
    runStatementOnDriver("create materialized view " + mviewName + " stored as orc TBLPROPERTIES ('transactional'='true') " +
      "as select a from tab_acid where b > 1");
    runStatementOnDriver("drop materialized view " + mviewName);

    int count = TestTxnDbUtil.countQueryAgent(hiveConf,
      "select count(*) from TXN_TO_WRITE_ID where T2W_TABLE = '" + mviewName + "'");
    Assert.assertEquals(1, count);

    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] stat = fs.listStatus(new Path(getWarehouseDir()),
      t -> t.getName().matches(mviewName + SOFT_DELETE_TABLE_PATTERN));
    if (1 != stat.length) {
      Assert.fail("Materialized view data was removed from FS");
    }
    MetastoreTaskThread houseKeeperService = new AcidHouseKeeperService();
    houseKeeperService.setConf(hiveConf);

    houseKeeperService.run();
    count = TestTxnDbUtil.countQueryAgent(hiveConf,
      "select count(*) from TXN_TO_WRITE_ID where T2W_TABLE = '" + mviewName + "'");
    Assert.assertEquals(0, count);

    try {
      runStatementOnDriver("select * from " + mviewName);
    } catch (Exception ex) {
      Assert.assertTrue(ex.getMessage().contains(
        ErrorMsg.INVALID_TABLE.getMsg(StringUtils.wrap(mviewName, "'"))));
    }
    // Check status of compaction job
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());

    Assert.assertEquals("Unexpected number of compactions in history", 1, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0 compaction state",
      TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(0).getState());

    runCleaner(hiveConf);

    FileStatus[] status = fs.listStatus(new Path(getWarehouseDir()),
      t -> t.getName().matches(mviewName + SOFT_DELETE_TABLE_PATTERN));
    Assert.assertEquals(0, status.length);
  }

  @Test
  public void testDropMaterializedViewWithoutSuffix() throws Exception {
    String tableName = "tab_acid";
    String mviewName = "mv_" + tableName;
    runStatementOnDriver("drop materialized view if exists " + mviewName);

    for (boolean enabled : asList(false, true)) {
      runStatementOnDriver("drop table if exists " + tableName);
      HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX, enabled);
      
      runStatementOnDriver("create table " + tableName + "(a int, b int) stored as orc TBLPROPERTIES ('transactional'='true')");
      runStatementOnDriver("insert into " + tableName + " values(1,2),(3,4)");
      runStatementOnDriver("create materialized view " + mviewName + " stored as orc TBLPROPERTIES ('transactional'='true') " +
        "as select a from tab_acid where b > 1");

      HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX, !enabled);
      runStatementOnDriver("drop materialized view " + mviewName);

      int count = TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from TXN_TO_WRITE_ID where T2W_TABLE = '" + mviewName + "'");
      Assert.assertEquals(0, count);

      FileSystem fs = FileSystem.get(hiveConf);
      FileStatus[] stat = fs.listStatus(new Path(getWarehouseDir()),
        t -> t.getName().equals(mviewName));
      Assert.assertEquals(0, stat.length);

      try {
        runStatementOnDriver("select * from " + mviewName);
      } catch (Exception ex) {
        Assert.assertTrue(ex.getMessage().contains(
          ErrorMsg.INVALID_TABLE.getMsg(StringUtils.wrap(mviewName, "'"))));
      }
      // Check status of compaction job
      TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
      ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
      Assert.assertEquals("Unexpected number of compactions in history", 0, resp.getCompactsSize());
    }
  }

  @Test
  public void testRenameMakeCopyPartition() throws Exception {
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition (p='a') values (1,2),(3,4)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition (p='b') values (5,5),(4,4)");

    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_RENAME_PARTITION_MAKE_COPY, true);
    runStatementOnDriver("alter table " + Table.ACIDTBLPART + " partition (p='b') rename to partition (p='c')");

    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] stat = fs.listStatus(new Path(getWarehouseDir(), Table.ACIDTBLPART.toString().toLowerCase() + "/p=b"),
      AcidUtils.baseFileFilter);
    if (1 != stat.length) {
      Assert.fail("Expecting 1 base and found " + stat.length + " files " + Arrays.toString(stat));
    }
    String name = stat[0].getPath().getName();
    Assert.assertEquals("base_0000003", name);
    stat = fs.listStatus(new Path(getWarehouseDir(), Table.ACIDTBLPART.toString().toLowerCase() + "/p=a"),
      AcidUtils.baseFileFilter);
    if (0 != stat.length) {
      Assert.fail("Expecting no base and found " + stat.length + " files " + Arrays.toString(stat));
    }
    
    List<String> r = runStatementOnDriver("select * from " + Table.ACIDTBLPART + " where p='b'");
    Assert.assertEquals(0, r.size());

    r = runStatementOnDriver("select * from " + Table.ACIDTBLPART);
    Assert.assertEquals(4, r.size());

    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());

    Assert.assertEquals("Unexpected number of compactions in history", 1, resp.getCompactsSize());
    Assert.assertTrue(resp.getCompacts().stream().anyMatch(
      ci -> TxnStore.CLEANING_RESPONSE.equals(ci.getState()) && "p=b".equals(ci.getPartitionname())));

    runCleaner(hiveConf);

    stat = fs.listStatus(new Path(getWarehouseDir(), Table.ACIDTBLPART.toString().toLowerCase()),
      path -> path.getName().equals("p=b"));
    if (0 != stat.length) {
      Assert.fail("Expecting partition data to be removed from FS");
    }
  }

  @Test
  public void testRenameMakeCopyNestedPartition() throws Exception {
    runStatementOnDriver("insert into " + Table.ACIDTBLNESTEDPART + " partition (p1='a', p2='b', p3='c') values (1,1),(2,2)");
    runStatementOnDriver("insert into " + Table.ACIDTBLNESTEDPART + " partition (p1='a', p2='b', p3='d') values (3,3),(4,4)");

    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_RENAME_PARTITION_MAKE_COPY, true);
    runStatementOnDriver("alter table " + Table.ACIDTBLNESTEDPART + " partition (p1='a', p2='b', p3='d')" +
      " rename to partition (p1='a', p2='c', p3='d')");

    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history", 1, resp.getCompactsSize());

    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] stat;
    
    String partName = "p1=a/p2=b/p3=d";
    Assert.assertTrue(resp.getCompacts().stream().anyMatch(
      ci -> TxnStore.CLEANING_RESPONSE.equals(ci.getState()) && partName.equals(ci.getPartitionname())));

    stat = fs.listStatus(new Path(getWarehouseDir(), Table.ACIDTBLNESTEDPART.toString().toLowerCase() + "/" + partName),
      AcidUtils.baseFileFilter);
    if (1 != stat.length) {
      Assert.fail("Expecting 1 base and found " + stat.length + " files " + Arrays.toString(stat));
    }
    String name = stat[0].getPath().getName();
    Assert.assertEquals("base_0000003", name);
    
    stat = fs.listStatus(new Path(getWarehouseDir(), Table.ACIDTBLNESTEDPART.toString().toLowerCase() + "/p1=a/p2=c/p3=d"),
      AcidUtils.baseFileFilter);
    if (0 != stat.length) {
      Assert.fail("Expecting no base and found " + stat.length + " files " + Arrays.toString(stat));
    }

    List<String> r = runStatementOnDriver("select * from " + Table.ACIDTBLNESTEDPART);
    Assert.assertEquals(4, r.size());

    runCleaner(hiveConf);
    
    stat = fs.listStatus(new Path(getWarehouseDir(), Table.ACIDTBLNESTEDPART.toString().toLowerCase() + "/p1=a/p2=b"),
      path -> path.getName().equals("p3=d"));
    if (0 != stat.length) {
      Assert.fail("Expecting partition data to be removed from FS");
    }
  }

  @Test
  public void testIsRawFormatFile() throws Exception {
    dropTable(new String[]{"file_formats"});
    
    runStatementOnDriver("CREATE TABLE `file_formats`(`id` int, `name` string) " +
      " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' " +
      "WITH SERDEPROPERTIES ( " +
      " 'field.delim'='|', " +
      " 'line.delim'='\n'," +
      " 'serialization.format'='|')  " +
      "STORED AS " +
      " INPUTFORMAT " +
      "   'org.apache.hadoop.mapred.TextInputFormat' " +
      " OUTPUTFORMAT " +
      "   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' " +
      "TBLPROPERTIES ( " +
      " 'transactional'='true'," +
      " 'transactional_properties'='insert_only')");
    
    runStatementOnDriver("insert into file_formats (id, name) values (1, 'Avro'),(2, 'Parquet'),(3, 'ORC')");
    
    List<String> res = runStatementOnDriver("select * from file_formats");
    Assert.assertEquals(3, res.size());
  }
  @Test
  public void testShowCompactions() throws Exception {
    //generate some compaction history
    runStatementOnDriver("drop database if exists mydb1 cascade");
    runStatementOnDriver("create database mydb1");
    runStatementOnDriver("create table mydb1.tbl0 " + "(a int, b int) partitioned by (p string) clustered by (a) into " +
      BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into mydb1.tbl0" + " PARTITION(p) " +
      " values(1,2,'p1'),(3,4,'p1'),(1,2,'p2'),(3,4,'p2'),(1,2,'p3'),(3,4,'p3')");
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION(p='p1') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION(p='p2') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION(p='p3') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);
    runStatementOnDriver("insert into mydb1.tbl0" + " PARTITION(p) " +
      " values(4,5,'p1'),(6,7,'p1'),(4,5,'p2'),(6,7,'p2'),(4,5,'p3'),(6,7,'p3')");
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION (p='p1') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION (p='p2') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION (p='p3')  compact 'MAJOR' pool 'pool0'");
    TestTxnCommands2.runWorker(hiveConf);
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);

    SessionState.get().setCurrentDatabase("mydb1");

    //testing show compaction command
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<String> r = runStatementOnDriver("SHOW COMPACTIONS");
    Assert.assertEquals(rsp.getCompacts().size()+1, r.size());//includes Header row

    r = runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb1 STATUS 'ready for cleaning'");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getState().equals("ready for cleaning")).count() +1,
            r.size());//includes Header row
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
      "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
      "Highest WriteId", r.get(0));
    Pattern p = Pattern.compile(".*mydb1.*\tready for cleaning.*");
    for(int i = 1; i < r.size(); i++) {
      Assert.assertTrue(p.matcher(r.get(i)).matches());
    }
    r = runStatementOnDriver("SHOW COMPACTIONS COMPACTIONID=1");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getId()==1).count() +1,
            r.size());//includes Header row
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
            "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
            "Highest WriteId", r.get(0));
    p = Pattern.compile("1\t.*");
    for(int i = 1; i < r.size(); i++) {
      Assert.assertTrue(p.matcher(r.get(i)).matches());
    }
    r = runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb1 TYPE 'MAJOR' ");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getDbname().equals("mydb1")).
      filter(x->x.getType().equals(CompactionType.MAJOR)).count()+1, r.size());//includes Header row
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
   "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
   "Highest WriteId", r.get(0));
    p = Pattern.compile(".*mydb1.*\tMAJOR.*");
    for(int i = 1; i < r.size(); i++) {
      Assert.assertTrue(p.matcher(r.get(i)).matches());
    }

    r = runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb1 POOL 'poolx' TYPE 'MINOR' ");
    //includes Header row
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getDbname().equals("mydb1")).
      filter(x->x.getPoolName().equals("poolx")).filter(x->x.getType().equals(CompactionType.MAJOR)).count()+1, r.size());
    Assert.assertEquals(1,r.size());//only header row

    r = runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb1 POOL 'pool0' TYPE 'MAJOR'");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getDbname().equals("mydb1")).
      filter(x->x.getPoolName().equals("pool0")).filter(x->x.getType().equals(CompactionType.MAJOR)).count()+1, r.size());//includes Header row
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
    "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
    "Highest WriteId", r.get(0));
    p = Pattern.compile(".*mydb1.*\tMAJOR.*\tpool0.*");
    for(int i = 1; i < r.size(); i++) {
      Assert.assertTrue(p.matcher(r.get(i)).matches());
    }

    r = runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb1 POOL 'pool0'");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getDbname().equals("mydb1")).
      filter(x->x.getPoolName().equals("pool0")).count()+1, r.size());//includes Header row
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
    "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
    "Highest WriteId", r.get(0));
    p = Pattern.compile(".*mydb1.*\tpool0.*");
    for(int i = 1; i < r.size(); i++) {
      Assert.assertTrue(p.matcher(r.get(i)).matches());
    }

    r = runStatementOnDriver("SHOW COMPACTIONS DATABASE mydb1 POOL 'pool0'");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getDbname().equals("mydb1")).
      filter(x->x.getPoolName().equals("pool0")).count()+1, r.size());//includes Header row
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
            "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
            "Highest WriteId", r.get(0));
    p = Pattern.compile(".*mydb1.*\tpool0.*");
    for(int i = 1; i < r.size(); i++) {
      Assert.assertTrue(p.matcher(r.get(i).toString()).matches());
    }

    r = runStatementOnDriver("SHOW COMPACTIONS tbl0 TYPE 'MAJOR' ");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getTablename().equals("tbl0")).
      filter(x->x.getType().equals(CompactionType.MAJOR)).count()+1, r.size());//includes Header row
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
            "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
            "Highest WriteId", r.get(0));
    p = Pattern.compile(".*tbl0.*\tMAJOR.*");
    for(int i = 1; i < r.size(); i++) {
      Assert.assertTrue(p.matcher(r.get(i)).matches());
    }

    r = runStatementOnDriver("SHOW COMPACTIONS mydb1.tbl0 PARTITION (p='p3') ");
    //includes Header row
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getDbname().equals("mydb1")).
      filter(x->x.getTablename().equals("tbl0")).filter(x->x.getPartitionname().equals("p=p3")).count() + 1, r.size());
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
            "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
            "Highest WriteId", r.get(0));
    p = Pattern.compile(".*mydb1\ttbl0\tp=p3.*");
    for(int i = 1; i < r.size(); i++) {
      Assert.assertTrue(p.matcher(r.get(i)).matches());
    }
    //includes Header row
    r = runStatementOnDriver("SHOW COMPACTIONS mydb1.tbl0 PARTITION (p='p3') pool 'pool0' TYPE 'MAJOR'");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getDbname().equals("mydb1")).
      filter(x->x.getTablename().equals("tbl0")).filter(x->x.getPartitionname().equals("p=p3")).
      filter(x->x.getPoolName().equals("pool0")).filter(x->x.getType().equals(CompactionType.MAJOR)).count() + 1, r.size());
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
            "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
            "Highest WriteId", r.get(0));
    p = Pattern.compile(".*mydb1\ttbl0\tp=p3\tMAJOR.*\tpool0.*");
    for(int i = 1; i < r.size(); i++) {
      Assert.assertTrue(p.matcher(r.get(i)).matches());
    }

  }

  @Test
  public void testShowCompactionFilterWithPartition()throws Exception {
    setUpCompactionRequestsData("mydb","tbl2");
    executeCompactionRequest("mydb","tbl2", "MAJOR","ds='mon'");
    SessionState.get().setCurrentDatabase("mydb");

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());

    //includes Header row
    List<String> r = runStatementOnDriver("SHOW COMPACTIONS");
    Assert.assertEquals(rsp.getCompacts().size()+1, r.size());

    //includes Header row
    r = runStatementOnDriver("SHOW COMPACTIONS tbl2 STATUS 'refused'");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getState().equals("refused")).count()+1, r.size());
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
            "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
            "Highest WriteId", r.get(0));
    Pattern p = Pattern.compile(".*tbl2.*\trefused.*");
    for(int i = 1; i < r.size(); i++) {
      Assert.assertTrue(p.matcher(r.get(i)).matches());
    }

    //includes Header row
    r = runStatementOnDriver("SHOW COMPACTIONS tbl2 ");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getTablename().equals("tbl2")).count()+1, r.size());
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
            "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
            "Highest WriteId", r.get(0));
    p = Pattern.compile(".*tbl2.*");
    for(int i = 1; i < r.size(); i++) {
      Assert.assertTrue(p.matcher(r.get(i)).matches());
    }

    //includes Header row
    r = runStatementOnDriver("SHOW COMPACTIONS mydb.tbl2 PARTITION (ds='mon') ");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getDbname().equals("mydb")).
    filter(x->x.getTablename().equals("tbl2")).filter(x->x.getPartitionname().equals("ds=mon")).count()+1, r.size());
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
            "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
            "Highest WriteId", r.get(0));
    p = Pattern.compile(".*mydb\ttbl2\tds=mon.*");
    for(int i = 1; i < r.size(); i++) {
      Assert.assertTrue(p.matcher(r.get(i)).matches());
    }

    //includes Header row
    r = runStatementOnDriver("SHOW COMPACTIONS mydb.tbl2 PARTITION (ds='mon') TYPE 'MAJOR' ");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getDbname().equals("mydb")).
            filter(x->x.getTablename().equals("tbl2")).filter(x->x.getPartitionname().equals("ds=mon")).
            filter(x->x.getType().equals(CompactionType.MAJOR)).count()+1, r.size());
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
            "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
            "Highest WriteId", r.get(0));
    p = Pattern.compile(".*mydb\ttbl2\tds=mon\tMAJOR.*");
    for(int i = 1; i < r.size(); i++) {
      Assert.assertTrue(p.matcher(r.get(i)).matches());
    }

    //includes Header row
    r = runStatementOnDriver("SHOW COMPACTIONS DATABASE mydb TYPE 'MAJOR' ");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getDbname().equals("mydb")).
            filter(x->x.getType().equals(CompactionType.MAJOR)).count()+1, r.size());
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
            "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
            "Highest WriteId", r.get(0));
    p = Pattern.compile(".*mydb.*\tMAJOR.*");
    for(int i = 1; i < r.size(); i++) {
      Assert.assertTrue(p.matcher(r.get(i)).matches());
    }

    executeCompactionRequest("mydb","tbl2", "MINOR","ds='wed'");

    rsp = txnHandler.showCompact(new ShowCompactRequest());
    //includes Header row
    r = runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb TYPE 'MINOR' ");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getDbname().equals("mydb")).
            filter(x->x.getType().equals(CompactionType.MINOR)).count()+1, r.size());
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
            "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
            "Highest WriteId", r.get(0));
    p = Pattern.compile(".*mydb.*\tMINOR.*");
    for(int i = 1; i < r.size(); i++) {
      Assert.assertTrue(p.matcher(r.get(i)).matches());
    }

    //includes Header row
    r = runStatementOnDriver("SHOW COMPACTIONS  mydb.tbl2 PARTITION (ds='wed') ");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getDbname().equals("mydb")).
    filter(x->x.getTablename().equals("tbl2")).filter(x->x.getPartitionname().equals("ds=wed")).count()+1, r.size());
    for(int i=1;i<r.size();i++) {
      Assert.assertTrue(r.get(i).contains("mydb"));
      Assert.assertTrue(r.get(i).contains("tbl2"));
      Assert.assertTrue(r.get(i).contains("ds=wed"));
    }

    r = runStatementOnDriver("SHOW COMPACTIONS tbl2 ");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getTablename().equalsIgnoreCase("tbl2")).count()+1, r.size());
    for(int i=1;i<r.size();i++) {
      Assert.assertTrue(r.get(i).contains("tbl2"));
    }
    //includes Header row
    r = runStatementOnDriver("SHOW COMPACTIONS SCHEMA mymydbdb2 TYPE 'MAJOR' ");
    Assert.assertEquals(rsp.getCompacts().stream().filter(x->x.getDbname().equals("mymydbdb2")).
            filter(x->x.getType().equals(CompactionType.MAJOR)).count()+1, r.size());
    Assert.assertEquals(1,r.size());//only header row
  }
  @Test
  public void testShowCompactionInputValidation() throws Exception {
    setUpCompactionRequestsData("mydb2","tbl2");
    executeCompactionRequest("mydb2","tbl2", "MAJOR", "ds='mon'");
    SessionState.get().setCurrentDatabase("mydb2");

    //validation testing of paramters
    expectedException.expect(RuntimeException.class);
    List<String> r  = runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb POOL 'pool0' TYPE 'MAJOR'");// validates db
    r = runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb2  TYPE 'MAJR'");// validates compaction type
    r = runStatementOnDriver("SHOW COMPACTIONS mydb2.tbl1 PARTITION (ds='mon') TYPE 'MINOR' " +
          "STATUS 'ready for clean'");// validates table
    r = runStatementOnDriver("SHOW COMPACTIONS mydb2.tbl2 PARTITION (p=101,day='Monday') POOL 'pool0' TYPE 'minor' " +
          "STATUS 'ready for clean'");// validates partspec
    r = runStatementOnDriver("SHOW COMPACTIONS mydb1.tbl0 PARTITION (p='p1') POOL 'pool0' TYPE 'minor' " +
          "STATUS 'ready for clean'");//validates compaction status
  }

  @Test
  public void testShowCompactionFilterSortingAndLimit() throws Exception {
    runStatementOnDriver("drop database if exists mydb1 cascade");
    runStatementOnDriver("create database mydb1");
    runStatementOnDriver("create table mydb1.tbl0 " + "(a int, b int) partitioned by (p string) clustered by (a) into " +
            BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into mydb1.tbl0" + " PARTITION(p) " +
            " values(1,2,'p1'),(3,4,'p1'),(1,2,'p2'),(3,4,'p2'),(1,2,'p3'),(3,4,'p3')");
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION(p='p1') compact 'MAJOR' pool 'poolx'");
    TestTxnCommands2.runWorker(hiveConf);
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION(p='p2') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);


    runStatementOnDriver("drop database if exists mydb cascade");
    runStatementOnDriver("create database mydb");
    runStatementOnDriver("create table mydb.tbl " + "(a int, b int) partitioned by (ds string) clustered by (a) into " +
            BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into mydb.tbl" + " PARTITION(ds) " +
            " values(1,2,'mon'),(3,4,'tue'),(1,2,'mon'),(3,4,'tue'),(1,2,'wed'),(3,4,'wed')");
    runStatementOnDriver("alter table mydb.tbl" + " PARTITION(ds='mon') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);
    runStatementOnDriver("alter table mydb.tbl" + " PARTITION(ds='tue') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);

    runStatementOnDriver("create table mydb.tbl2 " + "(a int, b int) partitioned by (dm string) clustered by (a) into " +
            BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into mydb.tbl2" + " PARTITION(dm) " +
            " values(1,2,'xxx'),(3,4,'xxx'),(1,2,'yyy'),(3,4,'yyy'),(1,2,'zzz'),(3,4,'zzz')");
    runStatementOnDriver("alter table mydb.tbl2" + " PARTITION(dm='yyy') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);
    runStatementOnDriver("alter table mydb.tbl2" + " PARTITION(dm='zzz') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());

    //includes Header row
    List<String> r = runStatementOnDriver("SHOW COMPACTIONS");
    Assert.assertEquals(rsp.getCompacts().size() + 1, r.size());
    r = runStatementOnDriver("SHOW COMPACTIONS LIMIT 3");
    Assert.assertEquals(4, r.size());
    r = runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb TYPE 'MAJOR' LIMIT 2");
    Assert.assertEquals(3, r.size());

    r = runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb TYPE 'MAJOR' ORDER BY 'tabname' DESC,'partname' ASC");
    Assert.assertEquals(5, r.size());
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
            "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
            "Highest WriteId", r.get(0));
    Pattern p = Pattern.compile(".*mydb\ttbl2\tdm.*");
    for (int i = 1; i < r.size() - 3; i++) {
      Assert.assertTrue(p.matcher(r.get(i)).matches());
    }
    p = Pattern.compile(".*mydb\ttbl\tds.*");
    for (int i = 3; i < r.size() - 1; i++) {
      Assert.assertTrue(p.matcher(r.get(i)).matches());
    }

    r = runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb1 TYPE 'MAJOR' ORDER BY 'poolname' ASC");
    Assert.assertEquals(3, r.size());
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
            "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
            "Highest WriteId", r.get(0));
    List<String> txnIdActualList = r.stream().skip(1).map(x -> x.split("\t")[15]).collect(Collectors.toList());
    List<String> txnIdExpectedList = r.stream().skip(1).map(x -> x.split("\t")[15]).sorted(Collections.reverseOrder()).
            collect(Collectors.toList());
    Assert.assertEquals(txnIdExpectedList, txnIdActualList);
    r = runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb TYPE 'MAJOR' ORDER BY 'txnid' DESC");
    Assert.assertEquals(5, r.size());
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
            "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
            "Highest WriteId", r.get(0));
    txnIdActualList = r.stream().skip(1).map(x -> x.split("\t")[16]).collect(Collectors.toList());
    txnIdExpectedList = r.stream().skip(1).map(x -> x.split("\t")[16]).sorted(Collections.reverseOrder()).
            collect(Collectors.toList());
    Collections.sort(txnIdExpectedList, Collections.reverseOrder());
    Assert.assertEquals(txnIdExpectedList, txnIdActualList);

    r = runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb TYPE 'MAJOR' ORDER BY TxnId DESC");
    Assert.assertEquals(5, r.size());
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time" +
            "\tDuration(ms)\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\t" +
            "Highest WriteId", r.get(0));
    txnIdActualList = r.stream().skip(1).map(x -> x.split("\t")[16]).collect(Collectors.toList());
    txnIdExpectedList = r.stream().skip(1).map(x -> x.split("\t")[16]).sorted(Collections.reverseOrder()).
            collect(Collectors.toList());
    Assert.assertEquals(txnIdExpectedList, txnIdActualList);


    expectedException.expect(RuntimeException.class);
    r = runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb TYPE 'MAJOR' ORDER BY tbl DESC,PARTITIONS ASC");
    expectedException.expect(RuntimeException.class);
    r = runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb TYPE 'MAJOR' ORDER BY tbl DESC,PARTITIONS ASC");

  }

  @Test
  public void testAbortCompactions() throws Exception {
    //generate some compaction history
    runStatementOnDriver("drop database if exists mydb1 cascade");
    runStatementOnDriver("create database mydb1");
    runStatementOnDriver("create table mydb1.tbl0 " + "(a int, b int) partitioned by (p string) clustered by (a) into " +
            BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into mydb1.tbl0" + " PARTITION(p) " +
            " values(1,2,'p1'),(3,4,'p1'),(1,2,'p2'),(3,4,'p2'),(1,2,'p3'),(3,4,'p3')");
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION(p='p1') compact 'MAJOR'");
    TestTxnCommands2.runInitiator(hiveConf);
    TestTxnCommands2.runWorker(hiveConf);
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION(p='p2') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION(p='p3') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);
    runStatementOnDriver("insert into mydb1.tbl0" + " PARTITION(p) " +
            " values(4,5,'p1'),(6,7,'p1'),(4,5,'p2'),(6,7,'p2'),(4,5,'p3'),(6,7,'p3')");
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION (p='p1') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION (p='p2') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION (p='p3')  compact 'MAJOR' pool 'pool0'");
    TestTxnCommands2.runInitiator(hiveConf);
    TestTxnCommands2.runWorker(hiveConf);
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);



    runStatementOnDriver("create table mydb1.tbl2 " + "(a int, b int) partitioned by (p string) clustered by (a) into " +
            BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into mydb1.tbl2" + " PARTITION(p) " +
            " values(1,2,'p1'),(3,4,'p1'),(1,2,'p2'),(3,4,'p2'),(1,2,'p3'),(3,4,'p3')");
    runStatementOnDriver("alter table mydb1.tbl2" + " PARTITION(p='p1') compact 'MAJOR'");
    runStatementOnDriver("alter table mydb1.tbl2" + " PARTITION(p='p2') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);
    TestTxnCommands2.runCleaner(hiveConf);
    runStatementOnDriver("alter table mydb1.tbl2" + " PARTITION(p='p3') compact 'MAJOR'");
    runStatementOnDriver("insert into mydb1.tbl0" + " PARTITION(p) " +
            " values(4,5,'p1'),(6,7,'p1'),(4,5,'p2'),(6,7,'p2'),(4,5,'p3'),(6,7,'p3')");
    TestTxnCommands2.runWorker(hiveConf);
    TestTxnCommands2.runCleaner(hiveConf);
    runStatementOnDriver("insert into mydb1.tbl2" + " PARTITION(p) " +
            " values(11,12,'p1'),(13,14,'p1'),(11,12,'p2'),(13,14,'p2'),(11,12,'p3'),(13,14,'p3')");
    runStatementOnDriver("alter table mydb1.tbl2" + " PARTITION (p='p1')  compact 'MINOR'");
    TestTxnCommands2.runWorker(hiveConf);

    runStatementOnDriver("create table mydb1.tbl3 " + "(a int, b int) partitioned by (ds string) clustered by (a) into " +
            BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into mydb1.tbl3" + " PARTITION(ds) " +
            " values(1,2,'today'),(3,4,'today'),(1,2,'tomorrow'),(3,4,'tomorrow'),(1,2,'yesterday'),(3,4,'yesterday')");
    runStatementOnDriver("alter table mydb1.tbl3" + " PARTITION(ds='today') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);

    SessionState.get().setCurrentDatabase("mydb1");

    //testing show compaction command

    List<String> r =  runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb1 STATUS 'initiated'");
    Assert.assertEquals(3,r.size());
    List<String>compIdsToAbort = r.stream().skip(1).map(x -> x.split("\t")[0]).collect(Collectors.toList());
    String abortCompactionCmd = "ABORT COMPACTIONS " +compIdsToAbort.get(0)+"\t"+compIdsToAbort.get(1);
    r = runStatementOnDriver(abortCompactionCmd);
    Assert.assertEquals(3,r.size());
    Assert.assertEquals("CompactionId\tStatus\tMessage", r.get(0));
    Assert.assertTrue(r.get(1).contains("Successfully aborted compaction"));
    Assert.assertTrue(r.get(2).contains("Successfully aborted compaction"));

    abortCompactionCmd = "ABORT COMPACTIONS " +compIdsToAbort.get(0)+"\t"+compIdsToAbort.get(1);
    r = runStatementOnDriver(abortCompactionCmd);
    Assert.assertEquals(3,r.size());
    Assert.assertEquals("CompactionId\tStatus\tMessage", r.get(0));
    Assert.assertTrue(r.get(1).contains("Error"));

    r =  runStatementOnDriver("SHOW COMPACTIONS SCHEMA mydb1 STATUS 'aborted'");
    Assert.assertEquals(3,r.size());

  }


  private void setUpCompactionRequestsData(String dbName, String tbName) throws Exception {
    runStatementOnDriver("drop database if exists " + dbName);
    runStatementOnDriver("create database " + dbName);
    runStatementOnDriver("create table " + dbName + "." + tbName + " (a int, b int) partitioned by (ds String)  stored as orc " +
       "TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into " + dbName + "." + tbName + " PARTITION (ds) " +
      " values(1,2,'mon'),(3,4,'mon'),(1,2,'tue'),(3,4,'tue'),(1,2,'wed'),(3,4,'wed')");
  }

  private void executeCompactionRequest(String dbName, String tbName, String compactiontype, String partition) throws Exception {
    runStatementOnDriver("alter table "+dbName+"."+tbName+" PARTITION (" +partition+") compact '"+compactiontype + "'" );
    TestTxnCommands2.runWorker(hiveConf);
  }

  @Test
  public void testFetchTaskCachingWithConversion() throws Exception {
    dropTable(new String[]{"fetch_task_table"});
    List actualRes = new ArrayList<>();
    runStatementOnDriver("create table fetch_task_table (a INT, b INT) stored as orc" +
            " tblproperties ('transactional'='true')");
    runStatementOnDriver("insert into table fetch_task_table values (1,2), (3,4), (5,6)");
    List expectedRes = runStatementOnDriver("select * from fetch_task_table");

    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_FETCH_TASK_CACHING, true);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
    d.run("select * from fetch_task_table");
    Assert.assertFalse(d.getFetchTask().isCachingEnabled());
    d.getFetchTask().fetch(actualRes);
    Assert.assertEquals(actualRes, expectedRes);
    actualRes.clear();

    hiveConf.setVar(HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "more");
    d.run("select * from fetch_task_table");
    Assert.assertTrue(d.getFetchTask().isCachingEnabled());
    d.getFetchTask().fetch(actualRes);
    Assert.assertEquals(actualRes, expectedRes);
  }
}
