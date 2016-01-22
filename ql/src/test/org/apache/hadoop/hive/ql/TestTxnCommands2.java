/**
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

package org.apache.hadoop.hive.ql;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HouseKeeperService;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.txn.CompactionTxnHandler;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.AcidCompactionHistoryService;
import org.apache.hadoop.hive.ql.txn.compactor.Cleaner;
import org.apache.hadoop.hive.ql.txn.compactor.Initiator;
import org.apache.hadoop.hive.ql.txn.compactor.Worker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TODO: this should be merged with TestTxnCommands once that is checked in
 * specifically the tests; the supporting code here is just a clone of TestTxnCommands
 */
public class TestTxnCommands2 {
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
    File.separator + TestTxnCommands2.class.getCanonicalName()
    + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
  private static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  //bucket count for test tables; set it to 1 for easier debugging
  private static int BUCKET_COUNT = 2;
  @Rule
  public TestName testName = new TestName();
  private HiveConf hiveConf;
  private Driver d;
  private static enum Table {
    ACIDTBL("acidTbl"),
    ACIDTBLPART("acidTblPart"),
    NONACIDORCTBL("nonAcidOrcTbl"),
    NONACIDPART("nonAcidPart");

    private final String name;
    @Override
    public String toString() {
      return name;
    }
    Table(String name) {
      this.name = name;
    }
  }

  @Before
  public void setUp() throws Exception {
    tearDown();
    hiveConf = new HiveConf(this.getClass());
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, TEST_WAREHOUSE_DIR);
    hiveConf.setVar(HiveConf.ConfVars.HIVEMAPREDMODE, "nonstrict");
    TxnDbUtil.setConfValues(hiveConf);
    TxnDbUtil.prepDb();
    File f = new File(TEST_WAREHOUSE_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(TEST_WAREHOUSE_DIR).mkdirs())) {
      throw new RuntimeException("Could not create " + TEST_WAREHOUSE_DIR);
    }
    SessionState.start(new SessionState(hiveConf));
    d = new Driver(hiveConf);
    dropTables();
    runStatementOnDriver("create table " + Table.ACIDTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + Table.ACIDTBLPART + "(a int, b int) partitioned by (p string) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + Table.NONACIDORCTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table " + Table.NONACIDPART + "(a int, b int) partitioned by (p string) stored as orc TBLPROPERTIES ('transactional'='false')");
  }
  private void dropTables() throws Exception {
    for(Table t : Table.values()) {
      runStatementOnDriver("drop table if exists " + t);
    }
  }
  @After
  public void tearDown() throws Exception {
    try {
      if (d != null) {
        dropTables();
        d.destroy();
        d.close();
        d = null;
      }
      TxnDbUtil.cleanDb();
    } finally {
      FileUtils.deleteDirectory(new File(TEST_DATA_DIR));
    }
  }
  @Test
  public void testOrcPPD() throws Exception  {
    testOrcPPD(true);
  }
  @Test
  public void testOrcNoPPD() throws Exception {
    testOrcPPD(false);
  }

  /**
   * this is run 2 times: 1 with PPD on, 1 with off
   * Also, the queries are such that if we were to push predicate down to an update/delete delta,
   * the test would produce wrong results
   * @param enablePPD
   * @throws Exception
   */
  private void testOrcPPD(boolean enablePPD) throws Exception {
    boolean originalPpd = hiveConf.getBoolVar(HiveConf.ConfVars.HIVEOPTINDEXFILTER);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVEOPTINDEXFILTER, enablePPD);//enables ORC PPD
    //create delta_0001_0001_0000 (should push predicate here)
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(new int[][]{{1, 2}, {3, 4}}));
    List<String> explain;
    String query = "update " + Table.ACIDTBL + " set b = 5 where a = 3";
    if (enablePPD) {
      explain = runStatementOnDriver("explain " + query);
      /*
      here is a portion of the above "explain".  The "filterExpr:" in the TableScan is the pushed predicate
      w/o PPD, the line is simply not there, otherwise the plan is the same
       Map Operator Tree:,
         TableScan,
          alias: acidtbl,
          filterExpr: (a = 3) (type: boolean),
            Filter Operator,
             predicate: (a = 3) (type: boolean),
             Select Operator,
             ...
       */
      assertPredicateIsPushed("filterExpr: (a = 3)", explain);
    }
    //create delta_0002_0002_0000 (can't push predicate)
    runStatementOnDriver(query);
    query = "select a,b from " + Table.ACIDTBL + " where b = 4 order by a,b";
    if (enablePPD) {
      /*at this point we have 2 delta files, 1 for insert 1 for update
      * we should push predicate into 1st one but not 2nd.  If the following 'select' were to
      * push into the 'update' delta, we'd filter out {3,5} before doing merge and thus
     * produce {3,4} as the value for 2nd row.  The right result is 0-rows.*/
      explain = runStatementOnDriver("explain " + query);
      assertPredicateIsPushed("filterExpr: (b = 4)", explain);
    }
    List<String> rs0 = runStatementOnDriver(query);
    Assert.assertEquals("Read failed", 0, rs0.size());
    runStatementOnDriver("alter table " + Table.ACIDTBL + " compact 'MAJOR'");
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setHiveConf(hiveConf);
    AtomicBoolean stop = new AtomicBoolean();
    AtomicBoolean looped = new AtomicBoolean();
    stop.set(true);
    t.init(stop, looped);
    t.run();
    //now we have base_0001 file
    int[][] tableData2 = {{1, 7}, {5, 6}, {7, 8}, {9, 10}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData2));
    //now we have delta_0003_0003_0000 with inserts only (ok to push predicate)
    if (enablePPD) {
      explain = runStatementOnDriver("explain delete from " + Table.ACIDTBL + " where a=7 and b=8");
      assertPredicateIsPushed("filterExpr: ((a = 7) and (b = 8))", explain);
    }
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where a=7 and b=8");
    //now we have delta_0004_0004_0000 with delete events

    /*(can't push predicate to 'delete' delta)
    * if we were to push to 'delete' delta, we'd filter out all rows since the 'row' is always NULL for
    * delete events and we'd produce data as if the delete never happened*/
    query = "select a,b from " + Table.ACIDTBL + " where a > 1 order by a,b";
    if(enablePPD) {
      explain = runStatementOnDriver("explain " + query);
      assertPredicateIsPushed("filterExpr: (a > 1)", explain);
    }
    List<String> rs1 = runStatementOnDriver(query);
    int [][] resultData = new int[][] {{3, 5}, {5, 6}, {9, 10}};
    Assert.assertEquals("Update failed", stringifyValues(resultData), rs1);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVEOPTINDEXFILTER, originalPpd);
  }
  private static void assertPredicateIsPushed(String ppd, List<String> queryPlan) {
    for(String line : queryPlan) {
      if(line != null && line.contains(ppd)) {
        return;
      }
    }
    Assert.assertFalse("PPD '" + ppd + "' wasn't pushed", true);
  }
  @Ignore("alter table")
  @Test
  public void testAlterTable() throws Exception {
    int[][] tableData = {{1,2}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData));
    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MAJOR'");
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setHiveConf(hiveConf);
    AtomicBoolean stop = new AtomicBoolean();
    AtomicBoolean looped = new AtomicBoolean();
    stop.set(true);
    t.init(stop, looped);
    t.run();
    int[][] tableData2 = {{5,6}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData2));
    List<String> rs1 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " where b > 0 order by a,b");

    runStatementOnDriver("alter table " + Table.ACIDTBL + " add columns(c int)");
    int[][] moreTableData = {{7,8,9}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b,c) " + makeValuesClause(moreTableData));
    List<String> rs0 = runStatementOnDriver("select a,b,c from " + Table.ACIDTBL + " where a > 0 order by a,b,c");
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
   * Test the query correctness and directory layout after ACID table conversion and MAJOR compaction
   * @throws Exception
   */
  @Test
  public void testNonAcidToAcidConversionAndMajorCompaction() throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;

    // 1. Insert a row to Non-ACID table
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,2)");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // There should be 2 original bucket files in the location (000000_0 and 000001_0)
    Assert.assertEquals(BUCKET_COUNT, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
    }
    List<String> rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    int [][] resultData = new int[][] {{1, 2}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    int resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 2. Convert NONACIDORCTBL to ACID table
    runStatementOnDriver("alter table " + Table.NONACIDORCTBL + " SET TBLPROPERTIES ('transactional'='true')");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // Everything should be same as before
    Assert.assertEquals(BUCKET_COUNT, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
    }
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 2}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 3. Insert another row to newly-converted ACID table
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(3,4)");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // There should be 2 original bucket files (000000_0 and 000001_0), plus a new delta directory.
    // The delta directory should also have 2 bucket files (bucket_00000 and bucket_00001)
    Assert.assertEquals(3, status.length);
    boolean sawNewDelta = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("delta_.*")) {
        sawNewDelta = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.STAGING_DIR_PATH_FILTER);
        Assert.assertEquals(BUCKET_COUNT, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
      } else {
        Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
      }
    }
    Assert.assertTrue(sawNewDelta);
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 2}, {3, 4}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 4. Perform a major compaction
    runStatementOnDriver("alter table "+ Table.NONACIDORCTBL + " compact 'MAJOR'");
    Worker w = new Worker();
    w.setThreadId((int) w.getId());
    w.setHiveConf(hiveConf);
    AtomicBoolean stop = new AtomicBoolean();
    AtomicBoolean looped = new AtomicBoolean();
    stop.set(true);
    w.init(stop, looped);
    w.run();
    // There should be 1 new directory: base_xxxxxxx.
    // Original bucket files and delta directory should stay until Cleaner kicks in.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    Assert.assertEquals(4, status.length);
    boolean sawNewBase = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("base_.*")) {
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.STAGING_DIR_PATH_FILTER);
        Assert.assertEquals(BUCKET_COUNT, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
      }
    }
    Assert.assertTrue(sawNewBase);
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 2}, {3, 4}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 5. Let Cleaner delete obsolete files/dirs
    // Note, here we create a fake directory along with fake files as original directories/files
    String fakeFile0 = TEST_WAREHOUSE_DIR + "/" + (Table.NONACIDORCTBL).toString().toLowerCase() +
        "/subdir/000000_0";
    String fakeFile1 = TEST_WAREHOUSE_DIR + "/" + (Table.NONACIDORCTBL).toString().toLowerCase() +
        "/subdir/000000_1";
    fs.create(new Path(fakeFile0));
    fs.create(new Path(fakeFile1));
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // Before Cleaner, there should be 5 items:
    // 2 original files, 1 original directory, 1 base directory and 1 delta directory
    Assert.assertEquals(5, status.length);
    Cleaner c = new Cleaner();
    c.setThreadId((int) c.getId());
    c.setHiveConf(hiveConf);
    stop = new AtomicBoolean();
    looped = new AtomicBoolean();
    stop.set(true);
    c.init(stop, looped);
    c.run();
    // There should be only 1 directory left: base_xxxxxxx.
    // Original bucket files and delta directory should have been cleaned up.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    Assert.assertEquals(1, status.length);
    Assert.assertTrue(status[0].getPath().getName().matches("base_.*"));
    FileStatus[] buckets = fs.listStatus(status[0].getPath(), FileUtils.STAGING_DIR_PATH_FILTER);
    Assert.assertEquals(BUCKET_COUNT, buckets.length);
    Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
    Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 2}, {3, 4}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));
  }

  @Test
  public void testUpdateMixedCase() throws Exception {
    int[][] tableData = {{1,2},{3,3},{5,3}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData));
    runStatementOnDriver("update " + Table.ACIDTBL + " set B = 7 where A=1");
    List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] updatedData = {{1,7},{3,3},{5,3}};
    Assert.assertEquals("Update failed", stringifyValues(updatedData), rs);
    runStatementOnDriver("update " + Table.ACIDTBL + " set B = B + 1 where A=1");
    List<String> rs2 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] updatedData2 = {{1,8},{3,3},{5,3}};
    Assert.assertEquals("Update failed", stringifyValues(updatedData2), rs2);
  }
  @Test
  public void testDeleteIn() throws Exception {
    int[][] tableData = {{1,2},{3,2},{5,2},{1,3},{3,3},{5,3}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData));
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,7),(3,7)");
    //todo: once multistatement txns are supported, add a test to run next 2 statements in a single txn
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where a in(select a from " + Table.NONACIDORCTBL + ")");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) select a,b from " + Table.NONACIDORCTBL);
    List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] updatedData = {{1,7},{3,7},{5,2},{5,3}};
    Assert.assertEquals("Bulk update failed", stringifyValues(updatedData), rs);
    runStatementOnDriver("update " + Table.ACIDTBL + " set b=19 where b in(select b from " + Table.NONACIDORCTBL + " where a = 3)");
    List<String> rs2 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] updatedData2 = {{1,19},{3,19},{5,2},{5,3}};
    Assert.assertEquals("Bulk update2 failed", stringifyValues(updatedData2), rs2);
  }

  /**
   * https://issues.apache.org/jira/browse/HIVE-10151
   */
  @Test
  public void testBucketizedInputFormat() throws Exception {
    int[][] tableData = {{1,2}};
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p=1) (a,b) " + makeValuesClause(tableData));

    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) select a,b from " + Table.ACIDTBLPART + " where p = 1");
    List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL);//no order by as it's just 1 row
    Assert.assertEquals("Insert into " + Table.ACIDTBL + " didn't match:", stringifyValues(tableData), rs);

    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) select a,b from " + Table.ACIDTBLPART + " where p = 1");
    List<String> rs2 = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);//no order by as it's just 1 row
    Assert.assertEquals("Insert into " + Table.NONACIDORCTBL + " didn't match:", stringifyValues(tableData), rs2);
  }
  @Test
  public void testInsertOverwriteWithSelfJoin() throws Exception {
    int[][] part1Data = {{1,7}};
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) " + makeValuesClause(part1Data));
    //this works because logically we need S lock on NONACIDORCTBL to read and X lock to write, but
    //LockRequestBuilder dedups locks on the same entity to only keep the highest level lock requested
    runStatementOnDriver("insert overwrite table " + Table.NONACIDORCTBL + " select 2, 9 from " + Table.NONACIDORCTBL + " T inner join " + Table.NONACIDORCTBL + " S on T.a=S.a");
    List<String> rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL + " order by a,b");
    int[][] joinData = {{2,9}};
    Assert.assertEquals("Self join non-part insert overwrite failed", stringifyValues(joinData), rs);
    int[][] part2Data = {{1,8}};
    runStatementOnDriver("insert into " + Table.NONACIDPART + " partition(p=1) (a,b) " + makeValuesClause(part1Data));
    runStatementOnDriver("insert into " + Table.NONACIDPART + " partition(p=2) (a,b) " + makeValuesClause(part2Data));
    //here we need X lock on p=1 partition to write and S lock on 'table' to read which should
    //not block each other since they are part of the same txn
    runStatementOnDriver("insert overwrite table " + Table.NONACIDPART + " partition(p=1) select a,b from " + Table.NONACIDPART);
    List<String> rs2 = runStatementOnDriver("select a,b from " + Table.NONACIDPART + " order by a,b");
    int[][] updatedData = {{1,7},{1,8},{1,8}};
    Assert.assertEquals("Insert overwrite partition failed", stringifyValues(updatedData), rs2);
    //insert overwrite not supported for ACID tables
  }
  /**
   * HIVE-12353
   * @throws Exception
   */
  @Test
  public void testInitiatorWithMultipleFailedCompactions() throws Exception {
    String tblName = "hive12353";
    runStatementOnDriver("drop table if exists " + tblName);
    runStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true')");
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 4);
    for(int i = 0; i < 5; i++) {
      //generate enough delta files so that Initiator can trigger auto compaction
      runStatementOnDriver("insert into " + tblName + " values(" + (i + 1) + ", 'foo'),(" + (i + 2) + ", 'bar'),(" + (i + 3) + ", 'baz')");
    }
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEFAILCOMPACTION, true);

    int numFailedCompactions = hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD);
    CompactionTxnHandler txnHandler = new CompactionTxnHandler(hiveConf);
    AtomicBoolean stop = new AtomicBoolean(true);
    //create failed compactions
    for(int i = 0; i < numFailedCompactions; i++) {
      //each of these should fail
      txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MINOR));
      runWorker(hiveConf);
    }
    //this should not schedule a new compaction due to prior failures
    Initiator init = new Initiator();
    init.setThreadId((int)init.getId());
    init.setHiveConf(hiveConf);
    init.init(stop, new AtomicBoolean());
    init.run();

    CompactionsByState cbs = countCompacts(txnHandler);
    Assert.assertEquals("Unexpected number of failed compactions", numFailedCompactions, cbs.failed);
    Assert.assertEquals("Unexpected total number of compactions", numFailedCompactions, cbs.total);
    
    hiveConf.setTimeVar(HiveConf.ConfVars.COMPACTOR_HISTORY_REAPER_INTERVAL, 10, TimeUnit.MILLISECONDS);
    AcidCompactionHistoryService compactionHistoryService = new AcidCompactionHistoryService();
    runHouseKeeperService(compactionHistoryService, hiveConf);//should not remove anything from history
    cbs = countCompacts(txnHandler);
    Assert.assertEquals("Number of failed compactions after History clean", numFailedCompactions, cbs.failed);
    Assert.assertEquals("Total number of compactions after History clean", numFailedCompactions, cbs.total);

    txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MAJOR));
    runWorker(hiveConf);//will fail
    txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MINOR));
    runWorker(hiveConf);//will fail
    cbs = countCompacts(txnHandler);
    Assert.assertEquals("Unexpected num failed1", numFailedCompactions + 2, cbs.failed);
    Assert.assertEquals("Unexpected num total1", numFailedCompactions + 2, cbs.total);
    runHouseKeeperService(compactionHistoryService, hiveConf);//should remove history so that we have
    //COMPACTOR_HISTORY_RETENTION_FAILED failed compacts left (and no other since we only have failed ones here)
    cbs = countCompacts(txnHandler);
    Assert.assertEquals("Unexpected num failed2", hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED), cbs.failed);
    Assert.assertEquals("Unexpected num total2", hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED), cbs.total);
    
    
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEFAILCOMPACTION, false);
    txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MINOR));
    //at this point "show compactions" should have (COMPACTOR_HISTORY_RETENTION_FAILED) failed + 1 initiated
    cbs = countCompacts(txnHandler);
    Assert.assertEquals("Unexpected num failed3", hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED), cbs.failed);
    Assert.assertEquals("Unexpected num initiated", 1, cbs.initiated);
    Assert.assertEquals("Unexpected num total3", hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED) + 1, cbs.total);

    runWorker(hiveConf);//will succeed and transition to Initiated->Working->Ready for Cleaning
    cbs = countCompacts(txnHandler);
    Assert.assertEquals("Unexpected num failed4", hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED), cbs.failed);
    Assert.assertEquals("Unexpected num ready to clean", 1, cbs.readyToClean);
    Assert.assertEquals("Unexpected num total4", hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED) + 1, cbs.total);
    
    runCleaner(hiveConf); // transition to Success state
    runHouseKeeperService(compactionHistoryService, hiveConf);//should not purge anything as all items within retention sizes
    cbs = countCompacts(txnHandler);
    Assert.assertEquals("Unexpected num failed5", hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED), cbs.failed);
    Assert.assertEquals("Unexpected num succeeded", 1, cbs.succeeded);
    Assert.assertEquals("Unexpected num total5", hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED) + 1, cbs.total);
  }
  private static class CompactionsByState {
    private int attempted;
    private int failed;
    private int initiated;
    private int readyToClean;
    private int succeeded;
    private int working;
    private int total;
  }
  private static CompactionsByState countCompacts(TxnHandler txnHandler) throws MetaException {
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    CompactionsByState compactionsByState = new CompactionsByState();
    compactionsByState.total = resp.getCompactsSize();
    for(ShowCompactResponseElement compact : resp.getCompacts()) {
      if(TxnHandler.FAILED_RESPONSE.equals(compact.getState())) {
        compactionsByState.failed++;
      }
      else if(TxnHandler.CLEANING_RESPONSE.equals(compact.getState())) {
        compactionsByState.readyToClean++;
      }
      else if(TxnHandler.INITIATED_RESPONSE.equals(compact.getState())) {
        compactionsByState.initiated++;
      }
      else if(TxnHandler.SUCCEEDED_RESPONSE.equals(compact.getState())) {
        compactionsByState.succeeded++;
      }
      else if(TxnHandler.WORKING_RESPONSE.equals(compact.getState())) {
        compactionsByState.working++;
      }
      else if(TxnHandler.ATTEMPTED_RESPONSE.equals(compact.getState())) {
        compactionsByState.attempted++;
      }
    }
    return compactionsByState;
  }
  private static void runWorker(HiveConf hiveConf) throws MetaException {
    AtomicBoolean stop = new AtomicBoolean(true);
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setHiveConf(hiveConf);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();
  }
  private static void runCleaner(HiveConf hiveConf) throws MetaException {
    AtomicBoolean stop = new AtomicBoolean(true);
    Cleaner t = new Cleaner();
    t.setThreadId((int) t.getId());
    t.setHiveConf(hiveConf);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();
  }

  private static void runHouseKeeperService(HouseKeeperService houseKeeperService, HiveConf conf) throws Exception {
    int lastCount = houseKeeperService.getIsAliveCounter();
    houseKeeperService.start(conf);
    while(houseKeeperService.getIsAliveCounter() <= lastCount) {
      try {
        Thread.sleep(100);//make sure it has run at least once
      }
      catch(InterruptedException ex) {
        //...
      }
    }
    houseKeeperService.stop();
  }

  /**
   * HIVE-12352 has details
   * @throws Exception
   */
  @Test
  public void writeBetweenWorkerAndCleaner() throws Exception {
    String tblName = "hive12352";
    runStatementOnDriver("drop table if exists " + tblName);
    runStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true')");

    //create some data
    runStatementOnDriver("insert into " + tblName + " values(1, 'foo'),(2, 'bar'),(3, 'baz')");
    runStatementOnDriver("update " + tblName + " set b = 'blah' where a = 3");

    //run Worker to execute compaction
    CompactionTxnHandler txnHandler = new CompactionTxnHandler(hiveConf);
    txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MINOR));
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setHiveConf(hiveConf);
    AtomicBoolean stop = new AtomicBoolean(true);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();

    //delete something, but make sure txn is rolled back
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEROLLBACKTXN, true);
    runStatementOnDriver("delete from " + tblName + " where a = 1");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEROLLBACKTXN, false);

    List<String> expected = new ArrayList<>();
    expected.add("1\tfoo");
    expected.add("2\tbar");
    expected.add("3\tblah");
    Assert.assertEquals("", expected,
      runStatementOnDriver("select a,b from " + tblName + " order by a"));

    //run Cleaner
    Cleaner c = new Cleaner();
    c.setThreadId((int)c.getId());
    c.setHiveConf(hiveConf);
    c.init(stop, new AtomicBoolean());
    c.run();

    //this seems odd, but we wan to make sure that to run CompactionTxnHandler.cleanEmptyAbortedTxns()
    Initiator i = new Initiator();
    i.setThreadId((int)i.getId());
    i.setHiveConf(hiveConf);
    i.init(stop, new AtomicBoolean());
    i.run();

    //check that aborted operation didn't become committed
    Assert.assertEquals("", expected,
      runStatementOnDriver("select a,b from " + tblName + " order by a"));
  }
  /**
   * takes raw data and turns it into a string as if from Driver.getResults()
   * sorts rows in dictionary order
   */
  private List<String> stringifyValues(int[][] rowsIn) {
    assert rowsIn.length > 0;
    int[][] rows = rowsIn.clone();
    Arrays.sort(rows, new RowComp());
    List<String> rs = new ArrayList<String>();
    for(int[] row : rows) {
      assert row.length > 0;
      StringBuilder sb = new StringBuilder();
      for(int value : row) {
        sb.append(value).append("\t");
      }
      sb.setLength(sb.length() - 1);
      rs.add(sb.toString());
    }
    return rs;
  }
  private static final class RowComp implements Comparator<int[]> {
    @Override
    public int compare(int[] row1, int[] row2) {
      assert row1 != null && row2 != null && row1.length == row2.length;
      for(int i = 0; i < row1.length; i++) {
        int comp = Integer.compare(row1[i], row2[i]);
        if(comp != 0) {
          return comp;
        }
      }
      return 0;
    }
  }
  private String makeValuesClause(int[][] rows) {
    assert rows.length > 0;
    StringBuilder sb = new StringBuilder("values");
    for(int[] row : rows) {
      assert row.length > 0;
      if(row.length > 1) {
        sb.append("(");
      }
      for(int value : row) {
        sb.append(value).append(",");
      }
      sb.setLength(sb.length() - 1);//remove trailing comma
      if(row.length > 1) {
        sb.append(")");
      }
      sb.append(",");
    }
    sb.setLength(sb.length() - 1);//remove trailing comma
    return sb.toString();
  }

  private List<String> runStatementOnDriver(String stmt) throws Exception {
    CommandProcessorResponse cpr = d.run(stmt);
    if(cpr.getResponseCode() != 0) {
      throw new RuntimeException(stmt + " failed: " + cpr);
    }
    List<String> rs = new ArrayList<String>();
    d.getResults(rs);
    return rs;
  }
}
