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
package org.apache.hadoop.hive.ql;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HMSMetricsListener;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.io.orc.TestVectorizedOrcAcidRowBatchReader;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.TxnManagerFactory;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorFactory;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorPipeline;
import org.apache.hadoop.hive.ql.txn.compactor.MRCompactor;
import org.apache.hadoop.hive.ql.txn.compactor.Worker;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.ql.lockmgr.TestDbTxnManager2.swapTxnManager;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class TestTxnCommands3 extends TxnCommandsBaseForTests {
  static final private Logger LOG = LoggerFactory.getLogger(TestTxnCommands3.class);
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
      File.separator + TestTxnCommands3.class.getCanonicalName()
      + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
  @Override
  protected String getTestDataDir() {
    return TEST_DATA_DIR;
  }

  @Test
  public void testRenameTable() throws Exception {
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID, true);
    hiveConf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, false);

    runStatementOnDriver("drop database if exists mydb1 cascade");
    runStatementOnDriver("drop database if exists mydb2 cascade");
    runStatementOnDriver("create database mydb1");
    runStatementOnDriver("create database mydb2");
    runStatementOnDriver("create table mydb1.T(a int, b int) stored as orc");
    runStatementOnDriver("insert into mydb1.T values(1,2),(4,5)");
    //put something in WRITE_SET
    runStatementOnDriver("update mydb1.T set b = 6 where b = 5");
    runStatementOnDriver("alter table mydb1.T compact 'minor'");

    runStatementOnDriver("alter table mydb1.T RENAME TO mydb1.S");

    String testQuery = "select ROW__ID, a, b, INPUT__FILE__NAME from mydb1.S";
    String[][] expected = new String[][] {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2",
            "s/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":2,\"bucketid\":536870913,\"rowid\":0}\t4\t6",
            "s/delta_0000002_0000002_0001/bucket_00000_0"}};
    checkResult(expected, testQuery, false, "check data", LOG);


    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from COMPLETED_TXN_COMPONENTS where CTC_TABLE='t'"));
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from COMPACTION_QUEUE where CQ_TABLE='t'"));
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from WRITE_SET where WS_TABLE='t'"));
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from TXN_TO_WRITE_ID where T2W_TABLE='t'"));
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from NEXT_WRITE_ID where NWI_TABLE='t'"));

    Assert.assertEquals(
        TestTxnDbUtil.queryToString(hiveConf, "select * from COMPLETED_TXN_COMPONENTS"), 2,
        TestTxnDbUtil.countQueryAgent(hiveConf,
            "select count(*) from COMPLETED_TXN_COMPONENTS where CTC_TABLE='s'"));
    Assert.assertEquals(1, TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from COMPACTION_QUEUE where CQ_TABLE='s'"));
    Assert.assertEquals(1, TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from WRITE_SET where WS_TABLE='s'"));
    Assert.assertEquals(3, TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from TXN_TO_WRITE_ID where T2W_TABLE='s'"));
    Assert.assertEquals(1, TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from NEXT_WRITE_ID where NWI_TABLE='s'"));

    runStatementOnDriver("alter table mydb1.S RENAME TO mydb2.bar");

    Assert.assertEquals(
        TestTxnDbUtil.queryToString(hiveConf, "select * from COMPLETED_TXN_COMPONENTS"), 2,
        TestTxnDbUtil.countQueryAgent(hiveConf,
            "select count(*) from COMPLETED_TXN_COMPONENTS where CTC_TABLE='bar'"));
    Assert.assertEquals(1, TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from COMPACTION_QUEUE where CQ_TABLE='bar'"));
    Assert.assertEquals(1, TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from WRITE_SET where WS_TABLE='bar'"));
    Assert.assertEquals(4, TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from TXN_TO_WRITE_ID where T2W_TABLE='bar'"));
    Assert.assertEquals(1, TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from NEXT_WRITE_ID where NWI_TABLE='bar'"));
  }

  @Test
  public void testDeleteEventPruningOn() throws Exception {
    HiveConf.setBoolVar(hiveConf,
        HiveConf.ConfVars.FILTER_DELETE_EVENTS, true);
    testDeleteEventPruning();
  }
  @Test
  public void testDeleteEventPruningOff() throws Exception {
    HiveConf.setBoolVar(hiveConf,
        HiveConf.ConfVars.FILTER_DELETE_EVENTS, false);
    testDeleteEventPruning();
  }
  /**
   * run with and w/o event fitlering enabled - should get the same results
   * {@link TestVectorizedOrcAcidRowBatchReader#testDeleteEventFiltering()}
   *
   * todo: add .q test using VerifyNumReducersHook.num.reducers to make sure
   * it does have 1 split for each input file.
   * Will need to crate VerifyNumMappersHook
   *
   * Also, consider
   * HiveSplitGenerator.java
   * RAW_INPUT_SPLITS and GROUPED_INPUT_SPLITS are the counters before and
   * after grouping splits PostExecTezSummaryPrinter post exec hook can be
   * used to printout specific counters
   */
  private void testDeleteEventPruning() throws Exception {
    HiveConf.setBoolVar(hiveConf,
        HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    dropTable(new String[] {"T"});
    runStatementOnDriver(
        "create transactional table T(a int, b int) stored as orc");
    runStatementOnDriver("insert into T values(1,2),(4,5)");
    runStatementOnDriver("insert into T values(4,6),(1,3)");
    runStatementOnDriver("delete from T where a = 1");
    List<String> rs = runStatementOnDriver(
        "select ROW__ID, a, b from T order by a, b");

    boolean isVectorized =
        hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED);
    String testQuery = isVectorized ?
        "select ROW__ID, a, b from T order by a, b" :
        "select ROW__ID, a, b, INPUT__FILE__NAME from T order by a, b";
    String[][] expected = new String[][]{
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t4\t5",
            "warehouse/t/delta_0000001_0000001_0000/bucket_00000"},
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}\t4\t6",
            "warehouse/t/delta_0000002_0000002_0000/bucket_00000"}};
    checkResult(expected, testQuery, isVectorized, "after delete", LOG);

    runStatementOnDriver("alter table T compact 'MAJOR'");
    runWorker(hiveConf);
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history",
        1, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0 compaction state",
        TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(0).getState());
    Assert.assertTrue(resp.getCompacts().get(0).getHadoopJobId()
        .startsWith("job_local"));

    String[][] expected2 = new String[][]{
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t4\t5",
            "warehouse/t/base_0000001/bucket_00000"},
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}\t4\t6",
            "warehouse/t/base_0000002/bucket_00000"}};
    checkResult(expected2, testQuery, isVectorized, "after compaction", LOG);
  }
  /**
   * HIVE-19985
   */
  @Test
  public void testAcidMetaColumsDecode() throws Exception {
    //this only applies in vectorized mode
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    hiveConf.set(MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID.getVarname(), "true");
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T (a int, b int) stored as orc");
    int[][] data1 = {{1, 2}, {3, 4}};
    runStatementOnDriver("insert into T" + makeValuesClause(data1));
    int[][] data2 = {{5, 6}, {7, 8}};
    runStatementOnDriver("insert into T" + makeValuesClause(data2));
    int[][] dataAll = {{1, 2}, {3, 4}, {5, 6}, {7, 8}};


    hiveConf.setBoolVar(HiveConf.ConfVars.OPTIMIZE_ACID_META_COLUMNS, true);
    List<String> rs = runStatementOnDriver("select a, b from T order by a, b");
    Assert.assertEquals(stringifyValues(dataAll), rs);

    hiveConf.setBoolVar(HiveConf.ConfVars.OPTIMIZE_ACID_META_COLUMNS, false);
    rs = runStatementOnDriver("select a, b from T order by a, b");
    Assert.assertEquals(stringifyValues(dataAll), rs);

    runStatementOnDriver("alter table T compact 'major'");
    runWorker(hiveConf);

    //check status of compaction job
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history",
        1, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0 compaction state",
        TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(0).getState());
    Assert.assertTrue(
        resp.getCompacts().get(0).getHadoopJobId().startsWith("job_local"));

    hiveConf.setBoolVar(HiveConf.ConfVars.OPTIMIZE_ACID_META_COLUMNS, true);
    rs = runStatementOnDriver("select a, b from T order by a, b");
    Assert.assertEquals(stringifyValues(dataAll), rs);

    hiveConf.setBoolVar(HiveConf.ConfVars.OPTIMIZE_ACID_META_COLUMNS, false);
    rs = runStatementOnDriver("select a, b from T order by a, b");
    Assert.assertEquals(stringifyValues(dataAll), rs);
  }

  /**
   * Test that rows are routed to proper files based on bucket col/ROW__ID
   * Only the Vectorized Acid Reader checks if bucketId in ROW__ID inside the file
   * matches the file name and only for files in delete_delta
   */
  @Test
  public void testSdpoBucketed() throws Exception {
    testSdpoBucketed(true, true, 1);
    testSdpoBucketed(true, false, 1);
    testSdpoBucketed(false, true, 1);
    testSdpoBucketed(false, false,1);

    testSdpoBucketed(true, true, 2);
    testSdpoBucketed(true, false, 2);
    testSdpoBucketed(false, true, 2);
    testSdpoBucketed(false, false,2);
  }
  private void testSdpoBucketed(boolean isVectorized, boolean isSdpo, int bucketing_version)
      throws Exception {
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, isVectorized);
    runStatementOnDriver("drop table if exists acid_uap");
    runStatementOnDriver("create transactional table acid_uap(a int, b varchar(128)) " +
        "partitioned by (ds string) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES " +
        "('bucketing_version'='" + bucketing_version + "')");
    runStatementOnDriver("insert into table acid_uap partition (ds='tomorrow') " +
        "values (1, 'bah'),(2, 'yah')");
    runStatementOnDriver("insert into table acid_uap partition (ds='today') " +
        "values (1, 'bah'),(2, 'yah')");
    runStatementOnDriver("select a,b, ds from acid_uap order by a,b, ds");

    String testQuery = isVectorized ?
        "select ROW__ID, a, b, ds from acid_uap order by ds, a, b" :
        "select ROW__ID, a, b, ds, INPUT__FILE__NAME from acid_uap order by ds, a, b";
    String[][] expected = new String[][]{
        {"{\"writeid\":2,\"bucketid\":536936448,\"rowid\":0}\t1\tbah\ttoday",
            "warehouse/acid_uap/ds=today/delta_0000002_0000002_0000/bucket_00001_0"},
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}\t2\tyah\ttoday",
            "warehouse/acid_uap/ds=today/delta_0000002_0000002_0000/bucket_00000_0"},

        {"{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t1\tbah\ttomorrow",
            "warehouse/acid_uap/ds=tomorrow/delta_0000001_0000001_0000/bucket_00001_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t2\tyah\ttomorrow",
            "warehouse/acid_uap/ds=tomorrow/delta_0000001_0000001_0000/bucket_00000_0"}};
    checkResult(expected, testQuery, isVectorized, "after insert", LOG);

    runStatementOnDriver("update acid_uap set b = 'fred'");

    String[][] expected2 = new String[][]{
        {"{\"writeid\":3,\"bucketid\":536936449,\"rowid\":0}\t1\tfred\ttoday",
            "warehouse/acid_uap/ds=today/delta_0000003_0000003_0001/bucket_00001_0"},
        {"{\"writeid\":3,\"bucketid\":536870913,\"rowid\":0}\t2\tfred\ttoday",
            "warehouse/acid_uap/ds=today/delta_0000003_0000003_0001/bucket_00000_0"},

        {"{\"writeid\":3,\"bucketid\":536936449,\"rowid\":0}\t1\tfred\ttomorrow",
            "warehouse/acid_uap/ds=tomorrow/delta_0000003_0000003_0001/bucket_00001_0"},
        {"{\"writeid\":3,\"bucketid\":536870913,\"rowid\":0}\t2\tfred\ttomorrow",
            "warehouse/acid_uap/ds=tomorrow/delta_0000003_0000003_0001/bucket_00000_0"}};
    checkResult(expected2, testQuery, isVectorized, "after update", LOG);
  }
  @Test
  public void testCleaner2() throws Exception {
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID, true);
    dropTable(new String[] {"T"});
    //note: transaction names T1, T2, etc below, are logical, the actual txnid will be different
    runStatementOnDriver("create table T (a int, b int) stored as orc");
    runStatementOnDriver("insert into T values(0,2)");//makes delta_1_1 in T1
    runStatementOnDriver("insert into T values(1,4)");//makes delta_2_2 in T2

    Driver driver2 = new Driver(new QueryState.Builder().withHiveConf(hiveConf).build());
    driver2.setMaxRows(10000);

    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(hiveConf);
    HiveTxnManager txnMgr1 = swapTxnManager(txnMgr2);
    Driver driver1 = swapDrivers(driver2);
    runStatementOnDriver("start transaction");//T3
         /* this select sees
     target/warehouse/t/
     ├── delta_0000001_0000001_0000
     │   ├── _orc_acid_version
     │   └── bucket_00000
     └── delta_0000002_0000002_0000
         ├── _orc_acid_version
         └── bucket_00000*/
    String testQuery = "select ROW__ID, a, b, INPUT__FILE__NAME from T";
    String[][] expected = new String[][] {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t0\t2",
            "t/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}\t1\t4",
            "t/delta_0000002_0000002_0000/bucket_00000_0"}};
    checkResult(expected, testQuery, false, "check data", LOG);


    txnMgr2 = swapTxnManager(txnMgr1);
    driver2 = swapDrivers(driver1);
    runStatementOnDriver("alter table T compact 'minor'");//T4
    runWorker(hiveConf);//makes delta_1_2
         /* Now we should have
     target/warehouse/t/
     ├── delta_0000001_0000001_0000
     │   ├── _orc_acid_version
     │   └── bucket_00000
     ├── delta_0000001_0000002_v0000020
     │   ├── _orc_acid_version
     │   └── bucket_00000
     └── delta_0000002_0000002_0000
         ├── _orc_acid_version
         └── bucket_00000*/
    FileSystem fs = FileSystem.get(hiveConf);
    Path warehousePath = new Path(getWarehouseDir());
    FileStatus[] actualList = fs.listStatus(new Path(warehousePath + "/t"),
        FileUtils.HIDDEN_FILES_PATH_FILTER);

    String[] expectedList = new String[] {
        "/t/delta_0000001_0000002_v0000020",
        "/t/delta_0000001_0000001_0000",
        "/t/delta_0000002_0000002_0000",
    };
    checkExpectedFiles(actualList, expectedList, warehousePath.toString());


    /*
    T3 is still running and cannot see anything compactor produces with v0000019 suffix
    so it may be reading delta_1_1 & delta_2_2 and so cleaner cannot delete any files
     at this point*/
    runCleaner(hiveConf);
    actualList = fs.listStatus(new Path(warehousePath + "/t"),
        FileUtils.HIDDEN_FILES_PATH_FILTER);
    checkExpectedFiles(actualList, expectedList, warehousePath.toString());

    txnMgr1 = swapTxnManager(txnMgr2);
    driver1 = swapDrivers(driver2);
    runStatementOnDriver("commit");//commits T3
    //so now cleaner should be able to delete delta_0000002_0000002_0000

    //insert a row so that compactor makes a new delta (due to HIVE-20901)
    runStatementOnDriver("insert into T values(2,5)");//makes delta_3_3 in T5

    runStatementOnDriver("alter table T compact 'minor'");
    runWorker(hiveConf);
    /*
    at this point delta_0000001_0000003_v0000023 is visible to everyone
    so cleaner removes all files shadowed by it (which is everything in this case)
    */
    runCleaner(hiveConf);
    runCleaner(hiveConf);

    expectedList = new String[] {
        "/t/delta_0000001_0000003_v0000023"
    };
    actualList = fs.listStatus(new Path(warehousePath + "/t"),
        FileUtils.HIDDEN_FILES_PATH_FILTER);
    checkExpectedFiles(actualList, expectedList, warehousePath.toString());
  }
  private static void checkExpectedFiles(FileStatus[] actualList, String[] expectedList, String filePrefix) throws Exception {
    Set<String> expectedSet = new HashSet<>();
    Set<String> unexpectedSet = new HashSet<>();
    for(String f : expectedList) {
      expectedSet.add(f);
    }
    for(FileStatus fs : actualList) {
      String endOfPath = fs.getPath().toString().substring(fs.getPath().toString().indexOf(filePrefix) + filePrefix.length());
      if(!expectedSet.remove(endOfPath)) {
        unexpectedSet.add(endOfPath);
      }
    }
    Assert.assertTrue("not found set: " + expectedSet + " unexpected set: " + unexpectedSet, expectedSet.isEmpty() && unexpectedSet.isEmpty());
  }
  @Test
  public void testCompactionAbort() throws Exception {
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID, true);
    dropTable(new String[] {"T"});
    //note: transaction names T1, T2, etc below, are logical, the actual txnid will be different
    runStatementOnDriver("create table T (a int, b int) stored as orc");
    runStatementOnDriver("insert into T values(0,2)");//makes delta_1_1 in T1
    runStatementOnDriver("insert into T values(1,4)");//makes delta_2_2 in T2

    //create failed compaction attempt so that compactor txn is aborted
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_COMPACTION, true);
    runStatementOnDriver("alter table T compact 'minor'");
    runWorker(hiveConf);

    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history",
        1, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0th compaction state",
        TxnStore.FAILED_RESPONSE, resp.getCompacts().get(0).getState());
    GetOpenTxnsResponse openResp =  txnHandler.getOpenTxns();
    Assert.assertEquals(openResp.toString(), 1, openResp.getOpen_txnsSize());
    //check that the compactor txn is aborted
    Assert.assertTrue(openResp.toString(), BitSet.valueOf(openResp.getAbortedBits()).get(0));

    runCleaner(hiveConf);
    //we still have 1 aborted (compactor) txn
    Assert.assertTrue(openResp.toString(), BitSet.valueOf(openResp.getAbortedBits()).get(0));
    Assert.assertEquals(1, TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from TXN_COMPONENTS"));
    //this returns 1 row since we only have 1 compaction executed
    int highestCompactWriteId = TestTxnDbUtil.countQueryAgent(hiveConf,
        "select CC_HIGHEST_WRITE_ID from COMPLETED_COMPACTIONS");
    /**
     * See {@link org.apache.hadoop.hive.metastore.txn.CompactionTxnHandler#updateCompactorState(CompactionInfo, long)}
     * for notes on why CC_HIGHEST_WRITE_ID=TC_WRITEID
     */
    Assert.assertEquals(1, TestTxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from TXN_COMPONENTS where TC_WRITEID=" + highestCompactWriteId));
    //now make a successful compactor run so that next Cleaner run actually cleans
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_COMPACTION, false);
    runStatementOnDriver("alter table T compact 'minor'");
    runWorker(hiveConf);

    resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history",
        2, resp.getCompactsSize());
    //check both combinations - don't know what order the db returns them in
    Assert.assertTrue("Unexpected compaction state",
        (TxnStore.FAILED_RESPONSE.equalsIgnoreCase(resp.getCompacts().get(0).getState())
            && TxnStore.CLEANING_RESPONSE.equalsIgnoreCase(resp.getCompacts().get(1).getState())) ||
            (TxnStore.CLEANING_RESPONSE.equalsIgnoreCase(resp.getCompacts().get(0).getState()) &&
                TxnStore.FAILED_RESPONSE.equalsIgnoreCase(resp.getCompacts().get(1).getState())));

    //delete metadata about aborted txn from txn_components and files (if any)
    runCleaner(hiveConf);
  }

  @Test
  public void testMajorCompactionAbortLeftoverFiles() throws Exception {
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID, true);

    dropTable(new String[] {"T"});
    //note: transaction names T1, T2, etc below, are logical, the actual txnid will be different
    runStatementOnDriver("create table T (a int, b int) stored as orc");
    runStatementOnDriver("insert into T values(0,2)"); //makes delta_1_1 in T1
    runStatementOnDriver("insert into T values(1,4)"); //makes delta_2_2 in T2

    runStatementOnDriver("alter table T compact 'minor'");
    //create failed compaction attempt so that compactor txn is aborted
    MRCompactor mrCompactor = Mockito.spy(new MRCompactor(HiveMetaStoreUtils.getHiveMetastoreClient(hiveConf)));

    Mockito.doAnswer((Answer<Void>) invocationOnMock -> {
      invocationOnMock.callRealMethod();
      throw new RuntimeException(
        "Will cause CompactorMR to fail all opening txn and creating directories for compaction.");
    }).when(mrCompactor).run(any());

    CompactorFactory mockedFactory = Mockito.mock(CompactorFactory.class);
    when(mockedFactory.getCompactorPipeline(any(), any(), any(), any())).thenReturn(new CompactorPipeline(mrCompactor));

    Worker worker = Mockito.spy(new Worker(mockedFactory));
    worker.setConf(hiveConf);
    worker.init(new AtomicBoolean(true));
    worker.run();

    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history",
        1, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0th compaction state",
        TxnStore.FAILED_RESPONSE, resp.getCompacts().get(0).getState());
    GetOpenTxnsResponse openResp =  txnHandler.getOpenTxns();
    Assert.assertEquals(openResp.toString(), 1, openResp.getOpen_txnsSize());
    //check that the compactor txn is aborted
    Assert.assertTrue(openResp.toString(), BitSet.valueOf(openResp.getAbortedBits()).get(0));
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(hiveConf,
        "SELECT count(*) FROM hive_locks WHERE hl_txnid=" + openResp.getOpen_txns().get(0)));

    FileSystem fs = FileSystem.get(hiveConf);
    Path warehousePath = new Path(getWarehouseDir());
    FileStatus[] actualList = fs.listStatus(new Path(warehousePath + "/t"),
        FileUtils.HIDDEN_FILES_PATH_FILTER);

    // we expect all the t/base_* files to be removed by the compactor failure
    String[] expectedList = new String[] {
        "/t/delta_0000001_0000001_0000",
        "/t/delta_0000002_0000002_0000",
    };
    checkExpectedFiles(actualList, expectedList, warehousePath.toString());
    //delete metadata about aborted txn from txn_components and files (if any)
    runCleaner(hiveConf);
  }

  @Test
  public void testMinorCompactionAbortLeftoverFiles() throws Exception {
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID, true);

    dropTable(new String[] {"T"});
    //note: transaction names T1, T2, etc below, are logical, the actual txnid will be different
    runStatementOnDriver("create table T (a int, b int) stored as orc");
    runStatementOnDriver("insert into T values(0,2)"); //makes delta_1_1 in T1
    runStatementOnDriver("insert into T values(1,4)"); //makes delta_2_2 in T2
    runStatementOnDriver("update T set a=3 where b=2"); //makes delta/(delete_delta)_3_3 in T3

    runStatementOnDriver("alter table T compact 'minor'");
    //create failed compaction attempt so that compactor txn is aborted
    MRCompactor mrCompactor = Mockito.spy(new MRCompactor(HiveMetaStoreUtils.getHiveMetastoreClient(hiveConf)));

    Mockito.doAnswer((Answer<Void>) invocationOnMock -> {
      invocationOnMock.callRealMethod();
      throw new RuntimeException(
        "Will cause CompactorMR to fail all opening txn and creating directories for compaction.");
    }).when(mrCompactor).run(any());

    CompactorFactory mockedFactory = Mockito.mock(CompactorFactory.class);
    when(mockedFactory.getCompactorPipeline(any(), any(), any(), any())).thenReturn(new CompactorPipeline(mrCompactor));

    Worker worker = Mockito.spy(new Worker(mockedFactory));
    worker.setConf(hiveConf);
    worker.init(new AtomicBoolean(true));
    worker.run();

    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history",
        1, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0th compaction state",
        TxnStore.FAILED_RESPONSE, resp.getCompacts().get(0).getState());
    GetOpenTxnsResponse openResp =  txnHandler.getOpenTxns();
    Assert.assertEquals(openResp.toString(), 1, openResp.getOpen_txnsSize());
    //check that the compactor txn is aborted
    Assert.assertTrue(openResp.toString(), BitSet.valueOf(openResp.getAbortedBits()).get(0));
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(hiveConf,
       "SELECT count(*) FROM hive_locks WHERE hl_txnid=" + openResp.getOpen_txns().get(0)));

    FileSystem fs = FileSystem.get(hiveConf);
    Path warehousePath = new Path(getWarehouseDir());
    FileStatus[] actualList = fs.listStatus(new Path(warehousePath + "/t"),
        FileUtils.HIDDEN_FILES_PATH_FILTER);

    // we expect all the t/base_* files to be removed by the compactor failure
    String[] expectedList = new String[] {
        "/t/delta_0000001_0000001_0000",
        "/t/delta_0000002_0000002_0000",
        "/t/delete_delta_0000003_0000003_0000",
        "/t/delta_0000003_0000003_0001",
    };
    checkExpectedFiles(actualList, expectedList, warehousePath.toString());
    //delete metadata about aborted txn from txn_components and files (if any)
    runCleaner(hiveConf);
  }

  /**
   * Not enough deltas to compact, no need to clean: there is absolutely nothing to do.
   */
  @Test public void testNotEnoughToCompact() throws Exception {
    int[][] tableData = {{1, 2}, {3, 4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData));
    runStatementOnDriver("alter table " + Table.ACIDTBL + " compact 'MAJOR'");

    runWorker(hiveConf);
    assertTableIsEmpty("TXN_COMPONENTS");

    runCleaner(hiveConf);
    assertTableIsEmpty("TXN_COMPONENTS");
  }

  /**
   * There aren't enough deltas to compact, but cleaning is needed because an insert overwrite
   * was executed.
   */
  @Test public void testNotEnoughToCompactNeedsCleaning() throws Exception {
    int[][] tableData = {{1, 2}, {3, 4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData));
    runStatementOnDriver(
        "insert overwrite table " + Table.ACIDTBL + " " + makeValuesClause(tableData));

    runStatementOnDriver("alter table " + Table.ACIDTBL + " compact 'MAJOR'");

    runWorker(hiveConf);
    assertTableIsEmpty("TXN_COMPONENTS");

    runCleaner(hiveConf);
    assertTableIsEmpty("TXN_COMPONENTS");
  }

  private void assertTableIsEmpty(String table) throws Exception {
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from " + table), 0,
        TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from " + table));
  }

  @Test
  public void testWritesToDisabledCompactionTableCtas() throws Exception {
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.METRICS_ENABLED, true);
    MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS,
        HMSMetricsListener.class.getName());

    runStatementOnDriver("insert into " + Table.ACIDTBL + " values(1,1)");
    runStatementOnDriver("create table mytable stored as orc tblproperties ('transactional'='true')"
        + "as select * from " + Table.ACIDTBL);
  }
}
