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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.FileDump;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.AcidHouseKeeperService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The LockManager is not ready, but for no-concurrency straight-line path we can
 * test AC=true, and AC=false with commit/rollback/exception and test resulting data.
 *
 * Can also test, calling commit in AC=true mode, etc, toggling AC...
 */
public class TestTxnCommands {
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
    File.separator + TestTxnCommands.class.getCanonicalName()
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
    ACIDTBL2("acidTbl2"),
    NONACIDORCTBL("nonAcidOrcTbl"),
    NONACIDORCTBL2("nonAcidOrcTbl2");

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
    runStatementOnDriver("create table " + Table.NONACIDORCTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table " + Table.NONACIDORCTBL2 + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create temporary  table " + Table.ACIDTBL2 + "(a int, b int, c int) clustered by (c) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
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
        runStatementOnDriver("set autocommit true");
        dropTables();
        d.destroy();
        d.close();
        d = null;
      }
    } finally {
      TxnDbUtil.cleanDb();
      FileUtils.deleteDirectory(new File(TEST_DATA_DIR));
    }
  }
  @Test
  public void testInsertOverwrite() throws Exception {
    runStatementOnDriver("insert overwrite table " + Table.NONACIDORCTBL + " select a,b from " + Table.NONACIDORCTBL2);
    runStatementOnDriver("create table " + Table.NONACIDORCTBL2 + "3(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='false')");

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
  private void dumpBucketData(Table table, long txnId, int stmtId, int bucketNum) throws Exception {
    if(true) {
      return;
    }
    Path bucket = AcidUtils.createBucketFile(new Path(new Path(TEST_WAREHOUSE_DIR, table.toString().toLowerCase()), AcidUtils.deltaSubdir(txnId, txnId, stmtId)), bucketNum);
    FileOutputStream delta = new FileOutputStream(testName.getMethodName() + "_" + bucket.getParent().getName() + "_" +  bucket.getName());
//    try {
//      FileDump.printJsonData(hiveConf, bucket.toString(), delta);
//    }
//    catch(FileNotFoundException ex) {
      ;//this happens if you change BUCKET_COUNT
//    }
    delta.close();
  }
  /**
   * Dump all data in the table by bucket in JSON format
   */
  private void dumpTableData(Table table, long txnId, int stmtId) throws Exception {
    for(int bucketNum = 0; bucketNum < BUCKET_COUNT; bucketNum++) {
      dumpBucketData(table, txnId, stmtId, bucketNum);
    }
  }
  @Test
  public void testSimpleAcidInsert() throws Exception {
    int[][] rows1 = {{1,2},{3,4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows1));
    //List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    //Assert.assertEquals("Data didn't match in autocommit=true (rs)", stringifyValues(rows1), rs);
    runStatementOnDriver("set autocommit false");
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
    runStatementOnDriver("COMMIT");//txn started implicitly by previous statement
    runStatementOnDriver("set autocommit true");
    List<String> rs1 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Data didn't match inside tx (rs0)", allData, rs1);
  }

  /**
   * add tests for all transitions - AC=t, AC=t, AC=f, commit (for example)
   * @throws Exception
   */
  @Test
  public void testErrors() throws Exception {
    runStatementOnDriver("set autocommit true");
    CommandProcessorResponse cpr = runStatementOnDriverNegative("start transaction");
    Assert.assertEquals("Error didn't match: " + cpr, ErrorMsg.OP_NOT_ALLOWED_IN_AUTOCOMMIT.getErrorCode(), cpr.getErrorCode());
    runStatementOnDriver("set autocommit false");
    runStatementOnDriver("start transaction");
    CommandProcessorResponse cpr2 = runStatementOnDriverNegative("create table foo(x int, y int)");
    Assert.assertEquals("Expected DDL to fail in an open txn", ErrorMsg.OP_NOT_ALLOWED_IN_TXN.getErrorCode(), cpr2.getErrorCode());
    runStatementOnDriver("set autocommit true");
    CommandProcessorResponse cpr3 = runStatementOnDriverNegative("update " + Table.ACIDTBL + " set a = 1 where b != 1");
    Assert.assertEquals("Expected update of bucket column to fail",
      "FAILED: SemanticException [Error 10302]: Updating values of bucketing columns is not supported.  Column a.",
      cpr3.getErrorMessage());
    //line below should in principle work but Driver doesn't propagate errorCode properly
    //Assert.assertEquals("Expected update of bucket column to fail", ErrorMsg.UPDATE_CANNOT_UPDATE_BUCKET_VALUE.getErrorCode(), cpr3.getErrorCode());
    cpr3 = runStatementOnDriverNegative("commit work");//not allowed in AC=true
    Assert.assertEquals("Error didn't match: " + cpr3, ErrorMsg.OP_NOT_ALLOWED_IN_AUTOCOMMIT.getErrorCode(), cpr.getErrorCode());
    cpr3 = runStatementOnDriverNegative("rollback work");//not allowed in AC=true
    Assert.assertEquals("Error didn't match: " + cpr3, ErrorMsg.OP_NOT_ALLOWED_IN_AUTOCOMMIT.getErrorCode(), cpr.getErrorCode());
    runStatementOnDriver("set autocommit false");
    cpr3 = runStatementOnDriverNegative("commit");//not allowed in w/o tx
    Assert.assertEquals("Error didn't match: " + cpr3, ErrorMsg.OP_NOT_ALLOWED_IN_AUTOCOMMIT.getErrorCode(), cpr.getErrorCode());
    cpr3 = runStatementOnDriverNegative("rollback");//not allowed in w/o tx
    Assert.assertEquals("Error didn't match: " + cpr3, ErrorMsg.OP_NOT_ALLOWED_IN_AUTOCOMMIT.getErrorCode(), cpr.getErrorCode());
    runStatementOnDriver("start transaction");
    cpr3 = runStatementOnDriverNegative("start transaction");//not allowed in a tx
    Assert.assertEquals("Expected start transaction to fail", ErrorMsg.OP_NOT_ALLOWED_IN_TXN.getErrorCode(), cpr3.getErrorCode());
    runStatementOnDriver("start transaction");//ok since previously opened txn was killed
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(1,2)");
    List<String> rs0 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Can't see my own write", 1, rs0.size());
    runStatementOnDriver("set autocommit true");//this should commit previous txn
    rs0 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Can't see my own write", 1, rs0.size());
  }
  @Test
  public void testReadMyOwnInsert() throws Exception {
    runStatementOnDriver("set autocommit false");
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
    runStatementOnDriver("set autocommit false");
    runStatementOnDriver("START TRANSACTION");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(1,2)");
    List<String> rs0 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Can't see my own write", 1, rs0.size());
    //next command should produce an error
    CommandProcessorResponse cpr = runStatementOnDriverNegative("select * from no_such_table");
    Assert.assertEquals("Txn didn't fail?",
      "FAILED: SemanticException [Error 10001]: Line 1:14 Table not found 'no_such_table'",
      cpr.getErrorMessage());
    runStatementOnDriver("start transaction");
    List<String> rs1 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    runStatementOnDriver("commit");
    Assert.assertEquals("Didn't rollback as expected", 0, rs1.size());
  }
  @Test
  public void testExplicitRollback() throws Exception {
    runStatementOnDriver("set autocommit false");
    runStatementOnDriver("START TRANSACTION");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(1,2)");
    runStatementOnDriver("ROLLBACK");
    runStatementOnDriver("set autocommit true");
    List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Rollback didn't rollback", 0, rs.size());
  }

  @Test
  public void testMultipleInserts() throws Exception {
    runStatementOnDriver("set autocommit false");
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
    runStatementOnDriver("set autocommit true");
    List<String> rs1 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Content didn't match after commit rs1", allData, rs1);
  }
  @Test
  public void testDelete() throws Exception {
    int[][] rows1 = {{1,2},{3,4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows1));
    List<String> rs0 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Content didn't match rs0", stringifyValues(rows1), rs0);
    runStatementOnDriver("set autocommit false");
    runStatementOnDriver("START TRANSACTION");
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where b = 4");
    int[][] updatedData2 = {{1,2}};
    List<String> rs3 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Wrong data after delete", stringifyValues(updatedData2), rs3);
    runStatementOnDriver("commit");
    runStatementOnDriver("set autocommit true");
    List<String> rs4 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Wrong data after commit", stringifyValues(updatedData2), rs4);
  }

  @Test
  public void testUpdateOfInserts() throws Exception {
    int[][] rows1 = {{1,2},{3,4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows1));
    List<String> rs0 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Content didn't match rs0", stringifyValues(rows1), rs0);
    runStatementOnDriver("set autocommit false");
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
    runStatementOnDriver("set autocommit true");
    List<String> rs4 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Wrong data after commit", stringifyValues(updatedData), rs4);
  }
  @Test
  public void testUpdateDeleteOfInserts() throws Exception {
    int[][] rows1 = {{1,2},{3,4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows1));
    List<String> rs0 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Content didn't match rs0", stringifyValues(rows1), rs0);
    runStatementOnDriver("set autocommit false");
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
    runStatementOnDriver("set autocommit true");
    List<String> rs4 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Wrong data after commit", stringifyValues(updatedData2), rs4);
  }
  @Test
  public void testMultipleDelete() throws Exception {
    int[][] rows1 = {{1,2},{3,4},{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(rows1));
    List<String> rs0 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals("Content didn't match rs0", stringifyValues(rows1), rs0);
    runStatementOnDriver("set autocommit false");
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
    runStatementOnDriver("set autocommit true");
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
    runStatementOnDriver("set autocommit false");
    runStatementOnDriver("start transaction");
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where a = 5");
    //make sure currently running txn is considered aborted by housekeeper
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TIMEDOUT_TXN_REAPER_START, 0, TimeUnit.SECONDS);
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TXN_TIMEOUT, 2, TimeUnit.MILLISECONDS);
    AcidHouseKeeperService houseKeeperService = new AcidHouseKeeperService();
    //this will abort the txn
    houseKeeperService.start(hiveConf);
    while(houseKeeperService.getIsAliveCounter() <= Integer.MIN_VALUE) {
      Thread.sleep(100);//make sure it has run at least once
    }
    houseKeeperService.stop();
    //this should fail because txn aborted due to timeout
    CommandProcessorResponse cpr = runStatementOnDriverNegative("delete from " + Table.ACIDTBL + " where a = 5");
    Assert.assertTrue("Actual: " + cpr.getErrorMessage(), cpr.getErrorMessage().contains("Transaction manager has aborted the transaction txnid:1"));
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
  private CommandProcessorResponse runStatementOnDriverNegative(String stmt) throws Exception {
    CommandProcessorResponse cpr = d.run(stmt);
    if(cpr.getResponseCode() != 0) {
      return cpr;
    }
    throw new RuntimeException("Didn't get expected failure!");
  }

//  @Ignore
  @Test
  public void exchangePartition() throws Exception {
    runStatementOnDriver("create database ex1");
    runStatementOnDriver("create database ex2");

    runStatementOnDriver("CREATE TABLE ex1.exchange_part_test1 (f1 string) PARTITIONED BY (ds STRING)");
    runStatementOnDriver("CREATE TABLE ex2.exchange_part_test2 (f1 string) PARTITIONED BY (ds STRING)");
    runStatementOnDriver("ALTER TABLE ex2.exchange_part_test2 ADD PARTITION (ds='2013-04-05')");
    runStatementOnDriver("ALTER TABLE ex1.exchange_part_test1 EXCHANGE PARTITION (ds='2013-04-05') WITH TABLE ex2.exchange_part_test2");
  }
}
