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
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.TxnInfo;
import org.apache.hadoop.hive.metastore.api.TxnState;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lockmgr.TestDbTxnManager2;
import org.apache.hadoop.hive.ql.metadata.HiveException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * The LockManager is not ready, but for no-concurrency straight-line path we can
 * test AC=true, and AC=false with commit/rollback/exception and test resulting data.
 *
 * Can also test, calling commit in AC=true mode, etc, toggling AC...
 * 
 * Tests here are for multi-statement transactions (WIP) and those that don't need to
 * run with Acid 2.0 (see subclasses of TestTxnCommands2)
 */
public class TestTxnCommands {
  static final private Logger LOG = LoggerFactory.getLogger(TestTxnCommands.class);
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
    ACIDTBLPART("acidTblPart"),
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
    hiveConf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    hiveConf.setBoolVar(HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK, true);
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
    d.setMaxRows(10000);
    dropTables();
    runStatementOnDriver("create table " + Table.ACIDTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + Table.ACIDTBLPART + "(a int, b int) partitioned by (p string) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
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
    TestTxnCommands2.runHouseKeeperService(houseKeeperService, hiveConf);
    //this should fail because txn aborted due to timeout
    CommandProcessorResponse cpr = runStatementOnDriverNegative("delete from " + Table.ACIDTBL + " where a = 5");
    Assert.assertTrue("Actual: " + cpr.getErrorMessage(), cpr.getErrorMessage().contains("Transaction manager has aborted the transaction txnid:1"));
    
    //now test that we don't timeout locks we should not
    //heartbeater should be running in the background every 1/2 second
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TXN_TIMEOUT, 1, TimeUnit.SECONDS);
    //hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEFAILHEARTBEATER, true);
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
    Assert.assertEquals(2, txnInfo.getId());
    Assert.assertEquals(TxnState.OPEN, txnInfo.getState());
    String s =TxnDbUtil.queryToString("select TXN_STARTED, TXN_LAST_HEARTBEAT from TXNS where TXN_ID = " + txnInfo.getId(), false);
    String[] vals = s.split("\\s+");
    Assert.assertEquals("Didn't get expected timestamps", 2, vals.length);
    long lastHeartbeat = Long.parseLong(vals[1]);
    //these 2 values are equal when TXN entry is made.  Should never be equal after 1st heartbeat, which we
    //expect to have happened by now since HIVE_TXN_TIMEOUT=1sec
    Assert.assertNotEquals("Didn't see heartbeat happen", Long.parseLong(vals[0]), lastHeartbeat);
    
    ShowLocksResponse slr = txnHandler.showLocks(new ShowLocksRequest());
    TestDbTxnManager2.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", Table.ACIDTBL.name, null, slr.getLocks());
    pause(750);
    TestTxnCommands2.runHouseKeeperService(houseKeeperService, hiveConf);
    pause(750);
    slr = txnHandler.showLocks(new ShowLocksRequest());
    Assert.assertEquals("Unexpected lock count: " + slr, 1, slr.getLocks().size());
    TestDbTxnManager2.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", Table.ACIDTBL.name, null, slr.getLocks());

    pause(750);
    TestTxnCommands2.runHouseKeeperService(houseKeeperService, hiveConf);
    slr = txnHandler.showLocks(new ShowLocksRequest());
    Assert.assertEquals("Unexpected lock count: " + slr, 1, slr.getLocks().size());
    TestDbTxnManager2.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", Table.ACIDTBL.name, null, slr.getLocks());

    //should've done several heartbeats
    s =TxnDbUtil.queryToString("select TXN_STARTED, TXN_LAST_HEARTBEAT from TXNS where TXN_ID = " + txnInfo.getId(), false);
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
  @Test
  public void testMergeNegative() throws Exception {
    CommandProcessorResponse cpr = runStatementOnDriverNegative("MERGE INTO " + Table.ACIDTBL +
      " target USING " + Table.NONACIDORCTBL +
      " source\nON target.a = source.a " +
      "\nWHEN MATCHED THEN UPDATE set b = 1 " +
      "\nWHEN MATCHED THEN DELETE " +
      "\nWHEN NOT MATCHED AND a < 1 THEN INSERT VALUES(1,2)");
    Assert.assertEquals(ErrorMsg.MERGE_PREDIACTE_REQUIRED, ((HiveException)cpr.getException()).getCanonicalErrorMsg());
  }
  @Test
  public void testMergeNegative2() throws Exception {
    CommandProcessorResponse cpr = runStatementOnDriverNegative("MERGE INTO "+ Table.ACIDTBL +
      " target USING " + Table.NONACIDORCTBL + "\n source ON target.pk = source.pk " +
      "\nWHEN MATCHED THEN UPDATE set b = 1 " +
      "\nWHEN MATCHED THEN UPDATE set b=a");
    Assert.assertEquals(ErrorMsg.MERGE_TOO_MANY_UPDATE, ((HiveException)cpr.getException()).getCanonicalErrorMsg());
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
    for(int i = 0; i < explain.size(); i++) {
      if(explain.get(i).contains("Edges:")) {
        Assert.assertTrue("At i+1=" + (i+1) + explain.get(i + 1), explain.get(i + 1).contains("Reducer 2 <- Map 1 (SIMPLE_EDGE), Map 7 (SIMPLE_EDGE)"));
        Assert.assertTrue("At i+1=" + (i+2) + explain.get(i + 2), explain.get(i + 2).contains("Reducer 3 <- Reducer 2 (SIMPLE_EDGE)"));
        Assert.assertTrue("At i+1=" + (i+3) + explain.get(i + 3), explain.get(i + 3).contains("Reducer 4 <- Reducer 2 (SIMPLE_EDGE)"));
        Assert.assertTrue("At i+1=" + (i+4) + explain.get(i + 4), explain.get(i + 4).contains("Reducer 5 <- Reducer 2 (SIMPLE_EDGE)"));
        Assert.assertTrue("At i+1=" + (i+5) + explain.get(i + 5), explain.get(i + 5).contains("Reducer 6 <- Reducer 2 (CUSTOM_SIMPLE_EDGE)"));
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
      "WHEN MATCHED AND s.a < 3 THEN update set b = 0 " +
      "WHEN MATCHED and t.a > 3 and t.a < 5 THEN DELETE " +
      "WHEN NOT MATCHED THEN INSERT VALUES(s.a, s.b) ";
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
    CommandProcessorResponse cpr = runStatementOnDriverNegative("MERGE INTO "+ Table.ACIDTBL +
      " target USING " + Table.NONACIDORCTBL +
      "\n source ON target.a = source.a " +
      "\nWHEN MATCHED THEN UPDATE set t = 1");
    Assert.assertEquals(ErrorMsg.INVALID_TARGET_COLUMN_IN_SET_CLAUSE,
      ((HiveException)cpr.getException()).getCanonicalErrorMsg());
    cpr = runStatementOnDriverNegative("update " + Table.ACIDTBL + " set t = 1");
    Assert.assertEquals(ErrorMsg.INVALID_TARGET_COLUMN_IN_SET_CLAUSE,
      ((HiveException)cpr.getException()).getCanonicalErrorMsg());
  }
  @Test
  public void testBadOnClause() throws Exception {
    CommandProcessorResponse cpr = runStatementOnDriverNegative("merge into " + Table.ACIDTBL +
      " trgt using (select * from " + Table.NONACIDORCTBL +
      "src) sub on sub.a = target.a when not matched then insert values (sub.a,sub.b)");
    Assert.assertTrue("Error didn't match: " + cpr, cpr.getErrorMessage().contains(
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
    runStatementOnDriver("alter table "+ TestTxnCommands2.Table.ACIDTBL +" compact 'major' AND WAIT");
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
}
