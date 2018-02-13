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

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.txn.AcidCompactionHistoryService;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.metastore.txn.AcidOpenTxnsCounterService;
import org.apache.hadoop.hive.ql.txn.compactor.Cleaner;
import org.apache.hadoop.hive.ql.txn.compactor.Initiator;
import org.apache.hadoop.hive.ql.txn.compactor.Worker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTxnCommands2 {
  static final private Logger LOG = LoggerFactory.getLogger(TestTxnCommands2.class);
  protected static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
    File.separator + TestTxnCommands2.class.getCanonicalName()
    + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
  protected static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  //bucket count for test tables; set it to 1 for easier debugging
  static int BUCKET_COUNT = 2;
  @Rule
  public TestName testName = new TestName();

  protected HiveConf hiveConf;
  protected Driver d;
  protected enum Table {
    ACIDTBL("acidTbl"),
    ACIDTBLPART("acidTblPart", "p"),
    NONACIDORCTBL("nonAcidOrcTbl"),
    NONACIDPART("nonAcidPart", "p"),
    NONACIDPART2("nonAcidPart2", "p2"),
    ACIDNESTEDPART("acidNestedPart", "p,q"),
    MMTBL("mmTbl");

    private final String name;
    private final String partitionColumns;
    @Override
    public String toString() {
      return name;
    }
    String getPartitionColumns() {
      return partitionColumns;
    }
    Table(String name) {
      this(name, null);
    }
    Table(String name, String partitionColumns) {
      this.name = name;
      this.partitionColumns = partitionColumns;
    }
  }
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    setUpWithTableProperties("'transactional'='true'");
  }

  void setUpWithTableProperties(String tableProperties) throws Exception {
    hiveConf = new HiveConf(this.getClass());
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, TEST_WAREHOUSE_DIR);
    hiveConf.setVar(HiveConf.ConfVars.HIVEINPUTFORMAT, HiveInputFormat.class.getName());
    hiveConf
        .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    hiveConf.setBoolVar(HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK, true);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSCOLAUTOGATHER, false);

    TxnDbUtil.setConfValues(hiveConf);
    TxnDbUtil.prepDb(hiveConf);
    File f = new File(TEST_WAREHOUSE_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(TEST_WAREHOUSE_DIR).mkdirs())) {
      throw new RuntimeException("Could not create " + TEST_WAREHOUSE_DIR);
    }
    SessionState ss = SessionState.start(hiveConf);
    ss.applyAuthorizationPolicy();
    d = new Driver(new QueryState.Builder().withHiveConf(hiveConf).nonIsolated().build(), null);
    d.setMaxRows(10000);
    dropTables();
    runStatementOnDriver("create table " + Table.ACIDTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES (" + tableProperties + ")");
    runStatementOnDriver("create table " + Table.ACIDTBLPART + "(a int, b int) partitioned by (p string) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES (" + tableProperties + ")");
    runStatementOnDriver("create table " + Table.NONACIDORCTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table " + Table.NONACIDPART + "(a int, b int) partitioned by (p string) stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table " + Table.NONACIDPART2 +
      "(a2 int, b2 int) partitioned by (p2 string) stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table " + Table.ACIDNESTEDPART +
      "(a int, b int) partitioned by (p int, q int) clustered by (a) into " + BUCKET_COUNT +
      " buckets stored as orc TBLPROPERTIES (" + tableProperties + ")");
    runStatementOnDriver("create table " + Table.MMTBL + "(a int, b int) TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')");
  }

  protected void dropTables() throws Exception {
    for(Table t : Table.values()) {
      runStatementOnDriver("drop table if exists " + t);
    }
  }
  @After
  public void tearDown() throws Exception {
    try {
      if (d != null) {
        dropTables();
        d.close();
        d.destroy();
        d = null;
      }
      TxnDbUtil.cleanDb(hiveConf);
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
      assertExplainHasString("filterExpr: (a = 3)", explain, "PPD wasn't pushed");
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
      assertExplainHasString("filterExpr: (b = 4)", explain, "PPD wasn't pushed");
    }
    List<String> rs0 = runStatementOnDriver(query);
    Assert.assertEquals("Read failed", 0, rs0.size());
    runStatementOnDriver("alter table " + Table.ACIDTBL + " compact 'MAJOR'");
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(hiveConf);
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
      assertExplainHasString("filterExpr: ((a = 7) and (b = 8))", explain, "PPD wasn't pushed");
    }
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where a=7 and b=8");
    //now we have delta_0004_0004_0000 with delete events

    /*(can't push predicate to 'delete' delta)
    * if we were to push to 'delete' delta, we'd filter out all rows since the 'row' is always NULL for
    * delete events and we'd produce data as if the delete never happened*/
    query = "select a,b from " + Table.ACIDTBL + " where a > 1 order by a,b";
    if(enablePPD) {
      explain = runStatementOnDriver("explain " + query);
      assertExplainHasString("filterExpr: (a > 1)", explain, "PPD wasn't pushed");
    }
    List<String> rs1 = runStatementOnDriver(query);
    int [][] resultData = new int[][] {{3, 5}, {5, 6}, {9, 10}};
    Assert.assertEquals("Update failed", stringifyValues(resultData), rs1);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVEOPTINDEXFILTER, originalPpd);
  }

  static void assertExplainHasString(String string, List<String> queryPlan, String errMsg) {
    for(String line : queryPlan) {
      if(line != null && line.contains(string)) {
        return;
      }
    }

    Assert.assertFalse(errMsg, true);
  }
  @Test
  public void testAlterTable() throws Exception {
    int[][] tableData = {{1,2}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData));
    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MAJOR'");
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(hiveConf);
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
//  @Ignore("not needed but useful for testing")
  @Test
  public void testNonAcidInsert() throws Exception {
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,2)");
    List<String> rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(2,3)");
    List<String> rs1 = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
  }

  @Test
  public void testOriginalFileReaderWhenNonAcidConvertedToAcid() throws Exception {
    // 1. Insert five rows to Non-ACID table.
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,2),(3,4),(5,6),(7,8),(9,10)");

    // 2. Convert NONACIDORCTBL to ACID table.  //todo: remove trans_prop after HIVE-17089
    runStatementOnDriver("alter table " + Table.NONACIDORCTBL + " SET TBLPROPERTIES ('transactional'='true', 'transactional_properties'='default')");
    runStatementOnDriver("update " + Table.NONACIDORCTBL + " set b = b*2 where b in (4,10)");
    runStatementOnDriver("delete from " + Table.NONACIDORCTBL + " where a = 7");

    List<String> rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL + " order by a,b");
    int[][] resultData = new int[][] {{1,2}, {3,8}, {5,6}, {9,20}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    // 3. Perform a major compaction.
    runStatementOnDriver("alter table "+ Table.NONACIDORCTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history", 1, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0 compaction state", TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(0).getState());
    Assert.assertTrue(resp.getCompacts().get(0).getHadoopJobId().startsWith("job_local"));

    // 3. Perform a delete.
    runStatementOnDriver("delete from " + Table.NONACIDORCTBL + " where a = 1");

    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL + " order by a,b");
    resultData = new int[][] {{3,8}, {5,6}, {9,20}};
    Assert.assertEquals(stringifyValues(resultData), rs);
  }
  /**
   * see HIVE-16177
   * See also {@link TestTxnCommands#testNonAcidToAcidConversion01()}
   * {@link TestTxnNoBuckets#testCTAS()}
   */
  @Test
  public void testNonAcidToAcidConversion02() throws Exception {
    //create 2 rows in a file 000001_0 (and an empty 000000_0)
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,2),(1,3)");
    //create 2 rows in a file 000000_0_copy1 and 2 rows in a file 000001_0_copy1
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(0,12),(0,13),(1,4),(1,5)");
    //create 1 row in a file 000001_0_copy2 (and empty 000000_0_copy2?)
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,6)");

    //convert the table to Acid  //todo: remove trans_prop after HIVE-17089
    runStatementOnDriver("alter table " + Table.NONACIDORCTBL + " SET TBLPROPERTIES ('transactional'='true', 'transactional_properties'='default')");
    List<String> rs1 = runStatementOnDriver("describe "+ Table.NONACIDORCTBL);
    //create a some of delta directories
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(0,15),(1,16)");
    runStatementOnDriver("update " + Table.NONACIDORCTBL + " set b = 120 where a = 0 and b = 12");
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(0,17)");
    runStatementOnDriver("delete from " + Table.NONACIDORCTBL + " where a = 1 and b = 3");

    List<String> rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from " +  Table.NONACIDORCTBL + " order by a,b");
    LOG.warn("before compact");
    for(String s : rs) {
      LOG.warn(s);
    }
    Assert.assertEquals(536870912, BucketCodec.V1.encode(new AcidOutputFormat.Options(hiveConf).bucket(0)));
    Assert.assertEquals(536936448, BucketCodec.V1.encode(new AcidOutputFormat.Options(hiveConf).bucket(1)));
    /*
     * All ROW__IDs are unique on read after conversion to acid
     * ROW__IDs are exactly the same before and after compaction
     * Also check the file name (only) after compaction for completeness
     * Note: order of rows in a file ends up being the reverse of order in values clause (why?!)
     */
    String[][] expected = {
      {"{\"transactionid\":0,\"bucketid\":536870912,\"rowid\":0}\t0\t13",  "bucket_00000"},
      {"{\"transactionid\":20,\"bucketid\":536870912,\"rowid\":0}\t0\t15", "bucket_00000"},
      {"{\"transactionid\":22,\"bucketid\":536870912,\"rowid\":0}\t0\t17", "bucket_00000"},
      {"{\"transactionid\":21,\"bucketid\":536870912,\"rowid\":0}\t0\t120", "bucket_00000"},
      {"{\"transactionid\":0,\"bucketid\":536936448,\"rowid\":1}\t1\t2",   "bucket_00001"},
      {"{\"transactionid\":0,\"bucketid\":536936448,\"rowid\":3}\t1\t4",   "bucket_00001"},
      {"{\"transactionid\":0,\"bucketid\":536936448,\"rowid\":2}\t1\t5",   "bucket_00001"},
      {"{\"transactionid\":0,\"bucketid\":536936448,\"rowid\":4}\t1\t6",   "bucket_00001"},
      {"{\"transactionid\":20,\"bucketid\":536936448,\"rowid\":0}\t1\t16", "bucket_00001"}
    };
    Assert.assertEquals("Unexpected row count before compaction", expected.length, rs.size());
    for(int i = 0; i < expected.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i) + "; expected " + expected[i][0],
          rs.get(i).startsWith(expected[i][0]));
    }
    //run Compaction
    runStatementOnDriver("alter table "+ TestTxnCommands2.Table.NONACIDORCTBL +" compact 'major'");
    TestTxnCommands2.runWorker(hiveConf);
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from " + Table.NONACIDORCTBL + " order by a,b");
    LOG.warn("after compact");
    for(String s : rs) {
      LOG.warn(s);
    }
    Assert.assertEquals("Unexpected row count after compaction", expected.length, rs.size());
    for(int i = 0; i < expected.length; i++) {
      Assert.assertTrue("Actual line " + i + " ac: " + rs.get(i), rs.get(i).startsWith(expected[i][0]));
      Assert.assertTrue("Actual line(bucket) " + i + " ac: " + rs.get(i), rs.get(i).endsWith(expected[i][1]));
    }
    //make sure they are the same before and after compaction
  }
  /**
   * In current implementation of ACID, altering the value of transactional_properties or trying to
   * set a value for previously unset value for an acid table will throw an exception.
   * @throws Exception
   */
  @Test
  public void testFailureOnAlteringTransactionalProperties() throws Exception {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("TBLPROPERTIES with 'transactional_properties' cannot be altered after the table is created");
    runStatementOnDriver("create table acidTblLegacy (a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("alter table acidTblLegacy SET TBLPROPERTIES ('transactional_properties' = 'insert_only')");
  }
  /**
   * Test the query correctness and directory layout for ACID table conversion
   * 1. Insert a row to Non-ACID table
   * 2. Convert Non-ACID to ACID table
   * 3. Insert a row to ACID table
   * 4. Perform Major compaction
   * 5. Clean
   * @throws Exception
   */
  @Test
  public void testNonAcidToAcidConversion1() throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;

    // 1. Insert a row to Non-ACID table
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,2)");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
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
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
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
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    // There should be 2 original bucket files (000000_0 and 000001_0), plus a new delta directory.
    // The delta directory should also have only 1 bucket file (bucket_00001)
    Assert.assertEquals(3, status.length);
    boolean sawNewDelta = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("delta_.*")) {
        sawNewDelta = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(1, buckets.length); // only one bucket file
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_00001"));
      } else {
        Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
      }
    }
    Assert.assertTrue(sawNewDelta);
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL + " order by a,b");
    resultData = new int[][] {{1, 2}, {3, 4}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 4. Perform a major compaction
    runStatementOnDriver("alter table "+ Table.NONACIDORCTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    // There should be 1 new directory: base_xxxxxxx.
    // Original bucket files and delta directory should stay until Cleaner kicks in.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(4, status.length);
    boolean sawNewBase = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("base_.*")) {
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(1, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_00001"));
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
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    // Before Cleaner, there should be 5 items:
    // 2 original files, 1 original directory, 1 base directory and 1 delta directory
    Assert.assertEquals(5, status.length);
    runCleaner(hiveConf);
    // There should be only 1 directory left: base_xxxxxxx.
    // Original bucket files and delta directory should have been cleaned up.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(1, status.length);
    Assert.assertTrue(status[0].getPath().getName().matches("base_.*"));
    FileStatus[] buckets = fs.listStatus(status[0].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(1, buckets.length);
    Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_00001"));
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 2}, {3, 4}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));
  }

  /**
   * Test the query correctness and directory layout for ACID table conversion
   * 1. Insert a row to Non-ACID table
   * 2. Convert Non-ACID to ACID table
   * 3. Update the existing row in ACID table
   * 4. Perform Major compaction
   * 5. Clean
   * @throws Exception
   */
  @Test
  public void testNonAcidToAcidConversion2() throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;

    // 1. Insert a row to Non-ACID table
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,2)");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
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
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
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

    // 3. Update the existing row in newly-converted ACID table
    runStatementOnDriver("update " + Table.NONACIDORCTBL + " set b=3 where a=1");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    // There should be 2 original bucket files (000000_0 and 000001_0), plus one delta directory
    // and one delete_delta directory. When split-update is enabled, an update event is split into
    // a combination of delete and insert, that generates the delete_delta directory.
    // The delta directory should also have 2 bucket files (bucket_00000 and bucket_00001)
    // and so should the delete_delta directory.
    Assert.assertEquals(4, status.length);
    boolean sawNewDelta = false;
    boolean sawNewDeleteDelta = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("delta_.*")) {
        sawNewDelta = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
      } else if (status[i].getPath().getName().matches("delete_delta_.*")) {
        sawNewDeleteDelta = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
      } else {
        Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
      }
    }
    Assert.assertTrue(sawNewDelta);
    Assert.assertTrue(sawNewDeleteDelta);
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 3}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 4. Perform a major compaction
    runStatementOnDriver("alter table "+ Table.NONACIDORCTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    // There should be 1 new directory: base_0000001.
    // Original bucket files and delta directory should stay until Cleaner kicks in.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(5, status.length);
    boolean sawNewBase = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("base_.*")) {
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_00001"));
      }
    }
    Assert.assertTrue(sawNewBase);
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 3}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 5. Let Cleaner delete obsolete files/dirs
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    // Before Cleaner, there should be 5 items:
    // 2 original files, 1 delta directory, 1 delete_delta directory and 1 base directory
    Assert.assertEquals(5, status.length);
    runCleaner(hiveConf);
    // There should be only 1 directory left: base_0000001.
    // Original bucket files, delta directory and delete_delta should have been cleaned up.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(1, status.length);
    Assert.assertTrue(status[0].getPath().getName().matches("base_.*"));
    FileStatus[] buckets = fs.listStatus(status[0].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
    Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_00001"));
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 3}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));
  }

  /**
   * Test the query correctness and directory layout for ACID table conversion
   * 1. Insert a row to Non-ACID table
   * 2. Convert Non-ACID to ACID table
   * 3. Perform Major compaction
   * 4. Insert a new row to ACID table
   * 5. Perform another Major compaction
   * 6. Clean
   * @throws Exception
   */
  @Test
  public void testNonAcidToAcidConversion3() throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;

    // 1. Insert a row to Non-ACID table
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,2)");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
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

    // 2. Convert NONACIDORCTBL to ACID table with split_update enabled. (txn_props=default)
    runStatementOnDriver("alter table " + Table.NONACIDORCTBL + " SET TBLPROPERTIES ('transactional'='true')");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
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

    // 3. Perform a major compaction
    runStatementOnDriver("alter table "+ Table.NONACIDORCTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    // There should be 1 new directory: base_-9223372036854775808
    // Original bucket files should stay until Cleaner kicks in.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(3, status.length);
    boolean sawNewBase = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("base_.*")) {
        Assert.assertEquals("base_-9223372036854775808", status[i].getPath().getName());
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
        Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
      }
    }
    Assert.assertTrue(sawNewBase);
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 2}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 4. Update the existing row, and insert another row to newly-converted ACID table
    runStatementOnDriver("update " + Table.NONACIDORCTBL + " set b=3 where a=1");
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(3,4)");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Arrays.sort(status);  // make sure delta_0000001_0000001_0000 appears before delta_0000002_0000002_0000
    // There should be 2 original bucket files (000000_0 and 000001_0), a base directory,
    // plus two new delta directories and one delete_delta directory that would be created due to
    // the update statement (remember split-update U=D+I)!
    Assert.assertEquals(6, status.length);
    int numDelta = 0;
    int numDeleteDelta = 0;
    sawNewBase = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("delta_.*")) {
        numDelta++;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Arrays.sort(buckets);
        if (numDelta == 1) {
          Assert.assertEquals("delta_0000024_0000024_0000", status[i].getPath().getName());
          Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
          Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
        } else if (numDelta == 2) {
          Assert.assertEquals("delta_0000025_0000025_0000", status[i].getPath().getName());
          Assert.assertEquals(1, buckets.length);
          Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
        }
      } else if (status[i].getPath().getName().matches("delete_delta_.*")) {
        numDeleteDelta++;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Arrays.sort(buckets);
        if (numDeleteDelta == 1) {
          Assert.assertEquals("delete_delta_0000024_0000024_0000", status[i].getPath().getName());
          Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
          Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
        }
      } else if (status[i].getPath().getName().matches("base_.*")) {
        Assert.assertEquals("base_-9223372036854775808", status[i].getPath().getName());
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
        Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
      } else {
        Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
      }
    }
    Assert.assertEquals(2, numDelta);
    Assert.assertEquals(1, numDeleteDelta);
    Assert.assertTrue(sawNewBase);

    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 3}, {3, 4}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 5. Perform another major compaction
    runStatementOnDriver("alter table "+ Table.NONACIDORCTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    // There should be 1 new base directory: base_0000001
    // Original bucket files, delta directories, delete_delta directories and the
    // previous base directory should stay until Cleaner kicks in.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Arrays.sort(status);
    Assert.assertEquals(7, status.length);
    int numBase = 0;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("base_.*")) {
        numBase++;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Arrays.sort(buckets);
        if (numBase == 1) {
          Assert.assertEquals("base_-9223372036854775808", status[i].getPath().getName());
          Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
          Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
        } else if (numBase == 2) {
          // The new base dir now has two bucket files, since the delta dir has two bucket files
          Assert.assertEquals("base_0000025", status[i].getPath().getName());
          Assert.assertEquals(1, buckets.length);
          Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
        }
      }
    }
    Assert.assertEquals(2, numBase);
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 3}, {3, 4}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 6. Let Cleaner delete obsolete files/dirs
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    // Before Cleaner, there should be 6 items:
    // 2 original files, 2 delta directories, 1 delete_delta directory and 2 base directories
    Assert.assertEquals(7, status.length);
    runCleaner(hiveConf);
    // There should be only 1 directory left: base_0000001.
    // Original bucket files, delta directories and previous base directory should have been cleaned up.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(1, status.length);
    Assert.assertEquals("base_0000025", status[0].getPath().getName());
    FileStatus[] buckets = fs.listStatus(status[0].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Arrays.sort(buckets);
    Assert.assertEquals(1, buckets.length);
    Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 3}, {3, 4}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));
  }
  @Test
  public void testValidTxnsBookkeeping() throws Exception {
    // 1. Run a query against a non-ACID table, and we shouldn't have txn logged in conf
    runStatementOnDriver("select * from " + Table.NONACIDORCTBL);
    String value = hiveConf.get(ValidTxnList.VALID_TXNS_KEY);
    Assert.assertNull("The entry should be null for query that doesn't involve ACID tables", value);
  }

  @Test
  public void testSimpleRead() throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.HIVEFETCHTASKCONVERSION, "more");
    int[][] tableData = {{1,2},{3,3}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + " " + makeValuesClause(tableData));
    int[][] tableData2 = {{5,3}};
    //this will cause next txn to be marked aborted but the data is still written to disk
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEROLLBACKTXN, true);
    runStatementOnDriver("insert into " + Table.ACIDTBL + " " + makeValuesClause(tableData2));
    assert hiveConf.get(ValidTxnList.VALID_TXNS_KEY) == null : "previous txn should've cleaned it";
    //so now if HIVEFETCHTASKCONVERSION were to use a stale value, it would use a
    //ValidTxnList with HWM=MAX_LONG, i.e. include the data for aborted txn
    List<String> rs = runStatementOnDriver("select * from " + Table.ACIDTBL);
    Assert.assertEquals("Extra data", 2, rs.size());
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
   * Test update that hits multiple partitions (i.e. requries dynamic partition insert to process)
   * @throws Exception
   */
  @Test
  public void updateDeletePartitioned() throws Exception {
    int[][] tableData = {{1,2},{3,4},{5,6}};
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p=1) (a,b) " + makeValuesClause(tableData));
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p=2) (a,b) " + makeValuesClause(tableData));
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    txnHandler.compact(new CompactionRequest("default", Table.ACIDTBLPART.name(), CompactionType.MAJOR));
    runWorker(hiveConf);
    runCleaner(hiveConf);
    runStatementOnDriver("update " + Table.ACIDTBLPART + " set b = b + 1 where a = 3");
    txnHandler.compact(new CompactionRequest("default", Table.ACIDTBLPART.toString(), CompactionType.MAJOR));
    runWorker(hiveConf);
    runCleaner(hiveConf);
    List<String> rs = runStatementOnDriver("select p,a,b from " + Table.ACIDTBLPART + " order by p, a, b");
    int[][] expectedData = {{1,1,2},{1,3,5},{1,5,6},{2,1,2},{2,3,5},{2,5,6}};
    Assert.assertEquals("Update " + Table.ACIDTBLPART + " didn't match:", stringifyValues(expectedData), rs);
  }

  /**
   * https://issues.apache.org/jira/browse/HIVE-17391
   */
  @Test
  public void testEmptyInTblproperties() throws Exception {
    runStatementOnDriver("create table t1 " + "(a int, b int) stored as orc TBLPROPERTIES ('serialization.null.format'='', 'transactional'='true')");
    runStatementOnDriver("insert into t1 " + "(a,b) values(1,7),(3,7)");
    runStatementOnDriver("update t1" + " set b = -2 where b = 2");
    runStatementOnDriver("alter table t1 " + " compact 'MAJOR'");
    runWorker(hiveConf);
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history", 1, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0 compaction state", TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(0).getState());
    Assert.assertTrue(resp.getCompacts().get(0).getHadoopJobId().startsWith("job_local"));
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
  private static void checkCompactionState(CompactionsByState expected, CompactionsByState actual) {
    Assert.assertEquals(TxnStore.ATTEMPTED_RESPONSE, expected.attempted, actual.attempted);
    Assert.assertEquals(TxnStore.FAILED_RESPONSE, expected.failed, actual.failed);
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, expected.initiated, actual.initiated);
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, expected.readyToClean, actual.readyToClean);
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, expected.succeeded, actual.succeeded);
    Assert.assertEquals(TxnStore.WORKING_RESPONSE, expected.working, actual.working);
    Assert.assertEquals("total", expected.total, actual.total);
  }
  /**
   * HIVE-12353
   * @throws Exception
   */
  @Test
  public void testInitiatorWithMultipleFailedCompactions() throws Exception {
    testInitiatorWithMultipleFailedCompactionsForVariousTblProperties("'transactional'='true'");
  }

  void testInitiatorWithMultipleFailedCompactionsForVariousTblProperties(String tblProperties) throws Exception {
    String tblName = "hive12353";
    runStatementOnDriver("drop table if exists " + tblName);
    runStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ( " + tblProperties + " )");
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 4);
    for(int i = 0; i < 5; i++) {
      //generate enough delta files so that Initiator can trigger auto compaction
      runStatementOnDriver("insert into " + tblName + " values(" + (i + 1) + ", 'foo'),(" + (i + 2) + ", 'bar'),(" + (i + 3) + ", 'baz')");
    }
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEFAILCOMPACTION, true);

    int numFailedCompactions = hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD);
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    AtomicBoolean stop = new AtomicBoolean(true);
    //create failed compactions
    for(int i = 0; i < numFailedCompactions; i++) {
      //each of these should fail
      txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MINOR));
      runWorker(hiveConf);
    }
    //this should not schedule a new compaction due to prior failures, but will create Attempted entry
    Initiator init = new Initiator();
    init.setThreadId((int)init.getId());
    init.setConf(hiveConf);
    init.init(stop, new AtomicBoolean());
    init.run();
    int numAttemptedCompactions = 1;
    checkCompactionState(new CompactionsByState(numAttemptedCompactions,numFailedCompactions,0,0,0,0,numFailedCompactions + numAttemptedCompactions), countCompacts(txnHandler));

    hiveConf.setTimeVar(HiveConf.ConfVars.COMPACTOR_HISTORY_REAPER_INTERVAL, 10, TimeUnit.MILLISECONDS);
    AcidCompactionHistoryService compactionHistoryService = new AcidCompactionHistoryService();
    compactionHistoryService.setConf(hiveConf);
    compactionHistoryService.run();
    checkCompactionState(new CompactionsByState(numAttemptedCompactions,numFailedCompactions,0,0,0,0,numFailedCompactions + numAttemptedCompactions), countCompacts(txnHandler));

    txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MAJOR));
    runWorker(hiveConf);//will fail
    txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MINOR));
    runWorker(hiveConf);//will fail
    init.run();
    numAttemptedCompactions++;
    init.run();
    numAttemptedCompactions++;
    checkCompactionState(new CompactionsByState(numAttemptedCompactions,numFailedCompactions + 2,0,0,0,0,numFailedCompactions + 2 + numAttemptedCompactions), countCompacts(txnHandler));

    compactionHistoryService.run();
    //COMPACTOR_HISTORY_RETENTION_FAILED failed compacts left (and no other since we only have failed ones here)
    checkCompactionState(new CompactionsByState(
      hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_ATTEMPTED),
      hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED),0,0,0,0,
      hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED) + hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_ATTEMPTED)), countCompacts(txnHandler));

    hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEFAILCOMPACTION, false);
    txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MINOR));
    //at this point "show compactions" should have (COMPACTOR_HISTORY_RETENTION_FAILED) failed + 1 initiated (explicitly by user)
    checkCompactionState(new CompactionsByState(
      hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_ATTEMPTED),
      hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED),1,0,0,0,
      hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED) +
        hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_ATTEMPTED)+ 1), countCompacts(txnHandler));

    runWorker(hiveConf);//will succeed and transition to Initiated->Working->Ready for Cleaning
    checkCompactionState(new CompactionsByState(
      hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_ATTEMPTED),
      hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED),0,1,0,0,
      hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED) +
        hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_ATTEMPTED)+ 1), countCompacts(txnHandler));

    runCleaner(hiveConf); // transition to Success state
    compactionHistoryService.run();
    checkCompactionState(new CompactionsByState(
      hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_ATTEMPTED),
      hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED),0,0,1,0,
      hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED) +
        hiveConf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_ATTEMPTED)+ 1), countCompacts(txnHandler));
  }

  /**
   * Make sure there's no FileSystem$Cache$Key leak due to UGI use
   * @throws Exception
   */
  @Test
  public void testFileSystemUnCaching() throws Exception {
    int cacheSizeBefore;
    int cacheSizeAfter;

    // get the size of cache BEFORE
    cacheSizeBefore = getFileSystemCacheSize();

    // Insert a row to ACID table
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(1,2)");

    // Perform a major compaction
    runStatementOnDriver("alter table " + Table.ACIDTBL + " compact 'major'");
    runWorker(hiveConf);
    runCleaner(hiveConf);

    // get the size of cache AFTER
    cacheSizeAfter = getFileSystemCacheSize();

    Assert.assertEquals(cacheSizeBefore, cacheSizeAfter);
  }

  private int getFileSystemCacheSize() throws Exception {
    try {
      Field cache = FileSystem.class.getDeclaredField("CACHE");
      cache.setAccessible(true);
      Object o = cache.get(null); // FileSystem.CACHE

      Field mapField = o.getClass().getDeclaredField("map");
      mapField.setAccessible(true);
      Map map = (HashMap)mapField.get(o); // FileSystem.CACHE.map

      return map.size();
    } catch (NoSuchFieldException e) {
      System.out.println(e);
    }
    return 0;
  }

  private static class CompactionsByState {
    private int attempted;
    private int failed;
    private int initiated;
    private int readyToClean;
    private int succeeded;
    private int working;
    private int total;
    CompactionsByState() {
      this(0,0,0,0,0,0,0);
    }
    CompactionsByState(int attempted, int failed, int initiated, int readyToClean, int succeeded, int working, int total) {
      this.attempted = attempted;
      this.failed = failed;
      this.initiated = initiated;
      this.readyToClean = readyToClean;
      this.succeeded = succeeded;
      this.working = working;
      this.total = total;
    }
  }
  private static CompactionsByState countCompacts(TxnStore txnHandler) throws MetaException {
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    CompactionsByState compactionsByState = new CompactionsByState();
    compactionsByState.total = resp.getCompactsSize();
    for(ShowCompactResponseElement compact : resp.getCompacts()) {
      if(TxnStore.FAILED_RESPONSE.equals(compact.getState())) {
        compactionsByState.failed++;
      }
      else if(TxnStore.CLEANING_RESPONSE.equals(compact.getState())) {
        compactionsByState.readyToClean++;
      }
      else if(TxnStore.INITIATED_RESPONSE.equals(compact.getState())) {
        compactionsByState.initiated++;
      }
      else if(TxnStore.SUCCEEDED_RESPONSE.equals(compact.getState())) {
        compactionsByState.succeeded++;
      }
      else if(TxnStore.WORKING_RESPONSE.equals(compact.getState())) {
        compactionsByState.working++;
      }
      else if(TxnStore.ATTEMPTED_RESPONSE.equals(compact.getState())) {
        compactionsByState.attempted++;
      }
      else {
        throw new IllegalStateException("Unexpected state: " + compact.getState());
      }
    }
    return compactionsByState;
  }
  public static void runWorker(HiveConf hiveConf) throws MetaException {
    AtomicBoolean stop = new AtomicBoolean(true);
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(hiveConf);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();
  }
  public static void runCleaner(HiveConf hiveConf) throws MetaException {
    AtomicBoolean stop = new AtomicBoolean(true);
    Cleaner t = new Cleaner();
    t.setThreadId((int) t.getId());
    t.setConf(hiveConf);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();
  }

  /**
   * HIVE-12352 has details
   * @throws Exception
   */
  @Test
  public void writeBetweenWorkerAndCleaner() throws Exception {
    writeBetweenWorkerAndCleanerForVariousTblProperties("'transactional'='true'");
  }

  protected void writeBetweenWorkerAndCleanerForVariousTblProperties(String tblProperties) throws Exception {
    String tblName = "hive12352";
    runStatementOnDriver("drop table if exists " + tblName);
    runStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ( " + tblProperties + " )");

    //create some data
    runStatementOnDriver("insert into " + tblName + " values(1, 'foo'),(2, 'bar'),(3, 'baz')");
    runStatementOnDriver("update " + tblName + " set b = 'blah' where a = 3");

    //run Worker to execute compaction
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MINOR));
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(hiveConf);
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
    c.setConf(hiveConf);
    c.init(stop, new AtomicBoolean());
    c.run();

    //this seems odd, but we wan to make sure that to run CompactionTxnHandler.cleanEmptyAbortedTxns()
    Initiator i = new Initiator();
    i.setThreadId((int)i.getId());
    i.setConf(hiveConf);
    i.init(stop, new AtomicBoolean());
    i.run();

    //check that aborted operation didn't become committed
    Assert.assertEquals("", expected,
      runStatementOnDriver("select a,b from " + tblName + " order by a"));
  }
  /**
   * Simulate the scenario when a heartbeat failed due to client errors such as no locks or no txns being found.
   * When a heartbeat fails, the query should be failed too.
   * @throws Exception
   */
  @Test
  public void testFailHeartbeater() throws Exception {
    // Fail heartbeater, so that we can get a RuntimeException from the query.
    // More specifically, it's the original IOException thrown by either MR's or Tez's progress monitoring loop.
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEFAILHEARTBEATER, true);
    Exception exception = null;
    try {
      runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(new int[][]{{1, 2}, {3, 4}}));
    } catch (RuntimeException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);
    Assert.assertTrue(exception.getMessage().contains("HIVETESTMODEFAILHEARTBEATER=true"));
  }

  @Test
  public void testOpenTxnsCounter() throws Exception {
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_MAX_OPEN_TXNS, 3);
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_COUNT_OPEN_TXNS_INTERVAL, 10, TimeUnit.MILLISECONDS);
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    OpenTxnsResponse openTxnsResponse = txnHandler.openTxns(new OpenTxnRequest(3, "me", "localhost"));

    AcidOpenTxnsCounterService openTxnsCounterService = new AcidOpenTxnsCounterService();
    openTxnsCounterService.setConf(hiveConf);
    openTxnsCounterService.run();  // will update current number of open txns to 3

    MetaException exception = null;
    // This should fail once it finds out the threshold has been reached
    try {
      txnHandler.openTxns(new OpenTxnRequest(1, "you", "localhost"));
    } catch (MetaException e) {
      exception = e;
    }
    Assert.assertNotNull("Opening new transaction shouldn't be allowed", exception);
    Assert.assertTrue(exception.getMessage().equals("Maximum allowed number of open transactions has been reached. See hive.max.open.txns."));

    // After committing the initial txns, and updating current number of open txns back to 0,
    // new transactions should be allowed to open
    for (long txnid : openTxnsResponse.getTxn_ids()) {
      txnHandler.commitTxn(new CommitTxnRequest(txnid));
    }
    openTxnsCounterService.run();  // will update current number of open txns back to 0
    exception = null;
    try {
      txnHandler.openTxns(new OpenTxnRequest(1, "him", "localhost"));
    } catch (MetaException e) {
      exception = e;
    }
    Assert.assertNull(exception);
  }

  @Test
  public void testCompactWithDelete() throws Exception {
    int[][] tableData = {{1,2},{3,4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData));
    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MAJOR'");
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(hiveConf);
    AtomicBoolean stop = new AtomicBoolean();
    AtomicBoolean looped = new AtomicBoolean();
    stop.set(true);
    t.init(stop, looped);
    t.run();
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where b = 4");
    runStatementOnDriver("update " + Table.ACIDTBL + " set b = -2 where b = 2");
    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MINOR'");
    t.run();
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history", 2, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0 compaction state", TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(0).getState());
    Assert.assertEquals("Unexpected 1 compaction state", TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(1).getState());
  }

  /**
   * make sure Aborted txns don't red-flag a base_xxxx (HIVE-14350)
   */
  @Test
  public void testNoHistory() throws Exception {
    int[][] tableData = {{1,2},{3,4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData));
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEROLLBACKTXN, true);
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData));
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEROLLBACKTXN, false);

    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    runCleaner(hiveConf);
    runStatementOnDriver("select count(*) from " + Table.ACIDTBL);
  }

  @Test
  public void testACIDwithSchemaEvolutionAndCompaction() throws Exception {
    testACIDwithSchemaEvolutionForVariousTblProperties("'transactional'='true'");
  }

  protected void testACIDwithSchemaEvolutionForVariousTblProperties(String tblProperties) throws Exception {
    String tblName = "acidWithSchemaEvol";
    int numBuckets = 1;
    runStatementOnDriver("drop table if exists " + tblName);
    runStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO " + numBuckets +" BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ( " + tblProperties + " )");

    // create some data
    runStatementOnDriver("insert into " + tblName + " values(1, 'foo'),(2, 'bar'),(3, 'baz')");
    runStatementOnDriver("update " + tblName + " set b = 'blah' where a = 3");

    // apply schema evolution by adding some columns
    runStatementOnDriver("alter table " + tblName + " add columns(c int, d string)");

    // insert some data in new schema
    runStatementOnDriver("insert into " + tblName + " values(4, 'acid', 100, 'orc'),"
        + "(5, 'llap', 200, 'tez')");

    // update old data with values for the new schema columns
    runStatementOnDriver("update " + tblName + " set d = 'hive' where a <= 3");
    runStatementOnDriver("update " + tblName + " set c = 999 where a <= 3");

    // read the entire data back and see if did everything right
    List<String> rs = runStatementOnDriver("select * from " + tblName + " order by a");
    String[] expectedResult = { "1\tfoo\t999\thive", "2\tbar\t999\thive", "3\tblah\t999\thive", "4\tacid\t100\torc", "5\tllap\t200\ttez" };
    Assert.assertEquals(Arrays.asList(expectedResult), rs);

    // now compact and see if compaction still preserves the data correctness
    runStatementOnDriver("alter table "+ tblName + " compact 'MAJOR'");
    runWorker(hiveConf);
    runCleaner(hiveConf); // Cleaner would remove the obsolete files.

    // Verify that there is now only 1 new directory: base_xxxxxxx and the rest have have been cleaned.
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" + tblName.toString().toLowerCase()),
        FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(1, status.length);
    boolean sawNewBase = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("base_.*")) {
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(numBuckets, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_00000"));
      }
    }
    Assert.assertTrue(sawNewBase);

    rs = runStatementOnDriver("select * from " + tblName + " order by a");
    Assert.assertEquals(Arrays.asList(expectedResult), rs);
  }

  @Test
  public void testETLSplitStrategyForACID() throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.HIVE_ORC_SPLIT_STRATEGY, "ETL");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVEOPTINDEXFILTER, true);
    runStatementOnDriver("insert into " + Table.ACIDTBL + " values(1,2)");
    runStatementOnDriver("alter table " + Table.ACIDTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    List<String> rs = runStatementOnDriver("select * from " +  Table.ACIDTBL  + " where a = 1");
    int[][] resultData = new int[][] {{1,2}};
    Assert.assertEquals(stringifyValues(resultData), rs);
  }

  @Test
  public void testAcidWithSchemaEvolution() throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.HIVE_ORC_SPLIT_STRATEGY, "ETL");
    String tblName = "acidTblWithSchemaEvol";
    runStatementOnDriver("drop table if exists " + tblName);
    runStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC TBLPROPERTIES ('transactional'='true')");

    runStatementOnDriver("INSERT INTO " + tblName + " VALUES (1, 'foo'), (2, 'bar')");

    // Major compact to create a base that has ACID schema.
    runStatementOnDriver("ALTER TABLE " + tblName + " COMPACT 'MAJOR'");
    runWorker(hiveConf);

    // Alter table for perform schema evolution.
    runStatementOnDriver("ALTER TABLE " + tblName + " ADD COLUMNS(c int)");

    // Validate there is an added NULL for column c.
    List<String> rs = runStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a");
    String[] expectedResult = { "1\tfoo\tNULL", "2\tbar\tNULL" };
    Assert.assertEquals(Arrays.asList(expectedResult), rs);
  }
  /**
   * Test that ACID works with multi-insert statement
   */
  @Test
  public void testMultiInsertStatement() throws Exception {
    int[][] sourceValsOdd = {{5,5},{11,11}};
    int[][] sourceValsEven = {{2,2}};
    //populate source
    runStatementOnDriver("insert into " + Table.NONACIDPART2 + " PARTITION(p2='odd') " + makeValuesClause(sourceValsOdd));
    runStatementOnDriver("insert into " + Table.NONACIDPART2 + " PARTITION(p2='even') " + makeValuesClause(sourceValsEven));
    int[][] targetValsOdd = {{5,6},{7,8}};
    int[][] targetValsEven = {{2,1},{4,3}};
    //populate target
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " PARTITION(p='odd') " + makeValuesClause(targetValsOdd));
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " PARTITION(p='even') " + makeValuesClause(targetValsEven));
    List<String> r = runStatementOnDriver("select a,b from " + Table.ACIDTBLPART + " order by a,b");
    int[][] targetVals = {{2,1},{4,3},{5,6},{7,8}};
    Assert.assertEquals(stringifyValues(targetVals), r);
    //currently multi-insrt doesn't allow same table/partition in > 1 output branch
    String s = "from " + Table.ACIDTBLPART + "  target right outer join " +
      Table.NONACIDPART2 + " source on target.a = source.a2 " +
      " INSERT INTO TABLE " + Table.ACIDTBLPART + " PARTITION(p='even') select source.a2, source.b2 where source.a2=target.a " +
      " insert into table " + Table.ACIDTBLPART + " PARTITION(p='odd') select source.a2,source.b2 where target.a is null";
    //r = runStatementOnDriver("explain formatted " + s);
    //LOG.info("Explain formatted: " + r.toString());
    runStatementOnDriver(s);
    r = runStatementOnDriver("select a,b from " + Table.ACIDTBLPART + " where p='even' order by a,b");
    int[][] rExpected = {{2,1},{2,2},{4,3},{5,5}};
    Assert.assertEquals(stringifyValues(rExpected), r);
    r = runStatementOnDriver("select a,b from " + Table.ACIDTBLPART + " where p='odd' order by a,b");
    int[][] rExpected2 = {{5,6},{7,8},{11,11}};
    Assert.assertEquals(stringifyValues(rExpected2), r);
  }
  /**
   * check that we can specify insert columns
   *
   * Need to figure out semantics: what if a row from base expr ends up in both Update and Delete clauses we'll write
   * Update event to 1 delta and Delete to another.  Given that we collapse events for same current txn for different stmt ids
   * to the latest one, delete will win.
   * In Acid 2.0 we'll end up with 2 Delete events for the same PK.  Logically should be OK, but may break Vectorized reader impl.... need to check
   *
   * 1:M from target to source results in ambiguous write to target - SQL Standard expects an error.  (I have an argument on how
   * to solve this with minor mods to Join operator written down somewhere)
   *
   * Only need 1 Stats task for MERGE (currently we get 1 per branch).
   * Should also eliminate Move task - that's a general ACID task
   */
  private void logResuts(List<String> r, String header, String prefix) {
    LOG.info(prefix + " " + header);
    StringBuilder sb = new StringBuilder();
    int numLines = 0;
    for(String line : r) {
      numLines++;
      sb.append(prefix).append(line).append("\n");
    }
    LOG.info(sb.toString());
    LOG.info(prefix + " Printed " + numLines + " lines");
  }


  /**
   * This tests that we handle non-trivial ON clause correctly
   * @throws Exception
   */
  @Test
  public void testMerge() throws Exception {
    int[][] baseValsOdd = {{5,5},{11,11}};
    runStatementOnDriver("insert into " + Table.NONACIDPART2 + " PARTITION(p2='odd') " + makeValuesClause(baseValsOdd));
    int[][] vals = {{2,1},{4,3},{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + " " + makeValuesClause(vals));
    List<String> r = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals(stringifyValues(vals), r);
    String query = "merge into " + Table.ACIDTBL +
      " using " + Table.NONACIDPART2 + " source ON " + Table.ACIDTBL + ".a = a2 and b + 1 = source.b2 + 1 " +
      "WHEN MATCHED THEN UPDATE set b = source.b2 " +
      "WHEN NOT MATCHED THEN INSERT VALUES(source.a2, source.b2)";
    runStatementOnDriver(query);

    r = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] rExpected = {{2,1},{4,3},{5,5},{5,6},{7,8},{11,11}};
    Assert.assertEquals(stringifyValues(rExpected), r);
  }
  @Test
  public void testMergeWithPredicate() throws Exception {
    int[][] baseValsOdd = {{2,2},{5,5},{8,8},{11,11}};
    runStatementOnDriver("insert into " + Table.NONACIDPART2 + " PARTITION(p2='odd') " + makeValuesClause(baseValsOdd));
    int[][] vals = {{2,1},{4,3},{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + " " + makeValuesClause(vals));
    List<String> r = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals(stringifyValues(vals), r);
    String query = "merge into " + Table.ACIDTBL +
      " t using " + Table.NONACIDPART2 + " s ON t.a = s.a2 " +
      "WHEN MATCHED AND t.b between 1 and 3 THEN UPDATE set b = s.b2 " +
      "WHEN NOT MATCHED and s.b2 >= 8 THEN INSERT VALUES(s.a2, s.b2)";
    runStatementOnDriver(query);

    r = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] rExpected = {{2,2},{4,3},{5,6},{7,8},{8,8},{11,11}};
    Assert.assertEquals(stringifyValues(rExpected), r);
    assertUniqueID(Table.ACIDTBL);
  }

  /**
   * Test combines update + insert clauses
   * @throws Exception
   */
  @Test
  public void testMerge2() throws Exception {
    int[][] baseValsOdd = {{5,5},{11,11}};
    int[][] baseValsEven = {{2,2},{4,44}};
    runStatementOnDriver("insert into " + Table.NONACIDPART2 + " PARTITION(p2='odd') " + makeValuesClause(baseValsOdd));
    runStatementOnDriver("insert into " + Table.NONACIDPART2 + " PARTITION(p2='even') " + makeValuesClause(baseValsEven));
    int[][] vals = {{2,1},{4,3},{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + " " + makeValuesClause(vals));
    List<String> r = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals(stringifyValues(vals), r);
    String query = "merge into " + Table.ACIDTBL +
      " using " + Table.NONACIDPART2 + " source ON " + Table.ACIDTBL + ".a = source.a2 " +
      "WHEN MATCHED THEN UPDATE set b = source.b2 " +
      "WHEN NOT MATCHED THEN INSERT VALUES(source.a2, source.b2) ";//AND b < 1
    r = runStatementOnDriver(query);
    //r = runStatementOnDriver("explain  " + query);
    //logResuts(r, "Explain logical1", "");

    r = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] rExpected = {{2,2},{4,44},{5,5},{7,8},{11,11}};
    Assert.assertEquals(stringifyValues(rExpected), r);
    assertUniqueID(Table.ACIDTBL);
  }

  /**
   * test combines delete + insert clauses
   * @throws Exception
   */
  @Test
  public void testMerge3() throws Exception {
    int[][] baseValsOdd = {{5,5},{11,11}};
    int[][] baseValsEven = {{2,2},{4,44}};
    runStatementOnDriver("insert into " + Table.NONACIDPART2 + " PARTITION(p2='odd') " + makeValuesClause(baseValsOdd));
    runStatementOnDriver("insert into " + Table.NONACIDPART2 + " PARTITION(p2='even') " + makeValuesClause(baseValsEven));
    int[][] vals = {{2,1},{4,3},{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + " " + makeValuesClause(vals));
    List<String> r = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals(stringifyValues(vals), r);
    String query = "merge into " + Table.ACIDTBL +
      " using " + Table.NONACIDPART2 + " source ON " + Table.ACIDTBL + ".a = source.a2 " +
      "WHEN MATCHED THEN DELETE " +
      "WHEN NOT MATCHED THEN INSERT VALUES(source.a2, source.b2) ";
    runStatementOnDriver(query);

    r = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] rExpected = {{7,8},{11,11}};
    Assert.assertEquals(stringifyValues(rExpected), r);
  }
  @Test
  public void testMultiInsert() throws Exception {
    runStatementOnDriver("create temporary table if not exists data1 (x int)");
    runStatementOnDriver("insert into data1 values (1),(2),(1)");
    d.destroy();
    hiveConf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    d = new Driver(hiveConf);

    runStatementOnDriver(" from data1 " +
      "insert into " + Table.ACIDTBLPART + " partition(p) select 0, 0, 'p' || x  "
      +
      "insert into " + Table.ACIDTBLPART + " partition(p='p1') select 0, 1");
    /**
     * Using {@link BucketCodec.V0} the output
     * is missing 1 of the (p1,0,1) rows because they have the same ROW__ID and only differ by
     * StatementId so {@link org.apache.hadoop.hive.ql.io.orc.OrcRawRecordMerger} skips one.
     * With split update (and V0), the data is read correctly (insert deltas are now the base) but we still
     * should get duplicate ROW__IDs.
     */
    List<String> r = runStatementOnDriver("select p,a,b from " + Table.ACIDTBLPART + " order by p, a, b");
    Assert.assertEquals("[p1\t0\t0, p1\t0\t0, p1\t0\t1, p1\t0\t1, p1\t0\t1, p2\t0\t0]", r.toString());
    assertUniqueID(Table.ACIDTBLPART);
    /**
     * this delete + select covers VectorizedOrcAcidRowBatchReader
     */
    runStatementOnDriver("delete from " + Table.ACIDTBLPART);
    r = runStatementOnDriver("select p,a,b from " + Table.ACIDTBLPART + " order by p, a, b");
    Assert.assertEquals("[]", r.toString());
  }
  /**
   * Investigating DP and WriteEntity, etc
   * @throws Exception
   */
  @Test
  @Ignore
  public void testDynamicPartitions() throws Exception {
    d.destroy();
    hiveConf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    //In DbTxnManager.acquireLocks() we have
    // 1 ReadEntity: default@values__tmp__table__1
    // 1 WriteEntity: default@acidtblpart Type=TABLE WriteType=INSERT isDP=false
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p1'),(2,2,'p1'),(3,3,'p1'),(4,4,'p2')");

    List<String> r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBLPART);
    Assert.assertEquals("4", r1.get(0));
    //In DbTxnManager.acquireLocks() we have
    // 2 ReadEntity: [default@acidtblpart@p=p1, default@acidtblpart]
    // 1 WriteEntity: default@acidtblpart Type=TABLE WriteType=INSERT isDP=false
    //todo: side note on the above: LockRequestBuilder combines the both default@acidtblpart entries to 1
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) select * from " + Table.ACIDTBLPART + " where p='p1'");

    //In DbTxnManager.acquireLocks() we have
    // 2 ReadEntity: [default@acidtblpart@p=p1, default@acidtblpart]
    // 1 WriteEntity: default@acidtblpart@p=p2 Type=PARTITION WriteType=INSERT isDP=false
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p2') select a,b from " + Table.ACIDTBLPART + " where p='p1'");

    //In UpdateDeleteSemanticAnalyzer, after super analyze
    // 3 ReadEntity: [default@acidtblpart, default@acidtblpart@p=p1, default@acidtblpart@p=p2]
    // 1 WriteEntity: [default@acidtblpart TABLE/INSERT]
    //after UDSA
    // Read [default@acidtblpart, default@acidtblpart@p=p1, default@acidtblpart@p=p2]
    // Write [default@acidtblpart@p=p1, default@acidtblpart@p=p2] - PARTITION/UPDATE, PARTITION/UPDATE
    //todo: Why acquire per partition locks - if you have many partitions that's hugely inefficient.
    //could acquire 1 table level Shared_write intead
    runStatementOnDriver("update " + Table.ACIDTBLPART + " set b = 1");

    //In UpdateDeleteSemanticAnalyzer, after super analyze
    // Read [default@acidtblpart, default@acidtblpart@p=p1]
    // Write default@acidtblpart TABLE/INSERT
    //after UDSA
    // Read [default@acidtblpart, default@acidtblpart@p=p1]
    // Write [default@acidtblpart@p=p1] PARTITION/UPDATE
    //todo: this causes a Read lock on the whole table - clearly overkill
    //for Update/Delete we always write exactly (at most actually) the partitions we read
    runStatementOnDriver("update " + Table.ACIDTBLPART + " set b = 1 where p='p1'");
  }
  @Test
  public void testDynamicPartitionsMerge() throws Exception {
    d.destroy();
    hiveConf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p1'),(2,2,'p1'),(3,3,'p1'),(4,4,'p2')");

    List<String> r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBLPART);
    Assert.assertEquals("4", r1.get(0));
    int[][] sourceVals = {{2,15},{4,44},{5,5},{11,11}};
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + " " + makeValuesClause(sourceVals));
    runStatementOnDriver("merge into " + Table.ACIDTBLPART + " using " + Table.NONACIDORCTBL +
      " as s ON " + Table.ACIDTBLPART + ".a = s.a " +
      "when matched then update set b = s.b " +
      "when not matched then insert values(s.a, s.b, 'new part')");
    r1 = runStatementOnDriver("select p,a,b from " + Table.ACIDTBLPART + " order by p, a, b");
    String result= r1.toString();
    Assert.assertEquals("[new part\t5\t5, new part\t11\t11, p1\t1\t1, p1\t2\t15, p1\t3\t3, p2\t4\t44]", result);
    //note: inserts go into 'new part'... so this won't fail
    assertUniqueID(Table.ACIDTBLPART);
  }
  /**
   * Using nested partitions and thus DummyPartition
   * @throws Exception
   */
  @Test
  public void testDynamicPartitionsMerge2() throws Exception {
    d.destroy();
    hiveConf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    int[][] targetVals = {{1,1,1},{2,2,2},{3,3,1},{4,4,2}};
    runStatementOnDriver("insert into " + Table.ACIDNESTEDPART + " partition(p=1,q) " + makeValuesClause(targetVals));

    List<String> r1 = runStatementOnDriver("select count(*) from " + Table.ACIDNESTEDPART);
    Assert.assertEquals("4", r1.get(0));
    int[][] sourceVals = {{2,15},{4,44},{5,5},{11,11}};
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + " " + makeValuesClause(sourceVals));
    runStatementOnDriver("merge into " + Table.ACIDNESTEDPART + " using " + Table.NONACIDORCTBL +
      " as s ON " + Table.ACIDNESTEDPART + ".a = s.a " +
      "when matched then update set b = s.b " +
      "when not matched then insert values(s.a, s.b, 3,4)");
    r1 = runStatementOnDriver("select p,q,a,b from " + Table.ACIDNESTEDPART + " order by p,q, a, b");
    Assert.assertEquals(stringifyValues(new int[][] {{1,1,1,1},{1,1,3,3},{1,2,2,15},{1,2,4,44},{3,4,5,5},{3,4,11,11}}), r1);
    //insert of merge lands in part (3,4) - no updates land there
    assertUniqueID(Table.ACIDNESTEDPART);
  }
  @Ignore("Covered elsewhere")
  @Test
  public void testMergeAliasedTarget() throws Exception {
    int[][] baseValsOdd = {{2,2},{4,44},{5,5},{11,11}};
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + " " + makeValuesClause(baseValsOdd));
    int[][] vals = {{2,1},{4,3},{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + " " + makeValuesClause(vals));
    String query = "merge into " + Table.ACIDTBL +
      " as target using " + Table.NONACIDORCTBL + " source ON target.a = source.a " +
      "WHEN MATCHED THEN update set b = 0 " +
      "WHEN NOT MATCHED THEN INSERT VALUES(source.a, source.b) ";
    runStatementOnDriver(query);

    List<String> r = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] rExpected = {{2,0},{4,0},{5,0},{7,8},{11,11}};
    Assert.assertEquals(stringifyValues(rExpected), r);
  }

  @Test
  @Ignore("Values clause with table constructor not yet supported")
  public void testValuesSource() throws Exception {
    int[][] targetVals = {{2,1},{4,3},{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + " " + makeValuesClause(targetVals));
    String query = "merge into " + Table.ACIDTBL +
      " as t using (select * from (values (2,2),(4,44),(5,5),(11,11)) as F(a,b)) s ON t.a = s.a " +
      "WHEN MATCHED and s.a < 5 THEN DELETE " +
      "WHEN MATCHED AND s.a < 3 THEN update set b = 0 " +
      "WHEN NOT MATCHED THEN INSERT VALUES(s.a, s.b) ";
    runStatementOnDriver(query);

    List<String> r = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] rExpected = {{5,6},{7,8},{11,11}};
    Assert.assertEquals(stringifyValues(rExpected), r);
  }

  @Test
  public void testBucketCodec() throws Exception {
    d.destroy();
    //insert data in "legacy" format
    hiveConf.setIntVar(HiveConf.ConfVars.TESTMODE_BUCKET_CODEC_VERSION, 0);
    d = new Driver(hiveConf);

    int[][] targetVals = {{2,1},{4,3},{5,6},{7,8}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + " " + makeValuesClause(targetVals));

    d.destroy();
    hiveConf.setIntVar(HiveConf.ConfVars.TESTMODE_BUCKET_CODEC_VERSION, 1);
    d = new Driver(hiveConf);
    //do some operations with new format
    runStatementOnDriver("update " + Table.ACIDTBL + " set b=11 where a in (5,7)");
    runStatementOnDriver("insert into " + Table.ACIDTBL + " values(11,11)");
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where a = 7");

    //make sure we get the right data back before/after compactions
    List<String> r = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] rExpected = {{2,1},{4,3},{5,11},{11,11}};
    Assert.assertEquals(stringifyValues(rExpected), r);

    runStatementOnDriver("ALTER TABLE " + Table.ACIDTBL + " COMPACT 'MINOR'");
    runWorker(hiveConf);

    r = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals(stringifyValues(rExpected), r);

    runStatementOnDriver("ALTER TABLE " + Table.ACIDTBL + " COMPACT 'MAJOR'");
    runWorker(hiveConf);

    r = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    Assert.assertEquals(stringifyValues(rExpected), r);
  }
  /**
   * Test the scenario when IOW comes in before a MAJOR compaction happens
   * @throws Exception
   */
  @Test
  public void testInsertOverwrite1() throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;

    // 1. Insert two rows to an ACID table
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(1,2)");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(3,4)");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.ACIDTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    // There should be 2 delta dirs in the location
    Assert.assertEquals(2, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("delta_.*"));
    }

    // 2. INSERT OVERWRITE
    // Prepare data for the source table
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(5,6),(7,8)");
    // Insert overwrite ACID table from source table
    runStatementOnDriver("insert overwrite table " + Table.ACIDTBL + " select a,b from " + Table.NONACIDORCTBL);
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.ACIDTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    // There should be 2 delta dirs, plus a base dir in the location
    Assert.assertEquals(3, status.length);
    boolean sawBase = false;
    String baseDir = "";
    int deltaCount = 0;
    for (int i = 0; i < status.length; i++) {
      String dirName = status[i].getPath().getName();
      if (dirName.matches("delta_.*")) {
        deltaCount++;
      } else {
        sawBase = true;
        baseDir = dirName;
        Assert.assertTrue(baseDir.matches("base_.*"));
      }
    }
    Assert.assertEquals(2, deltaCount);
    Assert.assertTrue(sawBase);
    // Verify query result
    List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a");
    int [][] resultData = new int[][] {{5,6},{7,8}};
    Assert.assertEquals(stringifyValues(resultData), rs);

    // 3. Perform a major compaction. Nothing should change. Both deltas and base dirs should have the same name.
    // Re-verify directory layout and query result by using the same logic as above
    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.ACIDTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    // There should be 2 delta dirs, plus a base dir in the location
    Assert.assertEquals(3, status.length);
    sawBase = false;
    deltaCount = 0;
    for (int i = 0; i < status.length; i++) {
      String dirName = status[i].getPath().getName();
      if (dirName.matches("delta_.*")) {
        deltaCount++;
      } else {
        sawBase = true;
        Assert.assertTrue(dirName.matches("base_.*"));
        Assert.assertEquals(baseDir, dirName);
      }
    }
    Assert.assertEquals(2, deltaCount);
    Assert.assertTrue(sawBase);
    // Verify query result
    rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a");
    Assert.assertEquals(stringifyValues(resultData), rs);

    // 4. Run Cleaner. It should remove the 2 delta dirs.
    runCleaner(hiveConf);
    // There should be only 1 directory left: base_xxxxxxx.
    // The delta dirs should have been cleaned up.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.ACIDTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(1, status.length);
    Assert.assertTrue(status[0].getPath().getName().matches("base_.*"));
    Assert.assertEquals(baseDir, status[0].getPath().getName());
    // Verify query result
    rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a");
    Assert.assertEquals(stringifyValues(resultData), rs);
  }

  /**
   * Test the scenario when IOW comes in after a MAJOR compaction happens
   * @throws Exception
   */
  @Test
  public void testInsertOverwrite2() throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;

    // 1. Insert two rows to an ACID table
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(1,2)");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(3,4)");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.ACIDTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    // There should be 2 delta dirs in the location
    Assert.assertEquals(2, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("delta_.*"));
    }

    // 2. Perform a major compaction. There should be an extra base dir now.
    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.ACIDTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    // There should be 2 delta dirs, plus a base dir in the location
    Assert.assertEquals(3, status.length);
    boolean sawBase = false;
    int deltaCount = 0;
    for (int i = 0; i < status.length; i++) {
      String dirName = status[i].getPath().getName();
      if (dirName.matches("delta_.*")) {
        deltaCount++;
      } else {
        sawBase = true;
        Assert.assertTrue(dirName.matches("base_.*"));
      }
    }
    Assert.assertEquals(2, deltaCount);
    Assert.assertTrue(sawBase);
    // Verify query result
    int [][] resultData = new int[][] {{1,2},{3,4}};
    List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a");
    Assert.assertEquals(stringifyValues(resultData), rs);

    // 3. INSERT OVERWRITE
    // Prepare data for the source table
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(5,6),(7,8)");
    // Insert overwrite ACID table from source table
    runStatementOnDriver("insert overwrite table " + Table.ACIDTBL + " select a,b from " + Table.NONACIDORCTBL);
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.ACIDTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    // There should be 2 delta dirs, plus 2 base dirs in the location
    Assert.assertEquals(4, status.length);
    int baseCount = 0;
    deltaCount = 0;
    for (int i = 0; i < status.length; i++) {
      String dirName = status[i].getPath().getName();
      if (dirName.matches("delta_.*")) {
        deltaCount++;
      } else {
        baseCount++;
      }
    }
    Assert.assertEquals(2, deltaCount);
    Assert.assertEquals(2, baseCount);
    // Verify query result
    resultData = new int[][] {{5,6},{7,8}};
    rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a");
    Assert.assertEquals(stringifyValues(resultData), rs);

    // 4. Perform another major compaction. Nothing should change. Both deltas and  both base dirs
    // should have the same name.
    // Re-verify directory layout and query result by using the same logic as above
    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.ACIDTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    // There should be 2 delta dirs, plus 2 base dirs in the location
    Assert.assertEquals(4, status.length);
    baseCount = 0;
    deltaCount = 0;
    for (int i = 0; i < status.length; i++) {
      String dirName = status[i].getPath().getName();
      if (dirName.matches("delta_.*")) {
        deltaCount++;
      } else {
        Assert.assertTrue(dirName.matches("base_.*"));
        baseCount++;
      }
    }
    Assert.assertEquals(2, deltaCount);
    Assert.assertEquals(2, baseCount);
    // Verify query result
    rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a");
    Assert.assertEquals(stringifyValues(resultData), rs);

    // 5. Run Cleaner. It should remove the 2 delta dirs and 1 old base dir.
    runCleaner(hiveConf);
    // There should be only 1 directory left: base_xxxxxxx.
    // The delta dirs should have been cleaned up.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.ACIDTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(1, status.length);
    Assert.assertTrue(status[0].getPath().getName().matches("base_.*"));
    // Verify query result
    rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a");
    Assert.assertEquals(stringifyValues(resultData), rs);
  }

  /**
   * Test compaction for Micro-managed table
   * 1. Regular compaction shouldn't impact any valid subdirectories of MM tables
   * 2. Compactions will only remove subdirectories for aborted transactions of MM tables, if any
   * @throws Exception
   */
  @Test
  public void testMmTableCompaction() throws Exception {
    // 1. Insert some rows into MM table
    runStatementOnDriver("insert into " + Table.MMTBL + "(a,b) values(1,2)");
    runStatementOnDriver("insert into " + Table.MMTBL + "(a,b) values(3,4)");
    // There should be 2 delta directories
    verifyDirAndResult(2);

    // 2. Perform a MINOR compaction. Since nothing was aborted, subdirs should stay.
    runStatementOnDriver("alter table "+ Table.MMTBL + " compact 'MINOR'");
    runWorker(hiveConf);
    verifyDirAndResult(2);

    // 3. Let a transaction be aborted
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEROLLBACKTXN, true);
    runStatementOnDriver("insert into " + Table.MMTBL + "(a,b) values(5,6)");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEROLLBACKTXN, false);
    // There should be 3 delta directories. The new one is the aborted one.
    verifyDirAndResult(3);

    // 4. Perform a MINOR compaction again. This time it will remove the subdir for aborted transaction.
    runStatementOnDriver("alter table "+ Table.MMTBL + " compact 'MINOR'");
    runWorker(hiveConf);
    // The worker should remove the subdir for aborted transaction
    verifyDirAndResult(2);

    // 5. Run Cleaner. Shouldn't impact anything.
    runCleaner(hiveConf);
    verifyDirAndResult(2);
  }

  private void verifyDirAndResult(int expectedDeltas) throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    // Verify the content of subdirs
    FileStatus[] status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.MMTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    int sawDeltaTimes = 0;
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("delta_.*"));
      sawDeltaTimes++;
      FileStatus[] files = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
      Assert.assertEquals(1, files.length);
      Assert.assertTrue(files[0].getPath().getName().equals("000000_0"));
    }
    Assert.assertEquals(expectedDeltas, sawDeltaTimes);

    // Verify query result
    int [][] resultData = new int[][] {{1,2}, {3,4}};
    List<String> rs = runStatementOnDriver("select a,b from " + Table.MMTBL);
    Assert.assertEquals(stringifyValues(resultData), rs);
  }

  /**
   * takes raw data and turns it into a string as if from Driver.getResults()
   * sorts rows in dictionary order
   */
  static List<String> stringifyValues(int[][] rowsIn) {
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
  static class RowComp implements Comparator<int[]> {
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
  static String makeValuesClause(int[][] rows) {
    assert rows.length > 0;
    StringBuilder sb = new StringBuilder(" values");
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

  protected List<String> runStatementOnDriver(String stmt) throws Exception {
    LOG.info("+runStatementOnDriver(" + stmt + ")");
    CommandProcessorResponse cpr = d.run(stmt);
    if(cpr.getResponseCode() != 0) {
      throw new RuntimeException(stmt + " failed: " + cpr);
    }
    List<String> rs = new ArrayList<String>();
    d.getResults(rs);
    return rs;
  }
  final void assertUniqueID(Table table) throws Exception {
    String partCols = table.getPartitionColumns();
    //check to make sure there are no duplicate ROW__IDs - HIVE-16832
    StringBuilder sb = new StringBuilder("select ");
    if(partCols != null && partCols.length() > 0) {
      sb.append(partCols).append(",");
    }
    sb.append(" ROW__ID, count(*) from ").append(table).append(" group by ");
    if(partCols != null && partCols.length() > 0) {
      sb.append(partCols).append(",");
    }
    sb.append("ROW__ID having count(*) > 1");
    List<String> r = runStatementOnDriver(sb.toString());
    Assert.assertTrue("Duplicate ROW__ID: " + r.toString(),r.size() == 0);
  }
}
