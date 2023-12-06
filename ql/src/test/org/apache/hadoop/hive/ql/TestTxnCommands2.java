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
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.AbortCompactResponse;
import org.apache.hadoop.hive.metastore.api.AbortCompactionRequest;
import org.apache.hadoop.hive.metastore.api.AbortCompactionResponseElement;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.AcidHouseKeeperService;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.ql.ddl.DDLTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.TxnManagerFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.metastore.txn.AcidOpenTxnsCounterService;
import org.apache.hadoop.hive.ql.scheduled.ScheduledQueryExecutionContext;
import org.apache.hadoop.hive.ql.scheduled.ScheduledQueryExecutionService;
import org.apache.hadoop.hive.ql.schq.MockScheduledQueryService;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorFactory;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorPipeline;
import org.apache.hadoop.hive.ql.txn.compactor.MRCompactor;
import org.apache.hadoop.hive.ql.txn.compactor.Worker;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.rules.ExpectedException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTxnCommands2 extends TxnCommandsBaseForTests {
  static final private Logger LOG = LoggerFactory.getLogger(TestTxnCommands2.class);
  protected static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
    File.separator + TestTxnCommands2.class.getCanonicalName()
    + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");

  @Override
  protected String getTestDataDir() {
    return TEST_DATA_DIR;
  }
  
  enum Table {
    ACIDTBL("acidTbl"),
    ACIDTBLPART("acidTblPart", "p"),
    NONACIDORCTBL("nonAcidOrcTbl"),
    NONACIDPART("nonAcidPart", "p"),
    NONACIDPART2("nonAcidPart2", "p2"),
    NONACIDNESTEDPART("nonAcidNestedPart", "p,q"),
    ACIDNESTEDPART("acidNestedPart", "p,q"),
    MMTBL("mmTbl");

    final String name;
    final String partitionColumns;
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
  
  @Override
  void initHiveConf() {
    super.initHiveConf();
    //TestTxnCommands2WithSplitUpdateAndVectorization has the vectorized version
    //of these tests.
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    //TestTxnCommands2WithAbortCleanupUsingCompactionCycle has the tests with abort cleanup in compaction cycle
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER, true);
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPTIMIZE_METADATA_QUERIES, false);
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_TRUNCATE_USE_BASE, false);
  }
  
  @Override
  protected void setUpSchema() throws Exception {
    runStatementOnDriver("create table " + Table.ACIDTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + Table.ACIDTBLPART + "(a int, b int) partitioned by (p string) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + Table.NONACIDORCTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table " + Table.NONACIDPART + "(a int, b int) partitioned by (p string) stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table " + Table.NONACIDPART2 +
      "(a2 int, b2 int) partitioned by (p2 string) stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table " + Table.NONACIDNESTEDPART +
      "(a int, b int) partitioned by (p string, q string) clustered by (a) into " + BUCKET_COUNT +
      " buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table " + Table.ACIDNESTEDPART +
      "(a int, b int) partitioned by (p int, q int) clustered by (a) into " + BUCKET_COUNT +
      " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + Table.MMTBL + "(a int, b int) TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')");
  }
  
  @Override
  protected void dropTables() throws Exception {
    for (TestTxnCommands2.Table t : TestTxnCommands2.Table.values()) {
      runStatementOnDriver("drop table if exists " + t);
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
    boolean originalPpd = hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_OPT_INDEX_FILTER);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_OPT_INDEX_FILTER, enablePPD);//enables ORC PPD
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
    runWorker(hiveConf);
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
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_OPT_INDEX_FILTER, originalPpd);
  }

  static void assertExplainHasString(String string, List<String> queryPlan, String errMsg) {
    for(String line : queryPlan) {
      if(line != null && line.contains(string)) {
        return;
      }
    }
    Assert.fail(errMsg);
  }
  
  @Test
  public void testAlterTable() throws Exception {
    int[][] tableData = {{1,2}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData));
    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
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
        {"{\"writeid\":0,\"bucketid\":536936448,\"rowid\":2}\t0\t13",  "bucket_00001"},
        {"{\"writeid\":10000001,\"bucketid\":536936448,\"rowid\":0}\t0\t15", "bucket_00001"},
        {"{\"writeid\":10000003,\"bucketid\":536936448,\"rowid\":0}\t0\t17", "bucket_00001"},
        {"{\"writeid\":10000002,\"bucketid\":536936449,\"rowid\":0}\t0\t120", "bucket_00001"},
        {"{\"writeid\":0,\"bucketid\":536936448,\"rowid\":1}\t1\t2",   "bucket_00001"},
        {"{\"writeid\":0,\"bucketid\":536936448,\"rowid\":5}\t1\t4",   "bucket_00001"},
        {"{\"writeid\":0,\"bucketid\":536936448,\"rowid\":4}\t1\t5",   "bucket_00001"},
        {"{\"writeid\":0,\"bucketid\":536936448,\"rowid\":6}\t1\t6",   "bucket_00001"},
        {"{\"writeid\":10000001,\"bucketid\":536936448,\"rowid\":1}\t1\t16", "bucket_00001"}
    };
    Assert.assertEquals("Unexpected row count before compaction", expected.length, rs.size());
    for(int i = 0; i < expected.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i) + "; expected " + expected[i][0],
          rs.get(i).startsWith(expected[i][0]));
    }
    //run Compaction
    runStatementOnDriver("alter table "+ TestTxnCommands2.Table.NONACIDORCTBL +" compact 'major'");
    runWorker(hiveConf);
    //runCleaner(hiveConf);
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_00000_0"));
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(4, status.length);
    boolean sawNewBase = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("base_.*")) {
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Arrays.sort(buckets);
        Assert.assertEquals(2, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
      }
    }
    Assert.assertTrue(sawNewBase);
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{3, 4}, {1, 2}};
    Assert.assertEquals(stringifyValuesNoSort(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 5. Let Cleaner delete obsolete files/dirs
    // Note, here we create a fake directory along with fake files as original directories/files
    String fakeFile0 = getWarehouseDir() + "/" + (Table.NONACIDORCTBL).toString().toLowerCase() +
      "/subdir/000000_0";
    String fakeFile1 = getWarehouseDir() + "/" + (Table.NONACIDORCTBL).toString().toLowerCase() +
      "/subdir/000000_1";
    fs.create(new Path(fakeFile0));
    fs.create(new Path(fakeFile1));
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    // Before Cleaner, there should be 5 items:
    // 2 original files, 1 original directory, 1 base directory and 1 delta directory
    Assert.assertEquals(5, status.length);
    runCleaner(hiveConf);
    // There should be only 1 directory left: base_xxxxxxx.
    // Original bucket files and delta directory should have been cleaned up.
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(1, status.length);
    Assert.assertTrue(status[0].getPath().getName().matches("base_.*"));
    FileStatus[] buckets = fs.listStatus(status[0].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Arrays.sort(buckets);
    Assert.assertEquals(2, buckets.length);
    Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{3, 4}, {1, 2}};
    Assert.assertEquals(stringifyValuesNoSort(resultData), rs);
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]_0"));
      } else if (status[i].getPath().getName().matches("delete_delta_.*")) {
        sawNewDeleteDelta = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01_0]_0"));
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    // Before Cleaner, there should be 5 items:
    // 2 original files, 1 delta directory, 1 delete_delta directory and 1 base directory
    Assert.assertEquals(5, status.length);
    runCleaner(hiveConf);
    // There should be only 1 directory left: base_0000001.
    // Original bucket files, delta directory and delete_delta should have been cleaned up.
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(3, status.length);
    boolean sawNewBase = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("base_.*")) {
        //should be base_-9223372036854775808_v0000023 but 23 is a txn id not write id so it makes
        //the tests fragile
        Assert.assertTrue(status[i].getPath().getName().startsWith("base_-9223372036854775808_v0000023"));
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
          Assert.assertEquals("delta_10000001_10000001_0001", status[i].getPath().getName());
          Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
          Assert.assertEquals("bucket_00001_0", buckets[0].getPath().getName());
        } else if (numDelta == 2) {
          Assert.assertEquals("delta_10000002_10000002_0000", status[i].getPath().getName());
          Assert.assertEquals(1, buckets.length);
          Assert.assertEquals("bucket_00000_0", buckets[0].getPath().getName());
        }
      } else if (status[i].getPath().getName().matches("delete_delta_.*")) {
        numDeleteDelta++;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Arrays.sort(buckets);
        if (numDeleteDelta == 1) {
          Assert.assertEquals("delete_delta_10000001_10000001_0000", status[i].getPath().getName());
          Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
          Assert.assertEquals("bucket_00001_0", buckets[0].getPath().getName());
        }
      } else if (status[i].getPath().getName().matches("base_.*")) {
        Assert.assertTrue("base_-9223372036854775808", status[i].getPath().getName().startsWith("base_-9223372036854775808_v0000023"));//_v0000023
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
          Assert.assertEquals("base_-9223372036854775808_v0000023", status[i].getPath().getName());
          Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
          Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
        } else if (numBase == 2) {
          // The new base dir now has two bucket files, since the delta dir has two bucket files
          Assert.assertEquals("base_10000002_v0000031", status[i].getPath().getName());
          Assert.assertEquals(2, buckets.length);
          Assert.assertEquals("bucket_00000", buckets[0].getPath().getName());
        }
      }
    }
    Assert.assertEquals(2, numBase);
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{3, 4}, {1, 3}};
    Assert.assertEquals(stringifyValuesNoSort(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 6. Let Cleaner delete obsolete files/dirs
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    // Before Cleaner, there should be 6 items:
    // 2 original files, 2 delta directories, 1 delete_delta directory and 2 base directories
    Assert.assertEquals(7, status.length);
    runCleaner(hiveConf);
    runCleaner(hiveConf);
    // There should be only 1 directory left: base_0000001.
    // Original bucket files, delta directories and previous base directory should have been cleaned up.
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
      (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(1, status.length);
    Assert.assertEquals("base_10000002_v0000031", status[0].getPath().getName());
    FileStatus[] buckets = fs.listStatus(status[0].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Arrays.sort(buckets);
    Assert.assertEquals(2, buckets.length);
    Assert.assertEquals("bucket_00000", buckets[0].getPath().getName());
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{3, 4}, {1, 3}};
    Assert.assertEquals(stringifyValuesNoSort(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
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
  public void testNonAcidToAcidConversion4() throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;

    // 1. Insert a row to Non-ACID nested partitioned table
    int[][] targetVals = {{1,2}};
    runStatementOnDriver("insert into " + Table.NONACIDNESTEDPART + " partition(p='p1',q='q1') " + makeValuesClause(targetVals));
    status = listFilesByTable(fs, Table.NONACIDNESTEDPART);

    // There should be 2 original bucket files in the location (000000_0 and 000001_0)
    Assert.assertEquals(BUCKET_COUNT, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
    }
    List<String> rs = runStatementOnDriver("select a,b from " + Table.NONACIDNESTEDPART);
    Assert.assertEquals(stringifyValues(targetVals), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDNESTEDPART);
    int resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 2. Convert NONACIDNESTEDPART to ACID table
    runStatementOnDriver("alter table " + Table.NONACIDNESTEDPART + " SET TBLPROPERTIES ('transactional'='true')");
    status = listFilesByTable(fs, Table.NONACIDNESTEDPART);
    // Everything should be same as before
    Assert.assertEquals(BUCKET_COUNT, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
    }
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDNESTEDPART);
    Assert.assertEquals(stringifyValues(targetVals), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDNESTEDPART);
    resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 3. Perform a major compaction
    runStatementOnDriver("alter table "+ Table.NONACIDNESTEDPART + " partition(p='p1',q='q1') compact 'MAJOR'");
    runWorker(hiveConf);
    // There should be 1 new directory: base_-9223372036854775808
    // Original bucket files should stay until Cleaner kicks in.
    status = listFilesByTable(fs, Table.NONACIDNESTEDPART);
    Assert.assertEquals(3, status.length);
    boolean sawNewBase = false;
    for (int i = 0; i < status.length; i++) {
      Path parent = status[i].getPath().getParent();
      if (parent.getName().matches("base_.*")) {
        //should be base_-9223372036854775808_v0000023 but 23 is a txn id not write id so it makes
        //the tests fragile
        Assert.assertTrue(parent.getName().startsWith("base_-9223372036854775808_v0000023"));
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(parent, FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
        Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
      }
    }
    Assert.assertTrue(sawNewBase);
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDNESTEDPART);
    Assert.assertEquals(stringifyValues(targetVals), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDNESTEDPART);
    resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 4. Update the existing row, and insert another row to newly-converted ACID table
    runStatementOnDriver("update " + Table.NONACIDNESTEDPART + " set b=3 where a=1");
    runStatementOnDriver("insert into " + Table.NONACIDNESTEDPART + "(a,b,p,q) values(3,4,'p1','q1')");
    status = listFilesByTable(fs, Table.NONACIDNESTEDPART);
    Arrays.sort(status);  // make sure delta_0000001_0000001_0000 appears before delta_0000002_0000002_0000
    // There should be 2 original bucket files (000000_0 and 000001_0), a base directory,
    // plus two new delta directories and one delete_delta directory that would be created due to
    // the update statement (remember split-update U=D+I)!
    Assert.assertEquals(6, status.length);
    int numDelta = 0;
    int numDeleteDelta = 0;
    sawNewBase = false;
    for (int i = 0; i < status.length; i++) {
      Path parent = status[i].getPath().getParent();
      if (parent.getName().matches("delta_.*")) {
        numDelta++;
        FileStatus[] buckets = fs.listStatus(parent, FileUtils.HIDDEN_FILES_PATH_FILTER);
        Arrays.sort(buckets);
        if (numDelta == 1) {
          Assert.assertEquals("delta_10000001_10000001_0001", parent.getName());
          Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
          Assert.assertEquals("bucket_00001_0", buckets[0].getPath().getName());
        } else if (numDelta == 2) {
          Assert.assertEquals("delta_10000002_10000002_0000", parent.getName());
          Assert.assertEquals(1, buckets.length);
          Assert.assertEquals("bucket_00000_0", buckets[0].getPath().getName());
        }
      } else if (parent.getName().matches("delete_delta_.*")) {
        numDeleteDelta++;
        FileStatus[] buckets = fs.listStatus(parent, FileUtils.HIDDEN_FILES_PATH_FILTER);
        Arrays.sort(buckets);
        if (numDeleteDelta == 1) {
          Assert.assertEquals("delete_delta_10000001_10000001_0000", parent.getName());
          Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
          Assert.assertEquals("bucket_00001_0", buckets[0].getPath().getName());
        }
      } else if (parent.getName().matches("base_.*")) {
        Assert.assertTrue("base_-9223372036854775808", parent.getName().startsWith("base_-9223372036854775808_v0000023"));//_v0000023
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(parent, FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
        Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
      }
    }
    Assert.assertEquals(2, numDelta);
    Assert.assertEquals(1, numDeleteDelta);
    Assert.assertTrue(sawNewBase);

    rs = runStatementOnDriver("select a,b from " + Table.NONACIDNESTEDPART);
    targetVals = new int[][] {{1, 3}, {3, 4}};
    Assert.assertEquals(stringifyValues(targetVals), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDNESTEDPART);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 5. Perform another major compaction
    runStatementOnDriver("alter table "+ Table.NONACIDNESTEDPART + " partition(p='p1',q='q1') compact 'MAJOR'");
    runWorker(hiveConf);
    // There should be 1 new base directory: base_0000001
    // Original bucket files, delta directories, delete_delta directories and the
    // previous base directory should stay until Cleaner kicks in.
    status = listFilesByTable(fs, Table.NONACIDNESTEDPART);
    Arrays.sort(status);
    Assert.assertEquals(8, status.length);
    int numBase = 0;
    Set<Path> bases = new HashSet<>();
    for (int i = 0; i < status.length; i++) {
      Path parent = status[i].getPath().getParent();
      if (parent.getName().matches("base_.*")) {
        numBase++;
        bases.add(parent);
        FileStatus[] buckets = fs.listStatus(parent, FileUtils.HIDDEN_FILES_PATH_FILTER);
        Arrays.sort(buckets);
        if (numBase == 1) {
          Assert.assertEquals("base_-9223372036854775808_v0000023", parent.getName());
          Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
          Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
        } else if (numBase == 2) {
          // The new base dir now has two bucket files, since the delta dir has two bucket files
          Assert.assertEquals("base_10000002_v0000031", parent.getName());
          Assert.assertEquals(2, buckets.length);
          Assert.assertEquals("bucket_00000", buckets[0].getPath().getName());
        }
      }
    }
    Assert.assertEquals(2,  bases.size());
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDNESTEDPART);
    targetVals = new int[][] {{3, 4}, {1, 3}};
    Assert.assertEquals(stringifyValuesNoSort(targetVals), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDNESTEDPART);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 6. Let Cleaner delete obsolete files/dirs
    status = listFilesByTable(fs, Table.NONACIDNESTEDPART);
    // Before Cleaner, there should be 8 items:
    // 2 original files, 2 delta directories (1 files each), 1 delete_delta directory (1 file) and 2 base directories (with one and two files respectively)

    Assert.assertEquals(8, status.length);
    runCleaner(hiveConf);
    runCleaner(hiveConf);
    // There should be only 1 directory left: base_0000001.
    // Original bucket files, delta directories and previous base directory should have been cleaned up. Only one base with 2 files.
    status = listFilesByTable(fs, Table.NONACIDNESTEDPART);
    Assert.assertEquals(2, status.length);
    Assert.assertEquals("base_10000002_v0000031", status[0].getPath().getParent().getName());
    FileStatus[] buckets = fs.listStatus(status[0].getPath().getParent(), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Arrays.sort(buckets);
    Assert.assertEquals(2, buckets.length);
    Assert.assertEquals("bucket_00000", buckets[0].getPath().getName());
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDNESTEDPART);
    targetVals = new int[][] {{3, 4}, {1, 3}};
    Assert.assertEquals(stringifyValuesNoSort(targetVals), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDNESTEDPART);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));
  }

  private FileStatus[] listFilesByTable(FileSystem fs, Table t) throws IOException {
    List<FileStatus> tmp = new ArrayList<>();
    RemoteIterator<LocatedFileStatus> f = fs.listFiles(new Path(getWarehouseDir() + "/" +
            t.toString().toLowerCase()), true);

    while (f.hasNext()) {
      LocatedFileStatus file = f.next();
      if (FileUtils.HIDDEN_FILES_PATH_FILTER.accept(file.getPath())) {
        tmp.add(file);
      }
    }
    return tmp.toArray(new FileStatus[0]);
  }

  @Test
  public void testValidTxnsBookkeeping() throws Exception {
    // 1. Run a query against a non-ACID table, and we shouldn't have txn logged in conf
    runStatementOnDriver("select * from " + Table.NONACIDORCTBL);
    String value = hiveConf.get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY);
    Assert.assertNull("The entry should be null for query that doesn't involve ACID tables", value);
  }

  @Test
  public void testSimpleRead() throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "more");
    int[][] tableData = {{1,2},{3,3}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + " " + makeValuesClause(tableData));
    int[][] tableData2 = {{5,3}};
    //this will cause next txn to be marked aborted but the data is still written to disk
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("insert into " + Table.ACIDTBL + " " + makeValuesClause(tableData2));
    assert hiveConf.get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY) == null : "previous txn should've cleaned it";
    //so now if HIVE_FETCH_TASK_CONVERSION were to use a stale value, it would use a
    //ValidWriteIdList with HWM=MAX_LONG, i.e. include the data for aborted txn
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
    CompactionRequest request = new CompactionRequest("default", Table.ACIDTBLPART.name(), CompactionType.MAJOR);
    request.setPartitionname("p=1");
    txnHandler.compact(request);
    runWorker(hiveConf);
    runCleaner(hiveConf);
    runStatementOnDriver("update " + Table.ACIDTBLPART + " set b = b + 1 where a = 3");
    txnHandler.compact(request);
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
    runStatementOnDriver("update t1" + " set b = -2 where a = 1");
    runStatementOnDriver("alter table t1 " + " compact 'MAJOR'");
    runWorker(hiveConf);
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
    Assert.assertEquals(TxnStore.DID_NOT_INITIATE_RESPONSE, expected.didNotInitiate, actual.didNotInitiate);
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
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_COMPACTION, true);
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON, true);
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON, true);

    int numFailedCompactions = MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD);
    AtomicBoolean stop = new AtomicBoolean(true);
    //create failed compactions
    for(int i = 0; i < numFailedCompactions; i++) {
      //each of these should fail
      txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MINOR));
      runWorker(hiveConf);
    }
    //this should not schedule a new compaction due to prior failures, but will create 'did not initiate' entry
    runInitiator(hiveConf);
    int numDidNotInitiateCompactions = 1;
    checkCompactionState(new CompactionsByState(numDidNotInitiateCompactions,numFailedCompactions,0,0,0,0,numFailedCompactions + numDidNotInitiateCompactions), countCompacts(txnHandler));

    MetastoreConf.setTimeVar(hiveConf, MetastoreConf.ConfVars.ACID_HOUSEKEEPER_SERVICE_INTERVAL, 10,
        TimeUnit.MILLISECONDS);
    MetastoreTaskThread houseKeeper = new AcidHouseKeeperService();
    houseKeeper.setConf(hiveConf);
    houseKeeper.run();
    checkCompactionState(new CompactionsByState(numDidNotInitiateCompactions,numFailedCompactions,0,0,0,0,numFailedCompactions + numDidNotInitiateCompactions), countCompacts(txnHandler));

    txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MAJOR));
    runWorker(hiveConf);//will fail
    txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MINOR));
    runWorker(hiveConf);//will fail
    runInitiator(hiveConf);
    numDidNotInitiateCompactions++;
    runInitiator(hiveConf);
    numDidNotInitiateCompactions++;
    checkCompactionState(new CompactionsByState(numDidNotInitiateCompactions,numFailedCompactions + 2,0,0,0,0,numFailedCompactions + 2 + numDidNotInitiateCompactions), countCompacts(txnHandler));

    houseKeeper.run();
    //COMPACTOR_HISTORY_RETENTION_FAILED failed compacts left (and no other since we only have failed ones here)
    checkCompactionState(new CompactionsByState(
      MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_DID_NOT_INITIATE),
            MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED),0,0,0,0,
            MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED) + MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_DID_NOT_INITIATE)),
        countCompacts(txnHandler));

    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_COMPACTION, false);
    txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MINOR));
    //at this point "show compactions" should have (COMPACTOR_HISTORY_RETENTION_FAILED) failed + 1 initiated (explicitly by user)
    checkCompactionState(new CompactionsByState(
      MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_DID_NOT_INITIATE),
            MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED),1,0,0,0,
            MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED) +
                MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_DID_NOT_INITIATE) + 1),
        countCompacts(txnHandler));

    runWorker(hiveConf);//will succeed and transition to Initiated->Working->Ready for Cleaning
    checkCompactionState(new CompactionsByState(
      MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_DID_NOT_INITIATE),
            MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED),0,1,0,0,
        MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED) +
        MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_DID_NOT_INITIATE) + 1),
        countCompacts(txnHandler));

    runCleaner(hiveConf); // transition to Success state
    houseKeeper.run();
    checkCompactionState(new CompactionsByState(
            MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_DID_NOT_INITIATE),
            MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED), 0, 0, 1, 0,
            MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED) + MetastoreConf
                .getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_DID_NOT_INITIATE) + 1),
        countCompacts(txnHandler));
  }

  @Test
  public void testInitiatorWithMinorCompactionForInsertOnlyTable() throws Exception {
    String tblName = "insertOnlyTable";
    runStatementOnDriver("drop table if exists " + tblName);
    runStatementOnDriver("create table " + tblName + " (a INT, b STRING) stored as orc tblproperties('transactional'='true', " +
            "'transactional_properties' = 'insert_only')");
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 4);
    hiveConf.setFloatVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_PCT_THRESHOLD, 1.0f);
    for(int i = 0; i < 20; i++) {
      //generate enough delta files so that Initiator can trigger auto compaction
      runStatementOnDriver("insert into " + tblName + " values(" + (i + 1) + ", 'foo'),(" + (i + 2) + ", 'bar'),(" + (i + 3) + ", 'baz')");
    }
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON, true);
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON, true);
    runInitiator(hiveConf);
    runWorker(hiveConf);
    runCleaner(hiveConf);

    for(int i = 0; i < 5; i++) {
      //generate enough delta files so that Initiator can trigger auto compaction
      runStatementOnDriver("insert into " + tblName + " values(" + (i + 1) + ", 'foo'),(" + (i + 2) + ", 'bar'),(" + (i + 3) + ", 'baz')");
    }
    runInitiator(hiveConf);
    runWorker(hiveConf);
    runCleaner(hiveConf);

    verifyDeltaDir(1, tblName, "");
    verifyBaseDir(1, tblName, "");
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
    private int didNotInitiate;
    private int failed;
    private int initiated;
    private int readyToClean;
    private int succeeded;
    private int working;
    private int total;
    CompactionsByState() {
      this(0,0,0,0,0,0,0);
    }
    CompactionsByState(int didNotInitiate, int failed, int initiated, int readyToClean, int succeeded, int working, int total) {
      this.didNotInitiate = didNotInitiate;
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
      else if(TxnStore.DID_NOT_INITIATE_RESPONSE.equals(compact.getState())) {
        compactionsByState.didNotInitiate++;
      }
      else {
        throw new IllegalStateException("Unexpected state: " + compact.getState());
      }
    }
    return compactionsByState;
  }

  @Test
  public void testDropTableAndCompactionConcurrent() throws Exception {
    execDDLOpAndCompactionConcurrently("DROP_TABLE", false);
  }
  @Test
  public void testTruncateTableAndCompactioConcurrent() throws Exception {
    execDDLOpAndCompactionConcurrently("TRUNCATE_TABLE", false);
  }
  @Test
  public void testDropPartitionAndCompactionConcurrent() throws Exception {
    execDDLOpAndCompactionConcurrently("DROP_PARTITION", true);
  }
  @Test
  public void testTruncatePartitionAndCompactionConcurrent() throws Exception {
    execDDLOpAndCompactionConcurrently("TRUNCATE_PARTITION", true);
  }
  private void execDDLOpAndCompactionConcurrently(String opType, boolean isPartioned) throws Exception {
    // Stats gathering needs to be disabled as it runs in a separate transaction, and it cannot be synchronized using
    // countdownlatch, because the Statsupdater instance is private and static.
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_COMPACTOR_GATHER_STATS, false);

    String tblName = "hive12352";
    String partName = "test";

    runStatementOnDriver("DROP TABLE if exists " + tblName);
    runStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING)" +
      (isPartioned ? "partitioned by (p STRING)" : "") +
      " STORED AS ORC  TBLPROPERTIES ( 'transactional'='true' )");

    //create some data
    runStatementOnDriver("INSERT INTO " + tblName +
      (isPartioned ? " PARTITION (p='" + partName + "')" : "") +
      " VALUES (1, 'foo'),(2, 'bar'),(3, 'baz')");
    runStatementOnDriver("UPDATE " + tblName + " SET b = 'blah' WHERE a = 3");

    //run Worker to execute compaction
    CompactionRequest req = new CompactionRequest("default", tblName, CompactionType.MAJOR);
    if (isPartioned) {
      req.setPartitionname("p=" + partName);
    }
    txnHandler.compact(req);
    MRCompactor mrCompactor = Mockito.spy(new MRCompactor(HiveMetaStoreUtils.getHiveMetastoreClient(hiveConf)));

    CountDownLatch ddlStart = new CountDownLatch(1);
    Mockito.doAnswer((Answer<JobConf>) invocationOnMock -> {
      JobConf job = (JobConf) invocationOnMock.callRealMethod();
      job.setMapperClass(SlowCompactorMap.class);
      //let concurrent DDL oparetaions to start in the middle of the Compaction Txn, right before SlowCompactorMap will
      //mimic a long-running compaction
      ddlStart.countDown();
      return job;
    }).when(mrCompactor).createBaseJobConf(any(), any(), any(), any(), any(), any());

    CompactorFactory mockedFactory = Mockito.mock(CompactorFactory.class);
    when(mockedFactory.getCompactorPipeline(any(), any(), any(), any())).thenReturn(new CompactorPipeline(mrCompactor));

    Worker worker = new Worker(mockedFactory);
    worker.setConf(hiveConf);
    worker.init(new AtomicBoolean(true));

    CompletableFuture<Void> compactionJob = CompletableFuture.runAsync(worker);

    if (!ddlStart.await(5000, TimeUnit.MILLISECONDS)) {
      Assert.fail("Waiting too long for compaction to start!");
    }

    int compHistory = 0;
    switch (opType) {
      case "DROP_TABLE":
      runStatementOnDriver("DROP TABLE " + tblName);
      runStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
          " STORED AS ORC  TBLPROPERTIES ( 'transactional'='true' )");
      break;
      case "TRUNCATE_TABLE":
        runStatementOnDriver("TRUNCATE TABLE " + tblName);
        compHistory = 1;
        break;
      case "DROP_PARTITION": {
        runStatementOnDriver("ALTER TABLE " + tblName + " DROP PARTITION (p='" + partName + "')");
        runStatementOnDriver("ALTER TABLE " + tblName + " ADD PARTITION (p='" + partName + "')");
        break;
      }
      case "TRUNCATE_PARTITION": {
        runStatementOnDriver("TRUNCATE TABLE " + tblName + " PARTITION (p='" + partName + "')");
        compHistory = 1;
        break;
      }
    }
    compactionJob.join();

    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history",
      compHistory, resp.getCompactsSize());
    if (compHistory != 0) {
      Assert.assertEquals("Unexpected 0th compaction state",
        TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(0).getState());
    }
    GetOpenTxnsResponse openResp =  txnHandler.getOpenTxns();
    Assert.assertEquals(openResp.toString(), 0, openResp.getOpen_txnsSize());

    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status = fs.listStatus(new Path(getWarehouseDir() + "/" + tblName +
        (isPartioned ? "/p=" + partName : "")),
      FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(0, status.length);
  }

  static class SlowCompactorMap<V extends Writable> extends MRCompactor.CompactorMap<V>{
    @Override
    public void cleanupTmpLocationOnTaskRetry(AcidOutputFormat.Options options, Path rootDir) throws IOException {
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      super.cleanupTmpLocationOnTaskRetry(options, rootDir);
    }
  }
  /**
   * HIVE-12352 has details
   * @throws Exception
   */
  @Test
  public void writeBetweenWorkerAndCleaner() throws Exception {
    writeBetweenWorkerAndCleanerForVariousTblProperties("'transactional'='true'");
  }

  private void writeBetweenWorkerAndCleanerForVariousTblProperties(String tblProperties) throws Exception {
    String tblName = "hive12352";
    runStatementOnDriver("drop table if exists " + tblName);
    runStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ( " + tblProperties + " )");

    //create some data
    runStatementOnDriver("insert into " + tblName + " values(1, 'foo'),(2, 'bar'),(3, 'baz')");
    runStatementOnDriver("update " + tblName + " set b = 'blah' where a = 3");

    //run Worker to execute compaction
    txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MINOR));
    runWorker(hiveConf);

    //delete something, but make sure txn is rolled back
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("delete from " + tblName + " where a = 1");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, false);

    List<String> expected = new ArrayList<>();
    expected.add("1\tfoo");
    expected.add("2\tbar");
    expected.add("3\tblah");
    Assert.assertEquals("", expected,
      runStatementOnDriver("select a,b from " + tblName + " order by a"));

    runCleaner(hiveConf);

    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status = fs.listStatus(new Path(getWarehouseDir() + "/" + tblName.toLowerCase()),
        FileUtils.HIDDEN_FILES_PATH_FILTER);
    Set<String> expectedDeltas = new HashSet<>();
    expectedDeltas.add("delete_delta_0000001_0000002_v0000021");
    expectedDeltas.add("delta_0000001_0000002_v0000021");
    expectedDeltas.add("delete_delta_0000003_0000003_0000");
    Set<String> actualDeltas = new HashSet<>();
    for(FileStatus file : status) {
      actualDeltas.add(file.getPath().getName());
    }
    Assert.assertEquals(expectedDeltas, actualDeltas);

    runStatementOnDriver("insert into " + tblName + " values(4, 'xyz')");
    expected.add("4\txyz");
    //run Worker to execute compaction
    txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MINOR));
    runWorker(hiveConf);
    runCleaner(hiveConf);

    status = fs.listStatus(new Path(getWarehouseDir() + "/" + tblName.toLowerCase()),
        FileUtils.HIDDEN_FILES_PATH_FILTER);
    expectedDeltas = new HashSet<>();
    expectedDeltas.add("delete_delta_0000001_0000004_v0000026");
    expectedDeltas.add("delta_0000001_0000004_v0000026");
    actualDeltas = new HashSet<>();
    for(FileStatus file : status) {
      actualDeltas.add(file.getPath().getName());
    }
    Assert.assertEquals(expectedDeltas, actualDeltas);

    //this seems odd, but we want to make sure that run CompactionTxnHandler.cleanEmptyAbortedTxns()
    runInitiator(hiveConf);
    //and check that aborted delete operation didn't become committed
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
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_HEARTBEATER, true);
    Exception exception = null;
    try {
      runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(new int[][]{{1, 2}, {3, 4}}));
    } catch (RuntimeException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);
    Assert.assertTrue(exception.getMessage().contains("HIVE_TEST_MODE_FAIL_HEARTBEATER=true"));
  }

  @Test
  public void testOpenTxnsCounter() throws Exception {
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_MAX_OPEN_TXNS, 3);
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_COUNT_OPEN_TXNS_INTERVAL, 10, TimeUnit.MILLISECONDS);
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
    runWorker(hiveConf);
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where b = 4");
    runStatementOnDriver("update " + Table.ACIDTBL + " set b = -2 where b = 2");
    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MINOR'");
    runWorker(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history", 2, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0 compaction state", TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(0).getState());
    Assert.assertEquals("Unexpected 1 compaction state", TxnStore.REFUSED_RESPONSE,
        resp.getCompacts().get(1).getState());
  }

  /**
   * make sure Aborted txns don't red-flag a base_xxxx (HIVE-14350)
   */
  @Test
  public void testNoHistory() throws Exception {
    int[][] tableData = {{1,2},{3,4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData));
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData));
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, false);

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
    // create a low water mark aborted transaction and clean the older ones
    createAbortLowWaterMark();
    runCleaner(hiveConf); // Cleaner would remove the obsolete files.

    // Verify that there is now only 1 new directory: base_xxxxxxx and the rest have have been cleaned.
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;
    status = fs.listStatus(new Path(getWarehouseDir() + "/" + tblName.toString().toLowerCase()),
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

  protected void createAbortLowWaterMark() throws Exception{
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("select * from " + Table.ACIDTBL);
    // wait for metastore.txn.opentxn.timeout
    Thread.sleep(1000);
    runInitiator(hiveConf);
  }

  @Test
  public void testETLSplitStrategyForACID() throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.HIVE_ORC_SPLIT_STRATEGY, "ETL");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_OPT_INDEX_FILTER, true);
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
    //currently multi-insert doesn't allow same table/partition in > 1 output branch
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
    runCleaner(hiveConf);

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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
        (Table.ACIDTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    // There should be 2 delta dirs in the location
    Assert.assertEquals(2, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("delta_.*"));
    }

    // 2. Perform a major compaction. There should be an extra base dir now.
    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    runCleaner(hiveConf);
    // There should be only 1 directory left: base_xxxxxxx.
    // The delta dirs should have been cleaned up.
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
        (Table.ACIDTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(1, status.length);
    Assert.assertTrue(status[0].getPath().getName().matches("base_.*"));
    // Verify query result
    rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a");
    Assert.assertEquals(stringifyValues(resultData), rs);
  }

  /**
   * Create a table with schema evolution, and verify that no data is lost during (MR major)
   * compaction.
   *
   * @throws Exception if a query fails
   */
  @Test
  public void testSchemaEvolutionCompaction() throws Exception {
    String tblName = "schemaevolutioncompaction";
    runStatementOnDriver("drop table if exists " + tblName);
    runStatementOnDriver("CREATE TABLE " + tblName + "(a INT) " +
        " PARTITIONED BY(part string)" +
        " STORED AS ORC TBLPROPERTIES ('transactional'='true')");

    // First INSERT round.
    runStatementOnDriver("insert into " + tblName + " partition (part='aa') values (1)");

    // ALTER TABLE ... ADD COLUMNS
    runStatementOnDriver("ALTER TABLE " + tblName + " ADD COLUMNS(b int)");

    // Second INSERT round.
    runStatementOnDriver("insert into " + tblName + " partition (part='aa') values (2, 2000)");

    // Validate data
    List<String> res = runStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a");
    Assert.assertEquals(2, res.size());
    Assert.assertEquals("1\tNULL\taa", res.get(0));
    Assert.assertEquals("2\t2000\taa", res.get(1));

    // Compact
    CompactionRequest compactionRequest =
        new CompactionRequest("default", tblName, CompactionType.MAJOR);
    compactionRequest.setPartitionname("part=aa");
    txnHandler.compact(compactionRequest);
    runWorker(hiveConf);
    runCleaner(hiveConf);

    // Verify successful compaction
    List<ShowCompactResponseElement> compacts =
        txnHandler.showCompact(new ShowCompactRequest()).getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, compacts.get(0).getState());

    // Validate data after compaction
    res = runStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a");
    Assert.assertEquals(2, res.size());
    Assert.assertEquals("1\tNULL\taa", res.get(0));
    Assert.assertEquals("2\t2000\taa", res.get(1));
  }

  /**
   * Test cleaner for TXN_TO_WRITE_ID table.
   * @throws Exception
   */
  @Test
  public void testCleanerForTxnToWriteId() throws Exception {
    int[][] tableData1 = {{1, 2}};
    int[][] tableData2 = {{2, 3}};
    int[][] tableData3 = {{3, 4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData1));
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData2));
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData3));
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p=1) (a,b) " + makeValuesClause(tableData1));
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p=2) (a,b) " + makeValuesClause(tableData2));

    // All inserts are committed and hence would expect in TXN_TO_WRITE_ID, 3 entries for acidTbl
    // and 2 entries for acidTblPart as each insert would have allocated a writeid.
    String acidTblWhereClause = " where t2w_database = " + quoteString("default")
            + " and t2w_table = " + quoteString(Table.ACIDTBL.name().toLowerCase());
    String acidTblPartWhereClause = " where t2w_database = " + quoteString("default")
            + " and t2w_table = " + quoteString(Table.ACIDTBLPART.name().toLowerCase());
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from TXN_TO_WRITE_ID" + acidTblWhereClause),
            3, TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXN_TO_WRITE_ID" + acidTblWhereClause));
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from TXN_TO_WRITE_ID" + acidTblPartWhereClause),
            2, TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXN_TO_WRITE_ID" + acidTblPartWhereClause));

    txnHandler.compact(new CompactionRequest("default", Table.ACIDTBL.name().toLowerCase(), CompactionType.MAJOR));
    runWorker(hiveConf);
    runCleaner(hiveConf);
    txnHandler.cleanTxnToWriteIdTable();

    // After compaction/cleanup, all entries from TXN_TO_WRITE_ID should be cleaned up as all txns are committed.
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from TXN_TO_WRITE_ID"),
            0, TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXN_TO_WRITE_ID"));

    // Following sequence of commit-abort-open-abort-commit.
    int[][] tableData4 = {{4, 5}};
    int[][] tableData5 = {{5, 6}};
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p=3) (a,b) " + makeValuesClause(tableData3));

    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("insert into " +  Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData4));
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, false);

    // Keep an open txn which refers to the aborted txn.
    Context ctx = new Context(hiveConf);
    HiveTxnManager txnMgr = TxnManagerFactory.getTxnManagerFactory().getTxnManager(hiveConf);
    txnMgr.openTxn(ctx, "u1");
    txnMgr.getValidTxns();

    // Start an INSERT statement transaction and roll back this transaction.
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("insert into " +  Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData5));
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, false);

    runStatementOnDriver("insert into " +  Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData5));

    // We would expect 4 entries in TXN_TO_WRITE_ID as each insert would have allocated a writeid
    // including aborted one.
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from TXN_TO_WRITE_ID" + acidTblWhereClause),
            3, TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXN_TO_WRITE_ID" + acidTblWhereClause));
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from TXN_TO_WRITE_ID" + acidTblPartWhereClause),
            1, TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXN_TO_WRITE_ID" + acidTblPartWhereClause));

    // The entry relevant to aborted txns shouldn't be removed from TXN_TO_WRITE_ID as
    // aborted txn would be removed from TXNS only after the compaction. Also, committed txn > open txn is retained.
    // As open txn doesn't allocate writeid, the 2 entries for aborted and committed should be retained.
    txnHandler.cleanEmptyAbortedAndCommittedTxns();
    txnHandler.cleanTxnToWriteIdTable();
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from TXN_TO_WRITE_ID" + acidTblWhereClause),
            3, TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXN_TO_WRITE_ID" + acidTblWhereClause));
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from TXN_TO_WRITE_ID" + acidTblPartWhereClause),
            0, TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXN_TO_WRITE_ID" + acidTblPartWhereClause));

    // The cleaner after the compaction will not run, since the open txnId < compaction commit txnId
    // Since aborted txns data/metadata was not removed, all data in TXN_TO_WRITE_ID should remain
    txnHandler.compact(new CompactionRequest("default", Table.ACIDTBL.name().toLowerCase(), CompactionType.MAJOR));
    runWorker(hiveConf);
    runCleaner(hiveConf);
    txnHandler.cleanEmptyAbortedAndCommittedTxns();
    txnHandler.cleanTxnToWriteIdTable();
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from TXN_TO_WRITE_ID"),
            3, TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXN_TO_WRITE_ID"));

    // Commit the open txn, which lets the cleanup on TXN_TO_WRITE_ID.
    txnMgr.commitTxn();
    txnMgr.openTxn(ctx, "u1");
    txnMgr.getValidTxns();
    // The txn opened after the compaction commit should not effect the Cleaner
    runCleaner(hiveConf);
    txnHandler.cleanEmptyAbortedAndCommittedTxns();
    txnHandler.cleanTxnToWriteIdTable();

    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from TXN_TO_WRITE_ID"),
            0, TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXN_TO_WRITE_ID"));
  }

  @Test
  public void testMmTableAbortWithCompaction() throws Exception {
    // 1. Insert some rows into MM table
    runStatementOnDriver("insert into " + Table.MMTBL + "(a,b) values(1,2)");
    // There should be 1 delta directory
    int [][] resultData1 =  new int[][] {{1,2}};
    verifyDeltaDirAndResult(1, Table.MMTBL.toString(), "", resultData1);
    List<String> r1 = runStatementOnDriver("select count(*) from " + Table.MMTBL);
    Assert.assertEquals("1", r1.get(0));

    // 2. Let a transaction be aborted
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("insert into " + Table.MMTBL + "(a,b) values(3,4)");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, false);
    // There should be 1 delta and 1 base directory. The base one is the aborted one.
    verifyDeltaDirAndResult(2, Table.MMTBL.toString(), "", resultData1);

    r1 = runStatementOnDriver("select count(*) from " + Table.MMTBL);
    Assert.assertEquals("1", r1.get(0));

    // Verify query result
    int [][] resultData2 = new int[][] {{1,2}, {5,6}};

    runStatementOnDriver("insert into " + Table.MMTBL + "(a,b) values(5,6)");
    verifyDeltaDirAndResult(3, Table.MMTBL.toString(), "", resultData2);
    r1 = runStatementOnDriver("select count(*) from " + Table.MMTBL);
    Assert.assertEquals("2", r1.get(0));

    // 4. Perform a MINOR compaction, expectation is it should remove aborted base dir
    runStatementOnDriver("alter table "+ Table.MMTBL + " compact 'MINOR'");
    // The worker should remove the subdir for aborted transaction
    runWorker(hiveConf);
    verifyDeltaDirAndResult(3, Table.MMTBL.toString(), "", resultData2);
    verifyBaseDir(0, Table.MMTBL.toString(), "");
    // 5. Run Cleaner. Shouldn't impact anything.
    runCleaner(hiveConf);
    // 6. Run initiator remove aborted entry from TXNS table
    runInitiator(hiveConf);

    // Verify query result
    List<String> rs = runStatementOnDriver("select a,b from " + Table.MMTBL + " order by a");
    Assert.assertEquals(stringifyValues(resultData2), rs);

    int [][] resultData3 = new int[][] {{1,2}, {5,6}, {7,8}};
    // 7. add few more rows
    runStatementOnDriver("insert into " + Table.MMTBL + "(a,b) values(7,8)");
    // 8. add one more aborted delta
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("insert into " + Table.MMTBL + "(a,b) values(9,10)");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, false);

    // 9. Perform a MAJOR compaction, expectation is it should remove aborted base dir
    runStatementOnDriver("alter table "+ Table.MMTBL + " compact 'MAJOR'");
    verifyDeltaDirAndResult(3, Table.MMTBL.toString(), "", resultData3);
    runWorker(hiveConf);
    verifyDeltaDirAndResult(2, Table.MMTBL.toString(), "", resultData3);
    verifyBaseDir(1, Table.MMTBL.toString(), "");
    runCleaner(hiveConf);
    verifyDeltaDirAndResult(0, Table.MMTBL.toString(), "", resultData3);
    verifyBaseDir(1, Table.MMTBL.toString(), "");
    runInitiator(hiveConf);
    verifyDeltaDirAndResult(0, Table.MMTBL.toString(), "", resultData3);
    verifyBaseDir(1, Table.MMTBL.toString(), "");

    // Verify query result
    rs = runStatementOnDriver("select a,b from " + Table.MMTBL + " order by a");
    Assert.assertEquals(stringifyValues(resultData3), rs);
  }

  @Test
  public void testMmTableAbortWithCompactionNoCleanup() throws Exception {
    // 1. Insert some rows into MM table
    runStatementOnDriver("insert into " + Table.MMTBL + "(a,b) values(1,2)");
    runStatementOnDriver("insert into " + Table.MMTBL + "(a,b) values(5,6)");
    // There should be 1 delta directory
    int [][] resultData1 =  new int[][] {{1,2}, {5,6}};
    verifyDeltaDirAndResult(2, Table.MMTBL.toString(), "", resultData1);
    List<String> r1 = runStatementOnDriver("select count(*) from " + Table.MMTBL);
    Assert.assertEquals("2", r1.get(0));

    // 2. Let a transaction be aborted
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("insert into " + Table.MMTBL + " values(3,4)");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, false);
    // There should be 1 delta and 1 base directory. The base one is the aborted one.
    verifyDeltaDirAndResult(3, Table.MMTBL.toString(), "", resultData1);
    r1 = runStatementOnDriver("select count(*) from " + Table.MMTBL);
    Assert.assertEquals("2", r1.get(0));

    // 3. Perform a MINOR compaction, expectation is it should remove aborted base dir
    runStatementOnDriver("alter table "+ Table.MMTBL + " compact 'MINOR'");
    // The worker should remove the subdir for aborted transaction
    runWorker(hiveConf);
    verifyDeltaDirAndResult(3, Table.MMTBL.toString(), "", resultData1);
    verifyBaseDir(0, Table.MMTBL.toString(), "");
    // Verify query result
    List<String> rs = runStatementOnDriver("select a,b from " + Table.MMTBL + " order by a");
    Assert.assertEquals(stringifyValues(resultData1), rs);

    int [][] resultData3 = new int[][] {{1,2}, {5,6}, {7,8}};
    // 4. add few more rows
    runStatementOnDriver("insert into " + Table.MMTBL + "(a,b) values(7,8)");
    // 5. add one more aborted delta
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("insert into " + Table.MMTBL + "(a,b) values(9,10)");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, false);
    verifyDeltaDirAndResult(5, Table.MMTBL.toString(), "", resultData3);

    // 6. Perform a MAJOR compaction, expectation is it should remove aborted delta dir
    runStatementOnDriver("alter table "+ Table.MMTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    verifyDeltaDirAndResult(4, Table.MMTBL.toString(), "", resultData3);
    verifyBaseDir(1, Table.MMTBL.toString(), "");

    // 7. Run one more Major compaction this should have been refused because there are no changes in the table
    CompactionResponse resp = txnHandler.compact(new CompactionRequest("default", Table.MMTBL.name.toLowerCase(), CompactionType.MAJOR));
    Assert.assertFalse(resp.isAccepted());
    Assert.assertEquals(TxnStore.REFUSED_RESPONSE, resp.getState());
    Assert.assertEquals("Compaction is already scheduled with state='ready for cleaning' and id=2", resp.getErrormessage());

    runCleaner(hiveConf);

    // Verify query result
    rs = runStatementOnDriver("select a,b from " + Table.MMTBL + " order by a");
    Assert.assertEquals(stringifyValues(resultData3), rs);
  }

  @Test
  public void testDynPartInsertWithAborts() throws Exception {
    boolean useCleanerForAbortCleanup = MetastoreConf.getBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER);
    int[][] resultData = new int[][]{{1, 1}, {2, 2}};
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p1'),(2,2,'p1')");
    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p1", resultData);

    // forcing a txn to abort before addDynamicPartitions
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_LOAD_DYNAMIC_PARTITION, true);
    runStatementOnDriverWithAbort("insert into " + Table.ACIDTBLPART + " partition(p) values(3,3,'p1'),(4,4,'p1')");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_LOAD_DYNAMIC_PARTITION, false);
    verifyDeltaDirAndResult(2, Table.ACIDTBLPART.toString(), "p=p1", resultData);

    int count = TestTxnDbUtil
        .countQueryAgent(hiveConf, "select count(*) from TXN_COMPONENTS where TC_OPERATION_TYPE='i'");
    // We should have 1 row corresponding to the aborted transaction
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from TXN_COMPONENTS"), 1, count);

    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD, 0, TimeUnit.MILLISECONDS);
    runInitiator(hiveConf);

    count = TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from COMPACTION_QUEUE");
    // Only one job is added to the queue per table. This job corresponds to all the entries for a particular table
    // with rows in TXN_COMPONENTS
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from COMPACTION_QUEUE"),
            useCleanerForAbortCleanup ? 0 : 1, count);

    runWorker(hiveConf);
    runCleaner(hiveConf);

    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p1", resultData);
  }

  @Test
  public void testDynPartInsertWithMultiPartitionAborts() throws Exception {
    boolean useCleanerForAbortCleanup = MetastoreConf.getBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER);
    int [][] resultData = new int[][] {{1,1}, {2,2}};
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p1'),(2,2,'p1')");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p2'),(2,2,'p2')");
    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p1", resultData);
    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p2", resultData);
    List<String> r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBLPART);
    Assert.assertEquals("4", r1.get(0));

    // forcing a txn to abort before addDynamicPartitions
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_LOAD_DYNAMIC_PARTITION, true);
    runStatementOnDriverWithAbort("insert into " + Table.ACIDTBLPART + " partition(p) values(3,3,'p1'),(4,4,'p1')");
    runStatementOnDriverWithAbort("insert into " + Table.ACIDTBLPART + " partition(p) values(3,3,'p2'),(4,4,'p2')");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_LOAD_DYNAMIC_PARTITION, false);
    verifyDeltaDirAndResult(2, Table.ACIDTBLPART.toString(), "p=p1", resultData);
    verifyDeltaDirAndResult(2, Table.ACIDTBLPART.toString(), "p=p2", resultData);
    r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBLPART);
    Assert.assertEquals("4", r1.get(0));

    int count = TestTxnDbUtil
        .countQueryAgent(hiveConf, "select count(*) from TXN_COMPONENTS where TC_OPERATION_TYPE='i'");
    // We should have 2 rows corresponding to the two aborted transactions
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from TXN_COMPONENTS"), 2, count);

    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD, 0, TimeUnit.MILLISECONDS);
    runInitiator(hiveConf);

    count = TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from COMPACTION_QUEUE");
    // Only one job is added to the queue per table. This job corresponds to all the entries for a particular table
    // with rows in TXN_COMPONENTS
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from COMPACTION_QUEUE"),
            useCleanerForAbortCleanup ? 0 : 1, count);

    r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBLPART);
    Assert.assertEquals("4", r1.get(0));

    runWorker(hiveConf);
    runCleaner(hiveConf);

    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p1", resultData);
    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p2", resultData);
    r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBLPART);
    Assert.assertEquals("4", r1.get(0));
  }

  @Test
  public void testDynPartIOWWithAborts() throws Exception {
    boolean useCleanerForAbortCleanup = MetastoreConf.getBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER);
    int [][] resultData = new int[][] {{1,1}, {2,2}};
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p1'),(2,2,'p1')");
    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p1", resultData);

    // forcing a txn to abort before addDynamicPartitions
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_LOAD_DYNAMIC_PARTITION, true);
    runStatementOnDriverWithAbort("insert overwrite table " + Table.ACIDTBLPART + " partition(p) values(3,3,'p1'),(4,4,'p1')");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_LOAD_DYNAMIC_PARTITION, false);
    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p1", resultData);
    verifyBaseDir(1, Table.ACIDTBLPART.toString(), "p=p1");

    int count = TestTxnDbUtil
        .countQueryAgent(hiveConf, "select count(*) from TXN_COMPONENTS where TC_OPERATION_TYPE='u'");
    // We should have 1 row corresponding to the aborted transaction
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from TXN_COMPONENTS"), 1, count);

    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD, 0, TimeUnit.MILLISECONDS);
    runInitiator(hiveConf);

    count = TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from COMPACTION_QUEUE");
    // Only one job is added to the queue per table. This job corresponds to all the entries for a particular table
    // with rows in TXN_COMPONENTS
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from COMPACTION_QUEUE"),
            useCleanerForAbortCleanup ? 0 : 1, count);

    runWorker(hiveConf);
    runCleaner(hiveConf);

    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p1", resultData);
    verifyBaseDir(0, Table.ACIDTBLPART.toString(), "p=p1");
  }

  @Test
  public void testDynPartIOWWithMultiPartitionAborts() throws Exception {
    boolean useCleanerForAbortCleanup = MetastoreConf.getBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER);
    int [][] resultData = new int[][] {{1,1}, {2,2}};
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p1'),(2,2,'p1')");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p2'),(2,2,'p2')");
    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p1", resultData);
    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p2", resultData);
    List<String> r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBLPART);
    Assert.assertEquals("4", r1.get(0));

    // forcing a txn to abort before addDynamicPartitions
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_LOAD_DYNAMIC_PARTITION, true);
    runStatementOnDriverWithAbort("insert overwrite table " + Table.ACIDTBLPART + " partition(p) values(3,3,'p1'),(4,4,'p1')");
    runStatementOnDriverWithAbort("insert overwrite table " + Table.ACIDTBLPART + " partition(p) values(3,3,'p2'),(4,4,'p2')");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_LOAD_DYNAMIC_PARTITION, false);
    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p1", resultData);
    verifyBaseDir(1, Table.ACIDTBLPART.toString(), "p=p1");
    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p2", resultData);
    verifyBaseDir(1, Table.ACIDTBLPART.toString(), "p=p2");
    r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBLPART);
    Assert.assertEquals("4", r1.get(0));

    int count = TestTxnDbUtil
        .countQueryAgent(hiveConf, "select count(*) from TXN_COMPONENTS where TC_OPERATION_TYPE='u'");
    // We should have two rows corresponding to the two aborted transactions
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from TXN_COMPONENTS"), 2, count);

    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD, 0, TimeUnit.MILLISECONDS);
    runInitiator(hiveConf);

    count = TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from COMPACTION_QUEUE");
    // Only one job is added to the queue per table. This job corresponds to all the entries for a particular table
    // with rows in TXN_COMPONENTS
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from COMPACTION_QUEUE"),
            useCleanerForAbortCleanup ? 0 : 1, count);

    r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBLPART);
    Assert.assertEquals("4", r1.get(0));

    runWorker(hiveConf);
    runCleaner(hiveConf);

    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p1", resultData);
    verifyBaseDir(0, Table.ACIDTBLPART.toString(), "p=p1");
    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p2", resultData);
    verifyBaseDir(0, Table.ACIDTBLPART.toString(), "p=p2");
    r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBLPART);
    Assert.assertEquals("4", r1.get(0));
  }

  @Test
  public void testDynPartUpdateWithAborts() throws Exception {
    boolean useCleanerForAbortCleanup = MetastoreConf.getBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER);
    int[][] resultData1 = new int[][]{{1, 2}, {3, 4}};
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values (1,2,'p1')");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values (3,4,'p1')");
    verifyDeltaDirAndResult(2, Table.ACIDTBLPART.toString(), "p=p1", resultData1);

    // forcing a txn to abort before addDynamicPartitions
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_LOAD_DYNAMIC_PARTITION, true);
    runStatementOnDriverWithAbort("update " + Table.ACIDTBLPART + " set b=a+2 where a<5");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_LOAD_DYNAMIC_PARTITION, false);
    verifyDeltaDirAndResult(2, Table.ACIDTBLPART.toString(), "p=p1", resultData1);
    verifyDeleteDeltaDir(1, Table.ACIDTBLPART.toString(), "p=p1");

    int count = TestTxnDbUtil
        .countQueryAgent(hiveConf, "select count(*) from TXN_COMPONENTS where TC_OPERATION_TYPE='u'");
    // We should have 1 row corresponding to the aborted transaction
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from TXN_COMPONENTS"), 1, count);

    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD, 0, TimeUnit.MILLISECONDS);
    runInitiator(hiveConf);

    count = TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from COMPACTION_QUEUE");
    // Only one job is added to the queue per table. This job corresponds to all the entries for a particular table
    // with rows in TXN_COMPONENTS
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from COMPACTION_QUEUE"),
            useCleanerForAbortCleanup ? 0 : 1, count);

    runWorker(hiveConf);
    runCleaner(hiveConf);

    verifyDeltaDirAndResult(2, Table.ACIDTBLPART.toString(), "p=p1", resultData1);
    verifyDeleteDeltaDir(0, Table.ACIDTBLPART.toString(), "p=p1");
  }

  @Test
  public void testDynPartMergeWithAborts() throws Exception {
    boolean useCleanerForAbortCleanup = MetastoreConf.getBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER);
    int [][] resultData = new int[][] {{1,1}, {2,2}};
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p1'),(2,2,'p1')");
    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p1", resultData);
    List<String> r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBLPART);
    Assert.assertEquals("2", r1.get(0));

    int[][] sourceVals = {{2,15},{4,44},{5,5},{11,11}};
    runStatementOnDriver("insert into " + TestTxnCommands2.Table.NONACIDORCTBL + " " + makeValuesClause(sourceVals));

    // forcing a txn to abort before addDynamicPartitions
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_LOAD_DYNAMIC_PARTITION, true);
    runStatementOnDriverWithAbort("merge into " + Table.ACIDTBLPART + " using " + TestTxnCommands2.Table.NONACIDORCTBL +
      " as s ON " + Table.ACIDTBLPART + ".a = s.a " +
      "when matched then update set b = s.b " +
      "when not matched then insert values(s.a, s.b, 'newpart')");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_LOAD_DYNAMIC_PARTITION, false);
    verifyDeltaDirAndResult(2, Table.ACIDTBLPART.toString(), "p=p1", resultData);
    verifyDeleteDeltaDir(1, Table.ACIDTBLPART.toString(), "p=p1");
    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=newpart", resultData);
    r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBLPART);
    Assert.assertEquals("2", r1.get(0));

    int count = TestTxnDbUtil
        .countQueryAgent(hiveConf, "select count(*) from TXN_COMPONENTS where TC_OPERATION_TYPE='u'");
    // We should have 1 row corresponding to the aborted transaction
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from TXN_COMPONENTS"), 1, count);

    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD, 0, TimeUnit.MILLISECONDS);
    runInitiator(hiveConf);

    count = TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from COMPACTION_QUEUE");
    // Only one job is added to the queue per table. This job corresponds to all the entries for a particular table
    // with rows in TXN_COMPONENTS
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from COMPACTION_QUEUE"),
            useCleanerForAbortCleanup ? 0 : 1, count);

    r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBLPART);
    Assert.assertEquals("2", r1.get(0));

    runWorker(hiveConf);
    runCleaner(hiveConf);

    verifyDeltaDirAndResult(1, Table.ACIDTBLPART.toString(), "p=p1", resultData);
    verifyDeleteDeltaDir(0, Table.ACIDTBLPART.toString(), "p=p1");
    verifyDeltaDirAndResult(0, Table.ACIDTBLPART.toString(), "p=newpart", resultData);
    r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBLPART);
    Assert.assertEquals("2", r1.get(0));
  }

  @Test
  public void testFullACIDAbortWithMinorMajorCompaction() throws Exception {
    // 1. Insert some rows into acid table
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(1,2)");
    // There should be 1 delta directory
    int [][] resultData1 =  new int[][] {{1,2}};
    verifyDeltaDirAndResult(1, Table.ACIDTBL.toString(), "", resultData1);
    List<String> r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBL);
    Assert.assertEquals("1", r1.get(0));

    // 2. Let a transaction be aborted
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(3,4)");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, false);
    // There should be 2 delta directories.
    verifyDeltaDirAndResult(2, Table.ACIDTBL.toString(), "", resultData1);

    r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBL);
    Assert.assertEquals("1", r1.get(0));

    // Verify query result
    int [][] resultData2 = new int[][] {{1,2}, {5,6}};
    // 3. insert few more rows in acid table
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(5,6)");
    verifyDeltaDirAndResult(3, Table.ACIDTBL.toString(), "", resultData2);
    r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBL);
    Assert.assertEquals("2", r1.get(0));

    // 4. Perform a MINOR compaction
    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MINOR'");
    runWorker(hiveConf);
    verifyDeltaDirAndResult(4, Table.ACIDTBL.toString(), "", resultData2);
    verifyBaseDir(0, Table.ACIDTBL.toString(), "");
    // 5. Run Cleaner, should remove compacted deltas including aborted ones.
    runCleaner(hiveConf);
    verifyDeltaDirAndResult(1, Table.ACIDTBL.toString(), "", resultData2);

    // Verify query result
    List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a");
    Assert.assertEquals(stringifyValues(resultData2), rs);

    int [][] resultData3 = new int[][] {{1,2}, {5,6}, {7,8}, {9,10}};
    // 6. add few more rows
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(7,8)");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(9,10)");
    // 7. add one more aborted delta
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(11,12)");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, false);

    // 8. Perform a MAJOR compaction
    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MAJOR'");
    verifyDeltaDirAndResult(4, Table.ACIDTBL.toString(), "", resultData3);
    runWorker(hiveConf);
    verifyDeltaDirAndResult(4, Table.ACIDTBL.toString(), "", resultData3);
    verifyBaseDir(1, Table.ACIDTBL.toString(), "");
    // The cleaner should remove compacted deltas including aborted ones.
    runCleaner(hiveConf);
    verifyDeltaDirAndResult(0, Table.ACIDTBL.toString(), "", resultData3);
    verifyBaseDir(1, Table.ACIDTBL.toString(), "");

    // Verify query result
    rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a");
    Assert.assertEquals(stringifyValues(resultData3), rs);
  }

  @Test
  public void testFullACIDAbortWithMajorCompaction() throws Exception {
    // 1. Insert some rows into acid table
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(1,2)");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(3,4)");
    // There should be 2 delta directories
    int [][] resultData1 =  new int[][] {{1,2}, {3,4}};
    verifyDeltaDirAndResult(2, Table.ACIDTBL.toString(), "", resultData1);
    List<String> r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBL);
    Assert.assertEquals("2", r1.get(0));

    // 2. Let a transaction be aborted
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(5,6)");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, false);
    // There should be 2 delta and 1 base directory. The base one is the aborted one.
    verifyDeltaDirAndResult(3, Table.ACIDTBL.toString(), "", resultData1);
    r1 = runStatementOnDriver("select count(*) from " + Table.ACIDTBL);
    Assert.assertEquals("2", r1.get(0));

    // 3.Perform a MAJOR compaction
    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MAJOR'");
    runWorker(hiveConf);

    verifyDeltaDirAndResult(3, Table.ACIDTBL.toString(), "", resultData1);
    verifyBaseDir(1, Table.ACIDTBL.toString(), "");
    // 4. Run Cleaner, should remove compacted deltas including aborted ones.
    runCleaner(hiveConf);
    verifyDeltaDirAndResult(0, Table.ACIDTBL.toString(), "", resultData1);
    verifyBaseDir(1, Table.ACIDTBL.toString(), "");

    // Verify query result
    List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a");
    Assert.assertEquals(stringifyValues(resultData1), rs);
  }

  @Test
  public void testFullACIDAbortWithCompactionNoCleanup() throws Exception {
    // 1. Insert some rows into acid table
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(1,2)");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(3,4)");
    // There should be 2 delta directories
    int [][] resultData1 =  new int[][] {{1,2}, {3,4}};
    verifyDeltaDirAndResult(2, Table.ACIDTBL.toString(), "", resultData1);

    // 2. abort one txns
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(5,6)");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, false);
    verifyDeltaDirAndResult(3, Table.ACIDTBL.toString(), "", resultData1);

    // 3. Perform a MAJOR compaction.
    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    verifyDeltaDirAndResult(3, Table.ACIDTBL.toString(), "", resultData1);
    verifyBaseDir(1, Table.ACIDTBL.toString(), "");

    // Verify query result
    List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a");
    Assert.assertEquals(stringifyValues(resultData1), rs);
  }

  @Test
  public void testFullACIDAbortWithManyPartitions() throws Exception {
    // 1. Insert some rows into acid table
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p1') (a,b) values(1,2)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p2') (a,b) values(1,2)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p3') (a,b) values(1,2)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p1') (a,b) values(3,4)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p2') (a,b) values(3,4)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p3') (a,b) values(3,4)");
    // There should be 2 delta directories in each partition
    int [][] resultData1 =  new int[][] {{1,2}, {3,4}};
    verifyDeltaDirAndResult(2, Table.ACIDTBLPART.toString(), "p=p1", resultData1);
    verifyDeltaDirAndResult(2, Table.ACIDTBLPART.toString(), "p=p2", resultData1);
    verifyDeltaDirAndResult(2, Table.ACIDTBLPART.toString(), "p=p3", resultData1);

    // 2. abort two txns in each partition
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p1') (a,b) values(5,6)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p2') (a,b) values(5,6)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p3') (a,b) values(5,6)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p1') (a,b) values(5,6)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p2') (a,b) values(5,6)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p3') (a,b) values(5,6)");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, false);
    verifyDeltaDirAndResult(4, Table.ACIDTBLPART.toString(), "p=p1", resultData1);
    verifyDeltaDirAndResult(4, Table.ACIDTBLPART.toString(), "p=p2", resultData1);
    verifyDeltaDirAndResult(4, Table.ACIDTBLPART.toString(), "p=p3", resultData1);

    // We should have total six rows corresponding to the above six aborted transactions
    Assert.assertEquals(TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXN_COMPONENTS"), 6);
    runCleaner(hiveConf);
    Assert.assertEquals(TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXN_COMPONENTS"), 6);

    // 3.1 Perform a MAJOR compaction on p='p1'.
    runStatementOnDriver("alter table "+ Table.ACIDTBLPART + " partition(p='p1') compact 'MAJOR'");
    runWorker(hiveConf);
    verifyDeltaDirAndResult(4, Table.ACIDTBLPART.toString(), "p=p1", resultData1);
    verifyDeltaDirAndResult(4, Table.ACIDTBLPART.toString(), "p=p2", resultData1);
    verifyDeltaDirAndResult(4, Table.ACIDTBLPART.toString(), "p=p3", resultData1);
    verifyBaseDir(1, Table.ACIDTBLPART.toString(), "p=p1");
    // The cleaner should remove compacted deltas including aborted ones.
    runCleaner(hiveConf);
    Assert.assertEquals(TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXN_COMPONENTS"), 4);

    // 3.2 Perform a MAJOR compaction on p='p2'.
    runStatementOnDriver("alter table "+ Table.ACIDTBLPART + " partition(p='p2') compact 'MAJOR'");
    runWorker(hiveConf);
    verifyDeltaDirAndResult(0, Table.ACIDTBLPART.toString(), "p=p1", resultData1);
    verifyDeltaDirAndResult(4, Table.ACIDTBLPART.toString(), "p=p2", resultData1);
    verifyDeltaDirAndResult(4, Table.ACIDTBLPART.toString(), "p=p3", resultData1);
    verifyBaseDir(1, Table.ACIDTBLPART.toString(), "p=p2");
    // The cleaner should remove compacted deltas including aborted ones.
    runCleaner(hiveConf);
    Assert.assertEquals(TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXN_COMPONENTS"), 2);

    // 3.3 Perform a MAJOR compaction on p='p3'.
    runStatementOnDriver("alter table "+ Table.ACIDTBLPART + " partition(p='p3') compact 'MAJOR'");
    runWorker(hiveConf);
    verifyDeltaDirAndResult(0, Table.ACIDTBLPART.toString(), "p=p1", resultData1);
    verifyDeltaDirAndResult(0, Table.ACIDTBLPART.toString(), "p=p2", resultData1);
    verifyDeltaDirAndResult(4, Table.ACIDTBLPART.toString(), "p=p3", resultData1);
    verifyBaseDir(1, Table.ACIDTBLPART.toString(), "p=p3");
    // The cleaner should remove compacted deltas including aborted ones.
    runCleaner(hiveConf);
    verifyDeltaDirAndResult(0, Table.ACIDTBLPART.toString(), "p=p3", resultData1);
    Assert.assertEquals(TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXN_COMPONENTS"), 0);

    // 4. Verify query result
    int [][] resultData2 =  new int[][] {{1,2},{1,2},{1,2},{3,4},{3,4},{3,4}};
    List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBLPART + " order by a");
    Assert.assertEquals(stringifyValues(resultData2), rs);
  }

  private void verifyDeltaDir(int expectedDeltas, String tblName, String partName) throws Exception {
    verifyDir(expectedDeltas, tblName, partName, "delta_.*");
  }

  private void verifyDeleteDeltaDir(int expectedDeltas, String tblName, String partName) throws Exception {
    verifyDir(expectedDeltas, tblName, partName, "delete_delta_.*");
  }

  private void verifyBaseDir(int expectedDeltas, String tblName, String partName) throws Exception {
    verifyDir(expectedDeltas, tblName, partName, "base_.*");
  }

  private void verifyDir(int expectedDeltas, String tblName, String partName, String pattern) throws Exception {
    Path warehouse = new Path(getWarehouseDir());
    tblName = tblName.toLowerCase();

    FileSystem fs = FileSystem.get(warehouse.toUri(), hiveConf);
    FileStatus[] status = fs.listStatus(new Path(warehouse, tblName + "/" + partName),
        FileUtils.HIDDEN_FILES_PATH_FILTER);

    int sawDeltaTimes = 0;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches(pattern)) {
        sawDeltaTimes++;
      }
    }
    Assert.assertEquals(expectedDeltas, sawDeltaTimes);
  }

  private void verifyDeltaDirAndResult(int expectedDeltas, String tblName, String partName, int [][] resultData) throws Exception {
    verifyDeltaDir(expectedDeltas, tblName, partName);
    if (partName.equals("p=newpart")) return;

    List<String> rs = runStatementOnDriver("select a,b from " + tblName + (partName.isEmpty() ?
      "" : " where p='" + partName.substring(2) + "'") + " order by a");
    Assert.assertEquals(stringifyValues(resultData), rs);
  }

  @Test
  public void testAcidOrcWritePreservesFieldNames() throws Exception {
    // with vectorization
    String tableName = "acidorcwritefieldnames";
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    runStatementOnDriver("DROP TABLE IF EXISTS " + tableName);
    runStatementOnDriver("CREATE TABLE " + tableName + " (a INT, b STRING) CLUSTERED BY (a) INTO " + BUCKET_COUNT + " BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("INSERT INTO " + tableName + " VALUES (1, 'foo'), (2, 'bar')");

    tableName = "acidorcwritefieldnames_complex";
    runStatementOnDriver("DROP TABLE IF EXISTS " + tableName);
    runStatementOnDriver("CREATE TABLE " + tableName + " (a INT, b STRING, s STRUCT<c:int, si:STRUCT<d:double," +
      "e:float>>) CLUSTERED BY (a) INTO " + BUCKET_COUNT +
      " BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("INSERT INTO " + tableName + " select a, b, named_struct('c',10,'si'," +
      "named_struct('d',cast(1.0 as double),'e',cast(2.0 as float))) from acidorcwritefieldnames");

    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] fileStatuses = fs.globStatus(new Path(getWarehouseDir() + "/" + tableName + "/" + AcidUtils.DELTA_PREFIX + "*/" + AcidUtils.BUCKET_PREFIX + "*"));
    Assert.assertEquals(BUCKET_COUNT, fileStatuses.length);

    OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(hiveConf);
    for (FileStatus fileStatus : fileStatuses) {
      Reader r = OrcFile.createReader(fileStatus.getPath(), readerOptions);
      TypeDescription rowSchema = r.getSchema().getChildren().get(5);
      Assert.assertEquals("struct<a:int,b:string,s:struct<c:int,si:struct<d:double,e:float>>>", rowSchema.toString());
    }

    // without vectorization
    tableName = "acidorcwritefieldnames";
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    runStatementOnDriver("DROP TABLE IF EXISTS " + tableName);
    runStatementOnDriver("CREATE TABLE " + tableName + " (a INT, b STRING) CLUSTERED BY (a) INTO " + BUCKET_COUNT + " BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("INSERT INTO " + tableName + " VALUES (1, 'foo'), (2, 'bar')");

    tableName = "acidorcwritefieldnames_complex";
    runStatementOnDriver("DROP TABLE IF EXISTS " + tableName);
    runStatementOnDriver("CREATE TABLE " + tableName + " (a INT, b STRING, s STRUCT<c:int, si:STRUCT<d:double," +
      "e:float>>) CLUSTERED BY (a) INTO " + BUCKET_COUNT +
      " BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("INSERT INTO " + tableName + " select a, b, named_struct('c',10,'si'," +
      "named_struct('d',cast(1.0 as double),'e',cast(2.0 as float))) from acidorcwritefieldnames");

    fs = FileSystem.get(hiveConf);
    fileStatuses = fs.globStatus(new Path(getWarehouseDir() + "/" + tableName + "/" + AcidUtils.DELTA_PREFIX + "*/" + AcidUtils.BUCKET_PREFIX + "*"));
    Assert.assertEquals(BUCKET_COUNT, fileStatuses.length);

    readerOptions = OrcFile.readerOptions(hiveConf);
    for (FileStatus fileStatus : fileStatuses) {
      Reader r = OrcFile.createReader(fileStatus.getPath(), readerOptions);
      TypeDescription rowSchema = r.getSchema().getChildren().get(5);
      Assert.assertEquals("struct<a:int,b:string,s:struct<c:int,si:struct<d:double,e:float>>>", rowSchema.toString());
    }
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
  }

  public static Stream<Arguments> generateBooleanArgs() {
    // Generates the required boolean input for the 20 test cases
    return IntStream.concat(IntStream.range(0, 16), IntStream.range(24, 28)).mapToObj(i ->
            Arguments.of((i & 1) == 0, ((i >>> 1) & 1) == 0, ((i >>> 2) & 1) == 0,
                    ((i >>> 3) & 1) == 0, ((i >>> 4) & 1) == 0));
  }

  @ParameterizedTest
  @MethodSource("generateBooleanArgs")
  public void testFailureScenariosCleanupCTAS(boolean isPartitioned,
                                         boolean isDirectInsertEnabled,
                                         boolean isLocklessReadsEnabled,
                                         boolean isLocationUsed,
                                         boolean isExclusiveCtas) throws Exception {
    String tableName = "atable";

    //Set configurations
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_ACID_DIRECT_INSERT_ENABLED, isDirectInsertEnabled);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, isLocklessReadsEnabled);
    hiveConf.setBoolVar(HiveConf.ConfVars.TXN_CTAS_X_LOCK, isExclusiveCtas);

    // Add a '1' at the end of table name for custom location.
    String querylocation = (isLocationUsed) ? " location '" + getWarehouseDir() + "/" + tableName + "1'" : "";
    String queryPartitions = (isPartitioned) ? " partitioned by (a)" : "";

    d.run("insert into " + Table.ACIDTBL + "(a,b) values (3,4)");
    d.run("drop table if exists " + tableName);
    d.compileAndRespond("create table " + tableName + queryPartitions + " stored as orc" + querylocation +
            " tblproperties ('transactional'='true') as select * from " + Table.ACIDTBL);
    long txnId = d.getQueryState().getTxnManager().getCurrentTxnId();
    mockTasksRecursively(d.getPlan().getRootTasks());
    int assertError = 0;
    try {
      d.run();
    } catch (Exception e) {
      assertError = 1;
    }

    runCleaner(hiveConf);

    Assert.assertEquals(assertError, 1);

    FileSystem fs = FileSystem.get(hiveConf);
    String assertLocation = (isLocationUsed) ? getWarehouseDir() + "/" + tableName + "1" :
            ((isLocklessReadsEnabled) ? getWarehouseDir() + "/" + tableName + AcidUtils.getPathSuffix(txnId)
                    : getWarehouseDir() + "/" + tableName);

    FileStatus[] fileStatuses = fs.globStatus(new Path(assertLocation + "/*"));
    for (FileStatus fileStatus : fileStatuses) {
      Assert.assertFalse(fileStatus.getPath().getName().startsWith(AcidUtils.DELTA_PREFIX));
    }
  }

  public void mockTasksRecursively(List<Task<?>> tasks) {
    for (int i = 0;i < tasks.size();i++) {
      Task<?> task = tasks.get(i);
      if (task instanceof DDLTask) {
        DDLTask ddltask = Mockito.spy(new DDLTask());
        Mockito.doThrow(new RuntimeException()).when(ddltask).execute();
        tasks.set(i, ddltask);
      }
      if (task.getNumChild() != 0) {
        mockTasksRecursively(task.getChildTasks());
      }
    }
  }

  /**
   * This tests that delete_delta_x_y dirs will be not produced during minor compaction if no input delete events.
   * See HIVE-20941.
   * @throws Exception
   */
  @Test
  public void testDeleteEventsCompaction() throws Exception {
    int[][] tableData1 = {{1, 2}};
    int[][] tableData2 = {{2, 3}};
    int[][] tableData3 = {{3, 4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData1));
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData2));
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData3));

    txnHandler.compact(new CompactionRequest("default", Table.ACIDTBL.name().toLowerCase(), CompactionType.MINOR));
    runWorker(hiveConf);
    runCleaner(hiveConf);

    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] fileStatuses = fs.globStatus(new Path(getWarehouseDir() + "/" + Table.ACIDTBL.name().toLowerCase() + "/*"));
    for(FileStatus fileStatus : fileStatuses) {
      Assert.assertFalse(fileStatus.getPath().getName().startsWith(AcidUtils.DELETE_DELTA_PREFIX));
    }
  }

  @Test
  public void testNoDeltaAfterDeleteAndMinorCompaction() throws Exception {
    String tableName = "test_major_delete_minor";

    runStatementOnDriver("drop table if exists " + tableName);
    runStatementOnDriver("create table " + tableName + " (name VARCHAR(50), age TINYINT, num_clicks BIGINT) stored as orc" +
            " tblproperties ('transactional'='true')");
    runStatementOnDriver("insert into " + tableName + " values ('amy', 35, 12341234)");
    runStatementOnDriver("insert into " + tableName + " values ('bob', 66, 1234712348712)");
    runStatementOnDriver("insert into " + tableName + " values ('cal', 21, 431)");
    runStatementOnDriver("insert into " + tableName + " values ('fse', 28, 8456)");
    runStatementOnDriver("alter table " + tableName + " compact 'major'");
    runWorker(hiveConf);
    runCleaner(hiveConf);
    runStatementOnDriver("delete from " + tableName + " where name='bob'");
    runStatementOnDriver("delete from " + tableName + " WHERE name='fse'");
    runStatementOnDriver("alter table " + tableName + " compact 'minor'");
    runWorker(hiveConf);
    runCleaner(hiveConf);

    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] fileStatuses = fs.globStatus(new Path(getWarehouseDir() + "/" + tableName + "/*"));
    for(FileStatus fileStatus : fileStatuses) {
      Assert.assertFalse(fileStatus.getPath().getName().startsWith(AcidUtils.DELTA_PREFIX));
    }
  }

  @Test
  public void testNoTxnComponentsForScheduledQueries() throws Exception {
    String tableName = "scheduledquerytable";
    int[][] tableData = {{1, 2},{3, 4}};
    runStatementOnDriver("create table " + tableName + " (a int, b int) stored as orc tblproperties ('transactional'='true')");

    int noOfTimesScheduledQueryExecuted = 4;

    // Logic for executing scheduled queries multiple times.
    for (int index = 0;index < noOfTimesScheduledQueryExecuted;index++) {
      ExecutorService executor =
              Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true)
                      .setNameFormat("Scheduled queries for transactional tables").build());

      // Mock service which initialises the query for execution.
      MockScheduledQueryService qService = new
              MockScheduledQueryService("insert into " + tableName + " (a,b) " + makeValuesClause(tableData));
      ScheduledQueryExecutionContext ctx = new ScheduledQueryExecutionContext(executor, hiveConf, qService);

      // Start the scheduled query execution.
      try (ScheduledQueryExecutionService sQ = ScheduledQueryExecutionService.startScheduledQueryExecutorService(ctx)) {
        // Wait for the scheduled query to finish. Hopefully 30 seconds should be more than enough.
        SessionState.getConsole().logInfo("Waiting for query execution to finish ...");
        synchronized (qService.notifier) {
          qService.notifier.wait(30000);
        }
        SessionState.getConsole().logInfo("Done waiting for query execution!");
      }

      assertThat(qService.lastProgressInfo.isSetExecutorQueryId(), is(true));
      assertThat(qService.lastProgressInfo.getExecutorQueryId(),
              Matchers.containsString(ctx.executorHostName + "/"));
    }

    // Check whether the table has delta files corresponding to the number of scheduled executions.
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] fileStatuses = fs.globStatus(new Path(getWarehouseDir() + "/" + tableName + "/*"));
    Assert.assertEquals(fileStatuses.length, noOfTimesScheduledQueryExecuted);
    for(FileStatus fileStatus : fileStatuses) {
      Assert.assertTrue(fileStatus.getPath().getName().startsWith(AcidUtils.DELTA_PREFIX));
    }

    // Check whether the COMPLETED_TXN_COMPONENTS table has records with
    // '__global_locks' database and associate writeId corresponding to the
    // number of scheduled executions.
    Assert.assertEquals(TestTxnDbUtil.countQueryAgent(hiveConf,
            "select count(*) from completed_txn_components" +
            " where ctc_database='__global_locks'"),
            0);

    // Compact the table which has inserts from the scheduled query.
    runStatementOnDriver("alter table " + tableName + " compact 'major'");
    runWorker(hiveConf);
    runCleaner(hiveConf);

    // Run AcidHouseKeeperService to cleanup the COMPLETED_TXN_COMPONENTS.
    MetastoreTaskThread houseKeeper = new AcidHouseKeeperService();
    houseKeeper.setConf(hiveConf);
    houseKeeper.run();

    // Check whether the table is compacted.
    fileStatuses = fs.globStatus(new Path(getWarehouseDir() + "/" + tableName + "/*"));
    Assert.assertEquals(fileStatuses.length, 1);
    for(FileStatus fileStatus : fileStatuses) {
      Assert.assertTrue(fileStatus.getPath().getName().startsWith(AcidUtils.BASE_PREFIX));
    }

    // Check whether the data in the table is correct.
    int[][] actualData = {{1,2}, {1,2}, {1,2}, {1,2}, {3,4}, {3,4}, {3,4}, {3,4}};
    List<String> resData = runStatementOnDriver("select a,b from " + tableName + " order by a");
    Assert.assertEquals(resData, stringifyValues(actualData));
  }

  @Test
  public void testCompactionOutputDirectoryNamesOnPartitionsAndOldDeltasDeleted() throws Exception {
    String p1 = "p=p1";
    String p2 = "p=p2";
    String oldDelta1 = "delta_0000001_0000001_0000";
    String oldDelta2 = "delta_0000002_0000002_0000";
    String oldDelta3 = "delta_0000003_0000003_0000";
    String oldDelta4 = "delta_0000004_0000004_0000";

    String expectedDelta1 = p1 + "/delta_0000001_0000002_v0000021";
    String expectedDelta2 = p2 + "/delta_0000003_0000004_v0000023";

    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p1') (a,b) values(1,2)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p1') (a,b) values(3,4)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p2') (a,b) values(1,2)");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p='p2') (a,b) values(3,4)");

    compactPartition(Table.ACIDTBLPART.name().toLowerCase(), CompactionType.MINOR, p1);
    compactPartition(Table.ACIDTBLPART.name().toLowerCase(), CompactionType.MINOR, p2);

    FileSystem fs = FileSystem.get(hiveConf);
    String tablePath = getWarehouseDir() + "/" + Table.ACIDTBLPART.name().toLowerCase() + "/";

    Assert.assertTrue(fs.exists(new Path(tablePath + expectedDelta1)));
    Assert.assertTrue(fs.exists(new Path(tablePath + expectedDelta2)));

    Assert.assertFalse(fs.exists(new Path(tablePath + oldDelta1)));
    Assert.assertFalse(fs.exists(new Path(tablePath + oldDelta2)));
    Assert.assertFalse(fs.exists(new Path(tablePath + oldDelta3)));
    Assert.assertFalse(fs.exists(new Path(tablePath + oldDelta4)));
  }

  @Test
  public void testShowCompactionOrder() throws Exception {

    d.destroy();
    hiveConf.setVar(HiveConf.ConfVars.DYNAMIC_PARTITIONING_MODE, "nonstrict");
    d = new Driver(hiveConf);
    //generate some compaction history
    runStatementOnDriver("drop database if exists mydb1 cascade");
    runStatementOnDriver("create database mydb1");

    runStatementOnDriver("create table mydb1.tbl0 " + "(a int, b int) partitioned by (p string) clustered by (a) into " +
            BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into mydb1.tbl0" + " PARTITION(p) " +
            " values(1,2,'p1'),(3,4,'p1'),(1,2,'p2'),(3,4,'p2'),(1,2,'p3'),(3,4,'p3')");
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION(p='p1') compact 'MAJOR'");
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION(p='p2') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);
    TestTxnCommands2.runCleaner(hiveConf);
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION(p='p3') compact 'MAJOR'");
    runStatementOnDriver("insert into mydb1.tbl0" + " PARTITION(p) " +
            " values(4,5,'p1'),(6,7,'p1'),(4,5,'p2'),(6,7,'p2'),(4,5,'p3'),(6,7,'p3')");
    TestTxnCommands2.runWorker(hiveConf);
    TestTxnCommands2.runCleaner(hiveConf);
    runStatementOnDriver("insert into mydb1.tbl0" + " PARTITION(p) " +
            " values(11,12,'p1'),(13,14,'p1'),(11,12,'p2'),(13,14,'p2'),(11,12,'p3'),(13,14,'p3')");
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION (p='p1')  compact 'MINOR'");
    TestTxnCommands2.runWorker(hiveConf);

    runStatementOnDriver("create table mydb1.tbl1 " + "(a int, b int) partitioned by (ds string) clustered by (a) into " +
            BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into mydb1.tbl1" + " PARTITION(ds) " +
            " values(1,2,'today'),(3,4,'today'),(1,2,'tomorrow'),(3,4,'tomorrow'),(1,2,'yesterday'),(3,4,'yesterday')");
    runStatementOnDriver("alter table mydb1.tbl1" + " PARTITION(ds='today') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);

    runStatementOnDriver("create table T (a int, b int) stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into T values(0,2)");//makes delta_1_1 in T1
    runStatementOnDriver("insert into T values(1,4)");//makes delta_2_2 in T2

    //create failed compaction attempt so that compactor txn is aborted
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_COMPACTION, true);
    runStatementOnDriver("alter table T compact 'minor'");
    TestTxnCommands2.runWorker(hiveConf);
    // Verify  compaction order
    List<ShowCompactResponseElement> compacts =
            txnHandler.showCompact(new ShowCompactRequest()).getCompacts();
    Assert.assertEquals(6, compacts.size());
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, compacts.get(0).getState());
    Assert.assertEquals(TxnStore.REFUSED_RESPONSE, compacts.get(1).getState());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, compacts.get(2).getState());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, compacts.get(3).getState());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, compacts.get(4).getState());
    Assert.assertEquals(TxnStore.REFUSED_RESPONSE, compacts.get(5).getState());

  }

  @Test
  public void testAbortCompaction() throws Exception {

    d.destroy();
    hiveConf.setVar(HiveConf.ConfVars.DYNAMIC_PARTITIONING_MODE, "nonstrict");
    d = new Driver(hiveConf);
    //generate some compaction history
    runStatementOnDriver("drop database if exists mydb1 cascade");
    runStatementOnDriver("create database mydb1");

    runStatementOnDriver("create table mydb1.tbl0 " + "(a int, b int) partitioned by (p string) clustered by (a) into " +
            BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into mydb1.tbl0" + " PARTITION(p) " +
            " values(1,2,'p1'),(3,4,'p1'),(1,2,'p2'),(3,4,'p2'),(1,2,'p3'),(3,4,'p3')");
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION(p='p1') compact 'MAJOR'");
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION(p='p2') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);
    TestTxnCommands2.runCleaner(hiveConf);
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION(p='p3') compact 'MAJOR'");
    runStatementOnDriver("insert into mydb1.tbl0" + " PARTITION(p) " +
            " values(4,5,'p1'),(6,7,'p1'),(4,5,'p2'),(6,7,'p2'),(4,5,'p3'),(6,7,'p3')");
    TestTxnCommands2.runWorker(hiveConf);
    TestTxnCommands2.runCleaner(hiveConf);
    runStatementOnDriver("insert into mydb1.tbl0" + " PARTITION(p) " +
            " values(11,12,'p1'),(13,14,'p1'),(11,12,'p2'),(13,14,'p2'),(11,12,'p3'),(13,14,'p3')");
    runStatementOnDriver("alter table mydb1.tbl0" + " PARTITION (p='p1')  compact 'MINOR'");
    TestTxnCommands2.runWorker(hiveConf);

    runStatementOnDriver("create table mydb1.tbl1 " + "(a int, b int) partitioned by (ds string) clustered by (a) into " +
            BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into mydb1.tbl1" + " PARTITION(ds) " +
            " values(1,2,'today'),(3,4,'today'),(1,2,'tomorrow'),(3,4,'tomorrow'),(1,2,'yesterday'),(3,4,'yesterday')");
    runStatementOnDriver("alter table mydb1.tbl1" + " PARTITION(ds='today') compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);

    runStatementOnDriver("drop table if exists myT1");
    runStatementOnDriver("create table myT1 (a int, b int) stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into myT1 values(0,2)");//makes delta_1_1 in T1
    runStatementOnDriver("insert into myT1 values(1,4)");//makes delta_2_2 in T2

    //create failed compaction attempt so that compactor txn is aborted
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_COMPACTION, true);
    runStatementOnDriver("alter table myT1 compact 'minor'");
    TestTxnCommands2.runWorker(hiveConf);
    // Verify  compaction order
    List<ShowCompactResponseElement> compacts =
            txnHandler.showCompact(new ShowCompactRequest()).getCompacts();
    Assert.assertEquals(6, compacts.size());
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, compacts.get(0).getState());
    Assert.assertEquals(TxnStore.REFUSED_RESPONSE, compacts.get(1).getState());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, compacts.get(2).getState());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, compacts.get(3).getState());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, compacts.get(4).getState());
    Assert.assertEquals(TxnStore.REFUSED_RESPONSE, compacts.get(5).getState());
    Map<Long, AbortCompactionResponseElement> expectedResponseMap = new HashMap<Long, AbortCompactionResponseElement>() {{
      put(Long.parseLong("6"),getAbortCompactionResponseElement(Long.parseLong("6"), "Success",
              "Successfully aborted compaction"));
      put(Long.parseLong("1") ,getAbortCompactionResponseElement(Long.parseLong("1"), "Error",
              "Error while aborting compaction as compaction is in state-refused"));
      put(Long.parseLong("2"),getAbortCompactionResponseElement(Long.parseLong("2"), "Error",
              "Error while aborting compaction as compaction is in state-succeeded"));
      put(Long.parseLong("3"),getAbortCompactionResponseElement(Long.parseLong("3"), "Error",
              "Error while aborting compaction as compaction is in state-ready for cleaning"));
      put(Long.parseLong("4"),getAbortCompactionResponseElement(Long.parseLong("4"), "Error",
              "Error while aborting compaction as compaction is in state-ready for cleaning"));
      put(Long.parseLong("5"),getAbortCompactionResponseElement(Long.parseLong("5"), "Error",
              "Error while aborting compaction as compaction is in state-refused"));
      put(Long.parseLong("12"),getAbortCompactionResponseElement(Long.parseLong("12"), "Error",
              "No Such Compaction Id Available"));
    }};

    List<Long> compactionsToAbort = Arrays.asList(Long.parseLong("12"), compacts.get(0).getId(),
            compacts.get(1).getId(), compacts.get(2).getId(), compacts.get(3).getId(), compacts.get(4).getId(),
            compacts.get(5).getId());

    AbortCompactionRequest rqst = new AbortCompactionRequest();
    rqst.setCompactionIds(compactionsToAbort);
    AbortCompactResponse resp = txnHandler.abortCompactions(rqst);
    Assert.assertEquals(7, resp.getAbortedcompactsSize());
    Map<Long, AbortCompactionResponseElement> testResp = resp.getAbortedcompacts();

    Assert.assertEquals(expectedResponseMap, testResp);

    //assert aborted compaction state
    compacts = txnHandler.showCompact(new ShowCompactRequest()).getCompacts();
    Assert.assertEquals(6, compacts.size());
    Assert.assertEquals(TxnStore.ABORTED_RESPONSE, compacts.get(0).getState());
    Assert.assertEquals(TxnStore.REFUSED_RESPONSE, compacts.get(1).getState());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, compacts.get(2).getState());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, compacts.get(3).getState());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, compacts.get(4).getState());
    Assert.assertEquals(TxnStore.REFUSED_RESPONSE, compacts.get(5).getState());

  }

  private AbortCompactionResponseElement getAbortCompactionResponseElement(long compactionId, String status, String message){
    AbortCompactionResponseElement resEle = new AbortCompactionResponseElement(compactionId);
    resEle.setMessage(message);
    resEle.setStatus(status);
    return resEle;
  }

  private void compactPartition(String table, CompactionType type, String partition)
      throws Exception {
    CompactionRequest compactionRequest = new CompactionRequest("default", table, type);
    compactionRequest.setPartitionname(partition);
    txnHandler.compact(compactionRequest);
    runWorker(hiveConf);
    runCleaner(hiveConf);
  }

  /**
   * takes raw data and turns it into a string as if from Driver.getResults()
   * sorts rows in dictionary order
   */
  private static List<String> stringifyValuesNoSort(int[][] rowsIn) {
    assert rowsIn.length > 0;
    int[][] rows = rowsIn.clone();
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

  private void runStatementOnDriverWithAbort(String stmt) {
    LOG.info("+runStatementOnDriver(" + stmt + ")");
    try {
      d.run(stmt);
    } catch (CommandProcessorException e) {
    }
  }
  @Test
  public void testShowCompactWithFilterOption() throws Exception {
    CompactionRequest rqst = new CompactionRequest("foo", "bar", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    rqst.setPoolName("mypool");
    txnHandler.compact(rqst);
    CompactionRequest rqst1 = new CompactionRequest("foo", "bar1", CompactionType.MAJOR);
    rqst1.setPartitionname("ds=today");
    txnHandler.compact(rqst1);
    CompactionRequest rqst2 = new CompactionRequest("bar", "bar1", CompactionType.MAJOR);
    rqst2.setPartitionname("ds=today");
    txnHandler.compact(rqst2);
    CompactionRequest rqst3 = new CompactionRequest("xxx", "yyy", CompactionType.MINOR);
    rqst3.setPartitionname("ds=today");
    txnHandler.compact(rqst3);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(4, compacts.size());
    ShowCompactRequest scr = new ShowCompactRequest();
    scr.setDbName("bar");
    Assert.assertEquals(1, txnHandler.showCompact(scr).getCompacts().size());
    scr = new ShowCompactRequest();
    scr.setTbName("bar");
    scr.setPoolName("mypool");
    List<ShowCompactResponseElement>  compRsp =txnHandler.showCompact(scr).getCompacts();
    Assert.assertEquals(1, compRsp.size());
    Assert.assertEquals("mypool", compRsp.get(0).getPoolName());
    scr = new ShowCompactRequest();
    scr.setTbName("bar1");
    Assert.assertEquals(2, txnHandler.showCompact(scr).getCompacts().size());
    scr = new ShowCompactRequest();
    scr.setDbName("bar22");
    scr.setTbName("bar1");
    Assert.assertEquals(0, txnHandler.showCompact(scr).getCompacts().size());
    scr = new ShowCompactRequest();
    scr.setDbName("bar");
    scr.setTbName("bar1");
    Assert.assertEquals(1, txnHandler.showCompact(scr).getCompacts().size());
    scr = new ShowCompactRequest();
    scr.setState("i");
    Assert.assertEquals(4, txnHandler.showCompact(scr).getCompacts().size());
    scr = new ShowCompactRequest();
    scr.setState("f");
    Assert.assertEquals(0, txnHandler.showCompact(scr).getCompacts().size());
    scr = new ShowCompactRequest();
    scr.setType(CompactionType.MAJOR);
    Assert.assertEquals(3, txnHandler.showCompact(scr).getCompacts().size());
    scr = new ShowCompactRequest();
    scr.setType(CompactionType.MINOR);
    Assert.assertEquals(1, txnHandler.showCompact(scr).getCompacts().size());

    scr = new ShowCompactRequest();
    scr.setPartName("ds=today");
    Assert.assertEquals(4, txnHandler.showCompact(scr).getCompacts().size());

  }
  private void assertUniqueID(Table table) throws Exception {
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
    Assert.assertEquals("Duplicate ROW__ID: " + r.toString(), 0, r.size());
  }

  private static String quoteString(String input) {
    return "'" + input + "'";
  }
}
