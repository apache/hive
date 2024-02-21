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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestTxnNoBuckets extends TxnCommandsBaseForTests {
  static final private Logger LOG = LoggerFactory.getLogger(TestTxnNoBuckets.class);
  private static final String NO_BUCKETS_TBL_NAME = "nobuckets";
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
    File.separator + TestTxnNoBuckets.class.getCanonicalName()
    + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
  @Rule
  public TestName testName = new TestName();
  @Override
  protected String getTestDataDir() {
    return TEST_DATA_DIR;
  }

  @Before
  public void setUp() throws Exception {
    setUpInternal();
    //see TestTxnNoBucketsVectorized for vectorized version
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
  }

  private boolean shouldVectorize() {
    return hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED);
  }
  /**
   * Tests that Acid can work with un-bucketed tables.
   */
  @Test
  public void testNoBuckets() throws Exception {
    int[][] sourceVals1 = {{0,0,0},{3,3,3}};
    int[][] sourceVals2 = {{1,1,1},{2,2,2}};
    runStatementOnDriver("drop table if exists tmp");
    runStatementOnDriver("create table tmp (c1 integer, c2 integer, c3 integer) stored as orc tblproperties('transactional'='false')");
    runStatementOnDriver("insert into tmp " + makeValuesClause(sourceVals1));
    runStatementOnDriver("insert into tmp " + makeValuesClause(sourceVals2));
    runStatementOnDriver(String.format("drop table if exists %s", NO_BUCKETS_TBL_NAME));
    runStatementOnDriver("create table " + NO_BUCKETS_TBL_NAME + " (c1 integer, c2 integer, c3 integer) stored " +
      "as orc tblproperties('transactional'='true', 'transactional_properties'='default')");
    String stmt = String.format("insert into %s select * from tmp", NO_BUCKETS_TBL_NAME);
    runStatementOnDriver(stmt);
    List<String> rs = runStatementOnDriver(
        String.format("select ROW__ID, c1, c2, c3, INPUT__FILE__NAME from %s order by ROW__ID", NO_BUCKETS_TBL_NAME));
    Assert.assertEquals("", 4, rs.size());
    LOG.warn("after insert");
    for(String s : rs) {
      LOG.warn(s);
    }
    /**the insert creates 2 output files (presumably because there are 2 input files)
     * The number in the file name is writerId.  This is the number encoded in ROW__ID.bucketId -
     * see {@link org.apache.hadoop.hive.ql.io.BucketCodec}*/
    Assert.assertTrue(rs.get(0), rs.get(0).startsWith("{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t0\t0\t0\t"));
    Assert.assertTrue(rs.get(0), rs.get(0).endsWith(NO_BUCKETS_TBL_NAME + "/delta_0000001_0000001_0000/bucket_00000_0"));
    Assert.assertTrue(rs.get(1), rs.get(1).startsWith("{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t3\t3\t3\t"));
    Assert.assertTrue(rs.get(1), rs.get(1).endsWith(NO_BUCKETS_TBL_NAME + "/delta_0000001_0000001_0000/bucket_00000_0"));
    Assert.assertTrue(rs.get(2), rs.get(2).startsWith("{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t1\t1\t1\t"));
    Assert.assertTrue(rs.get(2), rs.get(2).endsWith(NO_BUCKETS_TBL_NAME + "/delta_0000001_0000001_0000/bucket_00001_0"));
    Assert.assertTrue(rs.get(3), rs.get(3).startsWith("{\"writeid\":1,\"bucketid\":536936448,\"rowid\":1}\t2\t2\t2\t"));
    Assert.assertTrue(rs.get(3), rs.get(3).endsWith(NO_BUCKETS_TBL_NAME + "/delta_0000001_0000001_0000/bucket_00001_0"));

    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_EXPLAIN_USER, false);
    rs = runStatementOnDriver(String.format("explain  update %s set c3 = 17 where c3 in(0,1)", NO_BUCKETS_TBL_NAME));
    LOG.warn("Query Plan: ");
    for (String s : rs) {
      LOG.warn(s);
    }

    runStatementOnDriver(String.format("update %s set c3 = 17 where c3 in(0,1)", NO_BUCKETS_TBL_NAME));
    rs = runStatementOnDriver(
        String.format("select ROW__ID, c1, c2, c3, INPUT__FILE__NAME from %s order by INPUT__FILE__NAME, ROW__ID",
            NO_BUCKETS_TBL_NAME));
    LOG.warn("after update");
    for(String s : rs) {
      LOG.warn(s);
    }
    Assert.assertTrue(rs.get(0), rs.get(0).startsWith("{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t3\t3\t3\t"));
    Assert.assertTrue(rs.get(0), rs.get(0).endsWith(NO_BUCKETS_TBL_NAME + "/delta_0000001_0000001_0000/bucket_00000_0"));
    Assert.assertTrue(rs.get(1), rs.get(1).startsWith("{\"writeid\":1,\"bucketid\":536936448,\"rowid\":1}\t2\t2\t2\t"));
    Assert.assertTrue(rs.get(1), rs.get(1).endsWith(NO_BUCKETS_TBL_NAME + "/delta_0000001_0000001_0000/bucket_00001_0"));
    //so update has 1 writer, but which creates buckets where the new rows land
    Assert.assertTrue(rs.get(2), rs.get(2).startsWith("{\"writeid\":2,\"bucketid\":536870913,\"rowid\":0}\t1\t1\t17\t"));
    Assert.assertTrue(rs.get(2), rs.get(2).endsWith(NO_BUCKETS_TBL_NAME + "/delta_0000002_0000002_0001/bucket_00000_0"));
    // update for "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t1\t1\t1\t"
    Assert.assertTrue(rs.get(3), rs.get(3).startsWith("{\"writeid\":2,\"bucketid\":536936449,\"rowid\":0}\t0\t0\t17\t"));
    Assert.assertTrue(rs.get(3), rs.get(3).endsWith(NO_BUCKETS_TBL_NAME + "/delta_0000002_0000002_0001/bucket_00001_0"));

    Set<String> expectedFiles = new HashSet<>();
    //both delete events land in corresponding buckets to the original row-ids
    expectedFiles.add(NO_BUCKETS_TBL_NAME + "/delete_delta_0000002_0000002_0000/bucket_00000_0");
    expectedFiles.add(NO_BUCKETS_TBL_NAME + "/delete_delta_0000002_0000002_0000/bucket_00001_0");
    expectedFiles.add(NO_BUCKETS_TBL_NAME + "/delta_0000001_0000001_0000/bucket_00000_0");
    expectedFiles.add(NO_BUCKETS_TBL_NAME + "/delta_0000001_0000001_0000/bucket_00001_0");
    expectedFiles.add(NO_BUCKETS_TBL_NAME + "/delta_0000002_0000002_0001/bucket_00000_0");
    expectedFiles.add(NO_BUCKETS_TBL_NAME + "/delta_0000002_0000002_0001/bucket_00001_0");
    //check that we get the right files on disk
    assertExpectedFileSet(expectedFiles, getWarehouseDir() + "/" + NO_BUCKETS_TBL_NAME, NO_BUCKETS_TBL_NAME);
    //todo: it would be nice to check the contents of the files... could use orc.FileDump - it has
    // methods to print to a supplied stream but those are package private

    runStatementOnDriver(String.format("alter table %s compact 'major'", NO_BUCKETS_TBL_NAME));
    TestTxnCommands2.runWorker(hiveConf);
    rs = runStatementOnDriver(
        String.format("select ROW__ID, c1, c2, c3, INPUT__FILE__NAME from %s order by INPUT__FILE__NAME, ROW__ID",
            NO_BUCKETS_TBL_NAME));
    LOG.warn("after major compact");
    for(String s : rs) {
      LOG.warn(s);
    }
    /*
├── base_0000002_v0000025
│   ├── bucket_00000
│   └── bucket_00001
├── delete_delta_0000002_0000002_0000
│   └── bucket_00000
|   └── bucket_00001
├── delta_0000001_0000001_0000
│   ├── bucket_00000
│   └── bucket_00001
└── delta_0000002_0000002_0000
    └── bucket_00000
    */

    String expected[][] = {
        {"{\"writeid\":2,\"bucketid\":536936449,\"rowid\":0}\t0\t0\t17", NO_BUCKETS_TBL_NAME + "/base_0000002_v0000026/bucket_00001"},
        {"{\"writeid\":2,\"bucketid\":536870913,\"rowid\":0}\t1\t1\t17", NO_BUCKETS_TBL_NAME + "/base_0000002_v0000026/bucket_00000"},
        {"{\"writeid\":1,\"bucketid\":536936448,\"rowid\":1}\t2\t2\t2", NO_BUCKETS_TBL_NAME + "/base_0000002_v0000026/bucket_00001"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t3\t3\t3", NO_BUCKETS_TBL_NAME + "/base_0000002_v0000026/bucket_00000"}
    };
    checkResult(expected,
        "select ROW__ID, c1, c2, c3" + (shouldVectorize() ? "" : ", INPUT__FILE__NAME")
            + " from " + NO_BUCKETS_TBL_NAME + " order by c1, c2, c3",
        shouldVectorize(),
        "After Major Compaction", LOG);

    expectedFiles.clear();
    expectedFiles.add(NO_BUCKETS_TBL_NAME + "/delete_delta_0000002_0000002_0000/bucket_00000_0");
    expectedFiles.add(NO_BUCKETS_TBL_NAME + "/delete_delta_0000002_0000002_0000/bucket_00001_0");
    expectedFiles.add(NO_BUCKETS_TBL_NAME + "/delta_0000001_0000001_0000/bucket_00000_0");
    expectedFiles.add(NO_BUCKETS_TBL_NAME + "/delta_0000001_0000001_0000/bucket_00001_0");
    expectedFiles.add(NO_BUCKETS_TBL_NAME + "/delta_0000002_0000002_0001/bucket_00000_0");
    expectedFiles.add(NO_BUCKETS_TBL_NAME + "/delta_0000002_0000002_0001/bucket_00001_0");
    expectedFiles.add(NO_BUCKETS_TBL_NAME + "/base_0000002_v0000026/bucket_00000");
    expectedFiles.add(NO_BUCKETS_TBL_NAME + "/base_0000002_v0000026/bucket_00001");
    assertExpectedFileSet(expectedFiles, getWarehouseDir() + "/" + NO_BUCKETS_TBL_NAME, NO_BUCKETS_TBL_NAME);

    TestTxnCommands2.runCleaner(hiveConf);
    rs = runStatementOnDriver(String.format("select c1, c2, c3 from %s order by c1, c2, c3", NO_BUCKETS_TBL_NAME));
    int[][] result = {{0,0,17},{1,1,17},{2,2,2},{3,3,3}};
    Assert.assertEquals("Unexpected result after clean", stringifyValues(result), rs);

    expectedFiles.clear();
    expectedFiles.add(NO_BUCKETS_TBL_NAME + "/base_0000002_v0000026/bucket_00000");
    expectedFiles.add(NO_BUCKETS_TBL_NAME + "/base_0000002_v0000026/bucket_00001");
    assertExpectedFileSet(expectedFiles, getWarehouseDir() + "/" + NO_BUCKETS_TBL_NAME, NO_BUCKETS_TBL_NAME);
  }

  @Test
  public void testNoBucketsDP() throws Exception {
    int[][] sourceVals1 = {{0,0,0},{3,3,3}};
    int[][] sourceVals2 = {{1,1,1},{2,2,2}};
    int[][] sourceVals3 = {{3,3,3},{4,4,4}};
    int[][] sourceVals4 = {{5,5,5},{6,6,6}};
    runStatementOnDriver("drop table if exists tmp");
    runStatementOnDriver("create table tmp (c1 integer, c2 integer) partitioned by (c3 integer) stored as orc " +
      "tblproperties('transactional'='false')");
    runStatementOnDriver("insert into tmp " + makeValuesClause(sourceVals1));
    runStatementOnDriver("insert into tmp " + makeValuesClause(sourceVals2));
    runStatementOnDriver("insert into tmp " + makeValuesClause(sourceVals3));
    runStatementOnDriver("insert into tmp " + makeValuesClause(sourceVals4));
    runStatementOnDriver("drop table if exists " + NO_BUCKETS_TBL_NAME);
    runStatementOnDriver(String.format("create table %s (c1 integer, c2 integer) partitioned by (c3 integer) stored " +
      "as orc tblproperties('transactional'='true', 'transactional_properties'='default')", NO_BUCKETS_TBL_NAME));
    String stmt = String.format("insert into %s partition(c3) select * from tmp", NO_BUCKETS_TBL_NAME);
    runStatementOnDriver(stmt);
    List<String> rs = runStatementOnDriver(
      String.format("select ROW__ID, c1, c2, c3, INPUT__FILE__NAME from %s order by ROW__ID", NO_BUCKETS_TBL_NAME));
    Assert.assertEquals("", 8, rs.size());
    LOG.warn("after insert");
    for(String s : rs) {
      LOG.warn(s);
    }

    rs = runStatementOnDriver(
      String.format("select * from %s where c2 in (0,3)", NO_BUCKETS_TBL_NAME));
    Assert.assertEquals(3, rs.size());
    runStatementOnDriver(String.format("update %s set c2 = 17 where c2 in(0,3)", NO_BUCKETS_TBL_NAME));
    rs = runStatementOnDriver(
        String.format("select ROW__ID, c1, c2, c3, INPUT__FILE__NAME from %s order by INPUT__FILE__NAME, ROW__ID",
            NO_BUCKETS_TBL_NAME));
    LOG.warn("after update");
    for(String s : rs) {
      LOG.warn(s);
    }
    rs = runStatementOnDriver(String.format("select * from %s where c2=17", NO_BUCKETS_TBL_NAME));
    Assert.assertEquals(3, rs.size());
  }

  /**
   * See CTAS tests in TestAcidOnTez
   */
  @Test
  public void testCTAS() throws Exception {
    runStatementOnDriver("drop table if exists myctas");
    int[][] values = {{1,2},{3,4}};
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL +  makeValuesClause(values));
    runStatementOnDriver("create table myctas stored as ORC TBLPROPERTIES ('transactional" +
      "'='true', 'transactional_properties'='default') as select a, b from " + Table.NONACIDORCTBL);
    List<String> rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from myctas order by ROW__ID");
    String expected[][] = {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t3\t4", "warehouse/myctas/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t1\t2", "warehouse/myctas/delta_0000001_0000001_0000/bucket_00001_0"},
    };
    checkExpected(rs, expected, "Unexpected row count after ctas from non acid table");

    runStatementOnDriver("insert into " + Table.ACIDTBL + makeValuesClause(values));
    //todo: try this with acid default - it seem making table acid in listener is too late
    runStatementOnDriver("create table myctas2 stored as ORC TBLPROPERTIES ('transactional" +
      "'='true', 'transactional_properties'='default') as select a, b from " + Table.ACIDTBL);
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from myctas2 order by ROW__ID");
    String expected2[][] = {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2", "warehouse/myctas2/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t3\t4", "warehouse/myctas2/delta_0000001_0000001_0000/bucket_00001_0"}
    };
    checkExpected(rs, expected2, "Unexpected row count after ctas from acid table");

    runStatementOnDriver("create table myctas3 stored as ORC TBLPROPERTIES ('transactional" +
      "'='true', 'transactional_properties'='default') as select a, b from " + Table.NONACIDORCTBL +
      " union all select a, b from " + Table.ACIDTBL);
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from myctas3 order by ROW__ID");
    String expected3[][] = {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2", "warehouse/myctas3/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t3\t4", "warehouse/myctas3/delta_0000001_0000001_0000/bucket_00001_0"},
        {"{\"writeid\":1,\"bucketid\":537001984,\"rowid\":0}\t3\t4", "warehouse/myctas3/delta_0000001_0000001_0000/bucket_00002_0"},
        {"{\"writeid\":1,\"bucketid\":537067520,\"rowid\":0}\t1\t2", "warehouse/myctas3/delta_0000001_0000001_0000/bucket_00003_0"},
    };
    checkExpected(rs, expected3, "Unexpected row count after ctas from union all query");

    runStatementOnDriver("create table myctas4 stored as ORC TBLPROPERTIES ('transactional" +
      "'='true', 'transactional_properties'='default') as select a, b from " + Table.NONACIDORCTBL +
      " union distinct select a, b from " + Table.ACIDTBL);
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from myctas4 order by ROW__ID");
    String expected4[][] = {
      {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2", "/delta_0000001_0000001_0000/bucket_00000_0"},
      {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t3\t4", "/delta_0000001_0000001_0000/bucket_00000_0"},
    };
    checkExpected(rs, expected4, "Unexpected row count after ctas from union distinct query");
  }
  @Test
  public void testCtasEmpty() throws Exception {
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID, true);
    runStatementOnDriver("drop table if exists myctas");
    runStatementOnDriver("create table myctas stored as ORC as" +
        " select a, b from " + Table.NONACIDORCTBL);
    List<String> rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME" +
        " from myctas order by ROW__ID");
  }

  /**
   * Insert into unbucketed acid table from union all query
   * Union All is flattened so nested subdirs are created and acid move drops them since
   * delta dirs have unique names
   */
  @Test
  public void testInsertToAcidWithUnionRemove() throws Exception {
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_UNION_REMOVE, true);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
    d.close();
    d = new Driver(hiveConf);
    int[][] values = {{1,2},{3,4},{5,6},{7,8},{9,10}};
    runStatementOnDriver("insert into " + TxnCommandsBaseForTests.Table.ACIDTBL + makeValuesClause(values));//HIVE-17138: this creates 1 delta_0000013_0000013_0000/bucket_00001
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T (a int, b int) stored as ORC  TBLPROPERTIES ('transactional'='true')");
    /*
    So Union All removal kicks in and we get 3 subdirs in staging.
ekoifman:apache-hive-3.0.0-SNAPSHOT-bin ekoifman$ tree /Users/ekoifman/dev/hiverwgit/ql/target/tmp/org.apache.hadoop.hive.ql.TestTxnNoBuckets-1505516390532/warehouse/t/.hive-staging_hive_2017-09-15_16-05-06_895_1123322677843388168-1/
└── -ext-10000
    ├── HIVE_UNION_SUBDIR_19
    │   └── 000000_0
    │       ├── _orc_acid_version
    │       └── delta_0000016_0000016_0001
    ├── HIVE_UNION_SUBDIR_20
    │   └── 000000_0
    │       ├── _orc_acid_version
    │       └── delta_0000016_0000016_0002
    └── HIVE_UNION_SUBDIR_21
        └── 000000_0
            ├── _orc_acid_version
            └── delta_0000016_0000016_0003*/
    runStatementOnDriver("insert into T(a,b) select a, b from " + TxnCommandsBaseForTests.Table.ACIDTBL + " where a between 1 and 3 group by a, b union all select a, b from " + TxnCommandsBaseForTests.Table.ACIDTBL + " where a between 5 and 7 union all select a, b from " + TxnCommandsBaseForTests.Table.ACIDTBL + " where a >= 9");

    List<String> rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from T order by ROW__ID");

    String expected[][] = {
        {"{\"writeid\":1,\"bucketid\":536870913,\"rowid\":0}\t1\t2", "/delta_0000001_0000001_0001/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536870913,\"rowid\":1}\t3\t4", "/delta_0000001_0000001_0001/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536870914,\"rowid\":0}\t5\t6", "/delta_0000001_0000001_0002/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536870915,\"rowid\":0}\t9\t10", "/delta_0000001_0000001_0003/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536936450,\"rowid\":0}\t7\t8", "/delta_0000001_0000001_0002/bucket_00001_0"},
    };
    checkExpected(rs, expected, "Unexpected row count after ctas");
  }
  @Test
  public void testInsertOverwriteToAcidWithUnionRemove() throws Exception {
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_UNION_REMOVE, true);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
    d.close();
    d = new Driver(hiveConf);
    int[][] values = {{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}};
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T (a int, b int) stored as ORC  TBLPROPERTIES ('transactional'='true')");

    CommandProcessorException e = runStatementOnDriverNegative(
        "insert overwrite table T select a, b from " + TxnCommandsBaseForTests.Table.ACIDTBL +
            " where a between 1 and 3 group by a, b union all select a, b from " +
            TxnCommandsBaseForTests.Table.ACIDTBL +
            " where a between 5 and 7 union all select a, b from " +
            TxnCommandsBaseForTests.Table.ACIDTBL + " where a >= 9");
    Assert.assertTrue("", e.getMessage().contains("not supported due to OVERWRITE and UNION ALL"));
  }
  /**
   * The idea here is to create a non acid table that was written by multiple writers, i.e.
   * unbucketed table that has 000000_0 & 000001_0, for example.
   * Also, checks that we can handle a case when data files can be at multiple levels (subdirs)
   * of the table.
   */
  @Test
  public void testToAcidConversionMultiBucket() throws Exception {
    //need to disable these so that automatic merge doesn't merge the files
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_MERGE_MAPFILES, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_MERGE_MAPRED_FILES, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_MERGE_TEZ_FILES, false);
    d.close();
    d = new Driver(hiveConf);

    int[][] values = {{1,2},{2,4},{5,6},{6,8},{9,10}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + makeValuesClause(values));
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T (a int, b int) stored as ORC  TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("insert into T(a,b) select a, b from " + Table.ACIDTBL +
      " where a between 1 and 3 group by a, b union all select a, b from " + Table.ACIDTBL +
      " where a between 5 and 7 union all select a, b from " + Table.ACIDTBL + " where a >= 9");
    runStatementOnDriver("insert into T values(12,12)");

    List<String> rs = runStatementOnDriver("select a, b, INPUT__FILE__NAME from T order by a, b, INPUT__FILE__NAME");
    //previous insert+union creates 3 data files (0-3)
    //insert (12,12) creates 000000_0_copy_1
    String expected[][] = {
        {"1\t2",  "warehouse/t/000002_0"},
        {"2\t4",  "warehouse/t/000002_0"},
        {"5\t6",  "warehouse/t/000000_0"},
        {"6\t8",  "warehouse/t/000001_0"},
        {"9\t10", "warehouse/t/000000_0"},
        {"12\t12", "warehouse/t/000000_0_copy_1"}
    };
    checkExpected(rs, expected,"before converting to acid");

    //now do Insert from Union here to create data files in sub dirs
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_UNION_REMOVE, true);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
    d.close();
    d = new Driver(hiveConf);
    runStatementOnDriver("insert into T(a,b) select a * 10, b * 10 from " + Table.ACIDTBL +
        " where a between 1 and 3 group by a, b union all select a * 10, b * 10 from " + Table.ACIDTBL +
        " where a between 5 and 7");
    //now we have a table with data files at multiple different levels.
    String expected1[][] = {
        {"1\t2",  "warehouse/t/000002_0"},
        {"2\t4",  "warehouse/t/000002_0"},
        {"5\t6",  "warehouse/t/000000_0"},
        {"6\t8",  "warehouse/t/000001_0"},
        {"9\t10", "warehouse/t/000000_0"},
        {"10\t20", "warehouse/t/HIVE_UNION_SUBDIR_15/000000_0"},
        {"12\t12", "warehouse/t/000000_0_copy_1"},
        {"20\t40", "warehouse/t/HIVE_UNION_SUBDIR_15/000000_0"},
        {"50\t60", "warehouse/t/HIVE_UNION_SUBDIR_16/000000_0"},
        {"60\t80", "warehouse/t/HIVE_UNION_SUBDIR_16/000001_0"}
    };
    rs = runStatementOnDriver("select a, b, INPUT__FILE__NAME from T order by a, b, INPUT__FILE__NAME");
    checkExpected(rs, expected1,"before converting to acid (with multi level data layout)");

    //make it an Acid table and make sure we assign ROW__IDs correctly
    runStatementOnDriver("alter table T SET TBLPROPERTIES ('transactional'='true')");
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from T order by a, b, INPUT__FILE__NAME");
    /**
    now that T is Acid, data for each writerId is treated like a logical bucket (though T is not
    bucketed), so rowid are assigned per logical bucket (e.g. 000000_0 + 000000_0_copy_1 + subdirs).
     {@link AcidUtils.Directory#getOriginalFiles()} ensures consistent ordering of files within
     logical bucket (tranche)
     */
    String expected2[][] = {
        {"{\"writeid\":0,\"bucketid\":537001984,\"rowid\":0}\t1\t2",  "warehouse/t/000002_0"},
        {"{\"writeid\":0,\"bucketid\":537001984,\"rowid\":1}\t2\t4",  "warehouse/t/000002_0"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":0}\t5\t6",  "warehouse/t/000000_0"},
        {"{\"writeid\":0,\"bucketid\":536936448,\"rowid\":0}\t6\t8",  "warehouse/t/000001_0"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":1}\t9\t10", "warehouse/t/000000_0"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":3}\t10\t20", "warehouse/t/HIVE_UNION_SUBDIR_15/000000_0"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":2}\t12\t12", "warehouse/t/000000_0_copy_1"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":4}\t20\t40", "warehouse/t/HIVE_UNION_SUBDIR_15/000000_0"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":5}\t50\t60", "warehouse/t/HIVE_UNION_SUBDIR_16/000000_0"},
        {"{\"writeid\":0,\"bucketid\":536936448,\"rowid\":1}\t60\t80", "warehouse/t/HIVE_UNION_SUBDIR_16/000001_0"},
    };
    checkExpected(rs, expected2,"after converting to acid (no compaction)");
    Assert.assertEquals(0, BucketCodec.determineVersion(536870912).decodeWriterId(536870912));
    Assert.assertEquals(2, BucketCodec.determineVersion(537001984).decodeWriterId(537001984));
    Assert.assertEquals(1, BucketCodec.determineVersion(536936448).decodeWriterId(536936448));

    assertVectorized(shouldVectorize(), "update T set b = 88 where b = 80");
    runStatementOnDriver("update T set b = 88 where b = 80");
    assertVectorized(shouldVectorize(), "delete from T where b = 8");
    runStatementOnDriver("delete from T where b = 8");
    String expected3[][] = {
        {"{\"writeid\":0,\"bucketid\":537001984,\"rowid\":0}\t1\t2",  "warehouse/t/000002_0"},
        {"{\"writeid\":0,\"bucketid\":537001984,\"rowid\":1}\t2\t4",  "warehouse/t/000002_0"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":0}\t5\t6",  "warehouse/t/000000_0"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":1}\t9\t10", "warehouse/t/000000_0"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":3}\t10\t20", "warehouse/t/HIVE_UNION_SUBDIR_15/000000_0"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":2}\t12\t12", "warehouse/t/000000_0_copy_1"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":4}\t20\t40", "warehouse/t/HIVE_UNION_SUBDIR_15/000000_0"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":5}\t50\t60", "warehouse/t/HIVE_UNION_SUBDIR_16/000000_0"},
        // update for "{\"writeid\":0,\"bucketid\":536936448,\"rowid\":1}\t60\t80"
        {"{\"writeid\":10000001,\"bucketid\":536870913,\"rowid\":0}\t60\t88", "warehouse/t/delta_10000001_10000001_0001/bucket_00000_0"},
    };
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from T order by a, b, INPUT__FILE__NAME");
    checkExpected(rs, expected3,"after converting to acid (no compaction with updates)");

    //major compaction + check data + files
    runStatementOnDriver("alter table T compact 'major'");
    TestTxnCommands2.runWorker(hiveConf);
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from T order by a, b, INPUT__FILE__NAME");

    /*Compaction preserves location of rows wrt buckets/tranches (for now)*/
    String expected4[][] = {
        {"{\"writeid\":0,\"bucketid\":537001984,\"rowid\":0}\t1\t2",
            "warehouse/t/base_10000002_v0000030/bucket_00002"},
        {"{\"writeid\":0,\"bucketid\":537001984,\"rowid\":1}\t2\t4",
            "warehouse/t/base_10000002_v0000030/bucket_00002"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":0}\t5\t6",
            "warehouse/t/base_10000002_v0000030/bucket_00000"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":1}\t9\t10",
            "warehouse/t/base_10000002_v0000030/bucket_00000"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":3}\t10\t20",
            "warehouse/t/base_10000002_v0000030/bucket_00000"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":2}\t12\t12",
            "warehouse/t/base_10000002_v0000030/bucket_00000"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":4}\t20\t40",
            "warehouse/t/base_10000002_v0000030/bucket_00000"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":5}\t50\t60",
            "warehouse/t/base_10000002_v0000030/bucket_00000"},
        {"{\"writeid\":10000001,\"bucketid\":536870913,\"rowid\":0}\t60\t88",
            "warehouse/t/base_10000002_v0000030/bucket_00000"},
    };
    checkExpected(rs, expected4,"after major compact");
  }
  @Test
  public void testInsertFromUnion() throws Exception {
    int[][] values = {{1,2},{2,4},{5,6},{6,8},{9,10}};
    runStatementOnDriver("insert into " + Table.NONACIDNONBUCKET + makeValuesClause(values));
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T (a int, b int) stored as ORC  TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into T(a,b) select a, b from " + Table.NONACIDNONBUCKET + " where a between 1 and 3 group by a, b union all select a, b from " + Table.NONACIDNONBUCKET + " where a between 5 and 7 union all select a, b from " + Table.NONACIDNONBUCKET + " where a >= 9");
    List<String> rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from T order by a, b, INPUT__FILE__NAME");
    LOG.warn("before converting to acid");
    for(String s : rs) {
      LOG.warn(s);
    }
    /*
    The number of writers seems to be based on number of MR jobs for the src query.  todo check number of FileSinks
    warehouse/t/.hive-staging_hive_2017-09-13_08-59-28_141_6304543600372946004-1/-ext-10000/000000_0/delta_0000001_0000001_0000/bucket_00000 [length: 648]
    {"operation":0,"originalTransaction":1,"bucket":536870912,"rowId":0,"currentTransaction":1,"row":{"_col0":1,"_col1":2}}
    {"operation":0,"originalTransaction":1,"bucket":536870912,"rowId":1,"currentTransaction":1,"row":{"_col0":2,"_col1":4}}
    ________________________________________________________________________________________________________________________
    warehouse/t/.hive-staging_hive_2017-09-13_08-59-28_141_6304543600372946004-1/-ext-10000/000001_0/delta_0000001_0000001_0000/bucket_00001 [length: 658]
    {"operation":0,"originalTransaction":1,"bucket":536936448,"rowId":0,"currentTransaction":1,"row":{"_col0":5,"_col1":6}}
    {"operation":0,"originalTransaction":1,"bucket":536936448,"rowId":1,"currentTransaction":1,"row":{"_col0":6,"_col1":8}}
    {"operation":0,"originalTransaction":1,"bucket":536936448,"rowId":2,"currentTransaction":1,"row":{"_col0":9,"_col1":10}}
    */
    rs = runStatementOnDriver("select a, b from T order by a, b");
    Assert.assertEquals(stringifyValues(values), rs);
    rs = runStatementOnDriver("select ROW__ID from T group by ROW__ID having count(*) > 1");
    if(rs.size() > 0) {
      Assert.assertEquals("Duplicate ROW__IDs: " + rs.get(0), 0, rs.size());
    }
  }
  /**
   * see HIVE-16177
   * See also {@link TestTxnCommands2#testNonAcidToAcidConversion02()}
   */
  @Test
  public void testToAcidConversion02() throws Exception {
    //create 2 rows in a file 00000_0
    runStatementOnDriver("insert into " + Table.NONACIDNONBUCKET + "(a,b) values(1,2),(1,3)");
    //create 4 rows in a file 000000_0_copy_1
    runStatementOnDriver("insert into " + Table.NONACIDNONBUCKET + "(a,b) values(0,12),(0,13),(1,4),(1,5)");
    //create 1 row in a file 000000_0_copy_2
    runStatementOnDriver("insert into " + Table.NONACIDNONBUCKET + "(a,b) values(1,6)");

    //convert the table to Acid  //todo: remove trans_prop after HIVE-17089
    runStatementOnDriver("alter table " + Table.NONACIDNONBUCKET + " SET TBLPROPERTIES ('transactional'='true', 'transactional_properties'='default')");
    List<String> rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from " +  Table.NONACIDNONBUCKET + " order by ROW__ID");
    LOG.warn("before acid ops (after convert)");
    for(String s : rs) {
      LOG.warn(s);
    }
    //create a some of delta directories
    runStatementOnDriver("insert into " + Table.NONACIDNONBUCKET + "(a,b) values(0,15),(1,16)");
    runStatementOnDriver("update " + Table.NONACIDNONBUCKET + " set b = 120 where a = 0 and b = 12");
    runStatementOnDriver("insert into " + Table.NONACIDNONBUCKET + "(a,b) values(0,17)");
    runStatementOnDriver("delete from " + Table.NONACIDNONBUCKET + " where a = 1 and b = 3");

    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from " +  Table.NONACIDNONBUCKET + " order by a,b");
    LOG.warn("before compact");
    for(String s : rs) {
      LOG.warn(s);
    }
    Assert.assertEquals(0, BucketCodec.determineVersion(536870912).decodeWriterId(536870912));
    /*
     * All ROW__IDs are unique on read after conversion to acid
     * ROW__IDs are exactly the same before and after compaction
     * Also check the file name (only) after compaction for completeness
     */
    String[][] expected = {
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":3}\t0\t13",
            "bucket_00000", "000000_0_copy_1"},
        {"{\"writeid\":10000001,\"bucketid\":536870912,\"rowid\":0}\t0\t15",
            "bucket_00000", "bucket_00000_0"},
        {"{\"writeid\":10000003,\"bucketid\":536870912,\"rowid\":0}\t0\t17",
            "bucket_00000", "bucket_00000_0"},
        {"{\"writeid\":10000002,\"bucketid\":536936449,\"rowid\":0}\t0\t120",
            "bucket_00001", "bucket_00001_0"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":0}\t1\t2",
            "bucket_00000", "000000_0"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":4}\t1\t4",
            "bucket_00000", "000000_0_copy_1"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":5}\t1\t5",
            "bucket_00000", "000000_0_copy_1"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":6}\t1\t6",
            "bucket_00000", "000000_0_copy_2"},
        {"{\"writeid\":10000001,\"bucketid\":536870912,\"rowid\":1}\t1\t16",
            "bucket_00000", "bucket_00000_0"}
    };
    Assert.assertEquals("Unexpected row count before compaction", expected.length, rs.size());
    for(int i = 0; i < expected.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i), rs.get(i).startsWith(expected[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " bc: " + rs.get(i), rs.get(i).endsWith(expected[i][2]));
    }
    //run Compaction
    runStatementOnDriver("alter table "+ Table.NONACIDNONBUCKET +" compact 'major'");
    TestTxnCommands2.runWorker(hiveConf);
    /*
    nonacidnonbucket/
    ├── 000000_0
    ├── 000000_0_copy_1
    ├── 000000_0_copy_2
    ├── base_0000004
    │   └── bucket_00000
    ├── delete_delta_0000002_0000002_0000
    │   └── bucket_00000
    ├── delete_delta_0000004_0000004_0000
    │   └── bucket_00000
    ├── delta_0000001_0000001_0000
    │   └── bucket_00000
    ├── delta_0000002_0000002_0000
    │   └── bucket_00000
    └── delta_0000003_0000003_0000
        └── bucket_00000

    6 directories, 9 files
    */
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from " + Table.NONACIDNONBUCKET + " order by a,b");
    LOG.warn("after compact");
    for(String s : rs) {
      LOG.warn(s);
    }
    Assert.assertEquals("Unexpected row count after compaction", expected.length, rs.size());
    for(int i = 0; i < expected.length; i++) {
      Assert.assertTrue("Actual line " + i + " ac: " + rs.get(i), rs.get(i).startsWith(expected[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " ac: " + rs.get(i), rs.get(i).endsWith(expected[i][1]));
    }
    //make sure they are the same before and after compaction
  }
  /**
   * Currently CTAS doesn't support bucketed tables.  Correspondingly Acid only supports CTAS for
   * unbucketed tables.  This test is here to make sure that if CTAS is made to support unbucketed
   * tables, that it raises a red flag for Acid.
   */
  @Test
  public void testCtasBucketed() throws Exception {
    runStatementOnDriver("insert into " + Table.NONACIDNONBUCKET + "(a,b) values(1,2),(1,3)");
    CommandProcessorException e = runStatementOnDriverNegative("create table myctas " +
      "clustered by (a) into 2 buckets stored as ORC TBLPROPERTIES ('transactional'='true') as " +
      "select a, b from " + Table.NONACIDORCTBL);
    ErrorMsg.CTAS_PARCOL_COEXISTENCE.getErrorCode(); //this code doesn't propagate
//    Assert.assertEquals("Wrong msg", ErrorMsg.CTAS_PARCOL_COEXISTENCE.getErrorCode(), cpr.getErrorCode());
    Assert.assertTrue(e.getMessage().contains("CREATE-TABLE-AS-SELECT does not support"));
  }
  /**
   * Currently CTAS doesn't support partitioned tables.  Correspondingly Acid only supports CTAS for
   * un-partitioned tables.  This test is here to make sure that if CTAS is made to support
   * un-partitioned tables, that it raises a red flag for Acid.
   */
  @Test
  public void testCtasPartitioned() throws Exception {
    runStatementOnDriver("insert into " + Table.NONACIDNONBUCKET + "(a,b) values(1,2),(1,3)");
    CommandProcessorException e = runStatementOnDriverNegative("create table myctas partitioned " +
        "by (b int) stored as " +
        "ORC TBLPROPERTIES ('transactional'='true') as select a, b from " + Table.NONACIDORCTBL);
    ErrorMsg.CTAS_PARCOL_COEXISTENCE.getErrorCode(); //this code doesn't propagate
    Assert.assertTrue(e.getMessage().contains("CREATE-TABLE-AS-SELECT does not support " +
        "partitioning in the target table"));
  }
  /**
   * Tests to check that we are able to use vectorized acid reader,
   * VectorizedOrcAcidRowBatchReader, when reading "original" files,
   * i.e. those that were written before the table was converted to acid.
   * See also acid_vectorization_original*.q
   */
  @Test
  public void testNonAcidToAcidVectorzied() throws Exception {
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
    //this enables vectorization of ROW__ID
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ROW_IDENTIFIER_ENABLED, true);//HIVE-12631
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T(a int, b int) stored as orc tblproperties('transactional'='false')");
    int[][] values = {{1,2},{2,4},{5,6},{6,8},{9,10}};
    runStatementOnDriver("insert into T(a, b) " + makeValuesClause(values));
    //, 'transactional_properties'='default'
    runStatementOnDriver("alter table T SET TBLPROPERTIES ('transactional'='true')");
    //Execution mode: vectorized
      //this uses VectorizedOrcAcidRowBatchReader
    String query = "select a from T where b > 6 order by a";
    List<String> rs = runStatementOnDriver(query);
    String[][] expected = {
      {"6", ""},
      {"9", ""},
    };
    checkExpected(rs, expected, "After conversion");
    Assert.assertEquals(Integer.toString(6), rs.get(0));
    Assert.assertEquals(Integer.toString(9), rs.get(1));
    assertVectorized(shouldVectorize(), query);

    //why isn't PPD working.... - it is working but storage layer doesn't do row level filtering; only row group level
    //this uses VectorizedOrcAcidRowBatchReader
    query = "select ROW__ID, a from T where b > 6 order by a";
    rs = runStatementOnDriver(query);
    String[][] expected1 = {
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":3}", "6"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":4}", "9"}
    };
    checkExpected(rs, expected1, "After conversion with VC1");
    assertVectorized(shouldVectorize(), query);

    //this uses VectorizedOrcAcidRowBatchReader
    query = "select ROW__ID, a from T where b > 0 order by a";
    rs = runStatementOnDriver(query);
    String[][] expected2 = {
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":0}", "1"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":1}", "2"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":2}", "5"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":3}", "6"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":4}", "9"}
    };
    checkExpected(rs, expected2, "After conversion with VC2");
    assertVectorized(shouldVectorize(), query);

    //doesn't vectorize (uses neither of the Vectorzied Acid readers)
    query = "select ROW__ID, a, INPUT__FILE__NAME from T where b > 6 order by a";
    rs = runStatementOnDriver(query);
    Assert.assertEquals("", 2, rs.size());
    String[][] expected3 = {
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":3}\t6", "warehouse/t/000000_0"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":4}\t9", "warehouse/t/000000_0"}
    };
    checkExpected(rs, expected3, "After non-vectorized read");
    Assert.assertEquals(0, BucketCodec.determineVersion(536870912).decodeWriterId(536870912));
    //vectorized because there is INPUT__FILE__NAME
    assertVectorized(false, query);

    runStatementOnDriver("update T set b = 17 where a = 1");
    //this should use VectorizedOrcAcidRowReader
    query = "select ROW__ID, b from T where b > 0 order by a";
    rs = runStatementOnDriver(query);
    String[][] expected4 = {
        {"{\"writeid\":10000001,\"bucketid\":536870913,\"rowid\":0}","17"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":1}","4"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":2}","6"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":3}","8"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":4}","10"}
    };
    checkExpected(rs, expected4, "After conversion with VC4");
    assertVectorized(shouldVectorize(), query);

    runStatementOnDriver("alter table T compact 'major'");
    TestTxnCommands2.runWorker(hiveConf);
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history", 1, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0 compaction state", TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(0).getState());
    Assert.assertTrue(resp.getCompacts().get(0).getHadoopJobId().startsWith("job_local"));

    //this should not vectorize at all
    query = "select ROW__ID, a, b, INPUT__FILE__NAME from T where b > 0 order by a, b";
    rs = runStatementOnDriver(query);
    String[][] expected5 = {//the row__ids are the same after compaction
        {"{\"writeid\":10000001,\"bucketid\":536870913,\"rowid\":0}\t1\t17",
            "warehouse/t/base_10000001_v0000030/bucket_00000"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":1}\t2\t4",
            "warehouse/t/base_10000001_v0000030/bucket_00000"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":2}\t5\t6",
            "warehouse/t/base_10000001_v0000030/bucket_00000"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":3}\t6\t8",
            "warehouse/t/base_10000001_v0000030/bucket_00000"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":4}\t9\t10",
            "warehouse/t/base_10000001_v0000030/bucket_00000"}
    };
    checkExpected(rs, expected5, "After major compaction");
    //vectorized because there is INPUT__FILE__NAME
    assertVectorized(false, query);
  }
  private void checkExpected(List<String> rs, String[][] expected, String msg) {
    super.checkExpected(rs, expected, msg, LOG, true);
  }
  /**
   * HIVE-17900
   */
  @Test
  public void testCompactStatsGather() throws Exception {
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_OPT_SORT_DYNAMIC_PARTITION_THRESHOLD, -1);
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T(a int, b int) partitioned by (p int, q int) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");

    int[][] targetVals = {{4, 1, 1}, {4, 2, 2}, {4, 3, 1}, {4, 4, 2}};
    //we only recompute stats after major compact if they existed before
    runStatementOnDriver("insert into T partition(p=1,q) " + makeValuesClause(targetVals));
    runStatementOnDriver("analyze table T  partition(p=1) compute statistics for columns");

    //Check basic stats for p=1/q=2
    org.apache.hadoop.hive.ql.metadata.Table hiveTable = Hive.get().getTable("T");
    List<org.apache.hadoop.hive.ql.metadata.Partition> partitions = Hive.get().getPartitions(hiveTable);
    Map<String, String> parameters = partitions
            .stream()
            .filter(p -> p.getName().equals("p=1/q=2"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Could not get Partition"))
            .getParameters();
    Assert.assertEquals("The number of files is differing from the expected", "1", parameters.get("numFiles"));
    Assert.assertEquals("The number of rows is differing from the expected", "2", parameters.get("numRows"));
    Assert.assertEquals("The total table size is differing from the expected", "692", parameters.get("totalSize"));

    int[][] targetVals2 = {{5, 1, 1}, {5, 2, 2}, {5, 3, 1}, {5, 4, 2}};
    runStatementOnDriver("insert into T partition(p=1,q) " + makeValuesClause(targetVals2));

    String query = "select ROW__ID, p, q, a, b, INPUT__FILE__NAME from T order by p, q, a, b";
    List<String> rs = runStatementOnDriver(query);
    String[][] expected = {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t1\t4\t1", "t/p=1/q=1/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t1\t1\t4\t3", "t/p=1/q=1/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":0}\t1\t1\t5\t1", "t/p=1/q=1/delta_0000003_0000003_0000/bucket_00000_0"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":1}\t1\t1\t5\t3", "t/p=1/q=1/delta_0000003_0000003_0000/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2\t4\t2", "t/p=1/q=2/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t1\t2\t4\t4", "t/p=1/q=2/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":0}\t1\t2\t5\t2", "t/p=1/q=2/delta_0000003_0000003_0000/bucket_00000_0"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":1}\t1\t2\t5\t4", "t/p=1/q=2/delta_0000003_0000003_0000/bucket_00000_0"}
    };
    checkExpected(rs, expected, "insert data");

    //run major compaction
    runStatementOnDriver("alter table T partition(p=1,q=2) compact 'major'");
    TestTxnCommands2.runWorker(hiveConf);

    query = "select ROW__ID, p, q, a, b, INPUT__FILE__NAME from T order by p, q, a, b";
    rs = runStatementOnDriver(query);
    String[][] expected2 = {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t1\t4\t1", "t/p=1/q=1/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t1\t1\t4\t3", "t/p=1/q=1/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":0}\t1\t1\t5\t1", "t/p=1/q=1/delta_0000003_0000003_0000/bucket_00000_0"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":1}\t1\t1\t5\t3", "t/p=1/q=1/delta_0000003_0000003_0000/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2\t4\t2", "t/p=1/q=2/base_0000003_v0000021/bucket_00000"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t1\t2\t4\t4", "t/p=1/q=2/base_0000003_v0000021/bucket_00000"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":0}\t1\t2\t5\t2", "t/p=1/q=2/base_0000003_v0000021/bucket_00000"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":1}\t1\t2\t5\t4", "t/p=1/q=2/base_0000003_v0000021/bucket_00000"}
    };
    checkExpected(rs, expected2, "after major compaction");

    //check status of compaction job
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history", 1, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0 compaction state", TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(0).getState());
    Assert.assertTrue(resp.getCompacts().get(0).getHadoopJobId().startsWith("job_local"));

    //Check basic stats are updated for p=1/q=2, and compaction is done (numFiles=1)
    partitions = Hive.get().getPartitions(hiveTable);
    parameters = partitions
            .stream()
            .filter(p -> p.getName().equals("p=1/q=2"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Could not get Partition"))
            .getParameters();
    Assert.assertEquals("The number of files is differing from the expected", "1", parameters.get("numFiles"));
    Assert.assertEquals("The number of rows is differing from the expected", "4", parameters.get("numRows"));
    Assert.assertEquals("The total table size is differing from the expected", "705", parameters.get("totalSize"));
  }

  @Test
  public void testDefault() throws Exception {
    hiveConf.set(MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID.getVarname(), "true");
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T (a int, b int) stored as orc");
    runStatementOnDriver("insert into T values(1,2),(3,4)");
    String query = "select ROW__ID, a, b, INPUT__FILE__NAME from T order by a, b";
    List<String> rs = runStatementOnDriver(query);
    String[][] expected = {
        //this proves data is written in Acid layout so T was made Acid
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2", "t/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t3\t4", "t/delta_0000001_0000001_0000/bucket_00000_0"}
    };
    checkExpected(rs, expected, "insert data");
  }
  /**
   * see HIVE-18429
   */
  @Test
  public void testEmptyCompactionResult() throws Exception {
    hiveConf.set(MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID.getVarname(), "true");
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T (a int, b int) stored as orc");
    int[][] data = {{1,2}, {3,4}};
    runStatementOnDriver("insert into T" + makeValuesClause(data));
    runStatementOnDriver("insert into T" + makeValuesClause(data));

    //delete the bucket files so now we have empty delta dirs
    List<String> rs = runStatementOnDriver("select distinct INPUT__FILE__NAME from T");
    FileSystem fs = FileSystem.get(hiveConf);
    for(String path : rs) {
      fs.delete(new Path(path), true);
    }
    runStatementOnDriver("alter table T compact 'major'");
    TestTxnCommands2.runWorker(hiveConf);

    //check status of compaction job
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history", 1, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0 compaction state", TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(0).getState());
    Assert.assertTrue(resp.getCompacts().get(0).getHadoopJobId().startsWith("job_local"));

    //now run another compaction make sure empty dirs don't cause issues
    runStatementOnDriver("insert into T" + makeValuesClause(data));
    runStatementOnDriver("alter table T compact 'major'");
    TestTxnCommands2.runWorker(hiveConf);

    //check status of compaction job
    resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history", 2, resp.getCompactsSize());
    for(int i = 0; i < 2; i++) {
      Assert.assertEquals("Unexpected 0 compaction state", TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(i).getState());
      Assert.assertTrue(resp.getCompacts().get(i).getHadoopJobId().startsWith("job_local"));
    }
    rs = runStatementOnDriver("select a, b from T order by a, b");
    Assert.assertEquals(stringifyValues(data), rs);

  }

  /**
   * HIVE-27268
   */
  @Test
  public void testGetPartitionsNoSession() throws Exception {
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_OPT_SORT_DYNAMIC_PARTITION_THRESHOLD, -1);
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T(a int, b int) partitioned by (p int, q int) " +
        "stored as orc TBLPROPERTIES ('transactional'='true')");

    int[][] targetVals = {{4, 1, 1}, {4, 2, 2}, {4, 3, 1}, {4, 4, 2}};
    //we only recompute stats after major compact if they existed before
    runStatementOnDriver("insert into T partition(p=1,q) " + makeValuesClause(targetVals));
    runStatementOnDriver("analyze table T  partition(p=1) compute statistics for columns");

    Hive hive = Hive.get();
    org.apache.hadoop.hive.ql.metadata.Table hiveTable = hive.getTable("T");
    // this will ensure the getValidWriteIdList has no session to work with (thru getPartitions)
    SessionState.detachSession();
    List<org.apache.hadoop.hive.ql.metadata.Partition> partitions = hive.getPartitions(hiveTable);
    Assert.assertNotNull(partitions);
    // prevent tear down failure
    d.close();
    d = null;
  }
}

