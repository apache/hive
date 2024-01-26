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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.orc.OrcProto;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.stringifyValues;
import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.makeValuesClause;
import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.runWorker;
import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.runCleaner;

/**
 * This class resides in itests to facilitate running query using Tez engine, since the jars are
 * fully loaded here, which is not the case if it stays in ql.
 */
public class TestAcidOnTez {
  static final private Logger LOG = LoggerFactory.getLogger(TestAcidOnTez.class);
  public static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
      File.separator + TestAcidOnTez.class.getCanonicalName()
      + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
  public static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  //bucket count for test tables; set it to 1 for easier debugging
  private static int BUCKET_COUNT = 2;
  @Rule
  public TestName testName = new TestName();
  private HiveConf hiveConf;
  private IDriver d;
  private static enum Table {
    ACIDTBL("acidTbl"),
    ACIDTBLPART("acidTblPart"),
    ACIDNOBUCKET("acidNoBucket"),
    NONACIDORCTBL("nonAcidOrcTbl"),
    NONACIDPART("nonAcidPart"),
    NONACIDNONBUCKET("nonAcidNonBucket");

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
    hiveConf = new HiveConf(this.getClass());
    hiveConf.set(HiveConf.ConfVars.PRE_EXEC_HOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POST_EXEC_HOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname, TEST_WAREHOUSE_DIR);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_MAPRED_MODE, "nonstrict");
    hiveConf.setVar(HiveConf.ConfVars.HIVE_INPUT_FORMAT, HiveInputFormat.class.getName());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER, 
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON, true);
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON, true);
    TestTxnDbUtil.setConfValues(hiveConf);
    hiveConf.setInt(MRJobConfig.MAP_MEMORY_MB, 1024);
    hiveConf.setInt(MRJobConfig.REDUCE_MEMORY_MB, 1024);
    TestTxnDbUtil.prepDb(hiveConf);
    File f = new File(TEST_WAREHOUSE_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(TEST_WAREHOUSE_DIR).mkdirs())) {
      throw new RuntimeException("Could not create " + TEST_WAREHOUSE_DIR);
    }
    SessionState.start(new SessionState(hiveConf));
    d = DriverFactory.newDriver(hiveConf);
    dropTables();
    runStatementOnDriver("create table " + Table.ACIDTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT
        + " buckets stored as orc " + getTblProperties());
    runStatementOnDriver("create table " + Table.ACIDTBLPART
        + "(a int, b int) partitioned by (p string) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc "
        + getTblProperties());
    runStatementOnDriver("create table " + Table.NONACIDORCTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT
        + " buckets stored as orc ");
    runStatementOnDriver("create table " + Table.NONACIDPART
        + "(a int, b int) partitioned by (p string) stored as orc ");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(1,2)");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(3,4)");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(5,6)");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(7,8)");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(9,10)");
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,2),(3,4),(5,6),(7,8),(9,10)");
  }

  /**
   * this is to test differety types of Acid tables
   */
  String getTblProperties() {
    return "TBLPROPERTIES ('transactional'='true')";
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
        d.close();
        d.destroy();
        d = null;
      }
      TestTxnDbUtil.cleanDb(hiveConf);
    } finally {
      FileUtils.deleteDirectory(new File(TEST_DATA_DIR));
    }
  }

  @Test
  public void testMergeJoinOnMR() throws Exception {
    testJoin("mr", "MergeJoin");
  }

  @Test
  public void testMapJoinOnMR() throws Exception {
    testJoin("mr", "MapJoin");
  }

  @Test
  public void testMergeJoinOnTez() throws Exception {
    testJoin("tez", "MergeJoin");
  }

  @Test
  public void testMapJoinOnTez() throws Exception {
    testJoin("tez", "MapJoin");
  }

  /**
   * Tests non acid to acid conversion where starting table has non-standard layout, i.e.
   * where "original" files are not immediate children of the partition dir
   */
  @Ignore("HIVE-19509: Disable tests that are failing continuously")
  @Test
  public void testNonStandardConversion01() throws Exception {
    HiveConf confForTez = new HiveConf(hiveConf); // make a clone of existing hive conf
    setupTez(confForTez);
    //CTAS with non-ACID target table
    runStatementOnDriver("create table " + Table.NONACIDNONBUCKET + " stored as ORC TBLPROPERTIES('transactional'='false') as " +
      "select a, b from " + Table.ACIDTBL + " where a <= 5 union all select a, b from " + Table.NONACIDORCTBL + " where a >= 5", confForTez);

    List<String> rs = runStatementOnDriver("select a, b, INPUT__FILE__NAME from " + Table.NONACIDNONBUCKET + " order by a, b, INPUT__FILE__NAME", confForTez);
    String expected0[][] = {
      {"1\t2", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "1/000000_0"},
      {"3\t4", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "1/000000_0"},
      {"5\t6", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "1/000000_0"},
      {"5\t6", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "2/000000_0"},
      {"7\t8", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "2/000000_0"},
      {"9\t10", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "2/000000_0"},
    };
    Assert.assertEquals("Unexpected row count after ctas", expected0.length, rs.size());
    //verify data and layout
    for(int i = 0; i < expected0.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i), rs.get(i).startsWith(expected0[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " bc: " + rs.get(i), rs.get(i).endsWith(expected0[i][1]));
    }
    //make the table ACID
    runStatementOnDriver("alter table " + Table.NONACIDNONBUCKET + " SET TBLPROPERTIES ('transactional'='true')", confForTez);

    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from " + Table.NONACIDNONBUCKET + " order by ROW__ID", confForTez);
    LOG.warn("after ctas:");
    for (String s : rs) {
      LOG.warn(s);
    }
    Assert.assertEquals(0, BucketCodec.determineVersion(536870912).decodeWriterId(536870912));
    /*
    * Expected result 0th entry i the RecordIdentifier + data.  1st entry file before compact*/
    String expected[][] = {
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":0}\t5\t6", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "1/000000_0"},
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":1}\t3\t4", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "1/000000_0"},
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":2}\t1\t2", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "1/000000_0"},
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":3}\t9\t10", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "2/000000_0"},
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":4}\t7\t8", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "2/000000_0"},
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":5}\t5\t6", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "2/000000_0"},
    };
    Assert.assertEquals("Unexpected row count after ctas", expected.length, rs.size());
    //verify data and layout
    for(int i = 0; i < expected.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i), rs.get(i).startsWith(expected[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " bc: " + rs.get(i), rs.get(i).endsWith(expected[i][1]));
    }
    //perform some Update/Delete
    runStatementOnDriver("update " + Table.NONACIDNONBUCKET + " set a = 70, b  = 80 where a = 7", confForTez);
    runStatementOnDriver("delete from " + Table.NONACIDNONBUCKET + " where a = 5", confForTez);
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from " + Table.NONACIDNONBUCKET + " order by ROW__ID", confForTez);
    LOG.warn("after update/delete:");
    for (String s : rs) {
      LOG.warn(s);
    }
    String[][] expected2 = {
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":1}\t3\t4", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "1/000000_0"},
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":2}\t1\t2", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "1/000000_0"},
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":3}\t9\t10", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "2/000000_0"},
      {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t70\t80", "delta_0000001_0000001_0000/bucket_00000"}
    };
    Assert.assertEquals("Unexpected row count after update", expected2.length, rs.size());
    //verify data and layout
    for(int i = 0; i < expected2.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i), rs.get(i).startsWith(expected2[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " bc: " + rs.get(i), rs.get(i).endsWith(expected2[i][1]));
    }
    //now make sure delete deltas are present
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDNONBUCKET).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    String[] expectedDelDelta = {"delete_delta_0000001_0000001_0000", "delete_delta_0000002_0000002_0000"};
    for(FileStatus stat : status) {
      for(int i = 0; i < expectedDelDelta.length; i++) {
        if(expectedDelDelta[i] != null && stat.getPath().toString().endsWith(expectedDelDelta[i])) {
          expectedDelDelta[i] = null;
        }
      }
    }
    for(int i = 0; i < expectedDelDelta.length; i++) {
      Assert.assertNull("at " + i + " " + expectedDelDelta[i] + " not found on disk", expectedDelDelta[i]);
    }
    //run Minor compaction
    runStatementOnDriver("alter table " + Table.NONACIDNONBUCKET + " compact 'minor'", confForTez);
    runWorker(hiveConf);
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from " + Table.NONACIDNONBUCKET + " order by ROW__ID", confForTez);
    LOG.warn("after compact minor:");
    for (String s : rs) {
      LOG.warn(s);
    }
    Assert.assertEquals("Unexpected row count after update", expected2.length, rs.size());
    //verify the data is the same
    for(int i = 0; i < expected2.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i), rs.get(i).startsWith(expected2[i][0]));
      //todo: since HIVE-16669 is not done, Minor compact compacts insert delta as well - it should not
      //Assert.assertTrue("Actual line(file) " + i + " bc: " + rs.get(i), rs.get(i).endsWith(expected2[i][1]));
    }
    //check we have right delete delta files after minor compaction
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDNONBUCKET).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    String[] expectedDelDelta2 = {"delete_delta_0000001_0000001_0000", "delete_delta_0000002_0000002_0000", "delete_delta_0000001_0000002"};
    for(FileStatus stat : status) {
      for(int i = 0; i < expectedDelDelta2.length; i++) {
        if(expectedDelDelta2[i] != null && stat.getPath().toString().endsWith(expectedDelDelta2[i])) {
          expectedDelDelta2[i] = null;
          break;
        }
      }
    }
    for(int i = 0; i < expectedDelDelta2.length; i++) {
      Assert.assertNull("at " + i + " " + expectedDelDelta2[i] + " not found on disk", expectedDelDelta2[i]);
    }
    //run Major compaction
    runStatementOnDriver("alter table " + Table.NONACIDNONBUCKET + " compact 'major'", confForTez);
    runWorker(hiveConf);
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from " + Table.NONACIDNONBUCKET + " order by ROW__ID", confForTez);
    LOG.warn("after compact major:");
    for (String s : rs) {
      LOG.warn(s);
    }
    Assert.assertEquals("Unexpected row count after major compact", expected2.length, rs.size());
    for(int i = 0; i < expected2.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i), rs.get(i).startsWith(expected2[i][0]));
      //everything is now in base/
      Assert.assertTrue("Actual line(file) " + i + " bc: " + rs.get(i), rs.get(i).endsWith("base_0000002/bucket_00000"));
    }
  }
  /**
   * Tests non acid to acid conversion where starting table has non-standard layout, i.e.
   * where "original" files are not immediate children of the partition dir - partitioned table
   *
   * How to do this?  CTAS is the only way to create data files which are not immediate children
   * of the partition dir.  CTAS/Union/Tez doesn't support partition tables.  The only way is to copy
   * data files in directly.
   *
   * Actually Insert Into ... select ... union all ... with
   * HIVE_OPTIMIZE_UNION_REMOVE (and HIVE_FETCH_TASK_CONVERSION="none"?) will create subdirs
   * but if writing to non acid table there is a merge task on MR (but not on Tez)
   */
  @Ignore("HIVE-17214")//this consistently works locally but never in ptest....
  @Test
  public void testNonStandardConversion02() throws Exception {
    HiveConf confForTez = new HiveConf(hiveConf); // make a clone of existing hive conf
    confForTez.setBoolean("mapred.input.dir.recursive", true);
    setupTez(confForTez);
    runStatementOnDriver("create table " + Table.NONACIDNONBUCKET + " stored as ORC " +
      "TBLPROPERTIES('transactional'='false') as " +
      "select a, b from " + Table.ACIDTBL + " where a <= 3 union all " +
      "select a, b from " + Table.NONACIDORCTBL + " where a >= 7 " +
      "union all select a, b from " + Table.ACIDTBL + " where a = 5", confForTez);

    List<String> rs = runStatementOnDriver("select a, b, INPUT__FILE__NAME from " +
      Table.NONACIDNONBUCKET + " order by a, b", confForTez);
    String expected0[][] = {
      {"1\t2", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "1/000000_0"},
      {"3\t4", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "1/000000_0"},
      {"5\t6", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "3/000000_0"},
      {"7\t8", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "2/000000_0"},
      {"9\t10", AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "2/000000_0"},
    };
    Assert.assertEquals("Unexpected row count after ctas", expected0.length, rs.size());
    //verify data and layout
    for(int i = 0; i < expected0.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i), rs.get(i).startsWith(expected0[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " bc: " + rs.get(i), rs.get(i).endsWith(expected0[i][1]));
    }
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.NONACIDNONBUCKET).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    //ensure there is partition dir
    runStatementOnDriver("insert into " + Table.NONACIDPART + " partition (p=1) values (100,110)", confForTez);
    //creates more files in that partition
    for(FileStatus stat : status) {
      int limit = 5;
      Path p = stat.getPath();//dirs 1/, 2/, 3/
      Path to = new Path(TEST_WAREHOUSE_DIR + "/" +  Table.NONACIDPART+ "/p=1/" + p.getName());
      while(limit-- > 0 && !fs.rename(p, to)) {
        Thread.sleep(200);
      }
      if(limit <= 0) {
        throw new IllegalStateException("Could not rename " + p + " to " + to);
      }
    }
    /*
    This is what we expect on disk
    ekoifman:warehouse ekoifman$ tree nonacidpart/
    nonacidpart/
    └── p=1
    ├── 000000_0
    ├── HIVE_UNION_SUBDIR__1
    │   └── 000000_0
    ├── HIVE_UNION_SUBDIR_2
    │   └── 000000_0
    └── HIVE_UNION_SUBDIR_3
        └── 000000_0

4 directories, 4 files
    **/
    //make the table ACID
    runStatementOnDriver("alter table " + Table.NONACIDPART + " SET TBLPROPERTIES ('transactional'='true')", confForTez);
    rs = runStatementOnDriver("select ROW__ID, a, b, p, INPUT__FILE__NAME from " + Table.NONACIDPART + " order by ROW__ID", confForTez);
    LOG.warn("after acid conversion:");
    for (String s : rs) {
      LOG.warn(s);
    }
    String[][] expected = {
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":0}\t100\t110\t1", "nonacidpart/p=1/000000_0"},
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":1}\t1\t2\t1", "nonacidpart/p=1/" + AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "1/000000_0"},
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":2}\t3\t4\t1", "nonacidpart/p=1/" + AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "1/000000_0"},
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":3}\t9\t10\t1", "nonacidpart/p=1/" + AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "2/000000_0"},
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":4}\t7\t8\t1", "nonacidpart/p=1/" + AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "2/000000_0"},
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":5}\t5\t6\t1", "nonacidpart/p=1/" + AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "3/000000_0"}
    };
    Assert.assertEquals("Wrong row count", expected.length, rs.size());
    //verify data and layout
    for(int i = 0; i < expected.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i), rs.get(i).startsWith(expected[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " bc: " + rs.get(i), rs.get(i).endsWith(expected[i][1]));
    }

    //run Major compaction
    runStatementOnDriver("alter table " + Table.NONACIDPART + " partition (p=1) compact 'major'", confForTez);
    runWorker(hiveConf);
    rs = runStatementOnDriver("select ROW__ID, a, b, p, INPUT__FILE__NAME from " + Table.NONACIDPART + " order by ROW__ID", confForTez);
    LOG.warn("after major compaction:");
    for (String s : rs) {
      LOG.warn(s);
    }
    //verify data and layout
    for(int i = 0; i < expected.length; i++) {
      Assert.assertTrue("Actual line " + i + " ac: " + rs.get(i), rs.get(i).startsWith(expected[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " ac: " +
        rs.get(i), rs.get(i).endsWith("nonacidpart/p=1/base_-9223372036854775808/bucket_00000"));
    }

  }
  /**
   * CTAS + Tez + Union creates a non-standard layout in table dir
   * Each leg of the union places data into a subdir of the table/partition.
   * Subdirs are named HIVE_UNION_SUBDIR_1/, HIVE_UNION_SUBDIR_2/, etc
   * For Acid tables the writer for each dir must have a different statementId ensured by
   * {@link org.apache.hadoop.hive.ql.optimizer.QueryPlanPostProcessor}.
   * {@link org.apache.hadoop.hive.ql.metadata.Hive#moveAcidFiles(FileSystem, FileStatus[], Path, List)} drops the union subdirs
   * since each delta file has a unique name.
   */
  @Test
  public void testCtasTezUnion() throws Exception {
    HiveConf confForTez = new HiveConf(hiveConf); // make a clone of existing hive conf
    confForTez.setBoolVar(HiveConf.ConfVars.HIVE_EXPLAIN_USER, false);
    setupTez(confForTez);
    //CTAS with ACID target table
    List<String> rs0 = runStatementOnDriver("explain create table " + Table.ACIDNOBUCKET + " stored as ORC TBLPROPERTIES('transactional'='true') as " +
      "(select a, b from " + Table.ACIDTBL + " where a <= 5 order by a desc , b desc limit 3) " +
        "union all (select a, b from " + Table.NONACIDORCTBL + " where a >= 5 order by a desc, b desc limit 3)",
        confForTez);
    LOG.warn("explain ctas:");//TezEdgeProperty.EdgeType
    for (String s : rs0) {
      LOG.warn(s);
    }
    runStatementOnDriver("create table " + Table.ACIDNOBUCKET + " stored as ORC TBLPROPERTIES('transactional'='true') as " +
      "(select a, b from " + Table.ACIDTBL + " where a <= 5 order by a desc, b desc limit 3) " +
        "union all (select a, b from " + Table.NONACIDORCTBL + " where a >= 5 order by a desc, b desc limit 3)",
        confForTez);
    List<String> rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from " + Table.ACIDNOBUCKET + " order by ROW__ID", confForTez);
    LOG.warn("after ctas:");
    for (String s : rs) {
      LOG.warn(s);
    }
    Assert.assertEquals(0, BucketCodec.determineVersion(536870912).decodeWriterId(536870912));
    /*
    * Expected result 0th entry is the RecordIdentifier + data.  1st entry file before compact*/
    String expected[][] = {
      {"{\"writeid\":1,\"bucketid\":536870913,\"rowid\":0}\t5\t6", "/delta_0000001_0000001_0001/bucket_00000_0"},
      {"{\"writeid\":1,\"bucketid\":536870913,\"rowid\":1}\t3\t4", "/delta_0000001_0000001_0001/bucket_00000_0"},
      {"{\"writeid\":1,\"bucketid\":536870913,\"rowid\":2}\t1\t2", "/delta_0000001_0000001_0001/bucket_00000_0"},
      {"{\"writeid\":1,\"bucketid\":536870914,\"rowid\":0}\t9\t10", "/delta_0000001_0000001_0002/bucket_00000_0"},
      {"{\"writeid\":1,\"bucketid\":536870914,\"rowid\":1}\t7\t8", "/delta_0000001_0000001_0002/bucket_00000_0"},
      {"{\"writeid\":1,\"bucketid\":536870914,\"rowid\":2}\t5\t6", "/delta_0000001_0000001_0002/bucket_00000_0"},
    };
    Assert.assertEquals("Unexpected row count after ctas", expected.length, rs.size());
    //verify data and layout
    for(int i = 0; i < expected.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i), rs.get(i).startsWith(expected[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " bc: " + rs.get(i), rs.get(i).endsWith(expected[i][1]));
    }
    //perform some Update/Delete
    runStatementOnDriver("update " + Table.ACIDNOBUCKET + " set a = 70, b  = 80 where a = 7", confForTez);
    runStatementOnDriver("delete from " + Table.ACIDNOBUCKET + " where a = 5", confForTez);
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from " + Table.ACIDNOBUCKET + " order by ROW__ID", confForTez);
    LOG.warn("after update/delete:");
    for (String s : rs) {
      LOG.warn(s);
    }
    String[][] expected2 = {
      {"{\"writeid\":1,\"bucketid\":536870913,\"rowid\":1}\t3\t4", "/delta_0000001_0000001_0001/bucket_00000_0"},
      {"{\"writeid\":1,\"bucketid\":536870913,\"rowid\":2}\t1\t2", "/delta_0000001_0000001_0001/bucket_00000_0"},
      {"{\"writeid\":1,\"bucketid\":536870914,\"rowid\":0}\t9\t10", "/delta_0000001_0000001_0002/bucket_00000_0"},
      {"{\"writeid\":2,\"bucketid\":536870913,\"rowid\":0}\t70\t80", "/delta_0000002_0000002_0001/bucket_00000_0"}
    };
    Assert.assertEquals("Unexpected row count after update", expected2.length, rs.size());
    //verify data and layout
    for(int i = 0; i < expected2.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i), rs.get(i).startsWith(expected2[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " bc: " + rs.get(i), rs.get(i).endsWith(expected2[i][1]));
    }
    //now make sure delete deltas are present
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.ACIDNOBUCKET).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    String[] expectedDelDelta = {"delete_delta_0000002_0000002_0000", "delete_delta_0000003_0000003_0000"};
    for(FileStatus stat : status) {
      for(int i = 0; i < expectedDelDelta.length; i++) {
        if(expectedDelDelta[i] != null && stat.getPath().toString().endsWith(expectedDelDelta[i])) {
          expectedDelDelta[i] = null;
        }
      }
    }
    for(int i = 0; i < expectedDelDelta.length; i++) {
      Assert.assertNull("at " + i + " " + expectedDelDelta[i] + " not found on disk", expectedDelDelta[i]);
    }
    //run Minor compaction
    runStatementOnDriver("alter table " + Table.ACIDNOBUCKET + " compact 'minor'", confForTez);
    runWorker(hiveConf);
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from " + Table.ACIDNOBUCKET + " order by ROW__ID", confForTez);
    LOG.warn("after compact minor:");
    for (String s : rs) {
      LOG.warn(s);
    }
    Assert.assertEquals("Unexpected row count after update", expected2.length, rs.size());
    //verify the data is the same
    for(int i = 0; i < expected2.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i), rs.get(i).startsWith(expected2[i][0]));
      //todo: since HIVE-16669 is not done, Minor compact compacts insert delta as well - it should not
      //Assert.assertTrue("Actual line(file) " + i + " bc: " + rs.get(i), rs.get(i).endsWith(expected2[i][1]));
    }
    //check we have right delete delta files after minor compaction
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (Table.ACIDNOBUCKET).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    String[] expectedDelDelta2 = { "delete_delta_0000002_0000002_0000", "delete_delta_0000003_0000003_0000", "delete_delta_0000001_0000003_v0000023"};
    for(FileStatus stat : status) {
      for(int i = 0; i < expectedDelDelta2.length; i++) {
        if(expectedDelDelta2[i] != null && stat.getPath().toString().endsWith(expectedDelDelta2[i])) {
          expectedDelDelta2[i] = null;
          break;
        }
      }
    }
    for(int i = 0; i < expectedDelDelta2.length; i++) {
      Assert.assertNull("at " + i + " " + expectedDelDelta2[i] + " not found on disk", expectedDelDelta2[i]);
    }
    runCleaner(hiveConf);
    //run Major compaction
    runStatementOnDriver("alter table " + Table.ACIDNOBUCKET + " compact 'major'", confForTez);
    runWorker(hiveConf);
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from " + Table.ACIDNOBUCKET + " order by ROW__ID", confForTez);
    LOG.warn("after compact major:");
    for (String s : rs) {
      LOG.warn(s);
    }
    Assert.assertEquals("Unexpected row count after major compact", expected2.length, rs.size());
    for(int i = 0; i < expected2.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i), rs.get(i).startsWith(expected2[i][0]));
      //everything is now in base/
      Assert.assertTrue("Actual line(file) " + i + " bc: " + rs.get(i), rs.get(i).endsWith("base_0000003_v0000026/bucket_00000"));
    }
  }
  /**
   * 1. Insert into regular unbucketed table from Union all - union is removed and data is placed in
   * subdirs of target table.
   * 2. convert to acid table and check data
   * 3. compact and check data
   * Compare with {@link #testAcidInsertWithRemoveUnion()} where T is transactional=true
   */
  @Test
  public void testInsertWithRemoveUnion() throws Exception {
    int[][] values = {{1,2},{3,4},{5,6},{7,8},{9,10}};
    HiveConf confForTez = new HiveConf(hiveConf); // make a clone of existing hive conf
    setupTez(confForTez);
    runStatementOnDriver("drop table if exists T", confForTez);
    runStatementOnDriver("create table T (a int, b int) stored as ORC  TBLPROPERTIES ('transactional'='false')", confForTez);
    /*
ekoifman:apache-hive-3.0.0-SNAPSHOT-bin ekoifman$ tree  ~/dev/hiverwgit/itests/hive-unit/target/tmp/org.apache.hadoop.hive.ql.TestAcidOnTez-1505502329802/warehouse/t/.hive-staging_hive_2017-09-15_12-07-33_224_7717909516029836949-1/
/Users/ekoifman/dev/hiverwgit/itests/hive-unit/target/tmp/org.apache.hadoop.hive.ql.TestAcidOnTez-1505502329802/warehouse/t/.hive-staging_hive_2017-09-15_12-07-33_224_7717909516029836949-1/
└── -ext-10000
    ├── HIVE_UNION_SUBDIR_1
    │   └── 000000_0
    ├── HIVE_UNION_SUBDIR_2
    │   └── 000000_0
    └── HIVE_UNION_SUBDIR_3
        └── 000000_0

4 directories, 3 files
     */
    runStatementOnDriver("insert into T(a,b) select a, b from " + Table.ACIDTBL + " where a between 1 and 3 group by a, b union all select a, b from " + Table.ACIDTBL + " where a between 5 and 7 union all select a, b from " + Table.ACIDTBL + " where a >= 9", confForTez);
    List<String> rs = runStatementOnDriver("select a, b, INPUT__FILE__NAME from T order by a, b, INPUT__FILE__NAME", confForTez);
    LOG.warn(testName.getMethodName() + ": before converting to acid");
    for(String s : rs) {
      LOG.warn(s);
    }
    String[][] expected = {
      {"1\t2","warehouse/t/" + AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "1/000000_0"},
      {"3\t4","warehouse/t/" + AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "1/000000_0"},
      {"5\t6","warehouse/t/" + AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "2/000000_0"},
      {"7\t8","warehouse/t/" + AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "2/000000_0"},
      {"9\t10","warehouse/t/" + AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "3/000000_0"}
    };
    Assert.assertEquals("Unexpected row count after conversion", expected.length, rs.size());
    for(int i = 0; i < expected.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i), rs.get(i).startsWith(expected[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " bc: " + rs.get(i), rs.get(i).endsWith(expected[i][1]));
    }
    //make the table ACID
    runStatementOnDriver("alter table T SET TBLPROPERTIES ('transactional'='true')", confForTez);
    rs = runStatementOnDriver("select a,b from T order by a, b", confForTez);
    Assert.assertEquals("After to Acid conversion", stringifyValues(values), rs);

    //run Major compaction
    runStatementOnDriver("alter table T compact 'major'", confForTez);
    runWorker(hiveConf);
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from T order by ROW__ID", confForTez);
    LOG.warn(testName.getMethodName() + ": after compact major of T:");
    for (String s : rs) {
      LOG.warn(s);
    }
    String[][] expected2 = {
       {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":0}\t1\t2",
           "warehouse/t/base_-9223372036854775808_v0000023/bucket_00000"},
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":1}\t3\t4",
          "warehouse/t/base_-9223372036854775808_v0000023/bucket_00000"},
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":2}\t5\t6",
          "warehouse/t/base_-9223372036854775808_v0000023/bucket_00000"},
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":3}\t7\t8",
          "warehouse/t/base_-9223372036854775808_v0000023/bucket_00000"},
      {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":4}\t9\t10",
          "warehouse/t/base_-9223372036854775808_v0000023/bucket_00000"}
    };
    Assert.assertEquals("Unexpected row count after major compact", expected2.length, rs.size());
    for(int i = 0; i < expected2.length; i++) {
      Assert.assertTrue("Actual line " + i + " ac: " + rs.get(i), rs.get(i).startsWith(expected2[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " ac: " + rs.get(i), rs.get(i).endsWith(expected2[i][1]));
    }
  }
  /**
   * 1. Insert into unbucketed acid table from Union all - union is removed and data is placed in
   * subdirs of target table.
   * 2. convert to acid table and check data
   * 3. compact and check data
   * Compare with {@link #testInsertWithRemoveUnion()} where T is transactional=false
   */
  @Test
  public void testAcidInsertWithRemoveUnion() throws Exception {
    HiveConf confForTez = new HiveConf(hiveConf); // make a clone of existing hive conf
    setupTez(confForTez);
    runStatementOnDriver("drop table if exists T", confForTez);
    runStatementOnDriver("create table T (a int, b int) stored as ORC  TBLPROPERTIES ('transactional'='true')", confForTez);
    /*On Tez, below (T is transactional), we get the following layout
ekoifman:apache-hive-3.0.0-SNAPSHOT-bin ekoifman$ tree  ~/dev/hiverwgit/itests/hive-unit/target/tmp/org.apache.hadoop.hive.ql.TestAcidOnTez-1505500035574/warehouse/t/.hive-staging_hive_2017-09-15_11-28-33_960_9111484239090506828-1/
/Users/ekoifman/dev/hiverwgit/itests/hive-unit/target/tmp/org.apache.hadoop.hive.ql.TestAcidOnTez-1505500035574/warehouse/t/.hive-staging_hive_2017-09-15_11-28-33_960_9111484239090506828-1/
└── -ext-10000
    ├── HIVE_UNION_SUBDIR_1
    │   └── 000000_0
    │       ├── _orc_acid_version
    │       └── delta_0000001_0000001_0001
    │           └── bucket_00000
    ├── HIVE_UNION_SUBDIR_2
    │   └── 000000_0
    │       ├── _orc_acid_version
    │       └── delta_0000001_0000001_0002
    │           └── bucket_00000
    └── HIVE_UNION_SUBDIR_3
        └── 000000_0
            ├── _orc_acid_version
            └── delta_0000001_0000001_0003
                └── bucket_00000

10 directories, 6 files     */
    runStatementOnDriver("insert into T(a,b) select a, b from " + Table.ACIDTBL + " where a between 1 and 3 union all select a, b from " + Table.ACIDTBL + " where a between 5 and 7 union all select a, b from " + Table.ACIDTBL + " where a >= 9", confForTez);
    List<String> rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from T order by a, b", confForTez);
    LOG.warn(testName.getMethodName() + ": reading acid table T");
    for(String s : rs) {
      LOG.warn(s);
    }

    String[][] expected2 = {
      {"{\"writeid\":1,\"bucketid\":536870913,\"rowid\":0}\t1\t2", "warehouse/t/delta_0000001_0000001_0001/bucket_00000_0"},
      {"{\"writeid\":1,\"bucketid\":536870913,\"rowid\":1}\t3\t4", "warehouse/t/delta_0000001_0000001_0001/bucket_00000_0"},
      {"{\"writeid\":1,\"bucketid\":536870914,\"rowid\":0}\t5\t6", "warehouse/t/delta_0000001_0000001_0002/bucket_00000_0"},
      {"{\"writeid\":1,\"bucketid\":536870914,\"rowid\":1}\t7\t8", "warehouse/t/delta_0000001_0000001_0002/bucket_00000_0"},
      {"{\"writeid\":1,\"bucketid\":536870915,\"rowid\":0}\t9\t10", "warehouse/t/delta_0000001_0000001_0003/bucket_00000_0"}
    };
    Assert.assertEquals("Unexpected row count", expected2.length, rs.size());
    for(int i = 0; i < expected2.length; i++) {
      Assert.assertTrue("Actual line " + i + " ac: " + rs.get(i), rs.get(i).startsWith(expected2[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " ac: " + rs.get(i), rs.get(i).endsWith(expected2[i][1]));
    }
  }
  @Test
  public void testBucketedAcidInsertWithRemoveUnion() throws Exception {
    HiveConf confForTez = new HiveConf(hiveConf); // make a clone of existing hive conf
    setupTez(confForTez);
    int[][] values = {{1,2},{2,4},{5,6},{6,8},{9,10}};
    runStatementOnDriver("delete from " + Table.ACIDTBL, confForTez);
    //make sure both buckets are not empty
    runStatementOnDriver("insert into " + Table.ACIDTBL + makeValuesClause(values), confForTez);
    runStatementOnDriver("drop table if exists T", confForTez);
    /*
    With bucketed target table Union All is not removed

    ekoifman:apache-hive-3.0.0-SNAPSHOT-bin ekoifman$ tree  ~/dev/hiverwgit/itests/hive-unit/target/tmp/org.apache.hadoop.hive.ql.TestAcidOnTez-1505510130462/warehouse/t/.hive-staging_hive_2017-09-15_14-16-32_422_4626314315862498838-1/
/Users/ekoifman/dev/hiverwgit/itests/hive-unit/target/tmp/org.apache.hadoop.hive.ql.TestAcidOnTez-1505510130462/warehouse/t/.hive-staging_hive_2017-09-15_14-16-32_422_4626314315862498838-1/
└── -ext-10000
    ├── 000000_0
    │   ├── _orc_acid_version
    │   └── delta_0000001_0000001_0000
    │       └── bucket_00000
    └── 000001_0
        ├── _orc_acid_version
        └── delta_0000001_0000001_0000
            └── bucket_00001

5 directories, 4 files
*/
    runStatementOnDriver("create table T (a int, b int) clustered by (a) into 2 buckets stored as ORC  TBLPROPERTIES ('transactional'='true')", confForTez);
    runStatementOnDriver("insert into T(a,b) select a, b from " + Table.ACIDTBL + " where a between 1 and 3 union all select a, b from " + Table.ACIDTBL + " where a between 5 and 7 union all select a, b from " + Table.ACIDTBL + " where a >= 9", confForTez);
    List<String> rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from T order by a, b", confForTez);
    LOG.warn(testName.getMethodName() + ": reading bucketed acid table T");
    for(String s : rs) {
      LOG.warn(s);
    }
    String[][] expected2 = {
      {"{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t1\t2", "warehouse/t/delta_0000001_0000001_0000/bucket_00001_0"},
      {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t2\t4", "warehouse/t/delta_0000001_0000001_0000/bucket_00000_0"},
      {"{\"writeid\":1,\"bucketid\":536936448,\"rowid\":1}\t5\t6", "warehouse/t/delta_0000001_0000001_0000/bucket_00001_0"},
      {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t6\t8", "warehouse/t/delta_0000001_0000001_0000/bucket_00000_0"},
      {"{\"writeid\":1,\"bucketid\":536936448,\"rowid\":2}\t9\t10", "warehouse/t/delta_0000001_0000001_0000/bucket_00001_0"}
    };
    Assert.assertEquals("Unexpected row count", expected2.length, rs.size());
    for(int i = 0; i < expected2.length; i++) {
      Assert.assertTrue("Actual line " + i + " ac: " + rs.get(i), rs.get(i).startsWith(expected2[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " ac: " + rs.get(i), rs.get(i).endsWith(expected2[i][1]));
    }
  }

  @Test
  public void testGetSplitsLocks() throws Exception {
    // Need to test this with LLAP settings, which requires some additional configurations set.
    HiveConf modConf = new HiveConf(hiveConf);
    setupTez(modConf);
    modConf.setVar(ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    modConf.setVar(ConfVars.HIVE_FETCH_TASK_CONVERSION, "more");
    modConf.setVar(HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS, "localhost");

    // SessionState/Driver needs to be restarted with the Tez conf settings.
    restartSessionAndDriver(modConf);
    TxnStore txnHandler = TxnUtils.getTxnStore(modConf);

    try {
      // Request LLAP splits for a table.
      String queryParam = "select * from " + Table.ACIDTBL;
      runStatementOnDriver("select get_splits(\"" + queryParam + "\", 1)");

      // The get_splits call should have resulted in a lock on ACIDTBL
      ShowLocksResponse slr = txnHandler.showLocks(new ShowLocksRequest());
      TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED,
          "default", Table.ACIDTBL.name, null, slr.getLocks());
      assertEquals(1, slr.getLocksSize());

      // Try another table.
      queryParam = "select * from " + Table.ACIDTBLPART;
      runStatementOnDriver("select get_splits(\"" + queryParam + "\", 1)");

      // Should now have new lock on ACIDTBLPART
      slr = txnHandler.showLocks(new ShowLocksRequest());
      TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED,
          "default", Table.ACIDTBLPART.name, null, slr.getLocks());
      assertEquals(2, slr.getLocksSize());

      // There should be different txn IDs associated with each lock.
      Set<Long> txnSet = new HashSet<Long>();
      for (ShowLocksResponseElement lockResponseElem : slr.getLocks()) {
        txnSet.add(lockResponseElem.getTxnid());
      }
      assertEquals(2, txnSet.size());

      List<String> rows = runStatementOnDriver("show transactions");
      // Header row + 2 transactions = 3 rows
      assertEquals(3, rows.size());
    } finally {
      // Close the session which should free up the TxnHandler/locks held by the session.
      // Done in the finally block to make sure we free up the locks; otherwise
      // the cleanup in tearDown() will get stuck waiting on the lock held here on ACIDTBL.
      restartSessionAndDriver(hiveConf);
    }

    // Lock should be freed up now.
    ShowLocksResponse slr = txnHandler.showLocks(new ShowLocksRequest());
    assertEquals(0, slr.getLocksSize());

    List<String> rows = runStatementOnDriver("show transactions");
    // Transactions should be committed.
    // No transactions - just the header row
    assertEquals(1, rows.size());
  }

  @Test
  public void testGetSplitsLocksWithMaterializedView() throws Exception {
    // Need to test this with LLAP settings, which requires some additional configurations set.
    HiveConf modConf = new HiveConf(hiveConf);
    setupTez(modConf);
    modConf.setVar(ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    modConf.setVar(ConfVars.HIVE_FETCH_TASK_CONVERSION, "more");
    modConf.setVar(HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS, "localhost");

    // SessionState/Driver needs to be restarted with the Tez conf settings.
    restartSessionAndDriver(modConf);
    TxnStore txnHandler = TxnUtils.getTxnStore(modConf);
    String mvName = "mv_acidTbl";
    try {
      runStatementOnDriver("create materialized view " + mvName + " as select a from " + Table.ACIDTBL + " where a > 5");

      // Request LLAP splits for a table.
      String queryParam = "select a from " + Table.ACIDTBL + " where a > 5";
      runStatementOnDriver("select get_splits(\"" + queryParam + "\", 1)");

      // The get_splits call should have resulted in a lock on ACIDTBL and materialized view mv_acidTbl
      ShowLocksResponse slr = txnHandler.showLocks(new ShowLocksRequest());
      TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED,
              "default", Table.ACIDTBL.name, null, slr.getLocks());
      TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED,
              "default", mvName, null, slr.getLocks());
      assertEquals(2, slr.getLocksSize());
    } finally {
      // Close the session which should free up the TxnHandler/locks held by the session.
      // Done in the finally block to make sure we free up the locks; otherwise
      // the cleanup in tearDown() will get stuck waiting on the lock held here on ACIDTBL.
      restartSessionAndDriver(hiveConf);
      runStatementOnDriver("drop materialized view if exists " + mvName);
    }

    // Lock should be freed up now.
    ShowLocksResponse slr = txnHandler.showLocks(new ShowLocksRequest());
    assertEquals(0, slr.getLocksSize());

    List<String> rows = runStatementOnDriver("show transactions");
    // Transactions should be committed.
    // No transactions - just the header row
    assertEquals(1, rows.size());
  }
 
  /**
   * HIVE-20699
   *
   * see TestTxnCommands3.testCompactor
   */
  @Test
  public void testCrudMajorCompactionSplitGrouper() throws Exception {
    String tblName = "test_split_grouper";
    // make a clone of existing hive conf
    HiveConf confForTez = new HiveConf(hiveConf);
    setupTez(confForTez); // one-time setup to make query able to run with Tez
    HiveConf.setVar(confForTez, HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
    runStatementOnDriver("create transactional table " + tblName + " (a int, b int) clustered by (a) into 2 buckets "
        + "stored as ORC TBLPROPERTIES('bucketing_version'='2', 'transactional'='true',"
        + " 'transactional_properties'='default')", confForTez);
    runStatementOnDriver("insert into " + tblName + " values(1,2),(1,3),(1,4),(2,2),(2,3),(2,4)", confForTez);
    runStatementOnDriver("insert into " + tblName + " values(3,2),(3,3),(3,4),(4,2),(4,3),(4,4)", confForTez);
    runStatementOnDriver("delete from " + tblName + " where b = 2");
    List<String> expectedRs = new ArrayList<>();
    expectedRs.add("{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t2\t3");
    expectedRs.add("{\"writeid\":1,\"bucketid\":536870912,\"rowid\":2}\t2\t4");
    expectedRs.add("{\"writeid\":2,\"bucketid\":536870912,\"rowid\":1}\t3\t3");
    expectedRs.add("{\"writeid\":2,\"bucketid\":536870912,\"rowid\":2}\t3\t4");
    expectedRs.add("{\"writeid\":1,\"bucketid\":536936448,\"rowid\":1}\t1\t3");
    expectedRs.add("{\"writeid\":1,\"bucketid\":536936448,\"rowid\":2}\t1\t4");
    expectedRs.add("{\"writeid\":2,\"bucketid\":536936448,\"rowid\":1}\t4\t3");
    expectedRs.add("{\"writeid\":2,\"bucketid\":536936448,\"rowid\":2}\t4\t4");
    List<String> rs =
        runStatementOnDriver("select ROW__ID, * from " + tblName + " order by ROW__ID.bucketid, ROW__ID", confForTez);
    HiveConf.setVar(confForTez, HiveConf.ConfVars.SPLIT_GROUPING_MODE, "compactor");
    // No order by needed: this should use the compactor split grouping to return the rows in correct order
    List<String> rsCompact = runStatementOnDriver("select ROW__ID, * from  " + tblName, confForTez);
    Assert.assertEquals("normal read", expectedRs, rs);
    Assert.assertEquals("compacted read", rs, rsCompact);
  }

  /**
   * Tests the OrcInputFormat.isOrignal method for files in ACID and Non-ACID tables.
   * @throws IOException If there is a file reading error
   */
  @Test
  public void testIsOriginal() throws IOException {
    assertIsOriginal(new Path(TEST_WAREHOUSE_DIR, Table.ACIDTBL.toString().toLowerCase()), false);
    assertIsOriginal(new Path(TEST_WAREHOUSE_DIR, Table.NONACIDORCTBL.toString().toLowerCase()), true);
  }

  /**
   * Checks if the file format is original or ACID file based on OrcInputFormat static methods.
   * @param path The file to check
   * @param expected The expected result of the isOriginal
   * @throws IOException Error when reading the file
   */
  private void assertIsOriginal(Path path, boolean expected) throws FileNotFoundException, IOException {
    FileSystem fs = FileSystem.get(hiveConf);
    RemoteIterator<LocatedFileStatus> lfs = fs.listFiles(path, true);
    boolean foundAnyFile = false;
    while (lfs.hasNext()) {
      LocatedFileStatus lf = lfs.next();
      Path file = lf.getPath();
      if (!file.getName().startsWith(".") && !file.getName().startsWith("_")) {
        try (Reader reader = OrcFile.createReader(file, OrcFile.readerOptions(new Configuration()))) {
          OrcProto.Footer footer = reader.getFileTail().getFooter();
          assertEquals("Reader based original check", expected, OrcInputFormat.isOriginal(reader));
          assertEquals("Footer based original check", expected, OrcInputFormat.isOriginal(footer));
        }
        foundAnyFile = true;
      }
    }
    assertTrue("Checking if any file found to check", foundAnyFile);
  }

  private void restartSessionAndDriver(HiveConf conf) throws Exception {
    SessionState ss = SessionState.get();
    if (ss != null) {
      ss.close();
    }
    if (d != null) {
      d.close();
      d.destroy();
    }

    SessionState.start(conf);
    d = DriverFactory.newDriver(conf);
  }

  // Ideally test like this should be a qfile test. However, the explain output from qfile is always
  // slightly different depending on where the test is run, specifically due to file size estimation
  private void testJoin(String engine, String joinType) throws Exception {
    HiveConf confForTez = new HiveConf(hiveConf); // make a clone of existing hive conf
    HiveConf confForMR = new HiveConf(hiveConf);  // make a clone of existing hive conf

    if (engine.equals("tez")) {
      setupTez(confForTez); // one-time setup to make query able to run with Tez
    }

    if (joinType.equals("MapJoin")) {
      setupMapJoin(confForTez);
      setupMapJoin(confForMR);
    }

    runQueries(engine, joinType, confForTez, confForMR);

    // Perform compaction. Join result after compaction should still be the same
    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history", 1, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0 compaction state", TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(0).getState());
    runCleaner(hiveConf);

    runQueries(engine, joinType, confForTez, confForMR);
  }

  private void runQueries(String engine, String joinType, HiveConf confForTez, HiveConf confForMR) throws Exception {
    List<String> queries = new ArrayList<String>();
    queries.add("select count(*) from " + Table.ACIDTBL + " t1 join " + Table.ACIDTBL + " t2 on t1.a=t2.a");
    queries.add("select count(*) from " + Table.ACIDTBL + " t1 join " + Table.NONACIDORCTBL + " t2 on t1.a=t2.a");
    // more queries can be added here in the future to test acid joins

    List<String> explain; // stores Explain output
    int[][] expected = {{5}};
    List<String> rs = null;

    for (String query : queries) {
      if (engine.equals("tez")) {
        explain = runStatementOnDriver("explain " + query, confForTez);
        if (joinType.equals("MergeJoin")) {
          TestTxnCommands2.assertExplainHasString("Merge Join Operator", explain, "Didn't find " + joinType);
        } else { // MapJoin
          TestTxnCommands2.assertExplainHasString("Map Join Operator", explain, "Didn't find " + joinType);
        }
        rs = runStatementOnDriver(query, confForTez);
      } else { // mr
        explain = runStatementOnDriver("explain " + query, confForMR);
        if (joinType.equals("MergeJoin")) {
          TestTxnCommands2.assertExplainHasString("  Join Operator", explain, "Didn't find " + joinType);
        } else { // MapJoin
          TestTxnCommands2.assertExplainHasString("Map Join Operator", explain, "Didn't find " + joinType);
        }
        rs = runStatementOnDriver(query, confForMR);
      }
      Assert.assertEquals("Join result incorrect", stringifyValues(expected), rs);
    }
  }

  public static void setupTez(HiveConf conf) {
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    conf.setVar(HiveConf.ConfVars.HIVE_USER_INSTALL_DIR, TEST_DATA_DIR);
    conf.set("tez.am.resource.memory.mb", "128");
    conf.set("tez.am.dag.scheduler.class", "org.apache.tez.dag.app.dag.impl.DAGSchedulerNaturalOrderControlled");
    conf.setBoolean("tez.local.mode", true);
    conf.setBoolean("tez.local.mode.without.network", true);
    conf.set("fs.defaultFS", "file:///");
    conf.setBoolean("tez.runtime.optimize.local.fetch", true);
    conf.set("tez.staging-dir", TEST_DATA_DIR);
    conf.setBoolean("tez.ignore.lib.uris", true);
    conf.set("hive.tez.container.size", "128");
    conf.setBoolean("hive.merge.tezfiles", false); 
    conf.setBoolean("hive.in.tez.test", true);
  }

  private void setupMapJoin(HiveConf conf) {
    conf.setBoolVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN_NOCONDITIONALTASK, true);
    conf.setLongVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN_NOCONDITIONAL_TASK_THRESHOLD, 100000);
  }

  private List<String> runStatementOnDriver(String stmt) throws Exception {
    d.run(stmt);
    List<String> rs = new ArrayList<String>();
    d.getResults(rs);
    return rs;
  }

  /**
   * Run statement with customized hive conf
   */
  public static List<String> runStatementOnDriver(String stmt, HiveConf conf)
      throws Exception {
    IDriver driver = DriverFactory.newDriver(conf);
    driver.setMaxRows(10000);
    driver.run(stmt);
    List<String> rs = new ArrayList<String>();
    driver.getResults(rs);
    return rs;
  }
}
