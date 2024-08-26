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
package org.apache.hadoop.hive.ql.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.TxnCommandsBaseForTests;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class TestUpgradeTool extends TxnCommandsBaseForTests {
  private static final Logger LOG = LoggerFactory.getLogger(TestUpgradeTool.class);
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
      File.separator + TestUpgradeTool.class.getCanonicalName() + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
  @Override
  protected String getTestDataDir() {
    return TEST_DATA_DIR;
  }

  /**
   * includes 'execute' for postUpgrade
   */
  @Test
  public void testPostUpgrade() throws Exception {
    int[][] data = {{1, 2}, {3, 4}, {5, 6}};
    int[][] dataPart = {{1, 2, 10}, {3, 4, 11}, {5, 6, 12}};
    hiveConf.setVar(HiveConf.ConfVars.DYNAMIC_PARTITIONING_MODE, "dynamic");
    runStatementOnDriver("drop table if exists TAcid");
    runStatementOnDriver("drop table if exists TAcidPart");
    runStatementOnDriver("drop table if exists TFlat");
    runStatementOnDriver("drop table if exists TFlatText");

    //should be converted to Acid
    runStatementOnDriver("create table TAcid (a int, b int) clustered by (b) into 2 buckets" +
        " stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("insert into TAcid" + makeValuesClause(data));
    runStatementOnDriver("insert into TAcid" + makeValuesClause(data));//should now be copy_1
    runStatementOnDriver("insert into TAcid" + makeValuesClause(data));//should now be copy_2

    //should be converted to Acid
    runStatementOnDriver("create table TAcidPart (a int, b int) partitioned by (p int)" +
        " clustered by (b) into 2 buckets  stored as orc TBLPROPERTIES ('transactional'='false')");
    //to create some partitions
    runStatementOnDriver("insert into TAcidPart partition(p)" + makeValuesClause(dataPart));
    //and copy_1 files
    runStatementOnDriver("insert into TAcidPart partition(p)" + makeValuesClause(dataPart));

    //should be converted to Acid
    //todo add some files with non-standard names
    runStatementOnDriver("create table TFlat (a int, b int) stored as orc " +
        "tblproperties('transactional'='false')");
    runStatementOnDriver("insert into TFlat values(1,2)");//create 0000_0
    runStatementOnDriver("insert into TFlat values(2,3)");//create 0000_0_copy_1
    runStatementOnDriver("insert into TFlat values(3,4)");//create 0000_0_copy_2
    runStatementOnDriver("insert into TFlat values(4,5)");//create 0000_0_copy_3
    runStatementOnDriver("insert into TFlat values(5,6)");//create 0000_0_copy_4
    runStatementOnDriver("insert into TFlat values(6,7)");//create 0000_0_copy_5


    /*
     ├── 000000_0
     ├── 000000_0_copy_1
     ├── 000000_0_copy_2
     ├── 000000_0_copy_3
     └── 000000_0_copy_4
     └── 000000_0_copy_5

     to

      ├── 000000_0
      ├── 000000_0_copy_2
      ├── 1
      │   └── 000000_0
      ├── 2
      │   └── 000000_0
      └── subdir
      |  └── part-0001
      |--.hive-staging_hive_2018-07-04_11-12-18_760_5286422535984490754-1395/000000_0

*/
    FileSystem fs = FileSystem.get(hiveConf);
    //simulate Spark (non-Hive) write
    fs.rename(new Path(getWarehouseDir() + "/tflat/000000_0_copy_1"),
        new Path(getWarehouseDir()  + "/tflat/subdir/part-0001"));
    //simulate Insert ... Select ... Union All...
    fs.rename(new Path(getWarehouseDir() + "/tflat/000000_0_copy_3"),
        new Path(getWarehouseDir()  + "/tflat/1/000000_0"));
    fs.rename(new Path(getWarehouseDir() + "/tflat/000000_0_copy_4"),
        new Path(getWarehouseDir()  + "/tflat/2/000000_0"));
    fs.rename(new Path(getWarehouseDir() + "/tflat/000000_0_copy_5"),
        new Path(getWarehouseDir()  + "/tflat/.hive-staging_hive_2018-07-04_11-12-18_760_5286422535984490754-1395/000000_0"));

    String testQuery0 = "select a, b from TFlat order by a";
    String[][] expected0 = new String[][] {
        {"1\t2",""},
        {"2\t3",""},
        {"3\t4",""},
        {"4\t5",""},
        {"5\t6",""},
    };
    checkResult(expected0, testQuery0, true, "TFlat pre-check", LOG);


    //should be converted to MM
    runStatementOnDriver("create table TFlatText (a int, b int) stored as textfile " +
        "tblproperties('transactional'='false')");

    Hive db = Hive.get(hiveConf);
    org.apache.hadoop.hive.ql.metadata.Table tacid = db.getTable("default", "tacid");
    Assert.assertEquals("Expected TAcid to not be full acid", false,
        AcidUtils.isFullAcidTable(tacid));
    org.apache.hadoop.hive.ql.metadata.Table tacidpart = db.getTable("default", "tacidpart");
    Assert.assertEquals("Expected TAcidPart to not be full acid", false,
        AcidUtils.isFullAcidTable(tacidpart));

    org.apache.hadoop.hive.ql.metadata.Table t = db.getTable("default", "tflat");
    Assert.assertEquals("Expected TAcid to not be full acid", false,
        AcidUtils.isFullAcidTable(t));
    t = db.getTable("default", "tflattext");
    Assert.assertEquals("Expected TAcidPart to not be full acid", false,
        AcidUtils.isInsertOnlyTable(tacidpart));


    String[] args2 = {"-location", getTestDataDir(), "-execute"};
    UpgradeTool.hiveConf = hiveConf;
    UpgradeTool.main(args2);

    tacid = db.getTable("default", "tacid");
    Assert.assertEquals("Expected TAcid to become full acid", true,
        AcidUtils.isFullAcidTable(tacid));
    tacidpart = db.getTable("default", "tacidpart");
    Assert.assertEquals("Expected TAcidPart to become full acid", true,
        AcidUtils.isFullAcidTable(tacidpart));

    t = db.getTable("default", "tflat");
    Assert.assertEquals("Expected TAcid to become acid", true, AcidUtils.isFullAcidTable(t));
    t = db.getTable("default", "tflattext");
    Assert.assertEquals("Expected TAcidPart to become MM", true,
        AcidUtils.isInsertOnlyTable(t));

    /*make sure we still get the same data and row_ids are assigned and deltas are as expected:
     * each set of copy_N goes into matching delta_N_N.*/
    String testQuery = "select ROW__ID, a, b, INPUT__FILE__NAME from TAcid order by a, b, ROW__ID";
    String[][] expected = new String[][] {
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":0}\t1\t2",
            "tacid/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2",
            "tacid/delta_0000001_0000001/000000_0"},
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}\t1\t2",
            "tacid/delta_0000002_0000002/000000_0"},
        {"{\"writeid\":0,\"bucketid\":536936448,\"rowid\":0}\t3\t4",
            "tacid/000001_0"},
        {"{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t3\t4",
            "tacid/delta_0000001_0000001/000001_0"},
        {"{\"writeid\":2,\"bucketid\":536936448,\"rowid\":0}\t3\t4",
            "tacid/delta_0000002_0000002/000001_0"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":1}\t5\t6",
            "tacid/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t5\t6",
            "tacid/delta_0000001_0000001/000000_0"},
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":1}\t5\t6",
            "tacid/delta_0000002_0000002/000000_0"}
    };
    checkResult(expected, testQuery, false, "TAcid post-check", LOG);


    testQuery = "select ROW__ID, a, b, INPUT__FILE__NAME from TAcidPart order by a, b, p, ROW__ID";
    String[][] expected2 = new String[][] {
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":0}\t1\t2",
            "warehouse/tacidpart/p=10/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2",
            "tacidpart/p=10/delta_0000001_0000001/000000_0"},
        {"{\"writeid\":0,\"bucketid\":536936448,\"rowid\":0}\t3\t4",
            "tacidpart/p=11/000001_0"},
        {"{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t3\t4",
            "tacidpart/p=11/delta_0000001_0000001/000001_0"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":0}\t5\t6",
            "tacidpart/p=12/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t5\t6",
            "tacidpart/p=12/delta_0000001_0000001/000000_0"}
    };
    checkResult(expected2, testQuery, false, "TAcidPart post-check", LOG);

    /* Verify that we re-arranged/renamed so that files names follow hive naming convention
    and are spread among deltas/buckets
    The order of files in RemoteIterator<LocatedFileStatus> iter = fs.listFiles(p, true)
    is what determines which delta/file any original file ends up in

    The test is split into 2 parts to test data and metadata because RemoteIterator walks in
    different order on different machines*/

    testQuery = "select a, b from TFlat order by a";
    String[][] expectedData = new String[][] {
        {"1\t2"},
        {"2\t3"},
        {"3\t4"},
        {"4\t5"},
        {"5\t6"}
    };
    checkResult(expectedData, testQuery, true, "TFlat post-check data", LOG);

    testQuery = "select ROW__ID, INPUT__FILE__NAME from TFlat order by INPUT__FILE__NAME";
    String[][] expectedMetaData = new String[][] {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}",
            "tflat/delta_0000001_0000001/00000_0"},
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}",
            "tflat/delta_0000002_0000002/00000_0"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":0}",
            "tflat/delta_0000003_0000003/00000_0"},
        {"{\"writeid\":4,\"bucketid\":536870912,\"rowid\":0}",
            "tflat/delta_0000004_0000004/00000_0"},
        {"{\"writeid\":5,\"bucketid\":536870912,\"rowid\":0}",
            "tflat/delta_0000005_0000005/00000_0"}
    };
    checkResult(expectedMetaData, testQuery, false, "TFlat post-check files", LOG);
  }
  @Test
  public void testGuessNumBuckets() {
    Assert.assertEquals(1, UpgradeTool.guessNumBuckets(123));
    Assert.assertEquals(1, UpgradeTool.guessNumBuckets(30393930));
    Assert.assertEquals(1, UpgradeTool.guessNumBuckets((long) Math.pow(10, 9)));
    Assert.assertEquals(32, UpgradeTool.guessNumBuckets((long) Math.pow(10, 13)));//10 TB
    Assert.assertEquals(128, UpgradeTool.guessNumBuckets((long) Math.pow(10, 15)));//PB
  }
}
