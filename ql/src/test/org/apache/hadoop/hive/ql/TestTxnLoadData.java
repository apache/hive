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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Tests related to support of LOAD DATA with Acid tables
 * Most tests run in vectorized and non-vectorized mode since we currently have a vectorized and
 * a non-vectorized acid readers and it's critical that ROW_IDs are generated the same way.
 */
public class TestTxnLoadData extends TxnCommandsBaseForTests {
  static final private Logger LOG = LoggerFactory.getLogger(TestTxnLoadData.class);
  private static final String TEST_DATA_DIR =
    new File(System.getProperty("java.io.tmpdir") +
    File.separator + TestTxnLoadData.class.getCanonicalName()
    + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
  @Rule
  public TemporaryFolder folder= new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    setUpInternal();
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
  }
  @Override
  protected String getTestDataDir() {
    return TEST_DATA_DIR;
  }

  @Test
  public void loadData() throws Exception {
    loadData(false);
  }
  @Test
  public void loadDataVectorized() throws Exception {
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    loadData(true);
  }
  @Test
  public void loadDataUpdate() throws Exception {
    loadDataUpdate(false);
  }
  @Test
  public void loadDataUpdateVectorized() throws Exception {
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    loadDataUpdate(true);
  }
  @Test
  public void loadDataNonAcid2AcidConversion() throws Exception {
    loadDataNonAcid2AcidConversion(false);
  }
  @Test
  public void loadDataNonAcid2AcidConversionVectorized() throws Exception {
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    loadDataNonAcid2AcidConversion(true);
  }
  @Test
  public void testMultiStatement() throws Exception {
    testMultiStatement(false);
  }
  @Test
  public void testMultiStatementVectorized() throws Exception {
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    testMultiStatement(true);
  }
  private void loadDataUpdate(boolean isVectorized) throws Exception {
    dropTables("T", "Tstage");
    runStatementOnDriver(
      "create table T (a int, b int) stored as orc tblproperties('transactional'='true')");
    //Tstage is just a simple way to generate test data
    runStatementOnDriver("create table Tstage (a int, b int) stored as orc tblproperties('transactional'='false')");
    runStatementOnDriver("insert into Tstage values(1,2),(3,4)");
    //this creates an ORC data file with correct schema under table root
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/1'");
    //"load data local inpath" doesn't delete source files so clean it here
    runStatementOnDriver("truncate table Tstage");
    //and do a Load Data into the same table, which should now land in a delta_x_x.
    // 'data' is created by export command/
    runStatementOnDriver("load data local inpath '" + getWarehouseDir() + "/1/data' into table T");

    String testQuery = isVectorized ? "select ROW__ID, a, b from T order by ROW__ID" :
      "select ROW__ID, a, b, INPUT__FILE__NAME from T order by ROW__ID";
    String[][] expected = new String[][]{
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2", "t/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t3\t4", "t/delta_0000001_0000001_0000/000000_0"}};
    checkResult(expected, testQuery, isVectorized, "load data inpath");
    runStatementOnDriver("update T set b = 17 where a = 1");
    String[][] expected2 = new String[][]{
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t3\t4", "t/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":2,\"bucketid\":536870913,\"rowid\":0}\t1\t17", "t/delta_0000002_0000002_0001/bucket_00000_0"}
    };
    checkResult(expected2, testQuery, isVectorized, "update");

    runStatementOnDriver("insert into T values(2,2)");
    String[][] expectedInter2 = new String[][] {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t3\t4", "t/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":2,\"bucketid\":536870913,\"rowid\":0}\t1\t17", "t/delta_0000002_0000002_0001/bucket_00000_0"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":0}\t2\t2", "t/delta_0000003_0000003_0000/bucket_00000_0"}
    };
    checkResult(expectedInter2, testQuery, isVectorized, "insert");
    runStatementOnDriver("delete from T where a = 3");
    String[][] expectedInter3 = new String[][] {
        {"{\"writeid\":2,\"bucketid\":536870913,\"rowid\":0}\t1\t17", "t/delta_0000002_0000002_0001/bucket_00000_0"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":0}\t2\t2", "t/delta_0000003_0000003_0000/bucket_00000_0"}
    };
    checkResult(expectedInter3, testQuery, isVectorized, "delete");
    //test minor compaction
    runStatementOnDriver("alter table T compact 'minor'");
    TestTxnCommands2.runWorker(hiveConf);
    String[][] expected3 = new String[][] {
        {"{\"writeid\":2,\"bucketid\":536870913,\"rowid\":0}\t1\t17", "t/delta_0000001_0000004_v0000019/bucket_00000"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":0}\t2\t2", "t/delta_0000001_0000004_v0000019/bucket_00000"}
    };
    checkResult(expected3, testQuery, isVectorized, "delete compact minor");

    runStatementOnDriver("load data local inpath '" + getWarehouseDir() + "/1/data' overwrite into table T");
    String[][] expected4 = new String[][]{
        {"{\"writeid\":5,\"bucketid\":536870912,\"rowid\":0}\t1\t2", "t/base_0000005/000000_0"},
        {"{\"writeid\":5,\"bucketid\":536870912,\"rowid\":1}\t3\t4", "t/base_0000005/000000_0"}};
    checkResult(expected4, testQuery, isVectorized, "load data inpath overwrite");

    //load same data again (additive)
    runStatementOnDriver("load data local inpath '" + getWarehouseDir() + "/1/data' into table T");
    String[][] expectedInt1 = new String[][] {
        {"{\"writeid\":5,\"bucketid\":536870912,\"rowid\":0}\t1\t2", "t/base_0000005/000000_0"},
        {"{\"writeid\":5,\"bucketid\":536870912,\"rowid\":1}\t3\t4", "t/base_0000005/000000_0"},
        {"{\"writeid\":6,\"bucketid\":536870912,\"rowid\":0}\t1\t2", "t/delta_0000006_0000006_0000/000000_0"},
        {"{\"writeid\":6,\"bucketid\":536870912,\"rowid\":1}\t3\t4", "t/delta_0000006_0000006_0000/000000_0"}
    };
    checkResult(expectedInt1, testQuery, isVectorized, "load data local inpath");
    runStatementOnDriver("update T set b = 17 where a = 1");//matches 2 rows
    runStatementOnDriver("delete from T where a = 3");//matches 2 rows
    runStatementOnDriver("insert into T values(2,2)");
    String[][] expected5 = new String[][]{
        {"{\"writeid\":7,\"bucketid\":536870913,\"rowid\":0}\t1\t17", "t/delta_0000007_0000007_0001/bucket_00000_0"},
        {"{\"writeid\":7,\"bucketid\":536936449,\"rowid\":0}\t1\t17", "t/delta_0000007_0000007_0001/bucket_00001_0"},
        {"{\"writeid\":9,\"bucketid\":536870912,\"rowid\":0}\t2\t2", "t/delta_0000009_0000009_0000/bucket_00000_0"}
    };
    checkResult(expected5, testQuery, isVectorized, "load data inpath overwrite update");

    //test major compaction
    runStatementOnDriver("alter table T compact 'major'");
    TestTxnCommands2.runWorker(hiveConf);
    String[][] expected6 = new String[][]{
        {"{\"writeid\":7,\"bucketid\":536870913,\"rowid\":0}\t1\t17", "t/base_0000009_v0000035/bucket_00000"},
        {"{\"writeid\":7,\"bucketid\":536936449,\"rowid\":0}\t1\t17", "t/base_0000009_v0000035/bucket_00001"},
        {"{\"writeid\":9,\"bucketid\":536870912,\"rowid\":0}\t2\t2", "t/base_0000009_v0000035/bucket_00000"}
    };
    checkResult(expected6, testQuery, isVectorized, "load data inpath compact major");
  }
  private void loadData(boolean isVectorized) throws Exception {
    dropTables("T", "Tstage");
    runStatementOnDriver("create table T (a int, b int) stored as orc tblproperties('transactional'='true')");
    runStatementOnDriver("insert into T values(0,2),(0,4)");
    //Tstage is just a simple way to generate test data
    runStatementOnDriver("create table Tstage (a int, b int) stored as orc tblproperties('transactional'='false')");
    runStatementOnDriver("insert into Tstage values(1,2),(3,4)");
    //this creates an ORC data file with correct schema under table root
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() +"/1'");
    //"load data local inpath" doesn't delete source files so clean it here
    runStatementOnDriver("truncate table Tstage");
    //and do a Load Data into the same table, which should now land in a delta_x_x.
    // 'data' is created by export command/
    runStatementOnDriver("load data local inpath '" + getWarehouseDir() + "/1/data' into table T");

    String testQuery = isVectorized ? "select ROW__ID, a, b from T order by ROW__ID" :
      "select ROW__ID, a, b, INPUT__FILE__NAME from T order by ROW__ID";
    String[][] expected = new String[][] {
        //normal insert
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t0\t2", "t/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t0\t4", "t/delta_0000001_0000001_0000/bucket_00000_0"},
        //Load Data
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}\t1\t2", "t/delta_0000002_0000002_0000/000000_0"},
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":1}\t3\t4", "t/delta_0000002_0000002_0000/000000_0"}};
    checkResult(expected, testQuery, isVectorized, "load data inpath");

    //test minor compaction
    runStatementOnDriver("alter table T compact 'minor'");
    TestTxnCommands2.runWorker(hiveConf);
    String[][] expected1 = new String[][] {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t0\t2", "t/delta_0000001_0000002_v0000011/bucket_00000"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t0\t4", "t/delta_0000001_0000002_v0000011/bucket_00000"},
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}\t1\t2", "t/delta_0000001_0000002_v0000011/bucket_00000"},
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":1}\t3\t4", "t/delta_0000001_0000002_v0000011/bucket_00000"}
    };
    checkResult(expected1, testQuery, isVectorized, "load data inpath (minor)");

    //test major compaction
    runStatementOnDriver("insert into T values(2,2)");
    runStatementOnDriver("alter table T compact 'major'");
    TestTxnCommands2.runWorker(hiveConf);
    String[][] expected2 = new String[][] {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t0\t2", "t/base_0000003_v0000017/bucket_00000"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t0\t4", "t/base_0000003_v0000017/bucket_00000"},
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}\t1\t2", "t/base_0000003_v0000017/bucket_00000"},
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":1}\t3\t4", "t/base_0000003_v0000017/bucket_00000"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":0}\t2\t2", "t/base_0000003_v0000017/bucket_00000"}
    };
    checkResult(expected2, testQuery, isVectorized, "load data inpath (major)");

    //create more staging data and test Load Data Overwrite
    runStatementOnDriver("insert into Tstage values(5,6),(7,8)");
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() +"/2'");
    runStatementOnDriver("load data inpath '" + getWarehouseDir() + "/2/data' overwrite into table T");
    String[][] expected3 = new String[][] {
        {"{\"writeid\":4,\"bucketid\":536870912,\"rowid\":0}\t5\t6", "t/base_0000004/000000_0"},
        {"{\"writeid\":4,\"bucketid\":536870912,\"rowid\":1}\t7\t8", "t/base_0000004/000000_0"}};
    checkResult(expected3, testQuery, isVectorized, "load data inpath overwrite");

    //one more major compaction
    runStatementOnDriver("insert into T values(6,6)");
    runStatementOnDriver("alter table T compact 'major'");
    TestTxnCommands2.runWorker(hiveConf);
    String[][] expected4 = new String[][] {
        {"{\"writeid\":4,\"bucketid\":536870912,\"rowid\":0}\t5\t6", "t/base_0000005_v0000026/bucket_00000"},
        {"{\"writeid\":4,\"bucketid\":536870912,\"rowid\":1}\t7\t8", "t/base_0000005_v0000026/bucket_00000"},
        {"{\"writeid\":5,\"bucketid\":536870912,\"rowid\":0}\t6\t6", "t/base_0000005_v0000026/bucket_00000"}};
    checkResult(expected4, testQuery, isVectorized, "load data inpath overwrite (major)");
  }
  /**
   * Load Data [overwrite] in to an (un-)partitioned acid converted table
   */
  private void loadDataNonAcid2AcidConversion(boolean isVectorized) throws Exception {
    dropTables("T", "Tstage");
    runStatementOnDriver("create table T (a int, b int) stored as orc tblproperties('transactional'='false')");
    //per acid write to test nonAcid2acid conversion mixed with load data
    runStatementOnDriver("insert into T values(0,2),(0,4)");
    runStatementOnDriver("create table Tstage (a int, b int) stored as orc tblproperties('transactional'='false')");
    runStatementOnDriver("insert into Tstage values(1,2),(3,4)");
    //make 2 more inserts so that we have 000000_0_copy_1, 000000_0_copy_2 files in export
    //export works at file level so if you have copy_N in the table dir, you'll have those in output
    runStatementOnDriver("insert into Tstage values(2,2),(3,3)");
    runStatementOnDriver("insert into Tstage values(4,4),(5,5)");
    //create a file we'll import later
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() +"/1'");
    runStatementOnDriver("truncate table Tstage");//clean the staging table

    //now convert T to acid
    runStatementOnDriver("alter table T SET TBLPROPERTIES ('transactional' = 'true')");
    //and do a Load Data into the same table, which should now land in a delta/
    // (with 000000_0, 000000_0_copy_1, 000000_0_copy_2)
    runStatementOnDriver("load data local inpath '" + getWarehouseDir() + "/1/data' into table T");

    String testQuery = isVectorized ? "select ROW__ID, a, b from T order by ROW__ID" :
      "select ROW__ID, a, b, INPUT__FILE__NAME from T order by ROW__ID";
/*
{"writeid":0,"bucketid":536870912,"rowid":0}     0       2/000000_0
{"writeid":0,"bucketid":536870912,"rowid":1}     0       4/000000_0
{"writeid":1,"bucketid":536870912,"rowid":0}    4       4/delta_0000001_0000001_0000/000000_0
{"writeid":1,"bucketid":536870912,"rowid":1}    5       5/delta_0000001_0000001_0000/000000_0
*/
    String[][] expected = new String[][] {
        //from pre-acid insert
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":0}\t0\t2", "t/000000_0"},
        {"{\"writeid\":0,\"bucketid\":536870912,\"rowid\":1}\t0\t4", "t/000000_0"},
        //from Load Data into acid converted table
        {"{\"writeid\":10000001,\"bucketid\":536870912,\"rowid\":0}\t1\t2",
            "t/delta_10000001_10000001_0000/000000_0"},
        {"{\"writeid\":10000001,\"bucketid\":536870912,\"rowid\":1}\t3\t4",
            "t/delta_10000001_10000001_0000/000000_0"},
        {"{\"writeid\":10000001,\"bucketid\":536936448,\"rowid\":0}\t2\t2",
            "t/delta_10000001_10000001_0000/000001_0"},
        {"{\"writeid\":10000001,\"bucketid\":536936448,\"rowid\":1}\t3\t3",
            "t/delta_10000001_10000001_0000/000001_0"},
        {"{\"writeid\":10000001,\"bucketid\":537001984,\"rowid\":0}\t4\t4",
            "t/delta_10000001_10000001_0000/000002_0"},
        {"{\"writeid\":10000001,\"bucketid\":537001984,\"rowid\":1}\t5\t5",
            "t/delta_10000001_10000001_0000/000002_0"},
    };
    checkResult(expected, testQuery, isVectorized, "load data inpath");

    //create more staging data with copy_N files and do LD+Overwrite
    runStatementOnDriver("insert into Tstage values(5,6),(7,8)");
    runStatementOnDriver("insert into Tstage values(8,8)");
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() +"/2'");
    runStatementOnDriver("load data local inpath '" + getWarehouseDir() + "/2/data' overwrite into table T");

    String[][] expected2 = new String[][] {
        {"{\"writeid\":10000002,\"bucketid\":536870912,\"rowid\":0}\t5\t6",
            "t/base_10000002/000000_0"},
        {"{\"writeid\":10000002,\"bucketid\":536870912,\"rowid\":1}\t7\t8", "t/base_10000002/000000_0"},
        {"{\"writeid\":10000002,\"bucketid\":536936448,\"rowid\":0}\t8\t8",

            "t/base_10000002/000001_0"}
    };
    checkResult(expected2, testQuery, isVectorized, "load data inpath overwrite");

    //create 1 more delta_x_x so that compactor has > dir file to compact
    runStatementOnDriver("insert into T values(9,9)");
    runStatementOnDriver("alter table T compact 'major'");
    TestTxnCommands2.runWorker(hiveConf);

    String[][] expected3 = new String[][] {
        {"{\"writeid\":10000002,\"bucketid\":536870912,\"rowid\":0}\t5\t6",
            "t/base_10000003_v0000014/bucket_00000"},
        {"{\"writeid\":10000002,\"bucketid\":536870912,\"rowid\":1}\t7\t8",
            "t/base_10000003_v0000014/bucket_00000"},
        {"{\"writeid\":10000002,\"bucketid\":536936448,\"rowid\":0}\t8\t8",
            "t/base_10000003_v0000014/bucket_00001"},
        {"{\"writeid\":10000003,\"bucketid\":536870912,\"rowid\":0}\t9\t9",
            "t/base_10000003_v0000014/bucket_00000"}
    };
    checkResult(expected3, testQuery, isVectorized, "load data inpath overwrite (major)");
  }
  /**
   * Load Data [overwrite] in to a partitioned transactional table
   */
  @Test
  public void loadDataPartitioned() throws Exception {
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists Tstage");
    runStatementOnDriver("create table T (a int, b int) partitioned by (p int) stored as orc tblproperties('transactional'='true')");
    runStatementOnDriver("create table Tstage (a int, b int) stored as orc tblproperties('transactional'='false')");

    runStatementOnDriver("insert into Tstage values(0,2),(0,4)");
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() +"/1'");
    runStatementOnDriver("truncate table Tstage");//because 'local' inpath doesn't delete source files
    runStatementOnDriver("load data local inpath '" + getWarehouseDir() + "/1/data' into table T partition(p=0)");

    runStatementOnDriver("insert into Tstage values(1,2),(1,4)");
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() +"/2'");
    runStatementOnDriver("truncate table Tstage");
    runStatementOnDriver("load data local inpath '" + getWarehouseDir() + "/2/data' into table T partition(p=1)");

    runStatementOnDriver("insert into Tstage values(2,2),(2,4)");
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() +"/3'");
    runStatementOnDriver("truncate table Tstage");
    runStatementOnDriver("load data local inpath '" + getWarehouseDir() + "/3/data' into table T partition(p=1)");

    List<String> rs = runStatementOnDriver("select ROW__ID, p, a, b, INPUT__FILE__NAME from T order by p, ROW__ID");
    String[][] expected = new String[][] {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t0\t0\t2", "t/p=0/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t0\t0\t4", "t/p=0/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}\t1\t1\t2", "t/p=1/delta_0000002_0000002_0000/000000_0"},
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":1}\t1\t1\t4", "t/p=1/delta_0000002_0000002_0000/000000_0"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":0}\t1\t2\t2", "t/p=1/delta_0000003_0000003_0000/000000_0"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":1}\t1\t2\t4", "t/p=1/delta_0000003_0000003_0000/000000_0"}};
    checkExpected(rs, expected, "load data inpath partitioned");


    runStatementOnDriver("insert into Tstage values(5,2),(5,4)");
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() +"/4'");
    runStatementOnDriver("truncate table Tstage");
    runStatementOnDriver("load data inpath '" + getWarehouseDir() + "/4/data' overwrite into table T partition(p=1)");
    String[][] expected2 = new String[][] {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t0\t0\t2", "t/p=0/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t0\t0\t4", "t/p=0/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":4,\"bucketid\":536870912,\"rowid\":0}\t1\t5\t2", "t/p=1/base_0000004/000000_0"},
        {"{\"writeid\":4,\"bucketid\":536870912,\"rowid\":1}\t1\t5\t4", "t/p=1/base_0000004/000000_0"}};
    rs = runStatementOnDriver("select ROW__ID, p, a, b, INPUT__FILE__NAME from T order by p, ROW__ID");
    checkExpected(rs, expected2, "load data inpath partitioned overwrite");
  }

  /**
   * By default you can't load into bucketed tables.  Things will break badly in acid (data loss, etc)
   * if loaded data is not bucketed properly.  This test is to capture that this is still the default.
   * If the default is changed, Load Data should probably do more validation to ensure data is
   * properly distributed into files and files are named correctly.
   * With the availability of new feature to rewrite such "load data" commands into insert-as-select,
   * the test should let the load data pass.
   */
  @Test
  public void testValidations() throws Exception {
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists Tstage");
    runStatementOnDriver("create table T (a int, b int) clustered by (a) into 2 buckets stored as orc tblproperties('transactional'='true')");
    File createdFile= folder.newFile("myfile.txt");
    FileUtils.writeStringToFile(createdFile, "hello world");
    runStatementOnDriver("create table Tstage (a int, b int) stored as orc tblproperties('transactional'='false')");
    //this creates an ORC data file with correct schema under table root
    runStatementOnDriver("insert into Tstage values(1,2),(3,4)");
    // This will work with the new support of rewriting load into IAS.
    runStatementOnDriver("load data local inpath '" + getWarehouseDir() + "/Tstage' into table T");
  }

  private void checkExpected(List<String> rs, String[][] expected, String msg) {
    super.checkExpected(rs, expected, msg, LOG, true);
  }
  
  @Test
  public void testMMOrcTable() throws Exception {
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T (a int, b int) stored as orc tblproperties('transactional'='true', 'transactional_properties'='insert_only')");
    int[][] values = {{1,2},{3,4}};
    runStatementOnDriver("insert into T " + makeValuesClause(values));
    List<String> rs = runStatementOnDriver("select a, b from T order by b");
    Assert.assertEquals("", stringifyValues(values), rs);
  }

  /**
   * Make sure Load Data assigns ROW_IDs correctly when there is statementId suffix on delta dir
   * For example, delta_x_x_0001.
   */
  private void testMultiStatement(boolean isVectorized) throws Exception {
    dropTables("T", "Tstage");
    runStatementOnDriver("create table T (a int, b int) stored as orc tblproperties('transactional'='true')");
    //Tstage is just a simple way to generate test data
    runStatementOnDriver("create table Tstage (a int, b int) stored as orc tblproperties('transactional'='false')");
    runStatementOnDriver("insert into Tstage values(5,5),(6,6)");
    //this creates an ORC data file with correct schema under table root
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/1'");
    //and do a Load Data into the same table, which should now land in a delta_x_x.
    // 'data' is created by export command/
    runStatementOnDriver("START TRANSACTION");
    //statementId = 0
    runStatementOnDriver("insert into T values(1,2),(3,4)");
    //statementId = 1
    runStatementOnDriver("load data local inpath '" + getWarehouseDir() + "/1/data' into table T");
    runStatementOnDriver("COMMIT");

    String testQuery = isVectorized ? "select ROW__ID, a, b from T order by ROW__ID" :
      "select ROW__ID, a, b, INPUT__FILE__NAME from T order by ROW__ID";
    String[][] expected = new String[][] {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2", "t/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t3\t4", "t/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536870913,\"rowid\":0}\t5\t5", "t/delta_0000001_0000001_0001/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870913,\"rowid\":1}\t6\t6", "t/delta_0000001_0000001_0001/000000_0"}
    };
    checkResult(expected, testQuery, isVectorized, "load data inpath");

    runStatementOnDriver("alter table T compact 'major'");
    TestTxnCommands2.runWorker(hiveConf);
    String[][] expected2 = new String[][] {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2", "t/base_0000001_v0000010/bucket_00000"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t3\t4", "t/base_0000001_v0000010/bucket_00000"},
        {"{\"writeid\":1,\"bucketid\":536870913,\"rowid\":0}\t5\t5", "t/base_0000001_v0000010/bucket_00000"},
        {"{\"writeid\":1,\"bucketid\":536870913,\"rowid\":1}\t6\t6", "t/base_0000001_v0000010/bucket_00000"}
    };
    checkResult(expected2, testQuery, isVectorized, "load data inpath (major)");
    //at lest for now, Load Data w/Overwrite is not allowed in a txn: HIVE-18154
  }
  
  @Test
  public void testAbort() throws Exception {
    boolean isVectorized = false;
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists Tstage");
    runStatementOnDriver("create table T (a int, b int) stored as orc tblproperties('transactional'='true')");
    //Tstage is just a simple way to generate test data
    runStatementOnDriver("create table Tstage (a int, b int) stored as orc tblproperties('transactional'='false')");
    runStatementOnDriver("insert into Tstage values(5,5),(6,6)");
    //this creates an ORC data file with correct schema under table root
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/1'");
    //and do a Load Data into the same table, which should now land in a delta_x_x.
    // 'data' is created by export command/
    runStatementOnDriver("insert into T values(1,2),(3,4)");
    runStatementOnDriver("START TRANSACTION");
    runStatementOnDriver("load data local inpath '" + getWarehouseDir() + "/1/data' into table T");
    runStatementOnDriver("ROLLBACK");

    String testQuery = isVectorized ? "select ROW__ID, a, b from T order by ROW__ID" :
      "select ROW__ID, a, b, INPUT__FILE__NAME from T order by ROW__ID";
    String[][] expected = new String[][] {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2", "t/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t3\t4", "t/delta_0000001_0000001_0000/bucket_00000_0"}
    };
    checkResult(expected, testQuery, isVectorized, "load data inpath");
  }
  void checkResult(String[][] expectedResult, String query, boolean isVectorized,
      String msg) throws Exception{
    checkResult(expectedResult, query, isVectorized, msg, LOG);
  }
  @Test
  public void testLoadAcidFile() throws Exception {
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID, true);
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists T2");
    runStatementOnDriver(
        "create table T (a int, b int) stored as orc");
    //This is just a simple way to generate test data
    runStatementOnDriver("create table T2(a int, b int) stored as orc");
    runStatementOnDriver("insert into T values(1,2)");
    List<String> rs = runStatementOnDriver("select INPUT__FILE__NAME from T");
    Assert.assertEquals(1, rs.size());
    Assert.assertTrue("Unexpcted file name", rs.get(0)
        .endsWith("t/delta_0000001_0000001_0000/bucket_00000_0"));
    //T2 is an acid table so this should fail
    CommandProcessorException e =
        runStatementOnDriverNegative("load data local inpath '" + rs.get(0) + "' into table T2");
    Assert.assertEquals("Unexpected error code",
        ErrorMsg.LOAD_DATA_ACID_FILE.getErrorCode(), e.getErrorCode());
  }
}
