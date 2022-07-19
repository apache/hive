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
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Tests related to support of ADD PARTITION with Acid/MM tables

 * Most tests run in vectorized and non-vectorized mode since we currently have a vectorized and
 * a non-vectorized acid readers and it's critical that ROW_IDs are generated the same way.
 *
 * Side Note:  Alter Table Add Partition does no validations on the data - not file name checks,
 * not Input/OutputFormat, bucketing etc...
 */
public class TestTxnAddPartition extends TxnCommandsBaseForTests {
  static final private Logger LOG = LoggerFactory.getLogger(TestTxnAddPartition.class);
  private static final String TEST_DATA_DIR =
      new File(System.getProperty("java.io.tmpdir") +
          File.separator + TestTxnAddPartition.class.getCanonicalName()
          + "-" + System.currentTimeMillis()
      ).getPath().replaceAll("\\\\", "/");
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  @Rule
  public ExpectedException exception = ExpectedException.none();



  @Override
  protected String getTestDataDir() {
    return TEST_DATA_DIR;
  }


  @Test
  public void addPartition() throws Exception {

    addPartition(false);
  }

  @Test
  public void addPartitionVectorized() throws Exception {
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    addPartition(true);
  }

  /**
   * Tests adding multiple partitions
   * adding partition w/o location
   * adding partition when it already exists
   * adding partition when it already exists with "if not exists"
   */
  private void addPartition(boolean isVectorized) throws Exception {
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists Tstage");
    runStatementOnDriver("create table T (a int, b int) partitioned by (p int) stored as orc" +
        " tblproperties('transactional'='true')");
    runStatementOnDriver("create table Tstage (a int, b int) stored as orc" +
        " tblproperties('transactional'='false')");

    runStatementOnDriver("insert into Tstage values(0,2),(0,4)");
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/1'");
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/2'");

    runStatementOnDriver("ALTER TABLE T ADD" +
        " PARTITION (p=0) location '" + getWarehouseDir() + "/1/data'" +
        " PARTITION (p=1) location '" + getWarehouseDir() + "/2/data'" +
        " PARTITION (p=2)");

    String testQuery = isVectorized ? "select ROW__ID, p, a, b from T order by p, ROW__ID" :
        "select ROW__ID, p, a, b, INPUT__FILE__NAME from T order by p, ROW__ID";
    String[][] expected = new String[][]{
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t0\t0\t2",
            "warehouse/t/p=0/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t0\t0\t4",
            "warehouse/t/p=0/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t0\t2",
            "warehouse/t/p=1/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t1\t0\t4",
            "warehouse/t/p=1/delta_0000001_0000001_0000/000000_0"}};
    checkResult(expected, testQuery, isVectorized, "add 2 parts w/data and 1 empty", LOG);

    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/3'");
    //should be an error since p=3 exists
    CommandProcessorException e =
        runStatementOnDriverNegative("ALTER TABLE T ADD PARTITION (p=0) location '" + getWarehouseDir() + "/3/data'");
    Assert.assertTrue("add existing partition",
        e.getMessage() != null && e.getMessage().contains("Partition already exists"));

    //should be no-op since p=3 exists
    String stmt = "ALTER TABLE T ADD IF NOT EXISTS " +
        "PARTITION (p=0) location '" + getWarehouseDir() + "/3/data' "//p=0 exists and is not empty
        + "PARTITION (p=2) location '" + getWarehouseDir() + "/3/data'"//p=2 exists and is empty
        + "PARTITION (p=3) location '" + getWarehouseDir() + "/3/data'";//p=3 doesn't exist
    runStatementOnDriver(stmt);
    String[][] expected2 = new String[][]{
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t0\t0\t2",
            "warehouse/t/p=0/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t0\t0\t4",
            "warehouse/t/p=0/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t0\t2",
            "warehouse/t/p=1/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t1\t0\t4",
            "warehouse/t/p=1/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":0}\t3\t0\t2",
            "warehouse/t/p=3/delta_0000003_0000003_0000/000000_0"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":1}\t3\t0\t4",
            "warehouse/t/p=3/delta_0000003_0000003_0000/000000_0"}};
    checkResult(expected2, testQuery, isVectorized, "add 2 existing parts and 1 empty", LOG);
  }

  @Test
  public void addPartitionMM() throws Exception {
    addPartitionMM(false);
  }

  @Test
  public void addPartitionMMVectorized() throws Exception {
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    addPartitionMM(true);
  }

  /**
   * Micro managed table test
   * Tests adding multiple partitions
   * adding partition w/o location
   * adding partition when it already exists
   * adding partition when it already exists with "if not exists"
   */
  private void addPartitionMM(boolean isVectorized) throws Exception {
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists Tstage");

    runStatementOnDriver("create table T (a int, b int) partitioned by (p int) stored as orc" +
        " tblproperties('transactional'='true', 'transactional_properties'='insert_only')");
    runStatementOnDriver("create table Tstage (a int, b int) stored as orc" +
        " tblproperties('transactional'='false')");

    runStatementOnDriver("insert into Tstage values(0,2),(0,4)");
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/1'");
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/2'");

    runStatementOnDriver("ALTER TABLE T ADD" +
        " PARTITION (p=0) location '" + getWarehouseDir() + "/1/data'" +
        " PARTITION (p=1) location '" + getWarehouseDir() + "/2/data'" +
        " PARTITION (p=2)");

    String testQuery = isVectorized ? "select p, a, b from T order by p, a, b" :
        "select p, a, b, INPUT__FILE__NAME from T order by p, a, b";
    String[][] expected = new String[][]{
        {"0\t0\t2", "warehouse/t/p=0/delta_0000001_0000001_0000/000000_0"},
        {"0\t0\t4", "warehouse/t/p=0/delta_0000001_0000001_0000/000000_0"},
        {"1\t0\t2", "warehouse/t/p=1/delta_0000001_0000001_0000/000000_0"},
        {"1\t0\t4", "warehouse/t/p=1/delta_0000001_0000001_0000/000000_0"}};
    checkResult(expected, testQuery, isVectorized, "add 2 parts w/data and 1 empty", LOG);

    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/3'");
    //should be an error since p=3 exists
    CommandProcessorException e =
        runStatementOnDriverNegative("ALTER TABLE T ADD PARTITION (p=0) location '" + getWarehouseDir() + "/3/data'");
    Assert.assertTrue("add existing partition",
        e.getMessage() != null && e.getMessage().contains("Partition already exists"));

    //should be no-op since p=3 exists
    runStatementOnDriver("ALTER TABLE T ADD IF NOT EXISTS " +
        "PARTITION (p=0) location '" + getWarehouseDir() + "/3/data' "//p=0 exists and is not empty
        + "PARTITION (p=2) location '" + getWarehouseDir() + "/3/data'"//p=2 exists and is empty
        + "PARTITION (p=3) location '" + getWarehouseDir() + "/3/data'");//p=3 doesn't exist
    String[][] expected2 = new String[][]{
        {"0\t0\t2", "warehouse/t/p=0/delta_0000001_0000001_0000/000000_0"},
        {"0\t0\t4", "warehouse/t/p=0/delta_0000001_0000001_0000/000000_0"},
        {"1\t0\t2", "warehouse/t/p=1/delta_0000001_0000001_0000/000000_0"},
        {"1\t0\t4", "warehouse/t/p=1/delta_0000001_0000001_0000/000000_0"},
        {"3\t0\t2", "warehouse/t/p=3/delta_0000003_0000003_0000/000000_0"},
        {"3\t0\t4", "warehouse/t/p=3/delta_0000003_0000003_0000/000000_0"}};
    checkResult(expected2, testQuery, isVectorized, "add 2 existing parts and 1 empty", LOG);
  }

  @Test
  public void addPartitionBucketed() throws Exception {
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists Tstage");
    runStatementOnDriver("create table T (a int, b int) partitioned by (p int) " +
        "clustered by (a) into 2 buckets stored as orc tblproperties('transactional'='true')");
    runStatementOnDriver("create table Tstage (a int, b int)  clustered by (a) into 2 " +
        "buckets stored as orc tblproperties('transactional'='false')");

    runStatementOnDriver("insert into Tstage values(0,2),(1,4)");
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/1'");

    runStatementOnDriver("ALTER TABLE T ADD PARTITION (p=0) location '"
        + getWarehouseDir() + "/1/data'");

    List<String> rs = runStatementOnDriver(
        "select ROW__ID, p, a, b, INPUT__FILE__NAME from T order by p, ROW__ID");
    String[][] expected = new String[][]{
        {"{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t0\t0\t2",
            "warehouse/t/p=0/delta_0000001_0000001_0000/000001_0"},
        {"{\"writeid\":1,\"bucketid\":536936448,\"rowid\":1}\t0\t1\t4",
            "warehouse/t/p=0/delta_0000001_0000001_0000/000001_0"}};
    checkExpected(rs, expected, "add partition (p=0)");
  }

  private void checkExpected(List<String> rs, String[][] expected, String msg) {
    super.checkExpected(rs, expected, msg, LOG, true);
  }

  /**
   * Check to make sure that if files being loaded don't have standard Hive names, that they are
   * renamed during add.
   */
  @Test
  public void addPartitionRename() throws Exception {
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists Tstage");
    runStatementOnDriver("create table T (a int, b int) partitioned by (p int) " +
        "stored as orc tblproperties('transactional'='true')");
    //bucketed just so that we get 2 files
    runStatementOnDriver("create table Tstage (a int, b int)  clustered by (a) into 2 " +
        "buckets stored as orc tblproperties('transactional'='false')");

    runStatementOnDriver("insert into Tstage values(0,2),(1,4)");
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/1'");
    FileSystem fs = FileSystem.get(hiveConf);
    fs.rename(new Path(getWarehouseDir() + "/1/data/000000_0"), new Path(getWarehouseDir() + "/1/data/part-m000"));
    fs.rename(new Path(getWarehouseDir() + "/1/data/000001_0"), new Path(getWarehouseDir() + "/1/data/part-m001"));

    runStatementOnDriver("ALTER TABLE T ADD PARTITION (p=0) location '"
        + getWarehouseDir() + "/1/data'");

    List<String> rs = runStatementOnDriver(
        "select ROW__ID, p, a, b, INPUT__FILE__NAME from T order by p, ROW__ID");
    String[][] expected = new String[][]{
        {"{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t0\t0\t2",
            "warehouse/t/p=0/delta_0000001_0000001_0000/000001_0"},
        {"{\"writeid\":1,\"bucketid\":536936448,\"rowid\":1}\t0\t1\t4",
            "warehouse/t/p=0/delta_0000001_0000001_0000/000001_0"}};
    checkExpected(rs, expected, "add partition (p=0)");
  }
  
  @Test
  public void addPartitionTransactional() throws Exception {
    exception.expect(RuntimeException.class);
    exception.expectMessage("was created by Acid write");

    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists Tstage");
    runStatementOnDriver("create table T (a int, b int) partitioned by (p int) " +
        "clustered by (a) into 2 buckets stored as orc tblproperties('transactional'='true')");
    runStatementOnDriver("create table Tstage (a int, b int)  partitioned by (p int) clustered by (a) into 2 " +
        "buckets stored as orc tblproperties('transactional'='true')");

    runStatementOnDriver("insert into Tstage partition(p=1) values(0,2),(1,4)");

    runStatementOnDriver("ALTER TABLE T ADD PARTITION (p=0) location '"
        + getWarehouseDir() + "/tstage/p=1/delta_0000001_0000001_0000/bucket_00001_0'");
  }
}
