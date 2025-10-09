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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.cli;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hive.hcatalog.mapreduce.HCatBaseTest;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestSemanticAnalysis extends HCatBaseTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestSemanticAnalysis.class);
  private static final String TBL_NAME = "junit_sem_analysis";

  private IDriver hcatDriver = null;
  private String query;

  @Before
  public void setUpHCatDriver() throws IOException {
    if (hcatDriver == null) {
      HiveConf hcatConf = new HiveConf(hiveConf);
      hcatConf
      .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
          "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
      hcatConf.set(HiveConf.ConfVars.HIVE_DEFAULT_RCFILE_SERDE.varname,
          "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe");
      hcatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
          HCatSemanticAnalyzer.class.getName());
      hcatConf.setBoolVar(HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES, false);
      hcatDriver = DriverFactory.newDriver(hcatConf);
      SessionState.start(new CliSessionState(hcatConf));
    }
  }

  @Test
  public void testDescDB() throws Exception {
    hcatDriver.run("drop database if exists mydb cascade");
    hcatDriver.run("create database mydb");
    CommandProcessorResponse resp = hcatDriver.run("describe database mydb");
    ArrayList<String> result = new ArrayList<String>();
    hcatDriver.getResults(result);
    assertTrue(result.get(0).contains("mydb"));   // location is not shown in test mode
    hcatDriver.run("drop database mydb cascade");
  }

  @Test
  public void testCreateTblWithLowerCasePartNames() throws Exception {
    driver.run("drop table junit_sem_analysis");
    CommandProcessorResponse resp = driver.run("create table junit_sem_analysis (a int) partitioned by (B string) stored as TEXTFILE");
    Table tbl = client.getTable(Warehouse.DEFAULT_DATABASE_NAME, TBL_NAME);
    assertEquals("Partition key name case problem", "b", tbl.getPartitionKeys().get(0).getName());
    driver.run("drop table junit_sem_analysis");
  }

  @Test
  public void testAlterTblFFpart() throws Exception {

    driver.run("drop table junit_sem_analysis");
    driver.run("create table junit_sem_analysis (a int) partitioned by (b string) stored as TEXTFILE");
    driver.run("alter table junit_sem_analysis add partition (b='2010-10-10')");
    hcatDriver.run("alter table junit_sem_analysis partition (b='2010-10-10') set fileformat RCFILE");

    Table tbl = client.getTable(Warehouse.DEFAULT_DATABASE_NAME, TBL_NAME);
    assertEquals(TextInputFormat.class.getName(), tbl.getSd().getInputFormat());
    assertEquals(HiveIgnoreKeyTextOutputFormat.class.getName(), tbl.getSd().getOutputFormat());

    List<String> partVals = new ArrayList<String>(1);
    partVals.add("2010-10-10");
    Partition part = client.getPartition(Warehouse.DEFAULT_DATABASE_NAME, TBL_NAME, partVals);

    assertEquals(RCFileInputFormat.class.getName(), part.getSd().getInputFormat());
    assertEquals(RCFileOutputFormat.class.getName(), part.getSd().getOutputFormat());

    hcatDriver.run("drop table junit_sem_analysis");
  }

  @Test
  public void testUsNonExistentDB() throws Exception {
    try {
      hcatDriver.run("use no_such_db");
      assert false;
    } catch (CommandProcessorException e) {
      assertEquals(ErrorMsg.DATABASE_NOT_EXISTS.getErrorCode(), e.getResponseCode());
    }
  }

  @Test
  public void testDatabaseOperations() throws Exception {

    List<String> dbs = client.getAllDatabases();
    String testDb1 = "testdatabaseoperatons1";
    String testDb2 = "testdatabaseoperatons2";

    if (dbs.contains(testDb1.toLowerCase())) {
      hcatDriver.run("drop database " + testDb1);
    }

    if (dbs.contains(testDb2.toLowerCase())) {
      hcatDriver.run("drop database " + testDb2);
    }

    hcatDriver.run("create database " + testDb1);
    assertTrue(client.getAllDatabases().contains(testDb1));
    hcatDriver.run("create database if not exists " + testDb1);
    assertTrue(client.getAllDatabases().contains(testDb1));
    hcatDriver.run("create database if not exists " + testDb2);
    assertTrue(client.getAllDatabases().contains(testDb2));

    hcatDriver.run("drop database " + testDb1);
    hcatDriver.run("drop database " + testDb2);
    assertFalse(client.getAllDatabases().contains(testDb1));
    assertFalse(client.getAllDatabases().contains(testDb2));
  }

  @Test
  public void testCreateTableIfNotExists() throws Exception {

    hcatDriver.run("drop table " + TBL_NAME);
    hcatDriver.run("create table " + TBL_NAME + " (a int) stored as RCFILE");
    Table tbl = client.getTable(Warehouse.DEFAULT_DATABASE_NAME, TBL_NAME);
    List<FieldSchema> cols = tbl.getSd().getCols();
    assertEquals(1, cols.size());
    assertTrue(cols.get(0).equals(new FieldSchema("a", "int", null)));
    assertEquals(RCFileInputFormat.class.getName(), tbl.getSd().getInputFormat());
    assertEquals(RCFileOutputFormat.class.getName(), tbl.getSd().getOutputFormat());

    hcatDriver.run("create table if not exists junit_sem_analysis (a int) stored as RCFILE");
    tbl = client.getTable(Warehouse.DEFAULT_DATABASE_NAME, TBL_NAME);
    cols = tbl.getSd().getCols();
    assertEquals(1, cols.size());
    assertTrue(cols.get(0).equals(new FieldSchema("a", "int", null)));
    assertEquals(RCFileInputFormat.class.getName(), tbl.getSd().getInputFormat());
    assertEquals(RCFileOutputFormat.class.getName(), tbl.getSd().getOutputFormat());

    hcatDriver.run("drop table junit_sem_analysis");
  }

  @Test
  public void testAlterTblTouch() throws Exception {

    hcatDriver.run("drop table " + TBL_NAME);
    hcatDriver.run("create table " + TBL_NAME + " (a int) partitioned by (b string) stored as RCFILE");
    hcatDriver.run("alter table " + TBL_NAME + " touch");

    try {
      hcatDriver.run("alter table " + TBL_NAME + " touch partition (b='12')");
      fail("Expected that the command 'alter table " + TBL_NAME + " touch partition (b='12')' would fail");
    } catch (CommandProcessorException e) {
      assertTrue(e.getMessage().contains("Specified partition does not exist"));
    }

    hcatDriver.run("drop table " + TBL_NAME);
  }

  @Test
  public void testChangeColumns() throws Exception {
    hcatDriver.run("drop table junit_sem_analysis");
    hcatDriver.run("create table junit_sem_analysis (a int, c string) partitioned by (b string) stored as RCFILE");
    hcatDriver.run("alter table junit_sem_analysis change a a1 int");

    hcatDriver.run("alter table junit_sem_analysis change a1 a string");

    hcatDriver.run("alter table junit_sem_analysis change a a int after c");
    hcatDriver.run("drop table junit_sem_analysis");
  }

  @Test
  public void testAddReplaceCols() throws Exception {

    hcatDriver.run("drop table junit_sem_analysis");
    hcatDriver.run("create table junit_sem_analysis (a int, c string) partitioned by (b string) stored as RCFILE");
    hcatDriver.run("alter table junit_sem_analysis replace columns (a1 tinyint)");

    hcatDriver.run("alter table junit_sem_analysis add columns (d tinyint)");

    hcatDriver.run("describe extended junit_sem_analysis");
    Table tbl = client.getTable(Warehouse.DEFAULT_DATABASE_NAME, TBL_NAME);
    List<FieldSchema> cols = tbl.getSd().getCols();
    assertEquals(2, cols.size());
    assertTrue(cols.get(0).equals(new FieldSchema("a1", "tinyint", null)));
    assertTrue(cols.get(1).equals(new FieldSchema("d", "tinyint", null)));
    hcatDriver.run("drop table junit_sem_analysis");
  }

  @Test
  public void testAlterTblClusteredBy() throws Exception {

    hcatDriver.run("drop table junit_sem_analysis");
    hcatDriver.run("create table junit_sem_analysis (a int) partitioned by (b string) stored as RCFILE");
    hcatDriver.run("alter table junit_sem_analysis clustered by (a) into 7 buckets");
    hcatDriver.run("drop table junit_sem_analysis");
  }

  @Test
  public void testAlterTableRename() throws Exception {
    hcatDriver.run("drop table oldname");
    hcatDriver.run("drop table newname");
    hcatDriver.run("create table oldname (a int)");
    Table tbl = client.getTable(Warehouse.DEFAULT_DATABASE_NAME, "oldname");
    assertTrue("The old table location is: " + tbl.getSd().getLocation(), tbl.getSd().getLocation().contains("oldname"));

    hcatDriver.run("alter table oldname rename to newNAME");
    tbl = client.getTable(Warehouse.DEFAULT_DATABASE_NAME, "newname");
    // since the oldname table is not under its database (See HIVE-15059), the renamed oldname table will keep
    // its location after HIVE-14909. I changed to check the existence of the newname table and its name instead
    // of verifying its location
    // assertTrue(tbl.getSd().getLocation().contains("newname"));
    assertTrue(tbl != null);
    assertTrue(tbl.getTableName().equalsIgnoreCase("newname"));

    hcatDriver.run("drop table newname");
  }

  @Test
  public void testAlterTableSetFF() throws Exception {

    hcatDriver.run("drop table " + TBL_NAME);
    hcatDriver.run("create table " + TBL_NAME + " (a int) partitioned by (b string) stored as RCFILE");

    Table tbl = client.getTable(Warehouse.DEFAULT_DATABASE_NAME, TBL_NAME);
    assertEquals(RCFileInputFormat.class.getName(), tbl.getSd().getInputFormat());
    assertEquals(RCFileOutputFormat.class.getName(), tbl.getSd().getOutputFormat());

    hcatDriver.run("alter table " + TBL_NAME + " " +
        "set fileformat INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat' " +
        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' " +
        "serde 'myserde' " +
        "inputdriver 'mydriver' " +
        "outputdriver 'yourdriver'");
    hcatDriver.run("desc extended " + TBL_NAME);

    tbl = client.getTable(Warehouse.DEFAULT_DATABASE_NAME, TBL_NAME);
    assertEquals(RCFileInputFormat.class.getName(), tbl.getSd().getInputFormat());
    assertEquals(RCFileOutputFormat.class.getName(), tbl.getSd().getOutputFormat());

    hcatDriver.run("drop table junit_sem_analysis");
  }

  @Test
  public void testAddPartFail() throws Exception {

    driver.run("drop table junit_sem_analysis");
    driver.run("create table junit_sem_analysis (a int) partitioned by (b string) stored as RCFILE");
    hcatDriver.run("alter table junit_sem_analysis add partition (b='2') location 'README.txt'");
    driver.run("drop table junit_sem_analysis");
  }

  @Test
  public void testAddPartPass() throws Exception {

    hcatDriver.run("drop table junit_sem_analysis");
    hcatDriver.run("create table junit_sem_analysis (a int) partitioned by (b string) stored as RCFILE");
    hcatDriver.run("alter table junit_sem_analysis add partition (b='2') location '" + TEST_DATA_DIR + "'");
    hcatDriver.run("drop table junit_sem_analysis");
  }

  @Test
  public void testCTAS() throws Exception {
    hcatDriver.run("drop table junit_sem_analysis");
    query = "create table junit_sem_analysis (a int) as select * from tbl2";
    try {
      hcatDriver.run(query);
      assert false;
    } catch (CommandProcessorException e) {
      assertEquals(40000, e.getResponseCode());
      assertTrue(e.getMessage().contains(
          "FAILED: SemanticException Operation not supported. Create table as Select is not a valid operation."));
    }
    hcatDriver.run("drop table junit_sem_analysis");
  }

  @Test
  public void testStoredAs() throws Exception {
    hcatDriver.run("drop table junit_sem_analysis");
    query = "create table junit_sem_analysis (a int)";
    hcatDriver.run(query);
    hcatDriver.run("drop table junit_sem_analysis");
  }

  @Test
  public void testAddDriverInfo() throws Exception {

    hcatDriver.run("drop table junit_sem_analysis");
    query = "create table junit_sem_analysis (a int) partitioned by (b string)  stored as " +
        "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat' OUTPUTFORMAT " +
        "'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' inputdriver 'mydriver' outputdriver 'yourdriver' ";
    hcatDriver.run(query);

    Table tbl = client.getTable(Warehouse.DEFAULT_DATABASE_NAME, TBL_NAME);
    assertEquals(RCFileInputFormat.class.getName(), tbl.getSd().getInputFormat());
    assertEquals(RCFileOutputFormat.class.getName(), tbl.getSd().getOutputFormat());

    hcatDriver.run("drop table junit_sem_analysis");
  }

  @Test
  public void testInvalidateNonStringPartition() throws Exception {

    hcatDriver.run("drop table junit_sem_analysis");
    query = "create table junit_sem_analysis (a int) partitioned by (b int)  stored as RCFILE";

    try {
      hcatDriver.run(query);
    } catch (CommandProcessorException e) {
      assertEquals(40000, e.getResponseCode());
      assertEquals("FAILED: SemanticException Operation not supported. HCatalog only supports partition columns of " +
          "type string. For column: b Found type: int", e.getMessage());
    }
  }

  @Test
  public void testInvalidateSeqFileStoredAs() throws Exception {

    hcatDriver.run("drop table junit_sem_analysis");
    query = "create table junit_sem_analysis (a int) partitioned by (b string)  stored as SEQUENCEFILE";
    hcatDriver.run(query);
  }

  @Test
  public void testInvalidateTextFileStoredAs() throws Exception {

    hcatDriver.run("drop table junit_sem_analysis");
    query = "create table junit_sem_analysis (a int) partitioned by (b string)  stored as TEXTFILE";
    hcatDriver.run(query);
  }

  @Test
  public void testInvalidateClusteredBy() throws Exception {

    hcatDriver.run("drop table junit_sem_analysis");
    query = "create table junit_sem_analysis (a int) partitioned by (b string) clustered by (a) into 10 buckets stored as TEXTFILE";
    hcatDriver.run(query);
  }

  @Test
  public void testCTLFail() throws Exception {

    driver.run("drop table junit_sem_analysis");
    driver.run("drop table like_table");
    query = "create table junit_sem_analysis (a int) partitioned by (b string) stored as RCFILE";

    driver.run(query);
    query = "create table like_table like junit_sem_analysis";
    hcatDriver.run(query);
  }

  @Test
  public void testCTLPass() throws Exception {

    try {
      hcatDriver.run("drop table junit_sem_analysis");
    } catch (Exception e) {
      LOG.error("Error in drop table.", e);
    }
    query = "create table junit_sem_analysis (a int) partitioned by (b string) stored as RCFILE";

    hcatDriver.run(query);
    String likeTbl = "like_table";
    hcatDriver.run("drop table " + likeTbl);
    query = "create table like_table like junit_sem_analysis";
    hcatDriver.run(query);
//    Table tbl = client.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, likeTbl);
//    assertEquals(likeTbl,tbl.getDbTableName());
//    List<FieldSchema> cols = tbl.getSd().getCols();
//    assertEquals(1, cols.size());
//    assertEquals(new FieldSchema("a", "int", null), cols.get(0));
//    assertEquals("org.apache.hadoop.hive.ql.io.RCFileInputFormat",tbl.getSd().getInputFormat());
//    assertEquals("org.apache.hadoop.hive.ql.io.RCFileOutputFormat",tbl.getSd().getOutputFormat());
//    Map<String, String> tblParams = tbl.getParameters();
//    assertEquals("org.apache.hadoop.hive.hcat.rcfile.RCFileInputStorageDriver", tblParams.get("hcat.isd"));
//    assertEquals("org.apache.hadoop.hive.hcat.rcfile.RCFileOutputStorageDriver", tblParams.get("hcat.osd"));
//
//    hcatDriver.run("drop table junit_sem_analysis");
//    hcatDriver.run("drop table "+likeTbl);
  }

// This test case currently fails, since add partitions don't inherit anything from tables.

//  public void testAddPartInheritDrivers() throws MetaException, TException, NoSuchObjectException{
//
//    hcatDriver.run("drop table "+TBL_NAME);
//    hcatDriver.run("create table junit_sem_analysis (a int) partitioned by (b string) stored as RCFILE");
//    hcatDriver.run("alter table "+TBL_NAME+" add partition (b='2010-10-10')");
//
//    List<String> partVals = new ArrayList<String>(1);
//    partVals.add("2010-10-10");
//
//    Map<String,String> map = client.getPartition(MetaStoreUtils.DEFAULT_DATABASE_NAME, TBL_NAME, partVals).getParameters();
//    assertEquals(map.get(InitializeInput.HOWL_ISD_CLASS), RCFileInputStorageDriver.class.getName());
//    assertEquals(map.get(InitializeInput.HOWL_OSD_CLASS), RCFileOutputStorageDriver.class.getName());
//  }
}
