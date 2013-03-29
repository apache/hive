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
package org.apache.hcatalog.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hcatalog.listener.NotificationListener;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSemanticAnalysis extends TestCase{

  private Driver hcatDriver;
  private Driver hiveDriver;
  private HiveMetaStoreClient msc;
  private static final Logger LOG = LoggerFactory.getLogger(TestSemanticAnalysis.class);

  @Override
  protected void setUp() throws Exception {

	System.setProperty(ConfVars.METASTORE_EVENT_LISTENERS.varname, NotificationListener.class.getName());
    HiveConf hcatConf = new HiveConf(this.getClass());
    hcatConf.set(ConfVars.PREEXECHOOKS.varname, "");
    hcatConf.set(ConfVars.POSTEXECHOOKS.varname, "");
    hcatConf.set(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

    HiveConf hiveConf = new HiveConf(hcatConf,this.getClass());
    hiveDriver = new Driver(hiveConf);

    hcatConf.set(ConfVars.SEMANTIC_ANALYZER_HOOK.varname, HCatSemanticAnalyzer.class.getName());
    hcatDriver = new Driver(hcatConf);

    msc = new HiveMetaStoreClient(hcatConf);
    SessionState.start(new CliSessionState(hcatConf));
  }

  String query;
  private final String tblName = "junit_sem_analysis";

  public void testDescDB() throws CommandNeedRetryException, IOException {
	hcatDriver.run("drop database mydb cascade");
	assertEquals(0, hcatDriver.run("create database mydb").getResponseCode());
	CommandProcessorResponse resp = hcatDriver.run("describe database mydb");
	assertEquals(0, resp.getResponseCode());
	ArrayList<String> result = new ArrayList<String>();
	hcatDriver.getResults(result);
	assertTrue(result.get(0).contains("mydb.db"));
	hcatDriver.run("drop database mydb cascade");
  }

  public void testCreateTblWithLowerCasePartNames() throws CommandNeedRetryException, MetaException, TException, NoSuchObjectException{
    hiveDriver.run("drop table junit_sem_analysis");
    CommandProcessorResponse resp = hiveDriver.run("create table junit_sem_analysis (a int) partitioned by (B string) stored as TEXTFILE");
    assertEquals(resp.getResponseCode(), 0);
    assertEquals(null, resp.getErrorMessage());
    Table tbl = msc.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName);
    assertEquals("Partition key name case problem", "b" , tbl.getPartitionKeys().get(0).getName());
    hiveDriver.run("drop table junit_sem_analysis");
  }

  public void testAlterTblFFpart() throws MetaException, TException, NoSuchObjectException, CommandNeedRetryException {

    hiveDriver.run("drop table junit_sem_analysis");
    hiveDriver.run("create table junit_sem_analysis (a int) partitioned by (b string) stored as TEXTFILE");
    hiveDriver.run("alter table junit_sem_analysis add partition (b='2010-10-10')");
    hcatDriver.run("alter table junit_sem_analysis partition (b='2010-10-10') set fileformat RCFILE");

    Table tbl = msc.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName);
    assertEquals(TextInputFormat.class.getName(),tbl.getSd().getInputFormat());
    assertEquals(HiveIgnoreKeyTextOutputFormat.class.getName(),tbl.getSd().getOutputFormat());

    List<String> partVals = new ArrayList<String>(1);
    partVals.add("2010-10-10");
    Partition part = msc.getPartition(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName, partVals);

    assertEquals(RCFileInputFormat.class.getName(),part.getSd().getInputFormat());
    assertEquals(RCFileOutputFormat.class.getName(),part.getSd().getOutputFormat());

    hcatDriver.run("drop table junit_sem_analysis");
  }


  public void testUsNonExistentDB() throws CommandNeedRetryException {
	assertEquals(9, hcatDriver.run("use no_such_db").getResponseCode());
  }

  public void testDatabaseOperations() throws MetaException, CommandNeedRetryException {

    List<String> dbs = msc.getAllDatabases();
    String testDb1 = "testdatabaseoperatons1";
    String testDb2 = "testdatabaseoperatons2";

    if (dbs.contains(testDb1.toLowerCase())){
      assertEquals(0,hcatDriver.run("drop database "+testDb1).getResponseCode());
    }

    if (dbs.contains(testDb2.toLowerCase())){
      assertEquals(0,hcatDriver.run("drop database "+testDb2).getResponseCode());
    }

    assertEquals(0,hcatDriver.run("create database "+testDb1).getResponseCode());
    assertTrue(msc.getAllDatabases().contains(testDb1));
    assertEquals(0,hcatDriver.run("create database if not exists "+testDb1).getResponseCode());
    assertTrue(msc.getAllDatabases().contains(testDb1));
    assertEquals(0,hcatDriver.run("create database if not exists "+testDb2).getResponseCode());
    assertTrue(msc.getAllDatabases().contains(testDb2));

    assertEquals(0,hcatDriver.run("drop database "+testDb1).getResponseCode());
    assertEquals(0,hcatDriver.run("drop database "+testDb2).getResponseCode());
    assertFalse(msc.getAllDatabases().contains(testDb1));
    assertFalse(msc.getAllDatabases().contains(testDb2));
  }

  public void testCreateTableIfNotExists() throws MetaException, TException, NoSuchObjectException, CommandNeedRetryException{

    hcatDriver.run("drop table "+tblName);
    hcatDriver.run("create table junit_sem_analysis (a int) stored as RCFILE");
    Table tbl = msc.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName);
    List<FieldSchema> cols = tbl.getSd().getCols();
    assertEquals(1, cols.size());
    assertTrue(cols.get(0).equals(new FieldSchema("a", "int", null)));
    assertEquals(RCFileInputFormat.class.getName(),tbl.getSd().getInputFormat());
    assertEquals(RCFileOutputFormat.class.getName(),tbl.getSd().getOutputFormat());

    CommandProcessorResponse resp = hcatDriver.run("create table if not exists junit_sem_analysis (a int) stored as RCFILE");
    assertEquals(0, resp.getResponseCode());
    assertNull(resp.getErrorMessage());
    tbl = msc.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName);
    cols = tbl.getSd().getCols();
    assertEquals(1, cols.size());
    assertTrue(cols.get(0).equals(new FieldSchema("a", "int",null)));
    assertEquals(RCFileInputFormat.class.getName(),tbl.getSd().getInputFormat());
    assertEquals(RCFileOutputFormat.class.getName(),tbl.getSd().getOutputFormat());

    hcatDriver.run("drop table junit_sem_analysis");
  }

  public void testAlterTblTouch() throws CommandNeedRetryException{

    hcatDriver.run("drop table junit_sem_analysis");
    hcatDriver.run("create table junit_sem_analysis (a int) partitioned by (b string) stored as RCFILE");
    CommandProcessorResponse response = hcatDriver.run("alter table junit_sem_analysis touch");
    assertEquals(0, response.getResponseCode());

    hcatDriver.run("alter table junit_sem_analysis touch partition (b='12')");
    assertEquals(0, response.getResponseCode());

    hcatDriver.run("drop table junit_sem_analysis");
  }

  public void testChangeColumns() throws CommandNeedRetryException{
    hcatDriver.run("drop table junit_sem_analysis");
    hcatDriver.run("create table junit_sem_analysis (a int, c string) partitioned by (b string) stored as RCFILE");
    CommandProcessorResponse response = hcatDriver.run("alter table junit_sem_analysis change a a1 int");
    assertEquals(0, response.getResponseCode());

    response = hcatDriver.run("alter table junit_sem_analysis change a1 a string");
    assertEquals(0, response.getResponseCode());

    response = hcatDriver.run("alter table junit_sem_analysis change a a int after c");
    assertEquals(0, response.getResponseCode());
    hcatDriver.run("drop table junit_sem_analysis");
  }

  public void testAddReplaceCols() throws IOException, MetaException, TException, NoSuchObjectException, CommandNeedRetryException{

    hcatDriver.run("drop table junit_sem_analysis");
    hcatDriver.run("create table junit_sem_analysis (a int, c string) partitioned by (b string) stored as RCFILE");
    CommandProcessorResponse response = hcatDriver.run("alter table junit_sem_analysis replace columns (a1 tinyint)");
    assertEquals(0, response.getResponseCode());

    response = hcatDriver.run("alter table junit_sem_analysis add columns (d tinyint)");
    assertEquals(0, response.getResponseCode());
    assertNull(response.getErrorMessage());

    response = hcatDriver.run("describe extended junit_sem_analysis");
    assertEquals(0, response.getResponseCode());
    Table tbl = msc.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName);
    List<FieldSchema> cols = tbl.getSd().getCols();
    assertEquals(2, cols.size());
    assertTrue(cols.get(0).equals(new FieldSchema("a1", "tinyint", null)));
    assertTrue(cols.get(1).equals(new FieldSchema("d", "tinyint", null)));
    hcatDriver.run("drop table junit_sem_analysis");
  }

  public void testAlterTblClusteredBy() throws CommandNeedRetryException{

    hcatDriver.run("drop table junit_sem_analysis");
    hcatDriver.run("create table junit_sem_analysis (a int) partitioned by (b string) stored as RCFILE");
    CommandProcessorResponse response = hcatDriver.run("alter table junit_sem_analysis clustered by (a) into 7 buckets");
    assertEquals(0, response.getResponseCode());
    hcatDriver.run("drop table junit_sem_analysis");
  }

  public void testAlterTableSetFF() throws IOException, MetaException, TException, NoSuchObjectException, CommandNeedRetryException{

    hcatDriver.run("drop table junit_sem_analysis");
    hcatDriver.run("create table junit_sem_analysis (a int) partitioned by (b string) stored as RCFILE");

    Table tbl = msc.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName);
    assertEquals(RCFileInputFormat.class.getName(),tbl.getSd().getInputFormat());
    assertEquals(RCFileOutputFormat.class.getName(),tbl.getSd().getOutputFormat());

    hcatDriver.run("alter table junit_sem_analysis set fileformat INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat' OUTPUTFORMAT " +
        "'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' inputdriver 'mydriver' outputdriver 'yourdriver'");
    hcatDriver.run("desc extended junit_sem_analysis");

    tbl = msc.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName);
    assertEquals(RCFileInputFormat.class.getName(),tbl.getSd().getInputFormat());
    assertEquals(RCFileOutputFormat.class.getName(),tbl.getSd().getOutputFormat());

    hcatDriver.run("drop table junit_sem_analysis");
  }

  public void testAddPartFail() throws CommandNeedRetryException{

    hiveDriver.run("drop table junit_sem_analysis");
    hiveDriver.run("create table junit_sem_analysis (a int) partitioned by (b string) stored as RCFILE");
    CommandProcessorResponse response = hcatDriver.run("alter table junit_sem_analysis add partition (b='2') location 'README.txt'");
    assertEquals(0, response.getResponseCode());
    hiveDriver.run("drop table junit_sem_analysis");
  }

  public void testAddPartPass() throws IOException, CommandNeedRetryException{

    hcatDriver.run("drop table junit_sem_analysis");
    hcatDriver.run("create table junit_sem_analysis (a int) partitioned by (b string) stored as RCFILE");
    CommandProcessorResponse response = hcatDriver.run("alter table junit_sem_analysis add partition (b='2') location '/tmp'");
    assertEquals(0, response.getResponseCode());
    assertNull(response.getErrorMessage());
    hcatDriver.run("drop table junit_sem_analysis");
  }

  public void testCTAS() throws CommandNeedRetryException{
    hcatDriver.run("drop table junit_sem_analysis");
    query = "create table junit_sem_analysis (a int) as select * from tbl2";
    CommandProcessorResponse response = hcatDriver.run(query);
    assertEquals(10, response.getResponseCode());
    assertTrue(response.getErrorMessage().contains("FAILED: Error in semantic analysis: Operation not supported. Create table as Select is not a valid operation."));
    hcatDriver.run("drop table junit_sem_analysis");
  }

  public void testStoredAs() throws CommandNeedRetryException{
    hcatDriver.run("drop table junit_sem_analysis");
    query = "create table junit_sem_analysis (a int)";
    CommandProcessorResponse response = hcatDriver.run(query);
    assertEquals(0, response.getResponseCode());
    hcatDriver.run("drop table junit_sem_analysis");
  }

  public void testAddDriverInfo() throws IOException, MetaException, TException, NoSuchObjectException, CommandNeedRetryException{

    hcatDriver.run("drop table junit_sem_analysis");
    query =  "create table junit_sem_analysis (a int) partitioned by (b string)  stored as " +
    		"INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat' OUTPUTFORMAT " +
    		"'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' inputdriver 'mydriver' outputdriver 'yourdriver' ";
    assertEquals(0,hcatDriver.run(query).getResponseCode());

    Table tbl = msc.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName);
    assertEquals(RCFileInputFormat.class.getName(),tbl.getSd().getInputFormat());
    assertEquals(RCFileOutputFormat.class.getName(),tbl.getSd().getOutputFormat());

    hcatDriver.run("drop table junit_sem_analysis");
  }

  public void testInvalidateNonStringPartition() throws IOException, CommandNeedRetryException{

    hcatDriver.run("drop table junit_sem_analysis");
    query =  "create table junit_sem_analysis (a int) partitioned by (b int)  stored as RCFILE";

    CommandProcessorResponse response = hcatDriver.run(query);
    assertEquals(10,response.getResponseCode());
    assertEquals("FAILED: Error in semantic analysis: Operation not supported. HCatalog only supports partition columns of type string. For column: b Found type: int",
        response.getErrorMessage());

  }

  public void testInvalidateSeqFileStoredAs() throws IOException, CommandNeedRetryException{

    hcatDriver.run("drop table junit_sem_analysis");
    query =  "create table junit_sem_analysis (a int) partitioned by (b string)  stored as SEQUENCEFILE";

    CommandProcessorResponse response = hcatDriver.run(query);
    assertEquals(0,response.getResponseCode());

  }

  public void testInvalidateTextFileStoredAs() throws IOException, CommandNeedRetryException{

    hcatDriver.run("drop table junit_sem_analysis");
    query =  "create table junit_sem_analysis (a int) partitioned by (b string)  stored as TEXTFILE";

    CommandProcessorResponse response = hcatDriver.run(query);
    assertEquals(0,response.getResponseCode());

  }

  public void testInvalidateClusteredBy() throws IOException, CommandNeedRetryException{

    hcatDriver.run("drop table junit_sem_analysis");
    query =  "create table junit_sem_analysis (a int) partitioned by (b string) clustered by (a) into 10 buckets stored as TEXTFILE";

    CommandProcessorResponse response = hcatDriver.run(query);
    assertEquals(0,response.getResponseCode());
  }

  public void testCTLFail() throws IOException, CommandNeedRetryException{

    hiveDriver.run("drop table junit_sem_analysis");
    hiveDriver.run("drop table like_table");
    query =  "create table junit_sem_analysis (a int) partitioned by (b string) stored as RCFILE";

    hiveDriver.run(query);
    query = "create table like_table like junit_sem_analysis";
    CommandProcessorResponse response = hcatDriver.run(query);
    assertEquals(0,response.getResponseCode());
  }

  public void testCTLPass() throws IOException, MetaException, TException, NoSuchObjectException, CommandNeedRetryException{

    try{
      hcatDriver.run("drop table junit_sem_analysis");
    }
    catch( Exception e){
      LOG.error("Error in drop table.",e);
    }
    query =  "create table junit_sem_analysis (a int) partitioned by (b string) stored as RCFILE";

    hcatDriver.run(query);
    String likeTbl = "like_table";
    hcatDriver.run("drop table "+likeTbl);
    query = "create table like_table like junit_sem_analysis";
    CommandProcessorResponse resp = hcatDriver.run(query);
    assertEquals(0, resp.getResponseCode());
//    Table tbl = msc.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, likeTbl);
//    assertEquals(likeTbl,tbl.getTableName());
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
//    hcatDriver.run("drop table "+tblName);
//    hcatDriver.run("create table junit_sem_analysis (a int) partitioned by (b string) stored as RCFILE");
//    hcatDriver.run("alter table "+tblName+" add partition (b='2010-10-10')");
//
//    List<String> partVals = new ArrayList<String>(1);
//    partVals.add("2010-10-10");
//
//    Map<String,String> map = msc.getPartition(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName, partVals).getParameters();
//    assertEquals(map.get(InitializeInput.HOWL_ISD_CLASS), RCFileInputStorageDriver.class.getName());
//    assertEquals(map.get(InitializeInput.HOWL_OSD_CLASS), RCFileOutputStorageDriver.class.getName());
//  }
}
