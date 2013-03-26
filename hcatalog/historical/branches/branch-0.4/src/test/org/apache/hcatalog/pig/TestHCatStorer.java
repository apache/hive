/**
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
package org.apache.hcatalog.pig;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hcatalog.HcatTestUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.LogUtils;
import org.junit.Assert;

public class TestHCatStorer extends TestCase {
  private static final String TEST_DATA_DIR = System.getProperty("user.dir") +
      "/build/test/data/" + TestHCatStorer.class.getCanonicalName();
  private static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  private static final String INPUT_FILE_NAME = TEST_DATA_DIR + "/input.data";

  private Driver driver;

  @Override
  protected void setUp() throws Exception {
    if (driver == null) {
      HiveConf hiveConf = new HiveConf(this.getClass());
      hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
      hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
      hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
      hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, TEST_WAREHOUSE_DIR);
      driver = new Driver(hiveConf);
      SessionState.start(new CliSessionState(hiveConf));
    }
  }

//  public void testStoreFuncMap() throws IOException{
//
//    driver.run("drop table junit_unparted");
//    String createTable = "create table junit_unparted(b string,arr_of_maps array<map<string,string>>) stored as RCFILE " +
//        "tblproperties('hcat.isd'='org.apache.hadoop.hive.hCatalog.rcfile.RCFileInputStorageDriver'," +
//        "'hcat.osd'='org.apache.hadoop.hive.hCatalog.rcfile.RCFileOutputStorageDriver') ";
//    int retCode = driver.run(createTable).getResponseCode();
//    if(retCode != 0) {
//      throw new IOException("Failed to create table.");
//    }
//
//    MiniCluster.deleteFile(cluster, fileName);
//    MiniCluster.createInputFile(cluster, fileName, new String[]{"test\t{([a#haddop,b#pig])}","data\t{([b#hive,a#hcat])}"});
//
//    PigServer server = new PigServer(ExecType.LOCAL);
//    server.setBatchOn();
//    server.registerQuery("A = load '"+ fullFileName +"' as (b:chararray,arr_of_maps:bag{mytup:tuple ( mymap:map[ ])});");
//    server.registerQuery("store A into 'default.junit_unparted' using org.apache.hadoop.hive.hCatalog.pig.HCatStorer('','b:chararray,arr_of_maps:bag{mytup:tuple ( mymap:map[ ])}');");
//    server.executeBatch();
//
//
//
//    MiniCluster.deleteFile(cluster, fileName);
//
//    driver.run("select * from junit_unparted");
//    ArrayList<String> res = new ArrayList<String>();
//    driver.getResults(res);
//    driver.run("drop table junit_unparted");
//    Iterator<String> itr = res.iterator();
//    System.out.println(itr.next());
//    System.out.println(itr.next());
//   assertFalse(itr.hasNext());
//
//  }

  public void testPartColsInData() throws IOException, CommandNeedRetryException{

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int) partitioned by (b string) stored as RCFILE";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }
    int LOOP_SIZE = 11;
    String[] input = new String[LOOP_SIZE];
    for(int i = 0; i < LOOP_SIZE; i++) {
        input[i] = i + "\t1";
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.registerQuery("A = load '"+INPUT_FILE_NAME+"' as (a:int, b:chararray);");
    server.registerQuery("store A into 'default.junit_unparted' using "+HCatStorer.class.getName()+"('b=1');");
    server.registerQuery("B = load 'default.junit_unparted' using "+HCatLoader.class.getName()+"();");
    Iterator<Tuple> itr= server.openIterator("B");

    int i = 0;

    while(itr.hasNext()){
      Tuple t = itr.next();
      assertEquals(2, t.size());
      assertEquals(t.get(0), i);
      assertEquals(t.get(1), "1");
      i++;
    }

    assertFalse(itr.hasNext());
    assertEquals(11, i);
  }

  public void testMultiPartColsInData() throws IOException, CommandNeedRetryException{

    driver.run("drop table employee");
    String createTable = "CREATE TABLE employee (emp_id INT, emp_name STRING, emp_start_date STRING , emp_gender STRING ) " +
    		" PARTITIONED BY (emp_country STRING , emp_state STRING ) STORED AS RCFILE";

    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    String[] inputData = {"111237\tKrishna\t01/01/1990\tM\tIN\tTN",
                          "111238\tKalpana\t01/01/2000\tF\tIN\tKA",
                          "111239\tSatya\t01/01/2001\tM\tIN\tKL",
                          "111240\tKavya\t01/01/2002\tF\tIN\tAP"};

    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, inputData);
    PigServer pig = new PigServer(ExecType.LOCAL);
    pig.setBatchOn();
    pig.registerQuery("A = LOAD '"+INPUT_FILE_NAME+"' USING PigStorage() AS (emp_id:int,emp_name:chararray,emp_start_date:chararray," +
    		"emp_gender:chararray,emp_country:chararray,emp_state:chararray);");
    pig.registerQuery("TN = FILTER A BY emp_state == 'TN';");
    pig.registerQuery("KA = FILTER A BY emp_state == 'KA';");
    pig.registerQuery("KL = FILTER A BY emp_state == 'KL';");
    pig.registerQuery("AP = FILTER A BY emp_state == 'AP';");
    pig.registerQuery("STORE TN INTO 'employee' USING "+HCatStorer.class.getName()+"('emp_country=IN,emp_state=TN');");
    pig.registerQuery("STORE KA INTO 'employee' USING "+HCatStorer.class.getName()+"('emp_country=IN,emp_state=KA');");
    pig.registerQuery("STORE KL INTO 'employee' USING "+HCatStorer.class.getName()+"('emp_country=IN,emp_state=KL');");
    pig.registerQuery("STORE AP INTO 'employee' USING "+HCatStorer.class.getName()+"('emp_country=IN,emp_state=AP');");
    pig.executeBatch();
    driver.run("select * from employee");
    ArrayList<String> results = new ArrayList<String>();
    driver.getResults(results);
    assertEquals(4, results.size());
    Collections.sort(results);
    assertEquals(inputData[0], results.get(0));
    assertEquals(inputData[1], results.get(1));
    assertEquals(inputData[2], results.get(2));
    assertEquals(inputData[3], results.get(3));
    driver.run("drop table employee");
  }

  public void testStoreInPartiitonedTbl() throws IOException, CommandNeedRetryException{

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int) partitioned by (b string) stored as RCFILE";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }
    int LOOP_SIZE = 11;
    String[] input = new String[LOOP_SIZE];
    for(int i = 0; i < LOOP_SIZE; i++) {
        input[i] = i+"";
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.registerQuery("A = load '"+INPUT_FILE_NAME+"' as (a:int);");
    server.registerQuery("store A into 'default.junit_unparted' using "+HCatStorer.class.getName()+"('b=1');");
    server.registerQuery("B = load 'default.junit_unparted' using "+HCatLoader.class.getName()+"();");
    Iterator<Tuple> itr= server.openIterator("B");

    int i = 0;

    while(itr.hasNext()){
      Tuple t = itr.next();
      assertEquals(2, t.size());
      assertEquals(t.get(0), i);
      assertEquals(t.get(1), "1");
      i++;
    }

    assertFalse(itr.hasNext());
    assertEquals(11, i);
  }

  public void testNoAlias() throws IOException, CommandNeedRetryException{
    driver.run("drop table junit_parted");
    String createTable = "create table junit_parted(a int, b string) partitioned by (ds string) stored as RCFILE";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }
    PigServer server = new PigServer(ExecType.LOCAL);
    boolean errCaught = false;
    try{
      server.setBatchOn();
      server.registerQuery("A = load '"+ INPUT_FILE_NAME +"' as (a:int, b:chararray);");
      server.registerQuery("B = foreach A generate a+10, b;");
      server.registerQuery("store B into 'junit_parted' using "+HCatStorer.class.getName()+"('ds=20100101');");
      server.executeBatch();
    }
    catch(PigException fe){
      PigException pe = LogUtils.getPigException(fe);
      assertTrue(pe instanceof FrontendException);
      assertEquals(PigHCatUtil.PIG_EXCEPTION_CODE, pe.getErrorCode());
      assertTrue(pe.getMessage().contains("Column name for a field is not specified. Please provide the full schema as an argument to HCatStorer."));
      errCaught = true;
    }
    assertTrue(errCaught);
    errCaught = false;
    try{
      server.setBatchOn();
      server.registerQuery("A = load '"+ INPUT_FILE_NAME +"' as (a:int, B:chararray);");
      server.registerQuery("B = foreach A generate a, B;");
      server.registerQuery("store B into 'junit_parted' using "+HCatStorer.class.getName()+"('ds=20100101');");
      server.executeBatch();
    }
    catch(PigException fe){
      PigException pe = LogUtils.getPigException(fe);
      assertTrue(pe instanceof FrontendException);
      assertEquals(PigHCatUtil.PIG_EXCEPTION_CODE, pe.getErrorCode());
      assertTrue(pe.getMessage().contains("Column names should all be in lowercase. Invalid name found: B"));
      errCaught = true;
    }
    driver.run("drop table junit_parted");
    assertTrue(errCaught);
  }

  public void testStoreMultiTables() throws IOException, CommandNeedRetryException{

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b string) stored as RCFILE";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }
    driver.run("drop table junit_unparted2");
    createTable = "create table junit_unparted2(a int, b string) stored as RCFILE";
    retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE*LOOP_SIZE];
    int k = 0;
    for(int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for(int j=1;j<=LOOP_SIZE;j++) {
        input[k++] = si + "\t"+j;
      }
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '"+ INPUT_FILE_NAME +"' as (a:int, b:chararray);");
    server.registerQuery("B = filter A by a < 2;");
    server.registerQuery("store B into 'junit_unparted' using "+HCatStorer.class.getName()+"();");
    server.registerQuery("C = filter A by a >= 2;");
    server.registerQuery("store C into 'junit_unparted2' using "+HCatStorer.class.getName()+"();");
    server.executeBatch();

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("select * from junit_unparted2");
    ArrayList<String> res2 = new ArrayList<String>();
    driver.getResults(res2);

    res.addAll(res2);
    driver.run("drop table junit_unparted");
    driver.run("drop table junit_unparted2");

    Iterator<String> itr = res.iterator();
    for(int i = 0; i < LOOP_SIZE*LOOP_SIZE; i++) {
      assertEquals( input[i] ,itr.next());
    }

    assertFalse(itr.hasNext());

  }

  public void testStoreWithNoSchema() throws IOException, CommandNeedRetryException{

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b string) stored as RCFILE";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE*LOOP_SIZE];
    int k = 0;
    for(int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for(int j=1;j<=LOOP_SIZE;j++) {
        input[k++] = si + "\t"+j;
      }
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '"+ INPUT_FILE_NAME +"' as (a:int, b:chararray);");
    server.registerQuery("store A into 'default.junit_unparted' using "+HCatStorer.class.getName()+"('');");
    server.executeBatch();

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("drop table junit_unparted");
    Iterator<String> itr = res.iterator();
    for(int i = 0; i < LOOP_SIZE*LOOP_SIZE; i++) {
      assertEquals( input[i] ,itr.next());
    }

    assertFalse(itr.hasNext());

  }

  public void testStoreWithNoCtorArgs() throws IOException, CommandNeedRetryException{

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b string) stored as RCFILE";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE*LOOP_SIZE];
    int k = 0;
    for(int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for(int j=1;j<=LOOP_SIZE;j++) {
        input[k++] = si + "\t"+j;
      }
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '"+ INPUT_FILE_NAME +"' as (a:int, b:chararray);");
    server.registerQuery("store A into 'junit_unparted' using "+HCatStorer.class.getName()+"();");
    server.executeBatch();

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("drop table junit_unparted");
    Iterator<String> itr = res.iterator();
    for(int i = 0; i < LOOP_SIZE*LOOP_SIZE; i++) {
      assertEquals( input[i] ,itr.next());
    }

    assertFalse(itr.hasNext());

  }

  public void testEmptyStore() throws IOException, CommandNeedRetryException{

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b string) stored as RCFILE";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE*LOOP_SIZE];
    int k = 0;
    for(int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for(int j=1;j<=LOOP_SIZE;j++) {
        input[k++] = si + "\t"+j;
      }
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '"+INPUT_FILE_NAME+"' as (a:int, b:chararray);");
    server.registerQuery("B = filter A by a > 100;");
    server.registerQuery("store B into 'default.junit_unparted' using "+HCatStorer.class.getName()+"('','a:int,b:chararray');");
    server.executeBatch();

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("drop table junit_unparted");
    Iterator<String> itr = res.iterator();
    assertFalse(itr.hasNext());

  }

  public void testBagNStruct() throws IOException, CommandNeedRetryException{
  driver.run("drop table junit_unparted");
  String createTable = "create table junit_unparted(b string,a struct<a1:int>,  arr_of_struct array<string>, " +
  		"arr_of_struct2 array<struct<s1:string,s2:string>>,  arr_of_struct3 array<struct<s3:string>>) stored as RCFILE";
  int retCode = driver.run(createTable).getResponseCode();
  if(retCode != 0) {
    throw new IOException("Failed to create table.");
  }

  String[] inputData = new String[]{"zookeeper\t(2)\t{(pig)}\t{(pnuts,hdfs)}\t{(hadoop),(hcat)}",
      "chubby\t(2)\t{(sawzall)}\t{(bigtable,gfs)}\t{(mapreduce),(hcat)}"};

  HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, inputData);

  PigServer server = new PigServer(ExecType.LOCAL);
  server.setBatchOn();
  server.registerQuery("A = load '"+INPUT_FILE_NAME+"' as (b:chararray, a:tuple(a1:int), arr_of_struct:bag{mytup:tuple(s1:chararray)}, arr_of_struct2:bag{mytup:tuple(s1:chararray,s2:chararray)}, arr_of_struct3:bag{t3:tuple(s3:chararray)});");
  server.registerQuery("store A into 'default.junit_unparted' using "+HCatStorer.class.getName()+"('','b:chararray, a:tuple(a1:int)," +
  		" arr_of_struct:bag{mytup:tuple(s1:chararray)}, arr_of_struct2:bag{mytup:tuple(s1:chararray,s2:chararray)}, arr_of_struct3:bag{t3:tuple(s3:chararray)}');");
  server.executeBatch();

  driver.run("select * from junit_unparted");
  ArrayList<String> res = new ArrayList<String>();
  driver.getResults(res);
  driver.run("drop table junit_unparted");
  Iterator<String> itr = res.iterator();
  assertEquals("zookeeper\t{\"a1\":2}\t[\"pig\"]\t[{\"s1\":\"pnuts\",\"s2\":\"hdfs\"}]\t[{\"s3\":\"hadoop\"},{\"s3\":\"hcat\"}]", itr.next());
  assertEquals("chubby\t{\"a1\":2}\t[\"sawzall\"]\t[{\"s1\":\"bigtable\",\"s2\":\"gfs\"}]\t[{\"s3\":\"mapreduce\"},{\"s3\":\"hcat\"}]",itr.next());
 assertFalse(itr.hasNext());

  }

  public void testStoreFuncAllSimpleTypes() throws IOException, CommandNeedRetryException{

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b float, c double, d bigint, e string, f binary, g binary) stored as RCFILE";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    int i =0;
    String[] input = new String[3];
    input[i++]= "0\t\t\t\t\t\t"; //Empty values except first column
    input[i++]= "\t" + i * 2.1f +"\t"+ i*1.1d + "\t" + i * 2L +"\t"+"lets hcat"+"\tbinary-data"; //First column empty
    input[i++]= i + "\t" + i * 2.1f +"\t"+ i*1.1d + "\t" + i * 2L +"\t"+"lets hcat"+"\tbinary-data";

    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '"+INPUT_FILE_NAME+"' as (a:int, b:float, c:double, d:long, e:chararray, f:bytearray);");
    //null gets stored into column g which is a binary field.
    server.registerQuery("store A into 'default.junit_unparted' using "+HCatStorer.class.getName()+"('','a:int, b:float, c:double, d:long, e:chararray,f:bytearray');");
    server.executeBatch();


    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);

    Iterator<String> itr = res.iterator();
    assertEquals( "0\tNULL\tNULL\tNULL\tNULL\t\tnull" ,itr.next());
    assertEquals( "NULL\t4.2\t2.2\t4\tlets hcat\tbinary-data\tnull" ,itr.next());
    assertEquals( "3\t6.2999997\t3.3000000000000003\t6\tlets hcat\tbinary-data\tnull",itr.next());
    assertFalse(itr.hasNext());

    server.registerQuery("B = load 'junit_unparted' using "+HCatLoader.class.getName()+";");
    Iterator<Tuple> iter = server.openIterator("B");
    int count = 0;
    while(iter.hasNext()){
        Tuple t = iter.next();
        assertTrue(t.get(5) instanceof DataByteArray);
        assertNull(t.get(6));
        count++;
    }
    assertEquals(3, count);
    driver.run("drop table junit_unparted");
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }




  public void testStoreFuncSimple() throws IOException, CommandNeedRetryException{

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b string) stored as RCFILE";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    int LOOP_SIZE = 3;
    String[] inputData = new String[LOOP_SIZE*LOOP_SIZE];
    int k = 0;
    for(int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for(int j=1;j<=LOOP_SIZE;j++) {
        inputData[k++] = si + "\t"+j;
      }
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, inputData);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '"+INPUT_FILE_NAME+"' as (a:int, b:chararray);");
    server.registerQuery("store A into 'default.junit_unparted' using "+HCatStorer.class.getName()+"('','a:int,b:chararray');");
    server.executeBatch();

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("drop table junit_unparted");
    Iterator<String> itr = res.iterator();
    for(int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for(int j=1;j<=LOOP_SIZE;j++) {
        assertEquals( si + "\t"+j,itr.next());
      }
    }
   assertFalse(itr.hasNext());

  }


  public void testDynamicPartitioningMultiPartColsInDataPartialSpec() throws IOException, CommandNeedRetryException{

    driver.run("drop table if exists employee");
    String createTable = "CREATE TABLE employee (emp_id INT, emp_name STRING, emp_start_date STRING , emp_gender STRING ) " +
        " PARTITIONED BY (emp_country STRING , emp_state STRING ) STORED AS RCFILE";

    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    String[] inputData = {"111237\tKrishna\t01/01/1990\tM\tIN\tTN",
                          "111238\tKalpana\t01/01/2000\tF\tIN\tKA",
                          "111239\tSatya\t01/01/2001\tM\tIN\tKL",
                          "111240\tKavya\t01/01/2002\tF\tIN\tAP"};

    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, inputData);
    PigServer pig = new PigServer(ExecType.LOCAL);
    pig.setBatchOn();
    pig.registerQuery("A = LOAD '"+INPUT_FILE_NAME+"' USING PigStorage() AS (emp_id:int,emp_name:chararray,emp_start_date:chararray," +
        "emp_gender:chararray,emp_country:chararray,emp_state:chararray);");
    pig.registerQuery("IN = FILTER A BY emp_country == 'IN';");
    pig.registerQuery("STORE IN INTO 'employee' USING "+HCatStorer.class.getName()+"('emp_country=IN');");
    pig.executeBatch();
    driver.run("select * from employee");
    ArrayList<String> results = new ArrayList<String>();
    driver.getResults(results);
    assertEquals(4, results.size());
    Collections.sort(results);
    assertEquals(inputData[0], results.get(0));
    assertEquals(inputData[1], results.get(1));
    assertEquals(inputData[2], results.get(2));
    assertEquals(inputData[3], results.get(3));
    driver.run("drop table employee");
  }

  public void testDynamicPartitioningMultiPartColsInDataNoSpec() throws IOException, CommandNeedRetryException{

    driver.run("drop table if exists employee");
    String createTable = "CREATE TABLE employee (emp_id INT, emp_name STRING, emp_start_date STRING , emp_gender STRING ) " +
        " PARTITIONED BY (emp_country STRING , emp_state STRING ) STORED AS RCFILE";

    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    String[] inputData = {"111237\tKrishna\t01/01/1990\tM\tIN\tTN",
                          "111238\tKalpana\t01/01/2000\tF\tIN\tKA",
                          "111239\tSatya\t01/01/2001\tM\tIN\tKL",
                          "111240\tKavya\t01/01/2002\tF\tIN\tAP"};

    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, inputData);
    PigServer pig = new PigServer(ExecType.LOCAL);
    pig.setBatchOn();
    pig.registerQuery("A = LOAD '"+INPUT_FILE_NAME+"' USING PigStorage() AS (emp_id:int,emp_name:chararray,emp_start_date:chararray," +
        "emp_gender:chararray,emp_country:chararray,emp_state:chararray);");
    pig.registerQuery("IN = FILTER A BY emp_country == 'IN';");
    pig.registerQuery("STORE IN INTO 'employee' USING "+HCatStorer.class.getName()+"();");
    pig.executeBatch();
    driver.run("select * from employee");
    ArrayList<String> results = new ArrayList<String>();
    driver.getResults(results);
    assertEquals(4, results.size());
    Collections.sort(results);
    assertEquals(inputData[0], results.get(0));
    assertEquals(inputData[1], results.get(1));
    assertEquals(inputData[2], results.get(2));
    assertEquals(inputData[3], results.get(3));
    driver.run("drop table employee");
  }

    public void testDynamicPartitioningMultiPartColsNoDataInDataNoSpec() throws IOException, CommandNeedRetryException{

      driver.run("drop table if exists employee");
      String createTable = "CREATE TABLE employee (emp_id INT, emp_name STRING, emp_start_date STRING , emp_gender STRING ) " +
          " PARTITIONED BY (emp_country STRING , emp_state STRING ) STORED AS RCFILE";

      int retCode = driver.run(createTable).getResponseCode();
      if(retCode != 0) {
        throw new IOException("Failed to create table.");
      }

      String[] inputData = {};
      HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, inputData);

      PigServer pig = new PigServer(ExecType.LOCAL);
      pig.setBatchOn();
      pig.registerQuery("A = LOAD '"+INPUT_FILE_NAME+"' USING PigStorage() AS (emp_id:int,emp_name:chararray,emp_start_date:chararray," +
          "emp_gender:chararray,emp_country:chararray,emp_state:chararray);");
      pig.registerQuery("IN = FILTER A BY emp_country == 'IN';");
      pig.registerQuery("STORE IN INTO 'employee' USING "+HCatStorer.class.getName()+"();");
      pig.executeBatch();
      driver.run("select * from employee");
      ArrayList<String> results = new ArrayList<String>();
      driver.getResults(results);
      assertEquals(0, results.size());
      driver.run("drop table employee");
    }

    public void testPartitionPublish() throws IOException, CommandNeedRetryException{

        driver.run("drop table ptn_fail");
        String createTable = "create table ptn_fail(a int, c string) partitioned by (b string) stored as RCFILE";
        int retCode = driver.run(createTable).getResponseCode();
        if(retCode != 0) {
          throw new IOException("Failed to create table.");
        }
        int LOOP_SIZE = 11;
        String[] input = new String[LOOP_SIZE];

        for(int i = 0; i < LOOP_SIZE; i++) {
            input[i] = i + "\tmath";
        }
        HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
        PigServer server = new PigServer(ExecType.LOCAL);
        server.setBatchOn();
        server.registerQuery("A = load '"+ INPUT_FILE_NAME+"' as (a:int, c:chararray);");
        server.registerQuery("B = filter A by " + FailEvalFunc.class.getName() + "($0);");
        server.registerQuery("store B into 'ptn_fail' using "+HCatStorer.class.getName()+"('b=math');");
        server.executeBatch();

        String query = "show partitions ptn_fail";
        retCode = driver.run(query).getResponseCode();

        if( retCode != 0 ) {
          throw new IOException("Error " + retCode + " running query " + query);
        }

        ArrayList<String> res = new ArrayList<String>();
        driver.getResults(res);
        assertEquals(0, res.size());

        //Make sure the partitions directory is not in hdfs.
        Assert.assertTrue((new File(TEST_WAREHOUSE_DIR + "/ptn_fail")).exists());
        Assert.assertFalse((new File(TEST_WAREHOUSE_DIR + "/ptn_fail/b=math")).exists());
      }

    static public class FailEvalFunc extends EvalFunc<Boolean> {

        /* @param Tuple
        /* @return null
        /* @throws IOException
         * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
         */
        @Override
        public Boolean exec(Tuple tuple) throws IOException {
            throw new IOException("Eval Func to mimic Failure.");
        }

    }

}
