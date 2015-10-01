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

package org.apache.hadoop.hive.ql.security;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.HadoopShims.MiniDFSShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.junit.Before;
import org.junit.Test;

/**
 * This test the flag 'hive.warehouse.subdir.inherit.perms'.
 */
public abstract class FolderPermissionBase {
  protected static HiveConf conf;
  protected static Driver driver;
  protected static String dataFileDir;
  protected static Path dataFilePath;
  protected static FileSystem fs;

  protected static Path warehouseDir;
  protected static Path baseDfsDir;

  protected static final PathFilter hiddenFileFilter = new PathFilter(){
    public boolean accept(Path p){
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };


  public abstract void setPermission(String locn, int permIndex) throws Exception;

  public abstract void verifyPermission(String locn, int permIndex) throws Exception;


  public void setPermission(String locn) throws Exception {
    setPermission(locn, 0);
  }

  public void verifyPermission(String locn) throws Exception {
    verifyPermission(locn, 0);
  }


  public static void baseSetup() throws Exception {
    MiniDFSShim dfs = ShimLoader.getHadoopShims().getMiniDfs(conf, 4, true, null);
    fs = dfs.getFileSystem();
    baseDfsDir =  new Path(new Path(fs.getUri()), "/base");
    fs.mkdirs(baseDfsDir);
    warehouseDir = new Path(baseDfsDir, "warehouse");
    fs.mkdirs(warehouseDir);
    conf.setVar(ConfVars.METASTOREWAREHOUSE, warehouseDir.toString());

    // Assuming the tests are run either in C or D drive in Windows OS!
    dataFileDir = conf.get("test.data.files").replace('\\', '/')
        .replace("c:", "").replace("C:", "").replace("D:", "").replace("d:", "");
    dataFilePath = new Path(dataFileDir, "kv1.txt");

    // Set up scratch directory
    Path scratchDir = new Path(baseDfsDir, "scratchdir");
    conf.setVar(HiveConf.ConfVars.SCRATCHDIR, scratchDir.toString());

    //set hive conf vars
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS, true);
    conf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    int port = MetaStoreUtils.findFreePort();
    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge());

    SessionState.start(new CliSessionState(conf));
    driver = new Driver(conf);
    setupDataTable();
  }


  private static void setupDataTable() throws Exception {
    CommandProcessorResponse ret = driver.run("DROP TABLE IF EXISTS mysrc");
    Assert.assertEquals(0,ret.getResponseCode());

    ret = driver.run("CREATE TABLE mysrc (key STRING, value STRING) PARTITIONED BY (part1 string, part2 string) STORED AS TEXTFILE");
    Assert.assertEquals(0,ret.getResponseCode());

    ret = driver.run("LOAD DATA LOCAL INPATH '" + dataFilePath + "' INTO TABLE mysrc PARTITION (part1='1',part2='1')");
    Assert.assertEquals(0,ret.getResponseCode());

    ret = driver.run("LOAD DATA LOCAL INPATH '" + dataFilePath + "' INTO TABLE mysrc PARTITION (part1='2',part2='2')");
    Assert.assertEquals(0,ret.getResponseCode());
  }

  @Before
  public void setupBeforeTest() throws Exception {
    driver.run("USE default");
  }

  @Test
  public void testCreateDb() throws Exception {
    //see if db inherits permission from warehouse directory.
    String testDb = "mydb";
    String tableName = "createtable";

    setPermission(warehouseDir.toString());
    verifyPermission(warehouseDir.toString());

    CommandProcessorResponse ret = driver.run("CREATE DATABASE " + testDb);
    Assert.assertEquals(0,ret.getResponseCode());

    assertExistence(warehouseDir + "/" + testDb + ".db");
    verifyPermission(warehouseDir + "/" + testDb + ".db");

    ret = driver.run("USE " + testDb);
    Assert.assertEquals(0,ret.getResponseCode());

    ret = driver.run("CREATE TABLE " + tableName + " (key string, value string)");
    Assert.assertEquals(0,ret.getResponseCode());

    verifyPermission(warehouseDir + "/" + testDb + ".db/" + tableName);

    ret = driver.run("insert into table " + tableName + " select key,value from default.mysrc");
    Assert.assertEquals(0,ret.getResponseCode());

    assertExistence(warehouseDir + "/" + testDb + ".db/" + tableName);
    verifyPermission(warehouseDir + "/" + testDb + ".db/" + tableName);

    Assert.assertTrue(listStatus(warehouseDir + "/" + testDb + ".db/" + tableName).size() > 0);
    for (String child : listStatus(warehouseDir + "/" + testDb + ".db/" + tableName)) {
      verifyPermission(child);
    }

    ret = driver.run("USE default");
    Assert.assertEquals(0,ret.getResponseCode());

    //cleanup after the test.
    fs.delete(warehouseDir, true);
    fs.mkdirs(warehouseDir);
    Assert.assertEquals(listStatus(warehouseDir.toString()).size(), 0);
    setupDataTable();
  }

  @Test
  public void testCreateTable() throws Exception {
    String testDb = "mydb2";
    String tableName = "createtable";
    CommandProcessorResponse ret = driver.run("CREATE DATABASE " + testDb);
    Assert.assertEquals(0,ret.getResponseCode());

    assertExistence(warehouseDir + "/" + testDb + ".db");
    setPermission(warehouseDir + "/" + testDb + ".db");
    verifyPermission(warehouseDir + "/" + testDb + ".db");

    ret = driver.run("USE " + testDb);
    Assert.assertEquals(0,ret.getResponseCode());

    ret = driver.run("CREATE TABLE " + tableName + " (key string, value string)");
    Assert.assertEquals(0,ret.getResponseCode());

    verifyPermission(warehouseDir + "/" + testDb + ".db/" + tableName);

    ret = driver.run("insert into table " + tableName + " select key,value from default.mysrc");
    Assert.assertEquals(0,ret.getResponseCode());

    assertExistence(warehouseDir + "/" + testDb + ".db/" + tableName);
    verifyPermission(warehouseDir + "/" + testDb + ".db/" + tableName);

    Assert.assertTrue(listStatus(warehouseDir + "/" + testDb + ".db/" + tableName).size() > 0);
    for (String child : listStatus(warehouseDir + "/" + testDb + ".db/" + tableName)) {
      verifyPermission(child);
    }

    ret = driver.run("USE default");
    Assert.assertEquals(0,ret.getResponseCode());
  }


  @Test
  public void testInsertNonPartTable() throws Exception {
    //case 1 is non-partitioned table.
    String tableName = "nonpart";

    CommandProcessorResponse ret = driver.run("CREATE TABLE " + tableName + " (key string, value string)");
    Assert.assertEquals(0, ret.getResponseCode());

    String tableLoc = warehouseDir + "/" + tableName;
    assertExistence(warehouseDir + "/" + tableName);

    //case1A: insert into non-partitioned table.
    setPermission(warehouseDir + "/" + tableName);
    ret = driver.run("insert into table " + tableName + " select key,value from mysrc");
    Assert.assertEquals(0, ret.getResponseCode());

    verifyPermission(warehouseDir + "/" + tableName);
    Assert.assertTrue(listStatus(tableLoc).size() > 0);
    for (String child : listStatus(tableLoc)) {
      verifyPermission(child);
    }

    //case1B: insert overwrite non-partitioned-table
    setPermission(warehouseDir + "/" + tableName, 1);
    ret = driver.run("insert overwrite table " + tableName + " select key,value from mysrc");
    Assert.assertEquals(0, ret.getResponseCode());

    verifyPermission(warehouseDir + "/" + tableName, 1);
    Assert.assertTrue(listStatus(tableLoc).size() > 0);
    for (String child : listStatus(tableLoc)) {
      verifyPermission(child, 1);
    }
  }

  @Test
  public void testInsertStaticSinglePartition() throws Exception {
    String tableName = "singlestaticpart";
    CommandProcessorResponse ret = driver.run("CREATE TABLE " + tableName + " (key string, value string) partitioned by (part1 string)");
    Assert.assertEquals(0, ret.getResponseCode());

    assertExistence(warehouseDir + "/" + tableName);
    setPermission(warehouseDir + "/" + tableName);

    //insert into test
    ret = driver.run("insert into table " + tableName + " partition(part1='1') select key,value from mysrc where part1='1' and part2='1'");
    Assert.assertEquals(0, ret.getResponseCode());

    verifyPermission(warehouseDir + "/" + tableName);
    verifyPermission(warehouseDir + "/" + tableName + "/part1=1");

    Assert.assertTrue(listStatus(warehouseDir + "/" + tableName + "/part1=1").size() > 0);
    for (String child : listStatus(warehouseDir + "/" + tableName + "/part1=1")) {
      verifyPermission(child);
    }

    //insert overwrite test
    setPermission(warehouseDir + "/" + tableName, 1);
    setPermission(warehouseDir + "/" + tableName + "/part1=1", 1);
    ret = driver.run("insert overwrite table " + tableName + " partition(part1='1') select key,value from mysrc where part1='1' and part2='1'");
    Assert.assertEquals(0, ret.getResponseCode());

    verifyPermission(warehouseDir + "/" + tableName, 1);
    verifyPermission(warehouseDir + "/" + tableName + "/part1=1", 1);

    Assert.assertTrue(listStatus(warehouseDir + "/" + tableName + "/part1=1").size() > 0);
    for (String child : listStatus(warehouseDir + "/" + tableName + "/part1=1")) {
      verifyPermission(child, 1);
    }
  }

  @Test
  public void testInsertStaticDualPartition() throws Exception {
    String tableName = "dualstaticpart";
    CommandProcessorResponse ret = driver.run("CREATE TABLE " + tableName + " (key string, value string) partitioned by (part1 string, part2 string)");
    Assert.assertEquals(0, ret.getResponseCode());

    assertExistence(warehouseDir + "/" + tableName);
    setPermission(warehouseDir + "/" + tableName);

    //insert into test
    ret = driver.run("insert into table " + tableName + " partition(part1='1', part2='1') select key,value from mysrc where part1='1' and part2='1'");
    Assert.assertEquals(0, ret.getResponseCode());

    verifyPermission(warehouseDir + "/" + tableName);
    verifyPermission(warehouseDir + "/" + tableName + "/part1=1");
    verifyPermission(warehouseDir + "/" + tableName + "/part1=1/part2=1");

    Assert.assertTrue(listStatus(warehouseDir + "/" + tableName + "/part1=1/part2=1").size() > 0);
    for (String child : listStatus(warehouseDir + "/" + tableName + "/part1=1/part2=1")) {
      verifyPermission(child);
    }

    //insert overwrite test
    setPermission(warehouseDir + "/" + tableName, 1);
    setPermission(warehouseDir + "/" + tableName + "/part1=1", 1);
    setPermission(warehouseDir + "/" + tableName + "/part1=1/part2=1", 1);

    ret = driver.run("insert overwrite table " + tableName + " partition(part1='1', part2='1') select key,value from mysrc where part1='1' and part2='1'");
    Assert.assertEquals(0, ret.getResponseCode());

    verifyPermission(warehouseDir + "/" + tableName, 1);
    verifyPermission(warehouseDir + "/" + tableName + "/part1=1", 1);
    verifyPermission(warehouseDir + "/" + tableName + "/part1=1/part2=1", 1);

    Assert.assertTrue(listStatus(warehouseDir + "/" + tableName + "/part1=1/part2=1").size() > 0);
    for (String child : listStatus(warehouseDir + "/" + tableName + "/part1=1/part2=1")) {
      verifyPermission(child, 1);
    }
  }

  @Test
  public void testInsertDualDynamicPartitions() throws Exception {
    String tableName = "dualdynamicpart";

    CommandProcessorResponse ret = driver.run("CREATE TABLE " + tableName + " (key string, value string) partitioned by (part1 string, part2 string)");
    Assert.assertEquals(0, ret.getResponseCode());
    assertExistence(warehouseDir + "/" + tableName);

    //Insert into test, with permission set 0.
    setPermission(warehouseDir + "/" + tableName, 0);
    ret = driver.run("insert into table " + tableName + " partition (part1,part2) select key,value,part1,part2 from mysrc");
    Assert.assertEquals(0, ret.getResponseCode());

    verifyDualPartitionTable(warehouseDir + "/" + tableName, 0);

    //Insert overwrite test, with permission set 1.  We need reset existing partitions to 1 since the permissions
    //should be inherited from existing partition
    setDualPartitionTable(warehouseDir + "/" + tableName, 1);
    ret = driver.run("insert overwrite table " + tableName + " partition (part1,part2) select key,value,part1,part2 from mysrc");
    Assert.assertEquals(0, ret.getResponseCode());

    verifyDualPartitionTable(warehouseDir + "/" + tableName, 1);
  }

  @Test
  public void testInsertSingleDynamicPartition() throws Exception {
    String tableName = "singledynamicpart";

    CommandProcessorResponse ret = driver.run("CREATE TABLE " + tableName + " (key string, value string) partitioned by (part1 string)");
    Assert.assertEquals(0,ret.getResponseCode());
    String tableLoc = warehouseDir + "/" + tableName;
    assertExistence(tableLoc);

    //Insert into test, with permission set 0.
    setPermission(tableLoc, 0);
    ret = driver.run("insert into table " + tableName + " partition (part1) select key,value,part1 from mysrc");
    Assert.assertEquals(0,ret.getResponseCode());
    verifySinglePartition(tableLoc, 0);

    //Insert overwrite test, with permission set 1. We need reset existing partitions to 1 since the permissions
    //should be inherited from existing partition
    setSinglePartition(tableLoc, 1);
    ret = driver.run("insert overwrite table " + tableName + " partition (part1) select key,value,part1 from mysrc");
    Assert.assertEquals(0,ret.getResponseCode());
    verifySinglePartition(tableLoc, 1);

    //delete and re-insert using insert overwrite.  There's different code paths insert vs insert overwrite for new tables.
    ret = driver.run("DROP TABLE " + tableName);
    Assert.assertEquals(0, ret.getResponseCode());
    ret = driver.run("CREATE TABLE " + tableName + " (key string, value string) partitioned by (part1 string)");
    Assert.assertEquals(0, ret.getResponseCode());

    assertExistence(warehouseDir + "/" + tableName);
    setPermission(warehouseDir + "/" + tableName);

    ret = driver.run("insert overwrite table " + tableName + " partition (part1) select key,value,part1 from mysrc");
    Assert.assertEquals(0, ret.getResponseCode());

    verifySinglePartition(tableLoc, 0);
  }

  @Test
  public void testPartition() throws Exception {
    String tableName = "alterpart";
    CommandProcessorResponse ret = driver.run("CREATE TABLE " + tableName + " (key string, value string) partitioned by (part1 int, part2 int, part3 int)");
    Assert.assertEquals(0,ret.getResponseCode());

    assertExistence(warehouseDir + "/" + tableName);
    setPermission(warehouseDir + "/" + tableName);

    ret = driver.run("insert into table " + tableName + " partition(part1='1',part2='1',part3='1') select key,value from mysrc");
    Assert.assertEquals(0,ret.getResponseCode());

    assertExistence(warehouseDir + "/" + tableName);
    setPermission(warehouseDir + "/" + tableName, 1);

    //alter partition
    ret = driver.run("alter table " + tableName + " partition (part1='1',part2='1',part3='1') rename to partition (part1='2',part2='2',part3='2')");
    Assert.assertEquals(0,ret.getResponseCode());

    verifyPermission(warehouseDir + "/" + tableName + "/part1=2", 1);
    verifyPermission(warehouseDir + "/" + tableName + "/part1=2/part2=2", 1);
    verifyPermission(warehouseDir + "/" + tableName + "/part1=2/part2=2/part3=2", 1);

    Assert.assertTrue(listStatus(warehouseDir + "/" + tableName + "/part1=2/part2=2/part3=2").size() > 0);
    for (String child : listStatus(warehouseDir + "/" + tableName + "/part1=2/part2=2/part3=2")) {
      verifyPermission(child, 1);
    }

    String tableName2 = "alterpart2";
    ret = driver.run("CREATE TABLE " + tableName2 + " (key string, value string) partitioned by (part1 int, part2 int, part3 int)");
    Assert.assertEquals(0,ret.getResponseCode());

    assertExistence(warehouseDir + "/" + tableName2);
    setPermission(warehouseDir + "/" + tableName2);
    ret = driver.run("alter table " + tableName2 + " exchange partition (part1='2',part2='2',part3='2') with table " + tableName);
    Assert.assertEquals(0,ret.getResponseCode());

    //alter exchange can not change base table's permission
    //alter exchange can only control final partition folder's permission
    verifyPermission(warehouseDir + "/" + tableName2 + "/part1=2", 0);
    verifyPermission(warehouseDir + "/" + tableName2 + "/part1=2/part2=2", 0);
    verifyPermission(warehouseDir + "/" + tableName2 + "/part1=2/part2=2/part3=2", 1);
  }

  @Test
  public void testExternalTable() throws Exception {
    String tableName = "externaltable";

    String myLocation = warehouseDir + "/myfolder";
    FileSystem fs = FileSystem.get(new URI(myLocation), conf);
    fs.mkdirs(new Path(myLocation));
    setPermission(myLocation);

    CommandProcessorResponse ret = driver.run("CREATE TABLE " + tableName + " (key string, value string) LOCATION '" + myLocation + "'");
    Assert.assertEquals(0,ret.getResponseCode());

    ret = driver.run("insert into table " + tableName + " select key,value from mysrc");
    Assert.assertEquals(0,ret.getResponseCode());

    Assert.assertTrue(listStatus(myLocation).size() > 0);
    for (String child : listStatus(myLocation)) {
      verifyPermission(child);
    }
  }

  @Test
  public void testLoadLocal() throws Exception {
    //case 1 is non-partitioned table.
    String tableName = "loadlocal";

    CommandProcessorResponse ret = driver.run("CREATE TABLE " + tableName + " (key string, value string)");
    Assert.assertEquals(0,ret.getResponseCode());

    String tableLoc = warehouseDir + "/" + tableName;
    assertExistence(warehouseDir + "/" + tableName);

    //case1A: load data local into non-partitioned table.
    setPermission(warehouseDir + "/" + tableName);

    ret = driver.run("load data local inpath '" + dataFilePath + "' into table " + tableName);
    Assert.assertEquals(0,ret.getResponseCode());

    Assert.assertTrue(listStatus(tableLoc).size() > 0);
    for (String child : listStatus(tableLoc)) {
      verifyPermission(child);
    }

    //case1B: load data local into overwrite non-partitioned-table
    setPermission(warehouseDir + "/" + tableName, 1);
    for (String child : listStatus(tableLoc)) {
      setPermission(child, 1);
    }
    ret = driver.run("load data local inpath '" + dataFilePath + "' overwrite into table " + tableName);
    Assert.assertEquals(0,ret.getResponseCode());

    Assert.assertTrue(listStatus(tableLoc).size() > 0);
    for (String child : listStatus(tableLoc)) {
      verifyPermission(child, 1);
    }

    //case 2 is partitioned table.
    tableName = "loadlocalpartition";

    ret = driver.run("CREATE TABLE " + tableName + " (key string, value string) partitioned by (part1 int, part2 int)");
    Assert.assertEquals(0,ret.getResponseCode());
    tableLoc = warehouseDir + "/" + tableName;
    assertExistence(tableLoc);

    //case 2A: load data local into partitioned table.
    setPermission(tableLoc);
    ret = driver.run("LOAD DATA LOCAL INPATH '" + dataFilePath + "' INTO TABLE " + tableName + " PARTITION (part1='1',part2='1')");
    Assert.assertEquals(0,ret.getResponseCode());

    String partLoc = warehouseDir + "/" + tableName + "/part1=1/part2=1";
    Assert.assertTrue(listStatus(partLoc).size() > 0);
    for (String child : listStatus(partLoc)) {
      verifyPermission(child);
    }

    //case 2B: insert data overwrite into partitioned table. set testing table/partition folder hierarchy 1
    //local load overwrite just overwrite the existing partition content but not the permission
    setPermission(tableLoc, 1);
    setPermission(partLoc, 1);
    for (String child : listStatus(partLoc)) {
      setPermission(child, 1);
    }
    ret = driver.run("LOAD DATA LOCAL INPATH '" + dataFilePath + "' OVERWRITE INTO TABLE " + tableName + " PARTITION (part1='1',part2='1')");
    Assert.assertEquals(0,ret.getResponseCode());

    Assert.assertTrue(listStatus(tableLoc).size() > 0);
    for (String child : listStatus(partLoc)) {
      verifyPermission(child, 1);
    }
  }

  @Test
  public void testLoad() throws Exception {
    String tableName = "load";
    String location = "/hdfsPath";
    fs.copyFromLocalFile(dataFilePath, new Path(location));

    //case 1: load data
    CommandProcessorResponse ret = driver.run("CREATE TABLE " + tableName + " (key string, value string)");
    Assert.assertEquals(0,ret.getResponseCode());
    String tableLoc = warehouseDir + "/" + tableName;
    assertExistence(warehouseDir + "/" + tableName);

    //case1A: load data into non-partitioned table.
    setPermission(warehouseDir + "/" + tableName);

    ret = driver.run("load data inpath '" + location + "' into table " + tableName);
    Assert.assertEquals(0,ret.getResponseCode());

    Assert.assertTrue(listStatus(tableLoc).size() > 0);
    for (String child : listStatus(tableLoc)) {
      verifyPermission(child);
    }

    //case1B: load data into overwrite non-partitioned-table
    setPermission(warehouseDir + "/" + tableName, 1);
    for (String child : listStatus(tableLoc)) {
      setPermission(child, 1);
    }

    fs.copyFromLocalFile(dataFilePath, new Path(location));
    ret = driver.run("load data inpath '" + location + "' overwrite into table " + tableName);
    Assert.assertEquals(0,ret.getResponseCode());

    Assert.assertTrue(listStatus(tableLoc).size() > 0);
    for (String child : listStatus(tableLoc)) {
      verifyPermission(child, 1);
    }

    //case 2 is partitioned table.
    tableName = "loadpartition";

    ret = driver.run("CREATE TABLE " + tableName + " (key string, value string) partitioned by (part1 int, part2 int)");
    Assert.assertEquals(0,ret.getResponseCode());
    tableLoc = warehouseDir + "/" + tableName;
    assertExistence(tableLoc);

    //case 2A: load data into partitioned table.
    setPermission(tableLoc);
    fs.copyFromLocalFile(dataFilePath, new Path(location));
    ret = driver.run("LOAD DATA INPATH '" + location + "' INTO TABLE " + tableName + " PARTITION (part1='1',part2='1')");
    Assert.assertEquals(0,ret.getResponseCode());

    String partLoc = warehouseDir + "/" + tableName + "/part1=1/part2=1";
    Assert.assertTrue(listStatus(partLoc).size() > 0);
    for (String child : listStatus(partLoc)) {
      verifyPermission(child);
    }

    //case 2B: insert data overwrite into partitioned table. set testing table/partition folder hierarchy 1
    //load overwrite just overwrite the existing partition content but not the permission
    setPermission(tableLoc, 1);
    setPermission(partLoc, 1);
    Assert.assertTrue(listStatus(partLoc).size() > 0);
    for (String child : listStatus(partLoc)) {
      setPermission(child, 1);
    }

    fs.copyFromLocalFile(dataFilePath, new Path(location));
    ret = driver.run("LOAD DATA INPATH '" + location + "' OVERWRITE INTO TABLE " + tableName + " PARTITION (part1='1',part2='1')");
    Assert.assertEquals(0,ret.getResponseCode());

    Assert.assertTrue(listStatus(tableLoc).size() > 0);
    for (String child : listStatus(partLoc)) {
      verifyPermission(child, 1);
    }
  }

  @Test
  public void testCtas() throws Exception {
    String testDb = "ctasdb";
    String tableName = "createtable";
    CommandProcessorResponse ret = driver.run("CREATE DATABASE " + testDb);
    Assert.assertEquals(0,ret.getResponseCode());

    assertExistence(warehouseDir + "/" + testDb + ".db");
    setPermission(warehouseDir + "/" + testDb + ".db");
    verifyPermission(warehouseDir + "/" + testDb + ".db");

    ret = driver.run("USE " + testDb);
    Assert.assertEquals(0,ret.getResponseCode());

    ret = driver.run("create table " + tableName + " as select key,value from default.mysrc");
    Assert.assertEquals(0,ret.getResponseCode());

    assertExistence(warehouseDir + "/" + testDb + ".db/" + tableName);
    verifyPermission(warehouseDir + "/" + testDb + ".db/" + tableName);

    Assert.assertTrue(listStatus(warehouseDir + "/" + testDb + ".db/" + tableName).size() > 0);
    for (String child : listStatus(warehouseDir + "/" + testDb + ".db/" + tableName)) {
      verifyPermission(child);
    }

    ret = driver.run("USE default");
    Assert.assertEquals(0,ret.getResponseCode());
  }

  @Test
  public void testExim() throws Exception {

    //export the table to external file.
    String myLocation = warehouseDir + "/exim";
    FileSystem fs = FileSystem.get(new URI(myLocation), conf);
    fs.mkdirs(new Path(myLocation));
    setPermission(myLocation);
    myLocation = myLocation + "/temp";

    CommandProcessorResponse ret = driver.run("export table mysrc to '" + myLocation + "'");
    Assert.assertEquals(0,ret.getResponseCode());

    //check if exported data has inherited the permissions.
    assertExistence(myLocation);
    verifyPermission(myLocation);

    assertExistence(myLocation + "/part1=1/part2=1");
    verifyPermission(myLocation + "/part1=1/part2=1");
    Assert.assertTrue(listStatus(myLocation + "/part1=1/part2=1").size() > 0);
    for (String child : listStatus(myLocation + "/part1=1/part2=1")) {
      verifyPermission(child);
    }

    assertExistence(myLocation + "/part1=2/part2=2");
    verifyPermission(myLocation + "/part1=2/part2=2");
    Assert.assertTrue(listStatus(myLocation + "/part1=2/part2=2").size() > 0);
    for (String child : listStatus(myLocation + "/part1=2/part2=2")) {
      verifyPermission(child);
    }

    //import the table back into another database
    String testDb = "eximdb";
    ret = driver.run("CREATE DATABASE " + testDb);
    Assert.assertEquals(0,ret.getResponseCode());

    //use another permission for this import location, to verify that it is really set (permIndex=2)
    assertExistence(warehouseDir + "/" + testDb + ".db");
    setPermission(warehouseDir + "/" + testDb + ".db", 1);

    ret = driver.run("USE " + testDb);
    Assert.assertEquals(0,ret.getResponseCode());

    ret = driver.run("import from '" + myLocation + "'");
    Assert.assertEquals(0,ret.getResponseCode());

    //check permissions of imported, from the exported table
    assertExistence(warehouseDir + "/" + testDb + ".db/mysrc");
    verifyPermission(warehouseDir + "/" + testDb + ".db/mysrc", 1);

    myLocation = warehouseDir + "/" + testDb + ".db/mysrc";
    assertExistence(myLocation);
    verifyPermission(myLocation, 1);

    assertExistence(myLocation + "/part1=1/part2=1");
    verifyPermission(myLocation + "/part1=1/part2=1", 1);
    Assert.assertTrue(listStatus(myLocation + "/part1=1/part2=1").size() > 0);
    for (String child : listStatus(myLocation + "/part1=1/part2=1")) {
      verifyPermission(child, 1);
    }

    assertExistence(myLocation + "/part1=2/part2=2");
    verifyPermission(myLocation + "/part1=2/part2=2", 1);
    Assert.assertTrue(listStatus(myLocation + "/part1=2/part2=2").size() > 0);
    for (String child : listStatus(myLocation + "/part1=2/part2=2")) {
      verifyPermission(child, 1);
    }
  }

  /**
   * Tests the permission to the table doesn't change after the truncation
   * @throws Exception
   */
  @Test
  public void testTruncateTable() throws Exception {
    String tableName = "truncatetable";
    String partition = warehouseDir + "/" + tableName + "/part1=1";

    CommandProcessorResponse ret = driver.run("CREATE TABLE " + tableName + " (key STRING, value STRING) PARTITIONED BY (part1 INT)");
    Assert.assertEquals(0, ret.getResponseCode());

    setPermission(warehouseDir + "/" + tableName);

    ret = driver.run("insert into table " + tableName + " partition(part1='1') select key,value from mysrc where part1='1' and part2='1'");
    Assert.assertEquals(0, ret.getResponseCode());

    assertExistence(warehouseDir + "/" + tableName);

    verifyPermission(warehouseDir + "/" + tableName);
    verifyPermission(partition);

    ret = driver.run("TRUNCATE TABLE " + tableName);
    Assert.assertEquals(0, ret.getResponseCode());

    ret = driver.run("insert into table " + tableName + " partition(part1='1') select key,value from mysrc where part1='1' and part2='1'");
    Assert.assertEquals(0, ret.getResponseCode());

    verifyPermission(warehouseDir + "/" + tableName);

    assertExistence(partition);
    verifyPermission(partition);    
  }

  private void setSinglePartition(String tableLoc, int index) throws Exception {
    setPermission(tableLoc + "/part1=1", index);
    setPermission(tableLoc + "/part1=2", index);
  }

  private void verifySinglePartition(String tableLoc, int index) throws Exception {
    verifyPermission(tableLoc + "/part1=1", index);
    verifyPermission(tableLoc + "/part1=2", index);

    Assert.assertTrue(listStatus(tableLoc + "/part1=1").size() > 0);
    for (String child : listStatus(tableLoc + "/part1=1")) {
      verifyPermission(child, index);
    }

    Assert.assertTrue(listStatus(tableLoc + "/part1=2").size() > 0);
    for (String child : listStatus(tableLoc + "/part1=2")) {
      verifyPermission(child, index);
    }
  }

  private void setDualPartitionTable(String baseTablePath, int index) throws Exception {
    setPermission(baseTablePath, index);
    setPermission(baseTablePath + "/part1=1", index);
    setPermission(baseTablePath + "/part1=1/part2=1", index);

    setPermission(baseTablePath + "/part1=2", index);
    setPermission(baseTablePath + "/part1=2/part2=2", index);
  }

  private void verifyDualPartitionTable(String baseTablePath, int index) throws Exception {
    verifyPermission(baseTablePath, index);
    verifyPermission(baseTablePath + "/part1=1", index);
    verifyPermission(baseTablePath + "/part1=1/part2=1", index);

    verifyPermission(baseTablePath + "/part1=2", index);
    verifyPermission(baseTablePath + "/part1=2/part2=2", index);

    Assert.assertTrue(listStatus(baseTablePath + "/part1=1/part2=1").size() > 0);
    for (String child : listStatus(baseTablePath + "/part1=1/part2=1")) {
      verifyPermission(child, index);
    }

    Assert.assertTrue(listStatus(baseTablePath + "/part1=2/part2=2").size() > 0);
    for (String child : listStatus(baseTablePath + "/part1=2/part2=2")) {
      verifyPermission(child, index);
    }
  }

  private void assertExistence(String locn) throws Exception {
    Assert.assertTrue(fs.exists(new Path(locn)));
  }

  private List<String> listStatus(String locn) throws Exception {
    List<String> results = new ArrayList<String>();
    FileStatus[] listStatus = fs.listStatus(new Path(locn), hiddenFileFilter);
    for (FileStatus status : listStatus) {
      results.add(status.getPath().toString());
    }
    return results;
  }
}
