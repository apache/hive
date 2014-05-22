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

import junit.framework.Assert;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test the flag 'hive.warehouse.subdir.inherit.perms'.
 */
public class TestFolderPermissions {
  protected static HiveConf conf;
  protected static Driver driver;
  protected static String dataFileDir;
  protected static Path dataFilePath;
  protected static String testDir;
  protected static FileSystem fs;

  public static final PathFilter hiddenFileFilter = new PathFilter(){
    public boolean accept(Path p){
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };



  @BeforeClass
  public static void setUp() throws Exception {
    testDir = System.getProperty("test.warehouse.dir");

    conf = new HiveConf(TestFolderPermissions.class);
    fs = FileSystem.get(new URI(testDir), conf);
    dataFileDir = conf.get("test.data.files").replace('\\', '/')
        .replace("c:", "");
    dataFilePath = new Path(dataFileDir, "kv1.txt");

    int port = MetaStoreUtils.findFreePort();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS, true);
    conf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");

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

  @Test
  public void testCreateTablePerms() throws Exception {
    String testDb = "mydb";
    String tableName = "createtable";
    CommandProcessorResponse ret = driver.run("CREATE DATABASE " + testDb);
    Assert.assertEquals(0,ret.getResponseCode());

    assertExistence(testDir + "/" + testDb + ".db");
    setPermissions(testDir + "/" + testDb + ".db", FsPermission.createImmutable((short) 0777));

    ret = driver.run("USE " + testDb);
    Assert.assertEquals(0,ret.getResponseCode());

    ret = driver.run("CREATE TABLE " + tableName + " (key string, value string)");
    Assert.assertEquals(0,ret.getResponseCode());

    ret = driver.run("insert into table " + tableName + " select key,value from default.mysrc");

    assertExistence(testDir + "/" + testDb + ".db/" + tableName);
    Assert.assertEquals("rwxrwxrwx", getPermissions(testDir + "/" + testDb + ".db/" + tableName).toString());

    ret = driver.run("USE default");
    Assert.assertEquals(0,ret.getResponseCode());
  }


  @Test
  public void testStaticPartitionPerms() throws Exception {
    String tableName = "staticpart";
    CommandProcessorResponse ret = driver.run("CREATE TABLE " + tableName + " (key string, value string) partitioned by (part1 string, part2 string)");
    Assert.assertEquals(0,ret.getResponseCode());

    assertExistence(testDir + "/" + tableName);
    setPermissions(testDir + "/" + tableName, FsPermission.createImmutable((short) 0777));


    ret = driver.run("insert into table " + tableName + " partition(part1='1', part2='1') select key,value from mysrc where part1='1' and part2='1'");
    Assert.assertEquals(0,ret.getResponseCode());

    Assert.assertEquals("rwxrwxrwx", getPermissions(testDir + "/" + tableName + "/part1=1").toString());
    Assert.assertEquals("rwxrwxrwx", getPermissions(testDir + "/" + tableName + "/part1=1/part2=1").toString());

    Assert.assertTrue(listChildrenPerms(testDir + "/" + tableName + "/part1=1/part2=1").size() > 0);
    for (FsPermission perm : listChildrenPerms(testDir + "/" + tableName + "/part1=1/part2=1")) {
      Assert.assertEquals("rwxrwxrwx", perm.toString());
    }
  }

  @Test
  public void testAlterPartitionPerms() throws Exception {
    String tableName = "alterpart";
    CommandProcessorResponse ret = driver.run("CREATE TABLE " + tableName + " (key string, value string) partitioned by (part1 int, part2 int, part3 int)");
    Assert.assertEquals(0,ret.getResponseCode());

    assertExistence(testDir + "/" + tableName);
    setPermissions(testDir + "/" + tableName, FsPermission.createImmutable((short) 0777));

    ret = driver.run("insert into table " + tableName + " partition(part1='1',part2='1',part3='1') select key,value from mysrc");
    Assert.assertEquals(0,ret.getResponseCode());

    //alter partition
    ret = driver.run("alter table " + tableName + " partition (part1='1',part2='1',part3='1') rename to partition (part1='2',part2='2',part3='2')");
    Assert.assertEquals(0,ret.getResponseCode());

    Assert.assertEquals("rwxrwxrwx", getPermissions(testDir + "/" + tableName + "/part1=2").toString());
    Assert.assertEquals("rwxrwxrwx", getPermissions(testDir + "/" + tableName + "/part1=2/part2=2").toString());
    Assert.assertEquals("rwxrwxrwx", getPermissions(testDir + "/" + tableName + "/part1=2/part2=2/part3=2").toString());

    Assert.assertTrue(listChildrenPerms(testDir + "/" + tableName + "/part1=2/part2=2/part3=2").size() > 0);
    for (FsPermission perm : listChildrenPerms(testDir + "/" + tableName + "/part1=2/part2=2/part3=2")) {
      Assert.assertEquals("rwxrwxrwx", perm.toString());
    }
  }


  @Test
  public void testDynamicPartitions() throws Exception {
    String tableName = "dynamicpart";

    CommandProcessorResponse ret = driver.run("CREATE TABLE " + tableName + " (key string, value string) partitioned by (part1 string, part2 string)");
    Assert.assertEquals(0,ret.getResponseCode());

    assertExistence(testDir + "/" + tableName);
    setPermissions(testDir + "/" + tableName, FsPermission.createImmutable((short) 0777));

    ret = driver.run("insert into table " + tableName + " partition (part1,part2) select key,value,part1,part2 from mysrc");
    Assert.assertEquals(0,ret.getResponseCode());

    Assert.assertEquals("rwxrwxrwx", getPermissions(testDir + "/" + tableName + "/part1=1").toString());
    Assert.assertEquals("rwxrwxrwx", getPermissions(testDir + "/" + tableName + "/part1=1/part2=1").toString());

    Assert.assertEquals("rwxrwxrwx", getPermissions(testDir + "/" + tableName + "/part1=2").toString());
    Assert.assertEquals("rwxrwxrwx", getPermissions(testDir + "/" + tableName + "/part1=2/part2=2").toString());

    Assert.assertTrue(listChildrenPerms(testDir + "/" + tableName + "/part1=1/part2=1").size() > 0);
    for (FsPermission perm : listChildrenPerms(testDir + "/" + tableName + "/part1=1/part2=1")) {
      Assert.assertEquals("rwxrwxrwx", perm.toString());
    }

    Assert.assertTrue(listChildrenPerms(testDir + "/" + tableName + "/part1=2/part2=2").size() > 0);
    for (FsPermission perm : listChildrenPerms(testDir + "/" + tableName + "/part1=2/part2=2")) {
      Assert.assertEquals("rwxrwxrwx", perm.toString());
    }
  }

  @Test
  public void testExternalTable() throws Exception {
    String tableName = "externaltable";

    String myLocation = testDir + "/myfolder";
    FileSystem fs = FileSystem.get(new URI(myLocation), conf);
    fs.mkdirs(new Path(myLocation), FsPermission.createImmutable((short) 0777));

    CommandProcessorResponse ret = driver.run("CREATE TABLE " + tableName + " (key string, value string) LOCATION '" + myLocation + "'");
    Assert.assertEquals(0,ret.getResponseCode());

    ret = driver.run("insert into table " + tableName + " select key,value from mysrc");
    Assert.assertEquals(0,ret.getResponseCode());

    Assert.assertTrue(listChildrenPerms(myLocation).size() > 0);
    for (FsPermission perm : listChildrenPerms(myLocation)) {
      Assert.assertEquals("rwxrwxrwx", perm.toString());
    }
  }

  @Test
  public void testInsertOverwrite() throws Exception {
    //case 1 is non-partitioned table.
    String tableName = "insertoverwrite";

    CommandProcessorResponse ret = driver.run("CREATE TABLE " + tableName + " (key string, value string)");
    Assert.assertEquals(0,ret.getResponseCode());

    String tableLoc = testDir + "/" + tableName;
    assertExistence(testDir + "/" + tableName);
    setPermissions(testDir + "/" + tableName, FsPermission.createImmutable((short) 0777));

    ret = driver.run("insert overwrite table " + tableName + " select key,value from mysrc");
    Assert.assertEquals(0,ret.getResponseCode());

    Assert.assertTrue(listChildrenPerms(tableLoc).size() > 0);
    for (FsPermission perm : listChildrenPerms(tableLoc)) {
      Assert.assertEquals("rwxrwxrwx", perm.toString());
    }

    //case 2 is partitioned table.
    tableName = "insertoverwritepartition";

    ret = driver.run("CREATE TABLE " + tableName + " (key string, value string) partitioned by (part1 int, part2 int)");
    Assert.assertEquals(0,ret.getResponseCode());

    ret = driver.run("insert overwrite table " + tableName + " partition(part1='1',part2='1') select key,value from mysrc");
    Assert.assertEquals(0,ret.getResponseCode());

    String partLoc = testDir + "/" + tableName + "/part1=1/part2=1";
    assertExistence(partLoc);
    setPermissions(partLoc, FsPermission.createImmutable((short) 0777));

    ret = driver.run("insert overwrite table " + tableName + " partition(part1='1',part2='1') select key,value from mysrc");
    Assert.assertEquals(0,ret.getResponseCode());

    Assert.assertTrue(listChildrenPerms(tableLoc).size() > 0);
    for (FsPermission perm : listChildrenPerms(tableLoc)) {
      Assert.assertEquals("rwxrwxrwx", perm.toString());
    }
  }

  @Test
  public void testEximPermissionInheritance() throws Exception {

    //export the table to external file.
    String myLocation = testDir + "/exim";
    FileSystem fs = FileSystem.get(new URI(myLocation), conf);
    fs.mkdirs(new Path(myLocation), FsPermission.createImmutable((short) 0777));

    myLocation = myLocation + "/temp";

    CommandProcessorResponse ret = driver.run("export table mysrc to '" + myLocation + "'");
    Assert.assertEquals(0,ret.getResponseCode());

    //check if exported data has inherited the permissions.
    assertExistence(myLocation);
    Assert.assertEquals(getPermissions(myLocation).toString(), "rwxrwxrwx");

    assertExistence(myLocation + "/part1=1/part2=1");
    Assert.assertEquals(getPermissions(myLocation + "/part1=1/part2=1").toString(), "rwxrwxrwx");
    Assert.assertTrue(listChildrenPerms(myLocation + "/part1=1/part2=1").size() > 0);
    for (FsPermission perm : listChildrenPerms(myLocation + "/part1=1/part2=1")) {
      Assert.assertEquals("rwxrwxrwx", perm.toString());
    }

    assertExistence(myLocation + "/part1=2/part2=2");
    Assert.assertEquals(getPermissions(myLocation + "/part1=2/part2=2").toString(), "rwxrwxrwx");
    Assert.assertTrue(listChildrenPerms(myLocation + "/part1=2/part2=2").size() > 0);
    for (FsPermission perm : listChildrenPerms(myLocation + "/part1=2/part2=2")) {
      Assert.assertEquals("rwxrwxrwx", perm.toString());
    }

    //import the table back into another database
    String testDb = "eximdb";
    ret = driver.run("CREATE DATABASE " + testDb);
    Assert.assertEquals(0,ret.getResponseCode());

    //use another permission for this import location, to verify that it is really set.
    assertExistence(testDir + "/" + testDb + ".db");
    setPermissions(testDir + "/" + testDb + ".db", FsPermission.createImmutable((short) 0766));

    ret = driver.run("USE " + testDb);
    Assert.assertEquals(0,ret.getResponseCode());

    ret = driver.run("import from '" + myLocation + "'");
    Assert.assertEquals(0,ret.getResponseCode());

    //check permissions of imported, from the exported table
    assertExistence(testDir + "/" + testDb + ".db/mysrc");
    Assert.assertEquals("rwxrw-rw-", getPermissions(testDir + "/" + testDb + ".db/mysrc").toString());

    myLocation = testDir + "/" + testDb + ".db/mysrc";
    assertExistence(myLocation);
    Assert.assertEquals(getPermissions(myLocation).toString(), "rwxrw-rw-");

    assertExistence(myLocation + "/part1=1/part2=1");
    Assert.assertEquals(getPermissions(myLocation + "/part1=1/part2=1").toString(), "rwxrw-rw-");
    Assert.assertTrue(listChildrenPerms(myLocation + "/part1=1/part2=1").size() > 0);
    for (FsPermission perm : listChildrenPerms(myLocation + "/part1=1/part2=1")) {
      Assert.assertEquals("rwxrw-rw-", perm.toString());
    }

    assertExistence(myLocation + "/part1=2/part2=2");
    Assert.assertEquals(getPermissions(myLocation + "/part1=2/part2=2").toString(), "rwxrw-rw-");
    Assert.assertTrue(listChildrenPerms(myLocation + "/part1=2/part2=2").size() > 0);
    for (FsPermission perm : listChildrenPerms(myLocation + "/part1=2/part2=2")) {
      Assert.assertEquals("rwxrw-rw-", perm.toString());
    }
  }


  private void setPermissions(String locn, FsPermission permissions) throws Exception {
    fs.setPermission(new Path(locn), permissions);
  }

  private FsPermission getPermissions(String locn) throws Exception {
    return fs.getFileStatus(new Path(locn)).getPermission();
  }

  private void assertExistence(String locn) throws Exception {
    Assert.assertTrue(fs.exists(new Path(locn)));
  }

  private List<FsPermission> listChildrenPerms(String locn) throws Exception {
    HadoopShims hadoopShims = ShimLoader.getHadoopShims();
    List<FsPermission> result = new ArrayList<FsPermission>();
    List<FileStatus> fileStatuses = hadoopShims.listLocatedStatus(fs, new Path(locn), hiddenFileFilter);
    for (FileStatus status : fileStatuses) {
      result.add(status.getPermission());
    }
    return result;
  }
}
