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

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.junit.Test;

/**
 * This test the flag 'hive.warehouse.subdir.inherit.perms'.
 */
public class TestFolderPermissions extends TestCase {
  protected HiveConf conf;
  protected Driver driver;
  protected String dataFileDir;
  protected Path dataFilePath;
  protected String testDir;


  @Override
  protected void setUp() throws Exception {

    super.setUp();
    testDir = System.getProperty("test.warehouse.dir");

    conf = new HiveConf(this.getClass());
    dataFileDir = conf.get("test.data.files").replace('\\', '/')
        .replace("c:", "");
    dataFilePath = new Path(dataFileDir, "kv1.txt");

    int port = MetaStoreUtils.findFreePort();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS, true);

    // Turn off metastore-side authorization
    System.setProperty(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname,
        "");

    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge());

    SessionState.start(new CliSessionState(conf));
    driver = new Driver(conf);
  }

  @Test
  public void testStaticPartitionPerms() throws Exception {

    CommandProcessorResponse ret = driver.run("DROP TABLE IF EXISTS mysrc");
    assertEquals(0,ret.getResponseCode());

    ret = driver.run("CREATE TABLE mysrc (key STRING, value STRING) STORED AS TEXTFILE");
    assertEquals(0,ret.getResponseCode());

    ret = driver.run("LOAD DATA LOCAL INPATH '" + dataFilePath + "' INTO TABLE mysrc");
    assertEquals(0,ret.getResponseCode());

    ret = driver.run("CREATE TABLE newtable (key string, value string) partitioned by (part1 int, part2 int)");
    assertEquals(0,ret.getResponseCode());

    assertExistence(testDir + "/newtable");
    setPermissions(testDir + "/newtable", FsPermission.createImmutable((short) 0777));

    ret = driver.run("insert into table newtable partition(part1='1',part2='1') select * from mysrc");
    assertEquals(0,ret.getResponseCode());

    Assert.assertEquals("rwxrwxrwx", getPermissions(testDir + "/newtable/part1=1").toString());
    Assert.assertEquals("rwxrwxrwx", getPermissions(testDir + "/newtable/part1=1/part2=1").toString());
  }

  private void setPermissions(String locn, FsPermission permissions) throws Exception {
    FileSystem fs = FileSystem.get(new URI(locn), conf);
    fs.setPermission(new Path(locn), permissions);
  }

  private FsPermission getPermissions(String locn) throws Exception {
    FileSystem fs = FileSystem.get(new URI(locn), conf);
    return fs.getFileStatus(new Path(locn)).getPermission();
  }

  private void assertExistence(String locn) throws Exception {
    FileSystem fs = FileSystem.get(new URI(locn), conf);
    Assert.assertTrue(fs.exists(new Path(locn)));
  }
}
