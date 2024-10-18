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

package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.ViewDistributedFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.*;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class TestInsertIntoViewFsOverload {
  private static final Logger LOG = LoggerFactory.getLogger(TestInsertIntoViewFsOverload.class);
  private static MiniDFSCluster cluster;
  private static WarehouseInstance hiveWarehouse;
  @Rule
  public final TestName testName = new TestName();
  private static String dbName;
  private static String jksFile = System.getProperty("java.io.tmpdir") + "/test.jks";

  @BeforeClass
  public static void beforeClassSetup() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.key.provider.path", "jceks://file" + jksFile);
    conf.set("fs.hdfs.impl", ViewDistributedFileSystem.class.getName());
    String FS_DEFAULT_NAME = "hdfs://localhost:55149/";
    URI defaultURI = URI.create(FS_DEFAULT_NAME);
    conf.set("fs.viewfs.mounttable." + defaultURI.getHost() + ".linkFallback", defaultURI.toString());
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).nameNodePort(55149).format(true).build();
    hiveWarehouse = new WarehouseInstance(LOG, cluster, new HashMap<String, String>() {{
      put(HiveConf.ConfVars.HIVE_IN_TEST.varname, "false");
    }});
  }

  @AfterClass
  public static void classLevelTearDown() throws IOException {
    if (cluster != null) {
      FileSystem.closeAll();
      cluster.shutdown();
    }
    hiveWarehouse.close();
  }

  @Before
  public void setUpDB() throws Throwable {
    dbName = testName.getMethodName() + "_" + +System.currentTimeMillis();
    hiveWarehouse.run("create database " + dbName);
  }

  // Tests the functionality of hive insert into HDFS with ViewFs Overload scheme enabled
  @Test
  public void testInsertIntoHDFSViewFsOverload() throws Throwable {
    hiveWarehouse.run("use " + dbName)
        .run("create table t1 (a int)")
        .run("insert into table t1 values (1),(2)")
        .run("select * from t1")
        .verifyResults(new String[] { "1", "2" });
  }
}

