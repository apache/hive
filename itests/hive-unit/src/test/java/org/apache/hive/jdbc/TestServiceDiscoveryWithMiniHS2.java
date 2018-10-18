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

package org.apache.hive.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestServiceDiscoveryWithMiniHS2 {
  private MiniHS2 miniHS2 = null;
  private static HiveConf hiveConf;
  private static TestingServer zkServer;
  private static String zkRootNamespace = "hs2test";
  private static String dataFileDir;
  private static Path kvDataFilePath;

  private Connection hs2Conn = null;

  @BeforeClass
  public static void beforeTest() throws Exception {
    MiniHS2.cleanupLocalDir();
    zkServer = new TestingServer();
    Class.forName(MiniHS2.getJdbcDriverName());
    hiveConf = new HiveConf();
    hiveConf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    // Set up zookeeper dynamic service discovery configs
    enableZKServiceDiscoveryConfigs(hiveConf);
    dataFileDir = hiveConf.get("test.data.files").replace('\\', '/').replace("c:", "");
    kvDataFilePath = new Path(dataFileDir, "kv1.txt");
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (zkServer != null) {
      zkServer.close();
      zkServer = null;
    }
    MiniHS2.cleanupLocalDir();
  }

  @Before
  public void setUp() throws Exception {
    miniHS2 = new MiniHS2.Builder().withConf(hiveConf).cleanupLocalDirOnStartup(false).build();
  }

  @After
  public void tearDown() throws Exception {
    if (hs2Conn != null) {
      hs2Conn.close();
    }
    if ((miniHS2 != null) && miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  @Test
  public void testConnectionWithConfigsPublished() throws Exception {
    Map<String, String> confOverlay = new HashMap<String, String>();
    confOverlay.put("hive.server2.zookeeper.publish.configs", "true");
    miniHS2.start(confOverlay);
    openConnectionAndRunQuery();
  }

  @Test
  public void testConnectionWithoutConfigsPublished() throws Exception {
    Map<String, String> confOverlay = new HashMap<String, String>();
    confOverlay.put("hive.server2.zookeeper.publish.configs", "false");
    miniHS2.start(confOverlay);
    openConnectionAndRunQuery();
  }

  private static void enableZKServiceDiscoveryConfigs(HiveConf conf) {
    conf.setBoolVar(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY, true);
    conf.setVar(ConfVars.HIVE_ZOOKEEPER_QUORUM, zkServer.getConnectString());
    conf.setVar(ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE, zkRootNamespace);
  }

  private Connection getConnection(String jdbcURL, String user, String pwd) throws SQLException {
    Connection conn = DriverManager.getConnection(jdbcURL, user, pwd);
    return conn;
  }

  private void openConnectionAndRunQuery() throws Exception {
    hs2Conn = getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
    String tableName = "testTab1";
    Statement stmt = hs2Conn.createStatement();
    // create table
    stmt.execute("DROP TABLE IF EXISTS " + tableName);
    stmt.execute("CREATE TABLE " + tableName
        + " (under_col INT COMMENT 'the under column', value STRING) COMMENT ' test table'");
    // load data
    stmt.execute("load data local inpath '" + kvDataFilePath.toString() + "' into table "
        + tableName);
    ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
    assertTrue(res.next());
    assertEquals("val_238", res.getString(2));
    res.close();
    stmt.close();
  }

  @Test
  public void testGetAllUrlsZk() throws Exception {
    Map<String, String> confOverlay = new HashMap<String, String>();
    confOverlay.put("hive.server2.zookeeper.publish.configs", "true");
    miniHS2.start(confOverlay);
    String directUrl = HiveConnection.getAllUrls(miniHS2.getJdbcURL()).get(0).getJdbcUriString();
    String jdbcUrl = "jdbc:hive2://" + miniHS2.getHost() + ":" + miniHS2.getBinaryPort() +
        "/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hs2test;";
    assertEquals(jdbcUrl, directUrl);
  }

  @Test
  public void testGetAllUrlsDirect() throws Exception {
    Map<String, String> confOverlay = new HashMap<String, String>();
    confOverlay.put("hive.server2.zookeeper.publish.configs", "false");
    miniHS2.start(confOverlay);
    String directUrl = HiveConnection.getAllUrls(miniHS2.getJdbcURL()).get(0).getJdbcUriString();
    String jdbcUrl = "jdbc:hive2://" + miniHS2.getHost() + ":" + miniHS2.getBinaryPort() +
        "/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hs2test;";
    assertEquals(jdbcUrl, directUrl);
  }
}