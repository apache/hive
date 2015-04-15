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

package org.apache.hive.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJdbcWithMiniHS2 {
  private static MiniHS2 miniHS2 = null;
  private static Path dataFilePath;
  private static final String tmpDir = System.getProperty("test.tmp.dir");

  private Connection hs2Conn = null;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    HiveConf conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    miniHS2 = new MiniHS2(conf);
    String dataFileDir = conf.get("test.data.files").replace('\\', '/')
        .replace("c:", "");
    dataFilePath = new Path(dataFileDir, "kv1.txt");
    Map<String, String> confOverlay = new HashMap<String, String>();
    miniHS2.start(confOverlay);
  }

  @Before
  public void setUp() throws Exception {
    hs2Conn = getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
  }

  private Connection getConnection(String jdbcURL, String user, String pwd) throws SQLException {
    Connection conn = DriverManager.getConnection(jdbcURL, user, pwd);
    conn.createStatement().execute("set hive.support.concurrency = false");
    return conn;
  }

  @After
  public void tearDown() throws Exception {
    hs2Conn.close();
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  @Test
  public void testConnection() throws Exception {
    String tableName = "testTab1";
    Statement stmt = hs2Conn.createStatement();

    // create table
    stmt.execute("DROP TABLE IF EXISTS " + tableName);
    stmt.execute("CREATE TABLE " + tableName
        + " (under_col INT COMMENT 'the under column', value STRING) COMMENT ' test table'");

    // load data
    stmt.execute("load data local inpath '"
        + dataFilePath.toString() + "' into table " + tableName);

    ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
    assertTrue(res.next());
    assertEquals("val_238", res.getString(2));
    res.close();
    stmt.close();
  }


  /**   This test is to connect to any database without using the command "Use <<DB>>"
   *  1)connect to default database.
   *  2) Create a new DB test_default.
   *  3) Connect to test_default database.
   *  4) Connect and create table under test_default_test.
   *  5) Connect and display all tables.
   *  6) Connect to default database and shouldn't find table test_default_test.
   *  7) Connect and drop test_default_test.
   *  8) drop test_default database.
   */

  @Test
  public void testURIDatabaseName() throws Exception{

    String  jdbcUri  = miniHS2.getJdbcURL().substring(0, miniHS2.getJdbcURL().indexOf("default"));

    hs2Conn= getConnection(jdbcUri+"default", System.getProperty("user.name"),"bar");
    String dbName="test_connection_non_default_db";
    String tableInNonDefaultSchema="table_in_non_default_schema";
    Statement stmt = hs2Conn.createStatement();
    stmt.execute("create database  if not exists "+dbName);
    stmt.close();
    hs2Conn.close();

    hs2Conn = getConnection(jdbcUri+dbName,System.getProperty("user.name"),"bar");
    stmt = hs2Conn .createStatement();
    boolean expected = stmt.execute(" create table "+tableInNonDefaultSchema +" (x int)");
    stmt.close();
    hs2Conn .close();

    hs2Conn  = getConnection(jdbcUri+dbName,System.getProperty("user.name"),"bar");
    stmt = hs2Conn .createStatement();
    ResultSet res = stmt.executeQuery("show tables");
    boolean testTableExists = false;
    while (res.next()) {
      assertNotNull("table name is null in result set", res.getString(1));
      if (tableInNonDefaultSchema.equalsIgnoreCase(res.getString(1))) {
        testTableExists = true;
      }
    }
    assertTrue("table name  "+tableInNonDefaultSchema
        + "   found in SHOW TABLES result set", testTableExists);
    stmt.close();
    hs2Conn .close();

    hs2Conn  = getConnection(jdbcUri+"default",System.getProperty("user.name"),"bar");
    stmt = hs2Conn .createStatement();
    res = stmt.executeQuery("show tables");
    testTableExists = false;
    while (res.next()) {
      assertNotNull("table name is null in result set", res.getString(1));
      if (tableInNonDefaultSchema.equalsIgnoreCase(res.getString(1))) {
        testTableExists = true;
      }
    }

    assertFalse("table name "+tableInNonDefaultSchema
        + "  NOT  found in SHOW TABLES result set", testTableExists);
    stmt.close();
    hs2Conn .close();

    hs2Conn  = getConnection(jdbcUri+dbName,System.getProperty("user.name"),"bar");
    stmt = hs2Conn .createStatement();
    stmt.execute("set hive.support.concurrency = false");
    res = stmt.executeQuery("show tables");

    stmt.execute(" drop table if exists table_in_non_default_schema");
    expected = stmt.execute("DROP DATABASE "+ dbName);
    stmt.close();

    hs2Conn  = getConnection(jdbcUri+"default",System.getProperty("user.name"),"bar");
    stmt = hs2Conn .createStatement();
    res = stmt.executeQuery("show tables");
    testTableExists = false;
    while (res.next()) {
      assertNotNull("table name is null in result set", res.getString(1));
      if (tableInNonDefaultSchema.equalsIgnoreCase(res.getString(1))) {
        testTableExists = true;
      }
    }

    // test URI with no dbName
    hs2Conn  = getConnection(jdbcUri, System.getProperty("user.name"),"bar");
    verifyCurrentDB("default", hs2Conn);
    hs2Conn.close();

    hs2Conn  = getConnection(jdbcUri + ";", System.getProperty("user.name"),"bar");
    verifyCurrentDB("default", hs2Conn);
    hs2Conn.close();

    hs2Conn  = getConnection(jdbcUri + ";/foo=bar;foo1=bar1", System.getProperty("user.name"),"bar");
    verifyCurrentDB("default", hs2Conn);
    hs2Conn.close();
  }

  @Test
  public void testConnectionSchemaAPIs() throws Exception {
    String db1 = "DB1";
    /**
     * get/set Schema are new in JDK7 and not available in java.sql.Connection in JDK6.
     * Hence the test uses HiveConnection object to call these methods so that test will run with older JDKs
     */
    HiveConnection hiveConn = (HiveConnection)hs2Conn;

    assertEquals("default", hiveConn.getSchema());
    Statement stmt = hs2Conn.createStatement();
    stmt.execute("DROP DATABASE IF EXISTS " + db1 + " CASCADE");
    stmt.execute("CREATE DATABASE " + db1);
    assertEquals("default", hiveConn.getSchema());

    stmt.execute("USE " + db1);
    assertEquals(db1, hiveConn.getSchema());

    stmt.execute("USE default");
    assertEquals("default", hiveConn.getSchema());

    hiveConn.setSchema(db1);
    assertEquals(db1, hiveConn.getSchema());
    hiveConn.setSchema("default");
    assertEquals("default", hiveConn.getSchema());

    assertTrue(hiveConn.getCatalog().isEmpty());
    hiveConn.setCatalog("foo");
    assertTrue(hiveConn.getCatalog().isEmpty());
  }

  /**
   * verify that the current db is the one expected. first create table as <db>.tab and then
   * describe that table to check if <db> is the current database
   * @param expectedDbName
   * @param hs2Conn
   * @throws Exception
   */
  private void verifyCurrentDB(String expectedDbName, Connection hs2Conn) throws Exception {
    String verifyTab = "miniHS2DbVerificationTable";
    Statement stmt = hs2Conn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS " + expectedDbName + "." + verifyTab);
    stmt.execute("CREATE TABLE " + expectedDbName + "." + verifyTab + "(id INT)");
    stmt.execute("DESCRIBE " + verifyTab);
    stmt.execute("DROP TABLE IF EXISTS " + expectedDbName + "." + verifyTab);
    stmt.close();
  }

  /**
   * This method tests whether while creating a new connection, the config
   * variables specified in the JDBC URI are properly set for the connection.
   * This is a test for HiveConnection#configureConnection.
   *
   * @throws Exception
   */
  @Test
  public void testNewConnectionConfiguration() throws Exception {

    // Set some conf parameters
    String hiveConf = "hive.cli.print.header=true;hive.server2.async.exec.shutdown.timeout=20;"
        + "hive.server2.async.exec.threads=30;hive.server2.thrift.max.worker.threads=15";
    // Set some conf vars
    String hiveVar = "stab=salesTable;icol=customerID";
    String jdbcUri = miniHS2.getJdbcURL() + "?" + hiveConf + "#" + hiveVar;

    // Open a new connection with these conf & vars
    Connection con1 = DriverManager.getConnection(jdbcUri);

    // Execute "set" command and retrieve values for the conf & vars specified
    // above
    // Assert values retrieved
    Statement stmt = con1.createStatement();

    // Verify that the property has been properly set while creating the
    // connection above
    verifyConfProperty(stmt, "hive.cli.print.header", "true");
    verifyConfProperty(stmt, "hive.server2.async.exec.shutdown.timeout", "20");
    verifyConfProperty(stmt, "hive.server2.async.exec.threads", "30");
    verifyConfProperty(stmt, "hive.server2.thrift.max.worker.threads",
        "15");
    verifyConfProperty(stmt, "stab", "salesTable");
    verifyConfProperty(stmt, "icol", "customerID");
    con1.close();
  }

  private void verifyConfProperty(Statement stmt, String property,
      String expectedValue) throws Exception {
    ResultSet res = stmt.executeQuery("set " + property);
    while (res.next()) {
      String resultValues[] = res.getString(1).split("=");
      assertEquals(resultValues[1], expectedValue);
    }
  }

  /**
   * Tests the creation of the 3 scratch dirs: hdfs, local, downloaded resources (which is also local).
   * 1. Test with doAs=false: open a new JDBC session and verify the presence of directories/permissions
   * 2. Test with doAs=true: open a new JDBC session and verify the presence of directories/permissions
   * @throws Exception
   */
  @Test
  public void testSessionScratchDirs() throws Exception {
    // Stop HiveServer2
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
    HiveConf conf = new HiveConf();
    String userName;
    Path scratchDirPath;
    // 1. Test with doAs=false
    conf.setBoolean("hive.server2.enable.doAs", false);
    // Set a custom prefix for hdfs scratch dir path
    conf.set("hive.exec.scratchdir", tmpDir + "/hs2");
    // Set a scratch dir permission
    String fsPermissionStr = "700";
    conf.set("hive.scratch.dir.permission", fsPermissionStr);
    // Start an instance of HiveServer2 which uses miniMR
    miniHS2 = new MiniHS2(conf);
    Map<String, String> confOverlay = new HashMap<String, String>();
    miniHS2.start(confOverlay);
    userName = System.getProperty("user.name");
    hs2Conn = getConnection(miniHS2.getJdbcURL(), userName, "password");
    // FS
    FileSystem fs = miniHS2.getLocalFS();
    FsPermission expectedFSPermission = new FsPermission(HiveConf.getVar(conf,
        HiveConf.ConfVars.SCRATCHDIRPERMISSION));

    // Verify scratch dir paths and permission
    // HDFS scratch dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIR) + "/" + userName);
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, false);

    // Local scratch dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.LOCALSCRATCHDIR));
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, true);

    // Downloaded resources dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR));
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, true);

    // 2. Test with doAs=true
    // Restart HiveServer2 with doAs=true
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
    conf.setBoolean("hive.server2.enable.doAs", true);
    // Start HS2
    miniHS2 = new MiniHS2(conf);
    miniHS2.start(confOverlay);
    // Test for user "neo"
    userName = "neo";
    hs2Conn = getConnection(miniHS2.getJdbcURL(), userName, "the-one");

    // Verify scratch dir paths and permission
    // HDFS scratch dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIR) + "/" + userName);
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, false);

    // Local scratch dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.LOCALSCRATCHDIR));
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, true);

    // Downloaded resources dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR));
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, true);

    // Test for user "trinity"
    userName = "trinity";
    hs2Conn = getConnection(miniHS2.getJdbcURL(), userName, "the-one");

    // Verify scratch dir paths and permission
    // HDFS scratch dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIR) + "/" + userName);
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, false);

    // Local scratch dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.LOCALSCRATCHDIR));
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, true);

    // Downloaded resources dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR));
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, true);
  }

  /** Test UDF whitelist
   *   - verify default value
   *   - verify udf allowed with default whitelist
   *   - verify udf allowed with specific whitelist
   *   - verify udf disallowed when not in whitelist
   * @throws Exception
   */
  @Test
  public void testUdfWhiteList() throws Exception {
    HiveConf testConf = new HiveConf();
    assertTrue(testConf.getVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_WHITELIST).isEmpty());
    // verify that udf in default whitelist can be executed
    Statement stmt = hs2Conn.createStatement();
    stmt.executeQuery("SELECT substr('foobar', 4) ");
    hs2Conn.close();
    miniHS2.stop();

    // setup whitelist
    Set<String> funcNames = FunctionRegistry.getFunctionNames();
    funcNames.remove("reflect");
    String funcNameStr = "";
    for (String funcName : funcNames) {
      funcNameStr += "," + funcName;
    }
    funcNameStr = funcNameStr.substring(1); // remove ',' at begining
    testConf.setVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_WHITELIST, funcNameStr);
    miniHS2 = new MiniHS2(testConf);
    miniHS2.start(new HashMap<String, String>());

    hs2Conn = getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
    stmt = hs2Conn.createStatement();
    // verify that udf in whitelist can be executed
    stmt.executeQuery("SELECT substr('foobar', 3) ");

    // verify that udf not in whitelist fails
    try {
      stmt.executeQuery("SELECT reflect('java.lang.String', 'valueOf', 1) ");
      fail("reflect() udf invocation should fail");
    } catch (SQLException e) {
      // expected
    }
  }

  /** Test UDF blacklist
   *   - verify default value
   *   - verify udfs allowed with default blacklist
   *   - verify udf disallowed when in blacklist
   * @throws Exception
   */
  @Test
  public void testUdfBlackList() throws Exception {
    HiveConf testConf = new HiveConf();
    assertTrue(testConf.getVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_BLACKLIST).isEmpty());

    Statement stmt = hs2Conn.createStatement();
    // verify that udf in default whitelist can be executed
    stmt.executeQuery("SELECT substr('foobar', 4) ");

    miniHS2.stop();
    testConf.setVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_BLACKLIST, "reflect");
    miniHS2 = new MiniHS2(testConf);
    miniHS2.start(new HashMap<String, String>());
    hs2Conn = getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
    stmt = hs2Conn.createStatement();

    try {
      stmt.executeQuery("SELECT reflect('java.lang.String', 'valueOf', 1) ");
      fail("reflect() udf invocation should fail");
    } catch (SQLException e) {
      // expected
    }
  }

  /** Test UDF blacklist overrides whitelist
   * @throws Exception
   */
  @Test
  public void testUdfBlackListOverride() throws Exception {
    // setup whitelist
    HiveConf testConf = new HiveConf();

    Set<String> funcNames = FunctionRegistry.getFunctionNames();
    String funcNameStr = "";
    for (String funcName : funcNames) {
      funcNameStr += "," + funcName;
    }
    funcNameStr = funcNameStr.substring(1); // remove ',' at begining
    testConf.setVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_WHITELIST, funcNameStr);
    testConf.setVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_BLACKLIST, "reflect");
    miniHS2 = new MiniHS2(testConf);
    miniHS2.start(new HashMap<String, String>());

    hs2Conn = getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
    Statement stmt = hs2Conn.createStatement();

    // verify that udf in black list fails even though it's included in whitelist
    try {
      stmt.executeQuery("SELECT reflect('java.lang.String', 'valueOf', 1) ");
      fail("reflect() udf invocation should fail");
    } catch (SQLException e) {
      // expected
    }
  }

  /**
   * Tests the creation of the root hdfs scratch dir, which should be writable by all.
   *
   * @throws Exception
   */
  @Test
  public void testRootScratchDir() throws Exception {
    // Stop HiveServer2
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
    HiveConf conf = new HiveConf();
    String userName;
    Path scratchDirPath;
    conf.set("hive.exec.scratchdir", tmpDir + "/hs2");
    // Start an instance of HiveServer2 which uses miniMR
    miniHS2 = new MiniHS2(conf);
    Map<String, String> confOverlay = new HashMap<String, String>();
    miniHS2.start(confOverlay);
    userName = System.getProperty("user.name");
    hs2Conn = getConnection(miniHS2.getJdbcURL(), userName, "password");
    // FS
    FileSystem fs = miniHS2.getLocalFS();
    FsPermission expectedFSPermission = new FsPermission((short)00733);
    // Verify scratch dir paths and permission
    // HDFS scratch dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIR));
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, false);
    // Test with multi-level scratch dir path
    // Stop HiveServer2
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
    conf.set("hive.exec.scratchdir", tmpDir + "/level1/level2/level3");
    miniHS2 = new MiniHS2(conf);
    miniHS2.start(confOverlay);
    hs2Conn = getConnection(miniHS2.getJdbcURL(), userName, "password");
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIR));
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, false);
  }

  private void verifyScratchDir(HiveConf conf, FileSystem fs, Path scratchDirPath,
      FsPermission expectedFSPermission, String userName, boolean isLocal) throws Exception {
    String dirType = isLocal ? "Local" : "DFS";
    assertTrue("The expected " + dirType + " scratch dir does not exist for the user: " +
        userName, fs.exists(scratchDirPath));
    if (fs.exists(scratchDirPath) && !isLocal) {
      assertEquals("DFS scratch dir permissions don't match", expectedFSPermission,
          fs.getFileStatus(scratchDirPath).getPermission());
    }
  }
}