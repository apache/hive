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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hive.common.util.ReflectionUtil;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.NucleusContext;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.AbstractNucleusContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJdbcWithMiniHS2 {
  private static MiniHS2 miniHS2 = null;
  private static String dataFileDir;
  private static Path kvDataFilePath;
  private static final String tmpDir = System.getProperty("test.tmp.dir");

  private Connection hs2Conn = null;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    HiveConf conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    miniHS2 = new MiniHS2(conf);
    dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    kvDataFilePath = new Path(dataFileDir, "kv1.txt");
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
        + kvDataFilePath.toString() + "' into table " + tableName);

    ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
    assertTrue(res.next());
    assertEquals("val_238", res.getString(2));
    res.close();
    stmt.close();
  }

  @Test
  public void testConcurrentStatements() throws Exception {
    String tableName = "testConcurrentStatements";
    Statement stmt = hs2Conn.createStatement();

    // create table
    stmt.execute("DROP TABLE IF EXISTS " + tableName);
    stmt.execute("CREATE TABLE " + tableName
        + " (under_col INT COMMENT 'the under column', value STRING) COMMENT ' test table'");

    // load data
    stmt.execute("load data local inpath '"
        + kvDataFilePath.toString() + "' into table " + tableName);

    ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
    assertTrue(res.next());
    res.close();
    stmt.close();

    // Start concurrent testing
    int POOL_SIZE = 100;
    int TASK_COUNT = 300;

    SynchronousQueue<Runnable> executorQueue = new SynchronousQueue<Runnable>();
    ExecutorService workers = new ThreadPoolExecutor(1, POOL_SIZE, 20, TimeUnit.SECONDS, executorQueue);
    List<Future<Boolean>> list = new ArrayList<Future<Boolean>>();
    int i = 0;
    while(i < TASK_COUNT) {
      try {
        Future<Boolean> future = workers.submit(new JDBCTask(hs2Conn, i, tableName));
        list.add(future);
        i++;
      } catch (RejectedExecutionException ree) {
        try {
          TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    for (Future<Boolean> future : list) {
      try {
        Boolean result = future.get(30, TimeUnit.SECONDS);
        assertTrue(result);
      } catch (ExecutionException ee) {
        fail("Concurrent Statement failed: " + ee.getCause());
      } catch (TimeoutException te) {
        System.out.println("Task was timeout after 30 second: " + te);
      } catch (CancellationException ce) {
        System.out.println("Task was interrupted: " + ce);
      } catch (InterruptedException ie) {
        System.out.println("Thread was interrupted: " + ie);
      }
    }
    workers.shutdown();
  }

  static class JDBCTask implements Callable<Boolean> {
    private String showsql = "show tables";
    private String querysql;
    private int seq = 0;
    Connection con = null;
    Statement stmt = null;
    ResultSet res = null;

    JDBCTask(Connection con, int seq, String tblName) {
      this.con = con;
      this.seq = seq;
      querysql = "SELECT count(value) FROM " + tblName;
    }

    public Boolean call() throws SQLException {
      int mod = seq%10;
      try {
        if (mod < 2) {
          String name = con.getMetaData().getDatabaseProductName();
        } else if (mod < 5) {
          stmt = con.createStatement();
          res = stmt.executeQuery(querysql);
          while (res.next()) {
            res.getInt(1);
          }
        } else if (mod < 7) {
          res = con.getMetaData().getSchemas();
          if (res.next()) {
            res.getString(1);
          }
        } else {
          stmt = con.createStatement();
          res = stmt.executeQuery(showsql);
          if (res.next()) {
            res.getString(1);
          }
        }
        return new Boolean(true);
      } finally {
        try {
          if (res != null) {
            res.close();
            res = null;
          }
          if (stmt != null) {
            stmt.close();
            stmt = null;
          }
        } catch (SQLException sqle1) {
        }
      }
    }
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

  private void setSerializeInTasksInConf(HiveConf conf) {
    conf.setBoolean("hive.server2.thrift.resultset.serialize.in.tasks", true);
    conf.setInt("hive.server2.thrift.resultset.max.fetch.size", 1000);
  }

  private void unsetSerializeInTasksInConf(HiveConf conf) {
    conf.setBoolean("hive.server2.thrift.resultset.serialize.in.tasks", false);
    conf.unset("hive.server2.thrift.resultset.max.fetch.size");
  }

  @Test
  public void testMetadataQueriesWithSerializeThriftInTasks() throws Exception {
    //stop HiveServer2
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }

    HiveConf conf = new HiveConf();
    String userName;
    setSerializeInTasksInConf(conf);
    miniHS2 = new MiniHS2(conf);
    Map<String, String> confOverlay = new HashMap<String, String>();
    miniHS2.start(confOverlay);

    userName = System.getProperty("user.name");
    hs2Conn = getConnection(miniHS2.getJdbcURL(), userName, "password");
    Statement stmt = hs2Conn.createStatement();
    stmt.execute("drop table if exists testThriftSerializeShow");
    stmt.execute("create table testThriftSerializeShow (a int)");
    ResultSet rs = stmt.executeQuery("show tables");
    assertTrue(rs.next());
    stmt.execute("describe testThriftSerializeShow");
    stmt.execute("explain select a from testThriftSerializeShow");
    stmt.execute("drop table testThriftSerializeShow");
    stmt.close();
  }

  @Test
  public void testSelectThriftSerializeInTasks() throws Exception {
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }

    HiveConf conf = new HiveConf();
    String userName;
    setSerializeInTasksInConf(conf);
    conf.setBoolVar(HiveConf.ConfVars.HIVEOPTIMIZEMETADATAQUERIES, false);
    miniHS2 = new MiniHS2(conf);
    Map<String, String> confOverlay = new HashMap<String, String>();
    miniHS2.start(confOverlay);

    userName = System.getProperty("user.name");
    hs2Conn = getConnection(miniHS2.getJdbcURL(), userName, "password");
    Statement stmt = hs2Conn.createStatement();

    stmt.execute("drop table if exists testSelectThriftOrders");
    stmt.execute("drop table if exists testSelectThriftCustomers");
    stmt.execute("create table testSelectThriftOrders (orderid int, orderdate string, customerid int)");
    stmt.execute("create table testSelectThriftCustomers (customerid int, customername string, customercountry string)");
    stmt.execute("insert into testSelectThriftOrders values (1, '2015-09-09', 123), (2, '2015-10-10', 246), (3, '2015-11-11', 356)");
    stmt.execute("insert into testSelectThriftCustomers values (123, 'David', 'America'), (246, 'John', 'Canada'), (356, 'Mary', 'CostaRica')");
    ResultSet countOrders = stmt.executeQuery("select count(*) from testSelectThriftOrders");
    while (countOrders.next()) {
       assertEquals(3, countOrders.getInt(1));
    }
    ResultSet maxOrders = stmt.executeQuery("select max(customerid) from testSelectThriftCustomers");
    while (maxOrders.next()) {
      assertEquals(356, maxOrders.getInt(1));
    }
    stmt.execute("drop table testSelectThriftOrders");
    stmt.execute("drop table testSelectThriftCustomers");
    stmt.close();
  }

  @Test
  public void testJoinThriftSerializeInTasks() throws Exception {
    //stop HiveServer2
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
    HiveConf conf = new HiveConf();
    String userName;

    setSerializeInTasksInConf(conf);

    miniHS2 = new MiniHS2(conf);
    Map<String, String> confOverlay = new HashMap<String, String>();
    miniHS2.start(confOverlay);

    userName = System.getProperty("user.name");
    hs2Conn = getConnection(miniHS2.getJdbcURL(), userName, "password");
    Statement stmt = hs2Conn.createStatement();
    stmt.execute("drop table if exists testThriftJoinOrders");
    stmt.execute("drop table if exists testThriftJoinCustomers");
    stmt.execute("create table testThriftJoinOrders (orderid int, orderdate string, customerid int)");
    stmt.execute("create table testThriftJoinCustomers (customerid int, customername string, customercountry string)");
    stmt.execute("insert into testThriftJoinOrders values (1, '2015-09-09', 123), (2, '2015-10-10', 246), (3, '2015-11-11', 356)");
    stmt.execute("insert into testThriftJoinCustomers values (123, 'David', 'America'), (246, 'John', 'Canada'), (356, 'Mary', 'CostaRica')");
    ResultSet joinResultSet = stmt.executeQuery("select testThriftJoinOrders.orderid, testThriftJoinCustomers.customername from testThriftJoinOrders inner join testThriftJoinCustomers where testThriftJoinOrders.customerid=testThriftJoinCustomers.customerid");
    Map<Integer, String> expectedResult = new HashMap<Integer, String>();
    expectedResult.put(1, "David");
    expectedResult.put(2, "John");
    expectedResult.put(3, "Mary");
    for (int i = 1; i < 4; i++) {
      assertTrue(joinResultSet.next());
      assertEquals(joinResultSet.getString(2), expectedResult.get(i));
    }
    stmt.execute("drop table testThriftJoinOrders");
    stmt.execute("drop table testThriftJoinCustomers");
    stmt.close();
  }

  @Test
  public void testEmptyResultsetThriftSerializeInTasks() throws Exception {
    //stop HiveServer2
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }

    HiveConf conf = new HiveConf();
    String userName;
    setSerializeInTasksInConf(conf);
    miniHS2 = new MiniHS2(conf);
    Map<String, String> confOverlay = new HashMap<String, String>();
    miniHS2.start(confOverlay);

    userName = System.getProperty("user.name");
    hs2Conn = getConnection(miniHS2.getJdbcURL(), userName, "password");
    Statement stmt = hs2Conn.createStatement();
    stmt.execute("drop table if exists testThriftSerializeShow1");
    stmt.execute("drop table if exists testThriftSerializeShow2");
    stmt.execute("create table testThriftSerializeShow1 (a int)");
    stmt.execute("create table testThriftSerializeShow2 (b int)");
    stmt.execute("insert into testThriftSerializeShow1 values (1)");
    stmt.execute("insert into testThriftSerializeShow2 values (2)");
    ResultSet rs = stmt.executeQuery("select * from testThriftSerializeShow1 inner join testThriftSerializeShow2 where testThriftSerializeShow1.a=testThriftSerializeShow2.b");
    assertTrue(!rs.next());
    stmt.execute("drop table testThriftSerializeShow1");
    stmt.execute("drop table testThriftSerializeShow2");
    stmt.close();
  }

  @Test
  public void testFloatCast2DoubleThriftSerializeInTasks() throws Exception {
    //stop HiveServer2
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }

    HiveConf conf = new HiveConf();
    String userName;
    setSerializeInTasksInConf(conf);
    miniHS2 = new MiniHS2(conf);
    Map<String, String> confOverlay = new HashMap<String, String>();
    miniHS2.start(confOverlay);

    userName = System.getProperty("user.name");
    hs2Conn = getConnection(miniHS2.getJdbcURL(), userName, "password");
    Statement stmt = hs2Conn.createStatement();
    stmt.execute("drop table if exists testThriftSerializeShow1");
    stmt.execute("drop table if exists testThriftSerializeShow2");
    stmt.execute("create table testThriftSerializeShow1 (a float)");
    stmt.execute("create table testThriftSerializeShow2 (b double)");
    stmt.execute("insert into testThriftSerializeShow1 values (1.1), (2.2), (3.3)");
    stmt.execute("insert into testThriftSerializeShow2 values (2.2), (3.3), (4.4)");
    ResultSet rs = stmt.executeQuery("select * from testThriftSerializeShow1 inner join testThriftSerializeShow2 where testThriftSerializeShow1.a=testThriftSerializeShow2.b");
    assertTrue(!rs.next());
    stmt.execute("drop table testThriftSerializeShow1");
    stmt.execute("drop table testThriftSerializeShow2");
    stmt.close();
  }

   @Test
   public void testEnableThriftSerializeInTasks() throws Exception {
     //stop HiveServer2
     if (miniHS2.isStarted()) {
       miniHS2.stop();
     }

     HiveConf conf = new HiveConf();
     String userName;
     setSerializeInTasksInConf(conf);
     miniHS2 = new MiniHS2(conf);
     Map<String, String> confOverlay = new HashMap<String, String>();
     miniHS2.start(confOverlay);

     userName = System.getProperty("user.name");
     hs2Conn = getConnection(miniHS2.getJdbcURL(), userName, "password");
     Statement stmt = hs2Conn.createStatement();
     stmt.execute("drop table if exists testThriftSerializeShow1");
     stmt.execute("drop table if exists testThriftSerializeShow2");
     stmt.execute("create table testThriftSerializeShow1 (a int)");
     stmt.execute("create table testThriftSerializeShow2 (b int)");
     stmt.execute("insert into testThriftSerializeShow1 values (1)");
     stmt.execute("insert into testThriftSerializeShow2 values (2)");
     ResultSet rs = stmt.executeQuery("select * from testThriftSerializeShow1 inner join testThriftSerializeShow2 where testThriftSerializeShow1.a=testThriftSerializeShow2.b");
     assertTrue(!rs.next());

     unsetSerializeInTasksInConf(conf);
     rs = stmt.executeQuery("select * from testThriftSerializeShow1 inner join testThriftSerializeShow2 where testThriftSerializeShow1.a=testThriftSerializeShow2.b");
     assertTrue(!rs.next());

     setSerializeInTasksInConf(conf);
     rs = stmt.executeQuery("select * from testThriftSerializeShow1 inner join testThriftSerializeShow2 where testThriftSerializeShow1.a=testThriftSerializeShow2.b");
     assertTrue(!rs.next());

     stmt.execute("drop table testThriftSerializeShow1");
     stmt.execute("drop table testThriftSerializeShow2");
     stmt.close();
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

  /**
   * Test for http header size
   * @throws Exception
   */
  @Test
  public void testHttpHeaderSize() throws Exception {
    // Stop HiveServer2
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
    HiveConf conf = new HiveConf();
    conf.set("hive.server2.transport.mode", "http");
    conf.setInt("hive.server2.thrift.http.request.header.size", 1024);
    conf.setInt("hive.server2.thrift.http.response.header.size", 1024);
    miniHS2 = new MiniHS2(conf);
    Map<String, String> confOverlay = new HashMap<String, String>();
    miniHS2.start(confOverlay);

    // Username is added to the request header
    String userName = StringUtils.leftPad("*", 100);
    // This should go fine, since header should be less than the configured header size
    try {
      hs2Conn = getConnection(miniHS2.getJdbcURL(), userName, "password");
    } catch (Exception e) {
      fail("Not expecting exception: " + e);
    }

    // This should fail with given HTTP response code 413 in error message, since header is more
    // than the configured the header size
    userName = StringUtils.leftPad("*", 2000);
    try {
      hs2Conn = getConnection(miniHS2.getJdbcURL(), userName, "password");
    } catch (Exception e) {
      assertTrue("Header exception thrown", e != null);
      assertTrue(e.getMessage().contains("HTTP Response code: 413"));
    }

    // Stop HiveServer2 to increase header size
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
    conf.setInt("hive.server2.thrift.http.request.header.size", 3000);
    conf.setInt("hive.server2.thrift.http.response.header.size", 3000);
    miniHS2 = new MiniHS2(conf);
    miniHS2.start(confOverlay);

    // This should now go fine, since we increased the configured header size
    try {
      hs2Conn = getConnection(miniHS2.getJdbcURL(), userName, "password");
    } catch (Exception e) {
      fail("Not expecting exception: " + e);
    }
  }

  /**
   * Tests that DataNucleus' NucleusContext.classLoaderResolverMap clears cached class objects
   * (& hence doesn't leak classloaders) on closing any session
   *
   * @throws Exception
   */
  @Test
  public void testAddJarDataNucleusUnCaching() throws Exception {
    Path jarFilePath = new Path(dataFileDir, "identity_udf.jar");
    Connection conn = getConnection(miniHS2.getJdbcURL(), "foo", "bar");
    String tableName = "testAddJar";
    Statement stmt = conn.createStatement();
    stmt.execute("SET hive.support.concurrency = false");
    // Create table
    stmt.execute("DROP TABLE IF EXISTS " + tableName);
    stmt.execute("CREATE TABLE " + tableName + " (key INT, value STRING)");
    // Load data
    stmt.execute("LOAD DATA LOCAL INPATH '" + kvDataFilePath.toString() + "' INTO TABLE "
        + tableName);
    ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
    // Ensure table is populated
    assertTrue(res.next());

    int mapSizeBeforeClose;
    int mapSizeAfterClose;
    // Add the jar file
    stmt.execute("ADD JAR " + jarFilePath.toString());
    // Create a temporary function using the jar
    stmt.execute("CREATE TEMPORARY FUNCTION func AS 'IdentityStringUDF'");
    // Execute the UDF
    stmt.execute("SELECT func(value) from " + tableName);
    mapSizeBeforeClose = getNucleusClassLoaderResolverMapSize();
    System.out
        .println("classLoaderResolverMap size before connection close: " + mapSizeBeforeClose);
    // Cache size should be > 0 now
    Assert.assertTrue(mapSizeBeforeClose > 0);
    conn.close();
    mapSizeAfterClose = getNucleusClassLoaderResolverMapSize();
    System.out.println("classLoaderResolverMap size after connection close: " + mapSizeAfterClose);
    // Cache size should be 0 now
    Assert.assertTrue("Failed; NucleusContext classLoaderResolverMap size: " + mapSizeAfterClose,
        mapSizeAfterClose == 0);
  }

  @SuppressWarnings("unchecked")
  private int getNucleusClassLoaderResolverMapSize() {
    Field classLoaderResolverMap;
    Field pmf;
    JDOPersistenceManagerFactory jdoPmf = null;
    NucleusContext nc = null;
    Map<String, ClassLoaderResolver> cMap;
    try {
      pmf = ObjectStore.class.getDeclaredField("pmf");
      if (pmf != null) {
        pmf.setAccessible(true);
        jdoPmf = (JDOPersistenceManagerFactory) pmf.get(null);
        if (jdoPmf != null) {
          nc = jdoPmf.getNucleusContext();
        }
      }
    } catch (Exception e) {
      System.out.println(e);
    }
    if (nc != null) {
      try {
        classLoaderResolverMap = AbstractNucleusContext.class.getDeclaredField("classLoaderResolverMap");
        if (classLoaderResolverMap != null) {
          classLoaderResolverMap.setAccessible(true);
          cMap = (Map<String, ClassLoaderResolver>) classLoaderResolverMap.get(nc);
          if (cMap != null) {
            return cMap.size();
          }
        }
      } catch (Exception e) {
        System.out.println(e);
      }
    }
    return -1;
  }

  /**
   * Tests ADD JAR uses Hives ReflectionUtil.CONSTRUCTOR_CACHE
   *
   * @throws Exception
   */
  @Test
  public void testAddJarConstructorUnCaching() throws Exception {
    // This test assumes the hive-contrib JAR has been built as part of the Hive build.
    // Also dependent on the UDFExampleAdd class within that JAR.
    setReflectionUtilCache();
    String udfClassName = "org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd";
    String mvnRepo = System.getProperty("maven.local.repository");
    String hiveVersion = System.getProperty("hive.version");
    String jarFileName = "hive-contrib-" + hiveVersion + ".jar";
    String[] pathParts = {
        "org", "apache", "hive",
        "hive-contrib", hiveVersion, jarFileName
    };

    // Create path to hive-contrib JAR on local filesystem
    Path jarFilePath = new Path(mvnRepo);
    for (String pathPart : pathParts) {
      jarFilePath = new Path(jarFilePath, pathPart);
    }

    Connection conn = getConnection(miniHS2.getJdbcURL(), "foo", "bar");
    String tableName = "testAddJar";
    Statement stmt = conn.createStatement();
    stmt.execute("SET hive.support.concurrency = false");
    // Create table
    stmt.execute("DROP TABLE IF EXISTS " + tableName);
    stmt.execute("CREATE TABLE " + tableName + " (key INT, value STRING)");
    // Load data
    stmt.execute("LOAD DATA LOCAL INPATH '" + kvDataFilePath.toString() + "' INTO TABLE "
        + tableName);
    ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
    // Ensure table is populated
    assertTrue(res.next());

    long cacheBeforeAddJar, cacheAfterAddJar, cacheAfterClose;
    // Force the cache clear so we know its empty
    invalidateReflectionUtlCache();
    cacheBeforeAddJar = getReflectionUtilCacheSize();
    System.out.println("CONSTRUCTOR_CACHE size before add jar: " + cacheBeforeAddJar);
    System.out.println("CONSTRUCTOR_CACHE as map before add jar:" + getReflectionUtilCache().asMap());
    Assert.assertTrue("FAILED: CONSTRUCTOR_CACHE size before add jar: " + cacheBeforeAddJar,
            cacheBeforeAddJar == 0);

    // Add the jar file
    stmt.execute("ADD JAR " + jarFilePath.toString());
    // Create a temporary function using the jar
    stmt.execute("CREATE TEMPORARY FUNCTION func AS '" + udfClassName + "'");
    // Execute the UDF
    res = stmt.executeQuery("SELECT func(value) from " + tableName);
    assertTrue(res.next());

    // Check to make sure the cache is now being used
    cacheAfterAddJar = getReflectionUtilCacheSize();
    System.out.println("CONSTRUCTOR_CACHE size after add jar: " + cacheAfterAddJar);
    Assert.assertTrue("FAILED: CONSTRUCTOR_CACHE size after connection close: " + cacheAfterAddJar,
            cacheAfterAddJar > 0);
    conn.close();
    TimeUnit.SECONDS.sleep(10);
    // Have to force a cleanup of all expired entries here because its possible that the
    // expired entries will still be counted in Cache.size().
    // Taken from:
    // http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/cache/CacheBuilder.html
    cleanUpReflectionUtlCache();
    cacheAfterClose = getReflectionUtilCacheSize();
    System.out.println("CONSTRUCTOR_CACHE size after connection close: " + cacheAfterClose);
    Assert.assertTrue("FAILED: CONSTRUCTOR_CACHE size after connection close: " + cacheAfterClose,
            cacheAfterClose == 0);
  }

  private void setReflectionUtilCache() {
    Field constructorCacheField;
    Cache<Class<?>, Constructor<?>> tmp;
    try {
      constructorCacheField = ReflectionUtil.class.getDeclaredField("CONSTRUCTOR_CACHE");
      if (constructorCacheField != null) {
        constructorCacheField.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(constructorCacheField, constructorCacheField.getModifiers() & ~Modifier.FINAL);
        tmp = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.SECONDS).concurrencyLevel(64).weakKeys().weakValues().build();
        constructorCacheField.set(tmp.getClass(), tmp);
      }
    } catch (Exception e) {
      System.out.println("Error when setting the CONSTRUCTOR_CACHE to expire: " + e);
    }
  }

  private Cache getReflectionUtilCache() {
    Field constructorCacheField;
    try {
      constructorCacheField = ReflectionUtil.class.getDeclaredField("CONSTRUCTOR_CACHE");
      if (constructorCacheField != null) {
        constructorCacheField.setAccessible(true);
        return (Cache) constructorCacheField.get(null);
      }
    } catch (Exception e) {
      System.out.println("Error when getting the CONSTRUCTOR_CACHE var: " + e);
    }
    return null;
  }

  private void invalidateReflectionUtlCache() {
    try {
        Cache constructorCache = getReflectionUtilCache();
        if ( constructorCache != null ) {
          constructorCache.invalidateAll();
        }
    } catch (Exception e) {
      System.out.println("Error when trying to invalidate the cache: " + e);
    }
  }

  private void cleanUpReflectionUtlCache() {
    try {
      Cache constructorCache = getReflectionUtilCache();
      if ( constructorCache != null ) {
        constructorCache.cleanUp();
      }
    } catch (Exception e) {
      System.out.println("Error when trying to cleanUp the cache: " + e);
    }
  }

  private long getReflectionUtilCacheSize() {
    try {
        Cache constructorCache = getReflectionUtilCache();
        if ( constructorCache != null ) {
          return constructorCache.size();
        }
    } catch (Exception e) {
      System.out.println(e);
    }
    return -1;
  }
}
