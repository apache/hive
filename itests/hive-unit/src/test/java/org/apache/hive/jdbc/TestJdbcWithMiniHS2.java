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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Base64;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.PersistenceManagerProvider;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.LineageLogger;
import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx;
import org.apache.hive.common.util.ReflectionUtil;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.HiveSQLException;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.NucleusContext;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.AbstractNucleusContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;

public class TestJdbcWithMiniHS2 {
  private static MiniHS2 miniHS2 = null;
  private static String dataFileDir;
  private static Path kvDataFilePath;
  private static final String tmpDir = System.getProperty("test.tmp.dir");
  private static final String testDbName = "testjdbcminihs2";
  private static final String defaultDbName = "default";
  private static final String tableName = "testjdbcminihs2tbl";
  private static final String tableComment = "Simple table";
  private static Connection conDefault = null;
  private static Connection conTestDb = null;
  private static String testUdfClassName =
      "org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd";

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    MiniHS2.cleanupLocalDir();
    HiveConf conf = new HiveConf();
    dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    kvDataFilePath = new Path(dataFileDir, "kv1.txt");

    try {
      startMiniHS2(conf);
    } catch (Exception e) {
     System.out.println("Unable to start MiniHS2: " + e);
     throw e;
    }
    // Open default connections which will be used throughout the tests
    try {
      openDefaultConnections();
    } catch (Exception e) {
      System.out.println("Unable to open default connections to MiniHS2: " + e);
      throw e;
    }
    Statement stmt = conDefault.createStatement();
    stmt.execute("drop database if exists " + testDbName + " cascade");
    stmt.execute("create database " + testDbName + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')");
    stmt.close();

    try {
      openTestConnections();
    } catch (Exception e) {
      System.out.println("Unable to open default connections to MiniHS2: " + e);
      throw e;
    }

    // tables in test db
    createTestTables(conTestDb, testDbName);
  }

  private static Connection getConnection() throws Exception {
    return getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
  }

  private static Connection getConnection(String dbName) throws Exception {
    return getConnection(miniHS2.getJdbcURL(dbName), System.getProperty("user.name"), "bar");
  }

  private static Connection getConnection(String jdbcURL, String user, String pwd)
      throws SQLException {
    Connection conn = DriverManager.getConnection(jdbcURL, user, pwd);
    assertNotNull(conn);
    return conn;
  }

  private static void createTestTables(Connection conn, String dbName) throws SQLException {
    Statement stmt = conn.createStatement();
    Path dataFilePath = new Path(dataFileDir, "kv1.txt");
    // We've already dropped testDbName in constructor & we also drop it in tearDownAfterClass
    String prefix = dbName + ".";
    String tableName = prefix + TestJdbcWithMiniHS2.tableName;

    // create a table
    stmt.execute("create table " + tableName
        + " (int_col int comment 'the int column', value string) comment '" + tableComment + "'");
    // load data
    stmt.execute("load data local inpath '" + dataFilePath.toString() + "' into table " + tableName);

    stmt.close();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    // drop test db and its tables and views
    Statement stmt = conDefault.createStatement();
    stmt.execute("set hive.support.concurrency = false");
    stmt.execute("drop database if exists " + testDbName + " cascade");
    stmt.close();
    if (conTestDb != null) {
      conTestDb.close();
    }
    if (conDefault != null) {
      conDefault.close();
    }
    stopMiniHS2();
    cleanupMiniHS2();
  }

  private static void restoreMiniHS2AndConnections()  throws Exception {
    if (conTestDb != null) {
      try {
        conTestDb.close();
      } catch (SQLException e) {
        // Do nothing
      }
    }
    if (conDefault != null) {
      try {
        conDefault.close();
      } catch (SQLException e) {
        // Do nothing
      }
    }
    stopMiniHS2();
    HiveConf conf = new HiveConf();
    startMiniHS2(conf);
    openDefaultConnections();
    openTestConnections();
  }

  private static void startMiniHS2(HiveConf conf) throws Exception {
    startMiniHS2(conf, false);
  }

  private static void startMiniHS2(HiveConf conf, boolean httpMode) throws Exception {
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED, false);
    conf.setBoolVar(ConfVars.HIVESTATSCOLAUTOGATHER, false);
    // store post-exec hooks calls so we can look at them later
    conf.setVar(ConfVars.POST_EXEC_HOOKS, ReadableHook.class.getName() + "," +
        LineageLogger.class.getName());
    MiniHS2.Builder builder = new MiniHS2.Builder().withConf(conf).cleanupLocalDirOnStartup(false);
    if (httpMode) {
      builder = builder.withHTTPTransport();
    }
    miniHS2 = builder.build();
    Map<String, String> confOverlay = new HashMap<String, String>();
    miniHS2.start(confOverlay);
  }

  private static void stopMiniHS2() {
    if ((miniHS2 != null) && (miniHS2.isStarted())) {
      miniHS2.stop();
    }
  }

  private static void cleanupMiniHS2() throws IOException {
    if (miniHS2 != null) {
      miniHS2.cleanup();
    }
    MiniHS2.cleanupLocalDir();
  }

  private static void openDefaultConnections() throws Exception {
    conDefault = getConnection();
  }

  private static void openTestConnections() throws Exception {
    conTestDb = getConnection(testDbName);
  }

  @Test
  public void testConnection() throws Exception {
    Statement stmt = conTestDb.createStatement();
    ResultSet res = stmt.executeQuery("select * from " + tableName + " limit 5");
    assertTrue(res.next());
    res.close();
    stmt.close();
  }

  @Test
  public void testParallelCompilation() throws Exception {
    Statement stmt = conTestDb.createStatement();
    stmt.execute("set hive.driver.parallel.compilation=true");
    stmt.execute("set hive.server2.async.exec.async.compile=true");
    stmt.close();
    startConcurrencyTest(conTestDb, tableName, 10);
    Connection conn = getConnection(testDbName);
    startConcurrencyTest(conn, tableName, 10);
    conn.close();
  }

  @Test
  public void testParallelCompilation2() throws Exception {
    Statement stmt = conTestDb.createStatement();
    stmt.execute("set hive.driver.parallel.compilation=false");
    stmt.execute("set hive.server2.async.exec.async.compile=true");
    stmt.close();
    startConcurrencyTest(conTestDb, tableName, 10);
    Connection conn = getConnection(testDbName);
    startConcurrencyTest(conn, tableName, 10);
    conn.close();
  }

  @Test
  public void testParallelCompilation3() throws Exception {
    Statement stmt = conTestDb.createStatement();
    stmt.execute("set hive.driver.parallel.compilation=true");
    stmt.execute("set hive.server2.async.exec.async.compile=true");
    stmt.close();
    Connection conn = getConnection(testDbName);
    stmt = conn.createStatement();
    stmt.execute("set hive.driver.parallel.compilation=true");
    stmt.execute("set hive.server2.async.exec.async.compile=true");
    stmt.close();
    int poolSize = 100;
    SynchronousQueue<Runnable> executorQueue1 = new SynchronousQueue<Runnable>();
    ExecutorService workers1 =
        new ThreadPoolExecutor(1, poolSize, 20, TimeUnit.SECONDS, executorQueue1);
    SynchronousQueue<Runnable> executorQueue2 = new SynchronousQueue<Runnable>();
    ExecutorService workers2 =
        new ThreadPoolExecutor(1, poolSize, 20, TimeUnit.SECONDS, executorQueue2);
    List<Future<Boolean>> list1 = startTasks(workers1, conTestDb, tableName, 10);
    List<Future<Boolean>> list2 = startTasks(workers2, conn, tableName, 10);
    finishTasks(list1, workers1);
    finishTasks(list2, workers2);
    conn.close();
  }

  @Test
  public void testParallelCompilation4() throws Exception {
    Statement stmt = conTestDb.createStatement();
    stmt.execute("set hive.driver.parallel.compilation=true");
    stmt.execute("set hive.server2.async.exec.async.compile=false");
    stmt.close();
    Connection conn = getConnection(testDbName);
    stmt = conn.createStatement();
    stmt.execute("set hive.driver.parallel.compilation=true");
    stmt.execute("set hive.server2.async.exec.async.compile=false");
    stmt.close();
    int poolSize = 100;
    SynchronousQueue<Runnable> executorQueue1 = new SynchronousQueue<Runnable>();
    ExecutorService workers1 =
        new ThreadPoolExecutor(1, poolSize, 20, TimeUnit.SECONDS, executorQueue1);
    SynchronousQueue<Runnable> executorQueue2 = new SynchronousQueue<Runnable>();
    ExecutorService workers2 =
        new ThreadPoolExecutor(1, poolSize, 20, TimeUnit.SECONDS, executorQueue2);
    List<Future<Boolean>> list1 = startTasks(workers1, conTestDb, tableName, 10);
    List<Future<Boolean>> list2 = startTasks(workers2, conn, tableName, 10);
    finishTasks(list1, workers1);
    finishTasks(list2, workers2);
    conn.close();
  }

  @Test
  public void testConcurrentStatements() throws Exception {
    startConcurrencyTest(conTestDb, tableName, 50);
  }

  private static void startConcurrencyTest(Connection conn, String tableName, int numTasks) {
    // Start concurrent testing
    int poolSize = 100;
    SynchronousQueue<Runnable> executorQueue = new SynchronousQueue<Runnable>();
    ExecutorService workers =
        new ThreadPoolExecutor(1, poolSize, 20, TimeUnit.SECONDS, executorQueue);
    List<Future<Boolean>> list = startTasks(workers, conn, tableName, numTasks);
    finishTasks(list, workers);
  }

  private static List<Future<Boolean>> startTasks(ExecutorService workers, Connection conn,
      String tableName, int numTasks) {
    List<Future<Boolean>> list = new ArrayList<Future<Boolean>>();
    int i = 0;
    while (i < numTasks) {
      try {
        Future<Boolean> future = workers.submit(new JDBCTask(conn, i, tableName));
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
    return list;
  }

  private static void finishTasks(List<Future<Boolean>> list, ExecutorService workers) {
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
   *  1) connect to default database.
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

    Connection conn= getConnection(jdbcUri+"default", System.getProperty("user.name"),"bar");
    String dbName="test_connection_non_default_db";
    String tableInNonDefaultSchema="table_in_non_default_schema";
    Statement stmt = conn.createStatement();
    stmt.execute("create database  if not exists "+dbName);
    stmt.close();
    conn.close();

    conn = getConnection(jdbcUri+dbName,System.getProperty("user.name"),"bar");
    stmt = conn .createStatement();
    boolean expected = stmt.execute(" create table "+tableInNonDefaultSchema +" (x int)");
    stmt.close();
    conn .close();

    conn  = getConnection(jdbcUri+dbName,System.getProperty("user.name"),"bar");
    stmt = conn .createStatement();
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
    conn .close();

    conn  = getConnection(jdbcUri+"default",System.getProperty("user.name"),"bar");
    stmt = conn .createStatement();
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
    conn .close();

    conn  = getConnection(jdbcUri+dbName,System.getProperty("user.name"),"bar");
    stmt = conn .createStatement();
    stmt.execute("set hive.support.concurrency = false");
    res = stmt.executeQuery("show tables");

    stmt.execute(" drop table if exists table_in_non_default_schema");
    expected = stmt.execute("DROP DATABASE "+ dbName);
    stmt.close();
    conn.close();

    conn  = getConnection(jdbcUri+"default",System.getProperty("user.name"),"bar");
    stmt = conn .createStatement();
    res = stmt.executeQuery("show tables");
    testTableExists = false;
    while (res.next()) {
      assertNotNull("table name is null in result set", res.getString(1));
      if (tableInNonDefaultSchema.equalsIgnoreCase(res.getString(1))) {
        testTableExists = true;
      }
    }

    // test URI with no dbName
    conn  = getConnection(jdbcUri, System.getProperty("user.name"),"bar");
    verifyCurrentDB("default", conn);
    conn.close();

    conn  = getConnection(jdbcUri + ";", System.getProperty("user.name"),"bar");
    verifyCurrentDB("default", conn);
    conn.close();

    conn  = getConnection(jdbcUri + ";/foo=bar;foo1=bar1", System.getProperty("user.name"),"bar");
    verifyCurrentDB("default", conn);
    conn.close();
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

  @Test
  public void testConnectionSchemaAPIs() throws Exception {
    /**
     * get/set Schema are new in JDK7 and not available in java.sql.Connection in JDK6. Hence the
     * test uses HiveConnection object to call these methods so that test will run with older JDKs
     */
    HiveConnection hiveConn = (HiveConnection) conDefault;
    assertEquals(defaultDbName, hiveConn.getSchema());
    Statement stmt = conDefault.createStatement();

    stmt.execute("USE " + testDbName);
    assertEquals(testDbName, hiveConn.getSchema());

    stmt.execute("USE " + defaultDbName);
    assertEquals(defaultDbName, hiveConn.getSchema());

    hiveConn.setSchema(defaultDbName);
    assertEquals(defaultDbName, hiveConn.getSchema());
    hiveConn.setSchema(defaultDbName);
    assertEquals(defaultDbName, hiveConn.getSchema());

    assertTrue(hiveConn.getCatalog().isEmpty());
    hiveConn.setCatalog("foo");
    assertTrue(hiveConn.getCatalog().isEmpty());
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
    String hiveConf =
        "hive.cli.print.header=true;hive.server2.async.exec.shutdown.timeout=20;"
            + "hive.server2.async.exec.threads=30;hive.server2.thrift.max.worker.threads=15";
    // Set some conf vars
    String hiveVar = "stab=salesTable;icol=customerID";
    String jdbcUri = miniHS2.getJdbcURL() + "?" + hiveConf + "#" + hiveVar;
    // Open a new connection with these conf & vars
    Connection con1 = DriverManager.getConnection(jdbcUri);
    // Execute "set" command and retrieve values for the conf & vars specified above
    // Assert values retrieved
    Statement stmt = con1.createStatement();
    // Verify that the property has been properly set while creating the
    // connection above
    verifyConfProperty(stmt, "hive.cli.print.header", "true");
    verifyConfProperty(stmt, "hive.server2.async.exec.shutdown.timeout", "20");
    verifyConfProperty(stmt, "hive.server2.async.exec.threads", "30");
    verifyConfProperty(stmt, "hive.server2.thrift.max.worker.threads", "15");
    verifyConfProperty(stmt, "stab", "salesTable");
    verifyConfProperty(stmt, "icol", "customerID");
    stmt.close();
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

  @Test
  public void testMetadataQueriesWithSerializeThriftInTasks() throws Exception {
    Statement stmt = conTestDb.createStatement();
    setSerializeInTasksInConf(stmt);
    ResultSet rs = stmt.executeQuery("show tables");
    assertTrue(rs.next());
    stmt.execute("describe " + tableName);
    stmt.execute("explain select * from " + tableName);
    // Note: by closing stmt object, we are also reverting any session specific config changes.
    stmt.close();
  }

  @Test
  public void testSelectThriftSerializeInTasks() throws Exception {
    Statement stmt = conTestDb.createStatement();
    setSerializeInTasksInConf(stmt);
    stmt.execute("set hive.compute.query.using.stats=false");
    stmt.execute("drop table if exists testSelectThriftOrders");
    stmt.execute("drop table if exists testSelectThriftCustomers");
    stmt.execute("create table testSelectThriftOrders (orderid int, orderdate string, customerid int)");
    stmt.execute("create table testSelectThriftCustomers (customerid int, customername string, customercountry string)");
    stmt.execute("insert into testSelectThriftOrders values (1, '2015-09-09', 123), "
        + "(2, '2015-10-10', 246), (3, '2015-11-11', 356)");
    stmt.execute("insert into testSelectThriftCustomers values (123, 'David', 'America'), "
        + "(246, 'John', 'Canada'), (356, 'Mary', 'CostaRica')");
    ResultSet countOrders = stmt.executeQuery("select count(*) from testSelectThriftOrders");
    while (countOrders.next()) {
      assertEquals(3, countOrders.getInt(1));
    }
    ResultSet maxOrders =
        stmt.executeQuery("select max(customerid) from testSelectThriftCustomers");
    while (maxOrders.next()) {
      assertEquals(356, maxOrders.getInt(1));
    }
    stmt.execute("drop table testSelectThriftOrders");
    stmt.execute("drop table testSelectThriftCustomers");
    stmt.close();
  }


  // Test that jdbc does not allow shell commands starting with "!".
  @Test
  public void testBangCommand() throws Exception {
    try (Statement stmt = conTestDb.createStatement()) {
      stmt.execute("!ls --l");
      fail("statement should fail, allowing this would be bad security");
    } catch (HiveSQLException e) {
      assertTrue(e.getMessage().contains("cannot recognize input near '!'"));
    }
  }

  @Test
  public void testJoinThriftSerializeInTasks() throws Exception {
    Statement stmt = conTestDb.createStatement();
    setSerializeInTasksInConf(stmt);
    stmt.execute("drop table if exists testThriftJoinOrders");
    stmt.execute("drop table if exists testThriftJoinCustomers");
    stmt.execute("create table testThriftJoinOrders (orderid int, orderdate string, customerid int)");
    stmt.execute("create table testThriftJoinCustomers (customerid int, customername string, customercountry string)");
    stmt.execute("insert into testThriftJoinOrders values (1, '2015-09-09', 123), (2, '2015-10-10', 246), "
        + "(3, '2015-11-11', 356)");
    stmt.execute("insert into testThriftJoinCustomers values (123, 'David', 'America'), "
        + "(246, 'John', 'Canada'), (356, 'Mary', 'CostaRica')");
    ResultSet joinResultSet =
        stmt.executeQuery("select testThriftJoinOrders.orderid, testThriftJoinCustomers.customername "
            + "from testThriftJoinOrders inner join testThriftJoinCustomers where "
            + "testThriftJoinOrders.customerid=testThriftJoinCustomers.customerid");
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
    Statement stmt = conTestDb.createStatement();
    setSerializeInTasksInConf(stmt);
    stmt.execute("drop table if exists testThriftSerializeShow1");
    stmt.execute("drop table if exists testThriftSerializeShow2");
    stmt.execute("create table testThriftSerializeShow1 (a int)");
    stmt.execute("create table testThriftSerializeShow2 (b int)");
    stmt.execute("insert into testThriftSerializeShow1 values (1)");
    stmt.execute("insert into testThriftSerializeShow2 values (2)");
    ResultSet rs = stmt.executeQuery("select * from testThriftSerializeShow1 inner join "
        + "testThriftSerializeShow2 where testThriftSerializeShow1.a=testThriftSerializeShow2.b");
    assertTrue(!rs.next());
    stmt.execute("drop table testThriftSerializeShow1");
    stmt.execute("drop table testThriftSerializeShow2");
    stmt.close();
  }

  @Test
  public void testFloatCast2DoubleThriftSerializeInTasks() throws Exception {
    Statement stmt = conTestDb.createStatement();
    setSerializeInTasksInConf(stmt);
    stmt.execute("drop table if exists testThriftSerializeShow1");
    stmt.execute("drop table if exists testThriftSerializeShow2");
    stmt.execute("create table testThriftSerializeShow1 (a float)");
    stmt.execute("create table testThriftSerializeShow2 (b double)");
    stmt.execute("insert into testThriftSerializeShow1 values (1.1), (2.2), (3.3)");
    stmt.execute("insert into testThriftSerializeShow2 values (2.2), (3.3), (4.4)");
    ResultSet rs =
        stmt.executeQuery("select * from testThriftSerializeShow1 inner join "
            + "testThriftSerializeShow2 where testThriftSerializeShow1.a=testThriftSerializeShow2.b");
    assertTrue(!rs.next());
    stmt.execute("drop table testThriftSerializeShow1");
    stmt.execute("drop table testThriftSerializeShow2");
    stmt.close();
  }

  @Test
  public void testEnableThriftSerializeInTasks() throws Exception {
    Statement stmt = conTestDb.createStatement();
    stmt.execute("drop table if exists testThriftSerializeShow1");
    stmt.execute("drop table if exists testThriftSerializeShow2");
    stmt.execute("create table testThriftSerializeShow1 (a int)");
    stmt.execute("create table testThriftSerializeShow2 (b int)");
    stmt.execute("insert into testThriftSerializeShow1 values (1)");
    stmt.execute("insert into testThriftSerializeShow2 values (2)");
    ResultSet rs =
        stmt.executeQuery("select * from testThriftSerializeShow1 inner join "
            + "testThriftSerializeShow2 where testThriftSerializeShow1.a=testThriftSerializeShow2.b");
    assertTrue(!rs.next());

    unsetSerializeInTasksInConf(stmt);
    rs =
        stmt.executeQuery("select * from testThriftSerializeShow1 inner join "
            + "testThriftSerializeShow2 where testThriftSerializeShow1.a=testThriftSerializeShow2.b");
    assertTrue(!rs.next());

    setSerializeInTasksInConf(stmt);
    rs =
        stmt.executeQuery("select * from testThriftSerializeShow1 inner join "
            + "testThriftSerializeShow2 where testThriftSerializeShow1.a=testThriftSerializeShow2.b");
    assertTrue(!rs.next());

    stmt.execute("drop table testThriftSerializeShow1");
    stmt.execute("drop table testThriftSerializeShow2");
    stmt.close();
  }

  private void setSerializeInTasksInConf(Statement stmt) throws SQLException {
    stmt.execute("set hive.server2.thrift.resultset.serialize.in.tasks=true");
    stmt.execute("set hive.server2.thrift.resultset.max.fetch.size=1000");
  }

  private void unsetSerializeInTasksInConf(Statement stmt) throws SQLException {
    stmt.execute("set hive.server2.thrift.resultset.serialize.in.tasks=false");
    stmt.execute("set hive.server2.thrift.resultset.max.fetch.size");
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
    stopMiniHS2();
    HiveConf conf = new HiveConf();
    String userName;
    Path scratchDirPath;
    // Set a custom prefix for hdfs scratch dir path
    conf.set("hive.exec.scratchdir", tmpDir + "/hs2");
    // Set a scratch dir permission
    String fsPermissionStr = "700";
    conf.set("hive.scratch.dir.permission", fsPermissionStr);
    // Start an instance of HiveServer2 which uses miniMR
    startMiniHS2(conf);
    // 1. Test with doAs=false
    String sessionConf="hive.server2.enable.doAs=false";
    userName = System.getProperty("user.name");
    Connection conn = getConnection(miniHS2.getJdbcURL(testDbName, sessionConf), userName, "password");
    // FS
    FileSystem fs = miniHS2.getLocalFS();
    FsPermission expectedFSPermission = new FsPermission(HiveConf.getVar(conf,
        HiveConf.ConfVars.SCRATCH_DIR_PERMISSION));

    // Verify scratch dir paths and permission
    // HDFS scratch dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCH_DIR) + "/" + userName);
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, false);

    // Local scratch dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.LOCAL_SCRATCH_DIR));
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, true);

    // Downloaded resources dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR));
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, true);
    conn.close();

    // 2. Test with doAs=true
    sessionConf="hive.server2.enable.doAs=true";
    // Test for user "neo"
    userName = "neo";
    conn = getConnection(miniHS2.getJdbcURL(testDbName, sessionConf), userName, "the-one");

    // Verify scratch dir paths and permission
    // HDFS scratch dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCH_DIR) + "/" + userName);
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, false);

    // Local scratch dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.LOCAL_SCRATCH_DIR));
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, true);

    // Downloaded resources dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR));
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, true);
    conn.close();

    // Restore original state
    restoreMiniHS2AndConnections();
  }


  /**
   * Test UDF whitelist
   * - verify default value
   * - verify udf allowed with default whitelist
   * - verify udf allowed with specific whitelist
   * - verify udf disallowed when not in whitelist
   * @throws Exception
   */
  @Test
  public void testUdfWhiteBlackList() throws Exception {
    HiveConf testConf = new HiveConf();
    assertTrue(testConf.getVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_WHITELIST).isEmpty());
    // verify that udf in default whitelist can be executed
    Statement stmt = conDefault.createStatement();
    stmt.executeQuery("SELECT substr('foobar', 4) ");
    stmt.close();

    // setup whitelist
    stopMiniHS2();
    Set<String> funcNames = FunctionRegistry.getFunctionNames();
    funcNames.remove("reflect");
    String funcNameStr = "";
    for (String funcName : funcNames) {
      funcNameStr += "," + funcName;
    }
    funcNameStr = funcNameStr.substring(1); // remove ',' at begining
    testConf.setVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_WHITELIST, funcNameStr);
    startMiniHS2(testConf);
    Connection conn =
        getConnection(miniHS2.getJdbcURL(testDbName), System.getProperty("user.name"), "bar");
    stmt = conn.createStatement();
    // verify that udf in whitelist can be executed
    stmt.executeQuery("SELECT substr('foobar', 3) ");
    // verify that udf not in whitelist fails
    try {
      stmt.executeQuery("SELECT reflect('java.lang.String', 'valueOf', 1) ");
      fail("reflect() udf invocation should fail");
    } catch (SQLException e) {
      // expected
    }
    conn.close();

    // Restore original state
    restoreMiniHS2AndConnections();
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
    Statement stmt = conDefault.createStatement();
    // verify that udf in default whitelist can be executed
    stmt.executeQuery("SELECT substr('foobar', 4) ");

    stopMiniHS2();
    testConf.setVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_BLACKLIST, "reflect");
    startMiniHS2(testConf);
    Connection conn =
        getConnection(miniHS2.getJdbcURL(testDbName), System.getProperty("user.name"), "bar");
    stmt = conn.createStatement();

    try {
      stmt.executeQuery("SELECT reflect('java.lang.String', 'valueOf', 1) ");
      fail("reflect() udf invocation should fail");
    } catch (SQLException e) {
      // expected
    }
    conn.close();
    // Restore original state
    restoreMiniHS2AndConnections();
  }

  /** Test UDF blacklist overrides whitelist
   * @throws Exception
   */
  @Test
  public void testUdfBlackListOverride() throws Exception {
    stopMiniHS2();
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
    startMiniHS2(testConf);
    Connection conn =
        getConnection(miniHS2.getJdbcURL(testDbName), System.getProperty("user.name"), "bar");
    Statement stmt = conn.createStatement();

    // verify that udf in black list fails even though it's included in whitelist
    try {
      stmt.executeQuery("SELECT reflect('java.lang.String', 'valueOf', 1) ");
      fail("reflect() udf invocation should fail");
    } catch (SQLException e) {
      // expected
    }
    conn.close();
    // Restore original state
    restoreMiniHS2AndConnections();
  }

  /**
   * Tests the creation of the root hdfs scratch dir, which should be writable by all.
   *
   * @throws Exception
   */
  @Test
  public void testRootScratchDir() throws Exception {
    // Stop HiveServer2
    stopMiniHS2();
    HiveConf conf = new HiveConf();
    String userName;
    Path scratchDirPath;
    conf.set("hive.exec.scratchdir", tmpDir + "/hs2");
    // Start an instance of HiveServer2 which uses miniMR
    startMiniHS2(conf);
    userName = System.getProperty("user.name");
    Connection conn = getConnection(miniHS2.getJdbcURL(testDbName), userName, "password");
    // FS
    FileSystem fs = miniHS2.getLocalFS();
    FsPermission expectedFSPermission = new FsPermission((short)00733);
    // Verify scratch dir paths and permission
    // HDFS scratch dir
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCH_DIR));
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, false);
    conn.close();

    // Test with multi-level scratch dir path
    // Stop HiveServer2
    stopMiniHS2();
    conf.set("hive.exec.scratchdir", tmpDir + "/level1/level2/level3");
    startMiniHS2(conf);
    conn = getConnection(miniHS2.getJdbcURL(testDbName), userName, "password");
    scratchDirPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCH_DIR));
    verifyScratchDir(conf, fs, scratchDirPath, expectedFSPermission, userName, false);
    conn.close();

    // Restore original state
    restoreMiniHS2AndConnections();
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
    stopMiniHS2();
    HiveConf conf = new HiveConf();
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_REQUEST_HEADER_SIZE, 1024);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_RESPONSE_HEADER_SIZE, 1024);
    startMiniHS2(conf, true);

    // Username and password are added to the http request header.
    // We will test the reconfiguration of the header size by changing the password length.
    String userName = "userName";
    String password = StringUtils.leftPad("*", 100);
    Connection conn = null;
    // This should go fine, since header should be less than the configured header size
    try {
      conn = getConnection(miniHS2.getJdbcURL(testDbName), userName, password);
    } catch (Exception e) {
      fail("Not expecting exception: " + e);
    } finally {
      if (conn != null) {
        conn.close();
      }
    }

    // This should fail with given HTTP response code 431 in error message, since header is more
    // than the configured the header size
    password = StringUtils.leftPad("*", 2000);
    Exception headerException = null;
    try {
      conn = null;
      conn = getConnection(miniHS2.getJdbcURL(testDbName), userName, password);
    } catch (Exception e) {
      headerException = e;
    } finally {
      if (conn != null) {
        conn.close();
      }

      assertTrue("Header exception should be thrown", headerException != null);
      assertTrue("Incorrect HTTP Response:" + headerException.getMessage(),
          headerException.getMessage().contains("HTTP Response code: 431"));
    }

    // Stop HiveServer2 to increase header size
    stopMiniHS2();
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_REQUEST_HEADER_SIZE, 3000);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_RESPONSE_HEADER_SIZE, 3000);
    startMiniHS2(conf);

    // This should now go fine, since we increased the configured header size
    try {
      conn = null;
      conn = getConnection(miniHS2.getJdbcURL(testDbName), userName, password);
    } catch (Exception e) {
      fail("Not expecting exception: " + e);
    } finally {
      if (conn != null) {
        conn.close();
      }
    }

    // Restore original state
    restoreMiniHS2AndConnections();
  }

  /**
   * Test for jdbc driver retry on NoHttpResponseException
   * @throws Exception
   */
  @Test
  public void testHttpRetryOnServerIdleTimeout() throws Exception {
    // Stop HiveServer2
    stopMiniHS2();
    HiveConf conf = new HiveConf();
    // Set server's idle timeout to a very low value
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_MAX_IDLE_TIME, "5000");
    startMiniHS2(conf, true);
    String userName = System.getProperty("user.name");
    Connection conn = getConnection(miniHS2.getJdbcURL(testDbName)+";retries=3", userName, "password");
    Statement stmt = conn.createStatement();
    stmt.execute("select from_unixtime(unix_timestamp())");
    // Sleep for longer than server's idletimeout and execute a query
    TimeUnit.SECONDS.sleep(10);
    try {
      stmt.execute("select from_unixtime(unix_timestamp())");
    } catch (Exception e) {
      fail("Not expecting exception: " + e);
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
    // Restore original state
    restoreMiniHS2AndConnections();
  }

  /**
   * Tests that DataNucleus' NucleusContext.classLoaderResolverMap clears cached class objects
   * (& hence doesn't leak classloaders) on closing any session
   *
   * @throws Exception
   */
  @Test
  public void testAddJarDataNucleusUnCaching() throws Exception {
    Path jarFilePath = getHiveContribJarPath();
    // We need a new connection object as we'll check the cache size after connection close
    Connection conn =
        getConnection(miniHS2.getJdbcURL(testDbName), System.getProperty("user.name"), "password");
    Statement stmt = conn.createStatement();
    int mapSizeAfterClose;
    // Add the jar file
    stmt.execute("ADD JAR " + jarFilePath.toString());
    // Create a temporary function using the jar
    stmt.execute("CREATE TEMPORARY FUNCTION add_func AS '" + testUdfClassName + "'");
    ResultSet res = stmt.executeQuery("DESCRIBE FUNCTION add_func");
    checkForNotExist(res);
    // Execute the UDF
    stmt.execute("SELECT add_func(int_col, 1) from " + tableName + " limit 1");
    // Close the connection
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
      pmf = PersistenceManagerProvider.class.getDeclaredField("pmf");
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
    Path jarFilePath = getHiveContribJarPath();
    long cacheBeforeAddJar, cacheAfterClose;
    // Force the cache clear so we know its empty
    invalidateReflectionUtlCache();
    cacheBeforeAddJar = getReflectionUtilCacheSize();
    System.out.println("CONSTRUCTOR_CACHE size before add jar: " + cacheBeforeAddJar);
    System.out.println("CONSTRUCTOR_CACHE as map before add jar:" + getReflectionUtilCache().asMap());
    Assert.assertTrue("FAILED: CONSTRUCTOR_CACHE size before add jar: " + cacheBeforeAddJar,
            cacheBeforeAddJar == 0);

    // Add the jar file
    Statement stmt = conTestDb.createStatement();
    stmt.execute("ADD JAR " + jarFilePath.toString());
    // Create a temporary function using the jar
    stmt.execute("CREATE TEMPORARY FUNCTION add_func AS '" + testUdfClassName + "'");
    // Execute the UDF
    ResultSet res = stmt.executeQuery("SELECT add_func(int_col, 1) from " + tableName + " limit 1");
    assertTrue(res.next());
    TimeUnit.SECONDS.sleep(7);
    // Have to force a cleanup of all expired entries here because its possible that the
    // expired entries will still be counted in Cache.size().
    // Taken from:
    // http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/cache/CacheBuilder.html
    cleanUpReflectionUtlCache();
    cacheAfterClose = getReflectionUtilCacheSize();
    System.out.println("CONSTRUCTOR_CACHE size after connection close: " + cacheAfterClose);
    Assert.assertTrue("FAILED: CONSTRUCTOR_CACHE size after connection close: " + cacheAfterClose,
            cacheAfterClose == 0);
    stmt.execute("DROP TEMPORARY FUNCTION IF EXISTS add_func");
    stmt.close();
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
        modifiersField.setInt(constructorCacheField, constructorCacheField.getModifiers()
            & ~Modifier.FINAL);
        tmp =
            CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.SECONDS).concurrencyLevel(64)
                .weakKeys().weakValues().build();
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
      if (constructorCache != null) {
        constructorCache.invalidateAll();
      }
    } catch (Exception e) {
      System.out.println("Error when trying to invalidate the cache: " + e);
    }
  }

  private void cleanUpReflectionUtlCache() {
    try {
      Cache constructorCache = getReflectionUtilCache();
      if (constructorCache != null) {
        constructorCache.cleanUp();
      }
    } catch (Exception e) {
      System.out.println("Error when trying to cleanUp the cache: " + e);
    }
  }

  private long getReflectionUtilCacheSize() {
    try {
      Cache constructorCache = getReflectionUtilCache();
      if (constructorCache != null) {
        return constructorCache.size();
      }
    } catch (Exception e) {
      System.out.println(e);
    }
    return -1;
  }

  @Test
  public void testPermFunc() throws Exception {
    // This test assumes the hive-contrib JAR has been built as part of the Hive build.
    // Also dependent on the UDFExampleAdd class within that JAR.
    Path jarFilePath = getHiveContribJarPath();
    Statement stmt = conTestDb.createStatement();
    ResultSet res;
    // Add the jar file
    stmt.execute("ADD JAR " + jarFilePath.toString());

    // Register function
    String queryStr =
        "CREATE FUNCTION example_add AS '" + testUdfClassName + "' USING JAR '" + jarFilePath + "'";
    stmt.execute(queryStr);

    // Call describe
    res = stmt.executeQuery("DESCRIBE FUNCTION " + testDbName + ".example_add");
    checkForNotExist(res);

    // Use UDF in query
    res = stmt.executeQuery("SELECT example_add(1, 2) FROM " + tableName + " LIMIT 1");
    assertTrue("query has results", res.next());
    assertEquals(3, res.getInt(1));
    assertFalse("no more results", res.next());

    //try creating same function again which should fail with AlreadyExistsException
    String createSameFunctionAgain =
            "CREATE FUNCTION example_add AS '" + testUdfClassName + "' USING JAR '" + jarFilePath + "'";
    try {
      stmt.execute(createSameFunctionAgain);
    }catch (Exception e){
      assertTrue("recreating same function failed with AlreadyExistsException ", e.getMessage().contains("AlreadyExistsException"));
    }

    // Call describe to see if function still available in registry
    res = stmt.executeQuery("DESCRIBE FUNCTION " + testDbName + ".example_add");
    checkForNotExist(res);

    // A new connection should be able to call describe/use function without issue
    Connection conn2 = getConnection(testDbName);
    Statement stmt2 = conn2.createStatement();
    stmt2.execute("USE " + testDbName);
    res = stmt2.executeQuery("DESCRIBE FUNCTION " + testDbName + ".example_add");
    checkForNotExist(res);

    res =
        stmt2.executeQuery("SELECT " + testDbName + ".example_add(1, 1) FROM " + tableName
            + " LIMIT 1");
    assertTrue("query has results", res.next());
    assertEquals(2, res.getInt(1));
    assertFalse("no more results", res.next());
    conn2.close();
    stmt.execute("DROP FUNCTION IF EXISTS " + testDbName + ".example_add");
    stmt.close();
  }

  private Path getHiveContribJarPath() {
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
    return jarFilePath;
  }

  @Test
  public void testTempTable() throws Exception {
    // Create temp table with current connection
    String tempTableName = "tmp1";
    Statement stmt = conTestDb.createStatement();
    stmt.execute("CREATE TEMPORARY TABLE " + tempTableName + " (key string, value string)");
    stmt.execute("load data local inpath '" + kvDataFilePath.toString() + "' into table "
        + tempTableName);

    String resultVal = "val_238";
    String queryStr = "SELECT * FROM " + tempTableName + " where value = '" + resultVal + "'";

    ResultSet res = stmt.executeQuery(queryStr);
    assertTrue(res.next());
    assertEquals(resultVal, res.getString(2));
    res.close();
    stmt.close();

    // Test getTables()
    DatabaseMetaData md = conTestDb.getMetaData();
    assertTrue(md.getConnection() == conTestDb);

    ResultSet rs = md.getTables(null, null, tempTableName, null);
    boolean foundTable = false;
    while (rs.next()) {
      String tableName = rs.getString(3);
      if (tableName.equalsIgnoreCase(tempTableName)) {
        assertFalse("Table not found yet", foundTable);
        foundTable = true;
      }
    }
    assertTrue("Found temp table", foundTable);

    // Test getTables() with no table name pattern
    rs = md.getTables(null, null, null, null);
    foundTable = false;
    while (rs.next()) {
      String tableName = rs.getString(3);
      if (tableName.equalsIgnoreCase(tempTableName)) {
        assertFalse("Table not found yet", foundTable);
        foundTable = true;
      }
    }
    assertTrue("Found temp table", foundTable);

    // Test getColumns()
    rs = md.getColumns(null, null, tempTableName, null);
    assertTrue("First row", rs.next());
    assertTrue(rs.getString(3).equalsIgnoreCase(tempTableName));
    assertTrue(rs.getString(4).equalsIgnoreCase("key"));
    assertEquals(Types.VARCHAR, rs.getInt(5));

    assertTrue("Second row", rs.next());
    assertTrue(rs.getString(3).equalsIgnoreCase(tempTableName));
    assertTrue(rs.getString(4).equalsIgnoreCase("value"));
    assertEquals(Types.VARCHAR, rs.getInt(5));

    // A second connection should not be able to see the table
    Connection conn2 =
        DriverManager.getConnection(miniHS2.getJdbcURL(testDbName),
            System.getProperty("user.name"), "bar");
    Statement stmt2 = conn2.createStatement();
    stmt2.execute("USE " + testDbName);
    boolean gotException = false;
    try {
      res = stmt2.executeQuery(queryStr);
    } catch (SQLException err) {
      // This is expected to fail.
      assertTrue("Expecting table not found error, instead got: " + err,
          err.getMessage().contains("Table not found"));
      gotException = true;
    }
    assertTrue("Exception while querying non-existing temp table", gotException);
    conn2.close();
  }

  private void checkForNotExist(ResultSet res) throws Exception {
    int numRows = 0;
    while (res.next()) {
      numRows++;
      String strVal = res.getString(1);
      assertEquals("Should not find 'not exist'", -1, strVal.toLowerCase().indexOf("not exist"));
    }
    assertTrue("Rows returned from describe function", numRows > 0);
  }

  @Test
  public void testReplDumpResultSet() throws Exception {
    String tid =
        TestJdbcWithMiniHS2.class.getCanonicalName().toLowerCase().replace('.', '_') + "_"
            + System.currentTimeMillis();
    String testPathName = System.getProperty("test.warehouse.dir", "/tmp") + Path.SEPARATOR + tid;
    Path testPath = new Path(testPathName + Path.SEPARATOR
            + Base64.getEncoder().encodeToString(testDbName.toLowerCase().getBytes(StandardCharsets.UTF_8)));
    FileSystem fs = testPath.getFileSystem(new HiveConf());
    Statement stmt = conDefault.createStatement();
    try {
      stmt.execute("set hive.repl.rootdir = " + testPathName);
      ResultSet rs = stmt.executeQuery("repl dump " + testDbName);
      ResultSetMetaData rsMeta = rs.getMetaData();
      assertEquals(2, rsMeta.getColumnCount());
      int numRows = 0;
      while (rs.next()) {
        numRows++;
        URI uri = new URI(rs.getString(1));
        int notificationId = rs.getInt(2);
        assertNotNull(uri);
        assertEquals(testPath.toUri().getScheme(), uri.getScheme());
        assertEquals(testPath.toUri().getAuthority(), uri.getAuthority());
        // In test setup, we append '/next' to hive.repl.rootdir and use that as the dump location
        assertEquals(testPath.toUri().getPath() + "/next", uri.getPath());
        assertNotNull(notificationId);
      }
      assertEquals(1, numRows);
    } finally {
      // Clean up
      fs.delete(testPath, true);
    }
  }

  public static class SleepMsUDF extends UDF {
    public Integer evaluate(final Integer value, final Integer ms) {
      try {
        Thread.sleep(ms);
      } catch (InterruptedException e) {
        // No-op
      }
      return value;
    }
  }

  private static class ExceptionHolder {
    Throwable throwable;
  }

  @Test
  public void testFetchSize() throws Exception {
    Connection fsConn = getConnection(miniHS2.getJdbcURL("default", "fetchSize=50", ""),
      System.getProperty("user.name"), "bar");
    Statement stmt = fsConn.createStatement();
    stmt.execute("set");
    assertEquals(50, stmt.getFetchSize());
    stmt.close();
    fsConn.close();
  }

  /**
   * A test that checks that Lineage is correct when a multiple concurrent
   * requests are make on a connection
   */
  @Test
  public void testConcurrentLineage() throws Exception {
    // setup to run concurrent operations
    Statement stmt = conTestDb.createStatement();
    setSerializeInTasksInConf(stmt);
    stmt.execute("drop table if exists testConcurrentLineage1");
    stmt.execute("drop table if exists testConcurrentLineage2");
    stmt.execute("create table testConcurrentLineage1 (col1 int)");
    stmt.execute("create table testConcurrentLineage2 (col2 int)");

    // clear vertices list
    ReadableHook.clear();

    // run 5 sql inserts concurrently
    int numThreads = 5;        // set to 1 for single threading
    int concurrentCalls = 5;
    ExecutorService pool = Executors.newFixedThreadPool(numThreads);
    try {
      List<InsertCallable> tasks = new ArrayList<>();
      for (int i = 0; i < concurrentCalls; i++) {
        InsertCallable runner = new InsertCallable(conTestDb);
        tasks.add(runner);
      }
      List<Future<Void>> futures = pool.invokeAll(tasks);
      for (Future<Void> future : futures) {
        future.get(20, TimeUnit.SECONDS);
      }
      // check to see that the vertices are correct
      checkVertices();
    } finally {
      // clean up
      stmt.execute("drop table testConcurrentLineage1");
      stmt.execute("drop table testConcurrentLineage2");
      stmt.close();
      pool.shutdownNow();
    }
  }

  /**
   * A Callable that does 2 inserts
   */
  private class InsertCallable implements Callable<Void> {
    private Connection connection;

    InsertCallable(Connection conn) {
      this.connection = conn;
    }

    @Override public Void call() throws Exception {
      doLineageInserts(connection);
      return null;
    }

    private void doLineageInserts(Connection connection) throws SQLException {
      Statement stmt = connection.createStatement();
      stmt.execute("insert into testConcurrentLineage1 values (1)");
      stmt.execute("insert into testConcurrentLineage2 values (2)");
    }
  }
  /**
   * check to see that the vertices derived from the HookContexts are correct
   */
  private void checkVertices() {
    List<Set<LineageLogger.Vertex>> verticesLists = getVerticesFromHooks();

    assertEquals("5 runs of 2 inserts makes 10", 10, verticesLists.size());
    for (Set<LineageLogger.Vertex> vertices : verticesLists) {
      assertFalse("Each insert affects a column so should be some vertices",
          vertices.isEmpty());
      assertEquals("Each insert affects one column so should be one vertex",
          1, vertices.size());
      Iterator<LineageLogger.Vertex> iterator = vertices.iterator();
      assertTrue(iterator.hasNext());
      LineageLogger.Vertex vertex = iterator.next();
      assertEquals(0, vertex.getId());
      assertEquals(LineageLogger.Vertex.Type.COLUMN, vertex.getType());
      String label = vertex.getLabel();
      System.out.println("vertex.getLabel() = " + label);
      assertTrue("did not see one of the 2 expected column names",
          label.equals("testjdbcminihs2.testconcurrentlineage1.col1") ||
              label.equals("testjdbcminihs2.testconcurrentlineage2.col2"));
    }
  }

  /**
   * Use the logic in LineageLogger to get vertices from Hook Contexts
   */
  private List<Set<LineageLogger.Vertex>>  getVerticesFromHooks() {
    List<Set<LineageLogger.Vertex>> verticesLists = new ArrayList<>();
    List<HookContext> hookList = ReadableHook.getHookList();
    for (HookContext hookContext : hookList) {
      QueryPlan plan = hookContext.getQueryPlan();
      LineageCtx.Index index = hookContext.getIndex();
      assertNotNull(index);
      List<LineageLogger.Edge> edges = LineageLogger.getEdges(plan, index);
      Set<LineageLogger.Vertex> vertices = LineageLogger.getVertices(edges);
      verticesLists.add(vertices);
    }
    return verticesLists;
  }

  /**
   * Test 'describe extended' on tables that have special white space characters in the row format.
   */
  @Test
  public void testDescribe() throws Exception {
    try (Statement stmt = conTestDb.createStatement()) {
      String table = "testDescribe";
      stmt.execute("drop table if exists " + table);
      stmt.execute("create table " + table + " (orderid int, orderdate string, customerid int)"
          + " ROW FORMAT DELIMITED FIELDS terminated by '\\t' LINES terminated by '\\n'");
      String extendedDescription = getDetailedTableDescription(stmt, table);
      assertNotNull("could not get Detailed Table Information", extendedDescription);
      assertTrue("description appears truncated: " + extendedDescription,
          extendedDescription.endsWith(")"));
      assertTrue("bad line delimiter: " + extendedDescription,
          extendedDescription.contains("line.delim=\\n"));
      assertTrue("bad field delimiter: " + extendedDescription,
          extendedDescription.contains("field.delim=\\t"));

      String view = "testDescribeView";
      stmt.execute("create view " + view + " as select * from " + table);
      String extendedViewDescription = getDetailedTableDescription(stmt, view);
      assertTrue("bad view text: " + extendedViewDescription,
          extendedViewDescription.contains("viewOriginalText:select * from " + table));
      assertTrue("bad expanded view text: " + extendedViewDescription,
          extendedViewDescription.contains(
              "viewExpandedText:select `testdescribe`.`orderid`, `testdescribe`.`orderdate`, "
                  + "`testdescribe`.`customerid` from `testjdbcminihs2`"));
    }
  }

  /**
   * Get Detailed Table Information via jdbc
   */
  static String getDetailedTableDescription(Statement stmt, String table) throws SQLException {
    String extendedDescription = null;
    try (ResultSet rs = stmt.executeQuery("describe extended " + table)) {
      while (rs.next()) {
        String out = rs.getString(1);
        String tableInfo = rs.getString(2);
        if ("Detailed Table Information".equals(out)) { // from TextMetaDataFormatter
          extendedDescription = tableInfo;
        }
      }
    }
    return extendedDescription;
  }

  @Test
  public void testCustomPathsForCTLV() throws Exception {
    try (Statement stmt = conTestDb.createStatement()) {
      // Initialize
      stmt.execute("CREATE TABLE emp_table (id int, name string, salary int)");
      stmt.execute("insert into emp_table values(1,'aaaaa',20000)");
      stmt.execute("CREATE VIEW emp_view AS SELECT * FROM emp_table WHERE salary>10000");
      String customPath = System.getProperty("test.tmp.dir") + "/custom";

      //Test External CTLV
      String extPath = customPath + "/emp_ext_table";
      stmt.execute("CREATE EXTERNAL TABLE emp_ext_table like emp_view STORED AS PARQUET LOCATION '" + extPath + "'");
      assertTrue(getDetailedTableDescription(stmt, "emp_ext_table").contains(extPath));

      //Test Managed CTLV
      String mndPath = customPath + "/emp_mm_table";
      stmt.execute("CREATE TABLE emp_mm_table like emp_view STORED AS ORC LOCATION '" + mndPath + "'");
      assertTrue(getDetailedTableDescription(stmt, "emp_mm_table").contains(mndPath));
    }
  }

  @Test
  public void testInterruptPollingState() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(1);
    final CountDownLatch latch = new CountDownLatch(1);
    final Object[] results = new Object[2];
    results[0] = false;
    Future future = pool.submit(new Callable<Void>() {
      @Override
      public Void call() {
        try (Statement stmt = conTestDb.createStatement()) {
          stmt.execute("create temporary function sleepMsUDF as '" + SleepMsUDF.class.getName() + "'");
          stmt.execute("SELECT sleepMsUDF(1, 10000)");
          results[0] = true;
        } catch (Exception e) {
          results[1] = e;
        } finally {
          latch.countDown();
        }
        return null;
      }
    });
    Thread.sleep(2000);
    future.cancel(true);
    latch.await();
    assertEquals(false, results[0]);
    assertEquals("Interrupted while polling on the operation status", ((Exception)results[1]).getMessage());
  }
}
