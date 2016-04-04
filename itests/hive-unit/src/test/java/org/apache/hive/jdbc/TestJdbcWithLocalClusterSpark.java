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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.jdbc.miniHS2.MiniHS2.MiniClusterType;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSessionHook;
import org.apache.hive.service.cli.session.HiveSessionHookContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class is cloned from TestJdbcWithMiniMR, except use Spark as the execution engine.
 */
public class TestJdbcWithLocalClusterSpark {
  public static final String TEST_TAG = "miniHS2.localClusterSpark.tag";
  public static final String TEST_TAG_VALUE = "miniHS2.localClusterSpark.value";
  public static class LocalClusterSparkSessionHook implements HiveSessionHook {
    @Override
    public void run(HiveSessionHookContext sessionHookContext) throws HiveSQLException {
      sessionHookContext.getSessionConf().set(TEST_TAG, TEST_TAG_VALUE);
    }
  }

  private static MiniHS2 miniHS2 = null;
  private static HiveConf conf;
  private static Path dataFilePath;
  private static String  dbName = "mrTestDb";
  private Connection hs2Conn = null;
  private Statement stmt;

  private static HiveConf createHiveConf() {
    HiveConf conf = new HiveConf();
    conf.set("hive.execution.engine", "spark");
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.set("spark.master", "local-cluster[2,2,1024]");
    return conf;
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    conf = createHiveConf();
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    String dataFileDir = conf.get("test.data.files").replace('\\', '/')
        .replace("c:", "");
    dataFilePath = new Path(dataFileDir, "kv1.txt");
    DriverManager.setLoginTimeout(0);
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    miniHS2 = new MiniHS2(conf, MiniClusterType.MR);
    Map<String, String> overlayProps = new HashMap<String, String>();
    overlayProps.put(ConfVars.HIVE_SERVER2_SESSION_HOOK.varname,
        LocalClusterSparkSessionHook.class.getName());
    miniHS2.start(overlayProps);
    createDb();
  }

  // setup DB
  private static void createDb() throws Exception {
    Connection conn = DriverManager.
        getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
    Statement stmt2 = conn.createStatement();
    stmt2.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    stmt2.execute("CREATE DATABASE " + dbName);
    stmt2.close();
    conn.close();
  }

  @Before
  public void setUp() throws Exception {
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL(dbName),
        System.getProperty("user.name"), "bar");
    stmt = hs2Conn.createStatement();
    stmt.execute("USE " + dbName);
  }

  @After
  public void tearDown() throws Exception {
    if (hs2Conn != null) {
      hs2Conn.close();
    }
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  /**
   * Verify that the connection to HS2 with MiniMr is successful.
   * @throws Exception
   */
  @Test
  public void testConnection() throws Exception {
    // the session hook should set the property
    verifyProperty(TEST_TAG, TEST_TAG_VALUE);
  }

  /**
   * Run nonMr query.
   * @throws Exception
   */
  @Test
  public void testNonSparkQuery() throws Exception {
    String tableName = "testTab1";
    String resultVal = "val_238";
    String queryStr = "SELECT * FROM " + tableName;

    testKvQuery(tableName, queryStr, resultVal);
  }

  /**
   * Run nonMr query.
   * @throws Exception
   */
  @Test
  public void testSparkQuery() throws Exception {
    String tableName = "testTab2";
    String resultVal = "val_238";
    String queryStr = "SELECT * FROM " + tableName
        + " where value = '" + resultVal + "'";

    testKvQuery(tableName, queryStr, resultVal);
  }

  @Test
  public void testPermFunc() throws Exception {

    // This test assumes the hive-contrib JAR has been built as part of the Hive build.
    // Also dependent on the UDFExampleAdd class within that JAR.
    String udfClassName = "org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd";
    String mvnRepo = System.getProperty("maven.local.repository");
    String hiveVersion = System.getProperty("hive.version");
    String jarFileName = "hive-contrib-" + hiveVersion + ".jar";
    String[] pathParts = {
        "org", "apache", "hive",
        "hive-contrib", hiveVersion, jarFileName
    };

    // Create path to hive-contrib JAR on local filesystem
    Path contribJarPath = new Path(mvnRepo);
    for (String pathPart : pathParts) {
      contribJarPath = new Path(contribJarPath, pathPart);
    }
    FileSystem localFs = FileSystem.getLocal(conf);
    assertTrue("Hive contrib JAR exists at " + contribJarPath, localFs.exists(contribJarPath));

    String hdfsJarPathStr = "hdfs:///" + jarFileName;
    Path hdfsJarPath = new Path(hdfsJarPathStr);

    // Copy JAR to DFS
    FileSystem dfs = miniHS2.getDFS().getFileSystem();
    dfs.copyFromLocalFile(contribJarPath, hdfsJarPath);
    assertTrue("Verify contrib JAR copied to HDFS at " + hdfsJarPath, dfs.exists(hdfsJarPath));

    // Register function
    String queryStr = "CREATE FUNCTION example_add AS '" + udfClassName + "'"
        + " USING JAR '" + hdfsJarPathStr + "'";
    stmt.execute(queryStr);

    // Call describe
    ResultSet res;
    res = stmt.executeQuery("DESCRIBE FUNCTION " + dbName + ".example_add");
    checkForNotExist(res);

    // Use UDF in query
    String tableName = "testTab3";
    setupKv1Tabs(tableName);
    res = stmt.executeQuery("SELECT EXAMPLE_ADD(1, 2) FROM " + tableName + " LIMIT 1");
    assertTrue("query has results", res.next());
    assertEquals(3, res.getInt(1));
    assertFalse("no more results", res.next());

    // A new connection should be able to call describe/use function without issue
    Connection conn2 = DriverManager.getConnection(miniHS2.getJdbcURL(dbName),
        System.getProperty("user.name"), "bar");
    Statement stmt2 = conn2.createStatement();
    stmt2.execute("USE " + dbName);
    res = stmt2.executeQuery("DESCRIBE FUNCTION " + dbName + ".example_add");
    checkForNotExist(res);

    res = stmt2.executeQuery("SELECT " + dbName + ".example_add(1, 1) FROM " + tableName + " LIMIT 1");
    assertTrue("query has results", res.next());
    assertEquals(2, res.getInt(1));
    assertFalse("no more results", res.next());

    stmt.execute("DROP TABLE " + tableName);
  }

  @Test
  public void testTempTable() throws Exception {
    // Create temp table with current connection
    String tempTableName = "tmp1";
    stmt.execute("CREATE TEMPORARY TABLE " + tempTableName + " (key string, value string)");
    stmt.execute("load data local inpath '"
        + dataFilePath.toString() + "' into table " + tempTableName);

    String resultVal = "val_238";
    String queryStr = "SELECT * FROM " + tempTableName
        + " where value = '" + resultVal + "'";
    verifyResult(queryStr, resultVal, 2);

    // A second connection should not be able to see the table
    Connection conn2 = DriverManager.getConnection(miniHS2.getJdbcURL(dbName),
        System.getProperty("user.name"), "bar");
    Statement stmt2 = conn2.createStatement();
    stmt2.execute("USE " + dbName);
    boolean gotException = false;
    try {
      stmt2.executeQuery(queryStr);
    } catch (SQLException err) {
      // This is expected to fail.
      assertTrue("Expecting table not found error, instead got: " + err,
          err.getMessage().contains("Table not found"));
      gotException = true;
    }
    assertTrue("Exception while querying non-existing temp table", gotException);
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

  /**
   * Verify if the given property contains the expected value.
   * @param propertyName
   * @param expectedValue
   * @throws Exception
   */
  private void verifyProperty(String propertyName, String expectedValue) throws Exception {
    Statement stmt = hs2Conn .createStatement();
    ResultSet res = stmt.executeQuery("set " + propertyName);
    assertTrue(res.next());
    String[] results = res.getString(1).split("=");
    assertEquals("Property should be set", results.length, 2);
    assertEquals("Property should be set", expectedValue, results[1]);
  }

  // create tables, verify query
  private void testKvQuery(String tableName, String queryStr, String resultVal)
      throws SQLException {
    setupKv1Tabs(tableName);
    verifyResult(queryStr, resultVal, 2);
    stmt.execute("DROP TABLE " + tableName);
  }

  // create table and pupulate with kv1.txt
  private void setupKv1Tabs(String tableName) throws SQLException {
    Statement stmt = hs2Conn.createStatement();
    // create table
    stmt.execute("CREATE TABLE " + tableName
        + " (under_col INT COMMENT 'the under column', value STRING)"
        + " COMMENT ' test table'");

    // load data
    stmt.execute("load data local inpath '"
        + dataFilePath.toString() + "' into table " + tableName);
  }

  // run given query and validate expecated result
  private void verifyResult(String queryStr, String expString, int colPos)
      throws SQLException {
    ResultSet res = stmt.executeQuery(queryStr);
    assertTrue(res.next());
    assertEquals(expString, res.getString(colPos));
    res.close();
  }
}
