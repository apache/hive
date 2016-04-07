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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSessionHook;
import org.apache.hive.service.cli.session.HiveSessionHookContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class is cloned from TestJdbcWithMiniMR, except use Namenode HA.
 */
public class TestJdbcWithMiniHA {
  public static final String TEST_TAG = "miniHS2.miniHA.tag";
  public static final String TEST_TAG_VALUE = "miniHS2.miniHA.value";
  public static class HATestSessionHook implements HiveSessionHook {
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

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    String dataFileDir = conf.get("test.data.files").replace('\\', '/')
        .replace("c:", "");
    dataFilePath = new Path(dataFileDir, "kv1.txt");
    DriverManager.setLoginTimeout(0);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    miniHS2 = new MiniHS2.Builder().withConf(conf).withMiniMR().withHA().build();
    Map<String, String> overlayProps = new HashMap<String, String>();
    overlayProps.put(ConfVars.HIVE_SERVER2_SESSION_HOOK.varname,
        HATestSessionHook.class.getName());
    miniHS2.start(overlayProps);
    assertTrue(HAUtil.isHAEnabled(conf, DFSUtil.getNamenodeNameServiceId(conf)));
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
   * Verify that the connection to MiniHS2 is successful
   * @throws Exception
   */
  @Test
  public void testConnection() throws Exception {
    // the session hook should set the property
    verifyProperty(TEST_TAG, TEST_TAG_VALUE);
  }

  /**
   * Run nonMr query
   * @throws Exception
   */
  @Test
  public void testNonMrQuery() throws Exception {
    String tableName = "testTab1";
    String resultVal = "val_238";
    String queryStr = "SELECT * FROM " + tableName;

    testKvQuery(tableName, queryStr, resultVal);
  }

  /**
   * Run nonMr query
   * @throws Exception
   */
  @Test
  public void testMrQuery() throws Exception {
    String tableName = "testTab2";
    String resultVal = "val_238";
    String queryStr = "SELECT * FROM " + tableName +
        " where value = '" + resultVal + "'";

    testKvQuery(tableName, queryStr, resultVal);
  }

  /**
   * Verify if the given property contains the expected value
   * @param propertyName
   * @param expectedValue
   * @throws Exception
   */
  private void verifyProperty(String propertyName, String expectedValue) throws Exception {
    Statement stmt = hs2Conn .createStatement();
    ResultSet res = stmt.executeQuery("set " + propertyName);
    assertTrue(res.next());
    String results[] = res.getString(1).split("=");
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
