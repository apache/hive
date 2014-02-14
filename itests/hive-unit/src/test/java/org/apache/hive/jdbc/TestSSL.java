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
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSSL {
  private static final String KEY_STORE_NAME = "keystore.jks";
  private static final String TRUST_STORE_NAME = "truststore.jks";
  private static final String KEY_STORE_PASSWORD = "HiveJdbc";
  private static final String JAVA_TRUST_STORE_PROP = "javax.net.ssl.trustStore";
  private static final String JAVA_TRUST_STORE_PASS_PROP = "javax.net.ssl.trustStorePassword";
  private static final String HS2_BINARY_MODE = "binary";
  private static final String HS2_HTTP_MODE = "http";
  private static final String HS2_HTTP_ENDPOINT = "cliservice";
  private static final String HS2_BINARY_AUTH_MODE = "NONE";
  private static final String HS2_HTTP_AUTH_MODE = "NOSASL";

  private MiniHS2 miniHS2 = null;
  private static HiveConf conf = new HiveConf();
  private Connection hs2Conn = null;
  private String dataFileDir = conf.get("test.data.files");
  private Map<String, String> confOverlay;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
  }

  @Before
  public void setUp() throws Exception {
    DriverManager.setLoginTimeout(0);
    if (!System.getProperty("test.data.files", "").isEmpty()) {
      dataFileDir = System.getProperty("test.data.files");
    }
    dataFileDir = dataFileDir.replace('\\', '/').replace("c:", "");
    miniHS2 = new MiniHS2(conf);
    confOverlay = new HashMap<String, String>();
  }

  @After
  public void tearDown() throws Exception {
    if (hs2Conn != null) {
      hs2Conn.close();
    }
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
    System.clearProperty(JAVA_TRUST_STORE_PROP);
    System.clearProperty(JAVA_TRUST_STORE_PASS_PROP);
  }

  /***
   * Test SSL client with non-SSL server fails
   * @throws Exception
   */
  @Test
  public void testInvalidConfig() throws Exception {
    clearSslConfOverlay(confOverlay);
    // Test in binary mode
    setBinaryConfOverlay(confOverlay);
    miniHS2.start(confOverlay);
    DriverManager.setLoginTimeout(4);
    try {
      hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL() + ";ssl=true;sslTrustStore=" +
          dataFileDir + File.separator + TRUST_STORE_NAME + ";trustStorePassword=" +
          KEY_STORE_PASSWORD, System.getProperty("user.name"), "bar");
      fail("SSL connection should fail with NON-SSL server");
    } catch (SQLException e) {
      // expected error
      assertEquals("08S01", e.getSQLState().trim());
    }

    System.setProperty(JAVA_TRUST_STORE_PROP, dataFileDir + File.separator + TRUST_STORE_NAME );
    System.setProperty(JAVA_TRUST_STORE_PASS_PROP, KEY_STORE_PASSWORD);
    try {
      hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL() + ";ssl=true",
          System.getProperty("user.name"), "bar");
      fail("SSL connection should fail with NON-SSL server");
    } catch (SQLException e) {
      // expected error
      assertEquals("08S01", e.getSQLState().trim());
    }
    miniHS2.stop();

    // Test in http mode with ssl properties specified in url
    System.clearProperty(JAVA_TRUST_STORE_PROP);
    System.clearProperty(JAVA_TRUST_STORE_PASS_PROP);
    setHttpConfOverlay(confOverlay);
    miniHS2.start(confOverlay);
    try {
      hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL() +
          ";ssl=true;sslTrustStore=" + dataFileDir + File.separator +
          TRUST_STORE_NAME + ";trustStorePassword=" + KEY_STORE_PASSWORD +
          "?hive.server2.transport.mode=" + HS2_HTTP_MODE +
          ";hive.server2.thrift.http.path=" + HS2_HTTP_ENDPOINT,
          System.getProperty("user.name"), "bar");
      fail("SSL connection should fail with NON-SSL server");
    } catch (SQLException e) {
      // expected error
      assertEquals("08S01", e.getSQLState().trim());
    }
  }

  /***
   * Test non-SSL client with SSL server fails
   * @throws Exception
   */
  @Test
  public void testConnectionMismatch() throws Exception {
    setSslConfOverlay(confOverlay);
    // Test in binary mode
    setBinaryConfOverlay(confOverlay);
    miniHS2.start(confOverlay);
    // Start HS2 with SSL
    try {
      hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
      fail("NON SSL connection should fail with SSL server");
    } catch (SQLException e) {
      // expected error
      assertEquals("08S01", e.getSQLState().trim());
    }

    try {
      hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL()+ ";ssl=false",
          System.getProperty("user.name"), "bar");
      fail("NON SSL connection should fail with SSL server");
    } catch (SQLException e) {
      // expected error
      assertEquals("08S01", e.getSQLState().trim());
    }
    miniHS2.stop();

    // Test in http mode
    setHttpConfOverlay(confOverlay);
    miniHS2.start(confOverlay);
    try {
      hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL() +
          ";ssl=false;sslTrustStore=" + dataFileDir + File.separator +
          TRUST_STORE_NAME + ";trustStorePassword=" + KEY_STORE_PASSWORD +
          "?hive.server2.transport.mode=" + HS2_HTTP_MODE +
          ";hive.server2.thrift.http.path=" + HS2_HTTP_ENDPOINT,
          System.getProperty("user.name"), "bar");
      fail("NON SSL connection should fail with SSL server");
    } catch (SQLException e) {
      // expected error
      assertEquals("08S01", e.getSQLState().trim());
    }

  }

  /***
   * Test SSL client connection to SSL server
   * @throws Exception
   */
  @Test
  public void testSSLConnectionWithURL() throws Exception {
    setSslConfOverlay(confOverlay);
    // Test in binary mode
    setBinaryConfOverlay(confOverlay);
    // Start HS2 with SSL
    miniHS2.start(confOverlay);

    // make SSL connection
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL() + ";ssl=true;sslTrustStore=" +
        dataFileDir + File.separator + TRUST_STORE_NAME + ";trustStorePassword=" +
        KEY_STORE_PASSWORD, System.getProperty("user.name"), "bar");
    hs2Conn.close();
    miniHS2.stop();

    // Test in http mode
    setHttpConfOverlay(confOverlay);
    miniHS2.start(confOverlay);
    // make SSL connection
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL() +
        ";ssl=true;sslTrustStore=" + dataFileDir + File.separator +
        TRUST_STORE_NAME + ";trustStorePassword=" + KEY_STORE_PASSWORD +
        "?hive.server2.transport.mode=" + HS2_HTTP_MODE +
        ";hive.server2.thrift.http.path=" + HS2_HTTP_ENDPOINT,
        System.getProperty("user.name"), "bar");
    hs2Conn.close();
  }

  /***
   * Test SSL client connection to SSL server
   * @throws Exception
   */
  @Test
  public void testSSLConnectionWithProperty() throws Exception {
    setSslConfOverlay(confOverlay);
    // Test in binary mode
    setBinaryConfOverlay(confOverlay);
    // Start HS2 with SSL
    miniHS2.start(confOverlay);

    System.setProperty(JAVA_TRUST_STORE_PROP, dataFileDir + File.separator + TRUST_STORE_NAME );
    System.setProperty(JAVA_TRUST_STORE_PASS_PROP, KEY_STORE_PASSWORD);
    // make SSL connection
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL() + ";ssl=true",
        System.getProperty("user.name"), "bar");
    hs2Conn.close();
    miniHS2.stop();

    // Test in http mode
    setHttpConfOverlay(confOverlay);
    miniHS2.start(confOverlay);
    // make SSL connection
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL() +
        ";ssl=true;" + "?hive.server2.transport.mode=" + HS2_HTTP_MODE +
        ";hive.server2.thrift.http.path=" + HS2_HTTP_ENDPOINT,
        System.getProperty("user.name"), "bar");
    hs2Conn.close();
  }

  /**
   * Start HS2 in SSL mode, open a SSL connection and fetch data
   * @throws Exception
   */
  @Test
  public void testSSLFetch() throws Exception {
    setSslConfOverlay(confOverlay);
    // Test in binary mode
    setBinaryConfOverlay(confOverlay);
    // Start HS2 with SSL
    miniHS2.start(confOverlay);

    String tableName = "sslTab";
    Path dataFilePath = new Path(dataFileDir, "kv1.txt");

    // make SSL connection
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL() + ";ssl=true;sslTrustStore=" +
        dataFileDir + File.separator + TRUST_STORE_NAME + ";trustStorePassword=" +
        KEY_STORE_PASSWORD, System.getProperty("user.name"), "bar");

    // Set up test data
    setupTestTableWithData(tableName, dataFilePath, hs2Conn);

    Statement stmt = hs2Conn.createStatement();
    ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
    int rowCount = 0;
    while (res.next()) {
      ++rowCount;
      assertEquals("val_" + res.getInt(1), res.getString(2));
    }
    // read result over SSL
    assertEquals(500, rowCount);

    hs2Conn.close();
  }

  /**
   * Start HS2 in Http mode with SSL enabled, open a SSL connection and fetch data
   * @throws Exception
   */
  @Test
  public void testSSLFetchHttp() throws Exception {
    setSslConfOverlay(confOverlay);
    // Test in http mode
    setHttpConfOverlay(confOverlay);
    miniHS2.start(confOverlay);

    String tableName = "sslTab";
    Path dataFilePath = new Path(dataFileDir, "kv1.txt");

    // make SSL connection
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL() +
        ";ssl=true;sslTrustStore=" + dataFileDir + File.separator +
        TRUST_STORE_NAME + ";trustStorePassword=" + KEY_STORE_PASSWORD +
        "?hive.server2.transport.mode=" + HS2_HTTP_MODE +
        ";hive.server2.thrift.http.path=" + HS2_HTTP_ENDPOINT,
        System.getProperty("user.name"), "bar");

    // Set up test data
    setupTestTableWithData(tableName, dataFilePath, hs2Conn);
    Statement stmt = hs2Conn.createStatement();
    ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
    int rowCount = 0;
    while (res.next()) {
      ++rowCount;
      assertEquals("val_" + res.getInt(1), res.getString(2));
    }
    // read result over SSL
    assertEquals(500, rowCount);

    hs2Conn.close();
  }

  private void setupTestTableWithData(String tableName, Path dataFilePath,
      Connection hs2Conn) throws Exception {
    Statement stmt = hs2Conn.createStatement();
    stmt.execute("set hive.support.concurrency = false");

    stmt.execute("drop table if exists " + tableName);
    stmt.execute("create table " + tableName
        + " (under_col int comment 'the under column', value string)");

    // load data
    stmt.execute("load data local inpath '"
        + dataFilePath.toString() + "' into table " + tableName);
    stmt.close();
  }

  private void setSslConfOverlay(Map<String, String> confOverlay) {
    confOverlay.put(ConfVars.HIVE_SERVER2_USE_SSL.varname, "true");
    confOverlay.put(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname,
        dataFileDir + File.separator +  KEY_STORE_NAME);
    confOverlay.put(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname,
        KEY_STORE_PASSWORD);
  }

  private void clearSslConfOverlay(Map<String, String> confOverlay) {
    confOverlay.put(ConfVars.HIVE_SERVER2_USE_SSL.varname, "false");
  }

  // Currently http mode works with server in NOSASL auth mode & doesn't support doAs
  private void setHttpConfOverlay(Map<String, String> confOverlay) {
    confOverlay.put(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, HS2_HTTP_MODE);
    confOverlay.put(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname, HS2_HTTP_ENDPOINT);
    confOverlay.put(ConfVars.HIVE_SERVER2_AUTHENTICATION.varname,  HS2_HTTP_AUTH_MODE);
    confOverlay.put(ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "false");
  }

  private void setBinaryConfOverlay(Map<String, String> confOverlay) {
    confOverlay.put(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, HS2_BINARY_MODE);
    confOverlay.put(ConfVars.HIVE_SERVER2_AUTHENTICATION.varname,  HS2_BINARY_AUTH_MODE);
    confOverlay.put(ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "true");
  }
}
