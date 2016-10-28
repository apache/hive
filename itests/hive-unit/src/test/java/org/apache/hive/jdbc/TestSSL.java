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
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSSL {
  private static final Logger LOG = LoggerFactory.getLogger(TestSSL.class);
  private static final String LOCALHOST_KEY_STORE_NAME = "keystore.jks";
  private static final String EXAMPLEDOTCOM_KEY_STORE_NAME = "keystore_exampledotcom.jks";
  private static final String TRUST_STORE_NAME = "truststore.jks";
  private static final String KEY_STORE_TRUST_STORE_PASSWORD = "HiveJdbc";
  private static final String JAVA_TRUST_STORE_PROP = "javax.net.ssl.trustStore";
  private static final String JAVA_TRUST_STORE_PASS_PROP = "javax.net.ssl.trustStorePassword";
  private static final String HS2_BINARY_MODE = "binary";
  private static final String HS2_HTTP_MODE = "http";
  private static final String HS2_HTTP_ENDPOINT = "cliservice";
  private static final String HS2_BINARY_AUTH_MODE = "NONE";

  private MiniHS2 miniHS2 = null;
  private static HiveConf conf = new HiveConf();
  private Connection hs2Conn = null;
  private String dataFileDir = conf.get("test.data.files");
  private Map<String, String> confOverlay;
  private final String SSL_CONN_PARAMS = "ssl=true;sslTrustStore="
      + URLEncoder.encode(dataFileDir + File.separator + TRUST_STORE_NAME) + ";trustStorePassword="
      + KEY_STORE_TRUST_STORE_PASSWORD;

  @BeforeClass
  public static void beforeTest() throws Exception {
    MiniHS2.cleanupLocalDir();
    Class.forName(MiniHS2.getJdbcDriverName());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    MiniHS2.cleanupLocalDir();
  }

  @Before
  public void setUp() throws Exception {
    DriverManager.setLoginTimeout(0);
    if (!System.getProperty("test.data.files", "").isEmpty()) {
      dataFileDir = System.getProperty("test.data.files");
    }
    dataFileDir = dataFileDir.replace('\\', '/').replace("c:", "");
    miniHS2 = new MiniHS2.Builder().withConf(conf).cleanupLocalDirOnStartup(false).build();
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

  private int execCommand(String cmd) throws Exception {
    int exitCode;
    try {
      String output = Shell.execCommand("bash", "-c", cmd);
      LOG.info("Output from '" + cmd + "': " + output) ;
      exitCode = 0;
    } catch (Shell.ExitCodeException e) {
      exitCode = e.getExitCode();
      LOG.info("Error executing '" + cmd + "', exitCode = " + exitCode, e);
    }
    return exitCode;
  }

  /***
   * Tests to ensure SSLv2 and SSLv3 are disabled
   */
  @Test
  @Ignore("Temporarily disable until fixed")
  public void testSSLVersion() throws Exception {
    // we need openssl
    Assume.assumeTrue(execCommand("which openssl") == 0);
    // we depend on linux openssl exit codes
    Assume.assumeTrue(System.getProperty("os.name").toLowerCase().contains("linux"));

    setSslConfOverlay(confOverlay);
    // Test in binary mode
    setBinaryConfOverlay(confOverlay);
    // Start HS2 with SSL
    miniHS2.start(confOverlay);

    // make SSL connection
    hs2Conn =
        DriverManager.getConnection(miniHS2.getJdbcURL() + ";ssl=true;sslTrustStore=" + dataFileDir
            + File.separator + TRUST_STORE_NAME + ";trustStorePassword=" + KEY_STORE_TRUST_STORE_PASSWORD,
            System.getProperty("user.name"), "bar");
    hs2Conn.close();
    Assert.assertEquals("Expected exit code of 1", 1, execCommand("openssl s_client -connect "
        + miniHS2.getHost() + ":" + miniHS2.getBinaryPort() + " -ssl2 < /dev/null"));
    Assert.assertEquals("Expected exit code of 1", 1, execCommand("openssl s_client -connect "
        + miniHS2.getHost() + ":" + miniHS2.getBinaryPort() + " -ssl3 < /dev/null"));
    miniHS2.stop();

    // Test in http mode
    setHttpConfOverlay(confOverlay);
    miniHS2.start(confOverlay);
    // make SSL connection
    try {
      hs2Conn =
          DriverManager.getConnection(miniHS2.getJdbcURL() + ";ssl=true;sslTrustStore="
              + dataFileDir + File.separator + TRUST_STORE_NAME + ";trustStorePassword="
              + KEY_STORE_TRUST_STORE_PASSWORD, System.getProperty("user.name"), "bar");
      Assert.fail("Expected SQLException during connect");
    } catch (SQLException e) {
      LOG.info("Expected exception: " + e, e);
      Assert.assertEquals("08S01", e.getSQLState().trim());
      Throwable cause = e.getCause();
      Assert.assertNotNull(cause);
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      Assert.assertEquals("org.apache.http.NoHttpResponseException", cause.getClass().getName());
      Assert.assertTrue(cause.getMessage().contains("failed to respond"));
    }
    miniHS2.stop();
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
      hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL("default", SSL_CONN_PARAMS),
          System.getProperty("user.name"), "bar");
      fail("SSL connection should fail with NON-SSL server");
    } catch (SQLException e) {
      // expected error
      assertEquals("08S01", e.getSQLState().trim());
    }

    System.setProperty(JAVA_TRUST_STORE_PROP, dataFileDir + File.separator + TRUST_STORE_NAME );
    System.setProperty(JAVA_TRUST_STORE_PASS_PROP, KEY_STORE_TRUST_STORE_PASSWORD);
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
      hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL("default", SSL_CONN_PARAMS),
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
      hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL("default", ";ssl=false"),
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
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL("default", SSL_CONN_PARAMS),
        System.getProperty("user.name"), "bar");
    hs2Conn.close();
    miniHS2.stop();

    // Test in http mode
    setHttpConfOverlay(confOverlay);
    miniHS2.start(confOverlay);
    // make SSL connection
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL("default", SSL_CONN_PARAMS),
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
    System.setProperty(JAVA_TRUST_STORE_PASS_PROP, KEY_STORE_TRUST_STORE_PASSWORD);
    // make SSL connection
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL() + ";ssl=true",
        System.getProperty("user.name"), "bar");
    hs2Conn.close();
    miniHS2.stop();

    // Test in http mode
    setHttpConfOverlay(confOverlay);
    miniHS2.start(confOverlay);
    // make SSL connection
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL("default", SSL_CONN_PARAMS),
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
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL("default", SSL_CONN_PARAMS),
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
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL("default", SSL_CONN_PARAMS),
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

  /***
   * Test a new connection when server sends a certificate with wrong CN
   * (sends a certificate for www.example.com instead of localhost)
   * Opening a new connection with this wrong certificate should fail
   * @throws Exception
   */
  @Test
  public void testConnectionWrongCertCN() throws Exception {
    // This call sets the default ssl params including the correct keystore in the server config
    setSslConfOverlay(confOverlay);
    // Replace default keystore with keystore for www.example.com
    confOverlay.put(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname, dataFileDir + File.separator
        + EXAMPLEDOTCOM_KEY_STORE_NAME);
    // Binary (TCP) mode
    setBinaryConfOverlay(confOverlay);
    miniHS2.start(confOverlay);
    try {
      hs2Conn =
          DriverManager.getConnection(miniHS2.getJdbcURL("default", SSL_CONN_PARAMS),
              System.getProperty("user.name"), "bar");
      fail("SSL connection, with the server providing wrong certifcate (with CN www.example.com, "
          + "instead of localhost), should fail");
    } catch (SQLException e) {
      // Expected error: should throw java.security.cert.CertificateException
      assertEquals("08S01", e.getSQLState().trim());
      assertTrue(e.toString().contains("java.security.cert.CertificateException"));
    }

    miniHS2.stop();

    // Http mode
    setHttpConfOverlay(confOverlay);
    miniHS2.start(confOverlay);
    try {
      hs2Conn =
          DriverManager.getConnection(miniHS2.getJdbcURL("default", SSL_CONN_PARAMS),
              System.getProperty("user.name"), "bar");
      fail("SSL connection, with the server providing wrong certifcate (with CN www.example.com, "
          + "instead of localhost), should fail");
    } catch (SQLException e) {
      // Expected error: should throw javax.net.ssl.SSLPeerUnverifiedException
      assertEquals("08S01", e.getSQLState().trim());
      assertTrue(e.toString().contains("javax.net.ssl.SSLPeerUnverifiedException"));
    }
    // Revert to default keystore path
    confOverlay.put(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname, dataFileDir + File.separator
        + LOCALHOST_KEY_STORE_NAME);
  }

  /**
   * Test HMS server with SSL
   * @throws Exception
   */
  @Test
  public void testMetastoreWithSSL() throws Exception {
    setMetastoreSslConf(conf);
    setSslConfOverlay(confOverlay);
    // Test in http mode
    setHttpConfOverlay(confOverlay);
    miniHS2 = new MiniHS2.Builder().withRemoteMetastore().withConf(conf).cleanupLocalDirOnStartup(false).build();
    miniHS2.start(confOverlay);

    String tableName = "sslTab";
    Path dataFilePath = new Path(dataFileDir, "kv1.txt");

    // make SSL connection
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL("default", SSL_CONN_PARAMS),
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

  /**
   * Verify the HS2 can't connect to HMS if the certificate doesn't match
   * @throws Exception
   */
  @Test
  public void testMetastoreConnectionWrongCertCN() throws Exception {
    setMetastoreSslConf(conf);
    conf.setVar(ConfVars.HIVE_METASTORE_SSL_KEYSTORE_PATH,
        dataFileDir + File.separator +  EXAMPLEDOTCOM_KEY_STORE_NAME);
    miniHS2 = new MiniHS2.Builder().withRemoteMetastore().withConf(conf).cleanupLocalDirOnStartup(false).build();
    try {
      miniHS2.start(confOverlay);
    } catch (java.net.ConnectException e) {
      assertTrue(e.toString().contains("Connection refused"));
    }

    miniHS2.stop();
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
        dataFileDir + File.separator +  LOCALHOST_KEY_STORE_NAME);
    confOverlay.put(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname,
        KEY_STORE_TRUST_STORE_PASSWORD);
  }

  private void setMetastoreSslConf(HiveConf conf) {
    conf.setBoolVar(ConfVars.HIVE_METASTORE_USE_SSL, true);
    conf.setVar(ConfVars.HIVE_METASTORE_SSL_KEYSTORE_PATH,
        dataFileDir + File.separator +  LOCALHOST_KEY_STORE_NAME);
    conf.setVar(ConfVars.HIVE_METASTORE_SSL_KEYSTORE_PASSWORD,
        KEY_STORE_TRUST_STORE_PASSWORD);
    conf.setVar(ConfVars.HIVE_METASTORE_SSL_TRUSTSTORE_PATH,
        dataFileDir + File.separator +  TRUST_STORE_NAME);
    conf.setVar(ConfVars.HIVE_METASTORE_SSL_TRUSTSTORE_PASSWORD,
        KEY_STORE_TRUST_STORE_PASSWORD);
  }

  private void clearSslConfOverlay(Map<String, String> confOverlay) {
    confOverlay.put(ConfVars.HIVE_SERVER2_USE_SSL.varname, "false");
  }

  private void setHttpConfOverlay(Map<String, String> confOverlay) {
    confOverlay.put(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, HS2_HTTP_MODE);
    confOverlay.put(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname, HS2_HTTP_ENDPOINT);
    confOverlay.put(ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "true");
  }

  private void setBinaryConfOverlay(Map<String, String> confOverlay) {
    confOverlay.put(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, HS2_BINARY_MODE);
    confOverlay.put(ConfVars.HIVE_SERVER2_AUTHENTICATION.varname,  HS2_BINARY_AUTH_MODE);
    confOverlay.put(ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "true");
  }
}
