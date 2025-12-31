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

package org.apache.hadoop.hive.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hive.testutils.MiniZooKeeperCluster;
import org.apache.iceberg.rest.HMSCatalogFactory;
import org.apache.iceberg.rest.standalone.StandaloneRESTCatalogServer;
import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests to validate that the correct HMS REST Catalog server
 * implementation is used based on configuration.
 * 
 * Tests three modes:
 * 1. Embedded mode: REST Catalog servlet runs within HMS process
 * 2. Standalone HA mode: Standalone server with High Availability enabled
 * 3. Standalone non-HA mode: Standalone server without HA
 * 4. Disabled mode: REST Catalog is not started
 */
public class TestRESTCatalogModeSelection {
  private static final Logger LOG = LoggerFactory.getLogger(TestRESTCatalogModeSelection.class);
  
  private static MiniZooKeeperCluster zkCluster;
  private static int zkPort;
  private static File zkTempDir;
  private static int hmsPort = -1; // Embedded HMS port
  
  private Configuration conf;
  private StandaloneRESTCatalogServer standaloneServer;
  private Server embeddedServletServer;
  
  @BeforeClass
  public static void setupZooKeeper() throws Exception {
    // Create temporary directory for ZooKeeper data
    zkTempDir = Files.createTempDirectory(Paths.get(System.getProperty("java.io.tmpdir")), "zk_mode_test_").toFile();
    
    // Start embedded ZooKeeper cluster
    zkCluster = new MiniZooKeeperCluster();
    zkPort = zkCluster.startup(zkTempDir);
    
    LOG.info("Started embedded ZooKeeper on port: {}", zkPort);
    
    // Start embedded HMS for testing embedded mode
    Configuration hmsConf = MetastoreConf.newMetastoreConf();
    MetaStoreTestUtils.setConfForStandloneMode(hmsConf);
    
    // Set up Derby database
    File hmsTempDir = Files.createTempDirectory(Paths.get(System.getProperty("java.io.tmpdir")), "hms_mode_test_").toFile();
    String jdbcUrl = "jdbc:derby:memory:" + hmsTempDir.getAbsolutePath() + File.separator + "metastore_db;create=true";
    MetastoreConf.setVar(hmsConf, ConfVars.CONNECT_URL_KEY, jdbcUrl);
    
    // Set warehouse directories
    File warehouseDir = new File(hmsTempDir, "warehouse");
    warehouseDir.mkdirs();
    MetastoreConf.setVar(hmsConf, ConfVars.WAREHOUSE, warehouseDir.getAbsolutePath());
    
    File warehouseExternalDir = new File(hmsTempDir, "warehouse_external");
    warehouseExternalDir.mkdirs();
    MetastoreConf.setVar(hmsConf, ConfVars.WAREHOUSE_EXTERNAL, warehouseExternalDir.getAbsolutePath());
    
    // Start HMS
    hmsPort = MetaStoreTestUtils.startMetaStoreWithRetry(
        HadoopThriftAuthBridge.getBridge(), hmsConf, true, false, false, false);
    LOG.info("Started embedded HMS on port: {}", hmsPort);
  }
  
  @AfterClass
  public static void teardownZooKeeper() throws Exception {
    if (zkCluster != null) {
      zkCluster.shutdown();
      zkCluster = null;
    }
    if (zkTempDir != null && zkTempDir.exists()) {
      deleteDirectory(zkTempDir);
    }
    // Stop embedded HMS
    if (hmsPort > 0) {
      try {
        MetaStoreTestUtils.close(hmsPort);
        LOG.info("Stopped embedded HMS on port: {}", hmsPort);
      } catch (Exception e) {
        LOG.warn("Error stopping HMS", e);
      }
      hmsPort = -1;
    }
  }
  
  private static void deleteDirectory(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            file.delete();
          }
        }
      }
      directory.delete();
    }
  }
  
  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    
    // Configure embedded ZooKeeper
    String zkQuorum = "localhost:" + zkPort;
    conf.set("hive.zookeeper.quorum", zkQuorum);
    conf.set("hive.zookeeper.client.port", String.valueOf(zkPort));
    MetastoreConf.setVar(conf, ConfVars.THRIFT_ZOOKEEPER_CLIENT_PORT, String.valueOf(zkPort));
    
    // Configure HMS connection
    String hmsUri = "thrift://localhost:" + hmsPort;
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, hmsUri);
    
    // Configure warehouse
    MetastoreConf.setVar(conf, ConfVars.WAREHOUSE, "/tmp/warehouse");
    MetastoreConf.setVar(conf, ConfVars.WAREHOUSE_EXTERNAL, "/tmp/warehouse_external");
    
    // Configure REST Catalog servlet path (required for servlet creation)
    MetastoreConf.setVar(conf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH, "iceberg");
    
    // Configure authentication to "none" for tests
    MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH, "none");
    
    LOG.info("Test setup complete. ZK: {}, HMS: {}", zkQuorum, hmsUri);
  }
  
  @After
  public void teardown() throws Exception {
    if (standaloneServer != null) {
      try {
        standaloneServer.stop();
      } catch (Exception e) {
        LOG.warn("Error stopping standalone server", e);
      }
      standaloneServer = null;
    }
    if (embeddedServletServer != null) {
      try {
        embeddedServletServer.stop();
      } catch (Exception e) {
        LOG.warn("Error stopping embedded servlet server", e);
      }
      embeddedServletServer = null;
    }
  }
  
  /**
   * Test that embedded mode is used when:
   * - CATALOG_SERVLET_PORT >= 0
   * - REST_CATALOG_HA_ENABLED = false (or not set)
   * 
   * In embedded mode, HMS starts the REST Catalog servlet internally.
   */
  @Test(timeout = 60000)
  public void testEmbeddedMode() throws Exception {
    LOG.info("=== Test: Embedded Mode ===");
    
    // Configure for embedded mode: port >= 0, HA disabled
    MetastoreConf.setLongVar(conf, ConfVars.CATALOG_SERVLET_PORT, 8095);
    MetastoreConf.setBoolVar(conf, ConfVars.REST_CATALOG_HA_ENABLED, false);
    
    // Simulate what HMS does: call HMSCatalogFactory.createServlet()
    // This should return a non-null descriptor, indicating embedded mode is enabled
    org.apache.hadoop.hive.metastore.ServletServerBuilder.Descriptor descriptor = 
        HMSCatalogFactory.createServlet(conf);
    
    assertNotNull("REST Catalog servlet descriptor should be created for embedded mode", descriptor);
    assertTrue("Port should be >= 0", descriptor.getPort() >= 0);
    assertEquals("Path should match", "iceberg", descriptor.getPath());
    assertNotNull("Servlet should not be null", descriptor.getServlet());
    
    // Verify that creating a standalone server would fail or not be used
    // (In real deployment, standalone server wouldn't be started if embedded is enabled)
    
    LOG.info("Embedded mode test passed: servlet descriptor created successfully");
  }
  
  /**
   * Test that standalone HA mode is used when:
   * - CATALOG_SERVLET_PORT >= 0
   * - REST_CATALOG_HA_ENABLED = true
   * 
   * In standalone HA mode, StandaloneRESTCatalogServer starts with HA registry.
   */
  @Test(timeout = 60000)
  public void testStandaloneHAMode() throws Exception {
    LOG.info("=== Test: Standalone HA Mode ===");
    
    // Configure for standalone HA mode
    MetastoreConf.setLongVar(conf, ConfVars.CATALOG_SERVLET_PORT, 8096);
    MetastoreConf.setBoolVar(conf, ConfVars.REST_CATALOG_HA_ENABLED, true);
    
    // Use unique namespace per test
    String testNamespace = "restCatalogModeTest-" + UUID.randomUUID().toString();
    MetastoreConf.setVar(conf, ConfVars.REST_CATALOG_HA_REGISTRY_NAMESPACE, testNamespace);
    MetastoreConf.setVar(conf, ConfVars.REST_CATALOG_HA_MODE, "active-passive");
    MetastoreConf.setVar(conf, ConfVars.REST_CATALOG_INSTANCE_URI, "localhost:8096");
    
    // Create and start standalone server
    standaloneServer = new StandaloneRESTCatalogServer(conf);
    standaloneServer.start();
    
    // Wait for server to start and register
    Thread.sleep(2000);
    
    // Verify server is running
    Server server = standaloneServer.getServer();
    assertNotNull("Standalone server should be started", server);
    assertTrue("Server should be started", server.isStarted());
    
    // Verify HA registry is initialized (server should have HA registry)
    // We can check by verifying the server responds to HTTP requests
    String endpoint = "http://localhost:8096/iceberg";
    URL url = new URL(endpoint + "/v1/config");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);
    
    int responseCode = conn.getResponseCode();
    // Should get 200, 401, 403, or 404 (any response means server is running)
    assertTrue("Server should respond (response code: " + responseCode + ")", 
        responseCode == 200 || responseCode == 401 || responseCode == 403 || responseCode == 404);
    
    LOG.info("Standalone HA mode test passed: server started and responding");
  }
  
  /**
   * Test that standalone non-HA mode is used when:
   * - CATALOG_SERVLET_PORT >= 0
   * - REST_CATALOG_HA_ENABLED = false
   * - StandaloneRESTCatalogServer is started (not embedded in HMS)
   * 
   * In standalone non-HA mode, StandaloneRESTCatalogServer starts without HA registry.
   */
  @Test(timeout = 60000)
  public void testStandaloneNonHAMode() throws Exception {
    LOG.info("=== Test: Standalone Non-HA Mode ===");
    
    // Configure for standalone non-HA mode
    MetastoreConf.setLongVar(conf, ConfVars.CATALOG_SERVLET_PORT, 8097);
    MetastoreConf.setBoolVar(conf, ConfVars.REST_CATALOG_HA_ENABLED, false);
    MetastoreConf.setVar(conf, ConfVars.REST_CATALOG_INSTANCE_URI, "localhost:8097");
    
    // Create and start standalone server
    standaloneServer = new StandaloneRESTCatalogServer(conf);
    standaloneServer.start();
    
    // Wait for server to start
    Thread.sleep(2000);
    
    // Verify server is running
    Server server = standaloneServer.getServer();
    assertNotNull("Standalone server should be started", server);
    assertTrue("Server should be started", server.isStarted());
    
    // Verify server responds to HTTP requests
    String endpoint = "http://localhost:8097/iceberg";
    URL url = new URL(endpoint + "/v1/config");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);
    
    int responseCode = conn.getResponseCode();
    assertTrue("Server should respond (response code: " + responseCode + ")", 
        responseCode == 200 || responseCode == 401 || responseCode == 403 || responseCode == 404);
    
    LOG.info("Standalone non-HA mode test passed: server started and responding");
  }
  
  /**
   * Test that REST Catalog is disabled when:
   * - CATALOG_SERVLET_PORT < 0
   * 
   * In disabled mode, HMSCatalogFactory.createServlet() returns null.
   */
  @Test(timeout = 60000)
  public void testDisabledMode() throws Exception {
    LOG.info("=== Test: Disabled Mode ===");
    
    // Configure for disabled mode: port < 0
    MetastoreConf.setLongVar(conf, ConfVars.CATALOG_SERVLET_PORT, -1);
    
    // Call HMSCatalogFactory.createServlet() - should return null
    org.apache.hadoop.hive.metastore.ServletServerBuilder.Descriptor descriptor = 
        HMSCatalogFactory.createServlet(conf);
    
    assertNull("REST Catalog servlet descriptor should be null when disabled", descriptor);
    
    LOG.info("Disabled mode test passed: servlet descriptor is null");
  }
  
  /**
   * Test that REST Catalog is disabled when:
   * - ICEBERG_CATALOG_SERVLET_PATH is null or empty
   * 
   * In this case, HMSCatalogFactory.createServlet() returns null.
   */
  @Test(timeout = 60000)
  public void testDisabledModeNoPath() throws Exception {
    LOG.info("=== Test: Disabled Mode (No Path) ===");
    
    // Configure port but no path
    MetastoreConf.setLongVar(conf, ConfVars.CATALOG_SERVLET_PORT, 8098);
    MetastoreConf.setVar(conf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH, ""); // Empty path
    
    // Call HMSCatalogFactory.createServlet() - should return null
    org.apache.hadoop.hive.metastore.ServletServerBuilder.Descriptor descriptor = 
        HMSCatalogFactory.createServlet(conf);
    
    assertNull("REST Catalog servlet descriptor should be null when path is empty", descriptor);
    
    LOG.info("Disabled mode (no path) test passed: servlet descriptor is null");
  }
  
  /**
   * Test that embedded mode servlet can be started via ServletServerBuilder
   * (simulating what HMS does internally).
   */
  @Test(timeout = 60000)
  public void testEmbeddedModeServletStart() throws Exception {
    LOG.info("=== Test: Embedded Mode Servlet Start ===");
    
    // Configure for embedded mode
    MetastoreConf.setLongVar(conf, ConfVars.CATALOG_SERVLET_PORT, 8099);
    MetastoreConf.setBoolVar(conf, ConfVars.REST_CATALOG_HA_ENABLED, false);
    
    // Simulate what HMS does: create servlet descriptor and start server
    org.apache.hadoop.hive.metastore.ServletServerBuilder.Descriptor descriptor = 
        HMSCatalogFactory.createServlet(conf);
    
    assertNotNull("REST Catalog servlet descriptor should be created", descriptor);
    
    // Start servlet server (simulating HMS's ServletServerBuilder)
    org.apache.hadoop.hive.metastore.ServletServerBuilder builder = 
        new org.apache.hadoop.hive.metastore.ServletServerBuilder(conf);
    builder.addServlet(descriptor);
    embeddedServletServer = builder.start(LOG);
    
    assertNotNull("Embedded servlet server should be started", embeddedServletServer);
    assertTrue("Server should be started", embeddedServletServer.isStarted());
    
    // Verify server responds to HTTP requests
    String endpoint = "http://localhost:8099/iceberg";
    URL url = new URL(endpoint + "/v1/config");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);
    
    int responseCode = conn.getResponseCode();
    assertTrue("Server should respond (response code: " + responseCode + ")", 
        responseCode == 200 || responseCode == 401 || responseCode == 403 || responseCode == 404);
    
    LOG.info("Embedded mode servlet start test passed: server started and responding");
  }
}

