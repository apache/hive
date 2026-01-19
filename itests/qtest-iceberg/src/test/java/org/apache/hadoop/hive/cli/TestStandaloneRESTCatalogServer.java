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

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.iceberg.rest.standalone.StandaloneRESTCatalogServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for Standalone REST Catalog Server.
 * 
 * Tests that the standalone server can:
 * 1. Start independently of HMS
 * 2. Connect to an external HMS instance
 * 3. Serve REST Catalog requests
 * 4. Provide health check endpoint
 */
public class TestStandaloneRESTCatalogServer {
  private static final Logger LOG = LoggerFactory.getLogger(TestStandaloneRESTCatalogServer.class);
  
  private Configuration hmsConf;
  private Configuration restCatalogConf;
  private int hmsPort;
  private StandaloneRESTCatalogServer restCatalogServer;
  private File warehouseDir;
  private File hmsTempDir;
  
  @Before
  public void setup() throws Exception {
    // Setup temporary directories
    hmsTempDir = new File(System.getProperty("java.io.tmpdir"), "test-hms-" + System.currentTimeMillis());
    hmsTempDir.mkdirs();
    warehouseDir = new File(hmsTempDir, "warehouse");
    warehouseDir.mkdirs();
    
    // Configure and start embedded HMS
    hmsConf = MetastoreConf.newMetastoreConf();
    MetaStoreTestUtils.setConfForStandloneMode(hmsConf);
    
    String jdbcUrl = String.format("jdbc:derby:memory:%s;create=true",
        new File(hmsTempDir, "metastore_db").getAbsolutePath());
    MetastoreConf.setVar(hmsConf, ConfVars.CONNECT_URL_KEY, jdbcUrl);
    MetastoreConf.setVar(hmsConf, ConfVars.WAREHOUSE, warehouseDir.getAbsolutePath());
    MetastoreConf.setVar(hmsConf, ConfVars.WAREHOUSE_EXTERNAL, warehouseDir.getAbsolutePath());
    
    // Start HMS
    hmsPort = MetaStoreTestUtils.startMetaStoreWithRetry(
        HadoopThriftAuthBridge.getBridge(), hmsConf, true, false, false, false);
    LOG.info("Started embedded HMS on port: {}", hmsPort);
    
    // Configure standalone REST Catalog server
    restCatalogConf = MetastoreConf.newMetastoreConf();
    String hmsUri = "thrift://localhost:" + hmsPort;
    MetastoreConf.setVar(restCatalogConf, ConfVars.THRIFT_URIS, hmsUri);
    MetastoreConf.setVar(restCatalogConf, ConfVars.WAREHOUSE, warehouseDir.getAbsolutePath());
    MetastoreConf.setVar(restCatalogConf, ConfVars.WAREHOUSE_EXTERNAL, warehouseDir.getAbsolutePath());
    
    // Configure REST Catalog servlet
    int restPort = MetaStoreTestUtils.findFreePort();
    MetastoreConf.setLongVar(restCatalogConf, ConfVars.CATALOG_SERVLET_PORT, restPort);
    MetastoreConf.setVar(restCatalogConf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH, "iceberg");
    MetastoreConf.setVar(restCatalogConf, ConfVars.CATALOG_SERVLET_AUTH, "none");
    
    // Start standalone REST Catalog server
    restCatalogServer = new StandaloneRESTCatalogServer(restCatalogConf);
    restCatalogServer.start();
    LOG.info("Started standalone REST Catalog server on port: {}", restCatalogServer.getPort());
  }
  
  @After
  public void teardown() {
    if (restCatalogServer != null) {
      restCatalogServer.stop();
    }
    if (hmsPort > 0) {
      MetaStoreTestUtils.close(hmsPort);
    }
    if (hmsTempDir != null && hmsTempDir.exists()) {
      deleteDirectory(hmsTempDir);
    }
  }
  
  @Test(timeout = 60000)
  public void testHealthCheck() throws Exception {
    LOG.info("=== Test: Health Check ===");
    
    String healthUrl = "http://localhost:" + restCatalogServer.getPort() + "/health";
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet request = new HttpGet(healthUrl);
      try (CloseableHttpResponse response = httpClient.execute(request)) {
        assertEquals("Health check should return 200", 200, response.getStatusLine().getStatusCode());
        LOG.info("Health check passed");
      }
    }
  }
  
  @Test(timeout = 60000)
  public void testRESTCatalogConfig() throws Exception {
    LOG.info("=== Test: REST Catalog Config Endpoint ===");
    
    String configUrl = restCatalogServer.getRestEndpoint() + "/v1/config";
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet request = new HttpGet(configUrl);
      try (CloseableHttpResponse response = httpClient.execute(request)) {
        assertEquals("Config endpoint should return 200", 200, response.getStatusLine().getStatusCode());
        
        String responseBody = EntityUtils.toString(response.getEntity());
        LOG.info("Config response: {}", responseBody);
        // ConfigResponse should contain endpoints, defaults, and overrides
        assertTrue("Response should contain endpoints", responseBody.contains("endpoints"));
        assertTrue("Response should be valid JSON", responseBody.startsWith("{") && responseBody.endsWith("}"));
      }
    }
  }
  
  @Test(timeout = 60000)
  public void testRESTCatalogNamespaceOperations() throws Exception {
    LOG.info("=== Test: REST Catalog Namespace Operations ===");
    
    String namespacesUrl = restCatalogServer.getRestEndpoint() + "/v1/namespaces";
    String namespaceName = "testdb";
    
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      // List namespaces (before creation)
      HttpGet listRequest = new HttpGet(namespacesUrl);
      listRequest.setHeader("Content-Type", "application/json");
      try (CloseableHttpResponse response = httpClient.execute(listRequest)) {
        assertEquals("List namespaces should return 200", 200, response.getStatusLine().getStatusCode());
      }
      
      // Create namespace - REST Catalog API requires JSON body with namespace array
      HttpPost createRequest = new HttpPost(namespacesUrl);
      createRequest.setHeader("Content-Type", "application/json");
      String jsonBody = "{\"namespace\":[\"" + namespaceName + "\"]}";
      createRequest.setEntity(new StringEntity(jsonBody, "UTF-8"));
      
      try (CloseableHttpResponse response = httpClient.execute(createRequest)) {
        assertEquals("Create namespace should return 200", 200, response.getStatusLine().getStatusCode());
      }
      
      // Verify namespace exists by checking it in the list
      HttpGet listAfterRequest = new HttpGet(namespacesUrl);
      listAfterRequest.setHeader("Content-Type", "application/json");
      try (CloseableHttpResponse response = httpClient.execute(listAfterRequest)) {
        assertEquals("List namespaces after creation should return 200", 
            200, response.getStatusLine().getStatusCode());
        
        String responseBody = EntityUtils.toString(response.getEntity());
        LOG.info("Namespaces list response: {}", responseBody);
        assertTrue("Response should contain created namespace", responseBody.contains(namespaceName));
      }
      
      // Verify namespace exists by getting it directly
      String getNamespaceUrl = restCatalogServer.getRestEndpoint() + "/v1/namespaces/" + namespaceName;
      HttpGet getRequest = new HttpGet(getNamespaceUrl);
      getRequest.setHeader("Content-Type", "application/json");
      try (CloseableHttpResponse response = httpClient.execute(getRequest)) {
        assertEquals("Get namespace should return 200", 
            200, response.getStatusLine().getStatusCode());
        String responseBody = EntityUtils.toString(response.getEntity());
        LOG.info("Get namespace response: {}", responseBody);
        assertTrue("Response should contain namespace", responseBody.contains(namespaceName));
      }
    }
    
    LOG.info("Namespace operations passed");
  }
  
  @Test(timeout = 60000)
  public void testServerPort() {
    LOG.info("=== Test: Server Port ===");
    assertTrue("Server port should be > 0", restCatalogServer.getPort() > 0);
    assertNotNull("REST endpoint should not be null", restCatalogServer.getRestEndpoint());
    LOG.info("Server port: {}, Endpoint: {}", restCatalogServer.getPort(), restCatalogServer.getRestEndpoint());
  }
  
  private void deleteDirectory(File directory) {
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
}
