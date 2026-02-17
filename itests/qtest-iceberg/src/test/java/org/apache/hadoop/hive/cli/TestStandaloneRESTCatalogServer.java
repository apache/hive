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
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListener;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for Standalone REST Catalog Server with Spring Boot.
 *
 * Tests that the standalone server can:
 * 1. Start independently of HMS using Spring Boot
 * 2. Connect to an external HMS instance
 * 3. Serve REST Catalog requests
 * 4. Provide health check endpoints (liveness and readiness)
 * 5. Expose Prometheus metrics
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
    classes = {StandaloneRESTCatalogServer.class, TestStandaloneRESTCatalogServer.TestConfig.class},
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "spring.main.allow-bean-definition-overriding=true",
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration"
    }
)
@TestExecutionListeners(
    listeners = TestStandaloneRESTCatalogServer.HmsStartupListener.class,
    mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS
)
public class TestStandaloneRESTCatalogServer {
  private static final Logger LOG = LoggerFactory.getLogger(TestStandaloneRESTCatalogServer.class);

  @LocalServerPort
  private int port;

  @Autowired
  private StandaloneRESTCatalogServer server;

  private static Configuration hmsConf;
  private static int hmsPort;
  private static File warehouseDir;
  private static File hmsTempDir;

  /**
   * Starts HMS before the Spring ApplicationContext loads.
   * Spring loads the context before @BeforeClass, so we use a TestExecutionListener
   * which runs before context initialization.
   */
  public static class HmsStartupListener implements TestExecutionListener {
    @Override
    public void beforeTestClass(TestContext testContext) throws Exception {
      if (hmsPort > 0) {
        return; // Already started
      }
      hmsTempDir = new File(System.getProperty("java.io.tmpdir"), "test-hms-" + System.currentTimeMillis());
      hmsTempDir.mkdirs();
      warehouseDir = new File(hmsTempDir, "warehouse");
      warehouseDir.mkdirs();

      hmsConf = MetastoreConf.newMetastoreConf();
      MetaStoreTestUtils.setConfForStandloneMode(hmsConf);

      String jdbcUrl = String.format("jdbc:derby:memory:%s;create=true",
          new File(hmsTempDir, "metastore_db").getAbsolutePath());
      MetastoreConf.setVar(hmsConf, ConfVars.CONNECT_URL_KEY, jdbcUrl);
      MetastoreConf.setVar(hmsConf, ConfVars.WAREHOUSE, warehouseDir.getAbsolutePath());
      MetastoreConf.setVar(hmsConf, ConfVars.WAREHOUSE_EXTERNAL, warehouseDir.getAbsolutePath());

      hmsPort = MetaStoreTestUtils.startMetaStoreWithRetry(
          HadoopThriftAuthBridge.getBridge(), hmsConf, true, false, false, false);
      LOG.info("Started embedded HMS on port: {} (before Spring context)", hmsPort);
    }
  }

  /**
   * Test configuration that provides the Configuration bean.
   * Spring injects this into StandaloneRESTCatalogServer constructor.
   */
  @TestConfiguration
  public static class TestConfig {
    @Bean
    public Configuration hadoopConfiguration() {
      // Create Configuration for REST Catalog (standard Hive approach)
      Configuration restCatalogConf = MetastoreConf.newMetastoreConf();
      String hmsUri = "thrift://localhost:" + hmsPort;
      MetastoreConf.setVar(restCatalogConf, ConfVars.THRIFT_URIS, hmsUri);
      MetastoreConf.setVar(restCatalogConf, ConfVars.WAREHOUSE, warehouseDir.getAbsolutePath());
      MetastoreConf.setVar(restCatalogConf, ConfVars.WAREHOUSE_EXTERNAL, warehouseDir.getAbsolutePath());
      MetastoreConf.setVar(restCatalogConf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH, "iceberg");
      MetastoreConf.setVar(restCatalogConf, ConfVars.CATALOG_SERVLET_AUTH, "none");
      // HMSCatalogFactory returns null when CATALOG_SERVLET_PORT is -1; use 0 for Spring Boot managed port
      MetastoreConf.setLongVar(restCatalogConf, ConfVars.CATALOG_SERVLET_PORT, 0);
      return restCatalogConf;
    }
  }

  @AfterClass
  public static void teardownClass() {
    if (hmsPort > 0) {
      MetaStoreTestUtils.close(hmsPort);
    }
    if (hmsTempDir != null && hmsTempDir.exists()) {
      deleteDirectoryStatic(hmsTempDir);
    }
  }

  private static void deleteDirectoryStatic(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectoryStatic(file);
          } else {
            file.delete();
          }
        }
      }
      directory.delete();
    }
  }

  @Test(timeout = 60000)
  public void testLivenessProbe() throws Exception {
    LOG.info("=== Test: Liveness Probe (Kubernetes) ===");

    String livenessUrl = "http://localhost:" + port + "/actuator/health/liveness";
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet request = new HttpGet(livenessUrl);
      try (CloseableHttpResponse response = httpClient.execute(request)) {
        assertEquals("Liveness probe should return 200", 200, response.getStatusLine().getStatusCode());
        String body = EntityUtils.toString(response.getEntity());
        assertTrue("Liveness should be UP", body.contains("UP"));
        LOG.info("Liveness probe passed: {}", body);
      }
    }
  }

  @Test(timeout = 60000)
  public void testReadinessProbe() throws Exception {
    LOG.info("=== Test: Readiness Probe (Kubernetes) ===");

    String readinessUrl = "http://localhost:" + port + "/actuator/health/readiness";
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet request = new HttpGet(readinessUrl);
      try (CloseableHttpResponse response = httpClient.execute(request)) {
        assertEquals("Readiness probe should return 200", 200, response.getStatusLine().getStatusCode());
        String body = EntityUtils.toString(response.getEntity());
        assertTrue("Readiness should be UP", body.contains("UP"));
        LOG.info("Readiness probe passed: {}", body);
      }
    }
  }

  @Test(timeout = 60000)
  public void testPrometheusMetrics() throws Exception {
    LOG.info("=== Test: Prometheus Metrics (for K8s HPA) ===");

    String metricsUrl = "http://localhost:" + port + "/actuator/prometheus";
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet request = new HttpGet(metricsUrl);
      try (CloseableHttpResponse response = httpClient.execute(request)) {
        assertEquals("Metrics endpoint should return 200", 200, response.getStatusLine().getStatusCode());
        String body = EntityUtils.toString(response.getEntity());
        assertTrue("Should contain JVM metrics", body.contains("jvm_memory"));
        LOG.info("Prometheus metrics available");
      }
    }
  }

  @Test(timeout = 60000)
  public void testRESTCatalogConfig() throws Exception {
    LOG.info("=== Test: REST Catalog Config Endpoint ===");

    String configUrl = "http://localhost:" + port + "/iceberg/v1/config";
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

    String namespacesUrl = "http://localhost:" + port + "/iceberg/v1/namespaces";
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
      String getNamespaceUrl = "http://localhost:" + port + "/iceberg/v1/namespaces/" + namespaceName;
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
    assertTrue("Server port should be > 0", port > 0);
    assertNotNull("REST endpoint should not be null", server.getRestEndpoint());
    LOG.info("Server port: {}, Endpoint: {}", port, server.getRestEndpoint());
  }
}
