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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.iceberg.rest.standalone.StandaloneRESTCatalogServer;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for Standalone REST Catalog Server with Spring Boot (no auth).
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
    classes = BaseStandaloneRESTCatalogServerTest.TestRestCatalogApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration"
    }
)
@ContextConfiguration(initializers = TestStandaloneRESTCatalogServer.RestCatalogTestContextInitializer.class)
@TestExecutionListeners(
    listeners = BaseStandaloneRESTCatalogServerTest.HmsStartupListener.class,
    mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS
)
public class TestStandaloneRESTCatalogServer extends BaseStandaloneRESTCatalogServerTest {
  @LocalServerPort
  private int port;

  @Autowired
  private StandaloneRESTCatalogServer server;

  @Override
  protected int getPort() {
    return port;
  }

  /**
   * Registers Configuration and StandaloneRESTCatalogServer before the context loads.
   * Mirrors production main() - we create both and register them, so Spring uses our
   * Configuration (with THRIFT_URIS from HMS) and never tries to instantiate the server.
   */
  public static class RestCatalogTestContextInitializer
      implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(ConfigurableApplicationContext context) {
      Configuration restCatalogConf = MetastoreConf.newMetastoreConf();
      String hmsUri = "thrift://localhost:" + hmsPort;
      MetastoreConf.setVar(restCatalogConf, ConfVars.THRIFT_URIS, hmsUri);
      MetastoreConf.setVar(restCatalogConf, ConfVars.WAREHOUSE, warehouseDir.getAbsolutePath());
      MetastoreConf.setVar(restCatalogConf, ConfVars.WAREHOUSE_EXTERNAL, warehouseDir.getAbsolutePath());
      MetastoreConf.setVar(restCatalogConf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH, "iceberg");
      MetastoreConf.setVar(restCatalogConf, ConfVars.CATALOG_SERVLET_AUTH, "none");
      MetastoreConf.setLongVar(restCatalogConf, ConfVars.CATALOG_SERVLET_PORT, 0);
      context.getBeanFactory().registerSingleton("hadoopConfiguration", restCatalogConf);
      StandaloneRESTCatalogServer server = new StandaloneRESTCatalogServer(restCatalogConf);
      context.getBeanFactory().registerSingleton("standaloneRESTCatalogServer", server);
    }
  }

  @AfterClass
  public static void teardownClass() throws IOException {
    teardownBase();
  }

  @Override
  @Test(timeout = 60000)
  public void testLivenessProbe() throws Exception {
    LOG.info("=== Test: Liveness Probe (Kubernetes) ===");
    super.testLivenessProbe();
  }

  @Override
  @Test(timeout = 60000)
  public void testReadinessProbe() throws Exception {
    LOG.info("=== Test: Readiness Probe (Kubernetes) ===");
    super.testReadinessProbe();
  }

  @Override
  @Test(timeout = 60000)
  public void testPrometheusMetrics() throws Exception {
    LOG.info("=== Test: Prometheus Metrics (for K8s HPA) ===");
    super.testPrometheusMetrics();
  }

  @Test(timeout = 60000)
  public void testRESTCatalogConfig() throws Exception {
    LOG.info("=== Test: REST Catalog Config Endpoint ===");

    try (CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = httpClient.execute(get("/iceberg/v1/config"))) {
      assertEquals("Config endpoint should return 200", 200, response.getStatusLine().getStatusCode());

      String responseBody = EntityUtils.toString(response.getEntity());
      LOG.info("Config response: {}", responseBody);
      assertTrue("Response should contain endpoints", responseBody.contains("endpoints"));
      assertTrue("Response should be valid JSON", responseBody.startsWith("{") && responseBody.endsWith("}"));
    }
  }

  @Test(timeout = 60000)
  public void testRESTCatalogNamespaceOperations() throws Exception {
    LOG.info("=== Test: REST Catalog Namespace Operations ===");

    String namespacePath = "/iceberg/v1/namespaces";
    String namespaceName = "testdb";

    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      try (CloseableHttpResponse response = httpClient.execute(get(namespacePath))) {
        assertEquals("List namespaces should return 200", 200, response.getStatusLine().getStatusCode());
      }

      try (CloseableHttpResponse response = httpClient.execute(
          post(namespacePath, "{\"namespace\":[\"" + namespaceName + "\"]}"))) {
        assertEquals("Create namespace should return 200", 200, response.getStatusLine().getStatusCode());
      }

      try (CloseableHttpResponse response = httpClient.execute(get(namespacePath))) {
        assertEquals("List namespaces after creation should return 200",
            200, response.getStatusLine().getStatusCode());
        String responseBody = EntityUtils.toString(response.getEntity());
        LOG.info("Namespaces list response: {}", responseBody);
        assertTrue("Response should contain created namespace", responseBody.contains(namespaceName));
      }

      try (CloseableHttpResponse response = httpClient.execute(
          get("/iceberg/v1/namespaces/" + namespaceName))) {
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
    super.testServerPort(server);
  }
}
