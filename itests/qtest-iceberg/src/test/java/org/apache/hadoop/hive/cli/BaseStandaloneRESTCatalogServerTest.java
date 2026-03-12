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
import java.io.IOException;
import java.util.UUID;

import javax.net.ssl.SSLContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import javax.servlet.http.HttpServlet;

import org.apache.iceberg.rest.standalone.IcebergCatalogConfiguration;
import org.apache.iceberg.rest.standalone.RestCatalogServerRuntime;
import org.junit.Test;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListener;
import org.springframework.web.util.UriComponentsBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Base class for Standalone REST Catalog Server integration tests.
 *
 * Provides shared setup (HMS, listeners), HTTP helpers (with optional auth), and common tests
 * (liveness, readiness, Prometheus, server port). Subclasses provide auth-specific configuration
 * and tests.
 */
public abstract class BaseStandaloneRESTCatalogServerTest {
  protected static final Logger LOG = LoggerFactory.getLogger(BaseStandaloneRESTCatalogServerTest.class);

  protected static Configuration hmsConf;
  protected static int hmsPort;
  protected static File warehouseDir;
  protected static File hmsTempDir;

  @LocalServerPort
  private int port;

  @Autowired
  private RestCatalogServerRuntime server;

  /**
   * Starts HMS before the Spring ApplicationContext loads.
   * Spring loads the context before @BeforeClass, so we use a TestExecutionListener
   * which runs before context initialization.
   */
  @Order(Ordered.HIGHEST_PRECEDENCE)
  public static class HmsStartupListener implements TestExecutionListener {
    private static final String TEMP_DIR_PREFIX = "StandaloneRESTCatalogServer";

    @Override
    public void beforeTestClass(TestContext testContext) throws Exception {
      if (hmsPort > 0) {
        return;
      }
      String uniqueTestKey = String.format("%s_%s", TEMP_DIR_PREFIX, UUID.randomUUID());
      hmsTempDir = new File(MetaStoreTestUtils.getTestWarehouseDir(uniqueTestKey));
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

  @SpringBootApplication
  @Import(TestCatalogConfig.class)
  @ComponentScan(
      basePackages = "org.apache.iceberg.rest.standalone",
      excludeFilters = {
          @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = IcebergCatalogConfiguration.class),
          @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = RestCatalogServerRuntime.class)
      })
  public static class TestRestCatalogApplication {}

  /**
   * Test-specific config providing the REST catalog servlet.
   * Uses Configuration from test's TestConfig (with hmsPort, warehouseDir).
   * Does NOT import IcebergCatalogConfiguration to avoid production hadoopConfiguration.
   */
  @org.springframework.context.annotation.Configuration
  static class TestCatalogConfig {

    @Bean
    public Configuration hadoopConfiguration() {
      Configuration conf = createBaseTestConfiguration();
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH, "none");
      return conf;
    }

    @Bean
    public RestCatalogServerRuntime restCatalogServerRuntime(ServerProperties serverProperties) {
      Configuration conf = createBaseTestConfiguration();
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH, "none");
      return new RestCatalogServerRuntime(conf, serverProperties);
    }

    @Bean
    public ServletRegistrationBean<HttpServlet> restCatalogServlet(Configuration conf) {
      return IcebergCatalogConfiguration.createRestCatalogServlet(conf);
    }
  }

  protected String url(String path) {
    return UriComponentsBuilder.newInstance()
        .scheme("https")
        .host("localhost")
        .port(getPort())
        .path(path.startsWith("/") ? path : "/" + path)
        .toUriString();
  }

  /**
   * Creates an HttpClient that trusts the test server's self-signed certificate.
   */
  protected CloseableHttpClient createHttpClient() throws Exception {
    SSLContext sslContext = SSLContextBuilder.create()
        .loadTrustMaterial((chain, authType) -> true)
        .build();
    return HttpClients.custom()
        .setSSLContext(sslContext)
        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
        .build();
  }

  protected int getPort() {
    return port;
  }

  protected RestCatalogServerRuntime getServer() {
    return server;
  }

  /**
   * Creates base test Configuration with HMS URI and warehouse dirs.
   * Subclasses add auth-specific settings.
   */
  protected static Configuration createBaseTestConfiguration() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + hmsPort);
    MetastoreConf.setVar(conf, ConfVars.WAREHOUSE, warehouseDir.getAbsolutePath());
    MetastoreConf.setVar(conf, ConfVars.WAREHOUSE_EXTERNAL, warehouseDir.getAbsolutePath());
    MetastoreConf.setVar(conf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH, "iceberg");
    MetastoreConf.setLongVar(conf, ConfVars.CATALOG_SERVLET_PORT, 0);
    return conf;
  }

  /**
   * Returns the Bearer token for catalog API tests, or null if no auth.
   * Subclasses with auth (e.g. JWT) override to return a valid token.
   */
  protected String getBearerTokenForCatalogTests() {
    return null;
  }

  /**
   * Creates a GET request with optional Bearer token.
   *
   * @param path the request path (e.g. "/iceberg/v1/config")
   * @param bearerToken optional Bearer token; if null, no Authorization header is set
   */
  protected HttpGet get(String path, String bearerToken) {
    HttpGet request = new HttpGet(url(path));
    request.setHeader("Content-Type", "application/json");
    if (bearerToken != null) {
      request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken);
    }
    return request;
  }

  /**
   * Creates a GET request without auth.
   */
  protected HttpGet get(String path) {
    return get(path, null);
  }

  /**
   * Creates a POST request with optional Bearer token.
   *
   * @param path the request path
   * @param jsonBody the JSON body
   * @param bearerToken optional Bearer token; if null, no Authorization header is set
   */
  protected HttpPost post(String path, String jsonBody, String bearerToken) {
    HttpPost request = new HttpPost(url(path));
    request.setHeader("Content-Type", "application/json");
    if (bearerToken != null) {
      request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken);
    }
    if (jsonBody != null) {
      request.setEntity(new StringEntity(jsonBody, "UTF-8"));
    }
    return request;
  }

  /**
   * Creates a POST request without auth.
   */
  protected HttpPost post(String path, String jsonBody) {
    return post(path, jsonBody, null);
  }

  protected static void teardownBase() throws IOException {
    if (hmsPort > 0) {
      MetaStoreTestUtils.close(hmsPort);
    }
    if (hmsTempDir != null && hmsTempDir.exists()) {
      FileUtils.deleteDirectory(hmsTempDir);
    }
  }

  @Test(timeout = 60000)
  public void testLivenessProbe() throws Exception {
    try (CloseableHttpClient httpClient = createHttpClient();
        CloseableHttpResponse response = httpClient.execute(get("/actuator/health/liveness"))) {
      assertEquals("Liveness probe should return 200", 200, response.getStatusLine().getStatusCode());
      String body = EntityUtils.toString(response.getEntity());
      assertTrue("Liveness should be UP", body.contains("UP"));
      LOG.info("Liveness probe passed: {}", body);
    }
  }

  @Test(timeout = 60000)
  public void testReadinessProbe() throws Exception {
    try (CloseableHttpClient httpClient = createHttpClient();
        CloseableHttpResponse response = httpClient.execute(get("/actuator/health/readiness"))) {
      assertEquals("Readiness probe should return 200", 200, response.getStatusLine().getStatusCode());
      String body = EntityUtils.toString(response.getEntity());
      assertTrue("Readiness should be UP", body.contains("UP"));
      LOG.info("Readiness probe passed: {}", body);
    }
  }

  @Test(timeout = 60000)
  public void testPrometheusMetrics() throws Exception {
    try (CloseableHttpClient httpClient = createHttpClient();
        CloseableHttpResponse response = httpClient.execute(get("/actuator/prometheus"))) {
      assertEquals("Metrics endpoint should return 200", 200, response.getStatusLine().getStatusCode());
      String body = EntityUtils.toString(response.getEntity());
      assertTrue("Should contain JVM metrics", body.contains("jvm_memory"));
      LOG.info("Prometheus metrics available");
    }
  }

  @Test(timeout = 60000)
  public void testServerPort() {
    RestCatalogServerRuntime s = getServer();
    assertTrue("Server port should be > 0", getPort() > 0);
    assertNotNull("REST endpoint should not be null", s.getRestEndpoint());
    LOG.info("Server port: {}, Endpoint: {}", getPort(), s.getRestEndpoint());
  }

  @Test(timeout = 120000)
  public void testRESTCatalogConfig() throws Exception {
    String token = getBearerTokenForCatalogTests();
    try (CloseableHttpClient httpClient = createHttpClient();
        CloseableHttpResponse response = httpClient.execute(get(String.format("/%s/%s",
            IcebergCatalogConfiguration.DEFAULT_SERVLET_PATH, "v1/config"), token))) {
      assertEquals("Config endpoint should return 200", 200, response.getStatusLine().getStatusCode());
      String responseBody = EntityUtils.toString(response.getEntity());
      assertTrue("Response should contain endpoints", responseBody.contains("endpoints"));
      assertTrue("Response should be valid JSON", responseBody.startsWith("{") && responseBody.endsWith("}"));
    }
  }

  @Test(timeout = 120000)
  public void testRESTCatalogNamespaceOperations() throws Exception {
    String token = getBearerTokenForCatalogTests();
    String namespacePath = String.format("/%s/%s", IcebergCatalogConfiguration.DEFAULT_SERVLET_PATH, "v1/namespaces");
    String namespaceName = "testdb";

    try (CloseableHttpClient httpClient = createHttpClient()) {
      try (CloseableHttpResponse response = httpClient.execute(get(namespacePath, token))) {
        assertEquals("List namespaces should return 200", 200, response.getStatusLine().getStatusCode());
      }

      try (CloseableHttpResponse response = httpClient.execute(
          post(namespacePath, "{\"namespace\":[\"" + namespaceName + "\"]}", token))) {
        assertEquals("Create namespace should return 200", 200, response.getStatusLine().getStatusCode());
      }

      try (CloseableHttpResponse response = httpClient.execute(get(namespacePath, token))) {
        assertEquals("List namespaces after creation should return 200",
            200, response.getStatusLine().getStatusCode());
        String responseBody = EntityUtils.toString(response.getEntity());
        assertTrue("Response should contain created namespace", responseBody.contains(namespaceName));
      }

      try (CloseableHttpResponse response = httpClient.execute(
          get(String.format("/%s/%s/%s", IcebergCatalogConfiguration.DEFAULT_SERVLET_PATH,
              "v1/namespaces", namespaceName), token))) {
        assertEquals("Get namespace should return 200",
            200, response.getStatusLine().getStatusCode());
        String responseBody = EntityUtils.toString(response.getEntity());
        assertTrue("Response should contain namespace", responseBody.contains(namespaceName));
      }
    }
  }
}
