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
import org.apache.iceberg.rest.extension.OAuth2AuthorizationServer;
import org.apache.iceberg.rest.standalone.StandaloneRESTCatalogServer;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.annotation.Order;
import org.springframework.core.Ordered;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListener;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for Standalone REST Catalog Server with JWT authentication.
 *
 * Uses Keycloak (via Testcontainers) as the real OIDC server - matching the design of
 * existing OAuth2 tests (TestRESTCatalogOAuth2Jwt). Verifies that the standalone server correctly
 * enforces JWT auth.
 *
 * <p>Requires Docker to be available (Testcontainers starts Keycloak in a container).
 *
 * <p>Verifies:
 * - Accepts valid JWT tokens from Keycloak
 * - Rejects invalid/malformed tokens
 * - Rejects requests without a Bearer token
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
    classes = BaseStandaloneRESTCatalogServerTest.TestRestCatalogApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration"
    }
)
@ContextConfiguration(initializers = TestStandaloneRESTCatalogServerJwtAuth.RestCatalogJwtTestContextInitializer.class)
@TestExecutionListeners(
    listeners = {
        BaseStandaloneRESTCatalogServerTest.HmsStartupListener.class,
        TestStandaloneRESTCatalogServerJwtAuth.KeycloakStartupListener.class
    },
    mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS
)
public class TestStandaloneRESTCatalogServerJwtAuth extends BaseStandaloneRESTCatalogServerTest {
  @LocalServerPort
  private int port;

  @Autowired
  private StandaloneRESTCatalogServer server;

  private static OAuth2AuthorizationServer authorizationServer;

  @Override
  protected int getPort() {
    return port;
  }

  @Order(Ordered.HIGHEST_PRECEDENCE - 1)
  public static class KeycloakStartupListener implements TestExecutionListener {
    @Override
    public void beforeTestClass(TestContext testContext) throws Exception {
      if (authorizationServer != null) {
        return;
      }
      // Use accessTokenHeaderTypeRfc9068=false so Keycloak emits "JWT" (not "at+jwt") in the token
      // header - SimpleJWTAuthenticator accepts null and JWT but not "at+jwt" by default.
      authorizationServer = new OAuth2AuthorizationServer(
          org.testcontainers.containers.Network.newNetwork(), false);
      authorizationServer.start();
      LOG.info("Started Keycloak authorization server at {}", authorizationServer.getIssuer());
    }
  }

  public static class RestCatalogJwtTestContextInitializer
      implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(ConfigurableApplicationContext context) {
      String jwksUrl = authorizationServer.getIssuer() + "/protocol/openid-connect/certs";

      Configuration restCatalogConf = MetastoreConf.newMetastoreConf();
      String hmsUri = "thrift://localhost:" + hmsPort;
      MetastoreConf.setVar(restCatalogConf, ConfVars.THRIFT_URIS, hmsUri);
      MetastoreConf.setVar(restCatalogConf, ConfVars.WAREHOUSE, warehouseDir.getAbsolutePath());
      MetastoreConf.setVar(restCatalogConf, ConfVars.WAREHOUSE_EXTERNAL, warehouseDir.getAbsolutePath());
      MetastoreConf.setVar(restCatalogConf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH, "iceberg");
      MetastoreConf.setVar(restCatalogConf, ConfVars.CATALOG_SERVLET_AUTH, "jwt");
      MetastoreConf.setVar(restCatalogConf, ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL, jwksUrl);
      MetastoreConf.setLongVar(restCatalogConf, ConfVars.CATALOG_SERVLET_PORT, 0);
      context.getBeanFactory().registerSingleton("hadoopConfiguration", restCatalogConf);
      StandaloneRESTCatalogServer server = new StandaloneRESTCatalogServer(restCatalogConf);
      context.getBeanFactory().registerSingleton("standaloneRESTCatalogServer", server);
    }
  }

  @AfterClass
  public static void teardownClass() throws IOException {
    if (authorizationServer != null) {
      try {
        authorizationServer.stop();
      } catch (Exception e) {
        LOG.warn("Failed to stop Keycloak (may not have started): {}", e.getMessage());
      }
    }
    teardownBase();
  }

  @Test(timeout = 120000)
  public void testRESTCatalogConfigWithValidToken() throws Exception {
    LOG.info("=== Test: REST Catalog Config with Valid JWT from Keycloak ===");

    String token = authorizationServer.getAccessToken();
    try (CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = httpClient.execute(get("/iceberg/v1/config", token))) {
      assertEquals("Config endpoint with valid JWT should return 200", 200, response.getStatusLine().getStatusCode());
      String responseBody = EntityUtils.toString(response.getEntity());
      assertTrue("Response should contain endpoints", responseBody.contains("endpoints"));
      LOG.info("Config with valid JWT passed");
    }
  }

  @Test(timeout = 60000)
  public void testRESTCatalogRejectsInvalidToken() throws Exception {
    LOG.info("=== Test: REST Catalog Rejects Invalid JWT ===");

    String invalidToken = "invalid-token-not-a-valid-jwt";
    try (CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = httpClient.execute(get("/iceberg/v1/config", invalidToken))) {
      assertEquals("Config endpoint with invalid JWT should return 401", 401, response.getStatusLine().getStatusCode());
      LOG.info("Invalid JWT correctly rejected");
    }
  }

  @Test(timeout = 60000)
  public void testRESTCatalogRejectsRequestWithoutToken() throws Exception {
    LOG.info("=== Test: REST Catalog Rejects Request Without Token ===");

    try (CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = httpClient.execute(get("/iceberg/v1/config"))) {
      assertEquals("Config endpoint without token should return 401", 401, response.getStatusLine().getStatusCode());
      LOG.info("Request without token correctly rejected");
    }
  }

  @Test(timeout = 120000)
  public void testRESTCatalogNamespaceOperationsWithValidToken() throws Exception {
    LOG.info("=== Test: REST Catalog Namespace Operations with Valid JWT ===");

    String token = authorizationServer.getAccessToken();
    String namespacePath = "/iceberg/v1/namespaces";
    String namespaceName = "jwt_test_db";

    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      try (CloseableHttpResponse response = httpClient.execute(get(namespacePath, token))) {
        assertEquals("List namespaces with valid JWT should return 200", 200, response.getStatusLine().getStatusCode());
      }

      try (CloseableHttpResponse response = httpClient.execute(
          post(namespacePath, "{\"namespace\":[\"" + namespaceName + "\"]}", token))) {
        assertEquals("Create namespace with valid JWT should return 200", 200, 
            response.getStatusLine().getStatusCode());
      }

      try (CloseableHttpResponse response = httpClient.execute(
          get("/iceberg/v1/namespaces/" + namespaceName, token))) {
        assertEquals("Get namespace with valid JWT should return 200", 200, response.getStatusLine().getStatusCode());
        String responseBody = EntityUtils.toString(response.getEntity());
        assertTrue("Response should contain namespace", responseBody.contains(namespaceName));
      }
    }

    LOG.info("Namespace operations with JWT passed");
  }

  @Test(timeout = 60000)
  public void testServerPort() {
    super.testServerPort(server);
  }

  @Test(timeout = 60000)
  public void testHealthEndpointsRemainUnauthenticated() throws Exception {
    LOG.info("=== Test: Health endpoints work without auth (for K8s probes) ===");

    super.testLivenessProbe();
    super.testReadinessProbe();

    LOG.info("Health endpoints accessible without auth");
  }
}
