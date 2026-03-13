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
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.iceberg.rest.extension.OAuth2AuthorizationServer;
import org.apache.iceberg.rest.standalone.RestCatalogServerRuntime;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;
import org.springframework.core.Ordered;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListener;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

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
    classes = {
        BaseStandaloneRESTCatalogServerTest.TestRestCatalogApplication.class,
        TestStandaloneRESTCatalogServerJwtAuth.TestConfig.class
    },
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
        "spring.main.allow-bean-definition-overriding=true"
    }
)
@TestExecutionListeners(
    listeners = {
        BaseStandaloneRESTCatalogServerTest.HmsStartupListener.class,
        TestStandaloneRESTCatalogServerJwtAuth.KeycloakStartupListener.class
    },
    mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS
)
public class TestStandaloneRESTCatalogServerJwtAuth extends BaseStandaloneRESTCatalogServerTest {
  private static OAuth2AuthorizationServer authorizationServer;

  @Override
  protected String getBearerTokenForCatalogTests() {
    return authorizationServer != null ? authorizationServer.getAccessToken() : null;
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

  @TestConfiguration
  static class TestConfig {
    @Bean
    @Primary
    public Configuration hadoopConfiguration() {
      Configuration conf = createBaseTestConfiguration();
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH, "jwt");
      MetastoreConf.setVar(conf, ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL,
          authorizationServer.getIssuer() + "/protocol/openid-connect/certs");
      return conf;
    }

    @Bean
    @Primary
    public RestCatalogServerRuntime restCatalogServerRuntime(ServerProperties serverProperties) {
      Configuration conf = createBaseTestConfiguration();
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH, "jwt");
      MetastoreConf.setVar(conf, ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL,
          authorizationServer.getIssuer() + "/protocol/openid-connect/certs");
      return new RestCatalogServerRuntime(conf, serverProperties);
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

  @Test(timeout = 60000)
  public void testRESTCatalogRejectsInvalidToken() throws Exception {
    LOG.info("=== Test: REST Catalog Rejects Invalid JWT ===");

    String invalidToken = "invalid-token-not-a-valid-jwt";
    try (CloseableHttpClient httpClient = createHttpClient();
        CloseableHttpResponse response = httpClient.execute(get("/iceberg/v1/config", invalidToken))) {
      assertEquals("Config endpoint with invalid JWT should return 401", 401, response.getStatusLine().getStatusCode());
      LOG.info("Invalid JWT correctly rejected");
    }
  }

  @Test(timeout = 60000)
  public void testRESTCatalogRejectsRequestWithoutToken() throws Exception {
    LOG.info("=== Test: REST Catalog Rejects Request Without Token ===");

    try (CloseableHttpClient httpClient = createHttpClient();
        CloseableHttpResponse response = httpClient.execute(get("/iceberg/v1/config"))) {
      assertEquals("Config endpoint without token should return 401", 401, response.getStatusLine().getStatusCode());
      LOG.info("Request without token correctly rejected");
    }
  }
}
