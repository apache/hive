/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.rest.extension.HiveRESTCatalogServerExtension;
import org.apache.iceberg.rest.extension.JwksServer;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Category(MetastoreCheckinTest.class)
class TestRESTCatalogTestsAuthJwt extends BaseRESTCatalogTests {
  @RegisterExtension
  private static final HiveRESTCatalogServerExtension REST_CATALOG_EXTENSION = HiveRESTCatalogServerExtension.builder()
      .jwt().build();

  private static RESTCatalog catalog;
  private static Map<String, String> baseProperties;

  @BeforeAll
  static void beforeClass() throws Exception {
    baseProperties = Map.of(
        "uri", REST_CATALOG_EXTENSION.getRestEndpoint(),
        "token", JwksServer.generateValidJWT("USER_1")
    );
    catalog = RCKUtils.initCatalogClient(baseProperties);
    Assertions.assertEquals(Collections.singletonList(Namespace.of("default")), catalog.listNamespaces());
  }

  @BeforeEach
  void before() throws IOException {
    RCKUtils.purgeCatalogTestEntries(catalog);
    REST_CATALOG_EXTENSION.reset();
  }

  @AfterAll
  static void afterClass() throws Exception {
    catalog.close();
  }

  TestRESTCatalogTestsAuthJwt() {
    super(catalog, baseProperties);
  }

  @Test
  void testWithUnauthorizedKey() throws Exception {
    // "token" is a parameter for OAuth 2.0 Bearer token authentication. We use it to pass a JWT token
    Map<String, String> properties = Map.of(
        "uri", REST_CATALOG_EXTENSION.getRestEndpoint(),
        "token", JwksServer.generateInvalidJWT("USER_1")
    );
    NotAuthorizedException error = Assertions.assertThrows(NotAuthorizedException.class,
        () -> RCKUtils.initCatalogClient(properties));
    Assertions.assertEquals(
        "Not authorized: Authentication error: Failed to validate JWT from Bearer token in Authentication header",
        error.getMessage());
  }

  @Test
  void testWithoutToken() {
    Map<String, String> properties = Map.of(
        "uri", REST_CATALOG_EXTENSION.getRestEndpoint()
    );
    NotAuthorizedException error = Assertions.assertThrows(NotAuthorizedException.class,
        () -> RCKUtils.initCatalogClient(properties));
    Assertions.assertEquals(
        "Not authorized: Authentication error: Couldn't find bearer token in the auth header in the request",
        error.getMessage());
  }
}
