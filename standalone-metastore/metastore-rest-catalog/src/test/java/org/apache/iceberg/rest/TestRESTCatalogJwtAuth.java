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

import java.util.Map;
import org.apache.hadoop.hive.metastore.ServletSecurity.AuthType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.rest.extension.HiveRESTCatalogServerExtension;
import org.apache.iceberg.rest.extension.JwksServer;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Category(MetastoreCheckinTest.class)
class TestRESTCatalogJwtAuth extends BaseRESTCatalogTests {
  @RegisterExtension
  private static final HiveRESTCatalogServerExtension REST_CATALOG_EXTENSION =
      HiveRESTCatalogServerExtension.builder(AuthType.JWT).build();

  @Override
  protected Map<String, String> getDefaultClientConfiguration() throws Exception {
    return Map.of(
        "uri", REST_CATALOG_EXTENSION.getRestEndpoint(),
        "token", JwksServer.generateValidJWT("USER_1")
    );
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
