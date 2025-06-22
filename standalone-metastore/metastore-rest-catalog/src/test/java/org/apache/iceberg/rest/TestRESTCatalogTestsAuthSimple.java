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

import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.rest.extension.HiveRESTCatalogServerExtension;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Category(MetastoreCheckinTest.class)
class TestRESTCatalogTestsAuthSimple extends BaseRESTCatalogTests {
  @RegisterExtension
  private static final HiveRESTCatalogServerExtension REST_CATALOG_EXTENSION = HiveRESTCatalogServerExtension.builder()
      .build();

  private static RESTCatalog catalog;
  private static Map<String, String> baseProperties;

  @BeforeAll
  static void beforeClass() {
    baseProperties = Map.of(
        "uri", REST_CATALOG_EXTENSION.getRestEndpoint(),
        "header.x-actor-username", "USER_1"
    );
    catalog = RCKUtils.initCatalogClient(baseProperties);
    Assertions.assertEquals(Collections.singletonList(Namespace.of("default")), catalog.listNamespaces());
  }

  @BeforeEach
  void before() {
    RCKUtils.purgeCatalogTestEntries(catalog);
  }

  @AfterAll
  static void afterClass() throws Exception {
    catalog.close();
  }

  TestRESTCatalogTestsAuthSimple() {
    super(catalog, baseProperties);
  }

  @Test
  void testWithoutUserName() {
    Map<String, String> properties = Map.of(
        "uri", REST_CATALOG_EXTENSION.getRestEndpoint()
    );
    NotAuthorizedException error = Assertions.assertThrows(NotAuthorizedException.class,
        () -> RCKUtils.initCatalogClient(properties));
    Assertions.assertEquals("Not authorized: Authentication error: User header x-actor-username missing in request",
        error.getMessage());
  }
}
