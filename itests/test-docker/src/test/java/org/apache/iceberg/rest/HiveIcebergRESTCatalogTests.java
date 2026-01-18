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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.catalog.CatalogTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

class HiveIcebergRESTCatalogTests extends CatalogTests<RESTCatalog> {
  private static HiveIcebergRESTCatalogClient client;

  @BeforeAll
  static void beforeClass() throws Exception {
    client = new HiveIcebergRESTCatalogClient();

    assertThat(client.getRestCatalog().listNamespaces())
        .withFailMessage("Namespaces list should not contain: %s", RCKUtils.TEST_NAMESPACES)
        .doesNotContainAnyElementsOf(RCKUtils.TEST_NAMESPACES);
  }

  @BeforeEach
  void before() throws Exception {
    client.cleanupWarehouse();
  }

  @AfterAll
  static void afterClass() throws Exception {
    client.close();
  }

  @Override
  protected RESTCatalog catalog() {
    return client.getRestCatalog();
  }

  @Override
  protected RESTCatalog initCatalog(String catalogName, Map<String, String> additionalProperties) {
    try {
      return new HiveIcebergRESTCatalogClient(additionalProperties).getRestCatalog();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsNamesWithSlashes() {
    return false;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }
}
