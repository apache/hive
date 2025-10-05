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

import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.view.ViewCatalogTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class BaseRESTViewCatalogTests extends ViewCatalogTests<RESTCatalog> {
  private RESTCatalog catalog;

  protected abstract Map<String, String> getDefaultClientConfiguration() throws Exception;

  @BeforeAll
  void setupAll() throws Exception {
    catalog = RCKUtils.initCatalogClient(getDefaultClientConfiguration());
    Assertions.assertEquals(Collections.singletonList(Namespace.of("default")), catalog.listNamespaces());
  }

  @BeforeEach
  void setup() {
    RCKUtils.purgeCatalogTestEntries(catalog);
  }

  @AfterAll
  void teardownAll() throws Exception {
    catalog.close();
  }

  @Override
  protected RESTCatalog catalog() {
    return catalog;
  }

  @Override
  protected RESTCatalog tableCatalog() {
    return catalog;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }
}
