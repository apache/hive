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

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.catalog.CatalogTests;

abstract class BaseRESTCatalogTests extends CatalogTests<RESTCatalog> {
  private final Map<String, String> baseProperties;
  private final RESTCatalog catalog;

  protected BaseRESTCatalogTests(RESTCatalog catalog, Map<String, String> baseProperties) {
    this.catalog = catalog;
    this.baseProperties = baseProperties;
  }

  @Override
  protected RESTCatalog catalog() {
    return catalog;
  }

  @Override
  protected RESTCatalog initCatalog(String catalogName, Map<String, String> additionalProperties) {
    Map<String, String> properties = new HashMap<>(baseProperties);
    properties.putAll(additionalProperties);
    return RCKUtils.initCatalogClient(properties);
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
