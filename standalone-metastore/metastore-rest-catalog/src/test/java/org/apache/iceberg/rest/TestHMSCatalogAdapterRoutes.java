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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest;

import java.util.Map;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;

public class TestHMSCatalogAdapterRoutes {

  @Test
  public void testNoPrefix() {
    Pair<HMSCatalogAdapter.Route, Map<String, String>> route =
        HMSCatalogAdapter.Route.from(HTTPMethod.GET, "v1/namespaces");
    Assertions.assertNotNull(route, "Route should match");
    Assertions.assertEquals(HMSCatalogAdapter.Route.LIST_NAMESPACES, route.first());
    Assertions.assertNull(route.second().get("prefix"), "Should not have prefix");
  }

  @Test
  public void testSingleSegmentPrefix() {
    Pair<HMSCatalogAdapter.Route, Map<String, String>> route =
        HMSCatalogAdapter.Route.from(HTTPMethod.GET, "v1/my_catalog/namespaces/accounting/tables");
    Assertions.assertNotNull(route, "Route should match");
    Assertions.assertEquals(HMSCatalogAdapter.Route.LIST_TABLES, route.first());
    Assertions.assertEquals("my_catalog", route.second().get("prefix"));
    Assertions.assertEquals("accounting", route.second().get("namespace"));
  }

  @Test
  public void testMultiSegmentPrefix() {
    Pair<HMSCatalogAdapter.Route, Map<String, String>> route =
        HMSCatalogAdapter.Route.from(
            HTTPMethod.GET, "v1/catalogs/sales/namespaces/accounting/tables/my_table");
    Assertions.assertNotNull(route, "Route should match");
    Assertions.assertEquals(HMSCatalogAdapter.Route.LOAD_TABLE, route.first());
    Assertions.assertEquals("catalogs/sales", route.second().get("prefix"));
    Assertions.assertEquals("accounting", route.second().get("namespace"));
    Assertions.assertEquals("my_table", route.second().get("table"));
  }

  @Test
  public void testTripleSegmentPrefix() {
    Pair<HMSCatalogAdapter.Route, Map<String, String>> route =
        HMSCatalogAdapter.Route.from(
            HTTPMethod.GET, "v1/us-east-1/prod/tenant_99/namespaces/accounting/tables");
    Assertions.assertNotNull(route, "Route should match");
    Assertions.assertEquals(HMSCatalogAdapter.Route.LIST_TABLES, route.first());
    Assertions.assertEquals("us-east-1/prod/tenant_99", route.second().get("prefix"));
    Assertions.assertEquals("accounting", route.second().get("namespace"));
  }

  @Test
  public void testNegativeCases() {
    // 1. Wrong HTTP Method (POST instead of GET for LIST_NAMESPACES)
    Pair<HMSCatalogAdapter.Route, Map<String, String>> wrongMethod =
        HMSCatalogAdapter.Route.from(HTTPMethod.POST, "v1/catalogs/sales/namespaces");
    // Should match CREATE_NAMESPACE instead of LIST_NAMESPACES
    Assertions.assertNotNull(wrongMethod, "Route should match CREATE_NAMESPACE");
    Assertions.assertEquals(HMSCatalogAdapter.Route.CREATE_NAMESPACE, wrongMethod.first());

    // 2. Bad path (wrong resource string)
    Pair<HMSCatalogAdapter.Route, Map<String, String>> badResource =
        HMSCatalogAdapter.Route.from(HTTPMethod.GET, "v1/catalogs/sales/views/accounting");
    Assertions.assertNull(badResource, "Should not match any known pattern");

    // 3. Path too short (missing required anchors)
    Pair<HMSCatalogAdapter.Route, Map<String, String>> pathTooShort =
        HMSCatalogAdapter.Route.from(HTTPMethod.GET, "v1/");
    Assertions.assertNull(pathTooShort, "Should not match, missing namespaces anchor");

    // 4. Path too long for a route without a prefix placeholder (config route)
    Pair<HMSCatalogAdapter.Route, Map<String, String>> configTooLong =
        HMSCatalogAdapter.Route.from(HTTPMethod.GET, "v1/my_catalog/config");
    Assertions.assertNull(configTooLong, "Should not match config route, doesn't accept prefix");
  }

  private Pair<HMSCatalogAdapter.Route, Map<String, String>> route(String path) {
    return HMSCatalogAdapter.Route.from(HTTPMethod.GET, path);
  }

  @Test
  public void testResourceNamedNamespacesIsNotTreatedAsPrefix() {
    // Case 1: LOAD_NAMESPACE
    Pair<HMSCatalogAdapter.Route, Map<String, String>> loadNamespace =
        route("v1/namespaces/namespaces");
    Assertions.assertNotNull(loadNamespace, "Should match LOAD_NAMESPACE");
    Assertions.assertEquals(HMSCatalogAdapter.Route.LOAD_NAMESPACE, loadNamespace.first());

    // Case 2: LOAD_TABLE
    Pair<HMSCatalogAdapter.Route, Map<String, String>> loadTable =
        route("v1/namespaces/db/tables/namespaces");
    Assertions.assertNotNull(loadTable, "Should match LOAD_TABLE");
    Assertions.assertEquals(HMSCatalogAdapter.Route.LOAD_TABLE, loadTable.first());

    // Case 3: UPDATE_TABLE
    Pair<HMSCatalogAdapter.Route, Map<String, String>> updateTable =
        HMSCatalogAdapter.Route.from(HTTPMethod.POST, "v1/namespaces/db/tables/namespaces");
    Assertions.assertNotNull(updateTable, "Should match UPDATE_TABLE");
    Assertions.assertEquals(HMSCatalogAdapter.Route.UPDATE_TABLE, updateTable.first());

    // Case 4: LOAD_TABLE with prefix
    Pair<HMSCatalogAdapter.Route, Map<String, String>> loadTableWithPrefix =
        route("v1/catalogs/sales/namespaces/db/tables/namespaces");
    Assertions.assertNotNull(loadTableWithPrefix, "Should match LOAD_TABLE with prefix");
    Assertions.assertEquals(HMSCatalogAdapter.Route.LOAD_TABLE, loadTableWithPrefix.first());
  }
}
