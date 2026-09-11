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
  public void testPrefixRouting() {
    // 1. Test standard (no prefix)
    Pair<HMSCatalogAdapter.Route, Map<String, String>> noPrefix =
        HMSCatalogAdapter.Route.from(HTTPMethod.GET, "v1/namespaces");
    Assertions.assertNotNull(noPrefix, "Route should match");
    Assertions.assertEquals(HMSCatalogAdapter.Route.LIST_NAMESPACES, noPrefix.first());
    Assertions.assertNull(noPrefix.second().get("prefix"), "Should not have prefix");

    // 2. Test 1-segment prefix (e.g., Polaris)
    Pair<HMSCatalogAdapter.Route, Map<String, String>> singlePrefix =
        HMSCatalogAdapter.Route.from(HTTPMethod.GET, "v1/my_catalog/namespaces/accounting/tables");
    Assertions.assertNotNull(singlePrefix, "Route should match");
    Assertions.assertEquals(HMSCatalogAdapter.Route.LIST_TABLES, singlePrefix.first());
    Assertions.assertEquals("my_catalog", singlePrefix.second().get("prefix"));
    Assertions.assertEquals("accounting", singlePrefix.second().get("namespace"));

    // 3. Test multi-segment prefix (e.g., Databricks Unity)
    Pair<HMSCatalogAdapter.Route, Map<String, String>> multiPrefix =
        HMSCatalogAdapter.Route.from(
            HTTPMethod.GET, "v1/catalogs/sales/namespaces/accounting/tables/my_table");
    Assertions.assertNotNull(multiPrefix, "Route should match");
    Assertions.assertEquals(HMSCatalogAdapter.Route.LOAD_TABLE, multiPrefix.first());
    Assertions.assertEquals("catalogs/sales", multiPrefix.second().get("prefix"));
    Assertions.assertEquals("accounting", multiPrefix.second().get("namespace"));
    Assertions.assertEquals("my_table", multiPrefix.second().get("table"));

    // 4. Test 3-segment prefix
    Pair<HMSCatalogAdapter.Route, Map<String, String>> triplePrefix =
        HMSCatalogAdapter.Route.from(
            HTTPMethod.GET, "v1/us-east-1/prod/tenant_99/namespaces/accounting/tables");
    Assertions.assertNotNull(triplePrefix, "Route should match");
    Assertions.assertEquals(HMSCatalogAdapter.Route.LIST_TABLES, triplePrefix.first());
    Assertions.assertEquals("us-east-1/prod/tenant_99", triplePrefix.second().get("prefix"));
    Assertions.assertEquals("accounting", triplePrefix.second().get("namespace"));

    // 5. Test bad request (wrong resource)
    Pair<HMSCatalogAdapter.Route, Map<String, String>> badPath =
        HMSCatalogAdapter.Route.from(HTTPMethod.GET, "v1/catalogs/sales/views/accounting");
    Assertions.assertNull(badPath, "Should not match");
  }
}
