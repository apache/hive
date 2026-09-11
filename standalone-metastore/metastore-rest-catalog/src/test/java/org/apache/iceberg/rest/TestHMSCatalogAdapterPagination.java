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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import javax.servlet.http.HttpServletResponse;

public class TestHMSCatalogAdapterPagination {

  @Test
  public void testPaginationDelegation() throws Exception {
    Catalog catalog =
        Mockito.mock(
            Catalog.class,
            Mockito.withSettings().extraInterfaces(SupportsNamespaces.class, ViewCatalog.class));
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
    Mockito.when(nsCatalog.listNamespaces(Mockito.any())).thenReturn(Collections.emptyList());

    ListNamespacesResponse res3;
    try (HMSCatalogAdapter adapter =
        new HMSCatalogAdapter("test", catalog, null, Collections.emptyList())) {

      HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
      StringWriter stringWriter = new StringWriter();
      Mockito.when(response.getWriter()).thenReturn(new PrintWriter(stringWriter));

      // 1. Missing pageSize (should call unpaginated and succeed without NumberFormatException)
      Map<String, String> vars1 = ImmutableMap.of("pageToken", "0");
      ListNamespacesResponse res1 =
          adapter.execute(HTTPMethod.GET, "v1/namespaces", vars1, null, response);
      if (res1 == null) {
        System.err.println("Error output: " + stringWriter);
      }
      Assertions.assertNotNull(res1, "Response should not be null");

      // 2. Both pageToken and pageSize (should call paginated and slice without errors)
      Map<String, String> vars2 = ImmutableMap.of("pageToken", "0", "pageSize", "10");
      ListNamespacesResponse res2 =
          adapter.execute(HTTPMethod.GET, "v1/namespaces", vars2, null, response);
      Assertions.assertNotNull(res2, "Response should not be null");

      // 3. pageSize without pageToken
      Map<String, String> vars3 = ImmutableMap.of("pageSize", "10");
      res3 = adapter.execute(HTTPMethod.GET, "v1/namespaces", vars3, null, response);
    }
    Assertions.assertNotNull(res3, "Response should not be null");
  }
}
