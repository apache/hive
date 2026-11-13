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

import java.io.IOException;
import java.util.Collections;
import javax.servlet.http.HttpServletRequest;
import org.apache.iceberg.rest.HMSCatalogServlet.ServletRequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestHMSCatalogServlet {

  @Test
  public void testPathExtractionPreservesUrlEncodedCharacters() throws IOException {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getMethod()).thenReturn("GET");
    Mockito.when(request.getContextPath()).thenReturn("");
    Mockito.when(request.getServletPath()).thenReturn("/iceberg");
    Mockito.when(request.getRequestURI())
        .thenReturn("/iceberg/v1/namespaces/ns/tables/table%2Fwith%2Fslash");
    Mockito.when(request.getParameterMap()).thenReturn(Collections.emptyMap());

    ServletRequestContext context = ServletRequestContext.from(request);
    Assertions.assertTrue(context.error().isEmpty(), "Request should match a route without error");
    Assertions.assertEquals(
        "v1/namespaces/ns/tables/table%2Fwith%2Fslash",
        context.path(), "Raw percent-encoded slash must not be auto-decoded before route matching");
  }

  @Test
  public void testPathExtractionWithContextPath() throws IOException {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getMethod()).thenReturn("GET");
    Mockito.when(request.getContextPath()).thenReturn("/hms");
    Mockito.when(request.getServletPath()).thenReturn("/iceberg");
    Mockito.when(request.getRequestURI())
        .thenReturn("/hms/iceberg/v1/namespaces/ns/tables/table%20name");
    Mockito.when(request.getParameterMap()).thenReturn(Collections.emptyMap());

    ServletRequestContext context = ServletRequestContext.from(request);
    Assertions.assertTrue(context.error().isEmpty());
    Assertions.assertEquals(
        "v1/namespaces/ns/tables/table%20name",
        context.path(), "Raw percent-encoded space must be preserved in path");
  }

  @Test
  public void testPathExtractionWithRootMapping() throws IOException {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getMethod()).thenReturn("GET");
    Mockito.when(request.getContextPath()).thenReturn("");
    Mockito.when(request.getServletPath()).thenReturn("");
    Mockito.when(request.getRequestURI()).thenReturn("/v1/namespaces/ns/tables/table_name");
    Mockito.when(request.getParameterMap()).thenReturn(Collections.emptyMap());

    ServletRequestContext context = ServletRequestContext.from(request);
    Assertions.assertTrue(context.error().isEmpty());
    Assertions.assertEquals("v1/namespaces/ns/tables/table_name", context.path());
  }

  @Test
  public void testPathExtractionDefensiveNullHandling() throws IOException {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getMethod()).thenReturn("GET");
    Mockito.when(request.getContextPath()).thenReturn(null);
    Mockito.when(request.getServletPath()).thenReturn(null);
    Mockito.when(request.getRequestURI()).thenReturn("/v1/namespaces/ns/tables/table_name");
    Mockito.when(request.getParameterMap()).thenReturn(Collections.emptyMap());

    ServletRequestContext context = ServletRequestContext.from(request);
    Assertions.assertTrue(context.error().isEmpty());
    Assertions.assertEquals("v1/namespaces/ns/tables/table_name", context.path());
  }

  @Test
  public void testInvalidRouteReturnsBadRequest() throws IOException {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getMethod()).thenReturn("GET");
    Mockito.when(request.getContextPath()).thenReturn("");
    Mockito.when(request.getServletPath()).thenReturn("/iceberg");
    Mockito.when(request.getRequestURI()).thenReturn("/iceberg/invalid/unrecognized/endpoint");
    Mockito.when(request.getParameterMap()).thenReturn(Collections.emptyMap());

    ServletRequestContext context = ServletRequestContext.from(request);
    Assertions.assertTrue(
        context.error().isPresent(), "Unrecognized route should produce an error");
    Assertions.assertEquals(400, (int) context.error().get().code());
  }
}
