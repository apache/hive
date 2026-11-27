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
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Catalog.TableBuilder;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TestHMSCatalogAdapterETag {

  private static final String TABLE_PATH = "v1/namespaces/accounting/tables/my_table";
  private static final String TABLES_PATH = "v1/namespaces/accounting/tables";
  private static final String REGISTER_PATH = "v1/namespaces/accounting/register";
  private static final String METADATA_LOCATION = "file:///tmp/table/metadata.json";
  private static final Map<String, String> DEFAULT_SNAPSHOT_PARAMS =
      ImmutableMap.of(
          RESTCatalogProperties.SNAPSHOTS_QUERY_PARAMETER,
          RESTCatalogProperties.SnapshotMode.ALL.name().toLowerCase(Locale.ROOT));

  private Catalog catalog;
  private IcebergAuthorizer authorizer;
  private BaseTable table;
  private TableOperations operations;
  private HMSCatalogAdapter adapter;
  private HttpServletResponse response;
  private StringWriter stringWriter;
  private TableMetadata initialMetadata;

  @BeforeEach
  void setup() throws Exception {
    catalog =
        Mockito.mock(
            Catalog.class,
            Mockito.withSettings().extraInterfaces(SupportsNamespaces.class, ViewCatalog.class));
    authorizer = Mockito.mock(IcebergAuthorizer.class);

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    Snapshot snap1 = Mockito.mock(Snapshot.class);
    Mockito.when(snap1.snapshotId()).thenReturn(1L);
    Mockito.when(snap1.sequenceNumber()).thenReturn(1L);
    Mockito.when(snap1.timestampMillis()).thenReturn(1000L);

    Snapshot snap2 = Mockito.mock(Snapshot.class);
    Mockito.when(snap2.snapshotId()).thenReturn(2L);
    Mockito.when(snap2.sequenceNumber()).thenReturn(2L);
    Mockito.when(snap2.timestampMillis()).thenReturn(2000L);

    initialMetadata =
        TableMetadata.buildFromEmpty(2)
            .setLocation("file:///tmp/table")
            .addSchema(schema)
            .setDefaultPartitionSpec(PartitionSpec.unpartitioned())
            .setDefaultSortOrder(SortOrder.unsorted())
            .addSnapshot(snap1)
            .addSnapshot(snap2)
            .setBranchSnapshot(2L, SnapshotRef.MAIN_BRANCH)
            .discardChanges()
            .withMetadataLocation(METADATA_LOCATION)
            .build();

    table = Mockito.mock(BaseTable.class);
    operations = Mockito.mock(TableOperations.class);
    Mockito.when(table.operations()).thenReturn(operations);
    Mockito.when(operations.current()).thenReturn(initialMetadata);
    Mockito.when(catalog.loadTable(Mockito.any(TableIdentifier.class))).thenReturn(table);

    adapter = new HMSCatalogAdapter("test_catalog", catalog, authorizer, List.of());

    response = Mockito.mock(HttpServletResponse.class);
    stringWriter = new StringWriter();
    Mockito.when(response.getWriter()).thenReturn(new PrintWriter(stringWriter));
  }

  @AfterEach
  void tearDown() {
    if (adapter != null) {
      adapter.close();
    }
  }

  @Test
  void testLoadTableReturnsETagHeader() throws Exception {
    LoadTableResponse res =
        adapter.execute(HTTPMethod.GET, TABLE_PATH, DEFAULT_SNAPSHOT_PARAMS, null, response);

    Assertions.assertNotNull(res, "Response should not be null");

    String expectedETag = ETagProvider.of(METADATA_LOCATION, DEFAULT_SNAPSHOT_PARAMS);
    Assertions.assertTrue(
        expectedETag.startsWith("\"") && expectedETag.endsWith("\""),
        "ETag must be quoted per RFC 9110");

    Mockito.verify(response).setHeader("ETag", expectedETag);
  }

  @Test
  void testLoadTableWithoutSnapshotsParamMatchesDefaultAllETag() throws Exception {
    String expectedETag = ETagProvider.of(METADATA_LOCATION, DEFAULT_SNAPSHOT_PARAMS);
    Map<String, String> headers = Map.of("If-None-Match", expectedETag);

    LoadTableResponse res =
        adapter.execute(HTTPMethod.GET, TABLE_PATH, headers, Map.of(), null, response);

    Assertions.assertNull(res, "Response must be null on 304 Not Modified");
    Mockito.verify(response).setStatus(HttpServletResponse.SC_NOT_MODIFIED);
    Mockito.verify(response).setHeader("ETag", expectedETag);
  }

  @Test
  void testLoadTableWithMultiValueIfNoneMatchReturns304() throws Exception {
    String expectedETag = ETagProvider.of(METADATA_LOCATION, DEFAULT_SNAPSHOT_PARAMS);
    Map<String, String> headers =
        Map.of("If-None-Match", "\"old-etag-1\", " + expectedETag + ", \"old-etag-2\"");

    LoadTableResponse res =
        adapter.execute(
            HTTPMethod.GET, TABLE_PATH, headers, DEFAULT_SNAPSHOT_PARAMS, null, response);

    Assertions.assertNull(
        res, "Response must be null when one of the ETags in If-None-Match matches");
    Mockito.verify(response).setStatus(HttpServletResponse.SC_NOT_MODIFIED);
    Mockito.verify(response).setHeader("ETag", expectedETag);
  }

  @Test
  void testLoadTableWithWildcardIfNoneMatchReturns304() throws Exception {
    String expectedETag = ETagProvider.of(METADATA_LOCATION, DEFAULT_SNAPSHOT_PARAMS);
    Map<String, String> headers = Map.of("If-None-Match", "*");

    LoadTableResponse res =
        adapter.execute(
            HTTPMethod.GET, TABLE_PATH, headers, DEFAULT_SNAPSHOT_PARAMS, null, response);

    Assertions.assertNull(res, "Response must be null when If-None-Match is wildcard *");
    Mockito.verify(response).setStatus(HttpServletResponse.SC_NOT_MODIFIED);
    Mockito.verify(response).setHeader("ETag", expectedETag);
  }

  @Test
  void testLoadTableWithCaseInsensitiveIfNoneMatchHeader() throws Exception {
    String expectedETag = ETagProvider.of(METADATA_LOCATION, DEFAULT_SNAPSHOT_PARAMS);

    HttpServletResponse respLower = Mockito.mock(HttpServletResponse.class);
    Map<String, String> headersLower = Map.of("if-none-match", expectedETag);
    LoadTableResponse resLower =
        adapter.execute(
            HTTPMethod.GET, TABLE_PATH, headersLower, DEFAULT_SNAPSHOT_PARAMS, null, respLower);
    Assertions.assertNull(resLower);
    Mockito.verify(respLower).setStatus(HttpServletResponse.SC_NOT_MODIFIED);

    HttpServletResponse respUpper = Mockito.mock(HttpServletResponse.class);
    Map<String, String> headersUpper = Map.of("IF-NONE-MATCH", expectedETag);
    LoadTableResponse resUpper =
        adapter.execute(
            HTTPMethod.GET, TABLE_PATH, headersUpper, DEFAULT_SNAPSHOT_PARAMS, null, respUpper);
    Assertions.assertNull(resUpper);
    Mockito.verify(respUpper).setStatus(HttpServletResponse.SC_NOT_MODIFIED);
  }

  @Test
  void testLoadTableWithMismatchedIfNoneMatchReturns200() throws Exception {
    Map<String, String> headers = Map.of("If-None-Match", "\"stale-etag-value\"");

    LoadTableResponse res =
        adapter.execute(
            HTTPMethod.GET, TABLE_PATH, headers, DEFAULT_SNAPSHOT_PARAMS, null, response);

    Assertions.assertNotNull(res, "Response should not be null when ETag mismatches");
    Mockito.verify(response, Mockito.never()).setStatus(HttpServletResponse.SC_NOT_MODIFIED);

    String expectedETag = ETagProvider.of(METADATA_LOCATION, DEFAULT_SNAPSHOT_PARAMS);
    Mockito.verify(response).setHeader("ETag", expectedETag);
  }

  @Test
  void testDifferentSnapshotModeProducesDifferentETag() throws Exception {
    Map<String, String> queryParamsRefs =
        Map.of(
            RESTCatalogProperties.SNAPSHOTS_QUERY_PARAMETER,
            RESTCatalogProperties.SnapshotMode.REFS.name().toLowerCase(Locale.ROOT));

    String eTagAll = ETagProvider.of(METADATA_LOCATION, DEFAULT_SNAPSHOT_PARAMS);
    String eTagRefs = ETagProvider.of(METADATA_LOCATION, queryParamsRefs);

    Assertions.assertNotEquals(eTagAll, eTagRefs, "ETag must differ for different snapshot modes");

    Map<String, String> headers = Map.of("If-None-Match", eTagAll);
    LoadTableResponse res =
        adapter.execute(HTTPMethod.GET, TABLE_PATH, headers, queryParamsRefs, null, response);

    Assertions.assertNotNull(
        res, "Response should not be null when snapshot mode query param changes");
    Mockito.verify(response, Mockito.never()).setStatus(HttpServletResponse.SC_NOT_MODIFIED);
    Mockito.verify(response).setHeader("ETag", eTagRefs);
  }

  @Test
  void testCreateTableSetsETagHeader() throws Exception {
    TableBuilder builder = Mockito.mock(TableBuilder.class, Mockito.RETURNS_SELF);
    Mockito.when(catalog.buildTable(Mockito.any(), Mockito.any())).thenReturn(builder);
    Mockito.when(builder.create()).thenReturn(table);

    CreateTableRequest createRequest =
        CreateTableRequest.builder()
            .withName("my_table")
            .withSchema(new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())))
            .build();

    LoadTableResponse res =
        adapter.execute(HTTPMethod.POST, TABLES_PATH, null, createRequest, response);

    Assertions.assertNotNull(res);
    String expectedETag = ETagProvider.of(METADATA_LOCATION, DEFAULT_SNAPSHOT_PARAMS);
    Mockito.verify(response).setHeader("ETag", expectedETag);
  }

  @Test
  void testStageCreateTableDoesNotSetETagHeader() throws Exception {
    CreateTableRequest stageCreateRequest =
        CreateTableRequest.builder()
            .withName("staged_table")
            .withLocation("file:///tmp/staged_table")
            .withSchema(new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())))
            .stageCreate()
            .build();

    LoadTableResponse res =
        adapter.execute(HTTPMethod.POST, TABLES_PATH, null, stageCreateRequest, response);

    Assertions.assertNotNull(res);
    Assertions.assertNull(res.metadataLocation(), "Staged table creation must have null metadata location");
    Mockito.verify(response, Mockito.never()).setHeader(Mockito.eq("ETag"), Mockito.anyString());
  }

  @Test
  void testRegisterTableSetsETagHeader() throws Exception {
    Mockito.when(catalog.registerTable(Mockito.any(), Mockito.any(), Mockito.anyBoolean()))
        .thenReturn(table);
    Mockito.when(catalog.registerTable(Mockito.any(), Mockito.any())).thenReturn(table);

    RegisterTableRequest registerRequest = Mockito.mock(RegisterTableRequest.class);
    Mockito.when(registerRequest.name()).thenReturn("my_table");
    Mockito.when(registerRequest.metadataLocation()).thenReturn(METADATA_LOCATION);

    LoadTableResponse res =
        adapter.execute(HTTPMethod.POST, REGISTER_PATH, null, registerRequest, response);

    Assertions.assertNotNull(res);
    String expectedETag = ETagProvider.of(METADATA_LOCATION, DEFAULT_SNAPSHOT_PARAMS);
    Mockito.verify(response).setHeader("ETag", expectedETag);
  }

  @Test
  void testUpdateTableSetsETagHeader() throws Exception {
    UpdateTableRequest updateRequest = new UpdateTableRequest(List.of(), List.of());

    LoadTableResponse res =
        adapter.execute(HTTPMethod.POST, TABLE_PATH, null, updateRequest, response);

    Assertions.assertNotNull(res);
    String expectedETag = ETagProvider.of(METADATA_LOCATION, DEFAULT_SNAPSHOT_PARAMS);
    Mockito.verify(response).setHeader("ETag", expectedETag);
  }

  @Test
  void testServletServiceReturns304WithEmptyBody() throws Exception {
    HMSCatalogServlet servlet = new HMSCatalogServlet(adapter);
    String matchingETag = ETagProvider.of(METADATA_LOCATION, DEFAULT_SNAPSHOT_PARAMS);

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getMethod()).thenReturn("GET");
    Mockito.when(request.getPathInfo()).thenReturn("/" + TABLE_PATH);
    Mockito.when(request.getParameterMap())
        .thenReturn(
            Map.of(RESTCatalogProperties.SNAPSHOTS_QUERY_PARAMETER, new String[] {"all"}));

    Enumeration<String> headerNames = Collections.enumeration(List.of("If-None-Match"));
    Mockito.when(request.getHeaderNames()).thenReturn(headerNames);
    Mockito.when(request.getHeader("If-None-Match")).thenReturn(matchingETag);

    HttpServletResponse servletResponse = Mockito.mock(HttpServletResponse.class);
    StringWriter out = new StringWriter();
    Mockito.when(servletResponse.getWriter()).thenReturn(new PrintWriter(out));
    ArgumentCaptor<Integer> statusCaptor = ArgumentCaptor.forClass(Integer.class);
    Mockito.doNothing().when(servletResponse).setStatus(statusCaptor.capture());
    Mockito.when(servletResponse.getStatus())
        .thenAnswer(
            inv -> {
              var all = statusCaptor.getAllValues();
              return all.isEmpty() ? HttpServletResponse.SC_OK : all.getLast();
            });

    servlet.service(request, servletResponse);

    Mockito.verify(servletResponse).setStatus(HttpServletResponse.SC_NOT_MODIFIED);
    Mockito.verify(servletResponse).setHeader("ETag", matchingETag);
    Assertions.assertEquals("", out.toString(), "Response body must be empty on 304 Not Modified");
  }

  @Test
  void testServletServiceReturns200WithFullBodyWhenIfNoneMatchDiffers() throws Exception {
    HMSCatalogServlet servlet = new HMSCatalogServlet(adapter);

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getMethod()).thenReturn("GET");
    Mockito.when(request.getPathInfo()).thenReturn("/" + TABLE_PATH);
    Mockito.when(request.getParameterMap())
        .thenReturn(
            Map.of(RESTCatalogProperties.SNAPSHOTS_QUERY_PARAMETER, new String[] {"all"}));

    Enumeration<String> headerNames = Collections.enumeration(List.of("If-None-Match"));
    Mockito.when(request.getHeaderNames()).thenReturn(headerNames);
    Mockito.when(request.getHeader("If-None-Match")).thenReturn("\"stale-etag\"");

    HttpServletResponse servletResponse = Mockito.mock(HttpServletResponse.class);
    StringWriter out = new StringWriter();
    Mockito.when(servletResponse.getWriter()).thenReturn(new PrintWriter(out));

    servlet.service(request, servletResponse);

    Mockito.verify(servletResponse, Mockito.never()).setStatus(HttpServletResponse.SC_NOT_MODIFIED);
    Assertions.assertTrue(
        out.toString().contains("metadata-location"),
        "Response body must contain table metadata JSON on 200 OK");
  }

  @Test
  void testVoidOperationsReturn204NoContent() throws Exception {
    Mockito.when(catalog.tableExists(Mockito.any())).thenReturn(true);
    Mockito.when(catalog.dropTable(Mockito.any())).thenReturn(true);
    Mockito.when(catalog.dropTable(Mockito.any(), Mockito.anyBoolean())).thenReturn(true);

    HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
    StringWriter out = new StringWriter();
    Mockito.when(resp.getWriter()).thenReturn(new PrintWriter(out));

    adapter.execute(HTTPMethod.HEAD, TABLE_PATH, null, null, resp);
    Mockito.verify(resp).setStatus(HttpServletResponse.SC_NO_CONTENT);

    HttpServletResponse respDelete = Mockito.mock(HttpServletResponse.class);
    Mockito.when(respDelete.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
    adapter.execute(HTTPMethod.DELETE, TABLE_PATH, null, null, respDelete);
    Mockito.verify(respDelete).setStatus(HttpServletResponse.SC_NO_CONTENT);
  }

  @Test
  void testServletServiceReturns204ForVoidOperations() throws Exception {
    Mockito.when(catalog.tableExists(Mockito.any())).thenReturn(true);
    HMSCatalogServlet servlet = new HMSCatalogServlet(adapter);

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getMethod()).thenReturn("HEAD");
    Mockito.when(request.getPathInfo()).thenReturn("/" + TABLE_PATH);
    Mockito.when(request.getParameterMap()).thenReturn(Map.of());

    HttpServletResponse servletResponse = Mockito.mock(HttpServletResponse.class);
    StringWriter out = new StringWriter();
    Mockito.when(servletResponse.getWriter()).thenReturn(new PrintWriter(out));
    ArgumentCaptor<Integer> statusCaptor = ArgumentCaptor.forClass(Integer.class);
    Mockito.doNothing().when(servletResponse).setStatus(statusCaptor.capture());
    Mockito.when(servletResponse.getStatus())
        .thenAnswer(
            inv -> {
              var all = statusCaptor.getAllValues();
              return all.isEmpty() ? HttpServletResponse.SC_OK : all.getLast();
            });

    servlet.service(request, servletResponse);

    Mockito.verify(servletResponse).setStatus(HttpServletResponse.SC_NO_CONTENT);
    Assertions.assertEquals("", out.toString(), "Response body must be empty on 204 No Content");
  }
}
