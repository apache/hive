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
import java.util.Set;
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
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestHMSCatalogAdapterSnapshotMode {

  private static final String TABLE_PATH = "v1/namespaces/accounting/tables/my_table";

  private HMSCatalogAdapter adapter;
  private HttpServletResponse response;
  private StringWriter stringWriter;

  @BeforeEach
  void setup() throws Exception {
    Catalog catalog =
        Mockito.mock(
            Catalog.class,
            Mockito.withSettings().extraInterfaces(SupportsNamespaces.class, ViewCatalog.class));

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    Snapshot snap1 = Mockito.mock(Snapshot.class);
    Mockito.when(snap1.snapshotId()).thenReturn(1L);
    Mockito.when(snap1.sequenceNumber()).thenReturn(1L);
    Mockito.when(snap1.timestampMillis()).thenReturn(1000L);

    Snapshot snap2 = Mockito.mock(Snapshot.class);
    Mockito.when(snap2.snapshotId()).thenReturn(2L);
    Mockito.when(snap2.sequenceNumber()).thenReturn(2L);
    Mockito.when(snap2.timestampMillis()).thenReturn(2000L);

    TableMetadata initialMetadata =
        TableMetadata.buildFromEmpty(2)
            .setLocation("file:///tmp/table")
            .addSchema(schema)
            .setDefaultPartitionSpec(PartitionSpec.unpartitioned())
            .setDefaultSortOrder(SortOrder.unsorted())
            .addSnapshot(snap1)
            .addSnapshot(snap2)
            .setBranchSnapshot(2L, SnapshotRef.MAIN_BRANCH)
            .discardChanges()
            .withMetadataLocation("file:///tmp/table/metadata.json")
            .build();

    BaseTable table = Mockito.mock(BaseTable.class);
    TableOperations operations = Mockito.mock(TableOperations.class);
    Mockito.when(table.operations()).thenReturn(operations);
    Mockito.when(operations.current()).thenReturn(initialMetadata);
    Mockito.when(catalog.loadTable(Mockito.any(TableIdentifier.class))).thenReturn(table);

    adapter = new HMSCatalogAdapter("test_catalog", catalog, null, null, Collections.emptyList());

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

  private LoadTableResponse executeLoadTable(String snapshotMode) throws Exception {
    Map<String, String> queryParams =
        snapshotMode != null
            ? ImmutableMap.of(RESTCatalogProperties.SNAPSHOTS_QUERY_PARAMETER, snapshotMode)
            : null;
    return adapter.execute(HTTPMethod.GET, TABLE_PATH, Set.of(), queryParams, null, response);
  }

  @Test
  void testDefaultSnapshotMode() throws Exception {
    // When snapshots query parameter is omitted, mode defaults to SnapshotMode.ALL
    LoadTableResponse res = executeLoadTable(null);

    Assertions.assertNotNull(res, "Response should not be null");
    Assertions.assertNotNull(res.tableMetadata(), "TableMetadata should not be null");
    Assertions.assertEquals(
        2,
        res.tableMetadata().snapshots().size(),
        "Default mode (ALL) should preserve all snapshots including historical ones");
    Assertions.assertNotNull(res.tableMetadata().snapshot(1L), "Snapshot 1 should be retained");
    Assertions.assertNotNull(res.tableMetadata().snapshot(2L), "Snapshot 2 should be retained");
  }

  @Test
  void testSnapshotModeAll() throws Exception {
    LoadTableResponse res = executeLoadTable("all");

    Assertions.assertNotNull(res, "Response should not be null");
    Assertions.assertNotNull(res.tableMetadata(), "TableMetadata should not be null");
    Assertions.assertEquals(
        2, res.tableMetadata().snapshots().size(), "Mode ALL should return all snapshots");
    Assertions.assertNotNull(
        res.tableMetadata().snapshot(1L), "Historical snapshot 1 should be present");
    Assertions.assertNotNull(
        res.tableMetadata().snapshot(2L), "Branch snapshot 2 should be present");
  }

  @Test
  void testSnapshotModeRefs() throws Exception {
    LoadTableResponse res = executeLoadTable("refs");

    Assertions.assertNotNull(res, "Response should not be null");
    Assertions.assertNotNull(res.tableMetadata(), "TableMetadata should not be null");
    Assertions.assertEquals(
        1,
        res.tableMetadata().snapshots().size(),
        "Mode REFS should suppress historical snapshots");
    Assertions.assertNull(
        res.tableMetadata().snapshot(1L),
        "Historical snapshot 1 should be suppressed in REFS mode");
    Assertions.assertNotNull(
        res.tableMetadata().snapshot(2L), "Branch snapshot 2 should be retained in REFS mode");
  }

  @Test
  void testInvalidSnapshotMode() throws Exception {
    LoadTableResponse res = executeLoadTable("invalid");

    Assertions.assertNull(res, "Response should be null for an invalid snapshot mode");
    Mockito.verify(response).setStatus(400);
    Assertions.assertTrue(
        stringWriter.toString().contains("IllegalArgumentException"),
        "Error response should report IllegalArgumentException");
    Assertions.assertTrue(
        stringWriter.toString().contains("INVALID"),
        "Error response message should mention the invalid mode");
  }
}
