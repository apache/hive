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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Scan;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.mr.CloseableRestCatalog;
import org.apache.iceberg.mr.hive.RestCatalogScanPlanningUtil;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.HiveTableUtil;
import org.apache.iceberg.mr.mapreduce.IcebergInputFormat;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.apache.iceberg.TestBase.FILE_A;
import static org.apache.iceberg.TestBase.SCHEMA;
import static org.apache.iceberg.TestBase.SPEC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

/**
 * Integration tests for Hive server-side REST catalog scan planning via
 * {@code IcebergInputFormat#getSplits}: with catalog settings propagated from HS2 into the executor
 * job conf, split generation reloads a live {@link RESTTable} from the embedded REST server (then
 * closes the catalog) instead of planning only from the serialized snapshot ({@link SerializableTable}).
 *
 * <p>Without propagation, or when reload guards apply, executors keep the snapshot; those cases are
 * unit-tested in {@code TestHiveIcebergServerSideScanPlanning} ({@code iceberg-handler}). Iceberg
 * REST client {@code POST /plan} behavior is in {@code TestRestCatalogScanPlanningServerIT}.
 */
class TestHiveIcebergServerSideScanPlanningServerIT extends TestBaseWithRESTServer {

  private static final String CATALOG_NAME = "hive-iceberg-scan-planning";
  private static final TableIdentifier TABLE_ID = TableIdentifier.of(NS, "file_planning_table");

  @Override
  protected RESTCatalogAdapter createAdapterForServer() {
    return Mockito.spy(
        new RESTCatalogAdapter(backendCatalog) {
          @Override
          protected <T extends RESTResponse> T execute(
              HTTPRequest request,
              Class<T> responseType,
              Consumer<ErrorResponse> errorHandler,
              Consumer<Map<String, String>> responseHeaders) {
            Object body = roundTripSerialize(request.body(), "request");
            HTTPRequest req = ImmutableHTTPRequest.builder().from(request).body(body).build();
            T response = super.execute(req, responseType, errorHandler, responseHeaders);

            if (response instanceof LoadTableResponse) {
              return RESTCatalogAdapter.castResponse(
                  responseType,
                  withPlanningMode(
                      (LoadTableResponse) response,
                      RESTCatalogProperties.ScanPlanningMode.SERVER.modeName()));
            }

            if (req.body() instanceof PlanTableScanRequest) {
              return response;
            }

            return roundTripSerialize(response, "response");
          }
        });
  }

  @Override
  protected String catalogName() {
    return CATALOG_NAME;
  }

  @Override
  protected Map<String, String> additionalCatalogProperties() {
    Configuration conf = new Configuration();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, CATALOG_NAME);
    RestCatalogScanPlanningUtil.setScanPlanningMode(conf, CATALOG_NAME, "server");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED, true);
    return IcebergCatalogProperties.getCatalogProperties(conf, CATALOG_NAME);
  }

  @BeforeEach
  @Override
  public void before() throws Exception {
    super.before();
    adapterForRESTServer.setPlanningBehavior(
        new RESTCatalogAdapter.PlanningBehavior() {
          @Override
          public boolean shouldPlanTableScanAsync(Scan<?, FileScanTask, ?> scan) {
            return false;
          }

          @Override
          public int numberFileScanTasksPerPlanTask() {
            return 100;
          }
        });
  }

  /**
   * Negative path: executor job conf without propagated catalog settings must not reload from the
   * REST catalog, even when server mode is enabled on HS2. Without catalog URI/type in the job
   * conf, split generation falls back to the serialized table snapshot.
   */
  @Test
  void executorJobConfWithoutPropagationUsesSerializedTable() throws IOException {
    Table table = createTableWithData();
    Configuration jobConf = executorJobConf(table);

    Table resolved = HiveTableUtil.resolveTableForScanPlanning(jobConf, TABLE_ID.toString());
    assertThat(resolved).isInstanceOf(SerializableTable.class);
    assertThat(resolved.newScan()).isNotInstanceOf(RESTTableScan.class);
  }

  /**
   * Positive path: when catalog properties are propagated from HS2 into the executor job conf,
   * split generation reloads a live {@link RESTTable} from the REST catalog instead of using the
   * serialized metadata snapshot.
   */
  @Test
  void propagatedJobConfReloadsRestTableFromCatalog() throws IOException {
    Table table = createTableWithData();
    Configuration sessionConf = sessionConf();
    Configuration jobConf = executorJobConf(table);
    RestCatalogScanPlanningUtil.propagateCatalogPropertiesToJob(sessionConf, CATALOG_NAME, jobConf);

    ArgumentCaptor<HTTPRequest> requestCaptor = ArgumentCaptor.forClass(HTTPRequest.class);
    try (CloseableRestCatalog catalog = CloseableRestCatalog.load(jobConf, CATALOG_NAME)) {
      Table resolved = catalog.loadTable(TABLE_ID.toString());
      assertThat(resolved).isInstanceOf(RESTTable.class);
      assertThat(resolved.newScan()).isInstanceOf(RESTTableScan.class);
    }

    verify(adapterForRESTServer, atLeastOnce())
        .execute(requestCaptor.capture(), any(), any(), any(), any());
    assertThat(
            requestCaptor.getAllValues().stream()
                .anyMatch(req -> req.path().contains("/tables/")))
        .as("Expected Hive split planning to reload the table from the REST catalog")
        .isTrue();
  }

  /**
   * Compile-time snapshot is serialized into the job conf; if the catalog advances before split
   * generation, server-side planning must still plan the serialized snapshot, not {@code HEAD}.
   */
  @Test
  void serverSidePlanningPinsSerializedSnapshotAfterCatalogAdvances() throws IOException {
    Table table = createTableWithData();
    long pinnedSnapshotId = table.currentSnapshot().snapshotId();

    Configuration jobConf = executorJobConf(table);
    jobConf.set(InputFormatConfig.TABLE_LOCATION, table.location());
    RestCatalogScanPlanningUtil.propagateCatalogPropertiesToJob(sessionConf(), CATALOG_NAME, jobConf);

    DataFile fileB =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-b.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    restCatalog.loadTable(TABLE_ID).newAppend().appendFile(fileB).commit();
    assertThat(restCatalog.loadTable(TABLE_ID).currentSnapshot().snapshotId())
        .isNotEqualTo(pinnedSnapshotId);

    assertThat(planRequestsDuringGetSplits(jobConf))
        .singleElement()
        .extracting(PlanTableScanRequest::snapshotId)
        .isEqualTo(pinnedSnapshotId);
  }

  /** Time travel by snapshot id is sent to the REST server as the plan request snapshot. */
  @Test
  void serverSidePlanningUsesExplicitSnapshotId() throws IOException {
    Table table = createTableWithData();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    appendFileB();

    Configuration jobConf = propagatedJobConf(restCatalog.loadTable(TABLE_ID));
    jobConf.setLong(InputFormatConfig.SNAPSHOT_ID, firstSnapshotId);

    assertThat(planRequestsDuringGetSplits(jobConf))
        .singleElement()
        .extracting(PlanTableScanRequest::snapshotId)
        .isEqualTo(firstSnapshotId);
  }

  /**
   * Time travel by tag name resolves the ref against the compile-time table, so moving the ref in
   * the catalog after compilation does not change what is planned.
   */
  @Test
  void serverSidePlanningResolvesRefAgainstSerializedTable() throws IOException {
    Table table = createTableWithData();
    long taggedSnapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag("v1", taggedSnapshotId).commit();

    Configuration jobConf = propagatedJobConf(restCatalog.loadTable(TABLE_ID));
    jobConf.set(InputFormatConfig.SNAPSHOT_ID, "v1");

    appendFileB();
    Table live = restCatalog.loadTable(TABLE_ID);
    live.manageSnapshots().replaceTag("v1", live.currentSnapshot().snapshotId()).commit();

    assertThat(planRequestsDuringGetSplits(jobConf))
        .singleElement()
        .extracting(PlanTableScanRequest::snapshotId)
        .isEqualTo(taggedSnapshotId);
  }

  /** Incremental append scans are planned locally from the serialized table, without POST /plan. */
  @Test
  void incrementalAppendScanDoesNotUseServerSidePlanning() throws IOException {
    Table table = createTableWithData();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    appendFileB();

    Configuration jobConf = propagatedJobConf(restCatalog.loadTable(TABLE_ID));
    jobConf.setLong(InputFormatConfig.SNAPSHOT_ID_INTERVAL_FROM, firstSnapshotId);

    assertThat(planRequestsDuringGetSplits(jobConf)).isEmpty();
  }

  private Configuration propagatedJobConf(Table table) throws IOException {
    Configuration jobConf = executorJobConf(table);
    jobConf.set(InputFormatConfig.TABLE_LOCATION, table.location());
    RestCatalogScanPlanningUtil.propagateCatalogPropertiesToJob(sessionConf(), CATALOG_NAME, jobConf);
    return jobConf;
  }

  private void appendFileB() {
    DataFile fileB =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-b.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    restCatalog.loadTable(TABLE_ID).newAppend().appendFile(fileB).commit();
  }

  private List<PlanTableScanRequest> planRequestsDuringGetSplits(Configuration jobConf) {
    Mockito.clearInvocations(adapterForRESTServer);
    JobContext jobContext = new JobContextImpl(jobConf, new JobID("server_side_scan", 1));
    new IcebergInputFormat<>().getSplits(jobContext);

    ArgumentCaptor<HTTPRequest> requestCaptor = ArgumentCaptor.forClass(HTTPRequest.class);
    verify(adapterForRESTServer, Mockito.atLeast(0))
        .execute(requestCaptor.capture(), any(), any(), any(), any());
    return requestCaptor.getAllValues().stream()
        .map(HTTPRequest::body)
        .filter(PlanTableScanRequest.class::isInstance)
        .map(PlanTableScanRequest.class::cast)
        .toList();
  }

  private Table createTableWithData() {
    restCatalog.createNamespace(NS);
    Table table =
        restCatalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();
    table.newAppend().appendFile(FILE_A).commit();
    return table;
  }

  private Configuration sessionConf() {
    Configuration sessionConf = new Configuration();
    MetastoreConf.setVar(sessionConf, MetastoreConf.ConfVars.CATALOG_DEFAULT, CATALOG_NAME);
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey(CATALOG_NAME, CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    restCatalog.properties().forEach(
        (key, value) ->
            sessionConf.set(
                IcebergCatalogProperties.catalogPropertyConfigKey(CATALOG_NAME, key), value));
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey(CATALOG_NAME, CatalogProperties.URI),
        httpServer.getURI().toString());
    RestCatalogScanPlanningUtil.setScanPlanningMode(sessionConf, CATALOG_NAME, "server");
    HiveConf.setBoolVar(sessionConf, HiveConf.ConfVars.HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED, true);
    return sessionConf;
  }

  private Configuration executorJobConf(Table table) throws IOException {
    Configuration jobConf = new Configuration();
    jobConf.set(InputFormatConfig.TABLE_IDENTIFIER, TABLE_ID.toString());
    jobConf.set(InputFormatConfig.CATALOG_NAME, CATALOG_NAME);
    jobConf.set(
        InputFormatConfig.SERIALIZED_TABLE_PREFIX + TABLE_ID.toString(),
        HiveTableUtil.serializeTable(table, jobConf, null, null));
    return jobConf;
  }

  private static LoadTableResponse withPlanningMode(LoadTableResponse response, String mode) {
    return LoadTableResponse.builder()
        .withTableMetadata(response.tableMetadata())
        .addAllConfig(response.config())
        .addConfig(RESTCatalogProperties.SCAN_PLANNING_MODE, mode)
        .addAllCredentials(response.credentials())
        .build();
  }
}
