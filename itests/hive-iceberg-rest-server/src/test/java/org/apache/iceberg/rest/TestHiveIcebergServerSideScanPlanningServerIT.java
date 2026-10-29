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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.plan.TableDesc;
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
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.hive.RestCatalogScanPlanningUtil;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.HiveIcebergStorageHandler;
import org.apache.iceberg.mr.hive.HiveTableUtil;
import org.apache.iceberg.mr.mapreduce.IcebergInputFormat;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
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
import static org.mockito.Mockito.spy;
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
    return spy(
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
   * Negative path: without the HS2 per-table marker and catalog settings in the job conf, split generation
   * plans from the serialized table snapshot and never sends {@code POST /plan}.
   */
  @Test
  void executorJobConfWithoutPropagationUsesSerializedTable() throws IOException {
    Table table = createTableWithData();
    Configuration jobConf = executorJobConf(table);
    jobConf.set(InputFormatConfig.TABLE_LOCATION, table.location());

    assertThat(planRequestsDuringGetSplits(jobConf)).isEmpty();
  }

  /**
   * Positive path: when catalog properties are propagated from HS2 into the executor job conf,
   * split generation reloads a live {@link RESTTable} from the REST catalog instead of using the
   * serialized metadata snapshot.
   */
  @Test
  void propagatedJobConfReloadsRestTableFromCatalog() throws IOException {
    Table table = createTableWithData();
    Configuration jobConf = executorJobConf(table);
    propagateFromHs2(sessionConf(), table, jobConf);

    ArgumentCaptor<HTTPRequest> requestCaptor = ArgumentCaptor.forClass(HTTPRequest.class);
    try (RESTCatalog catalog = loadRestCatalog(jobConf)) {
      Table resolved = catalog.loadTable(TABLE_ID);
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
   * The REST server returns {@code scan-planning-mode=server} in {@code LoadTableResponse} while the
   * client {@code scan-planning-mode} is unset: Iceberg loads a server-planning {@link RESTTable}, and
   * Hive follows it instead of planning locally from manifests.
   */
  @Test
  void serverDecidedPlanningModeIsFollowedWithoutClientProperty() throws IOException {
    createTableWithData();
    Configuration sessionConf = sessionConf();
    sessionConf.unset(
        IcebergCatalogProperties.catalogPropertyConfigKey(CATALOG_NAME, RESTCatalogProperties.SCAN_PLANNING_MODE));

    try (RESTCatalog catalog = loadRestCatalog(sessionConf)) {
      Table table = catalog.loadTable(TABLE_ID);
      assertThat(table).isInstanceOf(RESTTable.class);
      assertThat(planRequestsDuringGetSplits(propagatedJobConf(sessionConf, table))).hasSize(1);
    }
  }

  /** With the Hive kill switch off, a table the server plans is still planned locally by Hive. */
  @Test
  void hiveSwitchOffKeepsLocalPlanningForServerPlannedTable() throws IOException {
    Table table = createTableWithData();
    Configuration sessionConf = sessionConf();
    HiveConf.setBoolVar(sessionConf, HiveConf.ConfVars.HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED, false);

    assertThat(table).isInstanceOf(RESTTable.class);
    assertThat(catalogSecrets(sessionConf)).isEmpty();
    assertThat(planRequestsDuringGetSplits(propagatedJobConf(sessionConf, table))).isEmpty();
  }

  /**
   * For a table Iceberg resolved for server-side planning, every {@code iceberg.catalog.<catalog>.*}
   * entry of the table's catalog goes to job secrets (HIVE-20651), whatever its name, so custom auth
   * properties such as {@code header.Authorization} are never exposed in plain job properties. Other
   * catalogs and unrelated keys are not copied.
   */
  @Test
  void catalogSecretsCopyAllCatalogPropertiesForServerPlannedTable() {
    Table table = createTableWithData();
    Configuration sessionConf = sessionConf();
    String headerKey = IcebergCatalogProperties.catalogPropertyConfigKey(CATALOG_NAME, "header.Authorization");
    sessionConf.set(headerKey, "Bearer token");
    String otherCatalogKey = IcebergCatalogProperties.catalogPropertyConfigKey("other", CatalogProperties.URI);
    sessionConf.set(otherCatalogKey, "http://other:8181");
    sessionConf.set("unrelated.key", "skip");

    assertThat(table).isInstanceOf(RESTTable.class);
    assertThat(catalogSecrets(sessionConf))
        .containsEntry(headerKey, "Bearer token")
        .containsEntry(
            IcebergCatalogProperties.catalogPropertyConfigKey(CATALOG_NAME, CatalogProperties.URI),
            httpServer.getURI().toString())
        .containsEntry(
            IcebergCatalogProperties.catalogPropertyConfigKey(CATALOG_NAME, CatalogUtil.ICEBERG_CATALOG_TYPE),
            CatalogUtil.ICEBERG_CATALOG_TYPE_REST)
        .doesNotContainKey(otherCatalogKey)
        .doesNotContainKey("unrelated.key");
  }

  /**
   * Compile-time snapshot is serialized into the job conf; if the catalog advances before split
   * generation, server-side planning must still plan the serialized snapshot, not {@code HEAD}.
   */
  @Test
  void serverSidePlanningPinsSerializedSnapshotAfterCatalogAdvances() throws IOException {
    Table table = createTableWithData();
    long pinnedSnapshotId = table.currentSnapshot().snapshotId();

    Configuration jobConf = propagatedJobConf(table);

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

  /**
   * Reading {@code t.branch_b1} while another writer commits to {@code b1} between query compilation
   * and split planning: the scan must use the {@code b1} head captured at compile time, not the new
   * head the reloaded REST table sees.
   */
  @Test
  void serverSidePlanningPinsBranchToSerializedTable() throws IOException {
    Table table = createTableWithData();
    long branchHeadId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch("b1", branchHeadId).commit();

    Configuration jobConf = propagatedJobConf(restCatalog.loadTable(TABLE_ID));
    jobConf.set(InputFormatConfig.OUTPUT_TABLE_SNAPSHOT_REF, "branch_b1");

    appendFileB("b1");
    assertThat(restCatalog.loadTable(TABLE_ID).refs().get("b1").snapshotId()).isNotEqualTo(branchHeadId);

    assertThat(planRequestsDuringGetSplits(jobConf))
        .singleElement()
        .extracting(PlanTableScanRequest::snapshotId)
        .isEqualTo(branchHeadId);
  }

  /**
   * Reading {@code t.branch_b1} after a column was added to the table but no new data was written to
   * {@code b1}: Iceberg reads branches with the current table schema, so {@code new_col} must still be
   * selected. The scan therefore uses the ref instead of pinning the snapshot id, which would switch to
   * the older snapshot schema without {@code new_col}.
   */
  @Test
  void serverSidePlanningKeepsTableSchemaForBranchWithOlderSchema() throws IOException {
    Table table = createTableWithData();
    long branchHeadId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch("b1", branchHeadId).commit();
    restCatalog.loadTable(TABLE_ID).updateSchema().addColumn("new_col", Types.StringType.get()).commit();

    Configuration jobConf = propagatedJobConf(restCatalog.loadTable(TABLE_ID));
    jobConf.set(InputFormatConfig.OUTPUT_TABLE_SNAPSHOT_REF, "branch_b1");

    PlanTableScanRequest planRequest = planRequestsDuringGetSplits(jobConf).get(0);
    assertThat(planRequest.snapshotId()).isEqualTo(branchHeadId);
    assertThat(planRequest.useSnapshotSchema()).isFalse();
    assertThat(planRequest.select()).contains("new_col");
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

  private static RESTCatalog loadRestCatalog(Configuration conf) {
    return (RESTCatalog) Catalogs.loadCatalog(conf, CATALOG_NAME).orElseThrow();
  }

  private Configuration propagatedJobConf(Table table) throws IOException {
    return propagatedJobConf(sessionConf(), table);
  }

  private Configuration propagatedJobConf(Configuration sessionConf, Table table) throws IOException {
    Configuration jobConf = executorJobConf(table);
    jobConf.set(InputFormatConfig.TABLE_LOCATION, table.location());
    propagateFromHs2(sessionConf, table, jobConf);
    return jobConf;
  }

  /**
   * Mimics what split generation in the AM sees: the per-table marker from the table's job
   * properties, decided on the table HS2 loaded, and the catalog properties from job secrets; both are
   * copied into the conf by {@code HiveInputFormat#addSplitsForGroup}.
   */
  private void propagateFromHs2(Configuration sessionConf, Table table, Configuration jobConf) {
    Map<String, String> jobProperties = Maps.newHashMap();
    RestCatalogScanPlanningUtil.propagateCatalogPropertiesToJob(
        sessionConf, table, TABLE_ID.toString(), jobProperties);
    jobProperties.forEach(jobConf::set);
    catalogSecrets(sessionConf).forEach(jobConf::set);
  }

  /** Job secrets HS2 builds for the table in {@code HiveIcebergStorageHandler#configureInputJobCredentials}. */
  private static Map<String, String> catalogSecrets(Configuration sessionConf) {
    TableDesc tableDesc = new TableDesc();
    Properties tableProps = new Properties();
    tableProps.setProperty(InputFormatConfig.CATALOG_NAME, CATALOG_NAME);
    tableProps.setProperty(Catalogs.NAME, TABLE_ID.toString());
    tableDesc.setProperties(tableProps);
    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    storageHandler.setConf(sessionConf);
    Map<String, String> jobSecrets = Maps.newHashMap();
    storageHandler.configureInputJobCredentials(tableDesc, jobSecrets);
    return jobSecrets;
  }

  private void appendFileB() {
    appendFileB(SnapshotRef.MAIN_BRANCH);
  }

  private void appendFileB(String branch) {
    DataFile fileB =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-b.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    restCatalog.loadTable(TABLE_ID).newAppend().appendFile(fileB).toBranch(branch).commit();
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

  private Configuration executorJobConf(Table table) {
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
