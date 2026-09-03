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
import java.util.Map;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.hive.rest.catalog.RestCatalogScanPlanning;
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
 * Embedded REST server tests for {@link RestCatalogScanPlanning}: when Hive-style configuration sets
 * {@code scan-planning-mode=server}, {@link RESTTable} / {@link RESTTableScan} delegate split
 * planning to the catalog server ({@code POST /plan}).
 */
class TestRestCatalogScanPlanningServerIT extends TestBaseWithRESTServer {

  private static final String CATALOG_NAME = "hive-rest-scan-planning";

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
    RestCatalogScanPlanning.setScanPlanningMode(conf, CATALOG_NAME, "server");
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
   * Positive path: REST catalog loaded with server planning mode returns a {@link RESTTable}; calling
   * {@code planTasks()} on a scan issues a {@link PlanTableScanRequest} to the embedded REST server.
   */
  @Test
  void hiveCatalogConfigurationIssuesPlanTableScanRequest() throws IOException {
    Configuration conf = new Configuration();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, CATALOG_NAME);
    RestCatalogScanPlanning.setScanPlanningMode(conf, CATALOG_NAME, "server");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED, true);
    assertThat(IcebergCatalogProperties.getCatalogProperties(conf, CATALOG_NAME))
        .containsEntry(RESTCatalogProperties.SCAN_PLANNING_MODE, "server");
    assertThat(RestCatalogScanPlanning.isServerMode(conf, CATALOG_NAME)).isTrue();

    restCatalog.createNamespace(NS);
    Table table =
        restCatalog.buildTable(TableIdentifier.of(NS, "scan_planning_table"), SCHEMA)
            .withPartitionSpec(SPEC)
            .create();
    table.newAppend().appendFile(FILE_A).commit();

    parserContext =
        ParserContext.builder()
            .add("specsById", table.specs())
            .add("caseSensitive", false)
            .build();

    assertThat(table).isInstanceOf(RESTTable.class);
    assertThat(table.newScan()).isInstanceOf(RESTTableScan.class);

    ArgumentCaptor<HTTPRequest> requestCaptor = ArgumentCaptor.forClass(HTTPRequest.class);
    assertThat(table.newScan().planTasks()).isNotEmpty();

    verify(adapterForRESTServer, atLeastOnce())
        .execute(requestCaptor.capture(), any(), any(), any(), any());
    assertThat(
            requestCaptor.getAllValues().stream()
                .anyMatch(req -> req.body() instanceof PlanTableScanRequest))
        .as("Expected server-side scan planning via POST /plan (PlanTableScanRequest)")
        .isTrue();
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
