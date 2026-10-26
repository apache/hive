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

package org.apache.iceberg.mr.hive;

import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RestCatalogScanPlanningUtil}: Hive configuration keys, server-mode detection,
 * and copying REST catalog settings from the HS2 session into Tez/MR job configuration. End-to-end
 * {@code POST /plan} behavior is in {@code TestRestCatalogScanPlanningServerIT}
 * ({@code itests/hive-iceberg-rest-server}).
 */
public class TestRestCatalogScanPlanningUtil {

  /**
   * {@link RestCatalogScanPlanningUtil#setScanPlanningMode} must use the standard
   * {@code iceberg.catalog.<name>.*} prefix so {@code scan-planning-mode} is read/written consistently
   * with other Iceberg catalog properties in {@code hive-site.xml} and session {@code SET}.
   */
  @Test
  void setScanPlanningModeUsesCatalogPropertyConfigKey() {
    Configuration conf = new Configuration();
    RestCatalogScanPlanningUtil.setScanPlanningMode(conf, "ice01", "server");
    assertThat(conf.get(
            IcebergCatalogProperties.catalogPropertyConfigKey(
                "ice01", RESTCatalogProperties.SCAN_PLANNING_MODE)))
        .isEqualTo("server");
  }

  /** The Hive kill switch is off by default. */
  @Test
  void serverSideScanPlanningIsDisabledByDefault() {
    Configuration conf = new Configuration();
    assertThat(RestCatalogScanPlanningUtil.isServerSideScanPlanningEnabled(conf)).isFalse();
    enableHiveServerMode(conf);
    assertThat(RestCatalogScanPlanningUtil.isServerSideScanPlanningEnabled(conf)).isTrue();
  }

  /**
   * Job propagation uses the per-table catalog name when present; otherwise the session default
   * catalog from {@link MetastoreConf.ConfVars#CATALOG_DEFAULT}.
   */
  @Test
  void resolveCatalogNameUsesSessionDefaultWhenTablePropertyMissing() {
    Configuration conf = new Configuration();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, "ice01");
    assertThat(HiveTableUtil.resolveCatalogName(conf, null)).isEqualTo("ice01");
    assertThat(HiveTableUtil.resolveCatalogName(conf, "ice02")).isEqualTo("ice02");
  }

  /**
   * Server-side planning needs both the Hive kill switch and a table Iceberg resolved for server-side
   * planning; a table that is not a {@code RESTTable} never qualifies, even in client {@code server} mode.
   */
  @Test
  void shouldPlanServerSideRequiresServerPlanningTable() {
    Table table = Mockito.mock(Table.class);
    assertThat(RestCatalogScanPlanningUtil.shouldPlanServerSide(restSessionConf(), table)).isFalse();
    assertThat(RestCatalogScanPlanningUtil.shouldPlanServerSide(restSessionConf(), null)).isFalse();
  }

  /**
   * The marker follows the table Iceberg loaded, not the client {@code scan-planning-mode}: a table
   * that is not a server-planning {@code RESTTable} gets no marker even in client {@code server} mode.
   * The positive case needs a REST server and is in {@code TestHiveIcebergServerSideScanPlanningServerIT}.
   */
  @Test
  void propagateCatalogPropertiesToJobSkipsTableNotPlannedServerSide() {
    Map<String, String> jobProperties = Maps.newHashMap();
    RestCatalogScanPlanningUtil.propagateCatalogPropertiesToJob(
        restSessionConf(), Mockito.mock(Table.class), "db.tbl", jobProperties);
    assertThat(jobProperties).isEmpty();
  }

  /**
   * Catalog secrets follow the same decision as the marker: a REST catalog table that Iceberg resolved
   * to client-side planning gets no {@code iceberg.catalog.<catalog>.*} secrets, even with the Hive kill
   * switch on and client {@code server} mode. The positive case needs a REST server and is in
   * {@code TestHiveIcebergServerSideScanPlanningServerIT}.
   */
  @Test
  void propagateCatalogSecretsToJobSkipsTableNotPlannedServerSide() {
    Map<String, String> jobSecrets = Maps.newHashMap();
    RestCatalogScanPlanningUtil.propagateCatalogSecretsToJob(
        Mockito.mock(Table.class), tableDesc("ice01"), jobSecrets, restSessionConf());
    assertThat(jobSecrets).isEmpty();
  }

  private static TableDesc tableDesc(String catalogName) {
    TableDesc tableDesc = new TableDesc();
    Properties tableProps = new Properties();
    tableProps.setProperty(InputFormatConfig.CATALOG_NAME, catalogName);
    tableDesc.setProperties(tableProps);
    return tableDesc;
  }

  private static Configuration restSessionConf() {
    Configuration sessionConf = new Configuration(false);
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri"), "http://localhost:8181");
    RestCatalogScanPlanningUtil.setScanPlanningMode(sessionConf, "ice01", "server");
    enableHiveServerMode(sessionConf);
    return sessionConf;
  }

  private static void enableHiveServerMode(Configuration conf) {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED, true);
  }
}
