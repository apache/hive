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

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.hive.rest.catalog.RestCatalogScanPlanning;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RestCatalogScanPlanning}: Hive configuration keys, server-mode detection,
 * and copying REST catalog settings from the HS2 session into Tez/MR job configuration. End-to-end
 * {@code POST /plan} behavior is in {@link TestRestCatalogScanPlanningServerIT}.
 */
public class TestRestCatalogScanPlanning {

  /**
   * {@link RestCatalogScanPlanning#catalogPropertyKey} must use the standard
   * {@code iceberg.catalog.<name>.*} prefix so {@code scan-planning-mode} is read/written consistently
   * with other Iceberg catalog properties in {@code hive-site.xml} and session {@code SET}.
   */
  @Test
  void catalogPropertyKeyUsesIcebergPropertyName() {
    assertThat(RestCatalogScanPlanning.catalogPropertyKey("ice01"))
        .isEqualTo(
            IcebergCatalogProperties.catalogPropertyConfigKey(
                "ice01", RESTCatalogProperties.SCAN_PLANNING_MODE));
  }

  /**
   * {@code scan-planning-mode=server} alone is not enough; {@link HiveConf.ConfVars
   * #HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED} must also be true.
   */
  @Test
  void requestsServerSidePlanningFromConfiguration() {
    Configuration conf = new Configuration();
    assertThat(RestCatalogScanPlanning.requestsServerSidePlanning("ice01", conf)).isFalse();

    RestCatalogScanPlanning.setScanPlanningMode(conf, "ice01", "server");
    assertThat(RestCatalogScanPlanning.requestsServerSidePlanning("ice01", conf)).isFalse();
    assertThat(RestCatalogScanPlanning.isServerMode(conf, "ice01")).isTrue();

    enableHiveServerSideScanPlanning(conf);
    assertThat(RestCatalogScanPlanning.requestsServerSidePlanning("ice01", conf)).isTrue();
    assertThat(RestCatalogScanPlanning.getScanPlanningMode(conf, "ice01").modeName())
        .isEqualTo("server");
  }

  /**
   * Job propagation uses the per-table catalog name when present; otherwise the session default
   * catalog from {@link MetastoreConf.ConfVars#CATALOG_DEFAULT}.
   */
  @Test
  void resolveCatalogNameUsesSessionDefaultWhenTablePropertyMissing() {
    Configuration conf = new Configuration();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, "ice01");
    assertThat(RestCatalogScanPlanning.resolveCatalogName(conf, null)).isEqualTo("ice01");
    assertThat(RestCatalogScanPlanning.resolveCatalogName(conf, "ice02")).isEqualTo("ice02");
  }

  /**
   * Catalog properties are copied to the job only for REST catalogs with server scan planning;
   * Hive (metastore) catalogs and REST catalogs in local planning mode are skipped.
   */
  @Test
  void shouldPropagateCatalogPropertiesOnlyForRestCatalogInServerMode() {
    Configuration conf = new Configuration();
    conf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    assertThat(RestCatalogScanPlanning.shouldPropagateCatalogPropertiesToJob("ice01", conf)).isFalse();

    RestCatalogScanPlanning.setScanPlanningMode(conf, "ice01", "server");
    assertThat(RestCatalogScanPlanning.shouldPropagateCatalogPropertiesToJob("ice01", conf)).isFalse();
    enableHiveServerSideScanPlanning(conf);
    assertThat(RestCatalogScanPlanning.shouldPropagateCatalogPropertiesToJob("ice01", conf)).isTrue();

    Configuration hiveConf = new Configuration();
    hiveConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE);
    RestCatalogScanPlanning.setScanPlanningMode(hiveConf, "ice01", "server");
    enableHiveServerSideScanPlanning(hiveConf);
    assertThat(RestCatalogScanPlanning.shouldPropagateCatalogPropertiesToJob("ice01", hiveConf))
        .isFalse();
  }

  /**
   * When propagation is enabled, all {@code iceberg.catalog.<catalog>.*} entries from the HS2 session
   * (type, URI, scan-planning mode, etc.) are copied into Tez/MR job properties; unrelated conf keys
   * are not.
   */
  @Test
  void propagateCatalogPropertiesToJobCopiesRestCatalogSettings() {
    Configuration sessionConf = new Configuration();
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri"), "http://localhost:8181");
    RestCatalogScanPlanning.setScanPlanningMode(sessionConf, "ice01", "server");
    enableHiveServerSideScanPlanning(sessionConf);
    sessionConf.set("unrelated.key", "skip");

    Map<String, String> jobProperties = Maps.newHashMap();
    RestCatalogScanPlanning.propagateCatalogPropertiesToJob(sessionConf, "ice01", jobProperties);

    assertThat(jobProperties)
        .containsEntry(
            IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
            CatalogUtil.ICEBERG_CATALOG_TYPE_REST)
        .containsEntry(
            IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri"), "http://localhost:8181")
        .containsEntry(RestCatalogScanPlanning.catalogPropertyKey("ice01"), "server")
        .containsEntry(
            HiveConf.ConfVars.HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED.varname, "true")
        .doesNotContainKey("unrelated.key");
  }

  /**
   * Same as {@link #propagateCatalogPropertiesToJobCopiesRestCatalogSettings()} for the
   * {@link Configuration} overload used by {@code HiveIcebergStorageHandler#configureJobConf}.
   */
  @Test
  void propagateCatalogPropertiesToJobConfigurationCopiesRestCatalogSettings() {
    Configuration sessionConf = new Configuration();
    MetastoreConf.setVar(sessionConf, MetastoreConf.ConfVars.CATALOG_DEFAULT, "ice01");
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri"), "http://localhost:8181");
    RestCatalogScanPlanning.setScanPlanningMode(sessionConf, "ice01", "server");
    enableHiveServerSideScanPlanning(sessionConf);

    Configuration jobConf = new Configuration();
    RestCatalogScanPlanning.propagateCatalogPropertiesToJob(sessionConf, null, jobConf);

    assertThat(jobConf.get(MetastoreConf.ConfVars.CATALOG_DEFAULT.getVarname())).isEqualTo("ice01");
    assertThat(jobConf.get(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE)))
        .isEqualTo(CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    assertThat(jobConf.get(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri")))
        .isEqualTo("http://localhost:8181");
    assertThat(jobConf.get(RestCatalogScanPlanning.catalogPropertyKey("ice01"))).isEqualTo("server");
  }

  /**
   * Negative path: REST catalog settings are not copied when {@code scan-planning-mode} is not
   * {@code server}, so executors keep using the serialized table snapshot for split generation.
   */
  @Test
  void propagateCatalogPropertiesToJobSkipsWhenServerModeDisabled() {
    Configuration sessionConf = new Configuration();
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri"), "http://localhost:8181");

    Map<String, String> jobProperties = Maps.newHashMap();
    RestCatalogScanPlanning.propagateCatalogPropertiesToJob(sessionConf, "ice01", jobProperties);

    assertThat(jobProperties).isEmpty();
  }

  private static void enableHiveServerSideScanPlanning(Configuration conf) {
    HiveConf.setBoolVar(
        conf, HiveConf.ConfVars.HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED, true);
  }
}
