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
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.hive.rest.catalog.RestCatalogAccessDelegation;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.junit.jupiter.api.Test;

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

  /**
   * Catalog {@code scan-planning-mode=server} alone is not enough; the Hive kill switch
   * {@link HiveConf.ConfVars#HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED} must also be on.
   */
  @Test
  void shouldUseServerSideScanPlanningRequiresCatalogModeAndHiveSwitch() {
    Configuration conf = new Configuration();
    assertThat(RestCatalogScanPlanningUtil.getScanPlanningMode(conf, "ice01"))
        .isEqualTo(RESTCatalogProperties.SCAN_PLANNING_MODE_DEFAULT);
    assertThat(RestCatalogScanPlanningUtil.shouldUseServerSideScanPlanning(conf, "ice01")).isFalse();

    RestCatalogScanPlanningUtil.setScanPlanningMode(conf, "ice01", "server");
    assertThat(RestCatalogScanPlanningUtil.getScanPlanningMode(conf, "ice01"))
        .isEqualTo(RESTCatalogProperties.ScanPlanningMode.SERVER);
    assertThat(RestCatalogScanPlanningUtil.isServerSideScanPlanningEnabled(conf)).isFalse();
    assertThat(RestCatalogScanPlanningUtil.shouldUseServerSideScanPlanning(conf, "ice01")).isFalse();

    enableHiveServerMode(conf);
    assertThat(RestCatalogScanPlanningUtil.isServerSideScanPlanningEnabled(conf)).isTrue();
    assertThat(RestCatalogScanPlanningUtil.shouldUseServerSideScanPlanning(conf, "ice01")).isTrue();
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
   * Catalog properties are copied to the job only for REST catalogs with server scan planning;
   * Hive (metastore) catalogs and REST catalogs in local planning mode are skipped.
   */
  @Test
  void shouldPropagateCatalogPropertiesOnlyForRestCatalogInServerMode() {
    Configuration conf = new Configuration();
    conf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    assertThat(RestCatalogScanPlanningUtil.shouldPropagateCatalogPropertiesToJob("ice01", conf)).isFalse();

    RestCatalogScanPlanningUtil.setScanPlanningMode(conf, "ice01", "server");
    assertThat(RestCatalogScanPlanningUtil.shouldPropagateCatalogPropertiesToJob("ice01", conf)).isFalse();
    enableHiveServerMode(conf);
    assertThat(RestCatalogScanPlanningUtil.shouldPropagateCatalogPropertiesToJob("ice01", conf)).isTrue();

    Configuration hiveConf = new Configuration();
    hiveConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE);
    RestCatalogScanPlanningUtil.setScanPlanningMode(hiveConf, "ice01", "server");
    enableHiveServerMode(hiveConf);
    assertThat(RestCatalogScanPlanningUtil.shouldPropagateCatalogPropertiesToJob("ice01", hiveConf))
        .isFalse();
  }

  /**
   * When propagation is enabled, non-secret {@code iceberg.catalog.<catalog>.*} entries from the HS2
   * session are copied into Tez/MR job properties; unrelated conf keys are not.
   */
  @Test
  void propagateCatalogPropertiesToJobCopiesRestCatalogSettings() {
    Configuration sessionConf = new Configuration();
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri"), "http://localhost:8181");
    RestCatalogScanPlanningUtil.setScanPlanningMode(sessionConf, "ice01", "server");
    enableHiveServerMode(sessionConf);
    sessionConf.set("unrelated.key", "skip");

    Map<String, String> jobProperties = Maps.newHashMap();
    RestCatalogScanPlanningUtil.propagateCatalogPropertiesToJob(sessionConf, "ice01", jobProperties);

    assertThat(jobProperties)
        .containsEntry(
            IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
            CatalogUtil.ICEBERG_CATALOG_TYPE_REST)
        .containsEntry(
            IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri"), "http://localhost:8181")
        .containsEntry(
            IcebergCatalogProperties.catalogPropertyConfigKey(
                "ice01", RESTCatalogProperties.SCAN_PLANNING_MODE),
            "server")
        .containsEntry(InputFormatConfig.serverSideScanPlanningKey("ice01"), "true")
        .doesNotContainKey(HiveConf.ConfVars.HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED.varname)
        .doesNotContainKey("unrelated.key");
  }

  /**
   * The HS2 session is authoritative: a session {@code SET} replaces an older value already present
   * in job properties or the job conf.
   */
  @Test
  void propagateCatalogPropertiesToJobOverwritesExistingValues() {
    Configuration sessionConf = new Configuration();
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    String uriKey = IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri");
    sessionConf.set(uriKey, "http://new:8181");
    RestCatalogScanPlanningUtil.setScanPlanningMode(sessionConf, "ice01", "server");
    enableHiveServerMode(sessionConf);

    Map<String, String> jobProperties = Maps.newHashMap();
    jobProperties.put(uriKey, "http://old:8181");
    RestCatalogScanPlanningUtil.propagateCatalogPropertiesToJob(sessionConf, "ice01", jobProperties);
    assertThat(jobProperties).containsEntry(uriKey, "http://new:8181");

    Configuration jobConf = new Configuration(false);
    jobConf.set(uriKey, "http://old:8181");
    RestCatalogScanPlanningUtil.propagateCatalogPropertiesToJob(sessionConf, "ice01", jobConf);
    assertThat(jobConf.get(uriKey)).isEqualTo("http://new:8181");
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
    RestCatalogScanPlanningUtil.setScanPlanningMode(sessionConf, "ice01", "server");
    enableHiveServerMode(sessionConf);

    Configuration jobConf = new Configuration();
    RestCatalogScanPlanningUtil.propagateCatalogPropertiesToJob(sessionConf, null, jobConf);

    assertThat(jobConf.get(MetastoreConf.ConfVars.CATALOG_DEFAULT.getVarname())).isEqualTo("ice01");
    assertThat(jobConf.get(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE)))
        .isEqualTo(CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    assertThat(jobConf.get(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri")))
        .isEqualTo("http://localhost:8181");
    assertThat(jobConf.get(
            IcebergCatalogProperties.catalogPropertyConfigKey(
                "ice01", RESTCatalogProperties.SCAN_PLANNING_MODE)))
        .isEqualTo("server");
    assertThat(jobConf.getBoolean(InputFormatConfig.serverSideScanPlanningKey("ice01"), false)).isTrue();
  }

  /**
   * Negative path: REST catalog settings are not copied when {@code scan-planning-mode} is not
   * {@code server}, so executors keep using the serialized table snapshot for split generation.
   */
  /**
   * OAuth / credential catalog settings go to job secrets ({@code configureInputJobCredentials}),
   * not plain job properties — same channel as vended storage credentials (HIVE-20651).
   */
  @Test
  void propagateInputJobCredentialsRoutesCredentialAndOAuthToJobSecrets() {
    Configuration sessionConf = new Configuration();
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri"), "http://localhost:8181");
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "oauth2-client-secret"),
        "oauth-secret");
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "credential"),
        "catalog-credential");
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey(
            "ice01", RestCatalogAccessDelegation.ACCESS_DELEGATION_PROPERTY),
        "vended-credentials");
    RestCatalogScanPlanningUtil.setScanPlanningMode(sessionConf, "ice01", "server");
    enableHiveServerMode(sessionConf);

    TableDesc tableDesc = new TableDesc();
    Properties tableProps = new Properties();
    tableProps.setProperty(InputFormatConfig.CATALOG_NAME, "ice01");
    tableDesc.setProperties(tableProps);

    Map<String, String> jobProperties = Maps.newHashMap();
    Map<String, String> jobSecrets = Maps.newHashMap();
    RestCatalogScanPlanningUtil.propagateCatalogPropertiesToJob(sessionConf, "ice01", jobProperties);
    RestCatalogScanPlanningUtil.propagateInputJobCredentials(tableDesc, jobSecrets, sessionConf);

    assertThat(jobProperties)
        .containsEntry(
            IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri"), "http://localhost:8181")
        .containsEntry(
            IcebergCatalogProperties.catalogPropertyConfigKey(
                "ice01", RestCatalogAccessDelegation.ACCESS_DELEGATION_PROPERTY),
            "vended-credentials")
        .doesNotContainKey(
            IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "oauth2-client-secret"))
        .doesNotContainKey(IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "credential"));
    assertThat(jobSecrets)
        .containsEntry(
            IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "oauth2-client-secret"),
            "oauth-secret")
        .containsEntry(
            IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "credential"),
            "catalog-credential")
        .doesNotContainKey(
            IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri"));
  }

  @Test
  void propagateCatalogPropertiesToJobSkipsWhenServerModeDisabled() {
    Configuration sessionConf = new Configuration();
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri"), "http://localhost:8181");

    Map<String, String> jobProperties = Maps.newHashMap();
    RestCatalogScanPlanningUtil.propagateCatalogPropertiesToJob(sessionConf, "ice01", jobProperties);

    assertThat(jobProperties).isEmpty();
  }

  private static void enableHiveServerMode(Configuration conf) {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED, true);
  }
}
