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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.Table;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.rest.RESTCatalogProperties;

/**
 * Utilities for Iceberg REST catalog server-side scan planning configuration.
 *
 * <p>When a REST catalog server advertises the scan-planning endpoints and
 * {@link RESTCatalogProperties#SCAN_PLANNING_MODE} is set to
 * {@link RESTCatalogProperties.ScanPlanningMode#SERVER}, Iceberg's {@code RESTSessionCatalog} returns a
 * {@code RESTTable} that delegates {@code planTasks()} to the server. Hive's
 * {@code IcebergInputFormat} calls {@code scan.planTasks()} via
 * {@code IcebergInputFormat#getSplits}, which reloads a live REST catalog table for {@code POST /plan}
 * (pinned to the serialized compile-time snapshot). The catalog decides the scan planning mode;
 * {@link HiveConf.ConfVars#HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED} only controls whether
 * Hive is allowed to follow it.
 *
 * <p>Tests: {@code TestRestCatalogScanPlanningUtil} in {@code iceberg-handler};
 * {@code TestHiveIcebergServerSideScanPlanning} in {@code iceberg-handler}; embedded REST server
 * tests {@code TestRestCatalogScanPlanningServerIT} and {@code TestHiveIcebergServerSideScanPlanningServerIT}
 * in {@code itests/hive-iceberg-rest-server}.
 *
 * <p>{@link #setScanPlanningMode(Configuration, String, String)},
 * {@link #getScanPlanningMode(Configuration, String)} and
 * {@link #propagateCatalogPropertiesToJob(Configuration, Table, String, Map)} are public so tests in other
 * Maven modules (for example {@code itests/hive-iceberg-rest-server}) can configure scan planning; those modules
 * compile against this artifact as a JAR and cannot call package-private members. They are not
 * intended as a general operator or application API.
 *
 * @see <a href="https://iceberg.apache.org/docs/latest/catalog-properties/">REST catalog properties</a>
 */
public final class RestCatalogScanPlanningUtil {

  private static final String REST_TABLE_CLASS_NAME = "org.apache.iceberg.rest.RESTTable";

  private RestCatalogScanPlanningUtil() {
  }

  public static void setScanPlanningMode(Configuration conf, String catalogName, String mode) {
    conf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey(
            catalogName, RESTCatalogProperties.SCAN_PLANNING_MODE),
        RESTCatalogProperties.ScanPlanningMode.fromString(mode).modeName());
  }

  public static RESTCatalogProperties.ScanPlanningMode getScanPlanningMode(
      Configuration conf, String catalogName) {
    String mode = conf.get(
        IcebergCatalogProperties.catalogPropertyConfigKey(
            catalogName, RESTCatalogProperties.SCAN_PLANNING_MODE),
        RESTCatalogProperties.SCAN_PLANNING_MODE_DEFAULT.modeName());
    return RESTCatalogProperties.ScanPlanningMode.fromString(mode);
  }

  /**
   * Returns true when Hive is allowed to use server-side scan planning (the Hive-side kill switch).
   */
  static boolean isServerSideScanPlanningEnabled(Configuration conf) {
    return conf != null &&
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED);
  }

  /**
   * Returns true when Iceberg resolved the loaded table for server-side scan planning. Iceberg decides
   * this per table, letting the mode in the server's {@code LoadTableResponse} take precedence over the
   * client {@code scan-planning-mode}, and then returns a package-private {@code RESTTable}.
   */
  static boolean usesServerSideScanPlanning(Table table) {
    return table != null && REST_TABLE_CLASS_NAME.equals(table.getClass().getName());
  }

  /**
   * Returns true when split generation should reload the live REST table for server-side scan planning:
   * the Hive kill switch is on and Iceberg resolved the loaded table for server-side planning. Both the
   * per-table marker and the catalog job secrets are gated on this.
   */
  static boolean shouldPlanServerSide(Configuration sessionConf, Table table) {
    return isServerSideScanPlanningEnabled(sessionConf) && usesServerSideScanPlanning(table);
  }

  /**
   * Writes the per-table {@link InputFormatConfig#serverSideScanPlanningKey(String)} marker and the
   * session default catalog into the table's job properties when {@link #shouldPlanServerSide} holds.
   * The {@code iceberg.catalog.<catalog>.*} entries go through job secrets instead, see
   * {@link #propagateCatalogSecretsToJob(String, Map, Configuration)}.
   * Session values replace existing entries so a session {@code SET} wins.
   */
  public static void propagateCatalogPropertiesToJob(
      Configuration sessionConf, Table table, String tableIdentifier, Map<String, String> jobProperties) {
    if (jobProperties == null || !shouldPlanServerSide(sessionConf, table)) {
      return;
    }
    jobProperties.put(InputFormatConfig.serverSideScanPlanningKey(tableIdentifier), Boolean.TRUE.toString());

    String sessionDefaultCatalog = MetastoreConf.getVar(sessionConf, MetastoreConf.ConfVars.CATALOG_DEFAULT);
    if (StringUtils.isNotBlank(sessionDefaultCatalog)) {
      jobProperties.put(MetastoreConf.ConfVars.CATALOG_DEFAULT.getVarname(), sessionDefaultCatalog);
    }
  }

  /**
   * Copies all {@code iceberg.catalog.<catalog>.*} entries from the HS2 session into job secrets
   * (HIVE-20651), so catalog credentials never land in plain job properties. Only split generation in
   * the AM needs them to load the REST catalog, so the caller ships them only when
   * {@link #shouldPlanServerSide} holds for the loaded table, the same decision as the per-table marker;
   * {@code HiveInputFormat#addSplitsForGroup} copies job secrets into the job conf before {@code getSplits}.
   */
  static void propagateCatalogSecretsToJob(
      String catalogName, Map<String, String> jobSecrets, Configuration sessionConf) {
    if (StringUtils.isEmpty(catalogName)) {
      return;
    }
    String catalogPrefix = IcebergCatalogProperties.CATALOG_CONFIG_PREFIX + catalogName + ".";
    sessionConf.forEach(entry -> {
      if (entry.getKey().startsWith(catalogPrefix)) {
        jobSecrets.put(entry.getKey(), entry.getValue());
      }
    });
  }
}
