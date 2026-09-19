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
 * limitations under the License.
 */

package org.apache.iceberg.hive.rest.catalog;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.rest.RESTCatalogProperties;

/**
 * Utilities for Iceberg REST catalog server-side scan planning configuration.
 *
 * <p>When a REST catalog server advertises the scan-planning endpoints and
 * {@link RESTCatalogProperties#SCAN_PLANNING_MODE} is set to
 * {@link RESTCatalogProperties.ScanPlanningMode#SERVER}, Iceberg's {@code RESTSessionCatalog} returns a
 * {@code RESTTable} that delegates {@code planTasks()} to the server. Hive's
 * {@code IcebergInputFormat} calls {@code scan.planTasks()} via
 * {@link org.apache.iceberg.mr.hive.HiveTableUtil#resolveTableForScanPlanning}, which reloads the
 * live REST catalog table (instead of a serialized metadata snapshot) when server mode is enabled and
 * {@link HiveConf.ConfVars#HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED} is true.
 * Operators can use this helper or set catalog {@code scan-planning-mode} directly in {@code hive-site.xml}.
 *
 * <p>Tests: {@code TestRestCatalogScanPlanning} and {@code TestRestCatalogScanPlanningServerIT} in
 * {@code iceberg-rest-catalog-client}; {@code TestHiveIcebergServerSideScanPlanning} and
 * {@code TestHiveIcebergServerSideScanPlanningServerIT} in {@code iceberg-handler}.
 *
 * @see <a href="https://iceberg.apache.org/docs/latest/catalog-properties/">REST catalog properties</a>
 */
public final class RestCatalogScanPlanning {

  private RestCatalogScanPlanning() {
  }

  public static String catalogPropertyKey(String catalogName) {
    return IcebergCatalogProperties.catalogPropertyConfigKey(
        catalogName, RESTCatalogProperties.SCAN_PLANNING_MODE);
  }

  public static void setScanPlanningMode(
      Configuration conf, String catalogName, RESTCatalogProperties.ScanPlanningMode mode) {
    conf.set(catalogPropertyKey(catalogName), mode.modeName());
  }

  public static void setScanPlanningMode(Configuration conf, String catalogName, String mode) {
    setScanPlanningMode(conf, catalogName, RESTCatalogProperties.ScanPlanningMode.fromString(mode));
  }

  public static RESTCatalogProperties.ScanPlanningMode getScanPlanningMode(
      Configuration conf, String catalogName) {
    String mode = conf.get(
        catalogPropertyKey(catalogName), RESTCatalogProperties.SCAN_PLANNING_MODE_DEFAULT.modeName());
    return RESTCatalogProperties.ScanPlanningMode.fromString(mode);
  }

  public static boolean isServerMode(Configuration conf, String catalogName) {
    return getScanPlanningMode(conf, catalogName) == RESTCatalogProperties.ScanPlanningMode.SERVER;
  }

  /**
   * Returns true when Hive server-side REST scan planning is enabled in configuration.
   */
  public static boolean isHiveServerSideScanPlanningEnabled(Configuration conf) {
    if (conf == null) {
      return false;
    }
    return HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED);
  }

  /**
   * Returns true when the catalog is configured for server-side scan planning and the Hive feature flag is on.
   */
  public static boolean requestsServerSidePlanning(String catalogName, Configuration conf) {
    if (conf == null || StringUtils.isEmpty(catalogName)) {
      return false;
    }
    return isHiveServerSideScanPlanningEnabled(conf) && isServerMode(conf, catalogName);
  }

  /**
   * Returns true when catalog properties should be copied into the Tez/MR job configuration so
   * executors can reload a live REST catalog table for server-side scan planning.
   */
  public static boolean shouldPropagateCatalogPropertiesToJob(String catalogName, Configuration conf) {
    String resolvedCatalogName = resolveCatalogName(conf, catalogName);
    if (StringUtils.isEmpty(resolvedCatalogName) || conf == null) {
      return false;
    }
    if (!CatalogUtil.ICEBERG_CATALOG_TYPE_REST.equals(
        IcebergCatalogProperties.getCatalogType(conf, resolvedCatalogName))) {
      return false;
    }
    return requestsServerSidePlanning(resolvedCatalogName, conf);
  }

  /**
   * Resolves the catalog name from per-table {@code iceberg.catalog} or the session default catalog.
   */
  public static String resolveCatalogName(Configuration conf, String catalogNameFromTable) {
    if (StringUtils.isNotBlank(catalogNameFromTable)) {
      return catalogNameFromTable;
    }
    if (conf == null) {
      return null;
    }
    return IcebergCatalogProperties.getCatalogName(conf);
  }

  /**
   * Copies {@code iceberg.catalog.<catalog>.*} entries from the HS2 session configuration into Tez/MR
   * job properties so executors can reload a live REST catalog table for server-side scan planning.
   *
   * <p>Session-level {@code SET} commands and {@code hive-site.xml} catalog settings are not
   * automatically present in the job configuration; without this step split generation falls back to
   * the serialized metadata snapshot ({@code DataTableScan}).
   */
  public static void propagateCatalogPropertiesToJob(
      Configuration sessionConf, String catalogName, Map<String, String> jobProperties) {
    if (sessionConf == null || jobProperties == null) {
      return;
    }

    if (!RestCatalogScanPlanning.shouldPropagateCatalogPropertiesToJob(catalogName, sessionConf)) {
      return;
    }

    propagateCatalogProperties(sessionConf, catalogName, (key, value) -> jobProperties.putIfAbsent(key, value));
  }

  /**
   * Copies REST catalog configuration from the HS2 session into a runtime job {@link Configuration}.
   * Called from {@code configureJobConf} so split generation sees catalog URI/type/scan-planning-mode
   * even when job properties were not copied yet.
   */
  public static void propagateCatalogPropertiesToJob(
      Configuration sessionConf, String catalogName, Configuration jobConf) {
    if (sessionConf == null || jobConf == null) {
      return;
    }
    propagateCatalogProperties(sessionConf, catalogName, (key, value) -> {
      if (jobConf.get(key) == null) {
        jobConf.set(key, value);
      }
    });
  }

  private static void propagateCatalogProperties(
      Configuration sessionConf, String catalogName, PropertyConsumer consumer) {
    String resolvedCatalogName = resolveCatalogName(sessionConf, catalogName);
    if (StringUtils.isEmpty(resolvedCatalogName)) {
      return;
    }

    if (!shouldPropagateCatalogPropertiesToJob(resolvedCatalogName, sessionConf)) {
      return;
    }

    consumer.accept(
        HiveConf.ConfVars.HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED.varname,
        String.valueOf(isHiveServerSideScanPlanningEnabled(sessionConf)));

    String sessionDefaultCatalog =
        MetastoreConf.getVar(sessionConf, MetastoreConf.ConfVars.CATALOG_DEFAULT);
    if (StringUtils.isNotBlank(sessionDefaultCatalog)) {
      consumer.accept(MetastoreConf.ConfVars.CATALOG_DEFAULT.getVarname(), sessionDefaultCatalog);
    }

    String catalogPrefix =
        IcebergCatalogProperties.CATALOG_CONFIG_PREFIX + resolvedCatalogName + ".";
    sessionConf.forEach(
        entry -> {
          if (entry.getKey().startsWith(catalogPrefix)) {
            consumer.accept(entry.getKey(), entry.getValue());
          }
        });
  }

  private interface PropertyConsumer {
    void accept(String key, String value);
  }
}
