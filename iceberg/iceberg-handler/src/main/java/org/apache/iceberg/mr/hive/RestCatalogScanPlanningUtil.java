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
import java.util.function.BiConsumer;
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
 * {@link HiveTableUtil#resolveTableForScanPlanning}, which reloads the live REST catalog table
 * (instead of a serialized metadata snapshot) when server mode is enabled and
 * {@link HiveConf.ConfVars#HIVE_ICEBERG_REST_SCAN_PLANNING_MODE} is {@code server}.
 * Operators can use this helper or set catalog {@code scan-planning-mode} directly in {@code hive-site.xml}.
 *
 * <p>Tests: {@code TestRestCatalogScanPlanningUtil} in {@code iceberg-handler};
 * {@code TestHiveIcebergServerSideScanPlanning} in {@code iceberg-handler}; embedded REST server
 * tests {@code TestRestCatalogScanPlanningServerIT} and {@code TestHiveIcebergServerSideScanPlanningServerIT}
 * in {@code itests/hive-iceberg-rest-server}.
 *
 * <p>{@link #setCatalogMode(Configuration, String, String)},
 * {@link #isCatalogServerMode(Configuration, String)}, and
 * {@link #setHiveMode(Configuration, String)} are public so tests in other Maven modules
 * (for example {@code itests/hive-iceberg-rest-server}) can configure scan planning; those modules
 * compile against this artifact as a JAR and cannot call package-private members. They are not
 * intended as a general operator or application API.
 *
 * @see <a href="https://iceberg.apache.org/docs/latest/catalog-properties/">REST catalog properties</a>
 */
public final class RestCatalogScanPlanningUtil {

  private RestCatalogScanPlanningUtil() {
  }

  private static void setCatalogMode(
      Configuration conf, String catalogName, RESTCatalogProperties.ScanPlanningMode mode) {
    conf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey(
            catalogName, RESTCatalogProperties.SCAN_PLANNING_MODE),
        mode.modeName());
  }

  public static void setCatalogMode(Configuration conf, String catalogName, String mode) {
    setCatalogMode(conf, catalogName, RESTCatalogProperties.ScanPlanningMode.fromString(mode));
  }

  static RESTCatalogProperties.ScanPlanningMode getCatalogMode(
      Configuration conf, String catalogName) {
    String mode = conf.get(
        IcebergCatalogProperties.catalogPropertyConfigKey(
            catalogName, RESTCatalogProperties.SCAN_PLANNING_MODE),
        RESTCatalogProperties.SCAN_PLANNING_MODE_DEFAULT.modeName());
    return RESTCatalogProperties.ScanPlanningMode.fromString(mode);
  }

  public static boolean isCatalogServerMode(Configuration conf, String catalogName) {
    return getCatalogMode(conf, catalogName) == RESTCatalogProperties.ScanPlanningMode.SERVER;
  }

  /**
   * Returns true when Hive server-side REST scan planning is enabled in configuration.
   */
  static boolean isHiveServerMode(Configuration conf) {
    if (conf == null) {
      return false;
    }
    return RESTCatalogProperties.ScanPlanningMode.SERVER ==
        RESTCatalogProperties.ScanPlanningMode.fromString(getHiveMode(conf));
  }

  static String getHiveMode(Configuration conf) {
    if (conf == null) {
      return RESTCatalogProperties.SCAN_PLANNING_MODE_DEFAULT.modeName();
    }
    return HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_ICEBERG_REST_SCAN_PLANNING_MODE);
  }

  public static void setHiveMode(Configuration conf, String mode) {
    HiveConf.setVar(
        conf,
        HiveConf.ConfVars.HIVE_ICEBERG_REST_SCAN_PLANNING_MODE,
        RESTCatalogProperties.ScanPlanningMode.fromString(mode).modeName());
  }

  /**
   * Returns true when the catalog is configured for server-side scan planning and the Hive feature flag is on.
   */
  static boolean isServerSidePlanningEnabled(String catalogName, Configuration conf) {
    if (conf == null || StringUtils.isEmpty(catalogName)) {
      return false;
    }
    return isHiveServerMode(conf) && isCatalogServerMode(conf, catalogName);
  }

  /**
   * Returns true when catalog properties should be copied into the Tez job configuration so
   * executors can reload a live REST catalog table for server-side scan planning.
   */
  static boolean shouldPropagateCatalogPropertiesToJob(String catalogName, Configuration conf) {
    String resolvedCatalogName = HiveTableUtil.resolveCatalogName(conf, catalogName);
    if (StringUtils.isEmpty(resolvedCatalogName) || conf == null) {
      return false;
    }
    if (!CatalogUtil.ICEBERG_CATALOG_TYPE_REST.equals(
        IcebergCatalogProperties.getCatalogType(conf, resolvedCatalogName))) {
      return false;
    }
    return isServerSidePlanningEnabled(resolvedCatalogName, conf);
  }

  /**
   * Copies {@code iceberg.catalog.<catalog>.*} entries from the HS2 session configuration into Tez
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
    propagateCatalogPropertiesToJob(sessionConf, catalogName, jobProperties::putIfAbsent);
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
    propagateCatalogPropertiesToJob(
        sessionConf,
        catalogName,
        (key, value) -> {
          if (jobConf.get(key) == null) {
            jobConf.set(key, value);
          }
        });
  }

  private static void propagateCatalogPropertiesToJob(
      Configuration sessionConf, String catalogName, BiConsumer<String, String> jobPropertySink) {
    if (!shouldPropagateCatalogPropertiesToJob(catalogName, sessionConf)) {
      return;
    }

    String resolvedCatalogName = HiveTableUtil.resolveCatalogName(sessionConf, catalogName);
    if (StringUtils.isEmpty(resolvedCatalogName)) {
      return;
    }

    jobPropertySink.accept(
        HiveConf.ConfVars.HIVE_ICEBERG_REST_SCAN_PLANNING_MODE.varname,
        getHiveMode(sessionConf));

    String sessionDefaultCatalog =
        MetastoreConf.getVar(sessionConf, MetastoreConf.ConfVars.CATALOG_DEFAULT);
    if (StringUtils.isNotBlank(sessionDefaultCatalog)) {
      jobPropertySink.accept(MetastoreConf.ConfVars.CATALOG_DEFAULT.getVarname(), sessionDefaultCatalog);
    }

    String catalogPrefix =
        IcebergCatalogProperties.CATALOG_CONFIG_PREFIX + resolvedCatalogName + ".";
    sessionConf.forEach(
        entry -> {
          if (entry.getKey().startsWith(catalogPrefix)) {
            jobPropertySink.accept(entry.getKey(), entry.getValue());
          }
        });
  }
}
