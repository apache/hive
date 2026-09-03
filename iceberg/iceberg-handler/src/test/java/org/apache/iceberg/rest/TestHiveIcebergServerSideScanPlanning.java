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

import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.hive.rest.catalog.RestCatalogScanPlanning;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.HiveTableUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for Hive server-side REST catalog scan planning via
 * {@link HiveTableUtil#resolveTableForScanPlanning}. Embedded REST server coverage is in
 * {@code TestHiveIcebergServerSideScanPlanningServerIT} in {@code itests/hive-iceberg-rest-server}.
 */
class TestHiveIcebergServerSideScanPlanning {

  private static final String CATALOG_NAME = "ice01";
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @TempDir
  private Path warehouse;

  /**
   * Negative path: do not reload from the REST catalog unless {@code scan-planning-mode=server}.
   * Even with a REST catalog and a serialized table in the job conf, split generation should keep
   * using the snapshot ({@link SerializableTable}) when server mode is off.
   */
  @Test
  void usesDeserializedTableWhenServerModeDisabled() {
    Table table = new HadoopTables().create(SCHEMA, PartitionSpec.unpartitioned(), warehouse.toString());
    Configuration conf = buildTableConf(table, false);

    Table resolved = HiveTableUtil.resolveTableForScanPlanning(conf, table.name());
    assertThat(resolved).isInstanceOf(SerializableTable.class);
  }

  /**
   * Negative path: intra-transaction read-after-write must not reload from the catalog even when
   * server mode is enabled. {@link InputFormatConfig#TABLE_METADATA_LOCATION} points at uncommitted
   * metadata that only exists in the job conf; reloading from the REST catalog would return stale
   * committed state and break same-txn visibility (e.g. INSERT then SELECT).
   */
  @Test
  void usesDeserializedTableForIntraTxnMetadataEvenInServerMode() {
    Table table = new HadoopTables().create(SCHEMA, PartitionSpec.unpartitioned(), warehouse.toString());
    Configuration conf = buildTableConf(table, true);
    conf.set(InputFormatConfig.TABLE_METADATA_LOCATION, warehouse + "/metadata/snapshot.metadata.json");

    Table resolved = HiveTableUtil.resolveTableForScanPlanning(conf, table.name());
    assertThat(resolved).isInstanceOf(BaseTable.class);
  }

  private Configuration buildTableConf(Table table, boolean serverMode) {
    Configuration conf = new Configuration();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, CATALOG_NAME);
    conf.set(InputFormatConfig.TABLE_IDENTIFIER, table.name());
    conf.set(InputFormatConfig.TABLE_LOCATION, warehouse.toString());
    conf.set(InputFormatConfig.CATALOG_NAME, CATALOG_NAME);
    conf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey(CATALOG_NAME, CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    if (serverMode) {
      RestCatalogScanPlanning.setScanPlanningMode(conf, CATALOG_NAME, "server");
      HiveConf.setBoolVar(
          conf, HiveConf.ConfVars.HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED, true);
    }
    conf.set(
        InputFormatConfig.SERIALIZED_TABLE_PREFIX + table.name(),
        HiveTableUtil.serializeTable(table, conf, null, null));
    return conf;
  }
}
