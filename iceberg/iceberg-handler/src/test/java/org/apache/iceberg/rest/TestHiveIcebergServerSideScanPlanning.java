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
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.HiveTableUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the reload guard {@link HiveTableUtil#shouldReloadForServerSideScanPlanning}. The
 * Tez AM reloads from the REST catalog only when HS2 set the per-table marker
 * {@link InputFormatConfig#serverSideScanPlanningKey(String)} and the scan can be planned by the
 * server. End-to-end behavior is in {@code TestHiveIcebergServerSideScanPlanningServerIT}.
 */
class TestHiveIcebergServerSideScanPlanning {

  private static final String CATALOG_NAME = "ice01";
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @TempDir
  private Path warehouse;

  private Table table;

  @BeforeEach
  void before() {
    table = new HadoopTables().create(SCHEMA, PartitionSpec.unpartitioned(), warehouse.toString());
  }

  @Test
  void reloadsWhenHs2SetMarker() {
    Configuration conf = buildTableConf(true);
    assertThat(HiveTableUtil.shouldReloadForServerSideScanPlanning(conf, serialized(table))).isTrue();
  }

  /**
   * The AM does not re-derive the decision from catalog type, {@code scan-planning-mode} or the Hive
   * switch: without the HS2 marker it keeps the serialized table.
   */
  @Test
  void doesNotReloadWithoutMarker() {
    Configuration conf = buildTableConf(false);
    assertThat(HiveTableUtil.shouldReloadForServerSideScanPlanning(conf, serialized(table))).isFalse();
  }

  /**
   * The marker is per table: split generation reuses one job conf for all tables of a vertex, so the
   * marker of one table must not make another table in the same conf reload.
   */
  @Test
  void doesNotReloadOtherTableInSameConf() {
    Configuration conf = buildTableConf(true);
    conf.set(InputFormatConfig.TABLE_IDENTIFIER, "db.other");
    assertThat(HiveTableUtil.shouldReloadForServerSideScanPlanning(conf, serialized(table))).isFalse();
  }

  /**
   * Intra-transaction read-after-write: {@link InputFormatConfig#TABLE_METADATA_LOCATION} points at
   * uncommitted metadata that only exists in the job conf; reloading would lose it.
   */
  @Test
  void doesNotReloadForIntraTxnMetadata() {
    Configuration conf = buildTableConf(true);
    conf.set(InputFormatConfig.TABLE_METADATA_LOCATION, warehouse + "/metadata/snapshot.metadata.json");
    assertThat(HiveTableUtil.shouldReloadForServerSideScanPlanning(conf, serialized(table))).isFalse();
  }

  /** {@code RESTTable} plans incremental append scans locally; reloading would only add drift. */
  @Test
  void doesNotReloadForIncrementalAppendScan() {
    Configuration conf = buildTableConf(true);
    conf.setLong(InputFormatConfig.SNAPSHOT_ID_INTERVAL_FROM, 1L);
    assertThat(HiveTableUtil.shouldReloadForServerSideScanPlanning(conf, serialized(table))).isFalse();
  }

  /** Metadata tables ({@code tbl.files}, {@code tbl.snapshots}, ...) are planned locally. */
  @Test
  void doesNotReloadForMetadataTable() {
    Configuration conf = buildTableConf(true);
    Table filesTable = MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.FILES);
    assertThat(HiveTableUtil.shouldReloadForServerSideScanPlanning(conf, serialized(filesTable))).isFalse();
  }

  private static Table serialized(Table source) {
    return SerializableTable.copyOf(source);
  }

  private Configuration buildTableConf(boolean marker) {
    Configuration conf = new Configuration();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, CATALOG_NAME);
    conf.set(InputFormatConfig.TABLE_IDENTIFIER, table.name());
    conf.set(InputFormatConfig.CATALOG_NAME, CATALOG_NAME);
    conf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey(CATALOG_NAME, CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    if (marker) {
      conf.setBoolean(InputFormatConfig.serverSideScanPlanningKey(table.name()), true);
    }
    return conf;
  }
}
