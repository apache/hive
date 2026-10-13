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

import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class TestServerSideScanSnapshotPinning {

  private static final String CATALOG_NAME = "ice01";
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  @TempDir
  private Path warehouse;

  @Test
  void pinsScanToSerializedSnapshotWhenCatalogHasAdvanced() throws Exception {
    Table table = new HadoopTables().create(SCHEMA, SPEC, warehouse.toString());
    DataFile fileA =
        DataFiles.builder(SPEC)
            .withPath(warehouse + "/data-a.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(fileA).commit();
    long pinnedSnapshotId = table.currentSnapshot().snapshotId();

    Configuration conf = buildServerModeConf(table);
    Table serializedTable = HiveTableUtil.deserializeTable(conf, table.name());

    DataFile fileB =
        DataFiles.builder(SPEC)
            .withPath(warehouse + "/data-b.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(fileB).commit();
    assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(pinnedSnapshotId);

    TableScan scan =
        HiveTableUtil.pinServerSideScanToSerializedTable(conf, serializedTable, table.newScan());
    List<String> plannedPaths = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      tasks.forEach(task -> plannedPaths.add(task.file().location()));
    }
    assertThat(plannedPaths).anyMatch(path -> path.contains("data-a.parquet"));
    assertThat(plannedPaths).noneMatch(path -> path.contains("data-b.parquet"));
    assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(pinnedSnapshotId);
  }

  @Test
  void pinnedScanAllowsColumnSelection() {
    Table table = new HadoopTables().create(SCHEMA, SPEC, warehouse.toString());
    table.newAppend()
        .appendFile(
            DataFiles.builder(SPEC)
                .withPath(warehouse + "/data-a.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build())
        .commit();
    Configuration conf = buildServerModeConf(table);
    Table serializedTable = HiveTableUtil.deserializeTable(conf, table.name());

    TableScan scan =
        HiveTableUtil.pinServerSideScanToSerializedTable(conf, serializedTable, table.newScan())
            .select("id");
    assertThat(scan.schema().columns()).extracting(Types.NestedField::name).containsExactly("id");
  }

  @Test
  void doesNotPinWhenExplicitSnapshotVersionIsSet() {
    Table table = new HadoopTables().create(SCHEMA, SPEC, warehouse.toString());
    Configuration conf = buildServerModeConf(table);
    conf.setLong(InputFormatConfig.SNAPSHOT_ID, 42L);
    Table serializedTable = HiveTableUtil.deserializeTable(conf, table.name());

    TableScan scan = table.newScan();
    TableScan pinned =
        HiveTableUtil.pinServerSideScanToSerializedTable(conf, serializedTable, scan);
    assertThat(pinned).isSameAs(scan);
  }

  private Configuration buildServerModeConf(Table table) {
    Configuration conf = new Configuration();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, CATALOG_NAME);
    conf.set(InputFormatConfig.TABLE_IDENTIFIER, table.name());
    conf.set(InputFormatConfig.CATALOG_NAME, CATALOG_NAME);
    conf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey(CATALOG_NAME, CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    RestCatalogScanPlanningUtil.setScanPlanningMode(conf, CATALOG_NAME, "server");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED, true);
    conf.set(
        InputFormatConfig.SERIALIZED_TABLE_PREFIX + table.name(),
        HiveTableUtil.serializeTable(table, conf, null, null));
    return conf;
  }
}
