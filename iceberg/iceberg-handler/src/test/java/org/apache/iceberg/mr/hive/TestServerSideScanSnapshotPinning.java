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
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestServerSideScanSnapshotPinning {

  private static final String CATALOG_NAME = "ice01";
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
  private static final String BRANCH = "b";

  @TempDir
  private Path warehouse;

  @Test
  void pinsScanToSerializedSnapshotWhenCatalogHasAdvanced() throws Exception {
    Table table = new HadoopTables().create(SCHEMA, SPEC, warehouse.toString());
    appendFile(table, "data-a.parquet");
    long pinnedSnapshotId = table.currentSnapshot().snapshotId();

    Configuration conf = buildServerModeConf(table);
    Table serializedTable = HiveTableUtil.deserializeTable(conf, table.name());

    appendFile(table, "data-b.parquet");
    assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(pinnedSnapshotId);

    TableScan scan =
        HiveTableUtil.pinServerSideScanToSerializedTable(conf, serializedTable, table.newScan());
    List<String> plannedPaths = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      tasks.forEach(task -> plannedPaths.add(task.file().location()));
    }
    assertThat(plannedPaths)
        .anyMatch(path -> path.contains("data-a.parquet"))
        .noneMatch(path -> path.contains("data-b.parquet"));
    assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(pinnedSnapshotId);
  }

  @Test
  void pinnedScanAllowsColumnSelection() {
    Table table = new HadoopTables().create(SCHEMA, SPEC, warehouse.toString());
    appendFile(table, "data-a.parquet");
    Configuration conf = buildServerModeConf(table);
    Table serializedTable = HiveTableUtil.deserializeTable(conf, table.name());

    TableScan scan =
        HiveTableUtil.pinServerSideScanToSerializedTable(conf, serializedTable, table.newScan())
            .select("id");
    assertThat(scan.schema().columns()).extracting(Types.NestedField::name).containsExactly("id");
  }

  @Test
  void failsWhenRefMissingFromSerializedTable() {
    Table table = new HadoopTables().create(SCHEMA, SPEC, warehouse.toString());
    appendFile(table, "data-a.parquet");
    Configuration conf = buildServerModeConf(table);
    Table serializedTable = HiveTableUtil.deserializeTable(conf, table.name());
    table.manageSnapshots().createBranch(BRANCH, table.currentSnapshot().snapshotId()).commit();
    TableScan scan = table.newScan();

    assertThatThrownBy(() -> HiveTableUtil.pinServerSideScanRefToSerializedTable(serializedTable, BRANCH, scan))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot find reference " + BRANCH);
  }

  @Test
  void pinsOlderSchemaBranchWithUseRefWhenBranchHasNotMoved() throws Exception {
    Table table = new HadoopTables().create(SCHEMA, SPEC, warehouse.toString());
    appendFile(table, "data-a.parquet");
    long branchSnapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch(BRANCH, branchSnapshotId).commit();

    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    appendFile(table, "data-b.parquet");

    Configuration conf = buildServerModeConf(table);
    Table serializedTable = HiveTableUtil.deserializeTable(conf, table.name());
    assertThat(serializedTable.snapshot(branchSnapshotId).schemaId())
        .isNotEqualTo(serializedTable.schema().schemaId());

    TableScan scan =
        HiveTableUtil.pinServerSideScanRefToSerializedTable(serializedTable, BRANCH, table.newScan());
    assertThat(scan.snapshot().snapshotId()).isEqualTo(branchSnapshotId);
    assertThat(scan.schema().columns())
        .extracting(Types.NestedField::name)
        .containsExactly("id", "extra");

    List<String> plannedPaths = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      tasks.forEach(task -> plannedPaths.add(task.file().location()));
    }
    assertThat(plannedPaths)
        .anyMatch(path -> path.contains("data-a.parquet"))
        .noneMatch(path -> path.contains("data-b.parquet"));
  }

  @Test
  void failsWhenBranchMovedSinceCompilation() {
    Table table = new HadoopTables().create(SCHEMA, SPEC, warehouse.toString());
    appendFile(table, "data-a.parquet");
    long branchSnapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch(BRANCH, branchSnapshotId).commit();

    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    appendFile(table, "data-b.parquet");

    Configuration conf = buildServerModeConf(table);
    Table serializedTable = HiveTableUtil.deserializeTable(conf, table.name());
    SnapshotRef compileTimeRef = serializedTable.refs().get(BRANCH);
    assertThat(compileTimeRef).isNotNull();
    assertThat(compileTimeRef.snapshotId()).isEqualTo(branchSnapshotId);
    assertThat(serializedTable.snapshot(branchSnapshotId).schemaId())
        .isNotEqualTo(serializedTable.schema().schemaId());

    appendFileOnBranch(table, BRANCH, "data-c.parquet");
    assertThat(table.refs().get(BRANCH).snapshotId()).isNotEqualTo(branchSnapshotId);
    TableScan scan = table.newScan();

    assertThatThrownBy(() -> HiveTableUtil.pinServerSideScanRefToSerializedTable(serializedTable, BRANCH, scan))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Branch " + BRANCH + " moved since query compilation");
  }

  @Test
  void pinsTagWithUseSnapshotWhenServerSideTableReloaded() throws Exception {
    Table table = new HadoopTables().create(SCHEMA, SPEC, warehouse.toString());
    appendFile(table, "data-a.parquet");
    long taggedSnapshot = table.currentSnapshot().snapshotId();
    String tag = "t1";
    table.manageSnapshots().createTag(tag, taggedSnapshot).commit();

    Configuration conf = buildServerModeConf(table);
    Table serializedTable = HiveTableUtil.deserializeTable(conf, table.name());

    appendFile(table, "data-b.parquet");
    assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(taggedSnapshot);

    TableScan scan =
        HiveTableUtil.pinServerSideScanRefToSerializedTable(serializedTable, tag, table.newScan());
    List<String> plannedPaths = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      tasks.forEach(task -> plannedPaths.add(task.file().location()));
    }
    assertThat(plannedPaths)
        .anyMatch(path -> path.contains("data-a.parquet"))
        .noneMatch(path -> path.contains("data-b.parquet"));
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

  private void appendFile(Table table, String fileName) {
    table.newAppend()
        .appendFile(
            DataFiles.builder(table.spec())
                .withPath(warehouse + "/" + fileName)
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build())
        .commit();
  }

  private void appendFileOnBranch(Table table, String branch, String fileName) {
    table.newAppend()
        .toBranch(branch)
        .appendFile(
            DataFiles.builder(table.spec())
                .withPath(warehouse + "/" + fileName)
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build())
        .commit();
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
