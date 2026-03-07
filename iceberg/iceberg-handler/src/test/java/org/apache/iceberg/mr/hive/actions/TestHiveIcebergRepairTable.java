/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.mr.hive.actions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.misc.msck.MsckDesc;
import org.apache.hadoop.hive.ql.ddl.misc.msck.MsckResult;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

/**
 * Tests for HiveIcebergRepairTable functionality.
 */
public class TestHiveIcebergRepairTable {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private HadoopCatalog catalog;
  private File warehouseDir;

  private TableIdentifier tableId;
  private Table table;
  private MsckDesc msckDesc;

  @Before
  public void before() throws IOException {
    Configuration conf = new Configuration();
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "data", Types.StringType.get())
    );
    // Create a table
    warehouseDir = temp.newFolder("warehouse");
    catalog = new HadoopCatalog(conf, warehouseDir.getAbsolutePath());

    tableId = TableIdentifier.of("db", "test_table");
    table = catalog.createTable(tableId, schema, PartitionSpec.unpartitioned());

    msckDesc = new MsckDesc(tableId.name(), null,
        new Path(warehouseDir.getAbsolutePath()), true, false, false);
  }

  @After
  public void after() throws Exception {
    if (catalog != null) {
      catalog.dropTable(tableId);
      catalog.close();
    }
  }

  @Test
  public void testRepairWithNoMissingFiles() throws Exception {
    // Add a data file that exists
    File dataFile = temp.newFile("data1.parquet");
    Files.write(dataFile.toPath(), "test data".getBytes());

    DataFile file = newDataFile(dataFile);

    table.newAppend()
        .appendFile(file)
        .commit();
    countFiles(table);
    // Run repair - should find no missing files
    HiveIcebergRepairTable repair = new HiveIcebergRepairTable(table, msckDesc);
    MsckResult result = repair.execute();

    assertEquals("No files should be removed when all files exist", 0, result.numIssues());

    // Verify the file is still there
    long fileCountAfter = countFiles(table);
    assertEquals("File should still be in metadata", 1, fileCountAfter);
  }

  @Test
  public void testRepairWithMissingFiles() throws Exception {
    // Add two data files - one that exists and one that doesn't
    File existingFile = temp.newFile("existing.parquet");
    Files.write(existingFile.toPath(), "test data".getBytes());

    File missingFile = new File(temp.getRoot(), "missing.parquet");
    // Don't create this file - it will be missing

    DataFile existingDataFile = newDataFile(existingFile);
    DataFile missingDataFile = newDataFile(missingFile);

    table.newAppend()
        .appendFile(existingDataFile)
        .appendFile(missingDataFile)
        .commit();

    // Verify we have 2 files before repair
    long fileCountBefore = countFiles(table);
    assertEquals("Should have 2 files before repair", 2, fileCountBefore);

    // Run repair - should remove the missing file reference
    HiveIcebergRepairTable repair = new HiveIcebergRepairTable(table, msckDesc);
    MsckResult result = repair.execute();

    assertEquals("One file reference should be removed", 1, result.numIssues());

    // Verify only one file remains after repair
    long fileCountAfter = countFiles(table);
    assertEquals("Should have 1 file after repair", 1, fileCountAfter);
  }

  @Test
  public void testRepairWithMultipleMissingFiles() throws Exception {
    // Add multiple missing files
    for (int i = 0; i < 3; i++) {
      File missingFile = new File(temp.getRoot(), "missing_" + i + ".parquet");

      DataFile missingDataFile = newDataFile(missingFile);

      table.newAppend()
          .appendFile(missingDataFile)
          .commit();
    }

    // Run repair
    HiveIcebergRepairTable repair = new HiveIcebergRepairTable(table, msckDesc);
    MsckResult result = repair.execute();

    assertEquals("Three file references should be removed", 3, result.numIssues());
  }

  @Test
  public void testRepairDryRunMode() throws Exception {
    // Add a missing file
    File missingFile = new File(temp.getRoot(), "missing.parquet");
    DataFile missingDataFile = newDataFile(missingFile);

    table.newAppend()
        .appendFile(missingDataFile)
        .commit();

    long fileCountBefore = countFiles(table);
    assertEquals("Should have 1 file before repair", 1, fileCountBefore);

    MsckDesc dryRun = new MsckDesc(tableId.name(), null,
        new Path(warehouseDir.getAbsolutePath()), false, false, false);
    // Run repair in dry-run mode
    HiveIcebergRepairTable repair = new HiveIcebergRepairTable(table, dryRun);
    MsckResult result = repair.execute();

    // Should identify the missing file
    assertEquals("Should identify 1 missing file", 1, result.numIssues());

    // But file should still be in metadata (dry-run doesn't commit)
    table.refresh();
    long fileCountAfter = countFiles(table);
    assertEquals("File should still be in metadata after dry-run", 1, fileCountAfter);
  }

  private static DataFile newDataFile(File file) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(file.getAbsolutePath())
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .withFormat(FileFormat.PARQUET)
        .build();
  }

  /**
   * Count the number of files in the table, properly closing resources.
   */
  private static long countFiles(Table table) {
    return Long.parseLong(table.currentSnapshot().summary()
        .get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
  }
}
