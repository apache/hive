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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergStorageHandlerTestUtils;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

/**
 * Verifies that Iceberg tables honor the {@code CLUSTERED BY} clause by checking HMS metadata
 * persistence, data readability and physical file distribution across buckets.
 */
public class TestHiveIcebergClusteredBy extends HiveIcebergStorageHandlerWithEngineBase {

  private static final String TABLE_NAME = "ice_clus_data_files";
  private static final TableIdentifier TABLE_ID = TableIdentifier.of("default", TABLE_NAME);
  private static final int NUM_BUCKETS = 3;

  private static final Map<Integer, Long> EXPECTED_BUCKET_RECORD_COUNTS = buildExpectedBuckets();

  private static Map<Integer, Long> buildExpectedBuckets() {
    Map<Integer, Long> bucketCounts = new LinkedHashMap<>();
    bucketCounts.put(0, 4L);  // customer_id=2
    bucketCounts.put(1, 3L);  // customer_id=3
    bucketCounts.put(2, 2L);  // customer_id=1
    return Collections.unmodifiableMap(bucketCounts);
  }

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}")
  public static Collection<Object[]> parameters() {
    return HiveIcebergStorageHandlerWithEngineBase.getParameters(p ->
        p.fileFormat() == FileFormat.PARQUET &&
        p.testTableType() == TestTableType.HIVE_CATALOG &&
        p.isVectorized());
  }

  private void setUpIcebergTable() {
    shell.executeStatement(
        "CREATE TABLE " + TABLE_NAME +
        " (customer_id BIGINT, first_name STRING, last_name STRING) " +
        "CLUSTERED BY (customer_id) INTO " + NUM_BUCKETS + " BUCKETS " +
        "STORED BY ICEBERG STORED AS PARQUET " +
        "TBLPROPERTIES ('format-version'='" + formatVersion + "')");

    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, TABLE_ID, false));
  }

  @Test
  public void testHmsHasBucketMetadata() throws TException, InterruptedException {
    setUpIcebergTable();
    validateHmsBucketMetadata(TABLE_NAME);
  }

  @Test
  public void testTotalRowCountAfterClusteredInsert() {
    setUpIcebergTable();
    List<Object[]> result = shell.executeStatement("SELECT COUNT(*) FROM " + TABLE_NAME);
    long count = ((Number) result.getFirst()[0]).longValue();
    Assert.assertEquals(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2.size(), count);
  }

  @Test
  public void testIcebergDataFilesAfterClusteredInsert() throws IOException {
    setUpIcebergTable();
    org.apache.iceberg.Table icebergTable = testTables.loadTable(TABLE_ID);
    Map<Integer, Long> actualBucketRecordCounts = extractBucketRecordCounts(icebergTable);
    Assert.assertEquals(EXPECTED_BUCKET_RECORD_COUNTS, actualBucketRecordCounts);
  }

  /**
   * Tests that CLUSTERED BY metadata and bucketing behavior are preserved when migrating
   * a native Hive table to Iceberg using ALTER TABLE CONVERT TO ICEBERG.
   */
  @Test
  public void testNativeToIcebergMigrationWithClusteredBy() throws TException, InterruptedException, IOException {
    String migrationTableName = "migration_clustered_table";
    TableIdentifier migrationTableId = TableIdentifier.of("default", migrationTableName);

    // Create a Native Hive table with CLUSTERED BY
    shell.executeStatement(
        "CREATE EXTERNAL TABLE " + migrationTableName +
        " (customer_id BIGINT, first_name STRING, last_name STRING) " +
        "CLUSTERED BY (customer_id) INTO " + NUM_BUCKETS + " BUCKETS " +
        "STORED AS ORC");

    // Insert initial records into the Native Hive table
    List<org.apache.iceberg.data.Record> initialRecords =
            HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2.subList(0, 4);
    shell.executeStatement(testTables.getInsertQuery(initialRecords, migrationTableId, false));

    validateHmsBucketMetadata(migrationTableName);

    List<Object[]> initialCount = shell.executeStatement("SELECT COUNT(*) FROM " + migrationTableName);
    Assert.assertEquals(4L, ((Number) initialCount.getFirst()[0]).longValue());

    // Convert the Native Hive table to Iceberg table
    shell.executeStatement("ALTER TABLE " + migrationTableName + " CONVERT TO ICEBERG");

    validateHmsBucketMetadata(migrationTableName);
    Table hmsTable = shell.metastore().getTable("default", migrationTableName);
    Assert.assertEquals("org.apache.iceberg.mr.hive.HiveIcebergStorageHandler",
        hmsTable.getParameters().get("storage_handler"));

    // Insert remaining records into the Iceberg table
    List<org.apache.iceberg.data.Record> remainingRecords =
            HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2.subList(4, 9);
    shell.executeStatement(testTables.getInsertQuery(remainingRecords, migrationTableId, false));

    // Validate Iceberg table metadata and bucketing behavior
    List<Object[]> finalCount = shell.executeStatement("SELECT COUNT(*) FROM " + migrationTableName);
    Assert.assertEquals(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2.size(),
        ((Number) finalCount.getFirst()[0]).longValue());

    org.apache.iceberg.Table icebergTable = testTables.loadTable(migrationTableId);
    Map<Integer, Long> bucketCounts = extractBucketRecordCounts(icebergTable);
    Assert.assertEquals(EXPECTED_BUCKET_RECORD_COUNTS, bucketCounts);

    shell.executeStatement("DROP TABLE IF EXISTS " + migrationTableName);
  }

  private void validateHmsBucketMetadata(String tableName) throws TException, InterruptedException {
    Table hmsTable = shell.metastore().getTable("default", tableName);
    Assert.assertEquals(NUM_BUCKETS, hmsTable.getSd().getNumBuckets());
    Assert.assertEquals(Collections.singletonList("customer_id"), hmsTable.getSd().getBucketCols());
  }

  /**
   * Extracts bucket ID from file names and counts records per bucket.
   * Handles both Iceberg file names ({bucketId}-{attemptId}-...) and native Hive file names ({bucketId}_0).
   */
  private Map<Integer, Long> extractBucketRecordCounts(org.apache.iceberg.Table icebergTable) throws IOException {
    Map<Integer, Long> bucketRecordCounts = new LinkedHashMap<>();
    try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        String path = task.file().location();
        String filename = path.substring(path.lastIndexOf('/') + 1);

        int bucketId;
        if (!filename.contains("-")) {
          // Native Hive: 000000_0 → bucket 0
          bucketId = Integer.parseInt(filename.split("_")[0]);
        } else {
          // Iceberg: 00000-0-... → bucket 0
          bucketId = Integer.parseInt(filename.split("-")[0]);
        }

        // Sum records across multiple files in the same bucket (e.g., multiple inserts)
        bucketRecordCounts.merge(bucketId, task.file().recordCount(), Long::sum);
      }
    }
    return bucketRecordCounts;
  }
}
