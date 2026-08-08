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
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergTestUtils;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

/**
 * JUnit coverage (Parquet) for Hive {@code CLUSTERED BY} + Iceberg z-order write behavior: bucket file layout,
 * TABLESAMPLE selection, on-disk z-order within each bucket (compare {@code ORDER BY ROW__POSITION}
 * vs {@code ORDER BY iceberg_zorder(...)}), ALTER z-order DDL, and reducer-cap routing.
 */
public class TestHiveIcebergClusteredByWithZOrder extends HiveIcebergStorageHandlerWithEngineBase {

  private static final String TABLE_NAME = "clustered_zorder_test";
  private static final TableIdentifier TABLE_ID = TableIdentifier.of("default", TABLE_NAME);
  private static final String ZORDER_REDUCER_CAP_TABLE = "ice_clus_zorder_reducer_cap";
  private static final TableIdentifier ZORDER_REDUCER_CAP_TABLE_ID =
      TableIdentifier.of("default", ZORDER_REDUCER_CAP_TABLE);
  private static final String ZORDER_ALTER_TABLE = "ice_clus_zorder_alter";
  private static final TableIdentifier ZORDER_ALTER_TABLE_ID =
      TableIdentifier.of("default", ZORDER_ALTER_TABLE);
  private static final int NUM_BUCKETS = 3;
  private static final int TOTAL_ROWS = 9;
  private static final int ZORDER_CLUSTER_BUCKET_COUNT = 8;
  private static final int REDUCERS_MAX_CAP = 2;

  /** Bucket distribution for {@link HiveIcebergStorageHandlerTestUtils#OTHER_CUSTOMER_RECORDS_2}. */
  private static final Map<Integer, Long> EXPECTED_CUSTOMER_BUCKET_RECORD_COUNTS = Map.of(
      0, 4L,
      1, 3L,
      2, 2L);

  /** Expected row counts per TABLESAMPLE bucket (BUCKET n OUT OF 3 → hash mod 3 == n-1). */
  private static final Map<Integer, Long> EXPECTED_TABLESAMPLE_COUNTS = Map.of(
      0, 5L,
      1, 2L,
      2, 2L);

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}")
  public static Collection<Object[]> parameters() {
    return HiveIcebergStorageHandlerWithEngineBase.getParameters(p ->
        p.fileFormat() == FileFormat.PARQUET &&
        p.testTableType() == TestTableType.HIVE_CATALOG &&
        p.isVectorized() &&
        p.formatVersion() == 2);
  }

  /**
   * CREATE TABLE with CLUSTERED BY + z-order, insert, one file per bucket,
   * z-order preserved on disk within each bucket (via TABLESAMPLE + virtual columns).
   */
  @Test
  public void testClusteredByWithZOrderViaCreateClause() throws TException, InterruptedException, IOException {
    createClusteredZOrderTable();
    validateHmsBucketMetadata();

    insertTestData();
    org.apache.iceberg.Table icebergTable = testTables.loadTable(TABLE_ID);

    icebergTable.refresh();
    BucketFileLayout bucketFileLayout = bucketFileLayout(icebergTable);

    validateBucketDistribution(bucketFileLayout.recordCountsByBucket());
    validateTableSampleBucketCounts();
    validateOneDataFilePerBucket(bucketFileLayout.fileCountsByBucket());
    validateFileLayoutZOrderWithinEachBucket();

    shell.executeStatement("DROP TABLE IF EXISTS " + TABLE_NAME);
  }

  /**
   * CLUSTERED BY created first, then z-order added via ALTER — one Iceberg data file per bucket.
   */
  @Test
  public void testClusteredByWithZOrderViaAlterProducesSingleFilePerBucket() throws IOException {
    setUpZOrderAlterTable();

    org.apache.iceberg.Table icebergTable = testTables.loadTable(ZORDER_ALTER_TABLE_ID);
    BucketFileLayout layout = bucketFileLayout(icebergTable);
    Assert.assertEquals(EXPECTED_CUSTOMER_BUCKET_RECORD_COUNTS, layout.recordCountsByBucket());

    Map<Integer, Integer> filesPerBucket = layout.fileCountsByBucket();
    for (int bucketId = 0; bucketId < NUM_BUCKETS; bucketId++) {
      Assert.assertEquals(
          "Expected one data file for bucket " + bucketId + " but saw " + filesPerBucket,
          Integer.valueOf(1),
          filesPerBucket.get(bucketId));
    }

    shell.executeStatement("DROP TABLE IF EXISTS " + ZORDER_ALTER_TABLE);
  }

  /**
   * With z-order and {@code hive.exec.reducers.max} below bucket count, bucket routing should
   * write data for multiple logical bucket prefixes.
   */
  @Test
  public void testClusteredByWithZOrderAndReducerCapProducesMultipleBucketPrefixedFiles() throws IOException {
    setUpZOrderReducerCapTable();

    org.apache.iceberg.Table icebergTable = testTables.loadTable(ZORDER_REDUCER_CAP_TABLE_ID);
    Map<Integer, Long> bucketRecordCounts = bucketFileLayout(icebergTable).recordCountsByBucket();

    long totalRows = bucketRecordCounts.values().stream().mapToLong(Long::longValue).sum();
    Assert.assertEquals(
        HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2.size(), totalRows);

    Assert.assertTrue(
        String.format(
            "Expected more than one bucket-parsed data file prefix with %s buckets and " +
                "hive.exec.reducers.max=%s; counts per parsed bucket prefix: %s",
            ZORDER_CLUSTER_BUCKET_COUNT,
            REDUCERS_MAX_CAP,
            bucketRecordCounts),
        bucketRecordCounts.size() > 1);

    shell.executeStatement("DROP TABLE IF EXISTS " + ZORDER_REDUCER_CAP_TABLE);
  }

  private void createClusteredZOrderTable() {
    shell.executeStatement(
        "CREATE TABLE " + TABLE_NAME +
        " (customer_id BIGINT, order_date DATE, amount DOUBLE, region STRING) " +
        "CLUSTERED BY (customer_id) INTO " + NUM_BUCKETS + " BUCKETS " +
        "WRITE LOCALLY ORDERED BY zorder(amount, region) " +
        "STORED BY ICEBERG STORED AS PARQUET " +
        "TBLPROPERTIES ('format-version'='" + formatVersion + "')");
  }

  private void setUpZOrderAlterTable() {
    shell.executeStatement(
        "CREATE TABLE " + ZORDER_ALTER_TABLE +
        " (customer_id BIGINT, first_name STRING, last_name STRING) " +
        "CLUSTERED BY (customer_id) INTO " + NUM_BUCKETS + " BUCKETS " +
        "STORED BY ICEBERG STORED AS PARQUET " +
        "TBLPROPERTIES ('format-version'='" + formatVersion + "')");

    shell.executeStatement(
        "ALTER TABLE " + ZORDER_ALTER_TABLE +
        " SET WRITE ORDERED BY ZORDER (customer_id, first_name)");

    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, ZORDER_ALTER_TABLE_ID, false));
  }

  private void setUpZOrderReducerCapTable() {
    shell.setHiveSessionValue("hive.exec.reducers.max", Integer.toString(REDUCERS_MAX_CAP));

    shell.executeStatement(
        "CREATE TABLE " + ZORDER_REDUCER_CAP_TABLE +
        " (customer_id BIGINT, first_name STRING, last_name STRING) " +
        "CLUSTERED BY (customer_id) INTO " + ZORDER_CLUSTER_BUCKET_COUNT + " BUCKETS " +
        "STORED BY ICEBERG STORED AS PARQUET " +
        "TBLPROPERTIES ('format-version'='" + formatVersion + "')");

    shell.executeStatement(
        "ALTER TABLE " + ZORDER_REDUCER_CAP_TABLE +
        " SET WRITE ORDERED BY ZORDER (customer_id, first_name)");

    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, ZORDER_REDUCER_CAP_TABLE_ID, false));
  }

  private void validateFileLayoutZOrderWithinEachBucket() {
    for (int bucket = 1; bucket <= NUM_BUCKETS; bucket++) {
      List<Object[]> physicalRows = readBucketRowsByRowPosition(bucket);
      List<Object[]> zSortedRows = readBucketRowsByZOrder(bucket);

      long expectedCount = EXPECTED_TABLESAMPLE_COUNTS.get(bucket - 1);
      Assert.assertEquals(
          "Unexpected row count for TABLESAMPLE bucket " + bucket + " (physical order)",
          expectedCount,
          physicalRows.size());
      Assert.assertEquals(
          "Unexpected row count for TABLESAMPLE bucket " + bucket + " (z-order sort)",
          expectedCount,
          zSortedRows.size());

      assertSameDataRows(physicalRows, zSortedRows,
          "On-disk row order should match z-order(amount, region) within TABLESAMPLE bucket " + bucket);
    }
  }

  private List<Object[]> readBucketRowsByRowPosition(int sampleBucket) {
    return shell.executeStatement(
        "SELECT s.amount, s.region, s.customer_id, s.ROW__POSITION " +
        "FROM " + TestHiveIcebergClusteredByWithZOrder.TABLE_NAME +
        " TABLESAMPLE(BUCKET " + sampleBucket + " OUT OF " + NUM_BUCKETS +
        " ON customer_id) s " +
        "ORDER BY s.ROW__POSITION");
  }

  private List<Object[]> readBucketRowsByZOrder(int sampleBucket) {
    return shell.executeStatement(
        "SELECT s.amount, s.region, s.customer_id " +
        "FROM " + TestHiveIcebergClusteredByWithZOrder.TABLE_NAME +
        " TABLESAMPLE(BUCKET " + sampleBucket + " OUT OF " + NUM_BUCKETS +
        " ON customer_id) s " +
        "ORDER BY iceberg_zorder(s.amount, s.region)");
  }

  private void validateTableSampleBucketCounts() {
    long total = 0;
    for (int bucket = 1; bucket <= NUM_BUCKETS; bucket++) {
      List<Object[]> result = shell.executeStatement(
          "SELECT COUNT(*) FROM " + TestHiveIcebergClusteredByWithZOrder.TABLE_NAME +
          " TABLESAMPLE(BUCKET " + bucket + " OUT OF " + NUM_BUCKETS + " ON customer_id)");
      long count = ((Number) result.getFirst()[0]).longValue();
      Assert.assertEquals(
          "TABLESAMPLE bucket " + bucket + " row count",
          EXPECTED_TABLESAMPLE_COUNTS.get(bucket - 1).longValue(),
          count);
      total += count;
    }
    Assert.assertEquals(TOTAL_ROWS, total);
  }

  private void validateOneDataFilePerBucket(Map<Integer, Integer> filesPerBucket) {
    for (int bucketId = 0; bucketId < NUM_BUCKETS; bucketId++) {
      Assert.assertEquals(
          "Expected one data file for bucket prefix " + bucketId + " but saw " + filesPerBucket,
          Integer.valueOf(1),
          filesPerBucket.get(bucketId));
    }
  }

  private void validateHmsBucketMetadata() throws TException, InterruptedException {
    Table hmsTable = shell.metastore().getTable("default", TestHiveIcebergClusteredByWithZOrder.TABLE_NAME);
    Assert.assertEquals(NUM_BUCKETS, hmsTable.getSd().getNumBuckets());
    Assert.assertEquals(Collections.singletonList("customer_id"), hmsTable.getSd().getBucketCols());
  }

  private void insertTestData() {
    shell.executeStatement(
        "INSERT INTO " + TABLE_NAME + " VALUES " +
        "(1, DATE '2024-01-15', 125.50, 'North'), " +
        "(2, DATE '2024-02-20', 89.75, 'South'), " +
        "(3, DATE '2024-01-10', 245.30, 'East'), " +
        "(1, DATE '2024-03-05', 67.90, 'North'), " +
        "(4, DATE '2024-02-28', 178.45, 'West'), " +
        "(2, DATE '2024-01-22', 312.80, 'South'), " +
        "(5, DATE '2024-03-12', 156.20, 'East'), " +
        "(3, DATE '2024-02-14', 234.65, 'East'), " +
        "(6, DATE '2024-01-30', 98.40, 'West')");
  }

  private void validateBucketDistribution(Map<Integer, Long> bucketCounts) {
    Assert.assertFalse("Should have at least one data file", bucketCounts.isEmpty());

    // Validate number of rows in the table
    long totalRecords = bucketCounts.values().stream().mapToLong(Long::longValue).sum();
    List<Object[]> countResult = shell.executeStatement("SELECT COUNT(*) FROM " + TABLE_NAME);
    long expectedCount = ((Number) countResult.getFirst()[0]).longValue();
    Assert.assertEquals("Total records in files should match table count", expectedCount, totalRecords);

    for (Integer bucketId : bucketCounts.keySet()) {
      Assert.assertTrue("Bucket ID should be within range [0, " + (NUM_BUCKETS - 1) + "]",
          bucketId >= 0 && bucketId < NUM_BUCKETS);
    }
  }

  private static void assertSameDataRows(List<Object[]> expected, List<Object[]> actual, String context) {
    Assert.assertEquals(context + ": row count", expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals(context + ": amount at row " + i, expected.get(i)[0], actual.get(i)[0]);
      Assert.assertEquals(context + ": region at row " + i, expected.get(i)[1], actual.get(i)[1]);
      Assert.assertEquals(context + ": customer_id at row " + i, expected.get(i)[2], actual.get(i)[2]);
    }
  }

  private static final class BucketFileLayout {
    private final Map<Integer, Integer> fileCountsByBucket;
    private final Map<Integer, Long> recordCountsByBucket;

    private BucketFileLayout(Map<Integer, Integer> fileCountsByBucket,
        Map<Integer, Long> recordCountsByBucket) {
      this.fileCountsByBucket = fileCountsByBucket;
      this.recordCountsByBucket = recordCountsByBucket;
    }

    private Map<Integer, Integer> fileCountsByBucket() {
      return fileCountsByBucket;
    }

    private Map<Integer, Long> recordCountsByBucket() {
      return recordCountsByBucket;
    }
  }

  private BucketFileLayout bucketFileLayout(org.apache.iceberg.Table icebergTable) throws IOException {
    Map<Integer, Integer> fileCountsByBucket = new LinkedHashMap<>();
    Map<Integer, Long> recordCountsByBucket = new LinkedHashMap<>();
    try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        int bucketId = HiveIcebergTestUtils.parseBucketIdFromFileName(task.file().location());
        fileCountsByBucket.merge(bucketId, 1, Integer::sum);
        recordCountsByBucket.merge(bucketId, task.file().recordCount(), Long::sum);
      }
    }
    return new BucketFileLayout(fileCountsByBucket, recordCountsByBucket);
  }
}
