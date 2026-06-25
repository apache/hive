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
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests HMS and Iceberg metadata for Hive CLUSTERED BY combined with linear
 * {@code WRITE LOCALLY ORDERED BY}. Physical sort order within buckets is covered by
 * {@code iceberg_clustered_by_with_linear_sort.q}.
 */
public class TestHiveIcebergClusteredByWithLinearSort extends HiveIcebergStorageHandlerWithEngineBase {

  private static final String TABLE_NAME = "clustered_linear_sort_test";
  private static final TableIdentifier TABLE_ID = TableIdentifier.of("default", TABLE_NAME);
  private static final int NUM_BUCKETS = 3;

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}")
  public static Collection<Object[]> parameters() {
    return HiveIcebergStorageHandlerWithEngineBase.getParameters(p ->
        p.fileFormat() == FileFormat.PARQUET &&
        p.testTableType() == TestTableType.HIVE_CATALOG &&
        p.isVectorized() &&
        p.formatVersion() == 3);
  }

  @Test
  public void testClusteredByWithLinearSortViaCreateClause() throws TException, InterruptedException, IOException {
    shell.executeStatement(
        "CREATE TABLE " + TABLE_NAME +
        " (customer_id BIGINT, order_date DATE, amount DOUBLE, region STRING) " +
        "CLUSTERED BY (customer_id) INTO " + NUM_BUCKETS + " BUCKETS " +
        "WRITE LOCALLY ORDERED BY order_date ASC, amount DESC " +
        "STORED BY ICEBERG STORED AS PARQUET " +
        "TBLPROPERTIES ('format-version'='" + formatVersion + "')");

    validateHmsBucketMetadata();

    org.apache.iceberg.Table icebergTable = testTables.loadTable(TABLE_ID);
    SortOrder sortOrder = icebergTable.sortOrder();
    Assert.assertFalse("Sort order should not be unsorted when WRITE LOCALLY ORDERED BY is specified",
        sortOrder.isUnsorted());
    Assert.assertEquals("Sort order should have 2 fields for order_date and amount",
        2, sortOrder.fields().size());
    Assert.assertEquals("order_date",
        icebergTable.schema().findField(sortOrder.fields().get(0).sourceId()).name());
    Assert.assertEquals(SortDirection.ASC, sortOrder.fields().get(0).direction());
    Assert.assertEquals("amount",
        icebergTable.schema().findField(sortOrder.fields().get(1).sourceId()).name());
    Assert.assertEquals(SortDirection.DESC, sortOrder.fields().get(1).direction());

    insertTestData();
    validateBucketDistribution(icebergTable);

    shell.executeStatement("DROP TABLE IF EXISTS " + TABLE_NAME);
  }

  private void validateHmsBucketMetadata() throws TException, InterruptedException {
    Table hmsTable = shell.metastore().getTable("default", TABLE_NAME);
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

  private void validateBucketDistribution(org.apache.iceberg.Table icebergTable) throws IOException {
    icebergTable.refresh();
    Map<Integer, Long> bucketCounts = extractBucketRecordCounts(icebergTable);

    Assert.assertTrue("Should have at least one data file", !bucketCounts.isEmpty());

    long totalRecords = bucketCounts.values().stream().mapToLong(Long::longValue).sum();
    List<Object[]> countResult = shell.executeStatement("SELECT COUNT(*) FROM " + TABLE_NAME);
    long expectedCount = ((Number) countResult.getFirst()[0]).longValue();
    Assert.assertEquals("Total records in files should match table count", expectedCount, totalRecords);

    for (Integer bucketId : bucketCounts.keySet()) {
      Assert.assertTrue("Bucket ID should be within range [0, " + (NUM_BUCKETS - 1) + "]",
          bucketId >= 0 && bucketId < NUM_BUCKETS);
    }
  }

  private Map<Integer, Long> extractBucketRecordCounts(org.apache.iceberg.Table icebergTable) throws IOException {
    Map<Integer, Long> bucketRecordCounts = new LinkedHashMap<>();
    try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        String path = task.file().location();
        String filename = path.substring(path.lastIndexOf('/') + 1);
        int bucketId = Integer.parseInt(filename.split("-")[0]);
        bucketRecordCounts.merge(bucketId, task.file().recordCount(), Long::sum);
      }
    }
    return bucketRecordCounts;
  }
}
