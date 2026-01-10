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

import java.util.Collections;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.mr.hive.TestTables.TestTableType;
import org.apache.iceberg.mr.hive.test.concurrent.WithMockedStorageHandler;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.mr.hive.TestConflictingDataFiles.STORAGE_HANDLER_STUB;

@WithMockedStorageHandler
public class TestOptimisticRetry extends HiveIcebergStorageHandlerWithEngineBase {

  @Override
  protected void validateTestParams() {
    Assume.assumeTrue(
        fileFormat == FileFormat.PARQUET &&
        testTableType == TestTableType.HIVE_CATALOG &&
        isVectorized);
  }

  @Test
  public void testConcurrentOverlappingUpdates() throws Exception {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion, Collections.emptyMap(), STORAGE_HANDLER_STUB);

    String sql = "UPDATE customers SET last_name='Changed' WHERE customer_id=3 or first_name='Joanna'";
    executeConcurrently(false, RETRY_STRATEGIES, sql);

    List<Object[]> res = shell.executeStatement("SELECT * FROM customers WHERE last_name='Changed'");
    Assert.assertEquals(5, res.size());
  }

  @Test
  public void testConcurrentOverwriteAndUpdate() throws Exception {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion, Collections.emptyMap(), STORAGE_HANDLER_STUB);

    String[] sql = new String[] {
        "INSERT OVERWRITE table customers SELECT * FROM customers where last_name='Taylor'",
        "UPDATE customers SET first_name='Changed' WHERE  last_name='Taylor'"
    };
    executeConcurrently(false, RETRY_STRATEGIES, sql);
  }

  @Test
  public void testNonOverlappingConcurrent2Updates() throws Exception {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion, Collections.emptyMap(), STORAGE_HANDLER_STUB);

    String[] sql = new String[]{
        "UPDATE customers SET last_name='Changed' WHERE customer_id=3 or first_name='Joanna'",
        "UPDATE customers SET last_name='Changed2' WHERE customer_id=2 and first_name='Jake'"
    };
    executeConcurrently(false, RETRY_STRATEGIES, sql);

    List<Object[]> res = shell.executeStatement("SELECT * FROM customers WHERE last_name='Changed'");
    Assert.assertEquals(5, res.size());

    res = shell.executeStatement("SELECT * FROM customers WHERE last_name='Changed2'");
    Assert.assertEquals(1, res.size());
  }

  @Test
  public void testConcurrent2MergeInserts() throws Exception {
    testTables.createTable(shell, "source", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1);

    testTables.createTable(shell, "target", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        formatVersion, Collections.emptyMap(), STORAGE_HANDLER_STUB);

    String sql = "MERGE INTO target t USING source s on t.customer_id = s.customer_id WHEN Not MATCHED THEN " +
        "INSERT values (s.customer_id, s.first_name, s.last_name)";
    executeConcurrently(false, RETRY_STRATEGIES, sql);

    List<Object[]> res = shell.executeStatement("SELECT * FROM target");
    Assert.assertEquals(6, res.size());
  }

  @Test
  public void testConcurrent2MergeUpdates() throws Exception {
    testTables.createTable(shell, "source",
        HiveIcebergStorageHandlerTestUtils.USER_CLICKS_SCHEMA,  PartitionSpec.unpartitioned(),
        fileFormat, HiveIcebergStorageHandlerTestUtils.USER_CLICKS_RECORDS_1,
        formatVersion);

    testTables.createTable(shell, "target",
        HiveIcebergStorageHandlerTestUtils.USER_CLICKS_SCHEMA,  PartitionSpec.unpartitioned(),
        fileFormat, HiveIcebergStorageHandlerTestUtils.USER_CLICKS_RECORDS_2,
        formatVersion, Collections.emptyMap(), STORAGE_HANDLER_STUB);

    String query1 = "merge into target t using source src on t.name = src.name " +
        "when matched then update set age=15";
    String query2 = "merge into target t using source src on t.age = src.age " +
        "when matched then update set age=15";

    String[] sql = new String[] {query1, query2};
    executeConcurrently(false, RETRY_STRATEGIES, sql);

    List<Object[]> res = shell.executeStatement("SELECT * FROM target where age = 15");
    Assert.assertEquals(2, res.size());
  }
}
