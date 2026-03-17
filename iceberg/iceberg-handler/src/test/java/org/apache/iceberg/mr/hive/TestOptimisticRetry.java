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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.mr.hive.test.concurrent.WithMockedStorageHandler;
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergStorageHandlerTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.iceberg.mr.hive.TestConflictingDataFiles.STORAGE_HANDLER_STUB;

@WithMockedStorageHandler
public class TestOptimisticRetry extends HiveIcebergStorageHandlerWithEngineBase {

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}")
  public static Collection<Object[]> parameters() {
    return HiveIcebergStorageHandlerWithEngineBase.getParameters(p ->
        p.fileFormat() == FileFormat.PARQUET &&
        p.testTableType() == TestTableType.HIVE_CATALOG &&
        p.isVectorized());
  }

  @Test
  public void testConcurrentOverlappingUpdates() throws Exception {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion, Collections.emptyMap(), STORAGE_HANDLER_STUB);

    String[][] sql = new String[][]{{
            "UPDATE customers SET last_name='Changed' WHERE customer_id=3 or first_name='Joanna'"
        }};
    executeConcurrentlyWithRetry(sql);

    List<Object[]> res = shell.executeStatement("SELECT count(*) FROM customers WHERE last_name='Changed'");
    Assert.assertEquals(5L, res.getFirst()[0]);
  }

  @Test
  public void testConcurrentOverwriteAndUpdate() throws Exception {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion, Collections.emptyMap(), STORAGE_HANDLER_STUB);

    String[][] sql = new String[][] {
        {
            "INSERT OVERWRITE table customers SELECT * FROM customers WHERE last_name='Taylor'"
        }, {
            "UPDATE customers SET first_name='Changed' WHERE last_name='Taylor'"
        }};
    executeConcurrentlyWithRetry(sql);

    List<Object[]> res = shell.executeStatement("SELECT count(*) FROM customers");
    Assert.assertEquals(1L, res.getFirst()[0]);

    res = shell.executeStatement("SELECT count(*) FROM customers WHERE first_name='Changed'");
    Assert.assertEquals(1L, res.getFirst()[0]);
  }

  @Test
  public void testNonOverlappingConcurrent2Updates() throws Exception {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion, Collections.emptyMap(), STORAGE_HANDLER_STUB);

    String[][] sql = new String[][]{
        {
            "UPDATE customers SET last_name='Changed' WHERE customer_id=3 or first_name='Joanna'"
        }, {
            "UPDATE customers SET last_name='Changed2' WHERE customer_id=2 and first_name='Jake'"
        }};
    executeConcurrentlyWithRetry(sql);

    List<Object[]> res = shell.executeStatement("SELECT count(*) FROM customers WHERE last_name='Changed'");
    Assert.assertEquals(5L, res.getFirst()[0]);

    res = shell.executeStatement("SELECT count(*) FROM customers WHERE last_name='Changed2'");
    Assert.assertEquals(1L, res.getFirst()[0]);
  }

  @Test
  public void testConcurrent2MergeInserts() throws Exception {
    testTables.createTable(shell, "source", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1);

    testTables.createTable(shell, "target", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        formatVersion, Collections.emptyMap(), STORAGE_HANDLER_STUB);

    String[][] sql = new String[][]{{
            "MERGE INTO target t USING source s on t.customer_id = s.customer_id " +
                "WHEN NOT MATCHED THEN " +
                "INSERT VALUES (s.customer_id, s.first_name, s.last_name)"
        }};
    executeConcurrentlyWithRetry(sql);

    List<Object[]> res = shell.executeStatement("SELECT count(*) FROM target");
    Assert.assertEquals(6L, res.getFirst()[0]);
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

    String[][] sql = new String[][] {
        {
            "MERGE INTO target t USING source src ON t.name = src.name " +
                "WHEN MATCHED THEN " +
                "UPDATE SET age=15"
        }, {
            "MERGE INTO target t USING source src ON t.age = src.age " +
                "WHEN MATCHED THEN " +
                "UPDATE SET age=15"
        }};
    executeConcurrentlyWithRetry(sql);

    List<Object[]> res = shell.executeStatement("SELECT count(*) FROM target where age = 15");
    Assert.assertEquals(2L, res.getFirst()[0]);
  }
}
