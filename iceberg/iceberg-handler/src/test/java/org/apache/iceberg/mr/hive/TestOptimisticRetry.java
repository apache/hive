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

import java.util.List;
import java.util.concurrent.Executors;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Throwables;
import org.apache.iceberg.util.Tasks;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.mr.hive.HiveIcebergStorageHandlerTestUtils.init;

public class TestOptimisticRetry extends HiveIcebergStorageHandlerWithEngineBase {

  @Override
  protected void validateTestParams() {
    Assume.assumeTrue(fileFormat == FileFormat.PARQUET && isVectorized &&
        testTableType == TestTables.TestTableType.HIVE_CATALOG && formatVersion == 2);
  }

  @Test
  public void testConcurrentOverlappingUpdates() {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion);
    String sql = "UPDATE customers SET last_name='Changed' WHERE customer_id=3 or first_name='Joanna'";

    try {
      Tasks.range(2)
          .executeWith(Executors.newFixedThreadPool(2))
          .run(i -> {
            init(shell, testTables, temp, executionEngine);
            HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, isVectorized);
            HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
            HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES,
                RETRY_STRATEGIES);
            shell.executeStatement(sql);
            shell.closeSession();
          });
    } catch (Throwable ex) {
      // If retry succeeds then it should not throw an ValidationException.
      Throwable cause = Throwables.getRootCause(ex);
      if (cause instanceof ValidationException && cause.getMessage().matches("^Found.*conflicting.*files(.*)")) {
        Assert.fail();
      }
    }

    List<Object[]> res = shell.executeStatement("SELECT * FROM customers WHERE last_name='Changed'");
    Assert.assertEquals(5, res.size());

  }


  @Test
  public void testNonOverlappingConcurrent2Updates() {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion);
    String[] sql = new String[]{"UPDATE customers SET last_name='Changed' WHERE customer_id=3 or first_name='Joanna'",
        "UPDATE customers SET last_name='Changed2' WHERE customer_id=2 and first_name='Jake'"};

    try {
      Tasks.range(2)
          .executeWith(Executors.newFixedThreadPool(2))
          .run(i -> {
            init(shell, testTables, temp, executionEngine);
            HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, isVectorized);
            HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
            HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES,
                RETRY_STRATEGIES);
            shell.executeStatement(sql[i]);
            shell.closeSession();
          });
    } catch (Throwable ex) {
      // If retry succeeds then it should not throw an ValidationException.
      Throwable cause = Throwables.getRootCause(ex);
      if (cause instanceof ValidationException && cause.getMessage().matches("^Found.*conflicting.*files(.*)")) {
        Assert.fail();
      }
    }

    List<Object[]> res = shell.executeStatement("SELECT * FROM customers WHERE last_name='Changed'");
    Assert.assertEquals(5, res.size());

    res = shell.executeStatement("SELECT * FROM customers WHERE last_name='Changed2'");
    Assert.assertEquals(1, res.size());
  }

  @Test
  public void testConcurrent2MergeInserts() {
    testTables.createTable(shell, "source", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1);
    testTables.createTable(shell, "target", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        formatVersion);

    String sql = "MERGE INTO target t USING source s on t.customer_id = s.customer_id WHEN Not MATCHED THEN " +
        "INSERT values (s.customer_id, s.first_name, s.last_name)";
    try {
      Tasks.range(2)
          .executeWith(Executors.newFixedThreadPool(2))
          .run(i -> {
            init(shell, testTables, temp, executionEngine);
            HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, isVectorized);
            HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
            HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES,
                RETRY_STRATEGIES);
            shell.executeStatement(sql);
            shell.closeSession();
          });
    } catch (Throwable ex) {
      // If retry succeeds then it should not throw an ValidationException.
      Throwable cause = Throwables.getRootCause(ex);
      if (cause instanceof ValidationException && cause.getMessage().matches("^Found.*conflicting.*files(.*)")) {
        Assert.fail();
      }
    }
    List<Object[]> res = shell.executeStatement("SELECT * FROM target");
    Assert.assertEquals(6, res.size());
  }

}
