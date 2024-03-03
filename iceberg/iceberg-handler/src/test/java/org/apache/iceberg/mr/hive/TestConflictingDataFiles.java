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
import java.util.concurrent.Executors;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.base.Throwables;
import org.apache.iceberg.util.Tasks;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.mr.hive.HiveIcebergStorageHandlerTestUtils.init;

public class TestConflictingDataFiles extends HiveIcebergStorageHandlerWithEngineBase {

  private final String storageHandlerStub = "'org.apache.iceberg.mr.hive.HiveIcebergStorageHandlerStub'";

  @Before
  public void setUpTables() {
    PartitionSpec spec =
        PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA).identity("last_name")
            .bucket("customer_id", 16).build();

    // create and insert an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, spec, fileFormat,
        HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, 2, Collections.emptyMap(), storageHandlerStub);
    // insert one more batch so that we have multiple data files within the same partition
    shell.executeStatement(testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1,
        TableIdentifier.of("default", "customers"), false));
    TestUtilPhaser.getInstance();
  }

  @After
  public void destroyTestSetUp() {
    TestUtilPhaser.destroyInstance();
  }

  @Test
  public void testSingleFilterUpdate() {
    String[] singleFilterQuery = new String[] { "UPDATE customers SET first_name='Changed' WHERE  last_name='Taylor'",
        "UPDATE customers SET first_name='Changed' WHERE  last_name='Donnel'" };

    try {
      Tasks.range(2).executeWith(Executors.newFixedThreadPool(2)).run(i -> {
        TestUtilPhaser.getInstance().getPhaser().register();
        init(shell, testTables, temp, executionEngine);
        HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, isVectorized);
        HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
        HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES,
            RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT);
        shell.executeStatement(singleFilterQuery[i]);
        shell.closeSession();
      });
      List<Object[]> objects =
          shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name, first_name");
      Assert.assertEquals(12, objects.size());
      List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
          .add(1L, "Joanna", "Pierce")
          .add(1L, "Changed", "Taylor")
          .add(2L, "Changed", "Donnel")
          .add(2L, "Susan", "Morrison")
          .add(2L, "Bob", "Silver")
          .add(2L, "Joanna", "Silver")
          .add(3L, "Marci", "Barna")
          .add(3L, "Blake", "Burr")
          .add(3L, "Trudy", "Henderson")
          .add(3L, "Trudy", "Johnson")
          .add(4L, "Laci", "Zold")
          .add(5L, "Peti", "Rozsaszin").build();
      HiveIcebergTestUtils.validateData(expected,
          HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);

    } catch (Throwable ex) {
      Throwable cause = Throwables.getRootCause(ex);
      Assert.fail(String.valueOf(cause));
    }
  }

  @Test
  public void testMultiFiltersUpdate() {

    String[] multiFilterQuery =
        new String[] { "UPDATE customers SET first_name='Changed' WHERE  last_name='Henderson' OR last_name='Johnson'",
            "UPDATE customers SET first_name='Changed' WHERE  last_name='Taylor' AND customer_id=1" };

    try {
      Tasks.range(2).executeWith(Executors.newFixedThreadPool(2)).run(i -> {
        TestUtilPhaser.getInstance().getPhaser().register();
        init(shell, testTables, temp, executionEngine);
        HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, isVectorized);
        HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
        HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES,
            RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT);
        shell.executeStatement(multiFilterQuery[i]);
        shell.closeSession();
      });
    } catch (Throwable ex) {
      // If retry succeeds then it should not throw an ValidationException.
      Throwable cause = Throwables.getRootCause(ex);
      Assert.assertTrue(cause instanceof ValidationException);
      if (cause.getMessage().matches("^Found.*conflicting.*files(.*)")) {
        Assert.fail();
      }
    }

    List<Object[]> objects =
        shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name, first_name");
    Assert.assertEquals(12, objects.size());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Joanna", "Pierce")
        .add(1L, "Changed", "Taylor")
        .add(2L, "Jake", "Donnel")
        .add(2L, "Susan", "Morrison")
        .add(2L, "Bob", "Silver")
        .add(2L, "Joanna", "Silver")
        .add(3L, "Marci", "Barna")
        .add(3L, "Blake", "Burr")
        .add(3L, "Changed", "Henderson")
        .add(3L, "Changed", "Johnson")
        .add(4L, "Laci", "Zold")
        .add(5L, "Peti", "Rozsaszin")
        .build();
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
  }

  @Test
  public void testDeleteFilters() {
    String[] sql = new String[] { "DELETE FROM customers WHERE  last_name='Taylor'",
        "DELETE FROM customers WHERE last_name='Donnel'",
        "DELETE FROM customers WHERE last_name='Henderson' OR last_name='Johnson'" };

    try {
      Tasks.range(3).executeWith(Executors.newFixedThreadPool(3)).run(i -> {
        TestUtilPhaser.getInstance().getPhaser().register();
        init(shell, testTables, temp, executionEngine);
        HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, isVectorized);
        HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
        HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES,
            RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT);
        shell.executeStatement(sql[i]);
        shell.closeSession();
      });
    } catch (Throwable ex) {
      Throwable cause = Throwables.getRootCause(ex);
      Assert.assertTrue(cause instanceof ValidationException);
      if (cause.getMessage().matches("^Found.*conflicting.*files(.*)")) {
        Assert.fail();
      }
    }

    List<Object[]> objects =
        shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name, first_name");
    Assert.assertEquals(8, objects.size());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Joanna", "Pierce")
        .add(2L, "Susan", "Morrison")
        .add(2L, "Bob", "Silver")
        .add(2L, "Joanna", "Silver")
        .add(3L, "Marci", "Barna")
        .add(3L, "Blake", "Burr")
        .add(4L, "Laci", "Zold")
        .add(5L, "Peti", "Rozsaszin")
        .build();
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
    TestUtilPhaser.destroyInstance();
  }

  @Test
  public void testConflictingUpdates() {
    String[] singleFilterQuery = new String[] { "UPDATE customers SET first_name='Changed' WHERE  last_name='Taylor'",
        "UPDATE customers SET first_name='Changed' WHERE  last_name='Taylor'" };

    try {
      Tasks.range(2).executeWith(Executors.newFixedThreadPool(2)).run(i -> {
        TestUtilPhaser.getInstance().getPhaser().register();
        init(shell, testTables, temp, executionEngine);
        HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, isVectorized);
        HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
        HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES,
            RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT);
        shell.executeStatement(singleFilterQuery[i]);
        shell.closeSession();
      });
    } catch (Throwable ex) {
      // Since there is a conflict it should throw ValidationException
      Throwable cause = Throwables.getRootCause(ex);
      Assert.assertTrue(cause instanceof ValidationException);
      Assert.assertTrue(cause.getMessage().matches("^Found.*conflicting" + ".*files(.*)"));
    }

    List<Object[]> objects =
        shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name, first_name");
    Assert.assertEquals(12, objects.size());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Joanna", "Pierce")
        .add(1L, "Changed", "Taylor")
        .add(2L, "Jake", "Donnel")
        .add(2L, "Susan", "Morrison")
        .add(2L, "Bob", "Silver")
        .add(2L, "Joanna", "Silver")
        .add(3L, "Marci", "Barna")
        .add(3L, "Blake", "Burr")
        .add(3L, "Trudy", "Henderson")
        .add(3L, "Trudy", "Johnson")
        .add(4L, "Laci", "Zold")
        .add(5L, "Peti", "Rozsaszin")
        .build();
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);

  }
}
