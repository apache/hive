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
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.mr.hive.TestTables.TestTableType;
import org.apache.iceberg.mr.hive.test.concurrent.WithMockedStorageHandler;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;

@WithMockedStorageHandler
public class TestConflictingDataFiles extends HiveIcebergStorageHandlerWithEngineBase {

  public static final String STORAGE_HANDLER_STUB =
      "'org.apache.iceberg.mr.hive.test.concurrent.HiveIcebergStorageHandlerStub'";

  @Override
  protected void validateTestParams() {
    Assume.assumeTrue(
        fileFormat.equals(FileFormat.PARQUET) &&
        testTableType.equals(TestTableType.HIVE_CATALOG) &&
        isVectorized);
  }

  @Before
  public void setUpTables() {
    PartitionSpec spec =
        PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA).identity("last_name")
            .bucket("customer_id", 16).build();

    // create and insert an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion, Collections.emptyMap(), STORAGE_HANDLER_STUB);

    // insert one more batch so that we have multiple data files within the same partition
    shell.executeStatement(testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1,
        TableIdentifier.of("default", "customers"), false));
  }

  @Test
  public void testSingleFilterUpdate() throws Exception {
    Assume.assumeTrue(formatVersion >= 2);

    String[] sql = new String[] {
        "UPDATE customers SET first_name='Changed' WHERE last_name='Taylor'",
        "UPDATE customers SET first_name='Changed' WHERE last_name='Donnel'"
    };
    executeConcurrently(false, RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT, sql);

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
  }

  @Test
  public void testMultiFiltersUpdate() throws Exception {
    Assume.assumeTrue(formatVersion >= 2);

    String[] sql = new String[] {
        "UPDATE customers SET first_name='Changed' WHERE last_name='Henderson' OR last_name='Johnson'",
        "UPDATE customers SET first_name='Changed' WHERE last_name='Taylor' AND customer_id=1"
    };
    executeConcurrently(false, RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT, sql);

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
  public void testDeleteFilters() throws Exception {
    String[] sql = new String[] {
        "DELETE FROM customers WHERE  last_name='Taylor'",
        "DELETE FROM customers WHERE last_name='Donnel'",
        "DELETE FROM customers WHERE last_name='Henderson' OR last_name='Johnson'"
    };
    executeConcurrently(false, RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT, sql);

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
  }

  @Test
  public void testConflictingDeletes() throws Exception {
    String[] sql = new String[]{
        "DELETE FROM customers WHERE customer_id=3 or first_name='Joanna'",
        "DELETE FROM customers WHERE last_name='Johnson'"
    };

    try {
      executeConcurrently(false, RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT, sql);
    } catch (ValidationException ex) {
      if (formatVersion == 2) {
        Assert.fail("Unexpected ValidationException for format version 2");
      }
      Assert.assertTrue(ex.getMessage().startsWith(
          formatVersion == 3 ? "Found concurrently added DV" : "Found conflicting files"));
    }

    List<Object[]> objects =
        shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name, first_name");
    Assert.assertEquals(6, objects.size());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Sharon", "Taylor")
        .add(2L, "Jake", "Donnel")
        .add(2L, "Susan", "Morrison")
        .add(2L, "Bob", "Silver")
        .add(4L, "Laci", "Zold")
        .add(5L, "Peti", "Rozsaszin")
        .build();
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
  }

  @Test
  public void testConflictingUpdates() {
    String sql = "UPDATE customers SET first_name='Changed' " +
        "WHERE last_name='Taylor'";

    Throwable ex = Assert.assertThrows(ValidationException.class,
        () -> executeConcurrently(false, RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT, sql));
    Assert.assertTrue(ex.getMessage().startsWith("Found conflicting files"));

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

  @Test
  public void testConflictingUpdateAndDelete() {
    Assume.assumeTrue(formatVersion >= 2);

    String[] sql = new String[]{
        "DELETE FROM customers WHERE customer_id=3 or first_name='Joanna'",
        "UPDATE customers SET last_name='Changed' WHERE customer_id=3 or first_name='Joanna'"
    };

    Throwable ex = Assert.assertThrows(ValidationException.class,
        () -> executeConcurrently(false, RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT, sql));
    Assert.assertTrue(ex.getMessage().startsWith("Found new conflicting delete files"));

    List<Object[]> res = shell.executeStatement("SELECT * FROM customers WHERE last_name='Changed'");
    Assert.assertEquals(0, res.size());
  }

  @Test
  public void testConflictingMergeInserts() {
    testTables.createTable(shell, "source", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1);

    testTables.createTable(shell, "target", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        formatVersion, Collections.emptyMap(), STORAGE_HANDLER_STUB);

    String sql = "MERGE INTO target t USING source src on t.customer_id = src.customer_id WHEN NOT MATCHED THEN " +
        "INSERT values (src.customer_id, src.first_name, src.last_name)";

    Throwable ex = Assert.assertThrows(ValidationException.class,
        () -> executeConcurrently(false, RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT, sql));
    Assert.assertTrue(ex.getMessage().startsWith("Found conflicting files"));

    List<Object[]> res = shell.executeStatement("SELECT * FROM target");
    Assert.assertEquals(6, res.size());
  }

  @Test
  public void testConcurrentInsertAndOverwrite() throws Exception {
    Assume.assumeTrue(formatVersion >= 2);

    Schema schema = new Schema(
        required(1, "i", Types.IntegerType.get()),
        required(2, "p", Types.IntegerType.get())
    );
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("i", 10).build();

    // create and insert an initial batch of records
    testTables.createTable(shell, "ice_t", schema, spec, fileFormat,
        TestHelper.RecordsBuilder.newInstance(schema)
          .add(1, 1)
          .add(2, 2)
          .add(10, 10)
          .add(20, 20)
          .add(40, 40)
          .add(30, 30)
          .build(),
        formatVersion, Collections.emptyMap(), STORAGE_HANDLER_STUB);

    String[] sql = new String[] {
        "INSERT INTO ice_t SELECT i*100, p*100 FROM ice_t",
        "INSERT OVERWRITE TABLE ice_t SELECT i+1, p+1 FROM ice_t"
    };
    executeConcurrently(true, RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT, sql);

    List<Object[]> objects =
        shell.executeStatement("SELECT * FROM ice_t");
    Assert.assertEquals(12, objects.size());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(schema)
        .add(2, 2)
        .add(3, 3)
        .add(11, 11)
        .add(21, 21)
        .add(31, 31)
        .add(41, 41)
        .add(101, 101)
        .add(201, 201)
        .add(1001, 1001)
        .add(2001, 2001)
        .add(3001, 3001)
        .add(4001, 4001)
        .build();
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(schema, objects), 0);
  }
}
