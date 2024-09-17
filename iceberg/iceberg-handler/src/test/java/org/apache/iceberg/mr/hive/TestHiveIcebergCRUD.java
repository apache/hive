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
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.base.Throwables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Tasks;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.mr.hive.HiveIcebergStorageHandlerTestUtils.init;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Tests Format specific features, such as reading/writing tables, using delete files, etc.
 */
public class TestHiveIcebergCRUD extends HiveIcebergStorageHandlerWithEngineBase {

  @Override
  protected void validateTestParams() {
  }

  @Test
  public void testReadAndWriteFormatV2UnpartitionedWithEqDelete() throws IOException {
    Assume.assumeTrue("Reading V2 tables with eq delete files are only supported currently in " +
        "non-vectorized mode", !isVectorized && formatVersion == 2);

    Table tbl = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, formatVersion);

    // delete one of the rows
    List<Record> toDelete = TestHelper.RecordsBuilder
        .newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA).add(1L, "Bob", null).build();
    DeleteFile deleteFile = HiveIcebergTestUtils.createEqualityDeleteFile(tbl, "dummyPath",
        ImmutableList.of("customer_id", "first_name"), fileFormat, toDelete);
    tbl.newRowDelta().addDeletes(deleteFile).commit();

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id");

    // only the other two rows are present
    Assert.assertEquals(2, objects.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, objects.get(0));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, objects.get(1));
  }

  @Test
  public void testReadAndWriteFormatV2Partitioned_EqDelete_AllColumnsSupplied() throws IOException {
    Assume.assumeTrue("Reading V2 tables with eq delete files are only supported currently in " +
        "non-vectorized mode", !isVectorized && formatVersion == 2);

    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("customer_id").build();
    Table tbl = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, formatVersion);

    // add one more row to the same partition
    shell.executeStatement("insert into customers values (1, 'Bob', 'Hoover')");

    // delete all rows with id=1 and first_name=Bob
    List<Record> toDelete = TestHelper.RecordsBuilder
        .newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA).add(1L, "Bob", null).build();
    DeleteFile deleteFile = HiveIcebergTestUtils.createEqualityDeleteFile(tbl, "dummyPath",
        ImmutableList.of("customer_id", "first_name"), fileFormat, toDelete);
    tbl.newRowDelta().addDeletes(deleteFile).commit();

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id");

    Assert.assertEquals(2, objects.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, objects.get(0));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, objects.get(1));
  }

  @Test
  public void testReadAndWriteFormatV2Partitioned_EqDelete_OnlyEqColumnsSupplied() throws IOException {
    Assume.assumeTrue("Reading V2 tables with eq delete files are only supported currently in " +
        "non-vectorized mode", !isVectorized && formatVersion == 2);

    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("customer_id").build();
    Table tbl = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, formatVersion);

    // add one more row to the same partition
    shell.executeStatement("insert into customers values (1, 'Bob', 'Hoover')");

    // delete all rows with id=1 and first_name=Bob
    Schema shorterSchema = new Schema(
        optional(1, "id", Types.LongType.get()), optional(2, "name", Types.StringType.get()));
    List<Record> toDelete = TestHelper.RecordsBuilder.newInstance(shorterSchema).add(1L, "Bob").build();
    DeleteFile deleteFile = HiveIcebergTestUtils.createEqualityDeleteFile(tbl, "dummyPath",
        ImmutableList.of("customer_id", "first_name"), fileFormat, toDelete);
    tbl.newRowDelta().addDeletes(deleteFile).commit();

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id");

    Assert.assertEquals(2, objects.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, objects.get(0));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, objects.get(1));
  }

  @Test
  public void testReadAndWriteFormatV2Unpartitioned_PosDelete() throws IOException {
    Assume.assumeTrue(formatVersion == 2);

    Table tbl = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, formatVersion);

    // delete one of the rows
    DataFile dataFile = StreamSupport.stream(tbl.currentSnapshot().addedDataFiles(tbl.io()).spliterator(), false)
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Did not find any data files for test table"));
    List<PositionDelete<Record>> deletes = ImmutableList.of(positionDelete(
        dataFile.path(), 2L, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS.get(2))
    );
    DeleteFile deleteFile = HiveIcebergTestUtils.createPositionalDeleteFile(tbl, "dummyPath",
        fileFormat, null, deletes);
    tbl.newRowDelta().addDeletes(deleteFile).commit();

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id");

    // only the other two rows are present
    Assert.assertEquals(2, objects.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, objects.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", "Green"}, objects.get(1));
  }

  @Test
  public void testReadAndWriteFormatV2Partitioned_PosDelete_RowNotSupplied() throws IOException {
    Assume.assumeTrue(formatVersion == 2);

    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("customer_id").build();
    Table tbl = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, formatVersion);

    // add some more data to the same partition
    shell.executeStatement("insert into customers values (0, 'Laura', 'Yellow'), (0, 'John', 'Green'), " +
        "(0, 'Blake', 'Blue')");
    tbl.refresh();

    // delete the first and third rows from the newly-added data file - with row supplied
    DataFile dataFile = StreamSupport.stream(tbl.currentSnapshot().addedDataFiles(tbl.io()).spliterator(), false)
        .filter(file -> file.partition().get(0, Long.class) == 0L)
        .filter(file -> file.recordCount() == 3)
        .findAny()
        .orElseThrow(() -> new RuntimeException("Did not find the desired data file in the test table"));
    List<PositionDelete<Record>> deletes = ImmutableList.of(
        positionDelete(dataFile.path(), 0L, null),
        positionDelete(dataFile.path(), 2L, null)
    );
    DeleteFile deleteFile = HiveIcebergTestUtils.createPositionalDeleteFile(tbl, "dummyPath",
        fileFormat, ImmutableMap.of("customer_id", 0L), deletes);
    tbl.newRowDelta().addDeletes(deleteFile).commit();

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, first_name");

    Assert.assertEquals(4, objects.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, objects.get(0));
    Assert.assertArrayEquals(new Object[] {0L, "John", "Green"}, objects.get(1));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", "Green"}, objects.get(2));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, objects.get(3));
  }

  @Test
  public void testReadAndWriteFormatV2Partitioned_PosDelete_RowSupplied() throws IOException {
    Assume.assumeTrue(formatVersion == 2);

    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("customer_id").build();
    Table tbl = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, formatVersion);

    // add some more data to the same partition
    shell.executeStatement("insert into customers values (0, 'Laura', 'Yellow'), (0, 'John', 'Green'), " +
        "(0, 'Blake', 'Blue')");
    tbl.refresh();

    // delete the first and third rows from the newly-added data file
    DataFile dataFile = StreamSupport.stream(tbl.currentSnapshot().addedDataFiles(tbl.io()).spliterator(), false)
        .filter(file -> file.partition().get(0, Long.class) == 0L)
        .filter(file -> file.recordCount() == 3)
        .findAny()
        .orElseThrow(() -> new RuntimeException("Did not find the desired data file in the test table"));
    List<Record> rowsToDel = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(0L, "Laura", "Yellow").add(0L, "Blake", "Blue").build();
    List<PositionDelete<Record>> deletes = ImmutableList.of(
        positionDelete(dataFile.path(), 0L, rowsToDel.get(0)),
        positionDelete(dataFile.path(), 2L, rowsToDel.get(1))
    );
    DeleteFile deleteFile = HiveIcebergTestUtils.createPositionalDeleteFile(tbl, "dummyPath",
        fileFormat, ImmutableMap.of("customer_id", 0L), deletes);
    tbl.newRowDelta().addDeletes(deleteFile).commit();

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, first_name");

    Assert.assertEquals(4, objects.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, objects.get(0));
    Assert.assertArrayEquals(new Object[] {0L, "John", "Green"}, objects.get(1));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", "Green"}, objects.get(2));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, objects.get(3));
  }

  @Test
  public void testDeleteStatementUnpartitioned() throws TException, InterruptedException {
    // create and insert an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion);

    // verify delete mode set to merge-on-read
    if (formatVersion == 2) {
      Assert.assertEquals(HiveIcebergStorageHandler.MERGE_ON_READ,
          shell.metastore().getTable("default", "customers")
          .getParameters().get(TableProperties.DELETE_MODE));
    }

    // insert one more batch so that we have multiple data files within the same partition
    shell.executeStatement(testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1,
        TableIdentifier.of("default", "customers"), false));

    shell.executeStatement("DELETE FROM customers WHERE customer_id=3 or first_name='Joanna'");

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name");
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
  public void testDeleteStatementPartitioned() {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").bucket("customer_id", 16).build();

    // create and insert an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, formatVersion);
    // insert one more batch so that we have multiple data files within the same partition
    shell.executeStatement(testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1,
        TableIdentifier.of("default", "customers"), false));

    shell.executeStatement("DELETE FROM customers WHERE customer_id=3 or first_name='Joanna'");

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name");
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
  public void testDeleteStatementWithOtherTable() {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").bucket("customer_id", 16).build();

    // create a couple of tables, with an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, formatVersion);
    testTables.createTable(shell, "other", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1, formatVersion);

    shell.executeStatement("DELETE FROM customers WHERE customer_id in (select t1.customer_id from customers t1 join " +
        "other t2 on t1.customer_id = t2.customer_id) or " +
        "first_name in (select first_name from customers where first_name = 'Bob')");

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name");
    Assert.assertEquals(5, objects.size());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Joanna", "Pierce")
        .add(1L, "Sharon", "Taylor")
        .add(2L, "Jake", "Donnel")
        .add(2L, "Susan", "Morrison")
        .add(2L, "Joanna", "Silver")
        .build();
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
  }

  @Test
  public void testDeleteStatementWithPartitionAndSchemaEvolution() {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").bucket("customer_id", 16).build();

    // create and insert an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, formatVersion);
    // insert one more batch so that we have multiple data files within the same partition
    shell.executeStatement(testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1,
        TableIdentifier.of("default", "customers"), false));

    // change the partition spec + schema, and insert some new records
    shell.executeStatement("ALTER TABLE customers SET PARTITION SPEC (bucket(64, last_name))");
    shell.executeStatement("ALTER TABLE customers ADD COLUMNS (department string)");
    shell.executeStatement("ALTER TABLE customers CHANGE COLUMN first_name given_name string first");
    shell.executeStatement("INSERT INTO customers VALUES ('Natalie', 20, 'Bloom', 'Finance'), ('Joanna', 22, " +
        "'Huberman', 'Operations')");

    // the delete should handle deleting records both from older specs and from the new spec without problems
    // there are records with Joanna in both the old and new spec, as well as the old and new schema
    shell.executeStatement("DELETE FROM customers WHERE customer_id=3 or given_name='Joanna'");

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name");
    Assert.assertEquals(7, objects.size());

    Schema newSchema = new Schema(
        optional(2, "given_name", Types.StringType.get()),
        optional(1, "customer_id", Types.LongType.get()),
        optional(3, "last_name", Types.StringType.get(), "This is last name"),
        optional(4, "department", Types.StringType.get())
    );
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(newSchema)
        .add("Sharon", 1L, "Taylor", null)
        .add("Jake", 2L, "Donnel", null)
        .add("Susan", 2L, "Morrison", null)
        .add("Bob", 2L, "Silver", null)
        .add("Laci", 4L, "Zold", null)
        .add("Peti", 5L, "Rozsaszin", null)
        .add("Natalie", 20L, "Bloom", "Finance")
        .build();
    HiveIcebergTestUtils.validateData(expected, HiveIcebergTestUtils.valueForRow(newSchema, objects), 0);
  }

  @Test
  public void testDeleteForSupportedTypes() throws IOException {
    Assume.assumeTrue(formatVersion == 2);

    for (int i = 0; i < SUPPORTED_TYPES.size(); i++) {
      Type type = SUPPORTED_TYPES.get(i);

      // TODO: remove this filter when issue #1881 is resolved
      if (type == Types.UUIDType.get() &&
            (fileFormat == FileFormat.PARQUET || fileFormat == FileFormat.ORC && isVectorized) ||
          type == Types.TimeType.get() &&
            fileFormat == FileFormat.PARQUET  && isVectorized) {
        continue;
      }

      // TODO: remove this filter when we figure out how we could test binary types
      if (type == Types.BinaryType.get() || type.equals(Types.FixedType.ofLength(5))) {
        continue;
      }

      String tableName = type.typeId().toString().toLowerCase() + "_table_" + i;
      String columnName = type.typeId().toString().toLowerCase() + "_column";

      Schema schema = new Schema(required(1, columnName, type));
      List<Record> records = TestHelper.generateRandomRecords(schema, 1, 0L);
      Table table = testTables.createTable(shell, tableName, schema, PartitionSpec.unpartitioned(), fileFormat, records,
          formatVersion);

      shell.executeStatement("DELETE FROM " + tableName);
      HiveIcebergTestUtils.validateData(table, ImmutableList.of(), 0);
    }
  }

  @Test
  public void testUpdateStatementUnpartitioned() throws TException, InterruptedException {
    // create and insert an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion);

    // verify update mode set to merge-on-read
    if (formatVersion == 2) {
      Assert.assertEquals(HiveIcebergStorageHandler.MERGE_ON_READ,
          shell.metastore().getTable("default", "customers")
          .getParameters().get(TableProperties.UPDATE_MODE));
    }

    // insert one more batch so that we have multiple data files within the same partition
    shell.executeStatement(testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1,
        TableIdentifier.of("default", "customers"), false));

    shell.executeStatement("UPDATE customers SET last_name='Changed' WHERE customer_id=3 or first_name='Joanna'");

    List<Object[]> objects =
        shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name, first_name");
    Assert.assertEquals(12, objects.size());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Joanna", "Changed")
        .add(1L, "Sharon", "Taylor")
        .add(2L, "Joanna", "Changed")
        .add(2L, "Jake", "Donnel")
        .add(2L, "Susan", "Morrison")
        .add(2L, "Bob", "Silver")
        .add(3L, "Blake", "Changed")
        .add(3L, "Marci", "Changed")
        .add(3L, "Trudy", "Changed")
        .add(3L, "Trudy", "Changed")
        .add(4L, "Laci", "Zold")
        .add(5L, "Peti", "Rozsaszin")
        .build();
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
  }

  @Test
  public void testUpdateStatementPartitioned() {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").bucket("customer_id", 16).build();

    // create and insert an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, formatVersion);
    // insert one more batch so that we have multiple data files within the same partition
    shell.executeStatement(testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1,
        TableIdentifier.of("default", "customers"), false));

    shell.executeStatement("UPDATE customers SET last_name='Changed' WHERE customer_id=3 or first_name='Joanna'");

    List<Object[]> objects =
        shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name, first_name");
    Assert.assertEquals(12, objects.size());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Joanna", "Changed")
        .add(1L, "Sharon", "Taylor")
        .add(2L, "Joanna", "Changed")
        .add(2L, "Jake", "Donnel")
        .add(2L, "Susan", "Morrison")
        .add(2L, "Bob", "Silver")
        .add(3L, "Blake", "Changed")
        .add(3L, "Marci", "Changed")
        .add(3L, "Trudy", "Changed")
        .add(3L, "Trudy", "Changed")
        .add(4L, "Laci", "Zold")
        .add(5L, "Peti", "Rozsaszin")
        .build();
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
  }

  @Test
  public void testUpdateStatementWithOtherTable() {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").bucket("customer_id", 16).build();

    // create a couple of tables, with an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, formatVersion);
    testTables.createTable(shell, "other", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1, formatVersion);

    shell.executeStatement("UPDATE customers SET last_name='Changed' WHERE customer_id in " +
        "(select t1.customer_id from customers t1 join other t2 on t1.customer_id = t2.customer_id) or " +
        "first_name in (select first_name from customers where first_name = 'Bob')");

    List<Object[]> objects =
        shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name, last_name");
    Assert.assertEquals(9, objects.size());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Joanna", "Pierce")
        .add(1L, "Sharon", "Taylor")
        .add(2L, "Bob", "Changed")
        .add(2L, "Jake", "Donnel")
        .add(2L, "Susan", "Morrison")
        .add(2L, "Joanna", "Silver")
        .add(3L, "Blake", "Changed")
        .add(3L, "Trudy", "Changed")
        .add(3L, "Trudy", "Changed")
        .build();
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
  }

  @Test
  public void testUpdateStatementWithPartitionAndSchemaEvolution() {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").bucket("customer_id", 16).build();

    // create and insert an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, formatVersion);
    // insert one more batch so that we have multiple data files within the same partition
    shell.executeStatement(testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1,
        TableIdentifier.of("default", "customers"), false));

    // change the partition spec + schema, and insert some new records
    shell.executeStatement("ALTER TABLE customers SET PARTITION SPEC (bucket(64, last_name))");
    shell.executeStatement("ALTER TABLE customers ADD COLUMNS (department string)");
    shell.executeStatement("ALTER TABLE customers CHANGE COLUMN first_name given_name string first");
    shell.executeStatement("INSERT INTO customers VALUES ('Natalie', 20, 'Bloom', 'Finance'), ('Joanna', 22, " +
        "'Huberman', 'Operations')");

    // update should handle changing records both from older specs and from the new spec without problems
    // there are records with Joanna in both the old and new spec, as well as the old and new schema
    shell.executeStatement("UPDATE customers set last_name='Changed' WHERE customer_id=3 or given_name='Joanna'");

    List<Object[]> objects =
        shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name, given_name");
    Assert.assertEquals(14, objects.size());

    Schema newSchema = new Schema(
        optional(2, "given_name", Types.StringType.get()),
        optional(1, "customer_id", Types.LongType.get()),
        optional(3, "last_name", Types.StringType.get(), "This is last name"),
        optional(4, "department", Types.StringType.get())
    );
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(newSchema)
        .add("Joanna", 1L, "Changed", null)
        .add("Sharon", 1L, "Taylor", null)
        .add("Joanna", 2L, "Changed", null)
        .add("Jake", 2L, "Donnel", null)
        .add("Susan", 2L, "Morrison", null)
        .add("Bob", 2L, "Silver", null)
        .add("Blake", 3L, "Changed", null)
        .add("Marci", 3L, "Changed", null)
        .add("Trudy", 3L, "Changed", null)
        .add("Trudy", 3L, "Changed", null)
        .add("Laci", 4L, "Zold", null)
        .add("Peti", 5L, "Rozsaszin", null)
        .add("Natalie", 20L, "Bloom", "Finance")
        .add("Joanna", 22L, "Changed", "Operations")
        .build();
    HiveIcebergTestUtils.validateData(expected, HiveIcebergTestUtils.valueForRow(newSchema, objects), 0);
  }

  @Test
  public void testUpdateForSupportedTypes() throws IOException {
    Assume.assumeTrue(formatVersion == 2);

    for (int i = 0; i < SUPPORTED_TYPES.size(); i++) {
      Type type = SUPPORTED_TYPES.get(i);

      // TODO: remove this filter when issue #1881 is resolved
      if (type == Types.UUIDType.get() &&
            (fileFormat == FileFormat.PARQUET || fileFormat == FileFormat.ORC && isVectorized) ||
          type == Types.TimeType.get() &&
            fileFormat == FileFormat.PARQUET  && isVectorized) {
        continue;
      }

      // TODO: remove this filter when we figure out how we could test binary types
      if (type == Types.BinaryType.get() || type.equals(Types.FixedType.ofLength(5))) {
        continue;
      }

      String tableName = type.typeId().toString().toLowerCase() + "_table_" + i;
      String columnName = type.typeId().toString().toLowerCase() + "_column";

      Schema schema = new Schema(required(1, columnName, type));
      List<Record> originalRecords = TestHelper.generateRandomRecords(schema, 1, 0L);
      Table table = testTables.createTable(shell, tableName, schema, PartitionSpec.unpartitioned(), fileFormat,
          originalRecords, formatVersion);

      List<Record> newRecords = TestHelper.generateRandomRecords(schema, 1, 3L);
      shell.executeStatement(testTables.getUpdateQuery(tableName, newRecords.get(0)));
      HiveIcebergTestUtils.validateData(table, newRecords, 0);
    }
  }

  @Test
  public void testConcurrent2Deletes() {
    Assume.assumeTrue(fileFormat == FileFormat.PARQUET && isVectorized &&
        testTableType == TestTables.TestTableType.HIVE_CATALOG);

    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion);
    String sql = "DELETE FROM customers WHERE customer_id=3 or first_name='Joanna'";

    try {
      Tasks.range(2)
          .executeWith(Executors.newFixedThreadPool(2))
          .run(i -> {
            init(shell, testTables, temp, executionEngine);
            HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, isVectorized);
            HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
            HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES,
                RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT);
            shell.executeStatement(sql);
            shell.closeSession();
          });
    } catch (Throwable ex) {
      Assert.assertEquals(1, (int) formatVersion);
      Throwable cause = Throwables.getRootCause(ex);
      Assert.assertTrue(cause instanceof ValidationException);
      Assert.assertTrue(cause.getMessage().startsWith("Found conflicting files"));
    }
    List<Object[]> res = shell.executeStatement("SELECT * FROM customers");
    Assert.assertEquals(4, res.size());
  }

  @Test
  public void testConcurrent2Updates() {
    Assume.assumeTrue(fileFormat == FileFormat.PARQUET && isVectorized &&
        testTableType == TestTables.TestTableType.HIVE_CATALOG);

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
                RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT);
            shell.executeStatement(sql);
            shell.closeSession();
          });
    } catch (Throwable ex) {
      Throwable cause = Throwables.getRootCause(ex);
      Assert.assertTrue(cause instanceof ValidationException);
      Assert.assertTrue(cause.getMessage().matches("^Found.*conflicting.*files(.*)"));
    }
    List<Object[]> res = shell.executeStatement("SELECT * FROM customers WHERE last_name='Changed'");
    Assert.assertEquals(5, res.size());
  }

  @Test
  public void testConcurrentUpdateAndDelete() {
    Assume.assumeTrue(fileFormat == FileFormat.PARQUET && isVectorized &&
        testTableType == TestTables.TestTableType.HIVE_CATALOG && formatVersion == 2);

    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion);
    String[] sql = new String[]{
        "DELETE FROM customers WHERE customer_id=3 or first_name='Joanna'",
        "UPDATE customers SET last_name='Changed' WHERE customer_id=3 or first_name='Joanna'"
    };

    boolean deleteFirst = false;
    try {
      Tasks.range(2)
          .executeWith(Executors.newFixedThreadPool(2))
          .run(i -> {
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
      Assert.assertTrue(cause.getMessage().matches("^Found.*conflicting.*files(.*)"));
      deleteFirst = cause.getMessage().contains("conflicting delete");
    }
    List<Object[]> res = shell.executeStatement("SELECT * FROM customers WHERE last_name='Changed'");
    Assert.assertEquals(deleteFirst ? 0 : 5, res.size());
  }

  @Test
  public void testConcurrent2MergeInserts() {
    Assume.assumeTrue(fileFormat == FileFormat.PARQUET && isVectorized &&
        testTableType == TestTables.TestTableType.HIVE_CATALOG);

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
                RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT);
            shell.executeStatement(sql);
            shell.closeSession();
          });
    } catch (Throwable ex) {
      Throwable cause = Throwables.getRootCause(ex);
      Assert.assertTrue(cause instanceof ValidationException);
      Assert.assertTrue(cause.getMessage().startsWith("Found conflicting files"));
    }
    List<Object[]> res = shell.executeStatement("SELECT * FROM target");
    Assert.assertEquals(6, res.size());
  }

  private static <T> PositionDelete<T> positionDelete(CharSequence path, long pos, T row) {
    PositionDelete<T> positionDelete = PositionDelete.create();
    return positionDelete.set(path, pos, row);
  }
}
