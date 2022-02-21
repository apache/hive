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
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Runs insert queries against Iceberg tables. These should cover the various insertion methods of Hive including:
 * inserting values, inserting from select subqueries, insert overwrite table, etc...
 */
public class TestHiveIcebergInserts extends HiveIcebergStorageHandlerWithEngineBase {

  @Test
  public void testInsert() throws IOException {
    Table table = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, ImmutableList.of());

    // The expected query is like
    // INSERT INTO customers VALUES (0, 'Alice'), (1, 'Bob'), (2, 'Trudy')
    StringBuilder query = new StringBuilder().append("INSERT INTO customers VALUES ");
    HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS.forEach(record -> query.append("(")
        .append(record.get(0)).append(",'")
        .append(record.get(1)).append("','")
        .append(record.get(2)).append("'),"));
    query.setLength(query.length() - 1);

    shell.executeStatement(query.toString());

    HiveIcebergTestUtils.validateData(table, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 0);
  }

  @Test
  public void testInsertSupportedTypes() throws IOException {
    for (int i = 0; i < SUPPORTED_TYPES.size(); i++) {
      Type type = SUPPORTED_TYPES.get(i);
      // TODO: remove this filter when issue #1881 is resolved
      if (type == Types.UUIDType.get() && fileFormat == FileFormat.PARQUET) {
        continue;
      }
      // TODO: remove this filter when we figure out how we could test binary types
      if (type.equals(Types.BinaryType.get()) || type.equals(Types.FixedType.ofLength(5))) {
        continue;
      }
      String columnName = type.typeId().toString().toLowerCase() + "_column";

      Schema schema = new Schema(required(1, "id", Types.LongType.get()), required(2, columnName, type));
      List<Record> expected = TestHelper.generateRandomRecords(schema, 5, 0L);

      Table table = testTables.createTable(shell, type.typeId().toString().toLowerCase() + "_table_" + i,
          schema, PartitionSpec.unpartitioned(), fileFormat, expected);

      HiveIcebergTestUtils.validateData(table, expected, 0);
    }
  }

  /**
   * Testing map only inserts.
   * @throws IOException If there is an underlying IOException
   */
  @Test
  public void testInsertFromSelect() throws IOException {
    Table table = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    shell.executeStatement("INSERT INTO customers SELECT * FROM customers");

    // Check that everything is duplicated as expected
    List<Record> records = new ArrayList<>(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    records.addAll(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    HiveIcebergTestUtils.validateData(table, records, 0);
  }

  @Test
  public void testInsertOverwriteNonPartitionedTable() throws IOException {
    TableIdentifier target = TableIdentifier.of("default", "target");
    Table table = testTables.createTable(shell, target.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, ImmutableList.of());

    // IOW overwrites the whole table (empty target table)
    testTables.createTable(shell, "source", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    shell.executeStatement("INSERT OVERWRITE TABLE target SELECT * FROM source");

    HiveIcebergTestUtils.validateData(table, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 0);

    // IOW overwrites the whole table (non-empty target table)
    List<Record> newRecords = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(0L, "Mike", "Taylor")
        .add(1L, "Christy", "Hubert")
        .build();
    shell.executeStatement(testTables.getInsertQuery(newRecords, target, true));

    HiveIcebergTestUtils.validateData(table, newRecords, 0);

    // IOW empty result set -> clears the target table
    shell.executeStatement("INSERT OVERWRITE TABLE target SELECT * FROM source WHERE FALSE");

    HiveIcebergTestUtils.validateData(table, ImmutableList.of(), 0);
  }

  @Test
  public void testInsertOverwritePartitionedTable() throws IOException {
    TableIdentifier target = TableIdentifier.of("default", "target");
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").build();
    Table table = testTables.createTable(shell, target.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, ImmutableList.of());

    // IOW into empty target table -> whole source result set is inserted
    List<Record> expected = new ArrayList<>(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    expected.add(TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(8L, "Sue", "Green").build().get(0)); // add one more to 'Green' so we have a partition w/ multiple records
    shell.executeStatement(testTables.getInsertQuery(expected, target, true));

    HiveIcebergTestUtils.validateData(table, expected, 0);

    // IOW into non-empty target table -> only the affected partitions are overwritten
    List<Record> newRecords = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(0L, "Mike", "Brown") // overwritten
        .add(1L, "Christy", "Green") // overwritten (partition has this single record now)
        .add(3L, "Bill", "Purple") // appended (new partition)
        .build();
    shell.executeStatement(testTables.getInsertQuery(newRecords, target, true));

    expected = new ArrayList<>(newRecords);
    expected.add(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS.get(2)); // existing, untouched partition ('Pink')
    HiveIcebergTestUtils.validateData(table, expected, 0);

    // IOW empty source result set -> has no effect on partitioned table
    shell.executeStatement("INSERT OVERWRITE TABLE target SELECT * FROM target WHERE FALSE");

    HiveIcebergTestUtils.validateData(table, expected, 0);
  }

  @Test
  public void testInsertOverwriteBucketPartitionedTableThrowsError() {
    TableIdentifier target = TableIdentifier.of("default", "target");
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .bucket("last_name", 16).identity("customer_id").build();
    testTables.createTable(shell, target.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, ImmutableList.of());

    AssertHelpers.assertThrows("IOW should not work on bucket partitioned table", IllegalArgumentException.class,
        "Cannot perform insert overwrite query on bucket partitioned Iceberg table",
        () -> shell.executeStatement(
            testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, target, true)));
  }

  /**
   * Testing map-reduce inserts.
   * @throws IOException If there is an underlying IOException
   */
  @Test
  public void testInsertFromSelectWithOrderBy() throws IOException {
    Table table = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // We expect that there will be Mappers and Reducers here
    shell.executeStatement("INSERT INTO customers SELECT * FROM customers ORDER BY customer_id");

    // Check that everything is duplicated as expected
    List<Record> records = new ArrayList<>(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    records.addAll(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    HiveIcebergTestUtils.validateData(table, records, 0);
  }

  @Test
  public void testInsertFromSelectWithProjection() throws IOException {
    Table table = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, ImmutableList.of());
    testTables.createTable(shell, "orders", ORDER_SCHEMA, fileFormat, ORDER_RECORDS);

    shell.executeStatement(
        "INSERT INTO customers (customer_id, last_name) SELECT distinct(customer_id), 'test' FROM orders");

    List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(0L, null, "test")
        .add(1L, null, "test")
        .build();

    HiveIcebergTestUtils.validateData(table, expected, 0);
  }

  @Test
  public void testInsertUsingSourceTableWithSharedColumnsNames() throws IOException {
    List<Record> records = HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS;
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").build();
    testTables.createTable(shell, "source_customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, records);
    Table table = testTables.createTable(shell, "target_customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, spec, fileFormat, ImmutableList.of());

    // Below select from source table should produce: "hive.io.file.readcolumn.names=customer_id,last_name".
    // Inserting into the target table should not fail because first_name is not selected from the source table
    shell.executeStatement("INSERT INTO target_customers SELECT customer_id, 'Sam', last_name FROM source_customers");

    List<Record> expected = Lists.newArrayListWithExpectedSize(records.size());
    records.forEach(r -> {
      Record copy = r.copy();
      copy.setField("first_name", "Sam");
      expected.add(copy);
    });
    HiveIcebergTestUtils.validateData(table, expected, 0);
  }

  @Test
  public void testInsertFromJoiningTwoIcebergTables() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").build();
    testTables.createTable(shell, "source_customers_1", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    testTables.createTable(shell, "source_customers_2", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    Table table = testTables.createTable(shell, "target_customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, spec, fileFormat, ImmutableList.of());

    shell.executeStatement("INSERT INTO target_customers SELECT a.customer_id, b.first_name, a.last_name FROM " +
        "source_customers_1 a JOIN source_customers_2 b ON a.last_name = b.last_name");

    HiveIcebergTestUtils.validateData(table, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 0);
  }

  @Test
  public void testMultiTableInsert() throws IOException {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    Schema target1Schema = new Schema(
        optional(1, "customer_id", Types.LongType.get()),
        optional(2, "first_name", Types.StringType.get())
    );

    Schema target2Schema = new Schema(
        optional(1, "last_name", Types.StringType.get()),
        optional(2, "customer_id", Types.LongType.get())
    );

    List<Record> target1Records = TestHelper.RecordsBuilder.newInstance(target1Schema)
        .add(0L, "Alice")
        .add(1L, "Bob")
        .add(2L, "Trudy")
        .build();

    List<Record> target2Records = TestHelper.RecordsBuilder.newInstance(target2Schema)
        .add("Brown", 0L)
        .add("Green", 1L)
        .add("Pink", 2L)
        .build();

    Table target1 = testTables.createTable(shell, "target1", target1Schema, fileFormat, ImmutableList.of());
    Table target2 = testTables.createTable(shell, "target2", target2Schema, fileFormat, ImmutableList.of());

    // simple insert: should create a single vertex writing to both target tables
    shell.executeStatement("FROM customers " +
        "INSERT INTO target1 SELECT customer_id, first_name " +
        "INSERT INTO target2 SELECT last_name, customer_id");

    // Check that everything is as expected
    HiveIcebergTestUtils.validateData(target1, target1Records, 0);
    HiveIcebergTestUtils.validateData(target2, target2Records, 1);

    // truncate the target tables
    testTables.truncateIcebergTable(target1);
    testTables.truncateIcebergTable(target2);

    // complex insert: should use a different vertex for each target table
    shell.executeStatement("FROM customers " +
        "INSERT INTO target1 SELECT customer_id, first_name ORDER BY first_name " +
        "INSERT INTO target2 SELECT last_name, customer_id ORDER BY last_name");

    // Check that everything is as expected
    HiveIcebergTestUtils.validateData(target1, target1Records, 0);
    HiveIcebergTestUtils.validateData(target2, target2Records, 1);
  }

  @Test
  public void testWriteWithDefaultWriteFormat() {
    Assume.assumeTrue("Testing the default file format is enough for a single scenario.",
        testTableType == TestTables.TestTableType.HIVE_CATALOG && fileFormat == FileFormat.ORC);

    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    // create Iceberg table without specifying a write format in the tbl properties
    // it should fall back to using the default file format
    shell.executeStatement(String.format("CREATE EXTERNAL TABLE %s (id bigint, name string) STORED BY '%s' %s %s",
        identifier,
        HiveIcebergStorageHandler.class.getName(),
        testTables.locationForCreateTableSQL(identifier),
        testTables.propertiesForCreateTableSQL(ImmutableMap.of())));

    shell.executeStatement(String.format("INSERT INTO %s VALUES (10, 'Linda')", identifier));
    List<Object[]> results = shell.executeStatement(String.format("SELECT * FROM %s", identifier));

    Assert.assertEquals(1, results.size());
    Assert.assertEquals(10L, results.get(0)[0]);
    Assert.assertEquals("Linda", results.get(0)[1]);
  }

  @Test
  public void testInsertEmptyResultSet() throws IOException {
    Table source = testTables.createTable(shell, "source", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, ImmutableList.of());
    Table target = testTables.createTable(shell, "target", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, ImmutableList.of());

    shell.executeStatement("INSERT INTO target SELECT * FROM source");
    HiveIcebergTestUtils.validateData(target, ImmutableList.of(), 0);

    testTables.appendIcebergTable(shell.getHiveConf(), source, fileFormat, null,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    shell.executeStatement("INSERT INTO target SELECT * FROM source WHERE first_name = 'Nobody'");
    HiveIcebergTestUtils.validateData(target, ImmutableList.of(), 0);
  }
}
