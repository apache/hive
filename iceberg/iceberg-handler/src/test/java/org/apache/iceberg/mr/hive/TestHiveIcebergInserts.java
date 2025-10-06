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
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
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

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.NullOrder.NULLS_LAST;
import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Runs insert queries against Iceberg tables. These should cover the various insertion methods of Hive including:
 * inserting values, inserting from select subqueries, insert overwrite table, etc...
 */
public class TestHiveIcebergInserts extends HiveIcebergStorageHandlerWithEngineBase {

  @Test
  public void testSortedInsert() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("default", "sort_table");

    Schema schema = new Schema(
        optional(1, "id", Types.IntegerType.get(), "unique ID"),
        optional(2, "data", Types.StringType.get())
    );
    SortOrder order = SortOrder.builderFor(schema)
        .asc("id", NULLS_FIRST)
        .desc("data", NULLS_LAST)
        .build();

    testTables.createTable(shell, identifier.name(), schema, order, PartitionSpec.unpartitioned(), fileFormat,
        ImmutableList.of(), formatVersion, ImmutableMap.of());
    shell.executeStatement(String.format("INSERT INTO TABLE %s VALUES (4, 'a'), (1, 'a'), (3, 'a'), (2, 'a'), " +
            "(null, 'a'), (3, 'b'), (3, null)", identifier.name()));

    List<Record> expected = TestHelper.RecordsBuilder.newInstance(schema)
        .add(null, "a").add(1, "a").add(2, "a").add(3, "b").add(3, "a").add(3, null).add(4, "a")
        .build();
    List<Object[]> result = shell.executeStatement(String.format("SELECT * FROM %s", identifier.name()));
    HiveIcebergTestUtils.validateData(expected, HiveIcebergTestUtils.valueForRow(schema, result));
  }

  @Test
  public void testSortedAndTransformedInsert() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("default", "sort_table");

    SortOrder order = SortOrder.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .asc(bucket("customer_id", 2), NULLS_FIRST)
        .desc(truncate("first_name", 4), NULLS_LAST)
        .asc("last_name", NULLS_LAST)
        .build();

    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, order,
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of(), formatVersion, ImmutableMap.of());

    StringBuilder insertQuery = new StringBuilder().append(String.format("INSERT INTO %s VALUES ", identifier.name()));
    HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2.forEach(record -> insertQuery.append("(")
        .append(record.get(0)).append(",'")
        .append(record.get(1)).append("','")
        .append(record.get(2)).append("'),"));
    insertQuery.setLength(insertQuery.length() - 1);

    shell.executeStatement(insertQuery.toString());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(2L, "Susan", "Morrison").add(1L, "Sharon", "Taylor").add(1L, "Joanna", "Pierce")
        .add(2L, "Joanna", "Silver").add(2L, "Jake", "Donnel").add(2L, "Bob", "Silver").add(3L, "Trudy", "Henderson")
        .add(3L, "Trudy", "Johnson").add(3L, "Blake", "Burr").build();
    List<Object[]> result = shell.executeStatement(String.format("SELECT * FROM %s", identifier.name()));
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, result));
  }

  @Test
  public void testSortedAndTransformedInsertIntoPartitionedTable() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("default", "tbl_bucketed");
    Schema schema = new Schema(
        optional(1, "a", Types.IntegerType.get()),
        optional(2, "b", Types.StringType.get()),
        optional(3, "c", Types.IntegerType.get())
    );
    SortOrder order = SortOrder.builderFor(schema)
        .desc("c", NULLS_FIRST)
        .asc(truncate("b", 1))
        .build();
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
        .bucket("b", 2)
        .build();
    testTables.createTable(shell, identifier.name(), schema, order, partitionSpec, fileFormat, ImmutableList.of(),
        formatVersion, ImmutableMap.of());
    shell.executeStatement(String.format("INSERT INTO %s VALUES (1, 'EUR', 10), (5, 'HUF', 30), (2, 'EUR', 10), " +
        "(8, 'PLN', 20), (6, 'USD', null)", identifier.name()));
    List<Object[]> result = shell.executeStatement(String.format("SELECT * FROM %s", identifier.name()));
    List<Record> expected =
        TestHelper.RecordsBuilder.newInstance(schema).add(1, "EUR", 10).add(2, "EUR", 10).add(6, "USD", null)
            .add(5, "HUF", 30).add(8, "PLN", 20).build();
    HiveIcebergTestUtils.validateData(expected, HiveIcebergTestUtils.valueForRow(schema, result));
  }

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
  public void testInsertIntoORCFile() throws IOException {
    Assume.assumeTrue("Testing the create table ... stored as ORCFILE syntax is enough for a single scenario.",
        testTableType == TestTables.TestTableType.HIVE_CATALOG && fileFormat == FileFormat.ORC);
    shell.executeStatement("CREATE TABLE t2(c0 DOUBLE , c1 DOUBLE , c2 DECIMAL) STORED BY " +
        "ICEBERG STORED AS ORCFILE");
    shell.executeStatement("INSERT INTO t2(c1, c0) VALUES(0.1803113419993464, 0.9381388537256228)");
    List<Object[]> results = shell.executeStatement("SELECT * FROM t2");
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(0.9381388537256228, results.get(0)[0]);
    Assert.assertEquals(0.1803113419993464, results.get(0)[1]);
    Assert.assertEquals(null, results.get(0)[2]);
  }


  @Test
  public void testStoredByIcebergInTextFile() {
    Assume.assumeTrue("Testing the create table ... stored as TEXTFILE syntax is enough for a single scenario.",
        testTableType == TestTables.TestTableType.HIVE_CATALOG && fileFormat == FileFormat.ORC);
    AssertHelpers.assertThrows("Create table should not work with textfile", IllegalArgumentException.class,
        "Unsupported fileformat",
        () ->
            shell.executeStatement("CREATE TABLE IF NOT EXISTS t2(c0 DOUBLE , c1 DOUBLE , c2 DECIMAL) STORED BY " +
                "ICEBERG STORED AS TEXTFILE"));
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
      if (type == Types.BinaryType.get() || type.equals(Types.FixedType.ofLength(5))) {
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
    List<Record> records = Lists.newArrayList(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
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
    List<Record> expected = Lists.newArrayList(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
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

    expected = Lists.newArrayList(newRecords);
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

  @Test
  public void testInsertOverwriteWithPartitionEvolutionThrowsError() throws IOException {
    TableIdentifier target = TableIdentifier.of("default", "target");
    Table table = testTables.createTable(shell, target.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    shell.executeStatement("ALTER TABLE target SET PARTITION SPEC(TRUNCATE(2, last_name))");
    List<Record> newRecords = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(0L, "Mike", "Taylor")
        .add(1L, "Christy", "Hubert")
        .build();
    AssertHelpers.assertThrows("IOW should not work on tables with partition evolution",
        IllegalArgumentException.class,
        "Cannot perform insert overwrite query on Iceberg table where partition evolution happened.",
        () -> shell.executeStatement(testTables.getInsertQuery(newRecords, target, true)));
    // TODO: we should add additional test cases after merge + compaction is supported in hive that allows us to
    // rewrite the data
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
    List<Record> records = Lists.newArrayList(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
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
  public void testStructMapWithNull() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "mapofstructs", Types.MapType.ofRequired(3, 4, Types.StringType.get(),
            Types.StructType.of(required(5, "something", Types.StringType.get()),
                required(6, "someone", Types.StringType.get()),
                required(7, "somewhere", Types.StringType.get())
            ))
        )
    );

    List<Record> records = TestHelper.RecordsBuilder.newInstance(schema)
        .add(0L, ImmutableMap.of())
        .build();

    testTables.createTable(shell, "mapwithnull", schema, fileFormat, records);

    List<Object[]> results = shell.executeStatement("select mapofstructs['context'].someone FROM mapwithnull");
    Assert.assertEquals(1, results.size());
    Assert.assertNull(results.get(0)[0]);
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

  @Test
  public void testInsertOverwriteOnEmptyV1Table() throws IOException {
    TableIdentifier target = TableIdentifier.of("default", "target");
    Table table = testTables.createTable(shell, target.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // Insert some data.
    List<Record> newRecords = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(0L, "ABC", "DBAK")
        .add(1L, "XYZ", "RDBS")
        .build();
    shell.executeStatement(testTables.getInsertQuery(newRecords, target, false));

    // Change the partition and then insert some more data.
    shell.executeStatement("ALTER TABLE target SET PARTITION SPEC(TRUNCATE(2, last_name))");
    newRecords = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(2L, "Mike", "Taylor")
        .add(3L, "Christy", "Hubert")
        .build();

    shell.executeStatement(testTables.getInsertQuery(newRecords, target, false));

    // Truncate the table
    shell.executeStatement("TRUNCATE TABLE target");

    newRecords = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(0L, "Mike", "Taylor")
        .add(3L, "ABC", "DBAK")
        .add(4L, "APL", "DBAM")
        .build();

    // Do an insert-overwrite and this should be successful because the table is empty.
    shell.executeStatement(testTables.getInsertQuery(newRecords, target, true));

    HiveIcebergTestUtils.validateData(table, newRecords, 0);
  }

  @Test
  public void testInsertOverwriteOnEmptyV2Table() throws IOException {
    // Create a V2 table with merge-on-read.
    TableIdentifier target = TableIdentifier.of("default", "target");
    Map<String, String> opTypes = ImmutableMap.of(
        TableProperties.DELETE_MODE, HiveIcebergStorageHandler.MERGE_ON_READ,
        TableProperties.MERGE_MODE, HiveIcebergStorageHandler.MERGE_ON_READ,
        TableProperties.UPDATE_MODE, HiveIcebergStorageHandler.MERGE_ON_READ);

    Table table = testTables.createTable(shell, target.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2, opTypes);

    // Insert some data.
    List<Record> newRecords = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(3L, "ABC", "DBAK")
        .add(4L, "XYZ", "RDBS")
        .add(5L, "CBO", "HIVE")
        .add(6L, "HADOOP", "HDFS")
        .build();
    shell.executeStatement(testTables.getInsertQuery(newRecords, target, false));

    // Perform some deletes & updates.
    shell.executeStatement("update target set first_name='WXYZ' where customer_id=1");
    shell.executeStatement("delete from target where customer_id%2=0");

    // Change the partition and then insert some more data.
    shell.executeStatement("ALTER TABLE target SET PARTITION SPEC(TRUNCATE(2, last_name))");
    newRecords = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(7L, "Mike", "Taylor")
        .add(8L, "Christy", "Hubert")
        .add(9L, "RDBMS", "Talk")
        .add(10L, "Notification", "Hub")
        .add(11L, "Vector", "HDFS")
        .build();
    shell.executeStatement(testTables.getInsertQuery(newRecords, target, false));

    // Perform some deletes & updates.
    shell.executeStatement("update target set first_name='RDBMSV2' where customer_id=9");
    shell.executeStatement("delete from target where customer_id%2=0 AND customer_id>6");

    Table icebergTable = testTables.loadTable(target);
    // There should be some delete files, due to our delete & update operations.
    Assert.assertNotEquals("0", icebergTable.currentSnapshot().summary().get(SnapshotSummary.TOTAL_DELETE_FILES_PROP));

    long preTruncateSnapshotId = icebergTable.currentSnapshot().snapshotId();
    List<Object[]> result =
        shell.executeStatement(String.format("SELECT * FROM %s order by customer_id", target.name()));

    // Truncate the table
    shell.executeStatement("TRUNCATE TABLE target");

    // Do an insert-overwrite and this should be successful because the table is empty.
    shell.executeStatement(
        "INSERT OVERWRITE TABLE target select * from target FOR SYSTEM_VERSION AS OF " + preTruncateSnapshotId);

    HiveIcebergTestUtils.validateData(table,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, result), 0);
    icebergTable = testTables.loadTable(target);

    // There should be no delete files, they should have been merged with the data files
    Assert.assertEquals("0", icebergTable.currentSnapshot().summary().get(SnapshotSummary.TOTAL_DELETE_FILES_PROP));
  }
}
