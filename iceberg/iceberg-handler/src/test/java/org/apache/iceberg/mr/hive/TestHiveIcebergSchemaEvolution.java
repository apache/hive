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
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Tests that schema changes of Iceberg backed Hive tables are correctly reflected in subsequent select statements.
 * Verification is done from both ends:
 *  - when ALTER TABLE statements were initiated from Hive
 *  - when schema changes were originally done on the Iceberg table
 */
public class TestHiveIcebergSchemaEvolution extends HiveIcebergStorageHandlerWithEngineBase {

  @Test
  public void testDescribeTable() throws IOException {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    List<Object[]> rows = shell.executeStatement("DESCRIBE default.customers");
    Assert.assertEquals(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA.columns().size(), rows.size());
    for (int i = 0; i < HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA.columns().size(); i++) {
      Types.NestedField field = HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA.columns().get(i);
      String comment = field.doc() == null ? "" : field.doc();
      Assert.assertArrayEquals(new Object[] {field.name(), HiveSchemaUtil.convert(field.type()).getTypeName(),
          comment}, rows.get(i));
    }
  }

  @Test
  public void testAlterChangeColumn() throws IOException {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    shell.executeStatement("ALTER TABLE customers CHANGE COLUMN last_name family_name string AFTER customer_id");

    List<Object[]> result = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id");
    Assert.assertEquals(3, result.size());
    Assert.assertArrayEquals(new Object[]{0L, "Brown", "Alice"}, result.get(0));
    Assert.assertArrayEquals(new Object[]{1L, "Green", "Bob"}, result.get(1));
    Assert.assertArrayEquals(new Object[]{2L, "Pink", "Trudy"}, result.get(2));

    shell.executeStatement("ALTER TABLE customers CHANGE COLUMN family_name family_name string FIRST");

    result = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id");
    Assert.assertEquals(3, result.size());
    Assert.assertArrayEquals(new Object[]{"Brown", 0L, "Alice"}, result.get(0));
    Assert.assertArrayEquals(new Object[]{"Green", 1L, "Bob"}, result.get(1));
    Assert.assertArrayEquals(new Object[]{"Pink", 2L, "Trudy"}, result.get(2));

    result = shell.executeStatement("SELECT customer_id, family_name FROM customers ORDER BY customer_id");
    Assert.assertEquals(3, result.size());
    Assert.assertArrayEquals(new Object[]{0L, "Brown"}, result.get(0));
    Assert.assertArrayEquals(new Object[]{1L, "Green"}, result.get(1));
    Assert.assertArrayEquals(new Object[]{2L, "Pink"}, result.get(2));

  }

  @Test
  public void testColumnReorders() throws IOException {
    Schema schema = new Schema(
        required(1, "a", Types.LongType.get()),
        required(2, "b", Types.StringType.get()),
        required(3, "c", Types.StringType.get()),
        required(4, "d", Types.IntegerType.get()),
        required(5, "e", Types.IntegerType.get()),
        required(6, "f", Types.StringType.get())
    );
    testTables.createTable(shell, "customers", schema, fileFormat, ImmutableList.of());
    shell.executeStatement("INSERT INTO customers VALUES (1, 'foo', 'bar', 33, 44, 'baz'), " +
        "(2, 'foo2', 'bar2', 55, 66, 'baz2')");

    // move one position to the right
    // a,b,c,d,e,f -> b,a,c,d,e,f
    shell.executeStatement("ALTER TABLE customers CHANGE COLUMN a a bigint AFTER b");
    List<Object[]> result = shell.executeStatement("SELECT * FROM customers ORDER BY a");
    Assert.assertEquals(2, result.size());
    Assert.assertArrayEquals(new Object[]{"foo", 1L, "bar", 33, 44, "baz"}, result.get(0));
    Assert.assertArrayEquals(new Object[]{"foo2", 2L, "bar2", 55, 66, "baz2"}, result.get(1));

    // move first to the last
    // b,a,c,d,e,f -> a,c,d,e,f,b
    shell.executeStatement("ALTER TABLE customers CHANGE COLUMN b b string AFTER f");
    result = shell.executeStatement("SELECT * FROM customers ORDER BY a");
    Assert.assertEquals(2, result.size());
    Assert.assertArrayEquals(new Object[]{1L, "bar", 33, 44, "baz", "foo"}, result.get(0));
    Assert.assertArrayEquals(new Object[]{2L, "bar2", 55, 66, "baz2", "foo2"}, result.get(1));

    // move middle to the first
    // a,c,d,e,f,b -> e,a,c,d,f,b
    shell.executeStatement("ALTER TABLE customers CHANGE COLUMN e e int FIRST");
    result = shell.executeStatement("SELECT * FROM customers ORDER BY a");
    Assert.assertEquals(2, result.size());
    Assert.assertArrayEquals(new Object[]{44, 1L, "bar", 33, "baz", "foo"}, result.get(0));
    Assert.assertArrayEquals(new Object[]{66, 2L, "bar2", 55, "baz2", "foo2"}, result.get(1));

    // move one position to the left
    // e,a,c,d,f,b -> e,a,d,c,f,b
    shell.executeStatement("ALTER TABLE customers CHANGE COLUMN d d int AFTER a");
    result = shell.executeStatement("SELECT * FROM customers ORDER BY a");
    Assert.assertEquals(2, result.size());
    Assert.assertArrayEquals(new Object[]{44, 1L, 33, "bar", "baz", "foo"}, result.get(0));
    Assert.assertArrayEquals(new Object[]{66, 2L, 55, "bar2", "baz2", "foo2"}, result.get(1));
  }

  // Tests CHANGE COLUMN feature similarly like above, but with a more complex schema, aimed to verify vectorized
  // reads support the feature properly, also combining with other schema changes e.g. ADD COLUMN
  @Test
  public void testSchemaEvolutionOnVectorizedReads() throws Exception {
    // Currently only ORC, but in the future this should run against each fileformat with vectorized read support.
    Assume.assumeTrue("Vectorized reads only.", isVectorized);

    Schema orderSchema = new Schema(
        optional(1, "order_id", Types.IntegerType.get()),
        optional(2, "customer_first_name", Types.StringType.get()),
        optional(3, "customer_last_name", Types.StringType.get()),
        optional(4, "quantity", Types.IntegerType.get()),
        optional(5, "price", Types.IntegerType.get()),
        optional(6, "item", Types.StringType.get())
    );

    List<Record> records = TestHelper.RecordsBuilder.newInstance(orderSchema)
        .add(1, "Doctor", "Strange", 100, 3, "apple")
        .add(2, "Tony", "Stark", 150, 2, "apple")
        .add(3, "Tony", "Stark", 200, 6, "orange")
        .add(4, "Steve", "Rogers", 100, 8, "banana")
        .add(5, "Doctor", "Strange", 800, 7, "orange")
        .add(6, "Thor", "Odinson", 650, 3, "apple")
        .build();

    testTables.createTable(shell, "orders", orderSchema, fileFormat, records);

    // Reorder columns and rename one column
    shell.executeStatement("ALTER TABLE orders CHANGE COLUMN " +
        "customer_first_name customer_first_name string AFTER customer_last_name");
    shell.executeStatement("ALTER TABLE orders CHANGE COLUMN " +
        "quantity quantity int AFTER price");
    shell.executeStatement("ALTER TABLE orders CHANGE COLUMN " +
        "item fruit string");
    List<Object[]> result = shell.executeStatement("SELECT customer_first_name, customer_last_name, SUM(quantity) " +
        "FROM orders where price >= 3 group by customer_first_name, customer_last_name order by customer_first_name");

    assertQueryResult(result, 4,
        "Doctor", "Strange", 900L,
        "Steve", "Rogers", 100L,
        "Thor", "Odinson", 650L,
        "Tony", "Stark", 200L);


    // Adding a new column (will end up as last col of the schema)
    shell.executeStatement("ALTER TABLE orders ADD COLUMNS (nickname string)");
    shell.executeStatement("INSERT INTO orders VALUES (7, 'Romanoff', 'Natasha', 3, 250, 'apple', 'Black Widow')");
    result = shell.executeStatement("SELECT customer_first_name, customer_last_name, nickname, SUM(quantity) " +
        " FROM orders where price >= 3 group by customer_first_name, customer_last_name, nickname " +
        " order by customer_first_name");
    assertQueryResult(result, 5,
        "Doctor", "Strange", null, 900L,
        "Natasha", "Romanoff", "Black Widow", 250L,
        "Steve", "Rogers", null, 100L,
        "Thor", "Odinson", null, 650L,
        "Tony", "Stark", null, 200L);

    // Re-order newly added column (move it away from being the last column)
    shell.executeStatement("ALTER TABLE orders CHANGE COLUMN fruit fruit string AFTER nickname");
    result = shell.executeStatement("SELECT customer_first_name, customer_last_name, nickname, fruit, SUM(quantity) " +
        " FROM orders where price >= 3 and fruit < 'o' group by customer_first_name, customer_last_name, nickname, " +
        "fruit order by customer_first_name");
    assertQueryResult(result, 4,
        "Doctor", "Strange", null, "apple", 100L,
        "Natasha", "Romanoff", "Black Widow", "apple", 250L,
        "Steve", "Rogers", null, "banana", 100L,
        "Thor", "Odinson", null, "apple", 650L);

    // Rename newly added column (+ reading with different file includes)
    shell.executeStatement("ALTER TABLE orders CHANGE COLUMN nickname nick string");
    result = shell.executeStatement("SELECT customer_first_name, nick, SUM(quantity) " +
        " FROM orders where fruit < 'o'and nick IS NOT NULL group by customer_first_name, nick");
    assertQueryResult(result, 1, "Natasha", "Black Widow", 250L);

    // Re-order between different types
    shell.executeStatement("ALTER TABLE orders CHANGE COLUMN order_id order_id int AFTER customer_first_name");
    result = shell.executeStatement("SELECT customer_first_name, nick, SUM(quantity), MIN(order_id) " +
        " FROM orders where fruit < 'o'and nick IS NOT NULL group by customer_first_name, nick");
    assertQueryResult(result, 1, "Natasha", "Black Widow", 250L, 7);

    // Drop columns via REPLACE COLUMNS
    shell.executeStatement("ALTER TABLE orders REPLACE COLUMNS (" +
        "customer_last_name string, order_id int, quantity int, nick string, fruit string)");

    result = shell.executeStatement("DESCRIBE orders");
    assertQueryResult(result, 5,
        "customer_last_name", "string", "",
        "order_id", "int", "",
        "quantity", "int", "",
        "nick", "string", "",
        "fruit", "string", "");
    result = shell.executeStatement("SELECT * FROM orders ORDER BY order_id");
    assertQueryResult(result, 7,
        "Strange", 1, 100, null, "apple",
        "Stark", 2, 150, null, "apple",
        "Stark", 3, 200, null, "orange",
        "Rogers", 4, 100, null, "banana",
        "Strange", 5, 800, null, "orange",
        "Odinson", 6, 650, null, "apple",
        "Romanoff", 7, 250, "Black Widow", "apple");

  }

  private static void assertQueryResult(List<Object[]> result, int expectedCount, Object... expectedRows) {
    Assert.assertEquals(expectedCount, result.size());
    int colCount = expectedRows.length / expectedCount;
    for (int i = 0; i < expectedCount; ++i) {
      Object[] rows = new Object[colCount];
      for (int j = 0; j < colCount; ++j) {
        rows[j] = expectedRows[i * colCount + j];
      }
      Assert.assertArrayEquals(rows, result.get(i));
    }
  }

  @Test
  public void testAddColumnToIcebergTable() throws IOException {
    // Create an Iceberg table with the columns customer_id, first_name and last_name with some initial data.
    Table icebergTable = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // Add a new column (age long) to the Iceberg table.
    icebergTable.updateSchema().addColumn("age", Types.LongType.get()).commit();
    if (testTableType != TestTables.TestTableType.HIVE_CATALOG) {
      // We need to update columns for non-Hive catalogs
      shell.executeStatement("ALTER TABLE customers UPDATE COLUMNS");
    }

    Schema customerSchemaWithAge = new Schema(optional(1, "customer_id", Types.LongType.get()),
        optional(2, "first_name", Types.StringType.get(), "This is first name"),
        optional(3, "last_name", Types.StringType.get(), "This is last name"),
        optional(4, "age", Types.LongType.get()));

    // Also add a new entry to the table where the age column is set.
    icebergTable = testTables.loadTable(TableIdentifier.of("default", "customers"));
    List<Record> newCustomerWithAge = TestHelper.RecordsBuilder.newInstance(customerSchemaWithAge)
        .add(3L, "James", "Red", 34L).add(4L, "Lily", "Blue", null).build();
    testTables.appendIcebergTable(shell.getHiveConf(), icebergTable, fileFormat, null, newCustomerWithAge);

    // Do a 'select *' from Hive and check if the age column appears in the result.
    // It should be null for the old data and should be filled for the data added after the column addition.
    TestHelper.RecordsBuilder customersWithAgeBuilder = TestHelper.RecordsBuilder.newInstance(customerSchemaWithAge)
        .add(0L, "Alice", "Brown", null).add(1L, "Bob", "Green", null).add(2L, "Trudy", "Pink", null)
        .add(3L, "James", "Red", 34L).add(4L, "Lily", "Blue", null);
    List<Record> customersWithAge = customersWithAgeBuilder.build();

    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithAge, HiveIcebergTestUtils.valueForRow(customerSchemaWithAge, rows),
        0);

    // Do a 'select customer_id, age' from Hive to check if the new column can be queried from Hive.
    // The customer_id is needed because of the result sorting.
    Schema customerSchemaWithAgeOnly =
        new Schema(optional(1, "customer_id", Types.LongType.get()), optional(4, "age", Types.LongType.get()));
    TestHelper.RecordsBuilder customerWithAgeOnlyBuilder = TestHelper.RecordsBuilder
        .newInstance(customerSchemaWithAgeOnly).add(0L, null).add(1L, null).add(2L, null).add(3L, 34L).add(4L, null);
    List<Record> customersWithAgeOnly = customerWithAgeOnlyBuilder.build();

    rows = shell.executeStatement("SELECT customer_id, age FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithAgeOnly,
        HiveIcebergTestUtils.valueForRow(customerSchemaWithAgeOnly, rows), 0);

    // Insert some data with age column from Hive. Insert an entry with null age and an entry with filled age.
    shell.executeStatement(
        "INSERT INTO default.customers values (5L, 'Lily', 'Magenta', NULL), (6L, 'Roni', 'Purple', 23L)");

    customersWithAgeBuilder.add(5L, "Lily", "Magenta", null).add(6L, "Roni", "Purple", 23L);
    customersWithAge = customersWithAgeBuilder.build();
    rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithAge, HiveIcebergTestUtils.valueForRow(customerSchemaWithAge, rows),
        0);

    customerWithAgeOnlyBuilder.add(5L, null).add(6L, 23L);
    customersWithAgeOnly = customerWithAgeOnlyBuilder.build();
    rows = shell.executeStatement("SELECT customer_id, age FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithAgeOnly,
        HiveIcebergTestUtils.valueForRow(customerSchemaWithAgeOnly, rows), 0);
  }

  @Test
  public void testAddRequiredColumnToIcebergTable() throws IOException {
    // Create an Iceberg table with the columns customer_id, first_name and last_name without initial data.
    // The reason why not to add initial data is that adding a required column is an incompatible change in Iceberg.
    // So there is no contract on what happens when trying to read the old data back. It behaves differently depending
    // on the underlying file format. So there is no point creating a test for that as there is no expected behaviour.
    Table icebergTable = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, null);

    // Add a new required column (age long) to the Iceberg table.
    icebergTable.updateSchema().allowIncompatibleChanges().addRequiredColumn("age", Types.LongType.get()).commit();
    if (testTableType != TestTables.TestTableType.HIVE_CATALOG) {
      // We need to update columns for non-Hive catalogs
      shell.executeStatement("ALTER TABLE customers UPDATE COLUMNS");
    }

    Schema customerSchemaWithAge = new Schema(optional(1, "customer_id", Types.LongType.get()),
        optional(2, "first_name", Types.StringType.get(), "This is first name"),
        optional(3, "last_name", Types.StringType.get(), "This is last name"),
        required(4, "age", Types.LongType.get()));

    // Insert some data with age column from Hive.
    shell.executeStatement(
        "INSERT INTO default.customers values (0L, 'Lily', 'Magenta', 28L), (1L, 'Roni', 'Purple', 33L)");

    // Do a 'select *' from Hive and check if the age column appears in the result.
    List<Record> customersWithAge = TestHelper.RecordsBuilder.newInstance(customerSchemaWithAge)
        .add(0L, "Lily", "Magenta", 28L).add(1L, "Roni", "Purple", 33L).build();
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithAge, HiveIcebergTestUtils.valueForRow(customerSchemaWithAge, rows),
        0);

    // Should add test step to insert NULL value into the new required column. But at the moment it
    // works inconsistently for different file types, so leave it for later when this behaviour is cleaned up.
  }

  @Test
  public void testAddColumnIntoStructToIcebergTable() throws IOException {
    // Create an Iceberg table with the columns id and person, where person is a struct, consists of the
    // columns first_name and last_name.
    Schema schema = new Schema(required(1, "id", Types.LongType.get()), required(2, "person", Types.StructType
        .of(required(3, "first_name", Types.StringType.get()), required(4, "last_name", Types.StringType.get()))));
    List<Record> people = TestHelper.generateRandomRecords(schema, 3, 0L);
    Table icebergTable = testTables.createTable(shell, "people", schema, fileFormat, people);

    // Add a new column (age long) to the Iceberg table into the person struct
    icebergTable.updateSchema().addColumn("person", "age", Types.LongType.get()).commit();

    Schema schemaWithAge = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "person", Types.StructType.of(required(3, "first_name", Types.StringType.get()),
            required(4, "last_name", Types.StringType.get()), optional(5, "age", Types.LongType.get()))));
    List<Record> newPeople = TestHelper.generateRandomRecords(schemaWithAge, 2, 10L);

    // Also add a new entry to the table where the age column is set.
    icebergTable = testTables.loadTable(TableIdentifier.of("default", "people"));
    testTables.appendIcebergTable(shell.getHiveConf(), icebergTable, fileFormat, null, newPeople);

    List<Record> sortedExpected = Lists.newArrayList(people);
    sortedExpected.addAll(newPeople);
    sortedExpected.sort(Comparator.comparingLong(record -> (Long) record.get(0)));
    List<Object[]> rows = shell
        .executeStatement("SELECT id, person.first_name, person.last_name, person.age FROM default.people order by id");
    Assert.assertEquals(sortedExpected.size(), rows.size());
    for (int i = 0; i < sortedExpected.size(); i++) {
      Object[] row = rows.get(i);
      Long id = (Long) sortedExpected.get(i).get(0);
      Record person = (Record) sortedExpected.get(i).getField("person");
      String lastName = (String) person.getField("last_name");
      String firstName = (String) person.getField("first_name");
      Long age = null;
      if (person.getField("age") != null) {
        age = (Long) person.getField("age");
      }
      Assert.assertEquals(id, (Long) row[0]);
      Assert.assertEquals(firstName, (String) row[1]);
      Assert.assertEquals(lastName, (String) row[2]);
      Assert.assertEquals(age, row[3]);
    }

    // Insert some data with age column from Hive. Insert an entry with null age and an entry with filled age.
    shell.executeStatement("CREATE TABLE dummy_tbl (id bigint, first_name string, last_name string, age bigint)");
    shell.executeStatement("INSERT INTO dummy_tbl VALUES (1, 'Lily', 'Blue', 34), (2, 'Roni', 'Grey', NULL)");
    shell.executeStatement("INSERT INTO default.people SELECT id, named_struct('first_name', first_name, " +
        "'last_name', last_name, 'age', age) from dummy_tbl");

    rows = shell.executeStatement("SELECT id, person.first_name, person.last_name, person.age FROM default.people " +
        "where id in (1, 2) order by id");
    Assert.assertEquals(2, rows.size());
    Assert.assertEquals((Long) 1L, (Long) rows.get(0)[0]);
    Assert.assertEquals("Lily", (String) rows.get(0)[1]);
    Assert.assertEquals("Blue", (String) rows.get(0)[2]);
    Assert.assertEquals((Long) 34L, (Long) rows.get(0)[3]);
    Assert.assertEquals((Long) 2L, (Long) rows.get(1)[0]);
    Assert.assertEquals("Roni", (String) rows.get(1)[1]);
    Assert.assertEquals("Grey", (String) rows.get(1)[2]);
    Assert.assertNull(rows.get(1)[3]);
  }

  @Test
  public void testMakeColumnRequiredInIcebergTable() throws IOException {
    // Create an Iceberg table with the columns customer_id, first_name and last_name with some initial data.
    Table icebergTable = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // Add a new required column (age long) to the Iceberg table.
    icebergTable.updateSchema().allowIncompatibleChanges().requireColumn("last_name").commit();

    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, rows), 0);

    // Insert some data with last_name no NULL.
    shell.executeStatement("INSERT INTO default.customers values (3L, 'Lily', 'Magenta'), (4L, 'Roni', 'Purple')");

    List<Record> customerRecords = TestHelper.RecordsBuilder
        .newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA).add(0L, "Alice", "Brown")
        .add(1L, "Bob", "Green").add(2L, "Trudy", "Pink").add(3L, "Lily", "Magenta").add(4L, "Roni", "Purple").build();

    rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(customerRecords,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, rows), 0);

    // Should add test step to insert NULL value into the new required column. But at the moment it
    // works inconsistently for different file types, so leave it for later when this behaviour is cleaned up.
  }

  @Test
  public void testRemoveColumnFromIcebergTable() throws IOException {
    // Create an Iceberg table with the columns customer_id, first_name and last_name with some initial data.
    Table icebergTable = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // Remove the first_name column from the table.
    icebergTable.updateSchema().deleteColumn("first_name").commit();

    Schema customerSchemaWithoutFirstName = new Schema(optional(1, "customer_id", Types.LongType.get()),
        optional(2, "last_name", Types.StringType.get(), "This is last name"));

    TestHelper.RecordsBuilder customersWithoutFirstNameBuilder = TestHelper.RecordsBuilder
        .newInstance(customerSchemaWithoutFirstName).add(0L, "Brown").add(1L, "Green").add(2L, "Pink");
    List<Record> customersWithoutFirstName = customersWithoutFirstNameBuilder.build();

    // Run a 'select *' from Hive to see if the result doesn't contain the first_name column any more.
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithoutFirstName,
        HiveIcebergTestUtils.valueForRow(customerSchemaWithoutFirstName, rows), 0);

    // Run a 'select first_name' and check if an exception is thrown.
    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "Invalid table alias or column reference 'first_name'", () -> {
          shell.executeStatement("SELECT first_name FROM default.customers");
        });

    // Insert an entry from Hive to check if it can be inserted without the first_name column.
    shell.executeStatement("INSERT INTO default.customers values (4L, 'Magenta')");

    rows = shell.executeStatement("SELECT * FROM default.customers");
    customersWithoutFirstNameBuilder.add(4L, "Magenta");
    customersWithoutFirstName = customersWithoutFirstNameBuilder.build();
    HiveIcebergTestUtils.validateData(customersWithoutFirstName,
        HiveIcebergTestUtils.valueForRow(customerSchemaWithoutFirstName, rows), 0);
  }

  @Test
  public void testRemoveAndAddBackColumnFromIcebergTable() throws IOException {
    // Create an Iceberg table with the columns customer_id, first_name and last_name with some initial data.
    Table icebergTable = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // Remove the first_name column
    icebergTable.updateSchema().deleteColumn("first_name").commit();
    // Add a new column with the name first_name
    icebergTable.updateSchema().addColumn("first_name", Types.StringType.get(), "This is new first name").commit();

    // Add new data to the table with the new first_name column filled.
    icebergTable = testTables.loadTable(TableIdentifier.of("default", "customers"));
    Schema customerSchemaWithNewFirstName = new Schema(optional(1, "customer_id", Types.LongType.get()),
        optional(2, "last_name", Types.StringType.get(), "This is last name"),
        optional(3, "first_name", Types.StringType.get(), "This is the newly added first name"));
    List<Record> newCustomersWithNewFirstName =
        TestHelper.RecordsBuilder.newInstance(customerSchemaWithNewFirstName).add(3L, "Red", "James").build();
    testTables.appendIcebergTable(shell.getHiveConf(), icebergTable, fileFormat, null, newCustomersWithNewFirstName);

    TestHelper.RecordsBuilder customersWithNewFirstNameBuilder =
        TestHelper.RecordsBuilder.newInstance(customerSchemaWithNewFirstName).add(0L, "Brown", null)
            .add(1L, "Green", null).add(2L, "Pink", null).add(3L, "Red", "James");
    List<Record> customersWithNewFirstName = customersWithNewFirstNameBuilder.build();

    // Run a 'select *' from Hive and check if the first_name column is returned.
    // It should be null for the old data and should be filled in the entry added after the column addition.
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithNewFirstName,
        HiveIcebergTestUtils.valueForRow(customerSchemaWithNewFirstName, rows), 0);

    Schema customerSchemaWithNewFirstNameOnly = new Schema(optional(1, "customer_id", Types.LongType.get()),
        optional(3, "first_name", Types.StringType.get(), "This is the newly added first name"));

    TestHelper.RecordsBuilder customersWithNewFirstNameOnlyBuilder = TestHelper.RecordsBuilder
        .newInstance(customerSchemaWithNewFirstNameOnly).add(0L, null).add(1L, null).add(2L, null).add(3L, "James");
    List<Record> customersWithNewFirstNameOnly = customersWithNewFirstNameOnlyBuilder.build();

    // Run a 'select first_name' from Hive to check if the new first-name column can be queried.
    rows = shell.executeStatement("SELECT customer_id, first_name FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithNewFirstNameOnly,
        HiveIcebergTestUtils.valueForRow(customerSchemaWithNewFirstNameOnly, rows), 0);

    // Insert data from Hive with first_name filled and with null first_name value.
    shell.executeStatement("INSERT INTO default.customers values (4L, 'Magenta', 'Lily'), (5L, 'Purple', NULL)");

    // Check if the newly inserted data is returned correctly by select statements.
    customersWithNewFirstNameBuilder.add(4L, "Magenta", "Lily").add(5L, "Purple", null);
    customersWithNewFirstName = customersWithNewFirstNameBuilder.build();
    rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithNewFirstName,
        HiveIcebergTestUtils.valueForRow(customerSchemaWithNewFirstName, rows), 0);

    customersWithNewFirstNameOnlyBuilder.add(4L, "Lily").add(5L, null);
    customersWithNewFirstNameOnly = customersWithNewFirstNameOnlyBuilder.build();
    rows = shell.executeStatement("SELECT customer_id, first_name FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithNewFirstNameOnly,
        HiveIcebergTestUtils.valueForRow(customerSchemaWithNewFirstNameOnly, rows), 0);
  }

  @Test
  public void testRenameColumnInIcebergTable() throws IOException {
    // Create an Iceberg table with the columns customer_id, first_name and last_name with some initial data.
    Table icebergTable = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // Rename the last_name column to family_name
    icebergTable.updateSchema().renameColumn("last_name", "family_name").commit();
    if (testTableType != TestTables.TestTableType.HIVE_CATALOG) {
      // We need to update columns for non-Hive catalogs
      shell.executeStatement("ALTER TABLE customers UPDATE COLUMNS");
    }

    Schema schemaWithFamilyName = new Schema(optional(1, "customer_id", Types.LongType.get()),
        optional(2, "first_name", Types.StringType.get(), "This is first name"),
        optional(3, "family_name", Types.StringType.get(), "This is last name"));

    // Run a 'select *' from Hive to check if the same records are returned in the same order as before the rename.
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(schemaWithFamilyName, rows), 0);

    Schema shemaWithFamilyNameOnly = new Schema(optional(1, "customer_id", Types.LongType.get()),
        optional(2, "family_name", Types.StringType.get(), "This is family name"));
    TestHelper.RecordsBuilder customersWithFamilyNameOnlyBuilder = TestHelper.RecordsBuilder
        .newInstance(shemaWithFamilyNameOnly).add(0L, "Brown").add(1L, "Green").add(2L, "Pink");
    List<Record> customersWithFamilyNameOnly = customersWithFamilyNameOnlyBuilder.build();

    // Run a 'select family_name' from Hive to check if the column can be queried with the new name.
    rows = shell.executeStatement("SELECT customer_id, family_name FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithFamilyNameOnly,
        HiveIcebergTestUtils.valueForRow(shemaWithFamilyNameOnly, rows), 0);

    // Run a 'select last_name' to check if an exception is thrown.
    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "Invalid table alias or column reference 'last_name'", () -> {
          shell.executeStatement("SELECT last_name FROM default.customers");
        });

    // Insert some data from Hive to check if the last_name column is still can be filled.
    shell.executeStatement("INSERT INTO default.customers values (3L, 'Lily', 'Magenta'), (4L, 'Roni', NULL)");

    List<Record> newCustomers = TestHelper.RecordsBuilder.newInstance(schemaWithFamilyName).add(0L, "Alice", "Brown")
        .add(1L, "Bob", "Green").add(2L, "Trudy", "Pink").add(3L, "Lily", "Magenta").add(4L, "Roni", null).build();
    rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(newCustomers, HiveIcebergTestUtils.valueForRow(schemaWithFamilyName, rows), 0);

    customersWithFamilyNameOnlyBuilder.add(3L, "Magenta").add(4L, null);
    customersWithFamilyNameOnly = customersWithFamilyNameOnlyBuilder.build();
    rows = shell.executeStatement("SELECT customer_id, family_name FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithFamilyNameOnly,
        HiveIcebergTestUtils.valueForRow(shemaWithFamilyNameOnly, rows), 0);
  }

  @Test
  public void testMoveLastNameToFirstInIcebergTable() throws IOException {
    // Create an Iceberg table with the columns customer_id, first_name and last_name with some initial data.
    Table icebergTable = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // Move the last_name column in the table schema as first_column
    icebergTable.updateSchema().moveFirst("last_name").commit();

    Schema customerSchemaLastNameFirst =
        new Schema(optional(1, "last_name", Types.StringType.get(), "This is last name"),
            optional(2, "customer_id", Types.LongType.get()),
            optional(3, "first_name", Types.StringType.get(), "This is first name"));

    TestHelper.RecordsBuilder customersWithLastNameFirstBuilder =
        TestHelper.RecordsBuilder.newInstance(customerSchemaLastNameFirst).add("Brown", 0L, "Alice")
            .add("Green", 1L, "Bob").add("Pink", 2L, "Trudy");
    List<Record> customersWithLastNameFirst = customersWithLastNameFirstBuilder.build();

    // Run a 'select *' to check if the order of the column in the result has been changed.
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithLastNameFirst,
        HiveIcebergTestUtils.valueForRow(customerSchemaLastNameFirst, rows), 1);

    // Query the data with names and check if the result is the same as when the table was created.
    rows = shell.executeStatement("SELECT customer_id, first_name, last_name FROM default.customers");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, rows), 0);

    // Insert data from Hive to check if the last_name column has to be first in the values list.
    shell.executeStatement("INSERT INTO default.customers values ('Magenta', 3L, 'Lily')");

    customersWithLastNameFirstBuilder.add("Magenta", 3L, "Lily");
    customersWithLastNameFirst = customersWithLastNameFirstBuilder.build();
    rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithLastNameFirst,
        HiveIcebergTestUtils.valueForRow(customerSchemaLastNameFirst, rows), 1);
  }

  @Test
  public void testMoveLastNameBeforeCustomerIdInIcebergTable() throws IOException {
    // Create an Iceberg table with the columns customer_id, first_name and last_name with some initial data.
    Table icebergTable = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // Move the last_name column before the customer_id in the table schema.
    icebergTable.updateSchema().moveBefore("last_name", "customer_id").commit();

    Schema customerSchemaLastNameFirst =
        new Schema(optional(1, "last_name", Types.StringType.get(), "This is last name"),
            optional(2, "customer_id", Types.LongType.get()),
            optional(3, "first_name", Types.StringType.get(), "This is first name"));

    TestHelper.RecordsBuilder customersWithLastNameFirstBuilder =
        TestHelper.RecordsBuilder.newInstance(customerSchemaLastNameFirst).add("Brown", 0L, "Alice")
            .add("Green", 1L, "Bob").add("Pink", 2L, "Trudy");
    List<Record> customersWithLastNameFirst = customersWithLastNameFirstBuilder.build();

    // Run a 'select *' to check if the order of the column in the result has been changed.
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithLastNameFirst,
        HiveIcebergTestUtils.valueForRow(customerSchemaLastNameFirst, rows), 1);

    // Query the data with names and check if the result is the same as when the table was created.
    rows = shell.executeStatement("SELECT customer_id, first_name, last_name FROM default.customers");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, rows), 0);

    // Insert data from Hive to check if the last_name column has to be before the customer_id in the values list.
    shell.executeStatement("INSERT INTO default.customers values ('Magenta', 3L, 'Lily')");

    customersWithLastNameFirstBuilder.add("Magenta", 3L, "Lily");
    customersWithLastNameFirst = customersWithLastNameFirstBuilder.build();
    rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithLastNameFirst,
        HiveIcebergTestUtils.valueForRow(customerSchemaLastNameFirst, rows), 1);
  }

  @Test
  public void testMoveCustomerIdAfterFirstNameInIcebergTable() throws IOException {
    // Create an Iceberg table with the columns customer_id, first_name and last_name with some initial data.
    Table icebergTable = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // Move the last_name column before the customer_id in the table schema.
    icebergTable.updateSchema().moveAfter("customer_id", "first_name").commit();

    Schema customerSchemaLastNameFirst =
        new Schema(optional(1, "first_name", Types.StringType.get(), "This is first name"),
            optional(2, "customer_id", Types.LongType.get()),
            optional(3, "last_name", Types.StringType.get(), "This is last name"));

    TestHelper.RecordsBuilder customersWithLastNameFirstBuilder =
        TestHelper.RecordsBuilder.newInstance(customerSchemaLastNameFirst).add("Alice", 0L, "Brown")
            .add("Bob", 1L, "Green").add("Trudy", 2L, "Pink");
    List<Record> customersWithLastNameFirst = customersWithLastNameFirstBuilder.build();

    // Run a 'select *' to check if the order of the column in the result has been changed.
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithLastNameFirst,
        HiveIcebergTestUtils.valueForRow(customerSchemaLastNameFirst, rows), 1);

    // Query the data with names and check if the result is the same as when the table was created.
    rows = shell.executeStatement("SELECT customer_id, first_name, last_name FROM default.customers");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, rows), 0);

    // Insert data from Hive to check if the last_name column has to be before the customer_id in the values list.
    shell.executeStatement("INSERT INTO default.customers values ('Lily', 3L, 'Magenta')");

    customersWithLastNameFirstBuilder.add("Lily", 3L, "Magenta");
    customersWithLastNameFirst = customersWithLastNameFirstBuilder.build();
    rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(customersWithLastNameFirst,
        HiveIcebergTestUtils.valueForRow(customerSchemaLastNameFirst, rows), 1);
  }

  @Test
  public void testUpdateColumnTypeInIcebergTable() throws IOException, TException, InterruptedException {
    // Create an Iceberg table with int, float and decimal(2,1) types with some initial records
    Schema schema = new Schema(optional(1, "id", Types.LongType.get()),
        optional(2, "int_col", Types.IntegerType.get(), "This is an integer type"),
        optional(3, "float_col", Types.FloatType.get(), "This is a float type"),
        optional(4, "decimal_col", Types.DecimalType.of(2, 1), "This is a decimal type"));

    List<Record> records = TestHelper.RecordsBuilder.newInstance(schema).add(0L, 35, 22F, BigDecimal.valueOf(13L, 1))
        .add(1L, 223344, 555.22F, BigDecimal.valueOf(22L, 1)).add(2L, -234, -342F, BigDecimal.valueOf(-12L, 1)).build();

    Table icebergTable = testTables.createTable(shell, "types_table", schema, fileFormat, records);

    // In the result set a float column is returned as double and a decimal is returned as string,
    // even though Hive has the columns with the right types.
    // Probably this conversation happens when fetching the result set after calling the select through the shell.
    // Because of this, a separate schema and record list has to be used when validating the returned values.
    Schema schemaForResultSet =
        new Schema(optional(1, "id", Types.LongType.get()), optional(2, "int_col", Types.IntegerType.get()),
            optional(3, "float_col", Types.DoubleType.get()), optional(4, "decimal_col", Types.StringType.get()));

    List<Record> expectedResults = TestHelper.RecordsBuilder.newInstance(schema).add(0L, 35, 22d, "1.3")
        .add(1L, 223344, 555.22d, "2.2").add(2L, -234, -342d, "-1.2").build();

    // Check the select result and the column types of the table.
    List<Object[]> rows = shell.executeStatement("SELECT * FROM types_table");
    HiveIcebergTestUtils.validateData(expectedResults, HiveIcebergTestUtils.valueForRow(schemaForResultSet, rows), 0);

    org.apache.hadoop.hive.metastore.api.Table table = shell.metastore().getTable("default", "types_table");
    Assert.assertNotNull(table);
    Assert.assertNotNull(table.getSd());
    List<FieldSchema> columns = table.getSd().getCols();
    Assert.assertEquals("id", columns.get(0).getName());
    Assert.assertEquals("bigint", columns.get(0).getType());
    Assert.assertEquals("int_col", columns.get(1).getName());
    Assert.assertEquals("int", columns.get(1).getType());
    Assert.assertEquals("float_col", columns.get(2).getName());
    Assert.assertEquals("float", columns.get(2).getType());
    Assert.assertEquals("decimal_col", columns.get(3).getName());
    Assert.assertEquals("decimal(2,1)", columns.get(3).getType());

    // Change the column types on the table to long, double and decimal(6,1)
    icebergTable.updateSchema().updateColumn("int_col", Types.LongType.get())
        .updateColumn("float_col", Types.DoubleType.get()).updateColumn("decimal_col", Types.DecimalType.of(6, 1))
        .commit();

    schemaForResultSet =
        new Schema(optional(1, "id", Types.LongType.get()), optional(2, "int_col", Types.LongType.get()),
            optional(3, "float_col", Types.DoubleType.get()), optional(4, "decimal_col", Types.StringType.get()));

    expectedResults = TestHelper.RecordsBuilder.newInstance(schema).add(0L, 35L, 22d, "1.3")
        .add(1L, 223344L, 555.219970703125d, "2.2").add(2L, -234L, -342d, "-1.2").build();

    rows = shell.executeStatement("SELECT * FROM types_table");
    HiveIcebergTestUtils.validateData(expectedResults, HiveIcebergTestUtils.valueForRow(schemaForResultSet, rows), 0);

    // Check if the column types in Hive have also changed to big_int, double an decimal(6,1)
    // Do this check only for Hive catalog, as this is the only case when the column types are updated
    // in the metastore. In case of other catalog types, the table is not updated in the metastore,
    // so no point in checking the column types.
    if (TestTables.TestTableType.HIVE_CATALOG.equals(this.testTableType)) {
      table = shell.metastore().getTable("default", "types_table");
      Assert.assertNotNull(table);
      Assert.assertNotNull(table.getSd());
      columns = table.getSd().getCols();
      Assert.assertEquals("id", columns.get(0).getName());
      Assert.assertEquals("bigint", columns.get(0).getType());
      Assert.assertEquals("int_col", columns.get(1).getName());
      Assert.assertEquals("bigint", columns.get(1).getType());
      Assert.assertEquals("float_col", columns.get(2).getName());
      Assert.assertEquals("double", columns.get(2).getType());
      Assert.assertEquals("decimal_col", columns.get(3).getName());
      Assert.assertEquals("decimal(6,1)", columns.get(3).getType());
    }

    // Insert some data which fit to the new column types and check if they are saved and can be queried correctly.
    // This should work for all catalog types.
    shell.executeStatement(
        "INSERT INTO types_table values (3, 3147483647, 111.333, 12345.5), (4, -3147483648, 55, -2234.5)");
    expectedResults = TestHelper.RecordsBuilder.newInstance(schema).add(3L, 3147483647L, 111.333d, "12345.5")
        .add(4L, -3147483648L, 55d, "-2234.5").build();
    rows = shell.executeStatement("SELECT * FROM types_table where id in(3, 4)");
    HiveIcebergTestUtils.validateData(expectedResults, HiveIcebergTestUtils.valueForRow(schemaForResultSet, rows), 0);
  }

  @Test
  public void testSchemaEvolutionForMigratedTables() {
    // create a standard Hive table w/ some records
    TableIdentifier tableIdentifier = TableIdentifier.of("default", "customers");
    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE customers (id bigint, first_name string, last_name string) STORED AS %s %s",
        fileFormat, testTables.locationForCreateTableSQL(tableIdentifier)));
    shell.executeStatement("INSERT INTO customers VALUES (11, 'Lisa', 'Truman')");

    // migrate it to Iceberg
    shell.executeStatement("ALTER TABLE customers SET TBLPROPERTIES " +
        "('storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler')");

    // try to perform illegal schema evolution operations
    AssertHelpers.assertThrows("issuing a replace columns operation on a migrated Iceberg table should throw",
        IllegalArgumentException.class, "Cannot perform REPLACE COLUMNS operation on a migrated Iceberg table",
        () -> shell.executeStatement("ALTER TABLE customers REPLACE COLUMNS (id bigint, last_name string)"));

    AssertHelpers.assertThrows("issuing a change column operation on a migrated Iceberg table should throw",
        IllegalArgumentException.class, "Cannot perform CHANGE COLUMN operation on a migrated Iceberg table",
        () -> shell.executeStatement("ALTER TABLE customers CHANGE COLUMN id customer_id bigint"));

    // check if valid ops are still okay
    shell.executeStatement("ALTER TABLE customers UPDATE COLUMNS");
    shell.executeStatement("ALTER TABLE customers ADD COLUMNS (date_joined timestamp)");

    // double check if schema change worked safely
    shell.executeStatement("INSERT INTO customers VALUES (22, 'Mike', 'Bloomfield', from_unixtime(unix_timestamp()))");
    List<Object[]> result = shell.executeStatement("SELECT * FROM customers ORDER BY id");
    Assert.assertEquals(2, result.size());
    Assert.assertNull(result.get(0)[3]); // first record has null timestamp
    Assert.assertNotNull(result.get(1)[3]); // second record has timestamp filled out
  }
}
