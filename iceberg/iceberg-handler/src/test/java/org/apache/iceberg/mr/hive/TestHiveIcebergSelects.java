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
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assume.assumeTrue;

/**
 * Runs miscellaneous select statements on Iceberg tables, and verifies the result. Tests meant to verify simple
 * reads, more complex selects, joins, various plan generation options, special table names, etc.. should be listed
 * here.
 */
public class TestHiveIcebergSelects extends HiveIcebergStorageHandlerWithEngineBase {

  @Test
  public void testScanTable() throws IOException {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // Adding the ORDER BY clause will cause Hive to spawn a local MR job this time.
    List<Object[]> descRows =
        shell.executeStatement("SELECT first_name, customer_id FROM default.customers ORDER BY customer_id DESC");

    Assert.assertEquals(3, descRows.size());
    Assert.assertArrayEquals(new Object[] {"Trudy", 2L}, descRows.get(0));
    Assert.assertArrayEquals(new Object[] {"Bob", 1L}, descRows.get(1));
    Assert.assertArrayEquals(new Object[] {"Alice", 0L}, descRows.get(2));
  }

  @Test
  public void testCBOWithSelectedColumnsNonOverlapJoin() throws IOException {
    shell.setHiveSessionValue("hive.cbo.enable", true);
    testTables.createTable(shell, "products", PRODUCT_SCHEMA, fileFormat, PRODUCT_RECORDS);
    testTables.createTable(shell, "orders", ORDER_SCHEMA, fileFormat, ORDER_RECORDS);

    List<Object[]> rows = shell.executeStatement(
        "SELECT o.order_id, o.customer_id, o.total, p.name " +
            "FROM default.orders o JOIN default.products p ON o.product_id = p.id ORDER BY o.order_id"
    );

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {100L, 0L, 11.11d, "skirt"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {101L, 0L, 22.22d, "tee"}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {102L, 1L, 33.33d, "watch"}, rows.get(2));
  }

  @Test
  public void testCBOWithSelectedColumnsOverlapJoin() throws IOException {
    shell.setHiveSessionValue("hive.cbo.enable", true);
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    testTables.createTable(shell, "orders", ORDER_SCHEMA, fileFormat, ORDER_RECORDS);

    List<Object[]> rows = shell.executeStatement(
        "SELECT c.first_name, o.order_id " +
            "FROM default.orders o JOIN default.customers c ON o.customer_id = c.customer_id " +
            "ORDER BY o.order_id DESC"
    );

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {"Bob", 102L}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {"Alice", 101L}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {"Alice", 100L}, rows.get(2));
  }

  @Test
  public void testCBOWithSelfJoin() throws IOException {
    shell.setHiveSessionValue("hive.cbo.enable", true);

    testTables.createTable(shell, "orders", ORDER_SCHEMA, fileFormat, ORDER_RECORDS);

    List<Object[]> rows = shell.executeStatement(
        "SELECT o1.order_id, o1.customer_id, o1.total " +
            "FROM default.orders o1 JOIN default.orders o2 ON o1.order_id = o2.order_id ORDER BY o1.order_id"
    );

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {100L, 0L, 11.11d}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {101L, 0L, 22.22d}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {102L, 1L, 33.33d}, rows.get(2));
  }

  @Test
  public void testJoinTablesSupportedTypes() throws IOException {
    for (int i = 0; i < SUPPORTED_TYPES.size(); i++) {
      Type type = SUPPORTED_TYPES.get(i);
      if ((type == Types.TimestampType.withZone()) && isVectorized && fileFormat == FileFormat.ORC) {
        // ORC/TIMESTAMP_INSTANT is not supported vectorized types for Hive
        continue;
      }
      // TODO: remove this filter when issue #1881 is resolved
      if (type == Types.UUIDType.get() && fileFormat == FileFormat.PARQUET) {
        continue;
      }
      String tableName = type.typeId().toString().toLowerCase() + "_table_" + i;
      String columnName = type.typeId().toString().toLowerCase() + "_column";

      Schema schema = new Schema(required(1, columnName, type));
      List<Record> records = TestHelper.generateRandomRecords(schema, 1, 0L);

      testTables.createTable(shell, tableName, schema, fileFormat, records);
      List<Object[]> queryResult = shell.executeStatement("select s." + columnName + ", h." + columnName +
          " from default." + tableName + " s join default." + tableName + " h on h." + columnName + "=s." +
          columnName);
      Assert.assertEquals("Non matching record count for table " + tableName + " with type " + type,
          1, queryResult.size());
    }
  }

  @Test
  public void testSelectDistinctFromTable() throws IOException {
    for (int i = 0; i < SUPPORTED_TYPES.size(); i++) {
      Type type = SUPPORTED_TYPES.get(i);
      if ((type == Types.TimestampType.withZone()) &&
          isVectorized && fileFormat == FileFormat.ORC) {
        // ORC/TIMESTAMP_INSTANT is not supported vectorized types for Hive
        continue;
      }
      // TODO: remove this filter when issue #1881 is resolved
      if (type == Types.UUIDType.get() && fileFormat == FileFormat.PARQUET) {
        continue;
      }
      String tableName = type.typeId().toString().toLowerCase() + "_table_" + i;
      String columnName = type.typeId().toString().toLowerCase() + "_column";

      Schema schema = new Schema(required(1, columnName, type));
      List<Record> records = TestHelper.generateRandomRecords(schema, 4, 0L);
      int size = records.stream().map(r -> r.getField(columnName)).collect(Collectors.toSet()).size();
      testTables.createTable(shell, tableName, schema, fileFormat, records);
      List<Object[]> queryResult = shell.executeStatement("select count(distinct(" + columnName +
          ")) from default." + tableName);
      int distinctIds = ((Long) queryResult.get(0)[0]).intValue();
      Assert.assertEquals(tableName, size, distinctIds);
    }
  }

  @Test
  public void testSpecialCharacters() {
    TableIdentifier table = TableIdentifier.of("default", "tar,! ,get");
    // note: the Chinese character seems to be accepted in the column name, but not
    // in the table name - this is the case for both Iceberg and standard Hive tables.
    shell.executeStatement(String.format(
        "CREATE TABLE `%s` (id bigint, `dep,! 是,t` string) STORED BY ICEBERG STORED AS %s %s %s",
        table.name(), fileFormat, testTables.locationForCreateTableSQL(table),
        testTables.propertiesForCreateTableSQL(ImmutableMap.of())));
    shell.executeStatement(String.format("INSERT INTO `%s` VALUES (1, 'moon'), (2, 'star')", table.name()));

    List<Object[]> result = shell.executeStatement(String.format(
        "SELECT `dep,! 是,t`, id FROM `%s` ORDER BY id", table.name()));

    Assert.assertEquals(2, result.size());
    Assert.assertArrayEquals(new Object[]{"moon", 1L}, result.get(0));
    Assert.assertArrayEquals(new Object[]{"star", 2L}, result.get(1));
  }

  @Test
  public void testScanTableCaseInsensitive() throws IOException {
    testTables.createTable(shell, "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA_WITH_UPPERCASE, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", "Green"}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, rows.get(2));

    rows = shell.executeStatement("SELECT * FROM default.customers where CustomER_Id < 2 " +
        "and first_name in ('Alice', 'Bob')");

    Assert.assertEquals(2, rows.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", "Green"}, rows.get(1));
  }

  /**
   * Column pruning could become problematic when a single Map Task contains multiple TableScan operators where
   * different columns are pruned. This only occurs on MR, as Tez initializes a single Map task for every TableScan
   * operator.
   */
  @Test
  public void testMultiColumnPruning() throws IOException {
    shell.setHiveSessionValue("hive.cbo.enable", true);

    Schema schema1 = new Schema(optional(1, "fk", Types.StringType.get()));
    List<Record> records1 = TestHelper.RecordsBuilder.newInstance(schema1).add("fk1").build();
    testTables.createTable(shell, "table1", schema1, fileFormat, records1);

    Schema schema2 = new Schema(optional(1, "fk", Types.StringType.get()), optional(2, "val", Types.StringType.get()));
    List<Record> records2 = TestHelper.RecordsBuilder.newInstance(schema2).add("fk1", "val").build();
    testTables.createTable(shell, "table2", schema2, fileFormat, records2);

    // MR is needed for the reproduction
    shell.setHiveSessionValue("hive.execution.engine", "mr");
    String query = "SELECT t2.val FROM table1 t1 JOIN table2 t2 ON t1.fk = t2.fk";
    List<Object[]> result = shell.executeStatement(query);
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(new Object[]{"val"}, result.get(0));
  }

  /**
   * Tests that vectorized ORC reading code path correctly handles when the same ORC file is split into multiple parts.
   * Although the split offsets and length will not always include the file tail that contains the metadata, the
   * vectorized reader needs to make sure to handle the tail reading regardless of the offsets. If this is not done
   * correctly, the last SELECT query will fail.
   * @throws Exception - any test error
   */
  @Test
  public void testVectorizedOrcMultipleSplits() throws Exception {
    assumeTrue(isVectorized && FileFormat.ORC.equals(fileFormat));

    // This data will be held by a ~870kB ORC file
    List<Record> records = TestHelper.generateRandomRecords(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        20000, 0L);

    // To support splitting the ORC file, we need to specify the stripe size to a small value. It looks like the min
    // value is about 220kB, no smaller stripes are written by ORC. Anyway, this setting will produce 4 stripes.
    shell.setHiveSessionValue("orc.stripe.size", "210000");

    testTables.createTable(shell, "targettab", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, records);

    // Will request 4 splits, separated on the exact stripe boundaries within the ORC file.
    // (Would request 5 if ORC split generation wouldn't be split (aka stripe) offset aware).
    shell.setHiveSessionValue(InputFormatConfig.SPLIT_SIZE, "210000");
    List<Object[]> result = shell.executeStatement("SELECT * FROM targettab ORDER BY last_name");

    Assert.assertEquals(20000, result.size());

  }

  @Test
  public void testHistory() throws IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "source");
    Table table = testTables.createTableWithVersions(shell, identifier.name(),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 1);
    List<Object[]> history = shell.executeStatement("SELECT snapshot_id FROM default.source.history");
    Assert.assertEquals(table.history().size(), history.size());
    for (int i = 0; i < table.history().size(); ++i) {
      Assert.assertEquals(table.history().get(i).snapshotId(), history.get(i)[0]);
    }
  }
}
