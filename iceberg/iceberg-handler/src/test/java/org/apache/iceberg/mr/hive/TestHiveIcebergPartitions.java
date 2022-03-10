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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Verifies partitioning features in Iceberg tables. Covers both identity and transform partition types, verifies by
 * writing content into partitioned tables and by reading it back.
 */
public class TestHiveIcebergPartitions extends HiveIcebergStorageHandlerWithEngineBase {

  @Test
  public void testPartitionPruning() throws IOException {
    Schema salesSchema = new Schema(
        required(1, "ss_item_sk", Types.IntegerType.get()),
        required(2, "ss_sold_date_sk", Types.IntegerType.get()));

    PartitionSpec salesSpec =
        PartitionSpec.builderFor(salesSchema).identity("ss_sold_date_sk").build();

    Schema dimSchema = new Schema(
        required(1, "d_date_sk", Types.IntegerType.get()),
        required(2, "d_moy", Types.IntegerType.get()));

    List<Record> salesRecords = TestHelper.RecordsBuilder.newInstance(salesSchema)
        .add(51, 5)
        .add(61, 6)
        .add(71, 7)
        .add(81, 8)
        .add(91, 9)
        .build();
    List<Record> dimRecords = TestHelper.RecordsBuilder.newInstance(salesSchema)
        .add(1, 10)
        .add(2, 20)
        .add(3, 30)
        .add(4, 40)
        .add(5, 50)
        .build();

    Table salesTable = testTables.createTable(shell, "x1_store_sales", salesSchema, salesSpec, fileFormat, null);

    PartitionKey partitionKey = new PartitionKey(salesSpec, salesSchema);
    for (Record r : salesRecords) {
      partitionKey.partition(r);
      testTables.appendIcebergTable(shell.getHiveConf(), salesTable, fileFormat, partitionKey, ImmutableList.of(r));
    }
    testTables.createTable(shell, "x1_date_dim", dimSchema, fileFormat, dimRecords);

    String query = "select s.ss_item_sk from x1_store_sales s, x1_date_dim d " +
        "where s.ss_sold_date_sk=d.d_date_sk*2 and d.d_moy=30";

    // Check the query results
    List<Object[]> rows = shell.executeStatement(query);

    Assert.assertEquals(1, rows.size());
    Assert.assertArrayEquals(new Object[] {61}, rows.get(0));

    // Check if Dynamic Partitioning is used
    Assert.assertTrue(shell.executeStatement("explain " + query).stream()
        .filter(a -> ((String) a[0]).contains("Dynamic Partitioning Event Operator"))
        .findAny()
        .isPresent());
  }

  @Test
  public void testPartitionedWrite() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .bucket("customer_id", 3)
        .build();

    List<Record> records = TestHelper.generateRandomRecords(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, 4, 0L);

    Table table = testTables.createTable(shell, "partitioned_customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, spec, fileFormat, records);

    HiveIcebergTestUtils.validateData(table, records, 0);
  }

  @Test
  public void testIdentityPartitionedWrite() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("customer_id")
        .build();

    List<Record> records = TestHelper.generateRandomRecords(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, 4, 0L);

    Table table = testTables.createTable(shell, "partitioned_customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, spec, fileFormat, records);

    HiveIcebergTestUtils.validateData(table, records, 0);
  }

  @Test
  public void testMultilevelIdentityPartitionedWrite() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("customer_id")
        .identity("last_name")
        .build();

    List<Record> records = TestHelper.generateRandomRecords(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, 4, 0L);

    Table table = testTables.createTable(shell, "partitioned_customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, spec, fileFormat, records);

    HiveIcebergTestUtils.validateData(table, records, 0);
  }

  @Test
  public void testYearTransform() throws IOException {
    Schema schema = new Schema(
        optional(1, "id", Types.LongType.get()),
        optional(2, "part_field", Types.DateType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).year("part_field").build();
    List<Record> records = TestHelper.RecordsBuilder.newInstance(schema)
        .add(1L, LocalDate.of(2020, 1, 21))
        .add(2L, LocalDate.of(2020, 1, 22))
        .add(3L, LocalDate.of(2019, 1, 21))
        .build();
    Table table = testTables.createTable(shell, "part_test", schema, spec, fileFormat, records);
    HiveIcebergTestUtils.validateData(table, records, 0);

    HiveIcebergTestUtils.validateDataWithSQL(shell, "part_test", records, "id");
  }

  @Test
  public void testMonthTransform() throws IOException {
    Assume.assumeTrue("ORC/TIMESTAMP_INSTANT is not a supported vectorized type for Hive",
        isVectorized && fileFormat == FileFormat.ORC);
    Schema schema = new Schema(
        optional(1, "id", Types.LongType.get()),
        optional(2, "part_field", Types.TimestampType.withZone()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).month("part_field").build();
    List<Record> records = TestHelper.RecordsBuilder.newInstance(schema)
        .add(1L, OffsetDateTime.of(2017, 11, 22, 11, 30, 7, 0, ZoneOffset.ofHours(1)))
        .add(2L, OffsetDateTime.of(2017, 11, 22, 11, 30, 7, 0, ZoneOffset.ofHours(2)))
        .add(3L, OffsetDateTime.of(2017, 11, 23, 11, 30, 7, 0, ZoneOffset.ofHours(3)))
        .build();
    Table table = testTables.createTable(shell, "part_test", schema, spec, fileFormat, records);
    HiveIcebergTestUtils.validateData(table, records, 0);

    HiveIcebergTestUtils.validateDataWithSQL(shell, "part_test", records, "id");
  }

  @Test
  public void testDayTransform() throws IOException {
    Schema schema = new Schema(
        optional(1, "id", Types.LongType.get()),
        optional(2, "part_field", Types.TimestampType.withoutZone()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).day("part_field").build();
    List<Record> records = TestHelper.RecordsBuilder.newInstance(schema)
        .add(1L, LocalDateTime.of(2019, 2, 22, 9, 44, 54))
        .add(2L, LocalDateTime.of(2019, 2, 22, 10, 44, 54))
        .add(3L, LocalDateTime.of(2019, 2, 23, 9, 44, 54))
        .build();
    Table table = testTables.createTable(shell, "part_test", schema, spec, fileFormat, records);
    HiveIcebergTestUtils.validateData(table, records, 0);

    HiveIcebergTestUtils.validateDataWithSQL(shell, "part_test", records, "id");
  }

  @Test
  public void testHourTransform() throws IOException {
    Schema schema = new Schema(
        optional(1, "id", Types.LongType.get()),
        optional(2, "part_field", Types.TimestampType.withoutZone()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).hour("part_field").build();
    List<Record> records = TestHelper.RecordsBuilder.newInstance(schema)
        .add(1L, LocalDateTime.of(2019, 2, 22, 9, 44, 54))
        .add(2L, LocalDateTime.of(2019, 2, 22, 10, 44, 54))
        .add(3L, LocalDateTime.of(2019, 2, 23, 9, 44, 54))
        .build();
    Table table = testTables.createTable(shell, "part_test", schema, spec, fileFormat, records);
    HiveIcebergTestUtils.validateData(table, records, 0);

    HiveIcebergTestUtils.validateDataWithSQL(shell, "part_test", records, "id");
  }

  @Test
  public void testBucketTransform() throws IOException {
    Schema schema = new Schema(
        optional(1, "id", Types.LongType.get()),
        optional(2, "part_field", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("part_field", 2).build();
    List<Record> records = TestHelper.RecordsBuilder.newInstance(schema)
        .add(1L, "Part1")
        .add(2L, "Part2")
        .add(3L, "Art3")
        .build();
    Table table = testTables.createTable(shell, "part_test", schema, spec, fileFormat, records);
    HiveIcebergTestUtils.validateData(table, records, 0);

    HiveIcebergTestUtils.validateDataWithSQL(shell, "part_test", records, "id");
  }

  @Test
  public void testTruncateTransform() throws IOException {
    Schema schema = new Schema(
        optional(1, "id", Types.LongType.get()),
        optional(2, "part_field", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("part_field", 2).build();
    List<Record> records = TestHelper.RecordsBuilder.newInstance(schema)
        .add(1L, "Part1")
        .add(2L, "Part2")
        .add(3L, "Art3")
        .build();
    Table table = testTables.createTable(shell, "part_test", schema, spec, fileFormat, records);
    HiveIcebergTestUtils.validateData(table, records, 0);

    HiveIcebergTestUtils.validateDataWithSQL(shell, "part_test", records, "id");
  }
}
