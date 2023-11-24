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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;

/**
 * Creates Iceberg tables using CTAS, and runs select queries against these new tables to verify table content.
 */
public class TestHiveIcebergCTAS extends HiveIcebergStorageHandlerWithEngineBase {

  @Override
  protected void validateTestParams() {
    Assume.assumeTrue(HiveIcebergSerDe.CTAS_EXCEPTION_MSG, testTableType == TestTables.TestTableType.HIVE_CATALOG &&
        isVectorized && formatVersion == 1);
  }

  @Test
  public void testCTASFromHiveTable() {
    shell.executeStatement("CREATE TABLE source (id bigint, name string) PARTITIONED BY (dept string) STORED AS ORC");
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, TableIdentifier.of("default", "source"), false));

    shell.executeStatement(String.format(
        "CREATE TABLE target STORED BY ICEBERG %s %s AS SELECT * FROM source",
        testTables.locationForCreateTableSQL(TableIdentifier.of("default", "target")),
        testTables.propertiesForCreateTableSQL(
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.toString()))));

    List<Object[]> objects = shell.executeStatement("SELECT * FROM target ORDER BY id");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
  }

  @Test
  public void testCTASPartitionedFromHiveTable() throws TException, InterruptedException {
    shell.executeStatement("CREATE TABLE source (id bigint, name string) PARTITIONED BY (dept string) STORED AS ORC");
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, TableIdentifier.of("default", "source"), false));

    shell.executeStatement(String.format(
        "CREATE TABLE target PARTITIONED BY (dept, name) " +
            "STORED BY ICEBERG %s AS SELECT * FROM source",
        testTables.propertiesForCreateTableSQL(
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.toString()))));

    // check table can be read back correctly
    List<Object[]> objects = shell.executeStatement("SELECT id, name, dept FROM target ORDER BY id");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);

    // check HMS table has been created correctly (no partition cols, props pushed down)
    org.apache.hadoop.hive.metastore.api.Table hmsTable = shell.metastore().getTable("default", "target");
    Assert.assertEquals(3, hmsTable.getSd().getColsSize());
    Assert.assertTrue(hmsTable.getPartitionKeys().isEmpty());
    Assert.assertEquals(
            fileFormat.toString().toLowerCase(), hmsTable.getParameters().get(TableProperties.DEFAULT_FILE_FORMAT));

    // check Iceberg table has correct partition spec
    Table table = testTables.loadTable(TableIdentifier.of("default", "target"));
    Assert.assertEquals(2, table.spec().fields().size());
    Assert.assertEquals("dept", table.spec().fields().get(0).name());
    Assert.assertEquals("name", table.spec().fields().get(1).name());
  }

  @Test
  public void testCTASTblPropsAndLocationClause() throws Exception {
    shell.executeStatement("CREATE TABLE source (id bigint, name string) PARTITIONED BY (dept string) STORED AS ORC");
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, TableIdentifier.of("default", "source"), false));

    String location = temp.newFolder().toURI().toString();
    shell.executeStatement(String.format(
        "CREATE TABLE target PARTITIONED BY (dept, name) " +
        "STORED BY ICEBERG STORED AS %s LOCATION '%s' TBLPROPERTIES ('customKey'='customValue') " +
        "AS SELECT * FROM source", fileFormat.toString(), location));

    // check table can be read back correctly
    List<Object[]> objects = shell.executeStatement("SELECT id, name, dept FROM target ORDER BY id");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);

    // check table is created at the correct location
    org.apache.hadoop.hive.metastore.api.Table tbl = shell.metastore().getTable("default", "target");
    Assert.assertEquals(location, tbl.getSd().getLocation() + "/" /* HMS trims the trailing dash */);

    // check if valid table properties are preserved, while serde props don't get preserved
    Assert.assertEquals("customValue", tbl.getParameters().get("customKey"));
    Assert.assertNull(tbl.getParameters().get(serdeConstants.LIST_COLUMNS));
    Assert.assertNull(tbl.getParameters().get(serdeConstants.LIST_PARTITION_COLUMNS));
  }

  @Test
  public void testCTASPartitionedBySpec() throws TException, InterruptedException {
    Schema schema = new Schema(
        optional(1, "id", Types.LongType.get()),
        optional(2, "year_field", Types.DateType.get()),
        optional(3, "month_field", Types.TimestampType.withZone()),
        optional(4, "day_field", Types.TimestampType.withoutZone()),
        optional(5, "hour_field", Types.TimestampType.withoutZone()),
        optional(6, "truncate_field", Types.StringType.get()),
        optional(7, "bucket_field", Types.StringType.get()),
        optional(8, "identity_field", Types.StringType.get())
    );

    List<Record> records =  TestHelper.generateRandomRecords(schema, 10, 0L);

    shell.executeStatement("CREATE TABLE source (id bigint, year_field date, month_field timestamp, " +
        "day_field timestamp, hour_field timestamp, truncate_field string, bucket_field string, " +
        "identity_field string)");

    shell.executeStatement(testTables.getInsertQuery(records, TableIdentifier.of("default", "source"), false));

    shell.executeStatement(String.format("CREATE TABLE target PARTITIONED BY SPEC " +
        "(year(year_field), month(month_field), day(day_field), hour(hour_field), " +
        "truncate(2, truncate_field), bucket(2, bucket_field), identity_field) " +
        "STORED BY ICEBERG %s AS SELECT * FROM source", testTables.propertiesForCreateTableSQL(
        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.toString()))));

    // check table can be read back correctly
    records.sort(Comparator.comparingLong(record -> (Long) record.get(0)));
    HiveIcebergTestUtils.validateDataWithSQL(shell, "default.target", records, "id");

    // check HMS table has been created correctly (no partition cols, props pushed down)
    org.apache.hadoop.hive.metastore.api.Table hmsTable = shell.metastore().getTable("default", "target");
    Assert.assertEquals(8, hmsTable.getSd().getColsSize());
    Assert.assertTrue(hmsTable.getPartitionKeys().isEmpty());
    Assert.assertEquals(
            fileFormat.toString().toLowerCase(), hmsTable.getParameters().get(TableProperties.DEFAULT_FILE_FORMAT));

    // check Iceberg table has correct partition spec
    Table table = testTables.loadTable(TableIdentifier.of("default", "target"));
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .year("year_field")
        .month("month_field")
        .day("day_field")
        .hour("hour_field")
        .truncate("truncate_field", 2)
        .bucket("bucket_field", 2)
        .identity("identity_field")
        .build();
    Assert.assertEquals(PartitionSpecParser.toJson(spec), PartitionSpecParser.toJson(table.spec()));
  }

  @Test
  public void testCTASFailureRollback() throws IOException {
    // force an execution error by passing in a committer class that Tez won't be able to load
    shell.setHiveSessionValue("hive.tez.mapreduce.output.committer.class", "org.apache.NotExistingClass");

    TableIdentifier target = TableIdentifier.of("default", "target");
    testTables.createTable(shell, "source", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    String[] partitioningSchemes = {"", "PARTITIONED BY (last_name)", "PARTITIONED BY (customer_id, last_name)"};
    for (String partitioning : partitioningSchemes) {
      AssertHelpers.assertThrows("Should fail while loading non-existent output committer class.",
          IllegalArgumentException.class, "org.apache.NotExistingClass",
          () -> shell.executeStatement(String.format(
              "CREATE TABLE target %s STORED BY ICEBERG AS SELECT * FROM source", partitioning)));
      // CTAS table should have been dropped by the lifecycle hook
      Assert.assertThrows(NoSuchTableException.class, () -> testTables.loadTable(target));
    }
  }

  @Test
  public void testCTASFollowedByTruncate() throws IOException {
    testTables.createTable(shell, "source", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE target STORED BY ICEBERG STORED AS %s %s AS SELECT * FROM source",
        fileFormat, testTables.locationForCreateTableSQL(TableIdentifier.of("default", "target"))));

    List<Object[]> objects = shell.executeStatement("SELECT * FROM target ORDER BY customer_id");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);

    shell.executeStatement("TRUNCATE TABLE target");

    objects = shell.executeStatement("SELECT * FROM target");
    Assert.assertTrue(objects.isEmpty());
  }

  @Test
  public void testCTASUnsupportedTypeWithoutAutoConversion() {
    Map<String, Type> notSupportedTypes = ImmutableMap.of(
        "TINYINT", Types.IntegerType.get(),
        "SMALLINT", Types.IntegerType.get(),
        "VARCHAR(1)", Types.StringType.get(),
        "CHAR(1)", Types.StringType.get());


    for (String notSupportedType : notSupportedTypes.keySet()) {
      shell.executeStatement(String.format("CREATE TABLE source (s %s) STORED AS ORC", notSupportedType));
      AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
          "Unsupported Hive type ", () -> {
            shell.executeStatement(String.format(
                "CREATE TABLE target STORED BY ICEBERG %s %s AS SELECT * FROM source",
                testTables.locationForCreateTableSQL(TableIdentifier.of("default", "target")),
                testTables.propertiesForCreateTableSQL(
                    ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.toString())
                )
            ));
          });
      shell.executeStatement("DROP TABLE source");
    }
  }

  @Test
  public void testCTASUnsupportedTypeWithAutoConversion() {
    Map<String, Type> notSupportedTypes = ImmutableMap.of(
        "TINYINT", Types.IntegerType.get(),
        "SMALLINT", Types.IntegerType.get(),
        "VARCHAR(1)", Types.StringType.get(),
        "CHAR(1)", Types.StringType.get());

    shell.setHiveSessionValue(InputFormatConfig.SCHEMA_AUTO_CONVERSION, "true");

    for (String notSupportedType : notSupportedTypes.keySet()) {
      shell.executeStatement(String.format("CREATE TABLE source (s %s) STORED AS ORC", notSupportedType));
      shell.executeStatement(String.format(
          "CREATE TABLE target STORED BY ICEBERG %s %s AS SELECT * FROM source",
          testTables.locationForCreateTableSQL(TableIdentifier.of("default", "target")),
          testTables.propertiesForCreateTableSQL(
              ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.toString())
          )
      ));

      org.apache.iceberg.Table icebergTable = testTables.loadTable(TableIdentifier.of("default", "target"));
      Assert.assertEquals(notSupportedTypes.get(notSupportedType), icebergTable.schema().columns().get(0).type());
      shell.executeStatement("DROP TABLE source");
      shell.executeStatement("DROP TABLE target");
    }
  }

  @Test
  public void testCTASForAllColumnTypes() {
    shell.setHiveSessionValue(InputFormatConfig.SCHEMA_AUTO_CONVERSION, "true");
    String sourceCreate = "CREATE EXTERNAL TABLE source (" +
        "timestamp_col_1 TIMESTAMP, " +
        "decimal3003_col_2 DECIMAL(30, 3), " +
        "tinyint_col_3 TINYINT, " +
        "decimal0101_col_4 DECIMAL(1, 1), " +
        "boolean_col_5 BOOLEAN, " +
        "float_col_6 FLOAT, " +
        "bigint_col_7 BIGINT, " +
        "varchar0098_col_8 VARCHAR(98), " +
        "timestamp_col_9 TIMESTAMP, " +
        "bigint_col_10 BIGINT, " +
        "decimal0903_col_11 DECIMAL(9, 3), " +
        "timestamp_col_12 TIMESTAMP, " +
        "timestamp_col_13 TIMESTAMP, " +
        "float_col_14 FLOAT, " +
        "char0254_col_15 CHAR(254), " +
        "double_col_16 DOUBLE, " +
        "timestamp_col_17 TIMESTAMP, " +
        "boolean_col_18 BOOLEAN, " +
        "decimal2608_col_19 DECIMAL(26, 8), " +
        "varchar0216_col_20 VARCHAR(216), " +
        "string_col_21 STRING, " +
        "bigint_col_22 BIGINT, " +
        "boolean_col_23 BOOLEAN, " +
        "timestamp_col_24 TIMESTAMP, " +
        "boolean_col_25 BOOLEAN, " +
        "decimal2016_col_26 DECIMAL(20, 16), " +
        "string_col_27 STRING, " +
        "decimal0202_col_28 DECIMAL(2, 2), " +
        "float_col_29 FLOAT, " +
        "decimal2020_col_30 DECIMAL(20, 20), " +
        "boolean_col_31 BOOLEAN, " +
        "double_col_32 DOUBLE, " +
        "varchar0148_col_33 VARCHAR(148), " +
        "decimal2121_col_34 DECIMAL(21, 21), " +
        "tinyint_col_35 TINYINT, " +
        "boolean_col_36 BOOLEAN, " +
        "boolean_col_37 BOOLEAN, " +
        "string_col_38 STRING, " +
        "decimal3420_col_39 DECIMAL(34, 20), " +
        "timestamp_col_40 TIMESTAMP, " +
        "decimal1408_col_41 DECIMAL(14, 8), " +
        "string_col_42 STRING, " +
        "decimal0902_col_43 DECIMAL(9, 2), " +
        "varchar0204_col_44 VARCHAR(204), " +
        "boolean_col_45 BOOLEAN, " +
        "timestamp_col_46 TIMESTAMP, " +
        "boolean_col_47 BOOLEAN, " +
        "bigint_col_48 BIGINT, " +
        "boolean_col_49 BOOLEAN, " +
        "smallint_col_50 SMALLINT, " +
        "decimal0704_col_51 DECIMAL(7, 4), " +
        "timestamp_col_52 TIMESTAMP, " +
        "boolean_col_53 BOOLEAN, " +
        "timestamp_col_54 TIMESTAMP, " +
        "int_col_55 INT, " +
        "decimal0505_col_56 DECIMAL(5, 5), " +
        "char0155_col_57 CHAR(155), " +
        "boolean_col_58 BOOLEAN, " +
        "bigint_col_59 BIGINT, " +
        "boolean_col_60 BOOLEAN, " +
        "boolean_col_61 BOOLEAN, " +
        "char0249_col_62 CHAR(249), " +
        "boolean_col_63 BOOLEAN, " +
        "timestamp_col_64 TIMESTAMP, " +
        "decimal1309_col_65 DECIMAL(13, 9), " +
        "int_col_66 INT, " +
        "float_col_67 FLOAT, " +
        "timestamp_col_68 TIMESTAMP, " +
        "timestamp_col_69 TIMESTAMP, " +
        "boolean_col_70 BOOLEAN, " +
        "timestamp_col_71 TIMESTAMP, " +
        "double_col_72 DOUBLE, " +
        "boolean_col_73 BOOLEAN, " +
        "char0222_col_74 CHAR(222), " +
        "float_col_75 FLOAT, " +
        "string_col_76 STRING, " +
        "decimal2612_col_77 DECIMAL(26, 12), " +
        "timestamp_col_78 TIMESTAMP, " +
        "char0128_col_79 CHAR(128), " +
        "timestamp_col_80 TIMESTAMP, " +
        "double_col_81 DOUBLE, " +
        "timestamp_col_82 TIMESTAMP, " +
        "float_col_83 FLOAT, " +
        "decimal2622_col_84 DECIMAL(26, 22), " +
        "double_col_85 DOUBLE, " +
        "float_col_86 FLOAT, " +
        "decimal0907_col_87 DECIMAL(9, 7)) " +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'";

    shell.executeStatement(sourceCreate);

    String ctas = "CREATE TABLE target STORED BY ICEBERG STORED AS orc AS SELECT * FROM source";
    shell.executeStatement(ctas);
  }
}
