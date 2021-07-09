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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.hive.MetastoreUtil;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestHiveIcebergStorageHandlerNoScan {
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  private static final Schema COMPLEX_SCHEMA = new Schema(
      optional(1, "id", Types.LongType.get()),
      optional(2, "name", Types.StringType.get()),
      optional(3, "employee_info", Types.StructType.of(
          optional(7, "employer", Types.StringType.get()),
          optional(8, "id", Types.LongType.get()),
          optional(9, "address", Types.StringType.get())
      )),
      optional(4, "places_lived", Types.ListType.ofOptional(10, Types.StructType.of(
          optional(11, "street", Types.StringType.get()),
          optional(12, "city", Types.StringType.get()),
          optional(13, "country", Types.StringType.get())
      ))),
      optional(5, "memorable_moments", Types.MapType.ofOptional(14, 15,
          Types.StringType.get(),
          Types.StructType.of(
              optional(16, "year", Types.IntegerType.get()),
              optional(17, "place", Types.StringType.get()),
              optional(18, "details", Types.StringType.get())
          ))),
      optional(6, "current_address", Types.StructType.of(
          optional(19, "street_address", Types.StructType.of(
              optional(22, "street_number", Types.IntegerType.get()),
              optional(23, "street_name", Types.StringType.get()),
              optional(24, "street_type", Types.StringType.get())
          )),
          optional(20, "country", Types.StringType.get()),
          optional(21, "postal_code", Types.StringType.get())
      ))
  );

  private static final Set<String> IGNORED_PARAMS = ImmutableSet.of("bucketing_version", "numFilesErasureCoded");

  @Parameters(name = "catalog={0}")
  public static Collection<Object[]> parameters() {
    Collection<Object[]> testParams = new ArrayList<>();
    for (TestTables.TestTableType testTableType : TestTables.ALL_TABLE_TYPES) {
      testParams.add(new Object[] {testTableType});
    }

    return testParams;
  }

  private static TestHiveShell shell;

  private TestTables testTables;

  @Parameter(0)
  public TestTables.TestTableType testTableType;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() {
    shell = HiveIcebergStorageHandlerTestUtils.shell();
  }

  @AfterClass
  public static void afterClass() {
    shell.stop();
  }

  @Before
  public void before() throws IOException {
    testTables = HiveIcebergStorageHandlerTestUtils.testTables(shell, testTableType, temp);
    // Uses spark as an engine so we can detect if we unintentionally try to use any execution engines
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp, "spark");
  }

  @After
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
  }

  @Test
  public void testPartitionEvolution() {
    Schema schema = new Schema(
        optional(1, "id", Types.LongType.get()),
        optional(2, "ts", Types.TimestampType.withZone())
    );
    TableIdentifier identifier = TableIdentifier.of("default", "part_test");
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier +
        " STORED BY ICEBERG " +
        testTables.locationForCreateTableSQL(identifier) +
        " TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
        SchemaParser.toJson(schema) + "', " +
        "'" + InputFormatConfig.CATALOG_NAME + "'='" + Catalogs.ICEBERG_DEFAULT_CATALOG_NAME + "')");

    shell.executeStatement("ALTER TABLE " + identifier + " SET PARTITION SPEC (month(ts))");

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .withSpecId(1)
        .month("ts")
        .build();
    Table table = testTables.loadTable(identifier);
    Assert.assertEquals(spec, table.spec());

    shell.executeStatement("ALTER TABLE " + identifier + " SET PARTITION SPEC (day(ts))");

    spec = PartitionSpec.builderFor(schema)
        .withSpecId(2)
        .alwaysNull("ts", "ts_month")
        .day("ts")
        .build();

    table.refresh();
    Assert.assertEquals(spec, table.spec());
  }

  @Test
  public void testSetPartitionTransform() {
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

    TableIdentifier identifier = TableIdentifier.of("default", "part_test");
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier +
        " PARTITIONED BY SPEC (year(year_field), hour(hour_field), " +
        "truncate(2, truncate_field), bucket(2, bucket_field), identity_field)" +
        " STORED BY ICEBERG " +
        testTables.locationForCreateTableSQL(identifier) +
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
        SchemaParser.toJson(schema) + "', " +
        "'" + InputFormatConfig.CATALOG_NAME + "'='" + Catalogs.ICEBERG_DEFAULT_CATALOG_NAME + "')");

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .year("year_field")
        .hour("hour_field")
        .truncate("truncate_field", 2)
        .bucket("bucket_field", 2)
        .identity("identity_field")
        .build();

    Table table = testTables.loadTable(identifier);
    Assert.assertEquals(spec, table.spec());

    shell.executeStatement("ALTER TABLE default.part_test SET PARTITION SPEC(year(year_field), month(month_field), " +
        "day(day_field))");

    spec = PartitionSpec.builderFor(schema)
        .withSpecId(1)
        .year("year_field")
        .alwaysNull("hour_field", "hour_field_hour")
        .alwaysNull("truncate_field", "truncate_field_trunc")
        .alwaysNull("bucket_field", "bucket_field_bucket")
        .alwaysNull("identity_field", "identity_field")
        .month("month_field")
        .day("day_field")
        .build();

    table.refresh();
    Assert.assertEquals(spec, table.spec());
  }

  @Test
  public void testPartitionTransform() {
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
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .year("year_field")
        .month("month_field")
        .day("day_field")
        .hour("hour_field")
        .truncate("truncate_field", 2)
        .bucket("bucket_field", 2)
        .identity("identity_field")
        .build();

    TableIdentifier identifier = TableIdentifier.of("default", "part_test");
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier +
        " PARTITIONED BY SPEC (year(year_field), month(month_field), day(day_field), hour(hour_field), " +
        "truncate(2, truncate_field), bucket(2, bucket_field), identity_field)" +
        " STORED BY ICEBERG " +
        testTables.locationForCreateTableSQL(identifier) +
        " TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
        SchemaParser.toJson(schema) + "', " +
        "'" + InputFormatConfig.CATALOG_NAME + "'='" + Catalogs.ICEBERG_DEFAULT_CATALOG_NAME + "')");
    Table table = testTables.loadTable(identifier);
    Assert.assertEquals(spec, table.spec());
  }

  @Test
  public void testCreateDropTable() throws TException, IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.executeStatement("CREATE EXTERNAL TABLE customers " +
        "STORED BY ICEBERG " +
        testTables.locationForCreateTableSQL(identifier) +
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
        SchemaParser.toJson(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA) + "', " +
        "'" + InputFormatConfig.PARTITION_SPEC + "'='" +
        PartitionSpecParser.toJson(PartitionSpec.unpartitioned()) + "', " +
        "'dummy'='test', " +
        "'" + InputFormatConfig.CATALOG_NAME + "'='" + Catalogs.ICEBERG_DEFAULT_CATALOG_NAME + "')");

    // Check the Iceberg table data
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA.asStruct(),
        icebergTable.schema().asStruct());
    Assert.assertEquals(PartitionSpec.unpartitioned(), icebergTable.spec());

    org.apache.hadoop.hive.metastore.api.Table hmsTable = shell.metastore().getTable("default", "customers");
    Properties tableProperties = new Properties();
    hmsTable.getParameters().entrySet().stream()
              .filter(e -> !IGNORED_PARAMS.contains(e.getKey()))
              .forEach(e -> tableProperties.put(e.getKey(), e.getValue()));
    if (!Catalogs.hiveCatalog(shell.getHiveConf(), tableProperties)) {
      shell.executeStatement("DROP TABLE customers");

      // Check if the table was really dropped even from the Catalog
      AssertHelpers.assertThrows("should throw exception", NoSuchTableException.class,
          "Table does not exist", () -> {
            testTables.loadTable(identifier);
          }
      );
    } else {
      Path hmsTableLocation = new Path(hmsTable.getSd().getLocation());

      // Drop the table
      shell.executeStatement("DROP TABLE customers");

      // Check if we drop an exception when trying to load the table
      AssertHelpers.assertThrows("should throw exception", NoSuchTableException.class,
          "Table does not exist", () -> {
            testTables.loadTable(identifier);
          }
      );

      // Check if the files are removed
      FileSystem fs = Util.getFs(hmsTableLocation, shell.getHiveConf());
      if (fs.exists(hmsTableLocation)) {
        // if table directory has been deleted, we're good. This is the expected behavior in Hive4.
        // if table directory exists, its contents should have been cleaned up, save for an empty metadata dir (Hive3).
        Assert.assertEquals(1, fs.listStatus(hmsTableLocation).length);
        Assert.assertEquals(0, fs.listStatus(new Path(hmsTableLocation, "metadata")).length);
      }
    }
  }

  @Test
  public void testCreateDropTableNonDefaultCatalog() throws TException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    String catalogName = "nondefaultcatalog";
    testTables.properties().entrySet()
        .forEach(e -> shell.setHiveSessionValue(e.getKey().replace(testTables.catalog, catalogName), e.getValue()));
    String createSql = "CREATE EXTERNAL TABLE " + identifier +
        " (customer_id BIGINT, first_name STRING COMMENT 'This is first name'," +
        " last_name STRING COMMENT 'This is last name')" +
        " STORED BY ICEBERG " +
        testTables.locationForCreateTableSQL(identifier) +
        testTables.propertiesForCreateTableSQL(ImmutableMap.of());
    shell.executeStatement(createSql);

    Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA.asStruct(),
        icebergTable.schema().asStruct());

    shell.executeStatement("DROP TABLE default.customers");
    // Check if the table was really dropped even from the Catalog
    AssertHelpers.assertThrows("should throw exception", NoSuchTableException.class,
        "Table does not exist", () -> {
          testTables.loadTable(identifier);
        }
    );
  }

  @Test
  public void testCreateTableStoredByIceberg() {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    String query = String.format("CREATE EXTERNAL TABLE customers (customer_id BIGINT, first_name STRING, last_name " +
        "STRING) STORED BY iceBerg %s TBLPROPERTIES ('%s'='%s')",
        testTables.locationForCreateTableSQL(identifier),
        InputFormatConfig.CATALOG_NAME,
        Catalogs.ICEBERG_DEFAULT_CATALOG_NAME);
    shell.executeStatement(query);
    Assert.assertNotNull(testTables.loadTable(identifier));
  }

  @Test
  public void testCreateTableStoredByIcebergWithSerdeProperties() {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    String query = String.format("CREATE EXTERNAL TABLE customers (customer_id BIGINT, first_name STRING, last_name " +
            "STRING) STORED BY iceberg WITH SERDEPROPERTIES('%s'='%s') %s TBLPROPERTIES ('%s'='%s')",
        TableProperties.DEFAULT_FILE_FORMAT,
        "orc",
        testTables.locationForCreateTableSQL(identifier),
        InputFormatConfig.CATALOG_NAME,
        Catalogs.ICEBERG_DEFAULT_CATALOG_NAME);
    shell.executeStatement(query);
    Table table = testTables.loadTable(identifier);
    Assert.assertNotNull(table);
    Assert.assertEquals("orc", table.properties().get(TableProperties.DEFAULT_FILE_FORMAT));
  }

  @Test
  public void testCreateTableWithoutSpec() {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.executeStatement("CREATE EXTERNAL TABLE customers " +
        "STORED BY ICEBERG " +
        testTables.locationForCreateTableSQL(identifier) +
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
        SchemaParser.toJson(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA) + "','" +
        InputFormatConfig.CATALOG_NAME + "'='" + Catalogs.ICEBERG_DEFAULT_CATALOG_NAME + "')");

    // Check the Iceberg table partition data
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(PartitionSpec.unpartitioned(), icebergTable.spec());
  }

  @Test
  public void testCreateTableWithUnpartitionedSpec() {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    // We need the location for HadoopTable based tests only
    shell.executeStatement("CREATE EXTERNAL TABLE customers " +
        "STORED BY ICEBERG " +
        testTables.locationForCreateTableSQL(identifier) +
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
        SchemaParser.toJson(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA) + "', " +
        "'" + InputFormatConfig.PARTITION_SPEC + "'='" +
        PartitionSpecParser.toJson(PartitionSpec.unpartitioned()) + "', " +
        "'" + InputFormatConfig.CATALOG_NAME + "'='" + Catalogs.ICEBERG_DEFAULT_CATALOG_NAME + "')");

    // Check the Iceberg table partition data
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(SPEC, icebergTable.spec());
  }

  @Test
  public void testDeleteBackingTable() throws TException, IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.executeStatement("CREATE EXTERNAL TABLE customers " +
        "STORED BY ICEBERG " +
        testTables.locationForCreateTableSQL(identifier) +
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
        SchemaParser.toJson(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA) + "', " +
        "'" + InputFormatConfig.EXTERNAL_TABLE_PURGE + "'='FALSE', " +
        "'" + InputFormatConfig.CATALOG_NAME + "'='" + Catalogs.ICEBERG_DEFAULT_CATALOG_NAME + "')");

    org.apache.hadoop.hive.metastore.api.Table hmsTable = shell.metastore().getTable("default", "customers");
    Properties tableProperties = new Properties();
    hmsTable.getParameters().entrySet().stream()
            .filter(e -> !IGNORED_PARAMS.contains(e.getKey()))
            .forEach(e -> tableProperties.put(e.getKey(), e.getValue()));
    if (!Catalogs.hiveCatalog(shell.getHiveConf(), tableProperties)) {
      shell.executeStatement("DROP TABLE customers");

      // Check if the table remains
      testTables.loadTable(identifier);
    } else {
      // Check the HMS table parameters
      Path hmsTableLocation = new Path(hmsTable.getSd().getLocation());

      // Drop the table
      shell.executeStatement("DROP TABLE customers");

      // Check if we drop an exception when trying to drop the table
      AssertHelpers.assertThrows("should throw exception", NoSuchTableException.class,
          "Table does not exist", () -> {
            testTables.loadTable(identifier);
          }
      );

      // Check if the files are kept
      FileSystem fs = Util.getFs(hmsTableLocation, shell.getHiveConf());
      Assert.assertEquals(1, fs.listStatus(hmsTableLocation).length);
      Assert.assertEquals(1, fs.listStatus(new Path(hmsTableLocation, "metadata")).length);
    }
  }

  @Test
  public void testCreateTableError() {
    TableIdentifier identifier = TableIdentifier.of("default", "withShell2");

    // Wrong schema
    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "Unrecognized token 'WrongSchema'", () -> {
          shell.executeStatement("CREATE EXTERNAL TABLE withShell2 " +
              "STORED BY ICEBERG " +
              testTables.locationForCreateTableSQL(identifier) +
              "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='WrongSchema'" +
              ",'" + InputFormatConfig.CATALOG_NAME + "'='" + Catalogs.ICEBERG_DEFAULT_CATALOG_NAME + "')");
        }
    );

    // Missing schema, we try to get the schema from the table and fail
    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "Please provide ", () -> {
          shell.executeStatement("CREATE EXTERNAL TABLE withShell2 " +
              "STORED BY ICEBERG " +
              testTables.locationForCreateTableSQL(identifier) +
              testTables.propertiesForCreateTableSQL(ImmutableMap.of()));
        }
    );

    if (!testTables.locationForCreateTableSQL(identifier).isEmpty()) {
      // Only test this if the location is required
      AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
          "Table location not set", () -> {
            shell.executeStatement("CREATE EXTERNAL TABLE withShell2 " +
                "STORED BY ICEBERG " +
                "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
                SchemaParser.toJson(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA) + "','" +
                InputFormatConfig.CATALOG_NAME + "'='" + Catalogs.ICEBERG_DEFAULT_CATALOG_NAME + "')");
          }
      );
    }
  }

  @Test
  public void testCreateTableAboveExistingTable() throws IOException {
    // Create the Iceberg table
    testTables.createIcebergTable(shell.getHiveConf(), "customers", COMPLEX_SCHEMA, FileFormat.PARQUET,
        Collections.emptyList());

    if (testTableType == TestTables.TestTableType.HIVE_CATALOG) {
      // In HiveCatalog we just expect an exception since the table is already exists
      AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
          "customers already exists", () -> {
            shell.executeStatement("CREATE EXTERNAL TABLE customers " +
                "STORED BY ICEBERG " +
                "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
                SchemaParser.toJson(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA) + "',' " +
                InputFormatConfig.CATALOG_NAME + "'='" + Catalogs.ICEBERG_DEFAULT_CATALOG_NAME + "')");
          }
      );
    } else {
      // With other catalogs, table creation should succeed
      shell.executeStatement("CREATE EXTERNAL TABLE customers " +
          "STORED BY ICEBERG " +
          testTables.locationForCreateTableSQL(TableIdentifier.of("default", "customers")) +
          testTables.propertiesForCreateTableSQL(ImmutableMap.of()));
    }
  }

  @Test
  public void testCreatePartitionedTableWithPropertiesAndWithColumnSpecification() {
    PartitionSpec spec =
        PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA).identity("last_name").build();

    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "Provide only one of the following", () -> {
          shell.executeStatement("CREATE EXTERNAL TABLE customers (customer_id BIGINT) " +
              "PARTITIONED BY (first_name STRING) " +
              "STORED BY ICEBERG " +
              testTables.locationForCreateTableSQL(TableIdentifier.of("default", "customers")) +
              " TBLPROPERTIES ('" + InputFormatConfig.PARTITION_SPEC + "'='" +
              PartitionSpecParser.toJson(spec) + "')");
        }
    );
  }

  @Test
  public void testCreateTableWithColumnSpecificationHierarchy() {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.executeStatement("CREATE EXTERNAL TABLE customers (" +
        "id BIGINT, name STRING, " +
        "employee_info STRUCT < employer: STRING, id: BIGINT, address: STRING >, " +
        "places_lived ARRAY < STRUCT <street: STRING, city: STRING, country: STRING >>, " +
        "memorable_moments MAP < STRING, STRUCT < year: INT, place: STRING, details: STRING >>, " +
        "current_address STRUCT < street_address: STRUCT " +
        "<street_number: INT, street_name: STRING, street_type: STRING>, country: STRING, postal_code: STRING >) " +
        "STORED BY ICEBERG " +
        testTables.locationForCreateTableSQL(identifier) +
        testTables.propertiesForCreateTableSQL(ImmutableMap.of()));

    // Check the Iceberg table data
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(COMPLEX_SCHEMA.asStruct(), icebergTable.schema().asStruct());
  }

  @Test
  public void testCreateTableWithAllSupportedTypes() {
    TableIdentifier identifier = TableIdentifier.of("default", "all_types");
    Schema allSupportedSchema = new Schema(
        optional(1, "t_float", Types.FloatType.get()),
        optional(2, "t_double", Types.DoubleType.get()),
        optional(3, "t_boolean", Types.BooleanType.get()),
        optional(4, "t_int", Types.IntegerType.get()),
        optional(5, "t_bigint", Types.LongType.get()),
        optional(6, "t_binary", Types.BinaryType.get()),
        optional(7, "t_string", Types.StringType.get()),
        optional(8, "t_timestamp", Types.TimestampType.withoutZone()),
        optional(9, "t_date", Types.DateType.get()),
        optional(10, "t_decimal", Types.DecimalType.of(3, 2))
    );

    // Intentionally adding some mixed letters to test that we handle them correctly
    shell.executeStatement("CREATE EXTERNAL TABLE all_types (" +
        "t_Float FLOaT, t_dOuble DOUBLE, t_boolean BOOLEAN, t_int INT, t_bigint BIGINT, t_binary BINARY, " +
        "t_string STRING, t_timestamp TIMESTAMP, t_date DATE, t_decimal DECIMAL(3,2)) " +
        "STORED BY ICEBERG " +
        testTables.locationForCreateTableSQL(identifier) +
        testTables.propertiesForCreateTableSQL(ImmutableMap.of()));

    // Check the Iceberg table data
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(allSupportedSchema.asStruct(), icebergTable.schema().asStruct());
  }

  @Test
  public void testCreateTableWithNotSupportedTypes() {
    TableIdentifier identifier = TableIdentifier.of("default", "not_supported_types");
    // Can not create INTERVAL types from normal create table, so leave them out from this test
    Map<String, Type> notSupportedTypes = ImmutableMap.of(
        "TINYINT", Types.IntegerType.get(),
        "SMALLINT", Types.IntegerType.get(),
        "VARCHAR(1)", Types.StringType.get(),
        "CHAR(1)", Types.StringType.get());

    for (String notSupportedType : notSupportedTypes.keySet()) {
      AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
          "Unsupported Hive type", () -> {
            shell.executeStatement("CREATE EXTERNAL TABLE not_supported_types " +
                "(not_supported " + notSupportedType + ") " +
                "STORED BY ICEBERG " +
                testTables.locationForCreateTableSQL(identifier) +
                testTables.propertiesForCreateTableSQL(ImmutableMap.of()));
          }
      );
    }
  }

  @Test
  public void testCreateTableWithNotSupportedTypesWithAutoConversion() {
    TableIdentifier identifier = TableIdentifier.of("default", "not_supported_types");
    // Can not create INTERVAL types from normal create table, so leave them out from this test
    Map<String, Type> notSupportedTypes = ImmutableMap.of(
        "TINYINT", Types.IntegerType.get(),
        "SMALLINT", Types.IntegerType.get(),
        "VARCHAR(1)", Types.StringType.get(),
         "CHAR(1)", Types.StringType.get());

    shell.setHiveSessionValue(InputFormatConfig.SCHEMA_AUTO_CONVERSION, "true");

    for (String notSupportedType : notSupportedTypes.keySet()) {
      shell.executeStatement("CREATE EXTERNAL TABLE not_supported_types (not_supported " + notSupportedType + ") " +
              "STORED BY ICEBERG " +
              testTables.locationForCreateTableSQL(identifier) +
              testTables.propertiesForCreateTableSQL(ImmutableMap.of()));

      org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
      Assert.assertEquals(notSupportedTypes.get(notSupportedType), icebergTable.schema().columns().get(0).type());
      shell.executeStatement("DROP TABLE not_supported_types");
    }
  }

  @Test
  public void testCreateTableWithColumnComments() {
    TableIdentifier identifier = TableIdentifier.of("default", "comment_table");
    shell.executeStatement("CREATE EXTERNAL TABLE comment_table (" +
        "t_int INT COMMENT 'int column',  " +
        "t_string STRING COMMENT 'string column') " +
        "STORED BY ICEBERG " +
        testTables.locationForCreateTableSQL(identifier) +
        testTables.propertiesForCreateTableSQL(ImmutableMap.of()));
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);

    List<Object[]> rows = shell.executeStatement("DESCRIBE default.comment_table");
    Assert.assertEquals(icebergTable.schema().columns().size(), rows.size());
    for (int i = 0; i < icebergTable.schema().columns().size(); i++) {
      Types.NestedField field = icebergTable.schema().columns().get(i);
      Assert.assertArrayEquals(new Object[] {field.name(), HiveSchemaUtil.convert(field.type()).getTypeName(),
          field.doc()}, rows.get(i));
    }
  }

  @Test
  public void testCreateTableWithoutColumnComments() {
    TableIdentifier identifier = TableIdentifier.of("default", "without_comment_table");
    shell.executeStatement("CREATE EXTERNAL TABLE without_comment_table (" +
            "t_int INT,  " +
            "t_string STRING) " +
            "STORED BY ICEBERG " +
            testTables.locationForCreateTableSQL(identifier) +
            testTables.propertiesForCreateTableSQL(ImmutableMap.of()));
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);

    List<Object[]> rows = shell.executeStatement("DESCRIBE default.without_comment_table");
    Assert.assertEquals(icebergTable.schema().columns().size(), rows.size());
    for (int i = 0; i < icebergTable.schema().columns().size(); i++) {
      Types.NestedField field = icebergTable.schema().columns().get(i);
      Assert.assertNull(field.doc());
      Assert.assertArrayEquals(new Object[] {field.name(), HiveSchemaUtil.convert(field.type()).getTypeName(),
          "from deserializer"}, rows.get(i));
    }
  }

  @Test
  public void testAlterTableProperties() {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    shell.executeStatement("CREATE EXTERNAL TABLE customers (" +
        "t_int INT,  " +
        "t_string STRING) " +
        "STORED BY ICEBERG " +
        testTables.locationForCreateTableSQL(identifier) +
        testTables.propertiesForCreateTableSQL(ImmutableMap.of()));
    String propKey = "dummy";
    String propValue = "dummy_val";
    // add new property
    shell.executeStatement(String.format("ALTER TABLE customers SET TBLPROPERTIES('%s'='%s')", propKey, propValue));
    // Check the Iceberg table parameters
    Table icebergTable = testTables.loadTable(identifier);
    Assert.assertTrue(icebergTable.properties().containsKey(propKey));
    Assert.assertEquals(icebergTable.properties().get(propKey), propValue);
    // update existing property
    propValue = "new_dummy_val";
    shell.executeStatement(String.format("ALTER TABLE customers SET TBLPROPERTIES('%s'='%s')", propKey, propValue));
    // Check the Iceberg table parameters
    icebergTable.refresh();
    Assert.assertTrue(icebergTable.properties().containsKey(propKey));
    Assert.assertEquals(icebergTable.properties().get(propKey), propValue);
    // remove existing property
    shell.executeStatement(String.format("ALTER TABLE customers UNSET TBLPROPERTIES('%s'='%s')", propKey, propValue));
    // Check the Iceberg table parameters
    icebergTable.refresh();
    Assert.assertFalse(icebergTable.properties().containsKey(propKey));
  }

  @Test
  public void testIcebergAndHmsTableProperties() throws Exception {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.executeStatement(String.format("CREATE EXTERNAL TABLE default.customers " +
        "STORED BY ICEBERG %s" +
        "TBLPROPERTIES ('%s'='%s', '%s'='%s', '%s'='%s', '%s'='%s')",
        testTables.locationForCreateTableSQL(identifier), // we need the location for HadoopTable based tests only
        InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA),
        InputFormatConfig.PARTITION_SPEC, PartitionSpecParser.toJson(SPEC),
        "custom_property", "initial_val",
        InputFormatConfig.CATALOG_NAME, Catalogs.ICEBERG_DEFAULT_CATALOG_NAME));


    // Check the Iceberg table parameters
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);

    Map<String, String> expectedIcebergProperties = new HashMap<>();
    expectedIcebergProperties.put("custom_property", "initial_val");
    expectedIcebergProperties.put("EXTERNAL", "TRUE");
    expectedIcebergProperties.put("storage_handler", HiveIcebergStorageHandler.class.getName());
    expectedIcebergProperties.put(serdeConstants.SERIALIZATION_FORMAT, "1");

    // Check the HMS table parameters
    org.apache.hadoop.hive.metastore.api.Table hmsTable = shell.metastore().getTable("default", "customers");
    Map<String, String> hmsParams = hmsTable.getParameters()
            .entrySet().stream()
            .filter(e -> !IGNORED_PARAMS.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Properties tableProperties = new Properties();
    tableProperties.putAll(hmsParams);

    if (Catalogs.hiveCatalog(shell.getHiveConf(), tableProperties)) {
      expectedIcebergProperties.put(TableProperties.ENGINE_HIVE_ENABLED, "true");
    }
    if (MetastoreUtil.hive3PresentOnClasspath()) {
      expectedIcebergProperties.put("bucketing_version", "2");
    }
    Assert.assertEquals(expectedIcebergProperties, icebergTable.properties());

    if (Catalogs.hiveCatalog(shell.getHiveConf(), tableProperties)) {
      Assert.assertEquals(11, hmsParams.size());
      Assert.assertEquals("initial_val", hmsParams.get("custom_property"));
      Assert.assertEquals("TRUE", hmsParams.get(InputFormatConfig.EXTERNAL_TABLE_PURGE));
      Assert.assertEquals("TRUE", hmsParams.get("EXTERNAL"));
      Assert.assertEquals("true", hmsParams.get(TableProperties.ENGINE_HIVE_ENABLED));
      Assert.assertEquals(HiveIcebergStorageHandler.class.getName(),
          hmsParams.get(hive_metastoreConstants.META_TABLE_STORAGE));
      Assert.assertEquals(BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase(),
          hmsParams.get(BaseMetastoreTableOperations.TABLE_TYPE_PROP));
      Assert.assertEquals(hmsParams.get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP),
              getCurrentSnapshotForHiveCatalogTable(icebergTable));
      Assert.assertNull(hmsParams.get(BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP));
      Assert.assertNotNull(hmsParams.get(hive_metastoreConstants.DDL_TIME));
      Assert.assertNotNull(hmsParams.get(InputFormatConfig.PARTITION_SPEC));
      Assert.assertNotNull(hmsParams.get(serdeConstants.SERIALIZATION_FORMAT));
    } else {
      Assert.assertEquals(8, hmsParams.size());
      Assert.assertNull(hmsParams.get(TableProperties.ENGINE_HIVE_ENABLED));
    }

    // Check HMS inputformat/outputformat/serde
    Assert.assertEquals(HiveIcebergInputFormat.class.getName(), hmsTable.getSd().getInputFormat());
    Assert.assertEquals(HiveIcebergOutputFormat.class.getName(), hmsTable.getSd().getOutputFormat());
    Assert.assertEquals(HiveIcebergSerDe.class.getName(), hmsTable.getSd().getSerdeInfo().getSerializationLib());

    // Add two new properties to the Iceberg table and update an existing one
    icebergTable.updateProperties()
        .set("new_prop_1", "true")
        .set("new_prop_2", "false")
        .set("custom_property", "new_val")
        .commit();

    // Refresh the HMS table to see if new Iceberg properties got synced into HMS
    hmsParams = shell.metastore().getTable("default", "customers").getParameters()
        .entrySet().stream()
        .filter(e -> !IGNORED_PARAMS.contains(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (Catalogs.hiveCatalog(shell.getHiveConf(), tableProperties)) {
      Assert.assertEquals(14, hmsParams.size()); // 2 newly-added properties + previous_metadata_location prop
      Assert.assertEquals("true", hmsParams.get("new_prop_1"));
      Assert.assertEquals("false", hmsParams.get("new_prop_2"));
      Assert.assertEquals("new_val", hmsParams.get("custom_property"));
      String prevSnapshot = getCurrentSnapshotForHiveCatalogTable(icebergTable);
      icebergTable.refresh();
      String newSnapshot = getCurrentSnapshotForHiveCatalogTable(icebergTable);
      Assert.assertEquals(hmsParams.get(BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP), prevSnapshot);
      Assert.assertEquals(hmsParams.get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP), newSnapshot);
    } else {
      Assert.assertEquals(8, hmsParams.size());
    }

    // Remove some Iceberg props and see if they're removed from HMS table props as well
    if (Catalogs.hiveCatalog(shell.getHiveConf(), tableProperties)) {
      icebergTable.updateProperties()
          .remove("custom_property")
          .remove("new_prop_1")
          .commit();
      hmsParams = shell.metastore().getTable("default", "customers").getParameters();
      Assert.assertFalse(hmsParams.containsKey("custom_property"));
      Assert.assertFalse(hmsParams.containsKey("new_prop_1"));
      Assert.assertTrue(hmsParams.containsKey("new_prop_2"));
    }

    // append some data and check whether HMS stats are aligned with snapshot summary
    if (Catalogs.hiveCatalog(shell.getHiveConf(), tableProperties)) {
      List<Record> records = HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS;
      testTables.appendIcebergTable(shell.getHiveConf(), icebergTable, FileFormat.PARQUET, null, records);
      hmsParams = shell.metastore().getTable("default", "customers").getParameters();
      Map<String, String> summary = icebergTable.currentSnapshot().summary();
      Assert.assertEquals(summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP), hmsParams.get(StatsSetupConst.NUM_FILES));
      Assert.assertEquals(summary.get(SnapshotSummary.TOTAL_RECORDS_PROP), hmsParams.get(StatsSetupConst.ROW_COUNT));
      Assert.assertEquals(summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP), hmsParams.get(StatsSetupConst.TOTAL_SIZE));
    }
  }

  @Test
  public void testIcebergHMSPropertiesTranslation() throws Exception {
    Assume.assumeTrue("Iceberg - HMS property translation is only relevant for HiveCatalog",
        testTableType == TestTables.TestTableType.HIVE_CATALOG);

    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    // Create HMS table with with a property to be translated
    shell.executeStatement(String.format("CREATE EXTERNAL TABLE default.customers " +
        "STORED BY ICEBERG " +
        "TBLPROPERTIES ('%s'='%s', '%s'='%s', '%s'='%s')",
        InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA),
        InputFormatConfig.PARTITION_SPEC, PartitionSpecParser.toJson(SPEC),
        InputFormatConfig.EXTERNAL_TABLE_PURGE, "false"));

    // Check that HMS table prop was translated to equivalent Iceberg prop (purge -> gc.enabled)
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals("false", icebergTable.properties().get(GC_ENABLED));
    Assert.assertNull(icebergTable.properties().get(InputFormatConfig.EXTERNAL_TABLE_PURGE));

    // Change Iceberg prop
    icebergTable.updateProperties()
        .set(GC_ENABLED, "true")
        .commit();

    // Check that Iceberg prop was translated to equivalent HMS prop (gc.enabled -> purge)
    Map<String, String> hmsParams = shell.metastore().getTable("default", "customers").getParameters();
    Assert.assertEquals("true", hmsParams.get(InputFormatConfig.EXTERNAL_TABLE_PURGE));
    Assert.assertNull(hmsParams.get(GC_ENABLED));
  }

  @Test
  public void testDropTableWithAppendedData() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, SPEC,
        FileFormat.PARQUET, ImmutableList.of());

    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    testTables.appendIcebergTable(shell.getHiveConf(), icebergTable, FileFormat.PARQUET, null,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    shell.executeStatement("DROP TABLE customers");
  }

  @Test
  public void testDropHiveTableWithoutUnderlyingTable() throws IOException {
    Assume.assumeFalse("Not relevant for HiveCatalog",
            testTableType.equals(TestTables.TestTableType.HIVE_CATALOG));

    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    // Create the Iceberg table in non-HiveCatalog
    testTables.createIcebergTable(shell.getHiveConf(), identifier.name(),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, FileFormat.PARQUET,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // Create Hive table on top
    String tableLocation = testTables.locationForCreateTableSQL(identifier);
    shell.executeStatement(testTables.createHiveTableSQL(identifier,
        ImmutableMap.of(InputFormatConfig.EXTERNAL_TABLE_PURGE, "TRUE")));

    // Drop the Iceberg table
    Properties properties = new Properties();
    properties.put(Catalogs.NAME, identifier.toString());
    properties.put(Catalogs.LOCATION, tableLocation);
    Catalogs.dropTable(shell.getHiveConf(), properties);

    // Finally drop the Hive table as well
    shell.executeStatement("DROP TABLE " + identifier);
  }

  @Test
  public void testAlterTableAddColumns() throws Exception {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, SPEC,
        FileFormat.PARQUET, ImmutableList.of());

    shell.executeStatement("ALTER TABLE default.customers ADD COLUMNS " +
        "(newintcol int, newstringcol string COMMENT 'Column with description')");

    verifyAlterTableAddColumnsTests();
  }

  @Test
  public void testAlterTableAddColumnsConcurrently() throws Exception {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, SPEC,
        FileFormat.PARQUET, ImmutableList.of());

    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);

    UpdateSchema updateSchema = icebergTable.updateSchema().addColumn("newfloatcol", Types.FloatType.get());

    shell.executeStatement("ALTER TABLE default.customers ADD COLUMNS " +
        "(newintcol int, newstringcol string COMMENT 'Column with description')");

    try {
      updateSchema.commit();
      Assert.fail();
    } catch (CommitFailedException expectedException) {
      // Should fail to commit the addition of newfloatcol as another commit went in from Hive side adding 2 other cols
    }

    // Same verification should be applied, as we expect newfloatcol NOT to be added to the schema
    verifyAlterTableAddColumnsTests();
  }

  @Test
  public void testAlterTableReplaceColumns() throws TException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    Schema schema = new Schema(
        optional(1, "customer_id", Types.IntegerType.get()),
        optional(2, "first_name", Types.StringType.get(), "This is first name"),
        optional(3, "last_name", Types.StringType.get(), "This is last name"),
        optional(4, "address",  Types.StructType.of(
            optional(5, "city", Types.StringType.get()),
            optional(6, "street", Types.StringType.get())), null)
    );
    testTables.createTable(shell, identifier.name(), schema, SPEC, FileFormat.PARQUET, ImmutableList.of());

    shell.executeStatement("ALTER TABLE default.customers REPLACE COLUMNS " +
        "(customer_id bigint, last_name string COMMENT 'This is last name', " +
        "address struct<city:string,street:string> COMMENT 'Adding some comment', " +
        "new_col string COMMENT 'This is a new column added')");

    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    org.apache.hadoop.hive.metastore.api.Table hmsTable = shell.metastore().getTable("default", "customers");

    List<FieldSchema> icebergSchema = HiveSchemaUtil.convert(icebergTable.schema());
    List<FieldSchema> hmsSchema = hmsTable.getSd().getCols();

    List<FieldSchema> expectedSchema = Lists.newArrayList(
        // customer_id: type promotion (int -> bigint), no change in comment
        new FieldSchema("customer_id", "bigint", null),
        // first_name column is dropped
        // last_name: no changes
        new FieldSchema("last_name", "string", "This is last name"),
        // address: comment added, no change in type
        new FieldSchema("address", "struct<city:string,street:string>", "Adding some comment"),
        // new_col: brand new column
        new FieldSchema("new_col", "string", "This is a new column added"));

    Assert.assertEquals(expectedSchema, icebergSchema);
    Assert.assertEquals(expectedSchema, hmsSchema);
  }

  /**
   * Checks that HiveIcebergMetaHook doesn't run into failures with undefined alter operation type (e.g. stat updates)
   * @throws Exception - any test failure
   */
  @Test
  public void testMetaHookWithUndefinedAlterOperationType() throws Exception {
    Assume.assumeTrue("Enough to check for one type only",
        testTableType.equals(TestTables.TestTableType.HIVE_CATALOG));
    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, SPEC,
        FileFormat.PARQUET, ImmutableList.of());
    org.apache.hadoop.hive.metastore.api.Table hmsTable = shell.metastore().getTable("default", "customers");

    HiveIcebergMetaHook metaHook = new HiveIcebergMetaHook(shell.getHiveConf());
    EnvironmentContext environmentContext = new EnvironmentContext(new HashMap<>());

    metaHook.preAlterTable(hmsTable, environmentContext);
    metaHook.commitAlterTable(hmsTable, environmentContext, null);
  }

  /**
   * Checks that the new schema has newintcol and newstring col columns on both HMS and Iceberg sides
   * @throws Exception - any test error
   */
  private void verifyAlterTableAddColumnsTests() throws Exception {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    org.apache.hadoop.hive.metastore.api.Table hmsTable = shell.metastore().getTable("default", "customers");

    List<FieldSchema> icebergSchema = HiveSchemaUtil.convert(icebergTable.schema());
    List<FieldSchema> hmsSchema = hmsTable.getSd().getCols();

    List<FieldSchema> expectedSchema = Lists.newArrayList(
        new FieldSchema("customer_id", "bigint", null),
        new FieldSchema("first_name", "string", "This is first name"),
        new FieldSchema("last_name", "string", "This is last name"),
        new FieldSchema("newintcol", "int", null),
        new FieldSchema("newstringcol", "string", "Column with description"));

    Assert.assertEquals(expectedSchema, icebergSchema);

    if (testTableType != TestTables.TestTableType.HIVE_CATALOG) {
      expectedSchema.get(0).setComment("from deserializer");
    }

    Assert.assertEquals(expectedSchema, hmsSchema);
  }

  private String getCurrentSnapshotForHiveCatalogTable(org.apache.iceberg.Table icebergTable) {
    return ((BaseMetastoreTableOperations) ((BaseTable) icebergTable).operations()).currentMetadataLocation();
  }
}
