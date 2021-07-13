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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.hive.MetastoreUtil;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
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
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;


@RunWith(Parameterized.class)
public class TestHiveIcebergStorageHandlerWithEngine {

  private static final String[] EXECUTION_ENGINES = new String[] {"tez"};

  private static final Schema ORDER_SCHEMA = new Schema(
          required(1, "order_id", Types.LongType.get()),
          required(2, "customer_id", Types.LongType.get()),
          required(3, "total", Types.DoubleType.get()),
          required(4, "product_id", Types.LongType.get())
  );

  private static final List<Record> ORDER_RECORDS = TestHelper.RecordsBuilder.newInstance(ORDER_SCHEMA)
          .add(100L, 0L, 11.11d, 1L)
          .add(101L, 0L, 22.22d, 2L)
          .add(102L, 1L, 33.33d, 3L)
          .build();

  private static final Schema PRODUCT_SCHEMA = new Schema(
          optional(1, "id", Types.LongType.get()),
          optional(2, "name", Types.StringType.get()),
          optional(3, "price", Types.DoubleType.get())
  );

  private static final List<Record> PRODUCT_RECORDS = TestHelper.RecordsBuilder.newInstance(PRODUCT_SCHEMA)
          .add(1L, "skirt", 11.11d)
          .add(2L, "tee", 22.22d)
          .add(3L, "watch", 33.33d)
          .build();

  private static final List<Type> SUPPORTED_TYPES =
          ImmutableList.of(Types.BooleanType.get(), Types.IntegerType.get(), Types.LongType.get(),
                  Types.FloatType.get(), Types.DoubleType.get(), Types.DateType.get(), Types.TimestampType.withZone(),
                  Types.TimestampType.withoutZone(), Types.StringType.get(), Types.BinaryType.get(),
                  Types.DecimalType.of(3, 1), Types.UUIDType.get(), Types.FixedType.ofLength(5),
                  Types.TimeType.get());

  private static final Map<String, String> STATS_MAPPING = ImmutableMap.of(
      StatsSetupConst.NUM_FILES, SnapshotSummary.TOTAL_DATA_FILES_PROP,
      StatsSetupConst.ROW_COUNT, SnapshotSummary.TOTAL_RECORDS_PROP,
      StatsSetupConst.TOTAL_SIZE, SnapshotSummary.TOTAL_FILE_SIZE_PROP
  );

  @Parameters(name = "fileFormat={0}, engine={1}, catalog={2}, isVectorized={3}")
  public static Collection<Object[]> parameters() {
    Collection<Object[]> testParams = new ArrayList<>();
    String javaVersion = System.getProperty("java.specification.version");

    // Run tests with every FileFormat for a single Catalog (HiveCatalog)
    for (FileFormat fileFormat : HiveIcebergStorageHandlerTestUtils.FILE_FORMATS) {
      for (String engine : EXECUTION_ENGINES) {
        // include Tez tests only for Java 8
        if (javaVersion.equals("1.8")) {
          testParams.add(new Object[] {fileFormat, engine, TestTables.TestTableType.HIVE_CATALOG, false});
          // test for vectorization=ON in case of ORC format and Tez engine
          if (fileFormat == FileFormat.ORC && "tez".equals(engine) && MetastoreUtil.hive3PresentOnClasspath()) {
            testParams.add(new Object[] {fileFormat, engine, TestTables.TestTableType.HIVE_CATALOG, true});
          }
        }
      }
    }

    // Run tests for every Catalog for a single FileFormat (PARQUET) and execution engine (tez)
    // skip HiveCatalog tests as they are added before
    for (TestTables.TestTableType testTableType : TestTables.ALL_TABLE_TYPES) {
      if (!TestTables.TestTableType.HIVE_CATALOG.equals(testTableType)) {
        testParams.add(new Object[]{FileFormat.PARQUET, "tez", testTableType, false});
      }
    }

    return testParams;
  }

  private static TestHiveShell shell;

  private TestTables testTables;

  @Parameter(0)
  public FileFormat fileFormat;

  @Parameter(1)
  public String executionEngine;

  @Parameter(2)
  public TestTables.TestTableType testTableType;

  @Parameter(3)
  public boolean isVectorized;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Rule
  public Timeout timeout = new Timeout(400_000, TimeUnit.MILLISECONDS);

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
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp, executionEngine);
    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, isVectorized);
  }

  @After
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
    // Mixing mr and tez jobs within the same JVM can cause problems. Mr jobs set the ExecMapper status to done=false
    // at the beginning and to done=true at the end. However, tez jobs also rely on this value to see if they should
    // proceed, but they do not reset it to done=false at the beginning. Therefore, without calling this after each test
    // case, any tez job that follows a completed mr job will erroneously read done=true and will not proceed.
    ExecMapper.setDone(false);
  }

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
  public void testMigrateHiveTableToIceberg() throws TException, InterruptedException {
    Assume.assumeTrue(fileFormat == FileFormat.AVRO || fileFormat == FileFormat.PARQUET);
    String tableName = "tbl";
    String createQuery = "CREATE EXTERNAL TABLE " +  tableName + " (a int) STORED AS " + fileFormat.name() + " " +
        testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName));
    shell.executeStatement(createQuery);
    shell.executeStatement("INSERT INTO " + tableName + " VALUES (1), (2), (3)");
    validateMigration(tableName);
  }

  @Test
  public void testMigratePartitionedHiveTableToIceberg() throws TException, InterruptedException {
    Assume.assumeTrue(fileFormat == FileFormat.AVRO || fileFormat == FileFormat.PARQUET);
    String tableName = "tbl_part";
    shell.executeStatement("CREATE EXTERNAL TABLE " + tableName + " (a int) PARTITIONED BY (b string) STORED AS " +
        fileFormat.name() + " " + testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName)));
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='aaa') VALUES (1), (2), (3)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='bbb') VALUES (4), (5)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ccc') VALUES (6)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ddd') VALUES (7), (8), (9), (10)");
    validateMigration(tableName);
  }

  @Test
  public void testMigratePartitionedBucketedHiveTableToIceberg() throws TException, InterruptedException {
    Assume.assumeTrue(fileFormat == FileFormat.AVRO || fileFormat == FileFormat.PARQUET);
    String tableName = "tbl_part_bucketed";
    shell.executeStatement("CREATE EXTERNAL TABLE " + tableName + " (a int) PARTITIONED BY (b string) clustered by " +
        "(a) INTO 2 BUCKETS STORED AS " + fileFormat.name() + " " +
        testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName)));
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='aaa') VALUES (1), (2), (3)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='bbb') VALUES (4), (5)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ccc') VALUES (6)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ddd') VALUES (7), (8), (9), (10)");
    validateMigration(tableName);
  }

  @Test
  public void testRollbackMigrateHiveTableToIceberg() throws TException, InterruptedException {
    Assume.assumeTrue(fileFormat == FileFormat.AVRO || fileFormat == FileFormat.PARQUET);
    String tableName = "tbl_rollback";
    shell.executeStatement("CREATE EXTERNAL TABLE " +  tableName + " (a int) STORED AS " + fileFormat.name() + " " +
        testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName)));
    shell.executeStatement("INSERT INTO " + tableName + " VALUES (1), (2), (3)");
    validateMigrationRollback(tableName);
  }

  @Test
  public void testRollbackMigratePartitionedHiveTableToIceberg() throws TException, InterruptedException {
    Assume.assumeTrue(fileFormat == FileFormat.AVRO || fileFormat == FileFormat.PARQUET);
    String tableName = "tbl_rollback";
    shell.executeStatement("CREATE EXTERNAL TABLE " + tableName + " (a int) PARTITIONED BY (b string) STORED AS " +
        fileFormat.name() + " " + testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName)));
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='aaa') VALUES (1), (2), (3)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='bbb') VALUES (4), (5)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ccc') VALUES (6)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ddd') VALUES (7), (8), (9), (10)");
    validateMigrationRollback(tableName);
  }

  @Test
  public void testRollbackMultiPartitionedHiveTableToIceberg() throws TException, InterruptedException {
    Assume.assumeTrue(fileFormat == FileFormat.AVRO || fileFormat == FileFormat.PARQUET);
    String tableName = "tbl_rollback";
    shell.executeStatement("CREATE EXTERNAL TABLE " + tableName + " (a int) PARTITIONED BY (b string, c int) " +
        "STORED AS " + fileFormat.name() + " " +
        testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName)));
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='aaa', c='111') VALUES (1), (2), (3)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='bbb', c='111') VALUES (4), (5)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='aaa', c='222') VALUES (6)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ccc', c='333') VALUES (7), (8), (9), (10)");
    validateMigrationRollback(tableName);
  }

  @Test
  public void testRollbackMigratePartitionedBucketedHiveTableToIceberg() throws TException, InterruptedException {
    Assume.assumeTrue(fileFormat == FileFormat.AVRO || fileFormat == FileFormat.PARQUET);
    String tableName = "tbl_part_bucketed";
    shell.executeStatement("CREATE EXTERNAL TABLE " + tableName + " (a int) PARTITIONED BY (b string) clustered by " +
        "(a) INTO 2 BUCKETS STORED AS " + fileFormat.name() + " " +
        testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName)));
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='aaa') VALUES (1), (2), (3)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='bbb') VALUES (4), (5)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ccc') VALUES (6)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ddd') VALUES (7), (8), (9), (10)");
    validateMigrationRollback(tableName);
  }

  @Test
  public void testAnalyzeTableComputeStatistics() throws IOException, TException, InterruptedException {
    String dbName = "default";
    String tableName = "customers";
    Table table = testTables
        .createTable(shell, tableName, HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    shell.executeStatement("ANALYZE TABLE " + dbName + "." + tableName + " COMPUTE STATISTICS");
    validateBasicStats(table, dbName, tableName);
  }

  @Test
  public void testAnalyzeTableComputeStatisticsForColumns() throws IOException, TException, InterruptedException {
    String dbName = "default";
    String tableName = "orders";
    Table table = testTables.createTable(shell, tableName, ORDER_SCHEMA, fileFormat, ORDER_RECORDS);
    shell.executeStatement("ANALYZE TABLE " + dbName + "." + tableName + " COMPUTE STATISTICS FOR COLUMNS");
    validateBasicStats(table, dbName, tableName);
  }

  @Test
  public void testAnalyzeTableComputeStatisticsEmptyTable() throws IOException, TException, InterruptedException {
    String dbName = "default";
    String tableName = "customers";
    Table table = testTables
        .createTable(shell, tableName, HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
            new ArrayList<>());
    shell.executeStatement("ANALYZE TABLE " + dbName + "." + tableName + " COMPUTE STATISTICS");
    validateBasicStats(table, dbName, tableName);
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
  public void testDescribeTable() throws IOException {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    List<Object[]> rows = shell.executeStatement("DESCRIBE default.customers");
    Assert.assertEquals(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA.columns().size(), rows.size());
    for (int i = 0; i < HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA.columns().size(); i++) {
      Types.NestedField field = HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA.columns().get(i);
      String comment = field.doc() == null ? "from deserializer" : field.doc();
      Assert.assertArrayEquals(new Object[] {field.name(), HiveSchemaUtil.convert(field.type()).getTypeName(),
          comment}, rows.get(i));
    }
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
      if (type == Types.TimestampType.withZone() && isVectorized) {
        // ORC/TIMESTAMP_INSTANT is not a supported vectorized type for Hive
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
      if (type == Types.TimestampType.withZone() && isVectorized) {
        // ORC/TIMESTAMP_INSTANT is not a supported vectorized type for Hive
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
  public void testSpecialCharacters() {
    TableIdentifier table = TableIdentifier.of("default", "tar,! ,get");
    // note: the Chinese character seems to be accepted in the column name, but not
    // in the table name - this is the case for both Iceberg and standard Hive tables.
    shell.executeStatement(String.format(
        "CREATE TABLE `%s` (id bigint, `dep,! 是,t` string) STORED BY ICEBERG STORED AS %s %s TBLPROPERTIES ('%s'='%s')",
        table.name(), fileFormat, testTables.locationForCreateTableSQL(table),
        InputFormatConfig.CATALOG_NAME, Catalogs.ICEBERG_DEFAULT_CATALOG_NAME));
    shell.executeStatement(String.format("INSERT INTO `%s` VALUES (1, 'moon'), (2, 'star')", table.name()));

    List<Object[]> result = shell.executeStatement(String.format(
        "SELECT `dep,! 是,t`, id FROM `%s` ORDER BY id", table.name()));

    Assert.assertEquals(2, result.size());
    Assert.assertArrayEquals(new Object[]{"moon", 1L}, result.get(0));
    Assert.assertArrayEquals(new Object[]{"star", 2L}, result.get(1));
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
  public void testCTASFromHiveTable() {
    Assume.assumeTrue(HiveIcebergSerDe.CTAS_EXCEPTION_MSG, testTableType == TestTables.TestTableType.HIVE_CATALOG);

    shell.executeStatement("CREATE TABLE source (id bigint, name string) PARTITIONED BY (dept string) STORED AS ORC");
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, TableIdentifier.of("default", "source"), false));

    shell.executeStatement(String.format(
        "CREATE TABLE target STORED BY ICEBERG %s TBLPROPERTIES ('%s'='%s') AS SELECT * FROM source",
        testTables.locationForCreateTableSQL(TableIdentifier.of("default", "target")),
        TableProperties.DEFAULT_FILE_FORMAT, fileFormat));

    List<Object[]> objects = shell.executeStatement("SELECT * FROM target ORDER BY id");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
  }

  @Test
  public void testCTASPartitionedFromHiveTable() throws TException, InterruptedException {
    Assume.assumeTrue(HiveIcebergSerDe.CTAS_EXCEPTION_MSG, testTableType == TestTables.TestTableType.HIVE_CATALOG);

    shell.executeStatement("CREATE TABLE source (id bigint, name string) PARTITIONED BY (dept string) STORED AS ORC");
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, TableIdentifier.of("default", "source"), false));

    shell.executeStatement(String.format(
        "CREATE TABLE target PARTITIONED BY (dept, name) " +
        "STORED BY ICEBERG TBLPROPERTIES ('%s'='%s') AS SELECT * FROM source",
        TableProperties.DEFAULT_FILE_FORMAT, fileFormat));

    // check table can be read back correctly
    List<Object[]> objects = shell.executeStatement("SELECT id, name, dept FROM target ORDER BY id");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);

    // check HMS table has been created correctly (no partition cols, props pushed down)
    org.apache.hadoop.hive.metastore.api.Table hmsTable = shell.metastore().getTable("default", "target");
    Assert.assertEquals(3, hmsTable.getSd().getColsSize());
    Assert.assertTrue(hmsTable.getPartitionKeys().isEmpty());
    Assert.assertEquals(fileFormat.toString(), hmsTable.getParameters().get(TableProperties.DEFAULT_FILE_FORMAT));

    // check Iceberg table has correct partition spec
    Table table = testTables.loadTable(TableIdentifier.of("default", "target"));
    Assert.assertEquals(2, table.spec().fields().size());
    Assert.assertEquals("dept", table.spec().fields().get(0).name());
    Assert.assertEquals("name", table.spec().fields().get(1).name());
  }

  @Test
  public void testCTASFailureRollback() throws IOException {
    Assume.assumeTrue(HiveIcebergSerDe.CTAS_EXCEPTION_MSG, testTableType == TestTables.TestTableType.HIVE_CATALOG);

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
  public void testWriteArrayOfPrimitivesInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "arrayofprimitives",
            Types.ListType.ofRequired(3, Types.StringType.get())));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteArrayOfArraysInTable() throws IOException {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "arrayofarrays",
                Types.ListType.ofRequired(3, Types.ListType.ofRequired(4, Types.StringType.get()))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 3, 1L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteArrayOfMapsInTable() throws IOException {
    Schema schema =
        new Schema(required(1, "id", Types.LongType.get()),
            required(2, "arrayofmaps", Types.ListType
                .ofRequired(3, Types.MapType.ofRequired(4, 5, Types.StringType.get(),
                    Types.StringType.get()))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 1L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteArrayOfStructsInTable() throws IOException {
    Schema schema =
        new Schema(required(1, "id", Types.LongType.get()),
            required(2, "arrayofstructs", Types.ListType.ofRequired(3, Types.StructType
                .of(required(4, "something", Types.StringType.get()), required(5, "someone",
                    Types.StringType.get()), required(6, "somewhere", Types.StringType.get())))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteMapOfPrimitivesInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "mapofprimitives", Types.MapType.ofRequired(3, 4, Types.StringType.get(),
            Types.StringType.get())));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteMapOfArraysInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "mapofarrays",
            Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.ListType.ofRequired(5,
                Types.StringType.get()))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteMapOfMapsInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "mapofmaps", Types.MapType.ofRequired(3, 4, Types.StringType.get(),
            Types.MapType.ofRequired(5, 6, Types.StringType.get(), Types.StringType.get()))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteMapOfStructsInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "mapofstructs", Types.MapType.ofRequired(3, 4, Types.StringType.get(),
            Types.StructType.of(required(5, "something", Types.StringType.get()),
                required(6, "someone", Types.StringType.get()),
                required(7, "somewhere", Types.StringType.get())))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteStructOfPrimitivesInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "structofprimitives",
            Types.StructType.of(required(3, "key", Types.StringType.get()), required(4, "value",
                Types.StringType.get()))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteStructOfArraysInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "structofarrays", Types.StructType
            .of(required(3, "names", Types.ListType.ofRequired(4, Types.StringType.get())),
                required(5, "birthdays", Types.ListType.ofRequired(6,
                    Types.StringType.get())))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 1L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteStructOfMapsInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "structofmaps", Types.StructType
            .of(required(3, "map1", Types.MapType.ofRequired(4, 5,
                Types.StringType.get(), Types.StringType.get())), required(6, "map2",
                Types.MapType.ofRequired(7, 8, Types.StringType.get(),
                    Types.StringType.get())))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteStructOfStructsInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "structofstructs", Types.StructType.of(required(3, "struct1", Types.StructType
            .of(required(4, "key", Types.StringType.get()), required(5, "value",
                Types.StringType.get()))))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
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
    Assume.assumeTrue("ORC/TIMESTAMP_INSTANT is not a supported vectorized type for Hive", isVectorized);
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
    shell.executeStatement(String.format("CREATE EXTERNAL TABLE %s (id bigint, name string) STORED BY '%s' %s",
        identifier,
        HiveIcebergStorageHandler.class.getName(),
        testTables.locationForCreateTableSQL(identifier)));

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
  public void testDecimalTableWithPredicateLiterals() throws IOException {
    Schema schema = new Schema(required(1, "decimal_field", Types.DecimalType.of(7, 2)));
    List<Record> records = TestHelper.RecordsBuilder.newInstance(schema)
        .add(new BigDecimal("85.00"))
        .add(new BigDecimal("100.56"))
        .add(new BigDecimal("100.57"))
        .build();
    testTables.createTable(shell, "dec_test", schema, fileFormat, records);

    // Use integer literal in predicate
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.dec_test where decimal_field >= 85");
    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {"85.00"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {"100.56"}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {"100.57"}, rows.get(2));

    // Use decimal literal in predicate with smaller scale than schema type definition
    rows = shell.executeStatement("SELECT * FROM default.dec_test where decimal_field > 99.1");
    Assert.assertEquals(2, rows.size());
    Assert.assertArrayEquals(new Object[] {"100.56"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {"100.57"}, rows.get(1));

    // Use decimal literal in predicate with higher scale than schema type definition
    rows = shell.executeStatement("SELECT * FROM default.dec_test where decimal_field > 100.565");
    Assert.assertEquals(1, rows.size());
    Assert.assertArrayEquals(new Object[] {"100.57"}, rows.get(0));

    // Use decimal literal in predicate with the same scale as schema type definition
    rows = shell.executeStatement("SELECT * FROM default.dec_test where decimal_field > 640.34");
    Assert.assertEquals(0, rows.size());
  }

  @Test
  public void testStructOfMapsInTable() throws IOException {
    Schema schema = new Schema(
        required(1, "structofmaps", Types.StructType
            .of(required(2, "map1", Types.MapType.ofRequired(3, 4,
                Types.StringType.get(), Types.StringType.get())), required(5, "map2",
                Types.MapType.ofRequired(6, 7, Types.StringType.get(),
                    Types.IntegerType.get())))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "structtable", schema, fileFormat, 1);
    // access a map entry inside a struct
    for (int i = 0; i < records.size(); i++) {
      GenericRecord expectedStruct = (GenericRecord) records.get(i).getField("structofmaps");
      Map<?, ?> expectedMap = (Map<?, ?>) expectedStruct.getField("map1");
      for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
        List<Object[]> queryResult = shell.executeStatement(String
            .format("SELECT structofmaps.map1[\"%s\"] from default.structtable LIMIT 1 OFFSET %d", entry.getKey(),
                i));
        Assert.assertEquals(entry.getValue(), queryResult.get(0)[0]);
      }
      expectedMap = (Map<?, ?>) expectedStruct.getField("map2");
      for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
        List<Object[]> queryResult = shell.executeStatement(String
            .format("SELECT structofmaps.map2[\"%s\"] from default.structtable LIMIT 1 OFFSET %d", entry.getKey(),
                i));
        Assert.assertEquals(entry.getValue(), queryResult.get(0)[0]);
      }
    }
  }

  @Test
  public void testStructOfArraysInTable() throws IOException {
    Schema schema = new Schema(
        required(1, "structofarrays", Types.StructType
            .of(required(2, "names", Types.ListType.ofRequired(3, Types.StringType.get())),
                required(4, "birthdays", Types.ListType.ofRequired(5,
                    Types.DateType.get())))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "structtable", schema, fileFormat, 1);
    // access an element of an array inside a struct
    for (int i = 0; i < records.size(); i++) {
      GenericRecord expectedStruct = (GenericRecord) records.get(i).getField("structofarrays");
      List<?> expectedList = (List<?>) expectedStruct.getField("names");
      for (int j = 0; j < expectedList.size(); j++) {
        List<Object[]> queryResult = shell.executeStatement(
            String.format("SELECT structofarrays.names[%d] FROM default.structtable LIMIT 1 OFFSET %d", j, i));
        Assert.assertEquals(expectedList.get(j), queryResult.get(0)[0]);
      }
      expectedList = (List<?>) expectedStruct.getField("birthdays");
      for (int j = 0; j < expectedList.size(); j++) {
        List<Object[]> queryResult = shell.executeStatement(
            String.format("SELECT structofarrays.birthdays[%d] FROM default.structtable LIMIT 1 OFFSET %d", j, i));
        Assert.assertEquals(expectedList.get(j).toString(), queryResult.get(0)[0]);
      }
    }
  }

  @Test
  public void testStructOfPrimitivesInTable() throws IOException {
    Schema schema = new Schema(required(1, "structofprimitives",
        Types.StructType.of(required(2, "key", Types.StringType.get()), required(3, "value",
            Types.IntegerType.get()))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "structtable", schema, fileFormat, 1);
    // access a single value in a struct
    for (int i = 0; i < records.size(); i++) {
      GenericRecord expectedStruct = (GenericRecord) records.get(i).getField("structofprimitives");
      List<Object[]> queryResult = shell.executeStatement(String.format(
          "SELECT structofprimitives.key, structofprimitives.value FROM default.structtable LIMIT 1 OFFSET %d", i));
      Assert.assertEquals(expectedStruct.getField("key"), queryResult.get(0)[0]);
      Assert.assertEquals(expectedStruct.getField("value"), queryResult.get(0)[1]);
    }
  }

  @Test
  public void testStructOfStructsInTable() throws IOException {
    Schema schema = new Schema(
        required(1, "structofstructs", Types.StructType.of(required(2, "struct1", Types.StructType
            .of(required(3, "key", Types.StringType.get()), required(4, "value",
                Types.IntegerType.get()))))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "structtable", schema, fileFormat, 1);
    // access a struct element inside a struct
    for (int i = 0; i < records.size(); i++) {
      GenericRecord expectedStruct = (GenericRecord) records.get(i).getField("structofstructs");
      GenericRecord expectedInnerStruct = (GenericRecord) expectedStruct.getField("struct1");
      List<Object[]> queryResult = shell.executeStatement(String.format(
          "SELECT structofstructs.struct1.key, structofstructs.struct1.value FROM default.structtable " +
              "LIMIT 1 OFFSET %d", i));
      Assert.assertEquals(expectedInnerStruct.getField("key"), queryResult.get(0)[0]);
      Assert.assertEquals(expectedInnerStruct.getField("value"), queryResult.get(0)[1]);
    }
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

  @Test
  public void testAddColumnToIcebergTable() throws IOException {
    // Create an Iceberg table with the columns customer_id, first_name and last_name with some initial data.
    Table icebergTable = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // Add a new column (age long) to the Iceberg table.
    icebergTable.updateSchema().addColumn("age", Types.LongType.get()).commit();

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

    List<Record> sortedExpected = new ArrayList<>(people);
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

  private void testComplexTypeWrite(Schema schema, List<Record> records) throws IOException {
    String tableName = "complex_table";
    Table table = testTables.createTable(shell, "complex_table", schema, fileFormat, ImmutableList.of());

    String dummyTableName = "dummy";
    shell.executeStatement("CREATE TABLE default." + dummyTableName + "(a int)");
    shell.executeStatement("INSERT INTO TABLE default." + dummyTableName + " VALUES(1)");
    records.forEach(r -> shell.executeStatement(insertQueryForComplexType(tableName, dummyTableName, schema, r)));
    HiveIcebergTestUtils.validateData(table, records, 0);
  }

  private String insertQueryForComplexType(String tableName, String dummyTableName, Schema schema, Record record) {
    StringBuilder query = new StringBuilder("INSERT INTO TABLE ").append(tableName).append(" SELECT ")
        .append(record.get(0)).append(", ");
    Type type = schema.asStruct().fields().get(1).type();
    query.append(buildComplexTypeInnerQuery(record.get(1), type));
    query.setLength(query.length() - 1);
    query.append(" FROM ").append(dummyTableName).append(" LIMIT 1");
    return query.toString();
  }

  private StringBuilder buildComplexTypeInnerQuery(Object field, Type type) {
    StringBuilder query = new StringBuilder();
    if (type instanceof Types.ListType) {
      query.append("array(");
      List<Object> elements = (List<Object>) field;
      Assert.assertFalse("Hive can not handle empty array() inserts", elements.isEmpty());
      Type innerType = ((Types.ListType) type).fields().get(0).type();
      if (!elements.isEmpty()) {
        elements.forEach(e -> query.append(buildComplexTypeInnerQuery(e, innerType)));
        query.setLength(query.length() - 1);
      }
      query.append("),");
    } else if (type instanceof Types.MapType) {
      query.append("map(");
      Map<Object, Object> entries = (Map<Object, Object>) field;
      Type keyType = ((Types.MapType) type).fields().get(0).type();
      Type valueType = ((Types.MapType) type).fields().get(1).type();
      if (!entries.isEmpty()) {
        entries.entrySet().forEach(e -> query.append(buildComplexTypeInnerQuery(e.getKey(), keyType)
            .append(buildComplexTypeInnerQuery(e.getValue(), valueType))));
        query.setLength(query.length() - 1);
      }
      query.append("),");
    } else if (type instanceof Types.StructType) {
      query.append("named_struct(");
      ((GenericRecord) field).struct().fields().stream()
          .forEach(f -> query.append(buildComplexTypeInnerQuery(f.name(), Types.StringType.get()))
              .append(buildComplexTypeInnerQuery(((GenericRecord) field).getField(f.name()), f.type())));
      query.setLength(query.length() - 1);
      query.append("),");
    } else if (type instanceof Types.StringType) {
      if (field != null) {
        query.append("'").append(field).append("',");
      }
    } else {
      throw new RuntimeException("Unsupported type in complex query build.");
    }
    return query;
  }

  private void validateBasicStats(Table icebergTable, String dbName, String tableName)
      throws TException, InterruptedException {
    Map<String, String> hmsParams = shell.metastore().getTable(dbName, tableName).getParameters();
    Map<String, String> summary = new HashMap<>();
    if (icebergTable.currentSnapshot() == null) {
      for (String key : STATS_MAPPING.values()) {
        summary.put(key, "0");
      }
    } else {
      summary = icebergTable.currentSnapshot().summary();
    }

    for (Map.Entry<String, String> entry : STATS_MAPPING.entrySet()) {
      Assert.assertEquals(summary.get(entry.getValue()), hmsParams.get(entry.getKey()));
    }
  }

  private void validateMigration(String tableName) throws TException, InterruptedException {
    List<Object[]> originalResult = shell.executeStatement("SELECT * FROM " + tableName + " ORDER BY a");
    shell.executeStatement("ALTER TABLE " + tableName + " SET TBLPROPERTIES " +
        "('storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler')");
    List<Object[]> alterResult = shell.executeStatement("SELECT * FROM " + tableName + " ORDER BY a");
    Assert.assertEquals(originalResult.size(), alterResult.size());
    for (int i = 0; i < originalResult.size(); i++) {
      Assert.assertTrue(Arrays.equals(originalResult.get(i), alterResult.get(i)));
    }
    validateSd(tableName, "iceberg");
  }

  private void validateMigrationRollback(String tableName) throws TException, InterruptedException {
    List<Object[]> originalResult = shell.executeStatement("SELECT * FROM " + tableName + " ORDER BY a");
    try (MockedStatic<HiveTableUtil> mockedTableUtil = Mockito.mockStatic(HiveTableUtil.class)) {
      mockedTableUtil.when(() -> HiveTableUtil.importFiles(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
          ArgumentMatchers.any(PartitionSpecProxy.class), ArgumentMatchers.anyList(),
          ArgumentMatchers.any(Properties.class), ArgumentMatchers.any(Configuration.class)))
          .thenThrow(new MetaException());
      try {
        shell.executeStatement("ALTER TABLE " + tableName + " SET TBLPROPERTIES " +
            "('storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler')");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("Error occurred during hive table migration to iceberg."));
        validateSd(tableName, fileFormat.name());
        shell.executeStatement("MSCK REPAIR TABLE " + tableName);
        List<Object[]> alterResult = shell.executeStatement("SELECT * FROM " + tableName + " ORDER BY a");
        Assert.assertEquals(originalResult.size(), alterResult.size());
        for (int i = 0; i < originalResult.size(); i++) {
          Assert.assertTrue(Arrays.equals(originalResult.get(i), alterResult.get(i)));
        }
        return;
      }
      Assert.fail("Alter table operations should have thrown an exception.");
    }
  }

  private void validateSd(String tableName, String format) throws TException, InterruptedException {
    org.apache.hadoop.hive.metastore.api.Table hmsTable = shell.metastore().getTable("default", tableName);
    StorageDescriptor sd = hmsTable.getSd();
    Assert.assertTrue(sd.getSerdeInfo().getSerializationLib().toLowerCase().contains(format.toLowerCase()));
    Assert.assertTrue(sd.getInputFormat().toLowerCase().contains(format.toLowerCase()));
    Assert.assertTrue(sd.getOutputFormat().toLowerCase(Locale.ROOT).contains(format.toLowerCase()));
  }
}
