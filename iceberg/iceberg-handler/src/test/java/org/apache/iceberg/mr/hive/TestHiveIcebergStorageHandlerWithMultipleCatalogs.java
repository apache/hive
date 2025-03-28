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
import java.util.Collection;
import java.util.List;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.TestTables.TestTableType;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.mr.hive.TestTables.TestTableType.HIVE_CATALOG;

@RunWith(Parameterized.class)
public class TestHiveIcebergStorageHandlerWithMultipleCatalogs {

  private static final String HIVECATALOGNAME = "table1_catalog";
  private static final String OTHERCATALOGNAME = "table2_catalog";
  private static TestHiveShell shell;

  @Parameterized.Parameter(0)
  public FileFormat fileFormat1;
  @Parameterized.Parameter(1)
  public FileFormat fileFormat2;
  @Parameterized.Parameter(2)
  public TestTables.TestTableType testTableType1;
  @Parameterized.Parameter(3)
  public String table1CatalogName;
  @Parameterized.Parameter(4)
  public TestTables.TestTableType testTableType2;
  @Parameterized.Parameter(5)
  public String table2CatalogName;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private TestTables testTables1;
  private TestTables testTables2;

  @Parameterized.Parameters(name = "fileFormat1={0}, fileFormat2={1}, tableType1={2}, catalogName1={3}, " +
          "tableType2={4}, catalogName2={5}")
  public static Collection<Object[]> parameters() {
    Collection<Object[]> testParams = Lists.newArrayList();

    // Run tests with PARQUET and ORC file formats for a two Catalogs
    for (TestTableType testTableType : TestTables.ALL_TABLE_TYPES) {
      if (!HIVE_CATALOG.equals(testTableType)) {
        testParams.add(new Object[]{FileFormat.PARQUET, FileFormat.ORC,
            HIVE_CATALOG, HIVECATALOGNAME, testTableType, OTHERCATALOGNAME});
      }
    }
    return testParams;
  }

  @BeforeClass
  public static void beforeClass() {
    shell = HiveIcebergStorageHandlerTestUtils.shell();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    shell.stop();
  }

  @Before
  public void before() throws IOException {
    testTables1 = HiveIcebergStorageHandlerTestUtils.testTables(shell, testTableType1, temp, table1CatalogName);
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables1, temp);
    testTables1.properties().entrySet().forEach(e -> shell.setHiveSessionValue(e.getKey(), e.getValue()));

    testTables2 = HiveIcebergStorageHandlerTestUtils.testTables(shell, testTableType2, temp, table2CatalogName);
    testTables2.properties().entrySet().forEach(e -> shell.setHiveSessionValue(e.getKey(), e.getValue()));
  }

  @After
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
  }

  @Test
  public void testJoinTablesFromDifferentCatalogs() throws IOException {
    createAndAddRecords(testTables1, fileFormat1, TableIdentifier.of("default", "customers1"),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    createAndAddRecords(testTables2, fileFormat2, TableIdentifier.of("default", "customers2"),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    List<Object[]> rows = shell.executeStatement("SELECT c2.customer_id, c2.first_name, c2.last_name " +
            "FROM default.customers2 c2 JOIN default.customers1 c1 ON c2.customer_id = c1.customer_id " +
            "ORDER BY c2.customer_id");
    Assert.assertEquals(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS.size(), rows.size());
    HiveIcebergTestUtils.validateData(Lists.newArrayList(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS),
            HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, rows), 0);
  }

  @Test
  public void testCTASFromOtherCatalog() throws IOException {
    testTables2.createTable(shell, "source", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat2, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    shell.executeStatement(String.format(
        "CREATE TABLE target STORED BY ICEBERG TBLPROPERTIES ('%s'='%s') AS SELECT * FROM source",
        InputFormatConfig.CATALOG_NAME, HIVECATALOGNAME));

    List<Object[]> objects = shell.executeStatement("SELECT * FROM target");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
  }

  @Test
  public void testCTASFromOtherCatalogFailureRollback() throws IOException {
    // force an execution error by passing in a committer class that Tez won't be able to load
    shell.setHiveSessionValue("hive.tez.mapreduce.output.committer.class", "org.apache.NotExistingClass");

    TableIdentifier target = TableIdentifier.of("default", "target");
    testTables2.createTable(shell, "source", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat2, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    AssertHelpers.assertThrows("Should fail while loading non-existent output committer class.",
        IllegalArgumentException.class, "org.apache.NotExistingClass",
        () -> shell.executeStatement(String.format(
            "CREATE TABLE target STORED BY ICEBERG TBLPROPERTIES ('%s'='%s') AS SELECT * FROM source",
            InputFormatConfig.CATALOG_NAME, HIVECATALOGNAME)));
    // CTAS table should have been dropped by the lifecycle hook
    Assert.assertThrows(NoSuchTableException.class, () -> testTables1.loadTable(target));
  }

  private void createAndAddRecords(TestTables testTables, FileFormat fileFormat, TableIdentifier identifier,
                                   List<Record> records) throws IOException {
    String createSql = String.format(
        "CREATE EXTERNAL TABLE %s (customer_id BIGINT, first_name STRING, last_name STRING)" +
        " STORED BY ICEBERG %s " +
        " TBLPROPERTIES ('%s'='%s', '%s'='%s')",
        identifier,
        testTables.locationForCreateTableSQL(identifier),
        InputFormatConfig.CATALOG_NAME, testTables.catalogName(),
        TableProperties.DEFAULT_FILE_FORMAT, fileFormat);
    shell.executeStatement(createSql);
    Table icebergTable = testTables.loadTable(identifier);
    testTables.appendIcebergTable(shell.getHiveConf(), icebergTable, fileFormat, null, records);
  }

}
