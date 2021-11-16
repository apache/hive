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
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Tests verifying correct statistics generation behaviour on Iceberg tables triggered by: ANALYZE queries, inserts,
 * CTAS, etc...
 */
public class TestHiveIcebergStatistics extends HiveIcebergStorageHandlerWithEngineBase {

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
  public void testStatsWithInsert() {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVESTATSAUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of());

    if (testTableType != TestTables.TestTableType.HIVE_CATALOG) {
      // If the location is set and we have to gather stats, then we have to update the table stats now
      shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    }

    String insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, false);
    shell.executeStatement(insert);

    checkColStat(identifier.name(), "customer_id", true);
    checkColStatMinMaxValue(identifier.name(), "customer_id", 0, 2);

    insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS, identifier, false);
    shell.executeStatement(insert);

    checkColStat(identifier.name(), "customer_id", true);
    checkColStatMinMaxValue(identifier.name(), "customer_id", 0, 5);
  }

  @Test
  public void testStatsWithInsertOverwrite() {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVESTATSAUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of());

    String insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS, identifier,
        true);
    shell.executeStatement(insert);

    checkColStat(identifier.name(), "customer_id", true);
    checkColStatMinMaxValue(identifier.name(), "customer_id", 3, 5);
  }

  @Test
  public void testStatsWithPartitionedInsert() {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").build();

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVESTATSAUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, spec,
        fileFormat, ImmutableList.of());

    if (testTableType != TestTables.TestTableType.HIVE_CATALOG) {
      // If the location is set and we have to gather stats, then we have to update the table stats now
      shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    }

    String insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, false);
    shell.executeStatement(insert);

    checkColStat(identifier.name(), "customer_id", true);
    checkColStat(identifier.name(), "first_name", true);
    checkColStatMinMaxValue(identifier.name(), "customer_id", 0, 2);
  }

  @Test
  public void testStatsWithCTAS() {
    Assume.assumeTrue(HiveIcebergSerDe.CTAS_EXCEPTION_MSG, testTableType == TestTables.TestTableType.HIVE_CATALOG);

    shell.executeStatement("CREATE TABLE source (id bigint, name string) PARTITIONED BY (dept string) STORED AS ORC");
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, TableIdentifier.of("default", "source"), false));

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVESTATSAUTOGATHER.varname, true);
    shell.executeStatement(String.format(
        "CREATE TABLE target STORED BY ICEBERG %s %s AS SELECT * FROM source",
        testTables.locationForCreateTableSQL(TableIdentifier.of("default", "target")),
        testTables.propertiesForCreateTableSQL(
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.toString()))));

    checkColStat("target", "id", true);
    checkColStatMinMaxValue("target", "id", 0, 2);
  }

  @Test
  public void testStatsWithPartitionedCTAS() {
    Assume.assumeTrue(HiveIcebergSerDe.CTAS_EXCEPTION_MSG, testTableType == TestTables.TestTableType.HIVE_CATALOG);

    shell.executeStatement("CREATE TABLE source (id bigint, name string) PARTITIONED BY (dept string) STORED AS ORC");
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, TableIdentifier.of("default", "source"), false));

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVESTATSAUTOGATHER.varname, true);
    shell.executeStatement(String.format(
        "CREATE TABLE target PARTITIONED BY (dept, name) STORED BY ICEBERG %s AS SELECT * FROM source s",
        testTables.propertiesForCreateTableSQL(
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.toString()))));

    checkColStat("target", "id", true);
    checkColStat("target", "dept", true);
    checkColStatMinMaxValue("target", "id", 0, 2);
    checkColStatMaxLengthDistinctValue("target", "dept", 5, 3);
    checkColStatMaxLengthDistinctValue("target", "name", 5, 3);
  }

  @Test
  public void testStatsRemoved() throws IOException {
    Assume.assumeTrue("Only HiveCatalog can remove stats which become obsolete",
        testTableType == TestTables.TestTableType.HIVE_CATALOG);

    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVESTATSAUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of());

    String insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, true);
    shell.executeStatement(insert);

    checkColStat(identifier.name(), "customer_id", true);
    checkColStatMinMaxValue(identifier.name(), "customer_id", 0, 2);

    // Create a Catalog where the KEEP_HIVE_STATS is false
    shell.metastore().hiveConf().set(HiveTableOperations.KEEP_HIVE_STATS, StatsSetupConst.FALSE);
    TestTables nonHiveTestTables = HiveIcebergStorageHandlerTestUtils.testTables(shell, testTableType, temp);
    Table nonHiveTable = nonHiveTestTables.loadTable(identifier);

    // Append data to the table through a this non-Hive engine (here java API)
    nonHiveTestTables.appendIcebergTable(shell.getHiveConf(), nonHiveTable, fileFormat, null,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    checkColStat(identifier.name(), "customer_id", false);
  }

  private void checkColStat(String tableName, String colName, boolean accurate) {
    List<Object[]> rows = shell.executeStatement("DESCRIBE " + tableName + " " + colName);

    if (accurate) {
      Assert.assertEquals(2, rows.size());
      Assert.assertEquals(StatsSetupConst.COLUMN_STATS_ACCURATE, rows.get(1)[0]);
      // Check if the value is not {} (empty)
      Assert.assertFalse(rows.get(1)[1].toString().matches("\\{\\}\\s*"));
    } else {
      // If we expect the stats to be not accurate
      if (rows.size() == 1) {
        // no stats now, we are ok
        return;
      } else {
        Assert.assertEquals(2, rows.size());
        Assert.assertEquals(StatsSetupConst.COLUMN_STATS_ACCURATE, rows.get(1)[0]);
        // Check if the value is {} (empty)
        Assert.assertTrue(rows.get(1)[1].toString().matches("\\{\\}\\s*"));
      }
    }
  }

  private void checkColStatMinMaxValue(String tableName, String colName, int minValue, int maxValue) {
    List<Object[]> rows = shell.executeStatement("DESCRIBE FORMATTED " + tableName + " " + colName);

    // Check min
    Assert.assertEquals("min", rows.get(2)[0]);
    Assert.assertEquals(String.valueOf(minValue), rows.get(2)[1]);

    // Check max
    Assert.assertEquals("max", rows.get(3)[0]);
    Assert.assertEquals(String.valueOf(maxValue), rows.get(3)[1]);
  }

  private void checkColStatMaxLengthDistinctValue(String tableName, String colName, int maxLength, int distinct) {
    List<Object[]> rows = shell.executeStatement("DESCRIBE FORMATTED " + tableName + " " + colName);

    // Check max length
    Assert.assertEquals("max_col_len", rows.get(7)[0]);
    Assert.assertEquals(String.valueOf(maxLength), rows.get(7)[1]);

    // Check distinct
    Assert.assertEquals("distinct_count", rows.get(5)[0]);
    Assert.assertEquals(String.valueOf(distinct), rows.get(5)[1]);
  }
}
