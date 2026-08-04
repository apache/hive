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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatistics;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.mr.hive.test.TestTables;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergStorageHandlerTestUtils;
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergTestUtils;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


/**
 * Tests verifying correct statistics generation behaviour on Iceberg tables triggered by: ANALYZE queries, inserts,
 * CTAS, etc...
 */
public class TestHiveIcebergStatistics extends HiveIcebergStorageHandlerWithEngineBase {

  @Parameterized.Parameter(4)
  public String statsSource;

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}, statsSource={4}")
  public static Collection<Object[]> parameters() {
    Collection<Object[]> testParams = Lists.newArrayList();

    for (Object[] params : HiveIcebergStorageHandlerWithEngineBase.getParameters(p ->
        p.isVectorized() && p.formatVersion() == 2)) {
      for (String statsSource : new String[]{"iceberg", "metastore"}) {
        testParams.add(ArrayUtils.add(params, statsSource));
      }
    }
    return testParams;
  }

  @Before
  public void setStatsSource() {
    HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_ICEBERG_STATS_SOURCE, statsSource);
  }

  @Test
  public void testAnalyzeTableComputeStatistics() throws IOException, TException, InterruptedException {
    Assume.assumeTrue(statsSource.equals("iceberg") || testTableType == TestTableType.HIVE_CATALOG);

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
            Lists.newArrayList());
    shell.executeStatement("ANALYZE TABLE " + dbName + "." + tableName + " COMPUTE STATISTICS");
    validateBasicStats(table, dbName, tableName);
  }

  @Test
  public void testStatsWithInsert() {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of());

    if (testTableType != TestTableType.HIVE_CATALOG) {
      // If the location is set and we have to gather stats, then we have to update the table stats now
      shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    }

    String insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, false);
    shell.executeStatement(insert);

    checkColStat(identifier.name(), "customer_id", true);
    checkColStatMinMaxValue(identifier.name(), "customer_id", 0, 2);

    insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1, identifier, false);
    shell.executeStatement(insert);

    checkColStat(identifier.name(), "customer_id", true);
    checkColStatMinMaxValue(identifier.name(), "customer_id", 0, 5);
  }

  @Test
  public void testStatsWithPessimisticLockInsert() {
    Assume.assumeTrue(testTableType == TestTableType.HIVE_CATALOG);
    TableIdentifier identifier = getTableIdentifierWithPessimisticLock("false");
    String insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, false);
    shell.executeStatement(insert);

    checkColStat(identifier.name(), "customer_id", true);
    checkColStatMinMaxValue(identifier.name(), "customer_id", 0, 2);
  }

  @Test
  public void testStatsWithPessimisticLockInsertWhenHiveLockEnabled() {
    Assume.assumeTrue(testTableType == TestTableType.HIVE_CATALOG);
    TableIdentifier identifier = getTableIdentifierWithPessimisticLock("true");
    String insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, false);
    AssertHelpers.assertThrows(
        "Should throw RuntimeException when Hive locking is on with 'engine.hive.lock-enabled=true'",
        RuntimeException.class,
        () -> shell.executeStatement(insert)
    );
  }

  private TableIdentifier getTableIdentifierWithPessimisticLock(String hiveLockEnabled) {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_TXN_EXT_LOCKING_ENABLED.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of(), formatVersion,
        ImmutableMap.of(TableProperties.HIVE_LOCK_ENABLED, hiveLockEnabled));
    return identifier;
  }

  @Test
  public void testStatsWithInsertOverwrite() {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of());

    String insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1, identifier,
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

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, spec,
        fileFormat, ImmutableList.of());

    if (testTableType != TestTableType.HIVE_CATALOG) {
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
    Assume.assumeTrue(HiveIcebergSerDe.CTAS_EXCEPTION_MSG, testTableType == TestTableType.HIVE_CATALOG);

    shell.executeStatement("CREATE TABLE source (id bigint, name string) PARTITIONED BY (dept string) STORED AS ORC");
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, TableIdentifier.of("default", "source"), false));

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
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
    Assume.assumeTrue(HiveIcebergSerDe.CTAS_EXCEPTION_MSG, testTableType == TestTableType.HIVE_CATALOG);

    shell.executeStatement("CREATE TABLE source (id bigint, name string) PARTITIONED BY (dept string) STORED AS ORC");
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, TableIdentifier.of("default", "source"), false));

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement(String.format(
        "CREATE TABLE target PARTITIONED BY (dept, name) STORED BY ICEBERG %s AS SELECT * FROM source s",
        testTables.propertiesForCreateTableSQL(
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.toString()))));

    if (statsSource.equals("iceberg")) {
      // TODO: Propagate partition spec from CREATE statement to the ColumnStatsSemanticAnalyzer
      shell.executeStatement("ANALYZE TABLE target COMPUTE STATISTICS FOR COLUMNS");
    }

    checkColStat("target", "id", true);
    checkColStat("target", "dept", true);
    checkColStatMinMaxValue("target", "id", 0, 2);
    checkColStatMaxLengthDistinctValue("target", "dept", 5, 3);
    checkColStatMaxLengthDistinctValue("target", "name", 5, 3);
  }

  @Test
  public void testStatsRemoved() throws IOException {
    Assume.assumeTrue("Only HiveCatalog can remove stats which become obsolete",
        testTableType == TestTableType.HIVE_CATALOG);

    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of());

    String insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, true);
    shell.executeStatement(insert);

    checkColStat(identifier.name(), "customer_id", true);
    checkColStatMinMaxValue(identifier.name(), "customer_id", 0, 2);

    // Create a Catalog where the KEEP_HIVE_STATS is false
    shell.metastore().hiveConf().set(ConfigProperties.KEEP_HIVE_STATS, StatsSetupConst.FALSE);
    TestTables nonHiveTestTables = HiveIcebergStorageHandlerTestUtils.testTables(shell, testTableType, temp);
    Table nonHiveTable = nonHiveTestTables.loadTable(identifier);

    // Append data to the table through a this non-Hive engine (here java API)
    nonHiveTestTables.appendIcebergTable(shell.getHiveConf(), nonHiveTable, fileFormat, null,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    checkColStat(identifier.name(), "customer_id", false);
  }

  @Test
  public void testColumnStatsAccurate() throws Exception {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of());

    String insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, true);
    shell.executeStatement(insert);

    org.apache.hadoop.hive.metastore.api.Table hmsTable = shell.metastore().getTable("default", identifier.name());

    // Assert whether basic stats and column stats are accurate.
    Assert.assertTrue(hmsTable.getParameters().containsKey(StatsSetupConst.COLUMN_STATS_ACCURATE));
    Assert.assertTrue(StatsSetupConst.areBasicStatsUptoDate(hmsTable.getParameters()));
    for (NestedField nestedField : HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA.columns()) {
      Assert.assertTrue(StatsSetupConst.areColumnStatsUptoDate(hmsTable.getParameters(), nestedField.name()));
    }
  }

  @Test
  public void testMergeStatsWithInsert() {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of());

    if (testTableType != TestTableType.HIVE_CATALOG) {
      // If the location is set and we have to gather stats, then we have to update the table stats now
      shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    }

    String insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, false);
    shell.executeStatement(insert);

    checkColStat(identifier.name(), "customer_id", true);
    checkColStatMinMaxDistinctValue(identifier.name(), "customer_id", 0, 2, 3, 0);

    insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1, identifier, false);
    shell.executeStatement(insert);

    checkColStat(identifier.name(), "customer_id", true);
    checkColStatMinMaxDistinctValue(identifier.name(), "customer_id", 0, 5, 6, 0);

    insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, identifier, false);
    shell.executeStatement(insert);
    checkColStat(identifier.name(), "customer_id", true);
    checkColStatMinMaxDistinctValue(identifier.name(), "customer_id", 0, 5, 6, 0);
  }

  @Test
  public void testIcebergColStatsPath() throws IOException {
    Assume.assumeTrue(statsSource.equals("iceberg"));

    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    Table table = testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of());

    String insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, false);
    shell.executeStatement(insert);

    table.refresh();

    Path tblColPath = new Path(IcebergTableUtil.getColStatsPath(table));
    Assert.assertNotNull(tblColPath);
    // Check that if colPath is created correctly
    Assert.assertTrue(tblColPath.getFileSystem(shell.getHiveConf()).exists(tblColPath));
    List<Object[]> result = shell.executeStatement("SELECT * FROM customers");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, result));
  }

  @Test
  public void testGetAggrBasicStatsForPartitioned() {
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    createPartitionedCustomers(identifier);

    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);
    HiveIcebergStorageHandler handler = storageHandler();

    List<String> partNames = partitionNames(handler, hmsTable);
    Assert.assertEquals(3, partNames.size());

    Map<String, Map<String, String>> aggr = handler.getAggrBasicStatsFor(hmsTable, partNames);
    Assert.assertEquals(3, aggr.size());

    // each customer record lands in its own last_name partition
    for (String partName : partNames) {
      Map<String, String> basicStats = aggr.get(partName);
      Assert.assertEquals("1", basicStats.get(StatsSetupConst.ROW_COUNT));
      Assert.assertEquals("1", basicStats.get(StatsSetupConst.NUM_FILES));
      Assert.assertTrue(Long.parseLong(basicStats.get(StatsSetupConst.TOTAL_SIZE)) > 0);
    }
    // no deletes: every partition's row count is answered exactly
    Map<String, Long> rowCounts = handler.getRowCount(hmsTable, partNames);
    Assert.assertEquals(partNames.size(), rowCounts.size());
    rowCounts.values().forEach(rowCount -> Assert.assertEquals(Long.valueOf(1), rowCount));
  }

  @Test
  public void testAnalyzePartitionSpecRejected() {
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    createPartitionedCustomers(identifier);
    String expected = ErrorMsg.ANALYZE_PARTITION_NON_NATIVE.getMsg();

    // basic statistics are maintained incrementally for all partitions as a whole:
    // a partition-scoped basic-stats ANALYZE cannot be honored and must be rejected
    AssertHelpers.assertThrows(
        "Should reject partition-scoped basic-stats ANALYZE for non-native partitioned tables",
        IllegalArgumentException.class, expected,
        () -> shell.executeStatement(
            "ANALYZE TABLE " + identifier + " PARTITION (last_name='Brown') COMPUTE STATISTICS")
    );
    // same for column statistics: the rewrite would drop every other partition's column stats
    AssertHelpers.assertThrows(
        "Should reject partition-scoped column-stats ANALYZE for non-native partitioned tables",
        IllegalArgumentException.class, expected,
        () -> shell.executeStatement(
            "ANALYZE TABLE " + identifier + " PARTITION (last_name='Brown') COMPUTE STATISTICS FOR COLUMNS")
    );
  }

  @Test
  public void testAnalyzeCatchesUpPartitionStats() {
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    createPartitionedCustomers(identifier);

    // a write that skips the stats auto-gathering leaves the new snapshot without a partition stats file
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, false);
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, identifier, false));

    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);
    HiveIcebergStorageHandler handler = storageHandler();
    List<String> partNames = partitionNames(handler, hmsTable);

    // the partition stats file is missing: every partition is reported missing (estimated by the planner)
    // and exact query answering is refused
    Assert.assertEquals(0, handler.getAggrBasicStatsFor(hmsTable, partNames).size());
    Assert.assertTrue(handler.getRowCount(hmsTable, partNames).isEmpty());

    // ANALYZE catches up: the incremental computation publishes the partition stats file
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS");

    Map<String, Map<String, String>> refreshed = handler.getAggrBasicStatsFor(hmsTable, partNames);
    Assert.assertEquals(partNames.size(), refreshed.size());
    refreshed.values().forEach(basicStats ->
        Assert.assertTrue(Long.parseLong(basicStats.get(StatsSetupConst.ROW_COUNT)) > 0));
    Assert.assertEquals(partNames.size(), handler.getRowCount(hmsTable, partNames).size());
  }

  @Test
  public void testGetAggrColStatsForPartitioned() throws Exception {
    // bulk partition column statistics: a single puffin read aggregated across the requested
    // partitions, mirroring testGetAggrBasicStatsForPartitioned
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    createPartitionedCustomers(identifier);

    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);
    HiveIcebergStorageHandler handler = storageHandler();

    List<String> partNames = partitionNames(handler, hmsTable);
    Assert.assertEquals(3, partNames.size());

    AggrStats aggrStats = handler.getAggrColStatsFor(hmsTable, ImmutableList.of("customer_id"), partNames);
    Assert.assertEquals(3, aggrStats.getPartsFound());
    Assert.assertEquals(1, aggrStats.getColStatsSize());
    ColumnStatisticsObj statsObj = aggrStats.getColStats().get(0);
    Assert.assertEquals("customer_id", statsObj.getColName());
    // customer ids 0..2, one per last_name partition, merged across the three partitions
    Assert.assertEquals(0, statsObj.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(2, statsObj.getStatsData().getLongStats().getHighValue());
  }

  @Test
  public void testCountStarPartitioned() {
    // end-to-end count(*) correctness on a partitioned table, for both stats sources:
    // iceberg source answers from the per-partition stats (exactness gate), metastore source must
    // not fail (regression: NumberFormatException on the missing per-partition ROW_COUNT) and a
    // partition-pruned count(*) must not be answered from the table-level row count
    assumeParquetHiveCatalog();

    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    createPartitionedCustomers(identifier);

    List<Object[]> result = shell.executeStatement("SELECT count(*) FROM " + identifier);
    Assert.assertEquals(3L, result.get(0)[0]);

    result = shell.executeStatement("SELECT count(*) FROM " + identifier + " WHERE last_name = 'Brown'");
    Assert.assertEquals(1L, result.get(0)[0]);

    if (statsSource.equals("iceberg")) {
      // the counts must be answered from statistics: the rewritten plans contain no table scan
      String plan = shell.executeAndStringify("EXPLAIN SELECT count(*) FROM " + identifier);
      Assert.assertFalse(plan, plan.contains("TableScan"));
      plan = shell.executeAndStringify("EXPLAIN SELECT count(*) FROM " + identifier + " WHERE last_name = 'Brown'");
      Assert.assertFalse(plan, plan.contains("TableScan"));
    }
  }

  @Test
  public void testCountStarWithoutPartitionStatsFile() {
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    createPartitionedCustomers(identifier, false);

    Table icebergTable = testTables.loadTable(identifier);
    Assert.assertNull(IcebergTableUtil.getPartitionStatsFile(
        icebergTable, icebergTable.currentSnapshot().snapshotId()));

    // the per-partition path needs that file, so a pruned count is answered by a scan, while the
    // unpruned one still comes from the table-level statistics
    Assert.assertEquals(1L, shell.executeStatement(
        "SELECT count(*) FROM " + identifier + " WHERE last_name = 'Brown'").get(0)[0]);
    Assert.assertEquals(3L, shell.executeStatement("SELECT count(*) FROM " + identifier).get(0)[0]);
    String plan = shell.executeAndStringify("EXPLAIN SELECT count(*) FROM " + identifier);
    Assert.assertFalse(plan, plan.contains("TableScan"));
  }

  @Test
  public void testRowCountWithDeletes() {
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    createPartitionedCustomers(identifier);
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, identifier, false));
    // position-delete one of the two Silver rows (the default delete mode is copy-on-write, which would
    // rewrite the file and leave no delete file behind)
    shell.executeStatement("ALTER TABLE " + identifier + " SET TBLPROPERTIES ('write.delete.mode'='merge-on-read')");
    shell.executeStatement("DELETE FROM " + identifier + " WHERE customer_id = 2 AND first_name = 'Joanna'");

    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);
    HiveIcebergStorageHandler handler = storageHandler();
    List<String> partNames = partitionNames(handler, hmsTable);
    Assert.assertEquals(11, partNames.size());

    // the delete-covered partition's count is inexact and must be omitted; all others stay exact
    Map<String, Long> rowCounts = handler.getRowCount(hmsTable, partNames);
    Assert.assertEquals(10, rowCounts.size());
    Assert.assertFalse(rowCounts.containsKey("last_name=Silver"));

    // end-to-end: the rewrite is refused for the delete-covered scope and the counts stay correct
    Assert.assertEquals(11L, shell.executeStatement("SELECT count(*) FROM " + identifier).get(0)[0]);
    Assert.assertEquals(1L,
        shell.executeStatement("SELECT count(*) FROM " + identifier + " WHERE last_name = 'Silver'").get(0)[0]);
  }

  @Test
  public void testStatsAfterEvolutionFromUnpartitioned() throws Exception {
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    createEvolvedCustomers(identifier);

    // ANALYZE computes both stats families on the evolved table
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    checkColStat(identifier.name(), "customer_id", true);
    Table icebergTable = testTables.loadTable(identifier);
    validateBasicStats(icebergTable, "default", identifier.name());

    // the partition stats file accounts for the legacy rows too, under the empty partition tuple
    Map<String, PartitionStatistics> fileStats =
        IcebergTableUtil.readPartitionStats(icebergTable, icebergTable.currentSnapshot());
    Assert.assertEquals(3L, fileStats.get(DummyPartition.VOID).dataRecordCount().longValue());

    // column stats blobs are written for the physical partitions only: values existing solely among the
    // legacy unpartitioned rows (Green, Pink) get no blob
    List<ColumnStatistics> colStats =
        IcebergTableUtil.readColStats(icebergTable, icebergTable.currentSnapshot().snapshotId(), null);
    Assert.assertEquals(
        List.of("last_name=Barna", "last_name=Brown", "last_name=Rozsaszin", "last_name=Zold"),
        colStats.stream().map(stats -> stats.getStatsDesc().getPartName()).sorted().toList());

    // a blob describes the physical partition's files only: the legacy Brown row (customer_id 0) does not
    // merge into the Brown partition's blob, which covers just the new-spec row (customer_id 3)
    ColumnStatisticsObj brownCustomerId = colStats.stream()
        .filter(stats -> "last_name=Brown".equals(stats.getStatsDesc().getPartName()))
        .flatMap(stats -> stats.getStatsObj().stream())
        .filter(obj -> "customer_id".equals(obj.getColName()))
        .findFirst().orElseThrow();
    Assert.assertEquals(3L, brownCustomerId.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(3L, brownCustomerId.getStatsData().getLongStats().getHighValue());

    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);
    HiveIcebergStorageHandler handler = storageHandler();
    List<String> partNames = partitionNames(handler, hmsTable);
    Assert.assertEquals(4, partNames.size());

    // planner basic stats answer the requested partitions and carry the legacy rows as an extra entry
    List<String> statNames = Lists.newArrayList(partNames);
    statNames.add(DummyPartition.VOID);
    Map<String, Map<String, String>> aggr = handler.getAggrBasicStatsFor(hmsTable, statNames);
    Assert.assertEquals(statNames.size(), aggr.size());
    partNames.forEach(name -> Assert.assertEquals("1", aggr.get(name).get(StatsSetupConst.ROW_COUNT)));
    Assert.assertEquals("3",
        aggr.get(DummyPartition.VOID).get(StatsSetupConst.ROW_COUNT));

    // an unpruned scan plans with the table-level statistics; a pruned scan sums the matched partitions
    // and the synthetic partition, which the pruner keeps whenever a legacy file survives file-level pruning
    String plan = shell.executeAndStringify("EXPLAIN SELECT * FROM " + identifier);
    Assert.assertTrue(plan, plan.contains("rows=7"));
    // the legacy values span Brown..Pink, so the legacy file is pruned away for Barna
    plan = shell.executeAndStringify("EXPLAIN SELECT * FROM " + identifier + " WHERE last_name = 'Barna'");
    Assert.assertTrue(plan, plan.contains("rows=1"));
    // Brown matches both the partition and the legacy file: 1 partition row + 3 legacy rows are read
    plan = shell.executeAndStringify("EXPLAIN SELECT * FROM " + identifier + " WHERE last_name = 'Brown'");
    Assert.assertTrue(plan, plan.contains("rows=4"));
    // Green has no partition of its own, only legacy rows: the synthetic partition alone answers
    plan = shell.executeAndStringify("EXPLAIN SELECT * FROM " + identifier + " WHERE last_name = 'Green'");
    Assert.assertTrue(plan, plan.contains("rows=3"));
  }

  @Test
  public void testRowCountAfterEvolutionFromUnpartitioned() {
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    createEvolvedCustomers(identifier);

    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);
    HiveIcebergStorageHandler handler = storageHandler();
    List<String> partNames = partitionNames(handler, hmsTable);

    // the synthetic partition cannot answer a predicate exactly, so a pruned list holding it is refused
    List<String> withPseudo = Lists.newArrayList(partNames);
    withPseudo.add(DummyPartition.VOID);
    Assert.assertTrue(handler.getRowCount(hmsTable, withPseudo).isEmpty());
    // without it the partition counts are exact
    Assert.assertEquals(partNames.size(), handler.getRowCount(hmsTable, partNames).size());

    // counts stay correct via a real scan, pruned and unpruned alike; Green exists only in the legacy
    // rows so its pruned list is empty (partition statistics alone would have answered 0)
    Assert.assertEquals(7L, shell.executeStatement("SELECT count(*) FROM " + identifier).get(0)[0]);
    Assert.assertEquals(1L,
        shell.executeStatement("SELECT count(*) FROM " + identifier + " WHERE last_name = 'Green'").get(0)[0]);
    Assert.assertEquals(1L,
        shell.executeStatement("SELECT count(*) FROM " + identifier + " WHERE last_name = 'Barna'").get(0)[0]);
    // Brown is both a partition of its own and a value among the legacy rows: the synthetic partition
    // keeps the filter in the plan, so only the two genuine Brown rows are returned
    Assert.assertEquals(2L,
        shell.executeStatement("SELECT count(*) FROM " + identifier + " WHERE last_name = 'Brown'").get(0)[0]);
    List<Object[]> brown = shell.executeStatement(
        "SELECT customer_id FROM " + identifier + " WHERE last_name = 'Brown' ORDER BY customer_id");
    Assert.assertEquals(2, brown.size());
    Assert.assertEquals(0L, brown.get(0)[0]);
    Assert.assertEquals(3L, brown.get(1)[0]);
  }

  private void createEvolvedCustomers(TableIdentifier identifier) {
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of(), formatVersion);
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, false));
    shell.executeStatement("ALTER TABLE " + identifier + " SET PARTITION SPEC (last_name)");
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1, identifier, false));
    // a Brown row in the new spec: its partition's value is also present among the legacy rows
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (3, 'Alice', 'Brown')");
  }

  /** The bulk-stats scenarios are independent of the file format and catalog; pin one combination. */
  private void assumeParquetHiveCatalog() {
    Assume.assumeTrue(fileFormat == FileFormat.PARQUET && testTableType == TestTableType.HIVE_CATALOG);
  }

  private void assumeParquetHiveCatalogIceberg() {
    assumeParquetHiveCatalog();
    Assume.assumeTrue(statsSource.equals("iceberg"));
  }

  private void createPartitionedCustomers(TableIdentifier identifier) {
    createPartitionedCustomers(identifier, true);
  }

  /** Auto-gathering on write triggers computeBasicStatistics, which publishes the partition stats file. */
  private void createPartitionedCustomers(TableIdentifier identifier, boolean autoGather) {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").build();
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, autoGather);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, spec,
        fileFormat, ImmutableList.of(), formatVersion);
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, false));
  }

  private org.apache.hadoop.hive.ql.metadata.Table hmsTable(TableIdentifier identifier) {
    try {
      return new org.apache.hadoop.hive.ql.metadata.Table(
          shell.metastore().getTable("default", identifier.name()));
    } catch (TException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private HiveIcebergStorageHandler storageHandler() {
    HiveIcebergStorageHandler handler = new HiveIcebergStorageHandler();
    handler.setConf(shell.getHiveConf());
    return handler;
  }

  private static List<String> partitionNames(HiveIcebergStorageHandler handler,
      org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    try {
      return handler.getPartitions(hmsTable, Collections.emptyMap(), true).stream()
          .map(Partition::getName)
          .toList();
    } catch (SemanticException e) {
      throw new RuntimeException(e);
    }
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

  private void checkColStatMinMaxDistinctValue(String tableName, String colName, int minValue, int maxValue,
      int distinct, int nulls) {
    List<Object[]> rows = shell.executeStatement("DESCRIBE FORMATTED " + tableName + " " + colName);

    // Check min
    Assert.assertEquals("min", rows.get(2)[0]);
    Assert.assertEquals(String.valueOf(minValue), rows.get(2)[1]);

    // Check max
    Assert.assertEquals("max", rows.get(3)[0]);
    Assert.assertEquals(String.valueOf(maxValue), rows.get(3)[1]);

    // Check num of nulls
    Assert.assertEquals("num_nulls", rows.get(4)[0]);
    Assert.assertEquals(String.valueOf(nulls), rows.get(4)[1]);

    // Check distinct
    Assert.assertEquals("distinct_count", rows.get(5)[0]);
    Assert.assertEquals(String.valueOf(distinct), rows.get(5)[1]);
  }
}
