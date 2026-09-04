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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatistics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.hive.stats.IcebergColStatsReader;
import org.apache.iceberg.mr.hive.stats.IcebergColStatsWriter;
import org.apache.iceberg.mr.hive.stats.IcebergPartitionStatsReader;
import org.apache.iceberg.mr.hive.stats.IcebergStoredStats;
import org.apache.iceberg.mr.hive.test.TestTables;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergStorageHandlerTestUtils;
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergTestUtils;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
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

  /** Rows of the partition that spans the whole range of id, one distinct amount each. */
  private static final int WIDE_ROWS = 400;
  /** Rows of the partition that repeats a single id and a single amount. */
  private static final int NARROW_ROWS = 200;

  /** What a partition holding no value for a partition column is named. */
  private static final String NULL_PART = "__HIVE_DEFAULT_PARTITION__";

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
    // these tests describe the per partition statistics an Iceberg table keeps when asked to
    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL, true);
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
  public void testBranchWriteLeavesTableStatsUntouched() {
    // a branch write leaves the table's snapshot where it is, so the table's column and basic
    // statistics must still describe the table's own rows
    TableIdentifier identifier = TableIdentifier.of("default", "customers_branch");

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of());
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, false));
    checkColStatMinMaxValue(identifier.name(), "customer_id", 0, 2);

    shell.executeStatement("ALTER TABLE " + identifier + " CREATE BRANCH b1");
    shell.executeStatement("INSERT INTO " + identifier + ".branch_b1 VALUES (100, \'Bob\', \'Brown\')");

    // answer from the data, not from the statistics under test
    shell.setHiveSessionValue("hive.compute.query.using.stats", false);
    List<Object[]> mainRows = shell.executeStatement("SELECT max(customer_id) FROM " + identifier);
    Assert.assertEquals("the row belongs to the branch, not the table", "2",
        String.valueOf(mainRows.get(0)[0]));

    // the table's column statistics still describe the table's rows
    checkColStatMinMaxValue(identifier.name(), "customer_id", 0, 2);
    checkColStat(identifier.name(), "customer_id", true);

    // and the basic statistics likewise count the table's rows, not the branch's
    Map<String, String> basicStats = storageHandler().getBasicStatistics(hmsTable(identifier));
    Assert.assertEquals("3", basicStats.get(StatsSetupConst.ROW_COUNT));
    Assert.assertEquals(Long.valueOf(3L), storageHandler().getRowCount(hmsTable(identifier)));

    // and the metastore parameters, which are table-scoped, still count the table's rows
    Assert.assertEquals("3", hmsTable(identifier).getParameters().get(StatsSetupConst.ROW_COUNT));

    // asked for the branch, the handler counts the branch's rows
    org.apache.hadoop.hive.ql.metadata.Table branchHmsTable = hmsTable(identifier);
    branchHmsTable.setSnapshotRef("branch_b1");
    Assert.assertEquals("4",
        storageHandler().getBasicStatistics(branchHmsTable).get(StatsSetupConst.ROW_COUNT));
    Assert.assertEquals(Long.valueOf(4L), storageHandler().getRowCount(branchHmsTable));
  }

  @Test
  public void testAnalyzeOnBranchLeavesTableBasicStatsUntouched() {
    // a plain analyze takes the footer scan path, whose row count describes the branch it named
    TableIdentifier identifier = TableIdentifier.of("default", "customers_analyze_branch_basic");

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of());
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, false));

    shell.executeStatement("ALTER TABLE " + identifier + " CREATE BRANCH b1");
    shell.executeStatement("INSERT INTO " + identifier + ".branch_b1 VALUES (100, \'Bob\', \'Brown\')");
    Assert.assertEquals("3", hmsTable(identifier).getParameters().get(StatsSetupConst.ROW_COUNT));

    shell.executeStatement("ANALYZE TABLE " + identifier + ".branch_b1 COMPUTE STATISTICS");

    Assert.assertEquals("the branch's row count is not the table's", "3",
        hmsTable(identifier).getParameters().get(StatsSetupConst.ROW_COUNT));
    Assert.assertEquals(Long.valueOf(3L), storageHandler().getRowCount(hmsTable(identifier)));
  }

  @Test
  public void testBranchWriteStoresItsColStatsOnTheBranch() {
    // the statistics file is anchored to a snapshot: a branch write stores what it gathered on
    // the branch's head, not on the table's
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers_branch_stats");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of());
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, false));

    shell.executeStatement("ALTER TABLE " + identifier + " CREATE BRANCH b1");
    shell.executeStatement("INSERT INTO " + identifier + ".branch_b1 VALUES (100, \'Bob\', \'Brown\')");

    Table icebergTable = testTables.loadTable(identifier);
    long branchSnapshotId = icebergTable.snapshot("b1").snapshotId();
    Assert.assertNotNull("the branch's head carries the statistics its write gathered",
        IcebergStoredStats.getColStatsFile(icebergTable, branchSnapshotId, partitionLevel(icebergTable)));

    // the increment extends the fork point's statistics: the table's 0..2 plus the branch's 100
    List<ColumnStatisticsObj> branchStats =
        IcebergColStatsReader.read(icebergTable, branchSnapshotId, null, true);
    ColumnStatisticsObj branchId = branchStats.stream()
        .filter(obj -> "customer_id".equals(obj.getColName())).findFirst().orElseThrow();
    Assert.assertEquals(0L, branchId.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(100L, branchId.getStatsData().getLongStats().getHighValue());

    checkColStatMinMaxValue(identifier.name(), "customer_id", 0, 2);
  }

  @Test
  public void testAnalyzeOnBranchStoresStatsOnTheBranch() {
    // an explicit ANALYZE of a branch describes the branch's rows: its statistics belong to the
    // branch's head, and the table's own statistics stay as they were
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers_analyze_branch");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of());
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, false));
    checkColStatMinMaxValue(identifier.name(), "customer_id", 0, 2);

    shell.executeStatement("ALTER TABLE " + identifier + " CREATE BRANCH b1");
    shell.executeStatement("INSERT INTO " + identifier + ".branch_b1 VALUES (100, \'Bob\', \'Brown\')");

    // a write that gathers nothing: its head carries no statistics, and 500 is a value neither the
    // table nor the statistics the previous write stored have ever seen
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, false);
    shell.executeStatement("INSERT INTO " + identifier + ".branch_b1 VALUES (500, \'Cy\', \'Green\')");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);

    long branchSnapshotId = testTables.loadTable(identifier).snapshot("b1").snapshotId();
    Assert.assertNull("the branch's head starts without statistics", IcebergStoredStats.getColStatsFile(
        testTables.loadTable(identifier), branchSnapshotId, partitionLevel(testTables.loadTable(identifier))));

    shell.executeStatement("ANALYZE TABLE " + identifier + ".branch_b1 COMPUTE STATISTICS FOR COLUMNS");

    // the branch ANALYZE leaves the table's own statistics alone
    checkColStatMinMaxValue(identifier.name(), "customer_id", 0, 2);

    // and it describes the branch's rows, stored on the branch's head
    Table icebergTable = testTables.loadTable(identifier);
    Assert.assertNotNull("the branch's head carries the statistics the analyze computed",
        IcebergStoredStats.getColStatsFile(icebergTable, branchSnapshotId, partitionLevel(icebergTable)));
    List<ColumnStatisticsObj> branchStats =
        IcebergColStatsReader.read(icebergTable, branchSnapshotId, null, true);
    ColumnStatisticsObj branchId = branchStats.stream()
        .filter(obj -> "customer_id".equals(obj.getColName())).findFirst().orElseThrow();
    Assert.assertEquals(0L, branchId.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals("only the branch holds this row", 500L,
        branchId.getStatsData().getLongStats().getHighValue());
  }

  @Test
  public void testBranchWriteLeavesTableColStatsAccuracyAlone() {
    // COLUMN_STATS_ACCURATE describes the table, so a branch write must not restore it
    assumeParquetHiveCatalogIceberg();

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    TableIdentifier identifier = TableIdentifier.of("default", "customers_branch_flag");
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier +
        " (id bigint) STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1),(2)");
    shell.executeStatement("ALTER TABLE " + identifier + " CREATE BRANCH b1");

    shell.executeStatement("DELETE FROM " + identifier + " WHERE id = 1");
    Assert.assertFalse("the delete stales the table's statistics", colStatsAccurate(identifier));

    shell.executeStatement("INSERT INTO " + identifier + ".branch_b1 VALUES (9)");
    Assert.assertFalse("the branch write describes the branch, not the table",
        colStatsAccurate(identifier));
  }

  @Test
  public void testStatsWithInsertOverwrite() {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of());

    // pre-existing statistics: the overwrite must replace them, not merge onto them
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, false));
    checkColStatMinMaxValue(identifier.name(), "customer_id", 0, 2);

    String insert = testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1, identifier,
        true);
    shell.executeStatement(insert);

    checkColStat(identifier.name(), "customer_id", true);
    checkColStatMinMaxValue(identifier.name(), "customer_id", 3, 5);
  }

  @Test
  public void testStatsWithPartitionedInsertOverwrite() {
    // a partition overwrite replaces the statistics of the partitions it wrote, and carries the
    // ones it never reached across unchanged
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_iow");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (2, 'a'), (7, 'b')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertTrue(colStatsAccurate(identifier));

    shell.executeStatement("INSERT OVERWRITE TABLE " + identifier + " VALUES (5, 'a')");
    Assert.assertTrue(hasColStatsForCurrentSnapshot(identifier));
    Assert.assertTrue(colStatsAccurate(identifier));

    List<ColumnStatistics> colStats = readCurrentColStats(identifier);
    // p=a was overwritten, so the rows it no longer holds stop bounding its range
    ColumnStatisticsObj idA = colStatsObj(colStats, "p=a", "id");
    Assert.assertEquals(5L, idA.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(5L, idA.getStatsData().getLongStats().getHighValue());
    // p=b was never written, so its statistics came across from the previous file
    ColumnStatisticsObj idB = colStatsObj(colStats, "p=b", "id");
    Assert.assertEquals(7L, idB.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(7L, idB.getStatsData().getLongStats().getHighValue());

    // and a recompute of the whole table agrees with what the overwrite left behind
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertTrue(colStatsAccurate(identifier));
    colStats = readCurrentColStats(identifier);
    Assert.assertEquals(5L, colStatsObj(colStats, "p=a", "id").getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(7L, colStatsObj(colStats, "p=b", "id").getStatsData().getLongStats().getLowValue());
  }

  @Test
  public void testPartitionScopedAnalyzeLeavesThePartitionItCannotNameAlone() {
    // the rows a table held before it was partitioned belong to a partition no value names, so a
    // partition scoped ANALYZE reaches some of them and must not describe that partition by those
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_evo_void");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (100, 'b')");
    shell.executeStatement("ALTER TABLE " + identifier + " SET PARTITION SPEC (p)");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (5, 'a')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertEquals(100L, colStatsObj(readCurrentColStats(identifier), DummyPartition.VOID, "id")
        .getStatsData().getLongStats().getHighValue());

    shell.executeStatement(
        "ANALYZE TABLE " + identifier + " PARTITION (p='a') COMPUTE STATISTICS FOR COLUMNS");

    List<ColumnStatistics> colStats = readCurrentColStats(identifier);
    Assert.assertEquals("the partition the statement cannot name keeps describing all of its rows",
        100L, colStatsObj(colStats, DummyPartition.VOID, "id")
            .getStatsData().getLongStats().getHighValue());
    Assert.assertEquals("and the one it named describes only the rows that partition holds",
        5L, colStatsObj(colStats, "p=a", "id").getStatsData().getLongStats().getHighValue());
  }

  @Test
  public void testPartitionScopedAnalyzeCanNameThePartitionOfNoValue() {
    // the name Hive gives that partition is the one a statement gives back to reach it
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_named_null");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier +
        " (id bigint, p1 string, p2 string) PARTITIONED BY SPEC (p1, p2) " +
        "STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier +
        " VALUES (1, 'a', 'x'), (4, 'a', NULL), (7, 'a', 'null')");

    shell.executeStatement("ANALYZE TABLE " + identifier + " PARTITION (p1='a', p2='" +
        NULL_PART + "') COMPUTE STATISTICS FOR COLUMNS");

    List<ColumnStatistics> colStats = readCurrentColStats(identifier);
    Assert.assertEquals("only the partition holding no value for p2 was measured",
        Set.of("p1=a/p2=" + NULL_PART), colStatsPartitions(testTables.loadTable(identifier)));
    // the row holding the text "null" belongs to a partition of its own, which this never named
    ColumnStatisticsObj id = colStatsObj(colStats, "p1=a/p2=" + NULL_PART, "id");
    Assert.assertEquals(4L, id.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(4L, id.getStatsData().getLongStats().getHighValue());
  }

  @Test
  public void testPartitionScopedAnalyzeMeasuresEveryPartitionThePartialSpecNames() {
    // naming some of the partition columns names every partition that agrees on them, and the
    // statistics of each stand for that partition alone
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_two_keys");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier +
        " (id bigint, p1 string, p2 string) PARTITIONED BY SPEC (p1, p2) " +
        "STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier +
        " VALUES (1, 'a', 'x'), (9, 'a', 'y'), (7, 'b', 'x'), (4, 'a', NULL)");

    shell.executeStatement(
        "ANALYZE TABLE " + identifier + " PARTITION (p1='a') COMPUTE STATISTICS FOR COLUMNS");

    List<ColumnStatistics> colStats = readCurrentColStats(identifier);
    // the partition holding no value for p2 is named apart from one holding the text of one
    Assert.assertEquals("every partition agreeing on p1 was measured, and nothing else",
        Set.of("p1=a/p2=x", "p1=a/p2=y", "p1=a/p2=" + NULL_PART),
        colStatsPartitions(testTables.loadTable(identifier)));
    Assert.assertEquals(1L, colStatsObj(colStats, "p1=a/p2=x", "id")
        .getStatsData().getLongStats().getHighValue());
    Assert.assertEquals(9L, colStatsObj(colStats, "p1=a/p2=y", "id")
        .getStatsData().getLongStats().getHighValue());
  }

  @Test
  public void testPartitionScopedAnalyzeMeasuresATransformPartitionWhole() {
    // a value names rows, and the partition holding them holds more; naming it measures all of it
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_trunc");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (truncate(1, p)) STORED BY ICEBERG STORED AS PARQUET " +
        "TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'aa'), (9, 'ab'), (7, 'bb')");

    shell.executeStatement(
        "ANALYZE TABLE " + identifier + " PARTITION (p='aa') COMPUTE STATISTICS FOR COLUMNS");

    List<ColumnStatistics> colStats = readCurrentColStats(identifier);
    ColumnStatisticsObj truncatedToA = colStatsObj(colStats, "p_trunc=a", "id");
    Assert.assertEquals("the rows the value named are not the only ones the partition holds",
        1L, truncatedToA.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(9L, truncatedToA.getStatsData().getLongStats().getHighValue());
    Assert.assertEquals("the partition the statement never named stays undescribed",
        Set.of("p_trunc=a"), colStatsPartitions(testTables.loadTable(identifier)));
  }

  @Test
  public void testPartitionScopedAnalyzeMeasuresTheCurrentSpecPartitionAlone() {
    // a write lands in the current spec, so the partitions of an older one keep describing
    // themselves, and naming a partition measures the one the table writes today
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_evo_multispec");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET " +
        "TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (2, 'a'), (7, 'b')");
    shell.executeStatement("ALTER TABLE " + identifier + " SET PARTITION SPEC (p, truncate(1, p))");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (9, 'a')");

    shell.executeStatement(
        "ANALYZE TABLE " + identifier + " PARTITION (p='a') COMPUTE STATISTICS FOR COLUMNS");

    List<ColumnStatistics> colStats = readCurrentColStats(identifier);
    ColumnStatisticsObj currentSpec = colStatsObj(colStats, "p=a/p_trunc_1=a", "id");
    Assert.assertEquals(9L, currentSpec.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(9L, currentSpec.getStatsData().getLongStats().getHighValue());
    Assert.assertEquals("the older spec's partitions are not the ones the statement named",
        Set.of("p=a/p_trunc_1=a"), colStatsPartitions(testTables.loadTable(identifier)));
  }

  @Test
  public void testPartitionScopedAnalyzeWithoutStoredColStats() {
    // there is nothing stored to carry, so the partition it named is all the file has to hold
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_scoped_first");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET");
    // an insert gathers nothing while the table keeps its statistics per partition
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (7, 'b')");
    Assert.assertFalse(hasColStatsForCurrentSnapshot(identifier));

    shell.executeStatement("ANALYZE TABLE " + identifier + " PARTITION (p='a') COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertEquals(List.of("p=a"), colStatsPartNames(identifier));
  }

  @Test
  public void testPartitionScopedAnalyzeAfterWholeTableOverwrite() {
    // the snapshot it reads was written by something that covered the table, which says nothing
    // about what the ANALYZE itself was pointed at
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_after_iow");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT OVERWRITE TABLE " + identifier + " VALUES (1, 'a'), (7, 'b')");
    Assert.assertEquals(List.of("p=a", "p=b"), colStatsPartNames(identifier));

    shell.executeStatement("ANALYZE TABLE " + identifier + " PARTITION (p='a') COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertEquals(List.of("p=a", "p=b"), colStatsPartNames(identifier));
    Assert.assertEquals(7L, colStatsObj(readCurrentColStats(identifier), "p=b", "id")
        .getStatsData().getLongStats().getHighValue());
  }

  @Test
  public void testFullAnalyzeRetiresVanishedPartition() {
    // recomputing the whole table is a reset: a partition it no longer finds keeps no statistics,
    // where one that named partitions would have carried them over
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_retire");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (7, 'b')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertEquals(List.of("p=a", "p=b"), colStatsPartNames(identifier));

    shell.executeStatement("DELETE FROM " + identifier + " WHERE p = 'b'");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertEquals(List.of("p=a"), colStatsPartNames(identifier));
  }

  @Test
  public void testStatsWithPartitionScopedAnalyze() {
    // an ANALYZE naming a partition recomputes that one and leaves the others as they were
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_scoped");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (2, 'a'), (9, 'a'), (7, 'b')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertEquals(9L, colStatsObj(readCurrentColStats(identifier), "p=a", "id")
        .getStatsData().getLongStats().getHighValue());

    // take the row that bounded p=a away, so only statistics that replace rather than combine can
    // report the range that is left
    shell.executeStatement("DELETE FROM " + identifier + " WHERE id = 9");
    shell.executeStatement("ANALYZE TABLE " + identifier + " PARTITION (p='a') COMPUTE STATISTICS FOR COLUMNS");

    List<ColumnStatistics> colStats = readCurrentColStats(identifier);
    Assert.assertEquals(2L, colStatsObj(colStats, "p=a", "id").getStatsData().getLongStats().getHighValue());
    // p=b was not named, so its statistics came across untouched
    Assert.assertEquals(7L, colStatsObj(colStats, "p=b", "id").getStatsData().getLongStats().getHighValue());
  }

  @Test
  public void testStatsWithTransformPartition() {
    // a transform names its partition by the value it produced, which reads nothing like the sort
    // ordinal the transform hands back
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_day");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, ts timestamp) " +
        "PARTITIONED BY SPEC (day(ts)) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT OVERWRITE TABLE " + identifier +
        " VALUES (1, timestamp'2024-01-01 10:00:00'), (7, timestamp'2024-06-15 12:00:00')");

    List<ColumnStatistics> colStats = readCurrentColStats(identifier);
    Assert.assertEquals(1L,
        colStatsObj(colStats, "ts_day=2024-01-01", "id").getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(7L,
        colStatsObj(colStats, "ts_day=2024-06-15", "id").getStatsData().getLongStats().getLowValue());
  }

  @Test
  public void testStatsWithTimestampIdentityPartition() {
    // an identity timestamp is one Hive and Iceberg spell differently
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_ts");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, ts timestamp) " +
        "PARTITIONED BY SPEC (ts) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT OVERWRITE TABLE " + identifier +
        " VALUES (1, timestamp'2024-01-01 10:00:00'), (7, timestamp'2024-06-15 12:00:00')");

    checkColStat(identifier.name(), "id", true);
    checkColStatMinMaxValue(identifier.name(), "id", 1, 7);
  }

  @Test
  public void testStatsWithPartitionFieldsOutOfSchemaOrder() {
    // the spec orders its fields as it likes, which the row carrying their values has to follow
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_order");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, b string, c string) " +
        "PARTITIONED BY SPEC (c, b) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT OVERWRITE TABLE " + identifier + " VALUES (1, 'bb', 'cc'), (7, 'bb2', 'cc2')");

    List<ColumnStatistics> colStats = readCurrentColStats(identifier);
    Assert.assertEquals(1L, colStatsObj(colStats, "c=cc/b=bb", "id").getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(7L, colStatsObj(colStats, "c=cc2/b=bb2", "id").getStatsData().getLongStats().getLowValue());
  }

  @Test
  public void testStatsWithNullPartitionValue() {
    // the rows a partition holds none of a value for still form a partition of their own
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_null");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, ts timestamp) " +
        "PARTITIONED BY SPEC (day(ts)) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT OVERWRITE TABLE " + identifier +
        " VALUES (1, null), (7, timestamp'2024-06-15 12:00:00')");

    List<ColumnStatistics> colStats = readCurrentColStats(identifier);
    Assert.assertEquals(1L, colStatsObj(colStats, "ts_day=" + NULL_PART, "id")
        .getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(7L,
        colStatsObj(colStats, "ts_day=2024-06-15", "id").getStatsData().getLongStats().getLowValue());
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
    // partition-level statistics are maintained by complete-scope writers, not inserts
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

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

    checkColStat("target", "id", true);
    checkColStat("target", "dept", true);
    checkColStatMinMaxValue("target", "id", 0, 2);
    checkColStatMaxLengthDistinctValue("target", "dept", 5, 3);
    checkColStatMaxLengthDistinctValue("target", "name", 5, 3);
  }

  @Test
  public void testTableLevelColStatsForPartitionedCtas() {
    // the create gathered statistics covering every row it wrote: at table granularity those are
    // the table's own, so they stand without an analyze to follow
    assumeParquetHiveCatalogIceberg();

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, false);
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE TABLE source (id bigint, name string) PARTITIONED BY (dept string) STORED AS ORC");
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, TableIdentifier.of("default", "source"), false));

    shell.executeStatement(String.format(
        "CREATE TABLE target PARTITIONED BY (dept, name) STORED BY ICEBERG %s AS SELECT * FROM source s",
        testTables.propertiesForCreateTableSQL(
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.toString()))));

    checkColStat("target", "id", true);
    checkColStatMinMaxValue("target", "id", 0, 2);
  }

  @Test
  public void testTableLevelColStatsForPartitionedInsertOverwrite() {
    // an overwrite of the whole table recomputes every row, so at table granularity its statistics
    // are the table's; one scoped to a partition leaves the rows of the others behind
    assumeParquetHiveCatalogIceberg();

    TableIdentifier source = TableIdentifier.of("default", "iow_source");
    TableIdentifier identifier = TableIdentifier.of("default", "iow_target");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, false);

    shell.executeStatement("CREATE EXTERNAL TABLE " + source +
        " (id bigint, p string) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + source + " VALUES (11, \'a\'), (19, \'b\')");
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier +
        " (id bigint) PARTITIONED BY (p string) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " PARTITION (p=\'a\') VALUES (1), (2)");
    shell.executeStatement("INSERT INTO " + identifier + " PARTITION (p=\'b\') VALUES (7), (9)");

    shell.executeStatement(
        "INSERT OVERWRITE TABLE " + identifier + " PARTITION (p) SELECT id, p FROM " + source);
    Assert.assertTrue("the overwrite rewrote every row", colStatsAccurate(identifier));
    checkColStatMinMaxValue(identifier.name(), "id", 11, 19);

    shell.executeStatement("INSERT OVERWRITE TABLE " + identifier + " PARTITION (p=\'a\') VALUES (100)");
    Assert.assertFalse("a partition overwrite describes a slice, not the table",
        colStatsAccurate(identifier));
  }

  @Test
  public void testColStatsForInsertOverwriteEmptyingTheTable() {
    // an overwrite that selects nothing empties an unpartitioned table: the statistics describing
    // the rows it held must not outlive them
    assumeParquetHiveCatalogIceberg();

    TableIdentifier source = TableIdentifier.of("default", "iow_empty_source");
    TableIdentifier identifier = TableIdentifier.of("default", "iow_empty_target");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, false);

    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier +
        " (id bigint) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1), (2), (3)");
    checkColStatMinMaxValue(identifier.name(), "id", 1, 3);

    shell.executeStatement("CREATE EXTERNAL TABLE " + source +
        " (id bigint) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT OVERWRITE TABLE " + identifier + " SELECT id FROM " + source);

    Assert.assertEquals("the overwrite left no rows", Long.valueOf(0L),
        storageHandler().getRowCount(hmsTable(identifier)));
    Assert.assertTrue("the empty table's statistics are its own", colStatsAccurate(identifier));
    Assert.assertTrue(hasColStatsForCurrentSnapshot(identifier));
  }

  @Test
  public void testColStatsForCtasSelectingNoRows() {
    // the create wrote no rows: statistics describing none are the ones the table has
    assumeParquetHiveCatalogIceberg();

    TableIdentifier source = TableIdentifier.of("default", "ctas_empty_source");
    TableIdentifier identifier = TableIdentifier.of("default", "ctas_empty_target");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, false);

    shell.executeStatement("CREATE EXTERNAL TABLE " + source +
        " (id bigint) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + source + " VALUES (1), (2)");
    shell.executeStatement("CREATE TABLE " + identifier.name() +
        " STORED BY ICEBERG STORED AS PARQUET AS SELECT id FROM " + source + " WHERE id < 0");

    // writing no rows commits no snapshot, so the table has no state for statistics to describe
    Assert.assertNull(testTables.loadTable(identifier).currentSnapshot());
    Assert.assertNull(storageHandler().getRowCount(hmsTable(identifier)));
    Assert.assertFalse("no statistics stand for a table that holds nothing",
        colStatsAccurate(identifier));
  }

  @Test
  public void testColStatsAfterEmptyingPartitionedTable() {
    // a delete of every row and a truncate both leave the table empty: the statistics of the rows
    // they removed must not outlive them
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_emptied");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, false);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint) PARTITIONED BY (p string) " +
        "STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2', 'external.table.purge'='true')");
    shell.executeStatement("INSERT INTO " + identifier + " PARTITION (p=\'a\') VALUES (1), (2)");
    shell.executeStatement("INSERT INTO " + identifier + " PARTITION (p=\'b\') VALUES (7), (9)");
    checkColStatMinMaxValue(identifier.name(), "id", 1, 9);

    shell.executeStatement("DELETE FROM " + identifier);
    Assert.assertEquals(Long.valueOf(0L), storageHandler().getRowCount(hmsTable(identifier)));
    Assert.assertTrue("the walk stops at the emptied snapshot", readCurrentColStats(identifier).isEmpty());
    Assert.assertFalse(colStatsAccurate(identifier));

    // and the next write re-anchors the chain rather than extending what the delete left
    shell.executeStatement("INSERT INTO " + identifier + " PARTITION (p=\'c\') VALUES (20)");
    Assert.assertTrue(hasColStatsForCurrentSnapshot(identifier));
    checkColStatMinMaxValue(identifier.name(), "id", 20, 20);

    shell.executeStatement("TRUNCATE TABLE " + identifier);
    Assert.assertEquals(Long.valueOf(0L), storageHandler().getRowCount(hmsTable(identifier)));
    Assert.assertTrue("the truncate leaves nothing to serve", readCurrentColStats(identifier).isEmpty());
  }

  @Test
  public void aFullTableAnalyzeReplacesRatherThanCarriesThePartitionsItDidNotGather() {
    // the hammer: a gather that read the whole table stores what it read and nothing besides, so a
    // partition that no longer exists leaves with it. A merge would carry it forward instead.
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_reanalyzed_whole");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint) PARTITIONED BY (p string) " +
        "STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2', 'external.table.purge'='true')");
    shell.executeStatement("INSERT INTO " + identifier + " PARTITION (p='a') VALUES (1), (2)");
    shell.executeStatement("INSERT INTO " + identifier + " PARTITION (p='b') VALUES (7), (9)");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertEquals("both partitions were gathered", 2, readCurrentColStats(identifier).size());

    shell.executeStatement("DELETE FROM " + identifier + " WHERE p = 'b'");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    List<ColumnStatistics> stored = readCurrentColStats(identifier);
    Assert.assertEquals("only the partition that still holds rows was gathered, and it is all that stands",
        1, stored.size());
    Assert.assertTrue("the partition left standing is the one that survived",
        stored.get(0).getStatsDesc().getPartName().contains("a"));
  }

  @Test
  public void anAnalyzeOfAnEmptiedTableServesNoneOfWhatItHeld() {
    // a gather of a table with no rows left groups nothing, so it stores no partition at all.
    // What must not happen is the statistics of the deleted rows outliving them.
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_emptied_reanalyzed");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint) PARTITIONED BY (p string) " +
        "STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2', 'external.table.purge'='true')");
    shell.executeStatement("INSERT INTO " + identifier + " PARTITION (p='a') VALUES (1), (2)");
    shell.executeStatement("INSERT INTO " + identifier + " PARTITION (p='b') VALUES (7), (9)");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertEquals("both partitions were gathered", 2, readCurrentColStats(identifier).size());

    shell.executeStatement("DELETE FROM " + identifier);
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    Assert.assertEquals(Long.valueOf(0L), storageHandler().getRowCount(hmsTable(identifier)));
    Assert.assertTrue("the statistics of the deleted rows must not outlive them",
        readCurrentColStats(identifier).isEmpty());
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
  public void testTableLevelColStatsTakeInWhatEachInsertAdds() {
    // statistics kept for the table as a whole take in what a write adds to it, so a table
    // described in full stays described as rows arrive
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_incr");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, false);
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET " +
        "TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (7, 'b')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertTrue(colStatsAccurate(identifier));
    checkColStatMinMaxValue(identifier.name(), "id", 1, 7);

    shell.executeStatement("INSERT INTO " + identifier + " VALUES (100, 'c')");

    Assert.assertTrue("the table stays described, having taken the write in",
        colStatsAccurate(identifier));
    checkColStatMinMaxValue(identifier.name(), "id", 1, 100);
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, true);
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

    Path tblColPath = new Path(IcebergStoredStats.getColStatsFile(
        table, table.currentSnapshot().snapshotId(), partitionLevel(table)).path());
    Assert.assertNotNull(tblColPath);
    // Check that if colPath is created correctly
    Assert.assertTrue(tblColPath.getFileSystem(shell.getHiveConf()).exists(tblColPath));
    List<Object[]> result = shell.executeStatement("SELECT * FROM customers");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, result));
  }

  @Test
  public void testGetAggrBasicStatsForPartitioned() throws SemanticException {
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
  public void anAskCoveringEveryPartitionIsAnsweredFromWhatWasFolded() throws MetaException {
    // reading every partition is asking what the table holds, which the file already states from
    // the same partitions - and asking about a subset still merges the partitions themselves
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_folded_ask");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1,'a'), (5,'b'), (9,'c')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);
    AggrStats every = storageHandler().getAggrColStatsFor(
        hmsTable, ImmutableList.of("id"), ImmutableList.of("p=a", "p=b", "p=c"));
    LongColumnStatsData all = every.getColStats().getFirst().getStatsData().getLongStats();
    Assert.assertEquals("the fold answers for every partition", 1L, all.getLowValue());
    Assert.assertEquals(9L, all.getHighValue());
    Assert.assertEquals("three partitions, three values", 3L, all.getNumDVs());

    // a subset still takes the partitions themselves, so it must answer for those alone
    AggrStats some = storageHandler().getAggrColStatsFor(
        hmsTable, ImmutableList.of("id"), ImmutableList.of("p=a"));
    LongColumnStatsData one = some.getColStats().getFirst().getStatsData().getLongStats();
    Assert.assertEquals("a subset is not the fold", 1L, one.getLowValue());
    Assert.assertEquals(1L, one.getHighValue());
  }

  @Test
  public void anotherEnginesBlobBesideHivesLeavesHivesReadableAndKeepsItsVector() throws IOException {
    // a file may hold both: another engine keeps its own sketches across its writes, and Hive's
    // entries sit beside them. One blob it cannot read must cost it that blob, not the file - and
    // an entry released before this still carries the vector a merge of distinct counts needs
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_mixed_blobs");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, false);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier +
        " (id bigint) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1), (5)");

    Table table = testTables.loadTable(identifier);
    int id = table.schema().findField("id").fieldId();
    long snapshotId = table.currentSnapshot().snapshotId();

    LongColumnStatsData data = new LongColumnStatsData(0L, 2L);
    data.setLowValue(1L);
    data.setHighValue(5L);
    data.setBitVectors(new byte[] {'H', 'L', 7, 7, 7, 7});
    ColumnStatisticsObj released = new ColumnStatisticsObj("id", "bigint",
        ColumnStatisticsData.longStats(data));

    String path = table.location() + "/stats/mixed-" + UUID.randomUUID();
    StatisticsFile statsFile;
    try (PuffinWriter writer = Puffin.write(table.io().newOutputFile(path)).createdBy("both").build()) {
      writer.add(new Blob(IcebergColStatsWriter.LEGACY_COL_STATS_BLOB, ImmutableList.of(id), snapshotId,
          table.currentSnapshot().sequenceNumber(),
          ByteBuffer.wrap(org.apache.commons.lang3.SerializationUtils.serialize(released)),
          PuffinCompressionCodec.NONE, ImmutableMap.of()));
      writer.add(new Blob("apache-datasketches-theta-v1", ImmutableList.of(id), snapshotId,
          table.currentSnapshot().sequenceNumber(), ByteBuffer.wrap(new byte[] {1}),
          PuffinCompressionCodec.NONE, ImmutableMap.of("ndv", "7")));
      writer.finish();
      statsFile = new GenericStatisticsFile(snapshotId, path, writer.fileSize(), writer.footerSize(),
          writer.writtenBlobsMetadata().stream().map(GenericBlobMetadata::from).toList());
    }
    table.updateStatistics().setStatistics(statsFile).commit();
    table.refresh();

    List<ColumnStatisticsObj> read =
        IcebergColStatsReader.read(table, table.currentSnapshot().snapshotId(), null, true);

    Assert.assertEquals("the blob it cannot read costs it that blob, not the file", 1, read.size());
    Assert.assertEquals(1L, read.getFirst().getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(5L, read.getFirst().getStatsData().getLongStats().getHighValue());
    Assert.assertTrue("the vector a merge would need is still there",
        read.getFirst().getStatsData().getLongStats().isSetBitVectors());
  }

  @Test
  public void testAnalyzePartitionSpecRejected() {
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    createPartitionedCustomers(identifier);

    // the metastore holds one row count for the table, with nowhere to record a single partition's
    AssertHelpers.assertThrows(
        "Should reject partition-scoped basic-stats ANALYZE for non-native partitioned tables",
        IllegalArgumentException.class, ErrorMsg.ANALYZE_PARTITION_NON_NATIVE.getMsg(),
        () -> shell.executeStatement(
            "ANALYZE TABLE " + identifier + " PARTITION (last_name='Brown') COMPUTE STATISTICS")
    );
    // and column statistics of one partition need a table that keeps them per partition
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, false);
    AssertHelpers.assertThrows(
        "Should reject partition-scoped column-stats ANALYZE when statistics are kept for the table",
        IllegalArgumentException.class, ErrorMsg.ANALYZE_PARTITION_NON_NATIVE.getMsg(),
        () -> shell.executeStatement(
            "ANALYZE TABLE " + identifier + " PARTITION (last_name='Brown') COMPUTE STATISTICS FOR COLUMNS")
    );
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, true);
  }

  @Test
  public void testStatsWithPartitionScopedInsertOverwrite() {
    // naming the partition to overwrite reaches the same statistics as letting the rows choose it
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_static");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (2, 'a'), (7, 'b')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    shell.executeStatement("INSERT OVERWRITE TABLE " + identifier + " PARTITION (p='a') VALUES (5)");

    List<ColumnStatistics> colStats = readCurrentColStats(identifier);
    Assert.assertEquals(5L, colStatsObj(colStats, "p=a", "id").getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(5L, colStatsObj(colStats, "p=a", "id").getStatsData().getLongStats().getHighValue());
    Assert.assertEquals(7L, colStatsObj(colStats, "p=b", "id").getStatsData().getLongStats().getHighValue());
  }

  @Test
  public void testAnalyzeCatchesUpPartitionStats() throws SemanticException {
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
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

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

    // single-partition probes pin each blob's content to its name, which the span check cannot
    AggrStats brown = handler.getAggrColStatsFor(hmsTable, ImmutableList.of("customer_id"),
        List.of("last_name=Brown"));
    Assert.assertEquals(1, brown.getPartsFound());
    Assert.assertEquals(0, brown.getColStats().get(0).getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(0, brown.getColStats().get(0).getStatsData().getLongStats().getHighValue());
    AggrStats pink = handler.getAggrColStatsFor(hmsTable, ImmutableList.of("customer_id"),
        List.of("last_name=Pink"));
    Assert.assertEquals(1, pink.getPartsFound());
    Assert.assertEquals(2, pink.getColStats().get(0).getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(2, pink.getColStats().get(0).getStatsData().getLongStats().getHighValue());
  }

  @Test
  public void testGetAggrColStatsForNullAndEmptyPartitions() throws Exception {
    // NULL and empty-string partition values render apart, and do so on both the
    // blob-write side (ANALYZE) and the pruned-name side; a rendering mismatch silently drops the
    // partition from the aggregation and partial aggregation extrapolates fabricated NDVs
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    createPartitionedCustomers(identifier);
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (3, 'Alice', NULL), (4, 'Eve', '')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);
    HiveIcebergStorageHandler handler = storageHandler();

    List<String> partNames = partitionNames(handler, hmsTable);
    Assert.assertEquals(partNames.toString(), 5, partNames.size());
    Assert.assertTrue(partNames.toString(), partNames.contains("last_name=" + NULL_PART));
    Assert.assertTrue(partNames.toString(), partNames.contains("last_name="));

    // the blobs must carry the read side's names: an empty-string value that decodes as null would
    // pass the aggregate below by double-serving the null partition's key
    Assert.assertEquals(partNames.stream().sorted().toList(), colStatsPartNames(identifier));
    assertAggrColStatsRange(identifier, "customer_id", partNames, 0, 4);
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
  public void testBranchWriteComputesItsOwnPartitionStats() {
    // the partition statistics file is anchored to a snapshot: a write to a branch computes it
    // against the branch's head, leaving the table's own file and head where they were
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers_branch_part");
    createPartitionedCustomers(identifier);

    Table icebergTable = testTables.loadTable(identifier);
    long tableSnapshotId = icebergTable.currentSnapshot().snapshotId();
    Assert.assertNotNull(IcebergStoredStats.getPartitionStatsFile(icebergTable, tableSnapshotId));

    shell.executeStatement("ALTER TABLE " + identifier + " CREATE BRANCH b1");
    shell.executeStatement("INSERT INTO " + identifier + ".branch_b1 VALUES (100, \'Bob\', \'Brown\')");

    icebergTable.refresh();
    Assert.assertEquals("the branch write leaves the table's head where it was",
        tableSnapshotId, icebergTable.currentSnapshot().snapshotId());
    Assert.assertNotNull("the table keeps the partition statistics describing it",
        IcebergStoredStats.getPartitionStatsFile(icebergTable, tableSnapshotId));
    Assert.assertNotNull("the branch's head carries the statistics its write computed",
        IcebergStoredStats.getPartitionStatsFile(icebergTable, icebergTable.snapshot("b1").snapshotId()));
  }

  @Test
  public void testCountStarWithoutPartitionStatsFile() {
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    createPartitionedCustomers(identifier, false);

    Table icebergTable = testTables.loadTable(identifier);
    Assert.assertNull(IcebergStoredStats.getPartitionStatsFile(
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
  public void testRowCountWithDeletes() throws SemanticException {
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
        IcebergPartitionStatsReader.read(icebergTable, icebergTable.currentSnapshot());
    Assert.assertEquals(3L, fileStats.get(DummyPartition.VOID).dataRecordCount().longValue());

    // column stats blobs are written per physical partition; the legacy unpartitioned rows share one
    // blob under the synthetic partition name, so values existing solely among them (Green, Pink)
    // are accounted there
    List<ColumnStatistics> colStats =
        readColStats(icebergTable, icebergTable.currentSnapshot().snapshotId());
    Assert.assertEquals(
        List.of(DummyPartition.VOID,
            "last_name=Barna", "last_name=Brown", "last_name=Rozsaszin", "last_name=Zold"),
        colStats.stream().map(stats -> stats.getStatsDesc().getPartName()).sorted().toList());

    // the legacy blob covers exactly the rows written before the table was partitioned
    ColumnStatisticsObj legacyCustomerId = colStatsObj(colStats, DummyPartition.VOID, "customer_id");
    Assert.assertEquals(0L, legacyCustomerId.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(2L, legacyCustomerId.getStatsData().getLongStats().getHighValue());

    // a blob describes the physical partition's files only: the legacy Brown row (customer_id 0) does not
    // merge into the Brown partition's blob, which covers just the new-spec row (customer_id 3)
    ColumnStatisticsObj brownCustomerId = colStatsObj(colStats, "last_name=Brown", "customer_id");
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
  public void testAggrColStatsAfterEvolutionFromUnpartitioned() throws Exception {
    // the legacy unpartitioned-spec rows are computed by a dedicated ANALYZE arm and stored under the
    // synthetic partition's blob, so an aggregation over a pruned list holding the synthetic partition
    // is complete: no extrapolation, and the legacy rows' values are accounted for
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    createEvolvedCustomers(identifier);
    // a genuine NULL partition value: its group's partition tuple is all null, exactly like the
    // legacy rows' - only the spec id may tell them apart
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (6, 'Nia', NULL)");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);
    HiveIcebergStorageHandler handler = storageHandler();
    List<String> statNames = Lists.newArrayList(partitionNames(handler, hmsTable));
    statNames.add(DummyPartition.VOID);
    Assert.assertTrue(statNames.toString(), statNames.contains("last_name=" + NULL_PART));

    // customer ids 0..2 exist only among the legacy unpartitioned rows, 3..6 in the partitioned ones
    assertAggrColStatsRange(identifier, "customer_id", statNames, 0, 6);
  }

  @Test
  public void testRowCountAfterEvolutionFromUnpartitioned() throws SemanticException {
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

  @Test
  public void testAnalyzeColStatsInBatches() {
    // guards the single-batch persist exemption: the storage handler holds one statistics file per
    // snapshot, so honoring hive.stats.max.num.stats would let every batch replace the previous one
    // (this cap would force one partition per batch and only the last would survive)
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    createPartitionedCustomers(identifier);
    // 3 stats objects per partition (customer_id, first_name, last_name): one partition per batch
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_MAX_NUM_STATS.varname, "3");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    Assert.assertEquals(
        List.of("last_name=Brown", "last_name=Green", "last_name=Pink"),
        colStatsPartNames(identifier));
  }

  @Test
  public void testAggrColStatsAfterPartitionedSpecEvolution() {
    // two partitioned specs: every row is grouped and its blob named under the spec that wrote it,
    // in a single ANALYZE pass (the per-spec union rewrite could not even compile - the branches'
    // partition structs had different field names)
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").build();
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    testTables.createTable(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, spec,
        fileFormat, ImmutableList.of(), formatVersion);
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, false));
    shell.executeStatement("ALTER TABLE " + identifier + " SET PARTITION SPEC (first_name)");
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1, identifier, false));

    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    Assert.assertEquals(
        List.of("first_name=Laci", "first_name=Marci", "first_name=Peti",
            "last_name=Brown", "last_name=Green", "last_name=Pink"),
        colStatsPartNames(identifier));
  }

  @Test
  public void testAggrColStatsForYearTransformPartitions() throws Exception {
    // time-transform partition values must render as Iceberg's human form ("2023"), not the raw
    // transform ordinal ("53"): statistics and partition pruning join on the rendered name
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_by_year");
    createDatePartitionedTable(identifier);
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, date '2023-03-04'), " +
        "(2, date '2023-11-11'), (3, date '2024-06-01'), (4, date '1969-06-01')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    // partition names as the read side renders them via partitionToPath (getPartitions cannot list
    // non-identity transforms - its partition filter only supports identity columns); the year
    // ordinal is negative for pre-1970 dates
    List<String> partNames = ImmutableList.of("d_year=1969", "d_year=2023", "d_year=2024");
    assertAggrColStatsRange(identifier, "id", partNames, 1, 4);
  }

  @Test
  public void testAggrColStatsForTimestampIdentityPartitions() throws Exception {
    // identity-partitioned timestamps: Hive renders the value with a space separator, the blob name
    // must carry Iceberg's ISO rendering (Conversions.fromPartitionString cannot parse timestamps)
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "events_by_ts");
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, ts timestamp) " +
        "PARTITIONED BY SPEC (ts) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier +
        " VALUES (1, timestamp '2024-06-01 10:00:00'), (2, timestamp '2024-06-01 10:00:00'), " +
        "(3, timestamp '2023-11-11 23:59:59')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    // partitionToPath URL-escapes the ISO rendering (':' -> %3A)
    List<String> partNames = ImmutableList.of("ts=2023-11-11T23%3A59%3A59", "ts=2024-06-01T10%3A00%3A00");
    Assert.assertEquals(partNames, colStatsPartNames(identifier));
    assertAggrColStatsRange(identifier, "id", partNames, 1, 3);
  }

  @Test
  public void testAggrColStatsForCaseSensitivePartitionField() throws Exception {
    // Hive's makePartName lowercases the wire keys, so the decode must match the partition field
    // case-insensitively: a case-preserving field name (e.g. Spark-created) would otherwise decode
    // every group's value to null
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "events_case");
    Schema schema = new Schema(
        NestedField.optional(1, "id", Types.LongType.get()),
        NestedField.optional(2, "eventDate", Types.DateType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("eventDate").build();
    testTables.createTable(shell, identifier.name(), schema, spec, fileFormat, ImmutableList.of(), formatVersion);
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, date '2023-03-04'), (2, date '2024-06-01')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    List<String> partNames = ImmutableList.of("eventDate=2023-03-04", "eventDate=2024-06-01");
    Assert.assertEquals(partNames, colStatsPartNames(identifier));
    assertAggrColStatsRange(identifier, "id", partNames, 1, 2);
  }

  @Test
  public void testAggrColStatsForTimestampLocalTZIdentityPartitions() throws Exception {
    // identity-partitioned zoned timestamps: Hive renders the group value with a trailing zone id
    // ("2024-06-01 10:00:00.0 UTC"), which the decode must map back to the instant's micros
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "events_by_ltz");
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier +
        " (id bigint, ts timestamp with local time zone) " +
        "PARTITIONED BY SPEC (ts) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier +
        " VALUES (1, timestamp '2024-06-01 10:00:00'), (2, timestamp '2024-06-01 10:00:00'), " +
        "(3, timestamp '2023-11-11 23:59:59')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);
    List<String> partNames = partitionNames(storageHandler(), hmsTable);
    Assert.assertEquals(2, partNames.size());
    Assert.assertEquals(partNames.stream().sorted().toList(), colStatsPartNames(identifier));
    assertAggrColStatsRange(identifier, "id", partNames, 1, 3);
  }

  @Test
  public void testAggrColStatsForTimeTransformEvolutions() throws Exception {
    // year -> month -> day evolutions: one ANALYZE pass names each group's blob with the human
    // rendering of the owning spec's transform ordinal
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_by_time");
    createDatePartitionedTable(identifier);
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, date '2023-03-04')");
    shell.executeStatement("ALTER TABLE " + identifier + " SET PARTITION SPEC (month(d))");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (2, date '2023-11-11')");
    shell.executeStatement("ALTER TABLE " + identifier + " SET PARTITION SPEC (day(d))");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (3, date '2024-06-01')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    List<String> partNames = List.of("d_day=2024-06-01", "d_month=2023-11", "d_year=2023");
    Assert.assertEquals(partNames, colStatsPartNames(identifier));
    assertAggrColStatsRange(identifier, "id", partNames, 1, 3);
  }

  @Test
  public void testAutoGatherSkipsPartitionedInsert() {
    // partition-level statistics are maintained by complete-scope writers only: with autogather
    // on, a plain INSERT into a partitioned table persists nothing and the analyzed file carries
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_autogather");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    createDatePartitionedTable(identifier);
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, date '2023-03-04')");
    Assert.assertFalse(hasColStatsForCurrentSnapshot(identifier));

    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertTrue(hasColStatsForCurrentSnapshot(identifier));
    Assert.assertTrue(colStatsAccurate(identifier));

    shell.executeStatement("INSERT INTO " + identifier + " VALUES (2, date '2023-11-11'), " +
        "(3, date '2024-06-01')");
    // no new statistics file: the analyzed one keeps serving as an approximation
    Assert.assertFalse(hasColStatsForCurrentSnapshot(identifier));
    Assert.assertFalse(colStatsAccurate(identifier));
    Assert.assertEquals(List.of("d_year=2023"), colStatsPartNames(identifier));
  }

  @Test
  public void testTableLevelColStatsFallbackForPartitioned() throws Exception {
    // hive.iceberg.stats.collect.partlevel=false trades partition granularity for cheap
    // maintenance: ANALYZE and autogather keep a single table-level file for the partitioned
    // table, inserts merge into it incrementally, and planning serves it over the pruned set
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_tbl_level");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, false);
    createDatePartitionedTable(identifier);
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, date '2023-03-04'), (3, date '2023-03-04')");
    Assert.assertTrue(hasColStatsForCurrentSnapshot(identifier));
    Assert.assertTrue(colStatsAccurate(identifier));
    // the file is table-level shaped: no blob carries a partition name
    Assert.assertTrue(testTables.loadTable(identifier).statisticsFiles().stream()
        .flatMap(statsFile -> statsFile.blobMetadata().stream())
        .noneMatch(blob -> blob.properties().containsKey(IcebergColStatsWriter.PARTITION_FIELD)));

    shell.executeStatement("INSERT INTO " + identifier + " VALUES (5, date '2024-05-05')");
    // the increment merged into the table-level statistics
    Assert.assertTrue(hasColStatsForCurrentSnapshot(identifier));
    Assert.assertTrue(colStatsAccurate(identifier));
    checkColStatMinMaxValue(identifier.name(), "id", 1, 5);

    // the partition-level aggregation finds nothing to serve, at no read cost: the planner
    // falls back to the table-level statistics
    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);
    AggrStats aggrStats = storageHandler().getAggrColStatsFor(hmsTable, ImmutableList.of("id"),
        partitionNames(storageHandler(), hmsTable));
    Assert.assertEquals(0, aggrStats.getPartsFound());
    Assert.assertTrue(aggrStats.getColStats().isEmpty());

    shell.executeStatement("DELETE FROM " + identifier + " WHERE id = 1");
    Assert.assertFalse(colStatsAccurate(identifier));
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertTrue(colStatsAccurate(identifier));
    checkColStatMinMaxValue(identifier.name(), "id", 3, 5);
  }

  @Test
  public void testAnalyzeReanchorsAfterStatsGap() {
    // snapshots committed without statistics leave the stored file behind: the previous
    // statistics keep serving as approximations, and ANALYZE re-anchors accounting every row
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_gap");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    createDatePartitionedTable(identifier);
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, date '2023-03-04')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertTrue(hasColStatsForCurrentSnapshot(identifier));
    Assert.assertTrue(colStatsAccurate(identifier));

    shell.executeStatement("INSERT INTO " + identifier + " VALUES (2, date '2023-11-11')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (3, date '2023-06-01')");
    Assert.assertFalse(hasColStatsForCurrentSnapshot(identifier));
    Assert.assertFalse(colStatsAccurate(identifier));
    // the pre-gap statistics keep serving
    Assert.assertFalse(readCurrentColStats(identifier).isEmpty());

    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    List<ColumnStatistics> colStats = readCurrentColStats(identifier);
    // after ANALYZE the statistics account for every row
    ColumnStatisticsObj id2023 = colStatsObj(colStats, "d_year=2023", "id");
    Assert.assertEquals(1L, id2023.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(3L, id2023.getStatsData().getLongStats().getHighValue());
    Assert.assertTrue(colStatsAccurate(identifier));
  }

  @Test
  public void testColStatsServedButFrozenAfterDelete() {
    // DML clears the accuracy flag: increments stop extending the statistics (ACID rule), the
    // pre-delete file keeps serving as an approximation, and ANALYZE recomputes exactly
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_after_delete");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    createDatePartitionedTable(identifier);
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, date '2023-03-04'), (2, date '2023-04-04')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertTrue(hasColStatsForCurrentSnapshot(identifier));
    Assert.assertTrue(colStatsAccurate(identifier));

    shell.executeStatement("DELETE FROM " + identifier + " WHERE id = 1");
    Assert.assertFalse(hasColStatsForCurrentSnapshot(identifier));
    Assert.assertFalse(colStatsAccurate(identifier));
    // the pre-delete statistics keep serving as an approximation
    ColumnStatisticsObj id2023 = colStatsObj(readCurrentColStats(identifier), "d_year=2023", "id");
    Assert.assertEquals(1L, id2023.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(2L, id2023.getStatsData().getLongStats().getHighValue());

    shell.executeStatement("INSERT INTO " + identifier + " VALUES (5, date '2024-05-05')");
    // an insert maintains no partition-level statistics: the pre-delete file keeps serving
    Assert.assertFalse(hasColStatsForCurrentSnapshot(identifier));
    Assert.assertFalse(colStatsAccurate(identifier));
    Assert.assertEquals(List.of("d_year=2023"), colStatsPartNames(identifier));

    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertTrue(colStatsAccurate(identifier));
    Assert.assertEquals(List.of("d_year=2023", "d_year=2024"), colStatsPartNames(identifier));
    // recomputed: the deleted row no longer bounds the range
    id2023 = colStatsObj(readCurrentColStats(identifier), "d_year=2023", "id");
    Assert.assertEquals(2L, id2023.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(2L, id2023.getStatsData().getLongStats().getHighValue());
  }

  @Test
  public void testColStatsNotAccurateAfterExternalWrite() {
    // an engine that maintains no Hive statistics can commit at any time: the accuracy flag is
    // trusted only while the current snapshot carries its own statistics file
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_external");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    createDatePartitionedTable(identifier);
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, date '2023-03-04')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertTrue(colStatsAccurate(identifier));

    // a foreign commit: new snapshot, no statistics file, no flag maintenance
    testTables.loadTable(identifier).newAppend().commit();

    shell.executeStatement("INSERT INTO " + identifier + " VALUES (2, date '2023-06-01')");
    // inserts maintain no partition-level statistics; the pre-existing ones keep serving as
    // approximations until ANALYZE recomputes
    Assert.assertFalse(hasColStatsForCurrentSnapshot(identifier));
    Assert.assertFalse(colStatsAccurate(identifier));
    Assert.assertFalse(readCurrentColStats(identifier).isEmpty());

    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertTrue(colStatsAccurate(identifier));
  }

  @Test
  public void testSubsetColumnAnalyzeReplacesFile() {
    // ANALYZE FOR COLUMNS on a subset replaces the statistics file whole: only the analyzed
    // columns' statistics remain
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_subset");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    createDatePartitionedTable(identifier);
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, date '2023-03-04'), (4, date '2023-04-04')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertTrue(colStatsAccurate(identifier));

    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS id");
    List<ColumnStatistics> colStats = readCurrentColStats(identifier);
    ColumnStatisticsObj id2023 = colStatsObj(colStats, "d_year=2023", "id");
    Assert.assertEquals(1L, id2023.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(4L, id2023.getStatsData().getLongStats().getHighValue());
    Assert.assertTrue(colStats.stream()
        .flatMap(stats -> stats.getStatsObj().stream())
        .noneMatch(obj -> "d".equals(obj.getColName())));
  }

  @Test
  public void testAnalyzeRecomputesAfterDml() {
    // DML stales the statistics; a table-wide ANALYZE recomputes every partition exactly and
    // restores the accuracy flag
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_part_analyze");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (2, 'a'), (7, 'b')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertTrue(colStatsAccurate(identifier));

    shell.executeStatement("DELETE FROM " + identifier + " WHERE id = 2");
    Assert.assertFalse(colStatsAccurate(identifier));

    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    List<ColumnStatistics> colStats = readCurrentColStats(identifier);
    // p=a recomputed exactly: the deleted row no longer bounds it
    ColumnStatisticsObj idA = colStatsObj(colStats, "p=a", "id");
    Assert.assertEquals(1L, idA.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(1L, idA.getStatsData().getLongStats().getHighValue());
    ColumnStatisticsObj idB = colStatsObj(colStats, "p=b", "id");
    Assert.assertEquals(7L, idB.getStatsData().getLongStats().getLowValue());
    Assert.assertTrue(colStatsAccurate(identifier));
  }

  @Test
  public void testColStatsSurviveDataNeutralRewrite() {
    // a rewrite that changes no rows (compaction, Hive's or a foreign engine's) commits a
    // "replace" snapshot: it neither outdates served statistics nor breaks the merge chain
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_rewritten");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint) " +
        "STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1)");
    Assert.assertTrue(colStatsAccurate(identifier));

    commitDataNeutralRewrite(identifier);

    shell.executeStatement("INSERT INTO " + identifier + " VALUES (2)");
    // the increment merged across the replace snapshot instead of being dropped
    Assert.assertTrue(hasColStatsForCurrentSnapshot(identifier));
    Assert.assertTrue(colStatsAccurate(identifier));
    checkColStatMinMaxValue(identifier.name(), "id", 1, 2);
  }

  /** Rewrites the table's single data file into a byte-identical copy: a "replace" commit. */
  private void commitDataNeutralRewrite(TableIdentifier identifier) {
    Table icebergTable = testTables.loadTable(identifier);
    try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().planFiles()) {
      DataFile dataFile = tasks.iterator().next().file();
      String copyPath = dataFile.location() + "-copy";
      try (InputStream in = icebergTable.io().newInputFile(dataFile.location()).newStream();
          OutputStream out = icebergTable.io().newOutputFile(copyPath).create()) {
        in.transferTo(out);
      }
      DataFile copy = DataFiles.builder(icebergTable.spec())
          .copy(dataFile)
          .withPath(copyPath)
          .build();
      icebergTable.newRewrite()
          .rewriteFiles(Set.of(dataFile), Set.of(copy))
          .commit();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    Assert.assertEquals(DataOperations.REPLACE,
        testTables.loadTable(identifier).currentSnapshot().operation());
  }

  @Test
  public void testEmptyWriteKeepsColStats() {
    // an insert that adds no files commits no snapshot: the statistics it computed describe
    // nothing and must not replace the stored ones
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_empty_write");
    TableIdentifier source = TableIdentifier.of("default", "orders_empty_src");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("CREATE EXTERNAL TABLE " + source + " (id bigint) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1), (5)");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    checkColStatMinMaxValue(identifier.name(), "id", 1, 5);
    long snapshotId = testTables.loadTable(identifier).currentSnapshot().snapshotId();

    shell.executeStatement("INSERT INTO " + identifier + " SELECT id FROM " + source);

    // no commit, so the statistics of the unchanged snapshot keep serving, accurate
    Assert.assertEquals(snapshotId, testTables.loadTable(identifier).currentSnapshot().snapshotId());
    checkColStatMinMaxValue(identifier.name(), "id", 1, 5);
    Assert.assertTrue(colStatsAccurate(identifier));
  }

  @Test
  public void testMergeCompletesOnlyTheColumnsTheStoredFileDescribes() {
    // ANALYZE of one column stores a file describing it alone; the increment an insert gathers
    // for the other column has no stored whole to add itself to, so it must not enter as one
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_partial_columns");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier +
        " (id bigint, v bigint) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 100)");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS id");

    shell.executeStatement("INSERT INTO " + identifier + " VALUES (2, 5)");

    // the file holds no half-truth for v: the increment's entry was not promoted
    List<ColumnStatisticsObj> stored = readCurrentColStats(identifier).getFirst().getStatsObj();
    Assert.assertEquals(List.of("id"), stored.stream().map(ColumnStatisticsObj::getColName).toList());
  }

  @Test
  public void testGatherFailureFailsTheWriteInsteadOfDroppingIt() {
    // a pull that fails after the first batch must fail the statement: reporting success with
    // the statistics silently dropped is what hive.stats.reliable forbids
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_failing_gather");
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1), (5)");

    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);
    ColumnStatistics first = new ColumnStatistics(
        new ColumnStatisticsDesc(true, "default", identifier.name()), List.of());
    Iterator<ColumnStatistics> failing = new Iterator<>() {
      private boolean served = false;

      @Override
      public boolean hasNext() {
        return true;
      }

      @Override
      public ColumnStatistics next() {
        if (served) {
          throw new RuntimeException("Failed to fetch computed column statistics");
        }
        served = true;
        return first;
      }
    };
    HiveIcebergStorageHandler handler = storageHandler();
    Assert.assertThrows(RuntimeException.class, () -> handler.setColStatistics(hmsTable, failing));
    Assert.assertFalse("a failed gather must not publish statistics", hasColStatsForCurrentSnapshot(identifier));
  }

  @Test
  public void testAGatherThatStoresNothingLeavesWhatIsStoredAlone() throws Exception {
    // a file naming no statistics of ours is not a file a read can use: it resolves it, finds
    // nothing in it, and the table is left with none - having replaced what it had. Storing one
    // is worse than storing nothing at all
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_nothing_gathered");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'x'), (100, 'y')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertTrue("the gather stored what it read", hasColStatsForCurrentSnapshot(identifier));

    // a gather naming no partition of a table that keeps its statistics per partition: every
    // group is passed over, and what would be stored describes nothing
    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);
    ColumnStatisticsDesc namesNoPartition =
        new ColumnStatisticsDesc(false, "default", identifier.name());
    ColumnStatistics nothing = new ColumnStatistics(namesNoPartition,
        List.of(new ColumnStatisticsObj("id", "bigint",
            org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.longStats(
                new LongColumnStatsData(0L, 2L)))));

    Assert.assertFalse("nothing was stored",
        storageHandler().setColStatistics(hmsTable, List.of(nothing).iterator()));
    Assert.assertTrue("and what was stored before it still is",
        hasColStatsForCurrentSnapshot(identifier));
  }

  @Test
  public void testEmptyWriteWithoutStoredColStatsPersistsNothing() {
    // the same insert onto a table that carries no statistics: an increment gathered over no rows
    // describes nothing, so it must not become the table's statistics either
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_empty_write_unanalyzed");
    TableIdentifier source = TableIdentifier.of("default", "orders_empty_write_src");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, false);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("CREATE EXTERNAL TABLE " + source + " (id bigint) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1), (5)");

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("INSERT INTO " + identifier + " SELECT id FROM " + source);

    Assert.assertTrue("a write over no rows must not publish statistics",
        readCurrentColStats(identifier).isEmpty());
    Assert.assertFalse(colStatsAccurate(identifier));
  }

  @Test
  public void testCarriedPartitionColStatsAreAnchoredByWhetherTheyStillHold() {
    // ANALYZE full table -> DML on two partitions -> ANALYZE one of them. The partition the DML
    // never touched is carried into the new file; the one it reached is not, having stopped
    // describing itself.
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_anchor");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (7, 'b'), (3, 'c')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    Table icebergTable = testTables.loadTable(identifier);

    // one write reaching p=a and p=b, leaving p=c alone
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (9, 'a'), (9, 'b')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " PARTITION (p='a') COMPUTE STATISTICS FOR COLUMNS");

    icebergTable.refresh();
    Assert.assertEquals("the write reached p=b, so what was stored for it is not carried",
        ImmutableSet.of("p=a", "p=c"), colStatsPartitions(icebergTable));
    Assert.assertEquals("everything carried is described by the file being written",
        icebergTable.currentSnapshot().snapshotId(), currentColStatsFile(icebergTable).snapshotId());
    Assert.assertEquals("and each of them still describes its partition",
        ImmutableMap.of("p=a", true, "p=c", true), upToDateByPartition(icebergTable));
  }

  @Test
  public void testEvolvedFromUnpartitionedDescribesTheSyntheticPartition() throws Exception {
    // rows written before the table was partitioned belong to a partition of their own, and both
    // the list a scan prunes to and the statistics stored have to name it
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_evo_void");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (100, 'b')");
    shell.executeStatement("ALTER TABLE " + identifier + " SET PARTITION SPEC (p)");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (5, 'a')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    Table icebergTable = testTables.loadTable(identifier);
    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);

    // the pruning path asks for every spec, which is how a scan reaches the legacy rows
    List<String> pruned = storageHandler().getPartitions(hmsTable).stream()
        .map(org.apache.hadoop.hive.ql.metadata.Partition::getName)
        .toList();
    Assert.assertTrue("the pruned list has to name the legacy rows' partition: " + pruned,
        pruned.contains(DummyPartition.VOID));
    Assert.assertTrue("and the statistics have to describe it: " + colStatsPartitions(icebergTable),
        colStatsPartitions(icebergTable).contains(DummyPartition.VOID));

    // a write to one partition and an ANALYZE naming only that one: the rest are carried
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (7, 'a')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " PARTITION (p='a') COMPUTE STATISTICS FOR COLUMNS");
    icebergTable.refresh();

    Assert.assertTrue("the legacy rows' partition has to survive an ANALYZE that never named it: " +
            colStatsPartitions(icebergTable),
        colStatsPartitions(icebergTable).contains(DummyPartition.VOID));
  }

  @Test
  public void testPartitionColStatsSurviveAWriteToAnotherPartition() {
    // a write reaches one partition, and the ones it never touched still describe themselves
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_untouched");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (7, 'b'), (3, 'c')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(ImmutableMap.of("p=a", true, "p=b", true, "p=c", true),
        upToDateByPartition(icebergTable));

    shell.executeStatement("DELETE FROM " + identifier + " WHERE p = 'a'");
    icebergTable.refresh();
    Assert.assertEquals("only the partition the delete reached stops describing itself",
        ImmutableMap.of("p=a", false, "p=b", true, "p=c", true), upToDateByPartition(icebergTable));
  }

  @Test
  public void testPartitionColStatsGoStaleOnEveryFormatVersion() {
    // a version 1 table numbers every snapshot 0, so sequence numbers cannot order them; where
    // they sit on the ancestry can, and that is what every version is read by
    assumeParquetHiveCatalogIceberg();
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);

    for (int version = 1; version <= 3; version++) {
      TableIdentifier identifier = TableIdentifier.of("default", "orders_format_v" + version);
      shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
          "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET " +
          "TBLPROPERTIES ('format-version'='" + version + "')");
      shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (7, 'b'), (3, 'c')");
      shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

      Table icebergTable = testTables.loadTable(identifier);
      if (version == 1) {
        Assert.assertEquals("every snapshot of a version 1 table shares one sequence number",
            0, icebergTable.currentSnapshot().sequenceNumber());
      }
      Assert.assertEquals("version " + version + ": what was just computed describes the table",
          ImmutableMap.of("p=a", true, "p=b", true, "p=c", true), upToDateByPartition(icebergTable));

      shell.executeStatement("INSERT INTO " + identifier + " VALUES (9, 'b')");
      icebergTable.refresh();
      Assert.assertEquals("version " + version + ": only the partition the insert reached stops " +
              "describing itself",
          ImmutableMap.of("p=a", true, "p=b", false, "p=c", true), upToDateByPartition(icebergTable));
    }
  }

  @Test
  public void testAggrColStatsCountsOnlyPartitionsCarryingEveryColumnAsked() throws Exception {
    // a partition described for only some of the columns asked about does not count as found
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_added_column");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (7, 'b')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    List<String> partNames = ImmutableList.of("p=a", "p=b");
    Assert.assertEquals("both partitions carry the column asked about", 2,
        storageHandler().getAggrColStatsFor(hmsTable(identifier), ImmutableList.of("id"), partNames)
            .getPartsFound());

    // a column added after the statistics were stored is described by no entry of theirs
    shell.executeStatement("ALTER TABLE " + identifier + " ADD COLUMNS (amount bigint)");
    AggrStats aggrStats = storageHandler().getAggrColStatsFor(
        hmsTable(identifier), ImmutableList.of("id", "amount"), partNames);
    Assert.assertEquals("a partition missing one of the columns asked about is not counted",
        0, aggrStats.getPartsFound());
  }

  @Test
  public void aWholeTableReadTakesNoPerPartitionFile() throws Exception {
    // statistics are served at the granularity the session keeps them at. A file holding
    // partitions states them, and what it folds from them states the table only while it holds
    // every one - so a whole-table read passes it by rather than answer from part of a table
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_two_granularities");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL, false);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (7, 'b')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertFalse("whole-table numbers were stored and nothing has happened since",
        storageHandler().getColStatistics(hmsTable(identifier), ImmutableList.of("id")).isEmpty());

    // the same table gathered per partition: the file at the current snapshot holds partitions
    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL, true);
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (9, 'c')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL, false);
    Assert.assertTrue("a whole-table read is not answered from the partitions of a later gather",
        storageHandler().getColStatistics(hmsTable(identifier), ImmutableList.of("id")).isEmpty());

    // and the partitions still answer for themselves, at the granularity they were kept at
    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL, true);
    AggrStats aggrStats = storageHandler().getAggrColStatsFor(hmsTable(identifier),
        ImmutableList.of("id"), ImmutableList.of("p=a", "p=b", "p=c"));
    Assert.assertEquals("every partition the ask names", 3, aggrStats.getPartsFound());
    LongColumnStatsData stats = aggrStats.getColStats().getFirst().getStatsData().getLongStats();
    Assert.assertEquals("the least value of every partition", 1L, stats.getLowValue());
    Assert.assertEquals("and the greatest", 9L, stats.getHighValue());
  }

  @Test
  public void testTheFoldLeavesOutAColumnAPartitionDidNotState() throws Exception {
    // a rename moves no snapshot, so the partitions this gather did not write stay named as they
    // were. Folding what they hold under the new name would state the table from one partition,
    // so the fold leaves such a column out and the whole-table question is declined
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_folded_rename");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, val bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 100, 'a'), (7, 7, 'b')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    List<String> everyPartition = ImmutableList.of("p=a", "p=b");
    Assert.assertEquals("every partition stated the column, so the fold states it", 2,
        storageHandler().getAggrColStatsFor(hmsTable(identifier), ImmutableList.of("val"),
            everyPartition).getPartsFound());

    shell.executeStatement("ALTER TABLE " + identifier + " CHANGE COLUMN val val2 bigint");
    shell.executeStatement("ANALYZE TABLE " + identifier + " PARTITION (p = 'b') COMPUTE STATISTICS FOR COLUMNS");

    Assert.assertEquals("only the partition just written states the new name, so the fold leaves it out",
        1, storageHandler().getAggrColStatsFor(hmsTable(identifier), ImmutableList.of("val2"),
            everyPartition).getPartsFound());
    Assert.assertEquals("a column every partition still states is folded as before", 2,
        storageHandler().getAggrColStatsFor(hmsTable(identifier), ImmutableList.of("id"),
            everyPartition).getPartsFound());
  }

  @Test
  public void theFoldDoesNotAnswerForAPartitionItNeverDescribed() throws Exception {
    // a partition written since the gather is not in the fold, so a question covering it is
    // declined: answering from what leaves it out would be exact and wrong
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_fold_short_of_the_ask");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (7, 'b')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);
    Assert.assertEquals("the fold answers for the two partitions it was written from", 2,
        storageHandler().getAggrColStatsFor(hmsTable, ImmutableList.of("id"),
            ImmutableList.of("p=a", "p=b")).getPartsFound());

    // a partition the gather never saw, and no ANALYZE since
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_COL_AUTOGATHER.varname, false);
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (9, 'c')");

    AggrStats aggrStats = storageHandler().getAggrColStatsFor(
        hmsTable(identifier), ImmutableList.of("id"), ImmutableList.of("p=a", "p=b", "p=c"));
    Assert.assertNotEquals("the fold covers two of the three asked about, so it cannot answer for all",
        3, aggrStats.getPartsFound());
  }

  @Test
  public void aPartitionScopedGatherWithNothingToCarryStatesNoTable() {
    // it measured one partition and had no stored file to carry the others from, so the file holds
    // that partition alone. A fold of it would read as the table's, and answer for rows it never saw
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_partial_first_gather");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, false);
    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (900, 'b')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " PARTITION (p='b') COMPUTE STATISTICS FOR COLUMNS");

    // read at the granularity the table is configured for by default
    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL, false);
    Assert.assertTrue("the partition it measured does not state the table",
        storageHandler().getColStatistics(hmsTable(identifier), ImmutableList.of("id")).isEmpty());
  }

  @Test
  public void aStalePartitionIsNotAnsweredForFromWhatWasFoldedOverIt() throws Exception {
    // the fold held every partition when it was written; a write since leaves one no longer
    // describing itself, and what was folded from it cannot be taken back out
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_fold_gone_stale");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, false);
    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1,'a'), (2,'b')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (5000,'b')");

    AggrStats aggrStats = storageHandler().getAggrColStatsFor(
        hmsTable(identifier), ImmutableList.of("id"), ImmutableList.of("p=a", "p=b"));
    Assert.assertEquals("the partition the write left alone answers for itself",
        1, aggrStats.getPartsFound());
    Assert.assertNotEquals("and the fold does not answer for the one that went stale under it",
        2, aggrStats.getPartsFound());
  }

  @Test
  public void testRowPreservingCommitsDoNotSpendTheSnapshotLookback() {
    // the lookback bounds manifest reads, and a commit that moves no rows costs none of it
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_rewritten");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (7, 'b'), (3, 'c')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    HiveConf.setIntVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_ICEBERG_STATS_MAX_SNAPSHOT_LOOKBACK, 1);
    Table icebergTable = testTables.loadTable(identifier);
    for (int rewrite = 0; rewrite < 3; rewrite++) {
      icebergTable.rewriteManifests().clusterBy(file -> "all").commit();
    }
    icebergTable.refresh();
    Assert.assertEquals(DataOperations.REPLACE, icebergTable.currentSnapshot().operation());
    Assert.assertEquals("more rewrites than the lookback allows still leave the statistics placed",
        ImmutableMap.of("p=a", true, "p=b", true, "p=c", true), upToDateByPartition(icebergTable));
  }

  @Test
  public void testUpdateStalesThePartitionItLeftAndTheOneItReached() {
    // moving a row between partitions deletes from one and writes to the other, and afterwards
    // neither of them describes itself
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_moved");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('format-version'='2')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (7, 'b'), (3, 'c')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(ImmutableMap.of("p=a", true, "p=b", true, "p=c", true),
        upToDateByPartition(icebergTable));

    shell.executeStatement("UPDATE " + identifier + " SET p = 'b' WHERE id = 1");
    icebergTable.refresh();
    Assert.assertEquals("the partition it took the row from and the one it put it in both changed",
        ImmutableMap.of("p=a", false, "p=b", false, "p=c", true), upToDateByPartition(icebergTable));
  }

  @Test
  public void testCopyOnWriteDeleteStalesOnlyTheRewrittenPartition() {
    // a copy-on-write delete rewrites the files of the partition it touches rather than adding
    // delete files, so the walk has to read what the snapshot replaced, not only what it removed
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_cow");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET " +
        "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='copy-on-write')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (2, 'a'), (7, 'b'), (3, 'c')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    shell.executeStatement("DELETE FROM " + identifier + " WHERE id = 1");
    Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals("only the partition whose files were rewritten stops describing itself",
        ImmutableMap.of("p=a", false, "p=b", true, "p=c", true), upToDateByPartition(icebergTable));
  }

  /** The statistics file a read of the table serves. */
  private StatisticsFile currentColStatsFile(Table icebergTable) {
    return IcebergStoredStats.findColStatsFile(
        icebergTable, icebergTable.currentSnapshot().snapshotId(), shell.getHiveConf());
  }

  /**
   * The partitions the current statistics file describes, whether or not they still hold: named
   * in its footer, as a read resolves them.
   */
  private Set<String> colStatsPartitions(Table icebergTable) {
    StatisticsFile statsFile = currentColStatsFile(icebergTable);
    try (PuffinReader reader =
        Puffin.read(icebergTable.io().newInputFile(statsFile.path()))
            .withFileSize(statsFile.fileSizeInBytes())
            .withFooterSize(statsFile.fileFooterSizeInBytes())
            .build()) {
      return reader.fileMetadata().blobs().stream()
          .map(metadata -> metadata.properties().get(IcebergColStatsWriter.PARTITION_FIELD))
          .filter(Objects::nonNull)
          .collect(Collectors.toSet());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Whether each stored partition's statistics still describe it at the current snapshot. */
  private Map<String, Boolean> upToDateByPartition(Table icebergTable) {
    StatisticsFile statsFile = currentColStatsFile(icebergTable);
    Predicate<String> upToDate = IcebergStoredStats.upToDateColStats(
        icebergTable, icebergTable.currentSnapshot(), statsFile, shell.getHiveConf(), true);
    return colStatsPartitions(icebergTable).stream()
        .collect(Collectors.toMap(partition -> partition, upToDate::test));
  }

  private boolean colStatsAccurate(TableIdentifier identifier) {
    return StatsSetupConst.areColumnStatsUptoDate(hmsTable(identifier).getParameters(), "id");
  }

  /** The granularity this table's statistics are written at, which is what a read asks back for. */
  private boolean partitionLevel(Table icebergTable) {
    return IcebergTableUtil.isPartitionStats(icebergTable, shell.getHiveConf());
  }

  private List<ColumnStatistics> readCurrentColStats(TableIdentifier identifier) {
    Table icebergTable = testTables.loadTable(identifier);
    return readColStats(icebergTable, icebergTable.currentSnapshot().snapshotId());
  }

  /** The stored entries at the table's granularity, one per partition or one for the table. */
  private List<ColumnStatistics> readColStats(Table icebergTable, long snapshotId) {
    if (!partitionLevel(icebergTable)) {
      List<ColumnStatisticsObj> statsObjs = IcebergColStatsReader.read(icebergTable, snapshotId, null, true);
      if (statsObjs.isEmpty()) {
        return List.of();
      }
      ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, "default", icebergTable.name());
      return List.of(new ColumnStatistics(statsDesc, statsObjs));
    }
    StatisticsFile statsFile = IcebergStoredStats.findColStatsFile(icebergTable, snapshotId, true);
    if (statsFile == null) {
      return List.of();
    }
    return IcebergColStatsReader.readPart(icebergTable, statsFile, null, null, true).entrySet().stream()
        .map(entry -> {
          ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(false, "default", icebergTable.name());
          statsDesc.setPartName(entry.getKey());
          return new ColumnStatistics(statsDesc, entry.getValue());
        }).toList();
  }

  private boolean hasColStatsForCurrentSnapshot(TableIdentifier identifier) {
    Table icebergTable = testTables.loadTable(identifier);
    long snapshotId = icebergTable.currentSnapshot().snapshotId();
    return icebergTable.statisticsFiles().stream().anyMatch(statsFile -> statsFile.snapshotId() == snapshotId);
  }

  @Test
  public void testIncrementalColStatsAfterTruncate() {
    // truncate empties the table, so the next increment is the whole table and re-anchors the chain
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_truncated");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint) STORED BY ICEBERG " +
        "STORED AS PARQUET TBLPROPERTIES ('external.table.purge'='true')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (0), (1), (2)");
    Assert.assertTrue(hasColStatsForCurrentSnapshot(identifier));
    checkColStatMinMaxValue(identifier.name(), "id", 0, 2);

    shell.executeStatement("TRUNCATE TABLE " + identifier);
    // the ancestor walk stops at the empty snapshot: nothing is served and no stats file is read
    Assert.assertTrue(readCurrentColStats(identifier).isEmpty());

    shell.executeStatement("INSERT INTO " + identifier + " VALUES (10), (11), (12)");
    Assert.assertTrue(hasColStatsForCurrentSnapshot(identifier));
    checkColStatMinMaxValue(identifier.name(), "id", 10, 12);
  }

  @Test
  public void testAColumnAddedBackDoesNotInheritWhatItsNamesakeStated() {
    // dropping a column and adding one of the same name makes a new field. What is stored for the
    // field it replaced describes rows this column never held, and a merge that matched the two by
    // name would state them as this column's own
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_readded");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, amount bigint) " +
        "STORED BY ICEBERG STORED AS PARQUET TBLPROPERTIES ('external.table.purge'='true')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 100), (2, 200)");
    checkColStatMinMaxValue(identifier.name(), "amount", 100, 200);

    shell.executeStatement("ALTER TABLE " + identifier + " REPLACE COLUMNS (id bigint)");
    shell.executeStatement("ALTER TABLE " + identifier + " ADD COLUMNS (amount bigint)");
    // an increment: what it gathers of the new field covers the inserted row alone
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (3, 5)");

    boolean statesAmount = readCurrentColStats(identifier).stream()
        .flatMap(colStats -> colStats.getStatsObj().stream())
        .anyMatch(statsObj -> "amount".equals(statsObj.getColName()));
    Assert.assertFalse("a column added back states nothing until it is gathered whole", statesAmount);
    // the column that stayed keeps its own, merged as any increment is
    checkColStatMinMaxValue(identifier.name(), "id", 1, 3);
  }

  @Test
  public void testTheTableMetadataRegistersOnePartitionEntryAndItNamesEveryField() {
    // a partition's statistics are addressed through the file's own footer: registering an entry
    // per partition would write that footer into the table metadata again, once per partition,
    // on every commit. One partition entry names every field the file states and marks the
    // granularity
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_registered");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (2, 'b'), (7, 'c')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    Table icebergTable = testTables.loadTable(identifier);
    List<org.apache.iceberg.BlobMetadata> registered =
        currentColStatsFile(icebergTable).blobMetadata();
    Assert.assertEquals("one entry stands for the partitions", 1, registered.stream()
        .filter(blob -> blob.properties().containsKey(IcebergColStatsWriter.PARTITION_FIELD))
        .count());
    Assert.assertFalse("and it names the fields", registered.stream()
        .filter(blob -> blob.properties().containsKey(IcebergColStatsWriter.PARTITION_FIELD))
        .findFirst().orElseThrow().fields().isEmpty());
    // the footer still names every partition, and both reads still answer
    Assert.assertEquals(Set.of("p=a", "p=b", "p=c"), colStatsPartitions(icebergTable));
    checkColStatMinMaxValue(identifier.name(), "id", 1, 7);
    Assert.assertEquals(1L, colStatsObj(readCurrentColStats(identifier), "p=a", "id")
        .getStatsData().getLongStats().getLowValue());
  }

  @Test
  public void testAnAggregateSeededFromTheStoredOneMatchesAggregatingEveryPartition() {
    // a write that replaced nothing stored leaves the stored aggregate answering for all it
    // carries: seeding from it and aggregating every partition anew must reach the same
    // table-level statistics, distinct counts included
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_seeded");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (2, 'a'), (7, 'b'), (9, 'b')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    // overwriting a partition the file does not hold carries every stored one unchanged
    shell.executeStatement("INSERT OVERWRITE TABLE " + identifier + " VALUES (100, 'c')");

    Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(Set.of("p=a", "p=b", "p=c"), colStatsPartitions(icebergTable));
    // the aggregate answers whole-table asks, and its distinct count is the union of every
    // partition's
    checkColStatMinMaxValue(identifier.name(), "id", 1, 100);
    int idField = icebergTable.schema().findField("id").fieldId();
    String ndv = currentColStatsFile(icebergTable).blobMetadata().stream()
        .filter(blob -> IcebergColStatsWriter.HIVE_COL_STATS_BLOB_V1.equals(blob.type()) &&
            blob.fields().equals(List.of(idField)))
        .findFirst().orElseThrow()
        .properties().get("ndv");
    Assert.assertEquals("5", ndv);
  }

  @Test
  public void testColStatsAfterPartitionTruncate() {
    // a partition truncate clears the accuracy flag like any DML: the pre-truncate file keeps
    // serving (the pruner never requests the wiped partition) and ANALYZE recomputes exactly
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_part_truncated");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'a'), (2, 'a'), (7, 'b')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertTrue(colStatsAccurate(identifier));

    shell.executeStatement("TRUNCATE TABLE " + identifier + " PARTITION (p = 'a')");
    Assert.assertFalse(colStatsAccurate(identifier));
    // the emptied partition describes rows it no longer holds, and the one beside it is untouched
    Assert.assertEquals(ImmutableMap.of("p=a", false, "p=b", true),
        upToDateByPartition(testTables.loadTable(identifier)));

    shell.executeStatement("INSERT INTO " + identifier + " VALUES (5, 'a')");
    Assert.assertFalse(colStatsAccurate(identifier));

    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertTrue(colStatsAccurate(identifier));
    List<ColumnStatistics> colStats = readCurrentColStats(identifier);
    ColumnStatisticsObj idA = colStatsObj(colStats, "p=a", "id");
    Assert.assertEquals(5L, idA.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(5L, idA.getStatsData().getLongStats().getHighValue());
    ColumnStatisticsObj idB = colStatsObj(colStats, "p=b", "id");
    Assert.assertEquals(7L, idB.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(7L, idB.getStatsData().getLongStats().getHighValue());
  }

  @Test
  public void testVoidTransformEvolutionUnifiesPartitionNames() {
    // a V1 removal keeps the field as a void transform: a legacy row with a null value and the
    // new-spec rows project to the same unified partition tuple, so their statistics merge under
    // one name - exactly as Iceberg's own partition statistics unify the rows
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_ambiguous");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, a string, b string) " +
        "PARTITIONED BY SPEC (a, b) STORED BY ICEBERG STORED AS PARQUET " +
        "TBLPROPERTIES ('format-version'='1')");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'x', NULL)");
    shell.executeStatement("ALTER TABLE " + identifier + " SET PARTITION SPEC (a)");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (2, 'x', 'whatever')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    Assert.assertTrue(hasColStatsForCurrentSnapshot(identifier));
    List<ColumnStatistics> colStats = readCurrentColStats(identifier);
    ColumnStatisticsObj id = colStatsObj(colStats, "a=x/b=" + NULL_PART, "id");
    Assert.assertEquals(1L, id.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(2L, id.getStatsData().getLongStats().getHighValue());
  }

  @Test
  public void testAggrColStatsAfterBucketAndYearEvolutionsFromUnpartitioned() throws Exception {
    // unpartitioned history plus two partitioned specs - different bucket widths and a year
    // transform - with null and empty-string source values scattered across all three: one ANALYZE
    // pass groups every row under the spec that wrote it and names the blobs like the read side
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_evolved");
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, a string, b date) " +
        "STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (0, 'x', date '2023-03-04'), " +
        "(1, '', NULL), (2, NULL, date '2024-06-01')");
    shell.executeStatement("ALTER TABLE " + identifier + " SET PARTITION SPEC (bucket(8, a))");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (3, 'x', date '2023-05-05'), " +
        "(4, '', date '2023-06-06'), (5, NULL, NULL)");
    shell.executeStatement("ALTER TABLE " + identifier + " SET PARTITION SPEC (bucket(4, a), year(b))");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (6, 'x', date '2023-07-07'), " +
        "(7, '', date '2024-08-08'), (8, NULL, NULL)");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    // the null source values produce null partition values; the empty string hashes to a genuine bucket
    List<String> statNames = Stream.of(
            DummyPartition.VOID,
            "a_bucket_8=" + bucket(8, "x"), "a_bucket_8=" + bucket(8, ""), "a_bucket_8=" + NULL_PART,
            "a_bucket_4=" + bucket(4, "x") + "/b_year=2023",
            "a_bucket_4=" + bucket(4, "") + "/b_year=2024",
            "a_bucket_4=" + NULL_PART + "/b_year=" + NULL_PART)
        .sorted().toList();

    Assert.assertEquals(statNames, colStatsPartNames(identifier));
    // ids 0..2 exist only among the unpartitioned rows, 6..8 only in the latest spec's
    assertAggrColStatsRange(identifier, "id", statNames, 0, 8);
  }

  private static int bucket(int numBuckets, String value) {
    return Transforms.bucket(numBuckets).bind(Types.StringType.get()).apply(value);
  }

  private void createDatePartitionedTable(TableIdentifier identifier) {
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, d date) " +
        "PARTITIONED BY SPEC (year(d)) STORED BY ICEBERG STORED AS PARQUET");
  }

  /** The named column's statistics object within the named partition's blob. */
  private static ColumnStatisticsObj colStatsObj(List<ColumnStatistics> colStats, String partName, String colName) {
    return colStats.stream()
        .filter(stats -> partName.equals(stats.getStatsDesc().getPartName()))
        .flatMap(stats -> stats.getStatsObj().stream())
        .filter(obj -> colName.equals(obj.getColName()))
        .findFirst().orElseThrow();
  }

  private List<String> colStatsPartNames(TableIdentifier identifier) {
    Table icebergTable = testTables.loadTable(identifier);
    List<ColumnStatistics> colStats =
        readColStats(icebergTable, icebergTable.currentSnapshot().snapshotId());
    return colStats.stream().map(stats -> stats.getStatsDesc().getPartName()).sorted().toList();
  }

  /** Asserts a complete aggregation over the given partition names: none missing, min/max spanning. */
  private void assertAggrColStatsRange(TableIdentifier identifier, String column, List<String> statNames,
      long lowValue, long highValue) throws Exception {
    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);
    AggrStats aggrStats = storageHandler().getAggrColStatsFor(hmsTable, ImmutableList.of(column), statNames);
    Assert.assertEquals(statNames.size(), aggrStats.getPartsFound());
    ColumnStatisticsObj statsObj = aggrStats.getColStats().get(0);
    Assert.assertEquals(lowValue, statsObj.getStatsData().getLongStats().getLowValue());
    Assert.assertEquals(highValue, statsObj.getStatsData().getLongStats().getHighValue());
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

  @Test
  public void testPartitionNameRendersAcrossEvolutionsAndTypes() {
    // SELECT PARTITION__NAME renders every row's name under its writing spec - identity with
    // characters partitionToPath escapes, a day transform over timestamps, nulls, and the
    // unpartitioned history - byte-equal to the names the partition listing produces
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_name_render");
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, s string, ts timestamp) " +
        "STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 'x=1/y', timestamp '2023-03-04 10:00:00')");
    shell.executeStatement("ALTER TABLE " + identifier + " SET PARTITION SPEC (s)");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (2, 'a b', timestamp '2023-03-04 11:00:00')");
    shell.executeStatement("ALTER TABLE " + identifier + " SET PARTITION SPEC (day(ts))");
    shell.executeStatement("INSERT INTO " + identifier +
        " VALUES (3, 'c', timestamp '2024-06-01 12:00:00'), (4, NULL, NULL)");
    shell.executeStatement("ALTER TABLE " + identifier + " SET PARTITION SPEC (bucket(4, s))");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (5, 'bucketed', timestamp '2025-01-01 00:00:00')");
    shell.executeStatement("ALTER TABLE " + identifier + " SET PARTITION SPEC (truncate(2, s), month(ts))");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (6, 'trunc-me', timestamp '2025-02-03 04:05:06')");

    List<Object[]> rows = shell.executeStatement(
        "SELECT id, PARTITION__NAME FROM " + identifier + " ORDER BY id");
    // every row renders its own writing spec's name
    Assert.assertEquals(6, rows.size());
    Assert.assertEquals(DummyPartition.VOID, rows.get(0)[1]);
    Assert.assertEquals("s=a+b", rows.get(1)[1]);
    Assert.assertEquals("ts_day=2024-06-01", rows.get(2)[1]);
    Assert.assertEquals("ts_day=" + NULL_PART, rows.get(3)[1]);
    Integer bucket = Transforms.bucket(4).bind(Types.StringType.get()).apply("bucketed");
    Assert.assertEquals("s_bucket_4=" + bucket, rows.get(4)[1]);
    Assert.assertEquals("s_trunc_2=tr/ts_month=2025-02", rows.get(5)[1]);
    Set<String> served = rows.stream().map(r -> String.valueOf(r[1])).collect(Collectors.toSet());

    // every spec's partitions, not only the latest spec's: rows keep their writing spec's name
    Set<String> expected;
    try {
      expected = storageHandler().getPartitions(hmsTable(identifier), Collections.emptyMap(), false).stream()
          .map(Partition::getName)
          .collect(Collectors.toSet());
    } catch (SemanticException e) {
      throw new RuntimeException(e);
    }
    // the legacy unpartitioned rows belong to no partition: the synthetic no-partition name
    expected.add(DummyPartition.VOID);
    Assert.assertEquals(expected, served);

    // the analyzed statistics land under exactly the served names
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    Assert.assertEquals(expected.stream().sorted().toList(), colStatsPartNames(identifier));
  }

  private org.apache.hadoop.hive.ql.metadata.Table hmsTable(TableIdentifier identifier) {
    try {
      return new org.apache.hadoop.hive.ql.metadata.Table(
          shell.metastore().getTable("default", identifier.name()));
    } catch (TException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testMergingTheVectorsCountsWhatThePartitionsHoldTogether() throws Exception {
    // two partitions holding overlapping values: three distinct in one, two in the other, four
    // between them. A read that asks about every partition is answered from what the write
    // folded, and the write held the vectors, so the count is what is actually there whether or
    // not this read would have fetched them. Asking about one partition still reads that
    // partition, where a read without the vectors can only bound the count.
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_merged_ndv");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, true);
    shell.setHiveSessionValue(MetastoreConf.ConfVars.STATS_FETCH_BITVECTOR.getHiveName(), true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier +
        " VALUES (1, 'x'), (2, 'x'), (3, 'x'), (3, 'y'), (4, 'y')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    org.apache.hadoop.hive.ql.metadata.Table hmsTable = hmsTable(identifier);
    List<String> both = ImmutableList.of("p=x", "p=y");
    long merged = ndvOf(storageHandler().getAggrColStatsFor(hmsTable, ImmutableList.of("id"), both));
    Assert.assertEquals("the two hold four values between them", 4L, merged);

    shell.setHiveSessionValue(MetastoreConf.ConfVars.STATS_FETCH_BITVECTOR.getHiveName(), false);
    Assert.assertEquals("and the fold still states four, having merged them where they were held",
        4L, ndvOf(storageHandler().getAggrColStatsFor(hmsTable, ImmutableList.of("id"), both)));

    // one partition is read rather than folded, so without the vectors the count is bounded
    long one = ndvOf(storageHandler().getAggrColStatsFor(
        hmsTable, ImmutableList.of("id"), ImmutableList.of("p=x")));
    Assert.assertEquals("the partition holds three of its own", 3L, one);
  }

  @Test
  public void testATableLevelVectorIsStoredWhateverAReadAsksFor() throws Exception {
    // a table-level vector is not stored for a read but for the next write: an insert merges its
    // own gather into what is stored, and only a vector lets the distinct counts be merged. So
    // the setting that governs fetching one must not govern storing one
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_vector_kept");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, false);
    shell.setHiveSessionValue(MetastoreConf.ConfVars.STATS_FETCH_BITVECTOR.getHiveName(), false);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier +
        " (id bigint) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1), (2), (3)");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    Table table = testTables.loadTable(identifier);
    Assert.assertFalse("the read was told to leave the vector alone",
        IcebergColStatsReader.read(table,
            table.currentSnapshot().snapshotId(), null, false)
            .getFirst().getStatsData().getLongStats().isSetBitVectors());
    Assert.assertTrue("but it was stored, because the next merge needs it",
        IcebergColStatsReader.read(table,
            table.currentSnapshot().snapshotId(), null, true)
            .getFirst().getStatsData().getLongStats().isSetBitVectors());
  }

  @Test
  public void anEstimateOfOnePartitionAnswersForThatPartitionAlone() {
    // What keeping the partitions apart is worth. One partition spans the whole range of id and
    // holds a distinct amount per row; another holds one id and one amount over and over. A scan
    // pruned to the narrow one is estimated from the range it actually covers only when the
    // statistics kept the partitions apart - statistics folded table-wide answer for the range
    // the other partition established, which the scan will never reach.
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_skewed");
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, amount bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET");

    StringBuilder wide = new StringBuilder();
    for (int i = 0; i < WIDE_ROWS; i++) {
      wide.append(i > 0 ? ", " : "").append("(").append(i).append(", ").append(i * 7).append(", 'wide')");
    }
    shell.executeStatement("INSERT INTO " + identifier + " VALUES " + wide);

    StringBuilder narrow = new StringBuilder();
    for (int i = 0; i < NARROW_ROWS; i++) {
      narrow.append(i > 0 ? ", " : "").append("(1, 1, 'narrow')");
    }
    shell.executeStatement("INSERT INTO " + identifier + " VALUES " + narrow);

    // no row of the narrow partition passes this, and no amount of it groups apart
    String filtered = "EXPLAIN SELECT count(*) FROM " + identifier + " WHERE p = 'narrow' AND id > 200";
    String grouped = "EXPLAIN SELECT amount, count(*) FROM " + identifier +
        " WHERE p = 'narrow' GROUP BY amount";

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, true);
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    long filteredApart = rowsAt("Filter Operator", shell.executeAndStringify(filtered));
    long groupedApart = rowsAt("Group By Operator", shell.executeAndStringify(grouped));

    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, false);
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");
    long filteredTogether = rowsAt("Filter Operator", shell.executeAndStringify(filtered));
    long groupedTogether = rowsAt("Group By Operator", shell.executeAndStringify(grouped));

    // the narrow partition holds no id above 200 and one amount throughout
    Assert.assertEquals("filter over the narrow partition, statistics kept per partition", 1, filteredApart);
    Assert.assertEquals("grouping over the narrow partition, statistics kept per partition", 1, groupedApart);
    // folded table-wide, the range and the distinct count the wide partition established answer
    Assert.assertTrue("a table-wide fold should overstate the filter, but said " + filteredTogether,
        filteredTogether >= NARROW_ROWS / 4);
    Assert.assertTrue("a table-wide fold should overstate the grouping, but said " + groupedTogether,
        groupedTogether >= NARROW_ROWS / 4);
  }

  /** Rows the plan estimates for the outermost operator of this kind, which is what it decides on. */
  private static long rowsAt(String operator, String plan) {
    Matcher rows = Pattern.compile(Pattern.quote(operator) + " \\[\\w+\\] \\(rows=(\\d+)").matcher(plan);
    Assert.assertTrue("no " + operator + " in plan:\n" + plan, rows.find());
    return Long.parseLong(rows.group(1));
  }

  @Test
  public void aMergedFileNamesEveryColumnItComesToHold() throws Exception {
    // a statement naming some columns of one partition leaves the rest of the table's standing,
    // so the file holds both - and what names the columns must say so, or a column the carried
    // partitions still describe reads as one with no statistics at all
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_named_altogether");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, true);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, amount bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier + " VALUES (1, 10, 'x'), (2, 20, 'y')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    Table table = testTables.loadTable(identifier);
    Set<Integer> whole = namedFields(table);
    Assert.assertEquals("the whole table was gathered, so every column is named - p among them",
        3, whole.size());

    // now one partition, one column: the other column still stands in the partition carried
    shell.executeStatement("ANALYZE TABLE " + identifier + " PARTITION (p='x') COMPUTE STATISTICS FOR COLUMNS id");

    table = testTables.loadTable(identifier);
    Assert.assertEquals("what the file holds is what it held plus what was read, not the read alone",
        whole, namedFields(table));
  }

  /** The columns a statistics file names, from the one blob that names any. */
  private Set<Integer> namedFields(Table table) {
    StatisticsFile statsFile =
        IcebergStoredStats.findColStatsFile(table, table.currentSnapshot().snapshotId(), true);
    Assert.assertNotNull(statsFile);
    return statsFile.blobMetadata().stream()
        .map(org.apache.iceberg.BlobMetadata::fields)
        .filter(ids -> !ids.equals(List.of(-1)))
        .flatMap(List::stream)
        .collect(java.util.stream.Collectors.toSet());
  }

  @Test
  public void aPartitionVectorIsStoredWhateverTheGatherWasToldAReadWouldWant() throws Exception {
    // the same rule one level down, where it matters more. A table-level vector is wanted by the
    // next write; a partition's is wanted by every read, because a count per partition is only
    // worth having if the counts can be merged across the partitions a scan reads. A gather told
    // that reads would not fetch one must still store it, or the fold is left with the largest
    // single partition's count for good.
    assumeParquetHiveCatalogIceberg();

    TableIdentifier identifier = TableIdentifier.of("default", "orders_part_vector_kept");
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVE_ICEBERG_STATS_COLLECT_PART_LEVEL.varname, true);
    shell.setHiveSessionValue(MetastoreConf.ConfVars.STATS_FETCH_BITVECTOR.getHiveName(), false);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier + " (id bigint, p string) " +
        "PARTITIONED BY SPEC (p) STORED BY ICEBERG STORED AS PARQUET");
    shell.executeStatement("INSERT INTO " + identifier +
        " VALUES (1, 'x'), (2, 'x'), (3, 'x'), (3, 'y'), (4, 'y')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS FOR COLUMNS");

    // the gather was told reads would not want the vectors; a later read that does want them
    // must still find them, and count what the partitions hold together rather than the most
    // any one of them holds
    shell.setHiveSessionValue(MetastoreConf.ConfVars.STATS_FETCH_BITVECTOR.getHiveName(), true);
    long merged = ndvOf(storageHandler().getAggrColStatsFor(
        hmsTable(identifier), ImmutableList.of("id"), ImmutableList.of("p=x", "p=y")));

    Assert.assertEquals("three in one partition and two in the other, four between them", 4L, merged);
  }

  private static long ndvOf(AggrStats stats) {
    Assert.assertEquals(1, stats.getColStatsSize());
    return stats.getColStats().getFirst().getStatsData().getLongStats().getNumDVs();
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
