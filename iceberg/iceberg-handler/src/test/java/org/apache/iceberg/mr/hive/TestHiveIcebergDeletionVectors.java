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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergStorageHandlerTestUtils;
import org.apache.iceberg.mr.hive.writer.RewritableDeletesTestUtils;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.DeleteFileSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

/**
 * Row-level DML on V3 tables: the deletion vectors of the data files.
 */
public class TestHiveIcebergDeletionVectors extends HiveIcebergStorageHandlerWithEngineBase {

  private static final int ROWS = 1000;

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}")
  public static Collection<Object[]> parameters() {
    return getParameters(p -> p.fileFormat() == FileFormat.PARQUET &&
        p.testTableType() == TestTableType.HIVE_CATALOG && p.formatVersion() == 3);
  }

  @Before
  public void splitEveryRowGroup() {
    // one Iceberg split per Parquet row group, and no Tez grouping of those splits back into one task
    shell.setHiveSessionValue(InputFormatConfig.SPLIT_SIZE, "1024");
    shell.setHiveSessionValue("tez.grouping.min-size", "1");
    shell.setHiveSessionValue("tez.grouping.max-size", "1");
  }

  private List<Record> records(long from, long to) {
    List<Record> records = TestHelper.generateRandomRecords(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        (int) (to - from), from);
    for (int i = 0; i < records.size(); i++) {
      records.get(i).setField("customer_id", from + i);
    }
    return records;
  }

  private Table createTable(String name, PartitionSpec spec) {
    return testTables.createTable(shell, name, HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, spec, fileFormat,
        records(0, ROWS), formatVersion, ImmutableMap.of("write.parquet.row-group-size-bytes", "1024"));
  }

  private Table createSingleFileTable(String name) {
    Table table = createTable(name, PartitionSpec.unpartitioned());
    List<FileScanTask> files = planFiles(table);
    Assert.assertEquals("one data file", 1, files.size());
    Assert.assertTrue("multiple row groups", files.get(0).file().splitOffsets().size() > 1);
    return table;
  }

  private static List<FileScanTask> planFiles(Table table) {
    return planFiles(table.newScan());
  }

  private static List<FileScanTask> planFiles(TableScan scan) {
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      return StreamSupport.stream(tasks.spliterator(), false).toList();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<DeleteFile> dvs(List<FileScanTask> tasks) {
    return tasks.stream()
        .flatMap(t -> t.deletes().stream())
        .filter(d -> d.content() == FileContent.POSITION_DELETES && d.format() == FileFormat.PUFFIN)
        .toList();
  }

  /**
   * Returns the data files whose DV Iceberg merged from duplicates at commit time, into a "merged-dvs-*.puffin".
   */
  private static Set<String> assertOneDVPerDataFile(List<FileScanTask> tasks) {
    List<DeleteFile> dvs = dvs(tasks);
    Assert.assertFalse("no DVs written", dvs.isEmpty());
    tasks.forEach(t -> Assert.assertTrue("more than one DV for " + t.file().location(), dvs(List.of(t)).size() <= 1));
    return dvs.stream()
        .filter(dv -> dv.location().contains("merged-dvs-"))
        .map(DeleteFile::referencedDataFile)
        .collect(Collectors.toSet());
  }

  private long count(String sql) {
    return (Long) shell.executeStatement(sql).get(0)[0];
  }

  /**
   * Main and the branch each hold a DV the other lacks: a DML into the branch merges only the branch's.
   */
  @Test
  public void testDmlIntoBranchMergesTheBranchDVs() {
    testTables.createTable(shell, "dv_branch", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, records(0, ROWS), formatVersion);
    shell.executeStatement("ALTER TABLE dv_branch CREATE BRANCH b1");
    shell.executeStatement("CREATE TABLE dv_merge_src STORED BY ICEBERG AS " +
        "SELECT customer_id FROM dv_branch WHERE customer_id % 7 = 0");
    shell.executeStatement("DELETE FROM dv_branch WHERE customer_id % 3 = 0");
    shell.executeStatement("DELETE FROM default.dv_branch.branch_b1 WHERE customer_id % 5 = 0");
    shell.executeStatement("MERGE INTO default.dv_branch.branch_b1 t USING dv_merge_src s " +
        "ON t.customer_id = s.customer_id WHEN MATCHED THEN DELETE");
    long deleted = LongStream.range(0, ROWS).filter(i -> i % 7 == 0 || i % 5 == 0).count();
    Assert.assertEquals(ROWS - deleted, count("SELECT count(*) FROM default.dv_branch.branch_b1"));
  }

  private void createTableWithBranch(String name) {
    testTables.createTable(shell, name, HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, records(0, ROWS), formatVersion);
    shell.executeStatement("ALTER TABLE " + name + " CREATE BRANCH b1");
  }

  /**
   * A DELETE from the branch whose subquery reads main: the DVs of the branch do not take the deletes of main.
   */
  @Test
  public void testDeleteFromBranchWithASubqueryOverMain() {
    createTableWithBranch("dv_branch_main");
    shell.executeStatement("DELETE FROM dv_branch_main WHERE customer_id % 3 = 0");
    shell.executeStatement("DELETE FROM default.dv_branch_main.branch_b1 WHERE customer_id IN " +
        "(SELECT customer_id FROM dv_branch_main WHERE customer_id % 5 = 0)");
    long deleted = LongStream.range(0, ROWS).filter(i -> i % 5 == 0 && i % 3 != 0).count();
    Assert.assertEquals(ROWS - deleted, count("SELECT count(*) FROM default.dv_branch_main.branch_b1"));
  }

  /**
   * A MERGE into main whose source reads the branch: the DVs of main do not take the deletes of the branch.
   */
  @Test
  public void testMergeIntoMainWithASourceOverTheBranch() {
    createTableWithBranch("dv_main_branch");
    shell.executeStatement("DELETE FROM default.dv_main_branch.branch_b1 WHERE customer_id % 3 = 0");
    shell.executeStatement("MERGE INTO dv_main_branch t USING " +
        "(SELECT customer_id FROM default.dv_main_branch.branch_b1 WHERE customer_id % 5 = 0) s " +
        "ON t.customer_id = s.customer_id WHEN MATCHED THEN DELETE");
    long deleted = LongStream.range(0, ROWS).filter(i -> i % 5 == 0 && i % 3 != 0).count();
    Assert.assertEquals(ROWS - deleted, count("SELECT count(*) FROM dv_main_branch"));
  }

  /**
   * A MERGE whose source reads an older snapshot of the target: the writers do not rewrite the DVs of that snapshot.
   */
  @Test
  public void testMergeWithASourceAsOfAnOlderSnapshot() {
    Table table = createSingleFileTable("dv_time_travel");
    shell.executeStatement("DELETE FROM dv_time_travel WHERE customer_id % 3 = 0");
    table.refresh();
    long olderSnapshot = table.currentSnapshot().snapshotId();
    shell.executeStatement("DELETE FROM dv_time_travel WHERE customer_id % 5 = 0");
    shell.executeStatement("MERGE INTO dv_time_travel t USING " +
        "(SELECT customer_id FROM dv_time_travel FOR SYSTEM_VERSION AS OF " + olderSnapshot +
        " WHERE customer_id % 7 = 0) s ON t.customer_id = s.customer_id " +
        "WHEN MATCHED THEN UPDATE SET first_name = 'x'");
    Assert.assertEquals(ROWS - LongStream.range(0, ROWS).filter(i -> i % 3 == 0 || i % 5 == 0).count(),
        count("SELECT count(*) FROM dv_time_travel"));
    Assert.assertEquals(LongStream.range(0, ROWS).filter(i -> i % 7 == 0 && i % 3 != 0 && i % 5 != 0).count(),
        count("SELECT count(*) FROM dv_time_travel WHERE first_name = 'x'"));
    assertOneDVPerDataFileAndNoMerge(table);
  }


  private static void assertOneDVPerDataFileAndNoMerge(Table table) {
    table.refresh();
    Assert.assertEquals("DVs merged at commit", Set.of(), assertOneDVPerDataFile(planFiles(table)));
  }

  private long rowsAffected(String sql) throws HiveSQLException {
    HiveSession session = shell.getSession();
    OperationHandle handle = session.executeStatement(sql, Map.of());
    try {
      return session.getSessionManager().getOperationManager().getOperation(handle).getNumModifiedRows();
    } finally {
      session.closeOperation(handle);
    }
  }

  @Test
  public void testDeleteOfSplitDataFile() {
    Table table = createSingleFileTable("dv_delete");
    shell.executeStatement("DELETE FROM dv_delete WHERE customer_id % 7 = 0");
    Assert.assertEquals(ROWS - (ROWS + 6) / 7, count("SELECT count(*) FROM dv_delete"));
    assertOneDVPerDataFileAndNoMerge(table);
  }

  @Test
  public void testDeleteOfSplitDataFilesInPartitionedTable() {
    // several writer tasks
    shell.setHiveSessionValue("mapreduce.job.reduces", "3");
    shell.setHiveSessionValue("hive.tez.auto.reducer.parallelism", "false");
    Table table = createTable("dv_delete_part",
        PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA).bucket("customer_id", 3).build());
    Assert.assertTrue("split data files", planFiles(table).stream()
        .allMatch(t -> t.file().splitOffsets().size() > 1));
    shell.executeStatement("DELETE FROM dv_delete_part WHERE customer_id % 7 = 0");
    Assert.assertEquals(ROWS - (ROWS + 6) / 7, count("SELECT count(*) FROM dv_delete_part"));
    assertOneDVPerDataFileAndNoMerge(table);
  }

  @Test
  public void testSecondDeleteOverExistingDVs() {
    Table table = createSingleFileTable("dv_delete2");
    shell.executeStatement("DELETE FROM dv_delete2 WHERE customer_id % 7 = 0");
    shell.executeStatement("DELETE FROM dv_delete2 WHERE customer_id % 5 = 0");
    long deleted = LongStream.range(0, ROWS).filter(i -> i % 7 == 0 || i % 5 == 0).count();
    Assert.assertEquals(ROWS - deleted, count("SELECT count(*) FROM dv_delete2"));
    table.refresh();
    Assert.assertEquals("one live DV holding every deleted position", deleted,
        dvs(planFiles(table)).stream().mapToLong(DeleteFile::recordCount).sum());
    assertOneDVPerDataFileAndNoMerge(table);
  }

  /**
   * A dir of the rewritable deletes named in the session is not used, by DMLs nor by reads.
   */
  @Test
  public void testRewritableDeletesKeyOfTheSessionIsIgnored() {
    Table table = createSingleFileTable("dv_session");
    shell.setHiveSessionValue("iceberg.mr.rewritable.deletes.default.dv_session", "file:/session-value");
    // without previous deletes, then over the DVs of the first DELETE
    shell.executeStatement("DELETE FROM dv_session WHERE customer_id % 7 = 0");
    shell.executeStatement("DELETE FROM dv_session WHERE customer_id % 5 = 0");
    long deleted = LongStream.range(0, ROWS).filter(i -> i % 7 == 0 || i % 5 == 0).count();
    Assert.assertEquals(ROWS - deleted, count("SELECT count(*) FROM dv_session"));
    assertOneDVPerDataFileAndNoMerge(table);
  }

  @Test
  public void testUpdateOfSplitDataFile() {
    Table table = createSingleFileTable("dv_update");
    shell.executeStatement("UPDATE dv_update SET first_name = 'x' WHERE customer_id % 7 = 0");
    Assert.assertEquals((ROWS + 6) / 7, count("SELECT count(*) FROM dv_update WHERE first_name = 'x'"));
    assertOneDVPerDataFileAndNoMerge(table);
  }

  private void createMergeSource(String select) {
    shell.executeStatement("CREATE TABLE dv_merge_src STORED BY ICEBERG AS " + select);
  }

  /**
   * Both clauses hit the same data file, their deletes go through one delete branch.
   */
  private void assertMerge(String merge, long updated, long deleted, long inserted) throws HiveSQLException {
    Table table = createSingleFileTable("dv_merge");
    createMergeSource("SELECT customer_id FROM dv_merge WHERE customer_id % 7 = 0 UNION ALL " +
        "SELECT customer_id + " + ROWS + " FROM dv_merge WHERE customer_id < " + inserted);
    Assert.assertEquals("rows affected", updated + deleted + inserted, rowsAffected(merge));
    Assert.assertEquals(ROWS - deleted + inserted, count("SELECT count(*) FROM dv_merge"));
    Assert.assertEquals(updated, count("SELECT count(*) FROM dv_merge WHERE first_name = 'x'"));
    assertOneDVPerDataFileAndNoMerge(table);
  }

  @Test
  public void testMergeUpdateAndDeleteOfSameDataFile() throws HiveSQLException {
    assertMerge("MERGE INTO dv_merge t USING dv_merge_src s ON t.customer_id = s.customer_id " +
        "WHEN MATCHED AND t.customer_id % 2 = 0 THEN UPDATE SET first_name = 'x' " +
        "WHEN MATCHED THEN DELETE",
        LongStream.range(0, ROWS).filter(i -> i % 7 == 0 && i % 2 == 0).count(),
        LongStream.range(0, ROWS).filter(i -> i % 7 == 0 && i % 2 != 0).count(), 0);
  }

  @Test
  public void testMergeDeleteBeforeUpdateOfSameDataFile() throws HiveSQLException {
    assertMerge("MERGE INTO dv_merge t USING dv_merge_src s ON t.customer_id = s.customer_id " +
        "WHEN MATCHED AND t.customer_id % 2 = 0 THEN DELETE " +
        "WHEN MATCHED THEN UPDATE SET first_name = 'x'",
        LongStream.range(0, ROWS).filter(i -> i % 7 == 0 && i % 2 != 0).count(),
        LongStream.range(0, ROWS).filter(i -> i % 7 == 0 && i % 2 == 0).count(), 0);
  }

  @Test
  public void testMergeUpdateDeleteAndInsert() throws HiveSQLException {
    assertMerge("MERGE INTO dv_merge t USING dv_merge_src s ON t.customer_id = s.customer_id " +
        "WHEN MATCHED AND t.customer_id % 2 = 0 THEN UPDATE SET first_name = 'x' " +
        "WHEN MATCHED THEN DELETE " +
        "WHEN NOT MATCHED THEN INSERT VALUES (s.customer_id, 'y', 'z')",
        LongStream.range(0, ROWS).filter(i -> i % 7 == 0 && i % 2 == 0).count(),
        LongStream.range(0, ROWS).filter(i -> i % 7 == 0 && i % 2 != 0).count(), 10);
  }

  @Test
  public void testMergeClausesOnDisjointDataFiles() {
    // customer ids [0, ROWS) in one file and [ROWS, 2 * ROWS) in another
    Table table = createSingleFileTable("dv_merge");
    shell.executeStatement(testTables.getInsertQuery(records(ROWS, 2 * ROWS),
        TableIdentifier.of("default", "dv_merge"), false));
    createMergeSource("SELECT customer_id FROM dv_merge WHERE customer_id % 7 = 0");
    shell.executeStatement("MERGE INTO dv_merge t USING dv_merge_src s ON t.customer_id = s.customer_id " +
        "WHEN MATCHED AND t.customer_id < " + ROWS + " THEN UPDATE SET first_name = 'x' " +
        "WHEN MATCHED THEN DELETE");
    long deleted = LongStream.range(ROWS, 2 * ROWS).filter(i -> i % 7 == 0).count();
    Assert.assertEquals(2 * ROWS - deleted, count("SELECT count(*) FROM dv_merge"));
    table.refresh();
    Assert.assertEquals("both data files have a DV", 2,
        dvs(planFiles(table)).stream().map(DeleteFile::referencedDataFile).distinct().count());
    assertOneDVPerDataFileAndNoMerge(table);
  }

  private static volatile List<String> scratchDirs;
  private static volatile Set<String> rewritableDeletesBefore;
  private static volatile boolean rewritableDeletesSeen;
  private static volatile Set<String> collectedDataFiles;

  // the files the scans wrote into the dirs of the rewritable deletes, with the deletes each holds
  private static Map<Path, Map<String, DeleteFileSet>> rewritableDeleteFiles() throws IOException {
    Map<Path, Map<String, DeleteFileSet>> files = Maps.newHashMap();
    for (String scratchDir : scratchDirs) {
      files.putAll(RewritableDeletesTestUtils.scanFiles(new Path(scratchDir), new HiveConf()));
    }
    return files;
  }

  private static Set<String> rewritableDeletes() throws IOException {
    return rewritableDeleteFiles().keySet().stream().map(Path::toString).collect(Collectors.toSet());
  }

  /**
   * Called by the scan tasks through java_method: records whether the scans of the query collected rewritable
   * deletes, and the data files they collected them for.
   */
  public static boolean probeRewritableDeletes() throws IOException {
    Map<Path, Map<String, DeleteFileSet>> files = rewritableDeleteFiles();
    files.keySet().removeIf(file -> rewritableDeletesBefore.contains(file.toString()));
    rewritableDeletesSeen = !files.isEmpty();
    collectedDataFiles = files.values().stream()
        .flatMap(deletes -> deletes.keySet().stream())
        .collect(Collectors.toSet());
    return true;
  }

  private void resetProbe() throws IOException {
    // a query in Hadoop local mode uses the local scratch dir
    scratchDirs = List.of(HiveConf.getVar(shell.getHiveConf(), HiveConf.ConfVars.SCRATCH_DIR),
        HiveConf.getVar(shell.getHiveConf(), HiveConf.ConfVars.LOCAL_SCRATCH_DIR));
    rewritableDeletesBefore = rewritableDeletes();
    rewritableDeletesSeen = false;
    collectedDataFiles = null;
  }

  private String probed(String dml) throws IOException {
    resetProbe();
    return dml + " AND java_method('" + TestHiveIcebergDeletionVectors.class.getName() +
        "', 'probeRewritableDeletes') = 'true'";
  }

  // the probe runs in the SET list: in the WHERE clause, a nondeterministic function stops the filter's pushdown
  private String probedUpdate(String tableName, String where) throws IOException {
    resetProbe();
    return "UPDATE " + tableName + " SET first_name = java_method('" + TestHiveIcebergDeletionVectors.class.getName() +
        "', 'probeRewritableDeletes') WHERE " + where;
  }

  private static Set<String> locations(List<FileScanTask> tasks) {
    return tasks.stream().map(t -> t.file().location()).collect(Collectors.toSet());
  }

  @Test
  public void testRewritableDeletesCollectedByTheScan() throws IOException {
    createSingleFileTable("dv_delete");
    shell.executeStatement("DELETE FROM dv_delete WHERE customer_id % 7 = 0");
    String delete = probed("DELETE FROM dv_delete WHERE customer_id % 5 = 0");
    shell.executeStatement("EXPLAIN " + delete);
    Assert.assertEquals("collected by EXPLAIN", rewritableDeletesBefore, rewritableDeletes());
    shell.executeStatement(delete);
    Assert.assertTrue("collected in the temp dir of the query", rewritableDeletesSeen);
  }

  /**
   * A DML that scans one partition collects the previous deletes of that partition's data files only; the deletes of
   * the other partitions remain.
   */
  @Test
  public void testPartitionFilteredDmlOverExistingDVs() throws IOException {
    Table table = createTable("dv_part_filter",
        PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
            .truncate("customer_id", ROWS / 2).build());
    shell.executeStatement("DELETE FROM dv_part_filter WHERE customer_id % 7 = 0");
    table.refresh();
    Set<String> firstPartition =
        locations(planFiles(table.newScan().filter(Expressions.lessThan("customer_id", ROWS / 2))));
    Assert.assertNotEquals(locations(planFiles(table)), firstPartition);

    shell.executeStatement(probedUpdate("dv_part_filter", "customer_id < " + ROWS / 2 + " AND customer_id % 5 = 0"));
    Assert.assertEquals("the deletes of the scanned partition only", firstPartition, collectedDataFiles);
    Assert.assertEquals(ROWS - LongStream.range(0, ROWS).filter(i -> i % 7 == 0).count(),
        count("SELECT count(*) FROM dv_part_filter"));
    Assert.assertEquals(LongStream.range(0, ROWS / 2).filter(i -> i % 5 == 0 && i % 7 != 0).count(),
        count("SELECT count(*) FROM dv_part_filter WHERE first_name = 'true'"));
    assertOneDVPerDataFileAndNoMerge(table);
  }

  /**
   * A DML filtered on a data column collects the previous deletes of the data files its stats-pruned scan reads.
   */
  @Test
  public void testDmlFilteredByDataFileStatsOverExistingDVs() throws IOException {
    // customer ids [0, ROWS) in one file and [ROWS, 2 * ROWS) in another
    Table table = createSingleFileTable("dv_stats_filter");
    shell.executeStatement(testTables.getInsertQuery(records(ROWS, 2 * ROWS),
        TableIdentifier.of("default", "dv_stats_filter"), false));
    shell.executeStatement("DELETE FROM dv_stats_filter WHERE customer_id % 7 = 0");
    table.refresh();
    Set<String> secondFile =
        locations(planFiles(table.newScan().filter(Expressions.greaterThanOrEqual("customer_id", ROWS))));
    Assert.assertEquals(1, secondFile.size());

    shell.executeStatement(probedUpdate("dv_stats_filter", "customer_id >= " + ROWS + " AND customer_id % 5 = 0"));
    Assert.assertEquals(secondFile, collectedDataFiles);
    Assert.assertEquals(2 * ROWS - LongStream.range(0, 2 * ROWS).filter(i -> i % 7 == 0).count(),
        count("SELECT count(*) FROM dv_stats_filter"));
    Assert.assertEquals(LongStream.range(ROWS, 2 * ROWS).filter(i -> i % 5 == 0 && i % 7 != 0).count(),
        count("SELECT count(*) FROM dv_stats_filter WHERE first_name = 'true'"));
    assertOneDVPerDataFileAndNoMerge(table);
  }

  /**
   * A MERGE whose source subquery reads the target table keeps the previous deletes.
   */
  @Test
  public void testMergeWithASubqueryOverTheTarget() {
    Table table = createSingleFileTable("dv_merge_self");
    shell.executeStatement("DELETE FROM dv_merge_self WHERE customer_id % 7 = 0");
    shell.executeStatement("MERGE INTO dv_merge_self t USING " +
        "(SELECT customer_id FROM dv_merge_self WHERE customer_id < 100) s ON t.customer_id = s.customer_id " +
        "WHEN MATCHED AND t.customer_id % 5 = 0 THEN DELETE");
    long deleted = LongStream.range(0, ROWS).filter(i -> i % 7 == 0 || i < 100 && i % 5 == 0).count();
    Assert.assertEquals(ROWS - deleted, count("SELECT count(*) FROM dv_merge_self"));
    assertOneDVPerDataFileAndNoMerge(table);
  }
}
