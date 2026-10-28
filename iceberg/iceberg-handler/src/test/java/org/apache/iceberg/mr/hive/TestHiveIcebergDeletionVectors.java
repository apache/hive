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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergStorageHandlerTestUtils;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
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
      return StreamSupport.stream(tasks.spliterator(), false).collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<DeleteFile> dvs(List<FileScanTask> tasks) {
    return tasks.stream()
        .flatMap(t -> t.deletes().stream())
        .filter(d -> d.content() == FileContent.POSITION_DELETES && d.format() == FileFormat.PUFFIN)
        .collect(Collectors.toList());
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
        .map(dv -> dv.referencedDataFile().toString())
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

  private static volatile boolean rewritableDeletesSeen;

  private static List<String> rewritableDeletes(Path tableLocation, Configuration conf) throws IOException {
    Path temp = new Path(tableLocation, "temp");
    FileSystem fs = temp.getFileSystem(conf);
    if (!fs.exists(temp)) {
      return List.of();
    }
    return Arrays.stream(fs.listStatus(temp)).map(FileStatus::getPath).map(Path::getName)
        .filter(name -> name.endsWith("-rewritable-deletes")).collect(Collectors.toList());
  }

  /**
   * Called by the writing tasks through java_method: records whether the rewritable deletes of the table exist.
   */
  public static boolean probeRewritableDeletes(String dataFile) throws IOException {
    rewritableDeletesSeen = !rewritableDeletes(new Path(dataFile).getParent().getParent(), new HiveConf()).isEmpty();
    return true;
  }

  @Test
  public void testRewritableDeletesLifecycle() throws IOException {
    Table table = createSingleFileTable("dv_delete");
    Path location = new Path(table.location());
    String delete = "DELETE FROM dv_delete WHERE customer_id % 7 = 0 AND " +
        "java_method('" + TestHiveIcebergDeletionVectors.class.getName() + "', 'probeRewritableDeletes', " +
        "FILE__PATH) = 'true'";
    shell.executeStatement("EXPLAIN " + delete);
    Assert.assertEquals("planned by EXPLAIN", List.of(), rewritableDeletes(location, shell.getHiveConf()));
    rewritableDeletesSeen = false;
    shell.executeStatement(delete);
    Assert.assertTrue("planned for the DML", rewritableDeletesSeen);
    Assert.assertEquals("left behind", List.of(), rewritableDeletes(location, shell.getHiveConf()));
  }

  @Test
  public void testFailedMergeLeavesNoRewritableDeletes() throws IOException {
    Table table = createSingleFileTable("dv_merge");
    // duplicate matches fail the MERGE on cardinality
    shell.executeStatement("CREATE TABLE dv_merge_src STORED BY ICEBERG AS " +
        "SELECT customer_id FROM dv_merge WHERE customer_id % 7 = 0 UNION ALL " +
        "SELECT customer_id FROM dv_merge WHERE customer_id % 7 = 0");
    rewritableDeletesSeen = false;
    Assert.assertThrows(IllegalArgumentException.class, () -> shell.executeStatement(
        "MERGE INTO dv_merge t USING dv_merge_src s ON t.customer_id = s.customer_id WHEN MATCHED AND " +
        "java_method('" + TestHiveIcebergDeletionVectors.class.getName() + "', 'probeRewritableDeletes', " +
        "t.FILE__PATH) = 'true' THEN DELETE"));
    Assert.assertTrue("planned for the DML", rewritableDeletesSeen);
    Assert.assertEquals(ROWS, count("SELECT count(*) FROM dv_merge"));
    Assert.assertEquals("left behind", List.of(), rewritableDeletes(new Path(table.location()), shell.getHiveConf()));
  }

  @Test
  public void testDeleteFailingBeforeTheWritersLeavesNoRewritableDeletes() throws IOException {
    Table table = createSingleFileTable("dv_abort");
    rewritableDeletesSeen = false;
    // the scan fails, so no DV writer task commits and the job has no location to list on abort
    Assert.assertThrows(IllegalArgumentException.class, () -> shell.executeStatement(
        "DELETE FROM dv_abort WHERE assert_true(java_method('" + TestHiveIcebergDeletionVectors.class.getName() +
        "', 'probeRewritableDeletes', FILE__PATH) = 'false') IS NULL"));
    Assert.assertTrue("planned for the DML", rewritableDeletesSeen);
    Assert.assertEquals(ROWS, count("SELECT count(*) FROM dv_abort"));
    Assert.assertEquals("left behind", List.of(), rewritableDeletes(new Path(table.location()), shell.getHiveConf()));
  }

  /**
   * A session can set the key of the rewritable deletes: no writer reads that location and no commit removes it.
   */
  @Test
  public void testRewritableDeletesOfTheSessionAreIgnored() throws IOException {
    createSingleFileTable("dv_session");
    testTables.createTable(shell, "dv_session_v2", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, records(0, ROWS), 2);
    File file = temp.newFile();
    long deleted = LongStream.range(0, ROWS).filter(i -> i % 5 == 0).count();
    for (String table : List.of("dv_session", "dv_session_v2")) {
      shell.setHiveSessionValue(InputFormatConfig.REWRITABLE_DELETES_PREFIX + "default." + table,
          file.toURI().toString());
      shell.executeStatement("INSERT INTO " + table + " SELECT * FROM " + table + " WHERE customer_id = 1");
      shell.executeStatement("DELETE FROM " + table + " WHERE customer_id % 5 = 0");
      Assert.assertEquals(ROWS + 1 - deleted, count("SELECT count(*) FROM " + table));
    }
    Assert.assertTrue(file.exists());
  }
}
