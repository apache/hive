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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
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

  @Test
  public void testMergeIntoBranchOverExistingDVs() {
    Table table = createSingleFileTable("dv_branch");
    shell.executeStatement("ALTER TABLE dv_branch CREATE BRANCH b1");
    shell.executeStatement("DELETE FROM default.dv_branch.branch_b1 WHERE customer_id % 5 = 0");
    shell.executeStatement("CREATE TABLE dv_merge_src STORED BY ICEBERG AS " +
        "SELECT customer_id FROM dv_branch WHERE customer_id % 7 = 0");
    shell.executeStatement("MERGE INTO default.dv_branch.branch_b1 t USING dv_merge_src s " +
        "ON t.customer_id = s.customer_id WHEN MATCHED THEN DELETE");
    long deleted = LongStream.range(0, ROWS).filter(i -> i % 7 == 0 || i % 5 == 0).count();
    Assert.assertEquals(ROWS, count("SELECT count(*) FROM dv_branch"));
    Assert.assertEquals(ROWS - deleted, count("SELECT count(*) FROM default.dv_branch.branch_b1"));
    table.refresh();
    List<FileScanTask> tasks = planFiles(table.newScan().useRef("b1"));
    Assert.assertEquals("one live DV holding every deleted position", deleted,
        dvs(tasks).stream().mapToLong(DeleteFile::recordCount).sum());
    assertOneDVPerDataFile(tasks);
  }
}
