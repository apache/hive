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

package org.apache.iceberg.mr.hive.writer;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DeleteFileSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestRewritableDeletes {
  private static final String QUERY_ID = "rewritable_deletes_query";
  private static final String TABLE_NAME = "default.rewritable_deletes";
  private static final Schema SCHEMA = new Schema(optional(1, "id", Types.LongType.get()));

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Table table;
  private DataFile fileA;
  private DataFile fileB;
  private File queryTempDir;
  private JobConf conf;

  @Before
  public void before() throws IOException {
    File dir = temp.newFolder();
    Assert.assertTrue(dir.delete());
    table = new HadoopTables().create(SCHEMA, PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"), dir.getAbsolutePath());
    fileA = dataFile(dir, "a");
    fileB = dataFile(dir, "b");
    table.newFastAppend().appendFile(fileA).appendFile(fileB).commit();
    queryTempDir = temp.newFolder();
    conf = new JobConf();
    conf.set(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "tez");
    conf.set(HiveConf.ConfVars.HIVE_QUERY_ID.varname, QUERY_ID);
    conf.set(DagUtils.TEZ_TMP_DIR_KEY, queryTempDir.getAbsolutePath());
  }

  @After
  public void after() {
    ObjectCacheFactory.removeLlapQueryCache(QUERY_ID);
    LlapProxy.setDaemon(false);
  }

  @Test
  public void testNoCollectorWithoutADVWriter() {
    Assert.assertNull(RewritableDeletes.collector(TABLE_NAME, table.currentSnapshot(), conf));
    Assert.assertEquals(Map.of(), RewritableDeletes.getFromJobConf(TABLE_NAME, table.currentSnapshot(), conf));
  }

  /**
   * Only the deletes of the data files a scan planned are collected.
   */
  @Test
  public void testCollectsTheDeletesOfTheScannedFilesOnly() {
    addDV(fileA);
    addDV(fileB);
    RewritableDeletes.addToJobConf(TABLE_NAME, conf);

    collect(tasks().stream().filter(t -> t.file().location().equals(fileA.location())).toList());

    Assert.assertEquals(Set.of(fileA.location()),
        RewritableDeletes.getFromJobConf(TABLE_NAME, table.currentSnapshot(), conf).keySet());
  }

  /**
   * Each scan of the table writes a file of its own, and the writers get the deletes of all of them.
   */
  @Test
  public void testUnionOfTheScans() throws IOException {
    addDV(fileA);
    addDV(fileB);
    RewritableDeletes.addToJobConf(TABLE_NAME, conf);
    for (FileScanTask task : tasks()) {
      collect(List.of(task));
    }
    Assert.assertEquals(2, RewritableDeletes.scanFiles(new Path(queryTempDir.getAbsolutePath()), conf).size());
    Assert.assertEquals(Set.of(fileA.location(), fileB.location()),
        RewritableDeletes.getFromJobConf(TABLE_NAME, table.currentSnapshot(), conf).keySet());
  }

  @Test
  public void testScanWithoutPreviousDeletes() {
    RewritableDeletes.addToJobConf(TABLE_NAME, conf);
    collect(tasks());
    Assert.assertEquals(Map.of(), RewritableDeletes.getFromJobConf(TABLE_NAME, table.currentSnapshot(), conf));
  }

  /**
   * Reading fails when no scan wrote a file.
   */
  @Test
  public void testWriterFailsWithoutAScanFile() {
    RewritableDeletes.addToJobConf(TABLE_NAME, conf);
    IllegalStateException thrown = Assert.assertThrows(IllegalStateException.class,
        () -> RewritableDeletes.getFromJobConf(TABLE_NAME, table.currentSnapshot(), conf));
    Assert.assertTrue(thrown.getMessage(), thrown.getMessage().startsWith("No scan collected the rewritable deletes"));
  }

  @Test
  public void testLoadedOncePerDaemon() {
    addDV(fileA);
    RewritableDeletes.addToJobConf(TABLE_NAME, conf);
    collect(tasks());

    LlapProxy.setDaemon(true);
    Map<String, DeleteFileSet> deletes = RewritableDeletes.getFromJobConf(TABLE_NAME, table.currentSnapshot(), conf);
    Assert.assertEquals(Set.of(fileA.location()), deletes.keySet());
    // the second writer of the query finds the rewritable deletes in the cache of the daemon
    Assert.assertSame(deletes, RewritableDeletes.getFromJobConf(TABLE_NAME, table.currentSnapshot(), conf));
    // which the writers of the daemon share
    Assert.assertThrows(UnsupportedOperationException.class, () -> deletes.remove(fileA.location()));
  }

  /**
   * A dir named in the session is not used: scans do not collect into it and writers do not read it.
   */
  @Test
  public void testSessionValueIsIgnored() {
    conf.set("iceberg.mr.rewritable.deletes." + TABLE_NAME, "file:/session-value");
    Assert.assertNull(RewritableDeletes.collector(TABLE_NAME, table.currentSnapshot(), conf));
    Assert.assertEquals(Map.of(), RewritableDeletes.getFromJobConf(TABLE_NAME, table.currentSnapshot(), conf));

    RewritableDeletes.addToJobConf(TABLE_NAME, conf);
    collect(tasks());
    Assert.assertEquals(Map.of(), RewritableDeletes.getFromJobConf(TABLE_NAME, table.currentSnapshot(), conf));
  }

  /**
   * The dir is named for the job's query only.
   */
  @Test
  public void testDirOfAnotherQueryIsIgnored() {
    RewritableDeletes.addToJobConf(TABLE_NAME, conf);
    conf.set(HiveConf.ConfVars.HIVE_QUERY_ID.varname, QUERY_ID + "_next");
    Assert.assertNull(RewritableDeletes.collector(TABLE_NAME, table.currentSnapshot(), conf));
  }

  /**
   * The scans of main and of a branch collect into the subdirs of their snapshots: the writers of a snapshot get its
   * deletes only.
   */
  @Test
  public void testScansOfAnotherSnapshotAreNotRead() {
    table.manageSnapshots().createBranch("b1").commit();
    addDV(fileA);
    Snapshot branch = table.snapshot("b1");
    RewritableDeletes.addToJobConf(TABLE_NAME, conf);
    collect(table.currentSnapshot(), tasks());
    collect(branch, tasks(table.newScan().useRef("b1")));

    Assert.assertEquals(Set.of(fileA.location()),
        RewritableDeletes.getFromJobConf(TABLE_NAME, table.currentSnapshot(), conf).keySet());
    Assert.assertEquals(Map.of(), RewritableDeletes.getFromJobConf(TABLE_NAME, branch, conf));
  }

  private void collect(List<FileScanTask> tasks) {
    collect(table.currentSnapshot(), tasks);
  }

  private void collect(Snapshot snapshot, List<FileScanTask> tasks) {
    RewritableDeletes.Collector collector = RewritableDeletes.collector(TABLE_NAME, snapshot, conf);
    tasks.forEach(collector::add);
    collector.write(conf);
  }

  private List<FileScanTask> tasks() {
    return tasks(table.newScan());
  }

  private static List<FileScanTask> tasks(TableScan scan) {
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      return StreamSupport.stream(tasks.spliterator(), false).toList();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static DataFile dataFile(File dir, String name) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(dir.getAbsolutePath() + "/data/" + name + ".parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(2)
        .build();
  }

  private void addDV(DataFile dataFile) {
    table.newRowDelta().addDeletes(FileMetadata.deleteFileBuilder(table.spec())
        .ofPositionDeletes()
        .withFormat(FileFormat.PUFFIN)
        .withPath(dataFile.location() + ".puffin")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .withReferencedDataFile(dataFile.location())
        .withContentOffset(4)
        .withContentSizeInBytes(6)
        .build()).commit();
  }
}
