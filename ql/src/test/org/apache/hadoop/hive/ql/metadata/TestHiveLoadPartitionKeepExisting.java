/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.exec.Utilities.PartitionDetails;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Regression tests for {@link Hive#loadPartition} commit semantics on dynamic partitions.
 *
 * <p>MoveTask calls {@code loadPartition} with {@link LoadFileType#KEEP_EXISTING} after
 * {@code INSERT INTO} (append) or {@link LoadFileType#REPLACE_ALL} after {@code INSERT OVERWRITE}.
 * These tests exercise that commit path directly — not a full SQL {@code INSERT} statement.
 */
public class TestHiveLoadPartitionKeepExisting {

  private static HiveConf hiveConf;
  private static Hive hive;

  private String tableName;

  @BeforeClass
  public static void setUpClass() throws Exception {
    hiveConf = new HiveConfForTest(TestHiveLoadPartitionKeepExisting.class);
    // hive-site.xml defaults to SQLStdHiveAuthorizerFactoryForTest (hive-it-util); use the ql
    // module factory so this test runs from the ql module alone (same as TestHive).
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    SessionState.start(hiveConf);
    hive = Hive.get(hiveConf);
  }

  @Before
  public void setUp() throws Exception {
    tableName = "test_load_partition_keep_existing_" + System.nanoTime();
    createExternalPartitionedTable(tableName);
  }

  @After
  public void tearDown() throws Exception {
    hive.dropTable(Warehouse.DEFAULT_DATABASE_NAME, tableName, true, true, true);
  }

  /**
   * {@link LoadFileType#KEEP_EXISTING} when partition data exists on disk but HMS has no entry yet.
   */
  @Test
  public void testKeepExistingAppendsWhenPartitionDirExistsButNotInHms() throws Exception {
    Table table = hive.getTable(tableName);
    Map<String, String> partSpec = partitionSpec("2026-06-14");
    Path partPath = new Path(table.getDataLocation(), Warehouse.makePartPath(partSpec));
    FileSystem fs = partPath.getFileSystem(hiveConf);

    Path existingFile = new Path(partPath, "000000_0_writer_a");
    fs.mkdirs(partPath);
    try (OutputStream out = fs.create(existingFile)) {
      out.write("writer-a\n".getBytes());
    }

    Path staging = createStagingDir(fs, table.getDataLocation(), "writer_b");
    Path stagingFile = new Path(staging, "000000_0_writer_b");
    try (OutputStream out = fs.create(stagingFile)) {
      out.write("writer-b\n".getBytes());
    }

    hive.loadPartition(staging, table, partSpec, LoadFileType.KEEP_EXISTING, true, false,
        false, false, false, false, null, 0, false, false);

    assertTrue("Existing partition file should survive KEEP_EXISTING loadPartition (append)",
        fs.exists(new Path(partPath, "000000_0_writer_a")));
    assertTrue("Staged file should be moved in by KEEP_EXISTING loadPartition",
        fs.exists(new Path(partPath, "000000_0_writer_b")));
  }

  @Test
  public void testReplaceAllReplacesExistingPartitionData() throws Exception {
    Table table = hive.getTable(tableName);
    Map<String, String> partSpec = partitionSpec("2026-06-15");
    Path partPath = new Path(table.getDataLocation(), Warehouse.makePartPath(partSpec));
    FileSystem fs = partPath.getFileSystem(hiveConf);

    fs.mkdirs(partPath);
    try (OutputStream out = fs.create(new Path(partPath, "000000_0_old"))) {
      out.write("old-data\n".getBytes());
    }

    Path staging = createStagingDir(fs, table.getDataLocation(), "overwrite");
    try (OutputStream out = fs.create(new Path(staging, "000000_0_new"))) {
      out.write("new-data\n".getBytes());
    }

    hive.loadPartition(staging, table, partSpec, LoadFileType.REPLACE_ALL, true, false,
        false, false, false, false, null, 0, true, false);

    assertFalse("REPLACE_ALL loadPartition should remove prior partition data",
        fs.exists(new Path(partPath, "000000_0_old")));
    assertTrue("REPLACE_ALL loadPartition should publish staged data",
        fs.exists(new Path(partPath, "000000_0_new")));
  }

  @Test
  public void testLoadPartitionReturnsMetastoreCreateTime() throws Exception {
    Table table = hive.getTable(tableName);
    Map<String, String> partSpec = partitionSpec("2026-07-22");
    FileSystem fs = table.getDataLocation().getFileSystem(hiveConf);
    Path staging = createStagingDir(fs, table.getDataLocation(), "create_time");
    try (OutputStream out = fs.create(new Path(staging, "000000_0"))) {
      out.write("data\n".getBytes());
    }

    Partition loaded = hive.loadPartition(staging, table, partSpec, LoadFileType.REPLACE_ALL,
        true, false, false, false, false, false, null, 0, true, false);
    Partition stored = hive.getPartition(table, partSpec, false);

    assertTrue("loadPartition should return the createTime assigned by HMS",
        loaded.getTPartition().getCreateTime() > 0);
    assertEquals(stored.getTPartition().getCreateTime(), loaded.getTPartition().getCreateTime());
  }

  @Test
  public void testLoadDynamicPartitionsReturnsMetastoreCreateTime() throws Exception {
    Table table = hive.getTable(tableName);
    FileSystem fs = table.getDataLocation().getFileSystem(hiveConf);
    Map<Path, PartitionDetails> partitionDetails = new LinkedHashMap<>();

    for (String loadDate : Arrays.asList("2026-07-23", "2026-07-24")) {
      Map<String, String> partSpec = partitionSpec(loadDate);
      Path staging = createStagingDir(fs, table.getDataLocation(), loadDate);
      try (OutputStream out = fs.create(new Path(staging, "000000_0"))) {
        out.write(loadDate.getBytes());
      }
      PartitionDetails details = new PartitionDetails();
      details.fullSpec = partSpec;
      partitionDetails.put(staging, details);
    }

    LoadTableDesc loadTableDesc = new LoadTableDesc(table.getDataLocation(), table,
        false, true, new LinkedHashMap<>());
    Map<Map<String, String>, Partition> loaded = hive.loadDynamicPartitions(loadTableDesc,
        0, false, 0, 0, false, AcidUtils.Operation.NOT_ACID, partitionDetails);

    assertEquals(2, loaded.size());
    for (Partition partition : loaded.values()) {
      Partition stored = hive.getPartition(table, partition.getSpec(), false);
      assertTrue("loadDynamicPartitions should return the createTime assigned by HMS",
          partition.getTPartition().getCreateTime() > 0);
      assertEquals(stored.getTPartition().getCreateTime(),
          partition.getTPartition().getCreateTime());
    }
  }

  @Test
  public void testConcurrentKeepExistingAppendsBothStagedLoads() throws Exception {
    Table table = hive.getTable(tableName);
    Map<String, String> partSpec = partitionSpec("2026-06-16");
    FileSystem fs = table.getDataLocation().getFileSystem(hiveConf);

    Path stagingA = createStagingDir(fs, table.getDataLocation(), "writer_a");
    try (OutputStream out = fs.create(new Path(stagingA, "000000_0_writer_a"))) {
      out.write("writer-a\n".getBytes());
    }

    Path stagingB = createStagingDir(fs, table.getDataLocation(), "writer_b");
    try (OutputStream out = fs.create(new Path(stagingB, "000000_0_writer_b"))) {
      out.write("writer-b\n".getBytes());
    }

    CyclicBarrier barrier = new CyclicBarrier(2);
    SessionState parentSession = SessionState.get();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> writerA = executor.submit(() -> {
        SessionState.setCurrentSessionState(parentSession);
        barrierAwait(barrier);
        hive.loadPartition(stagingA, table, partSpec, LoadFileType.KEEP_EXISTING, true, false,
            false, false, false, false, null, 0, false, false);
        return null;
      });

      Future<?> writerB = executor.submit(() -> {
        SessionState.setCurrentSessionState(parentSession);
        barrierAwait(barrier);
        hive.loadPartition(stagingB, table, partSpec, LoadFileType.KEEP_EXISTING, true, false,
            false, false, false, false, null, 1, false, false);
        return null;
      });
      writerA.get(2, TimeUnit.MINUTES);
      writerB.get(2, TimeUnit.MINUTES);
    } finally {
      executor.shutdownNow();
    }

    Path partPath = new Path(table.getDataLocation(), Warehouse.makePartPath(partSpec));
    assertTrue("First KEEP_EXISTING loadPartition should publish its staged file",
        fs.exists(new Path(partPath, "000000_0_writer_a")));
    assertTrue("Second KEEP_EXISTING loadPartition should append its staged file",
        fs.exists(new Path(partPath, "000000_0_writer_b")));
  }

  private static void barrierAwait(CyclicBarrier barrier) {
    try {
      barrier.await(1, TimeUnit.MINUTES);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void createExternalPartitionedTable(String name) throws HiveException {
    hive.dropTable(Warehouse.DEFAULT_DATABASE_NAME, name, true, true, true);
    hive.createTable(name,
        Arrays.asList("value"),
        Arrays.asList("load_date"),
        TextInputFormat.class,
        HiveIgnoreKeyTextOutputFormat.class);
    Table table = hive.getTable(name);
    table.setTableType(TableType.EXTERNAL_TABLE);
    table.getParameters().put("EXTERNAL", "TRUE");
    table.getParameters().put("transactional", "false");
    hive.alterTable(table, false, null, false);
  }

  private static Map<String, String> partitionSpec(String loadDate) {
    Map<String, String> partSpec = new LinkedHashMap<>();
    partSpec.put("load_date", loadDate);
    return partSpec;
  }

  private static Path createStagingDir(FileSystem fs, Path tablePath, String suffix) throws Exception {
    Path staging = new Path(tablePath, "_staging_" + suffix + "_" + System.nanoTime());
    fs.mkdirs(staging);
    return staging;
  }
}
