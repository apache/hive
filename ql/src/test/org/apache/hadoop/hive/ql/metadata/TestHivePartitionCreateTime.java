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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.exec.Utilities.PartitionDetails;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.plan.BasicStatsWork;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.BasicStatsTask;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests partition creation time through a remote HMS, including the statistics reload path.
 */
public class TestHivePartitionCreateTime {

  private static HiveConf hiveConf;
  private static Hive hive;

  private String tableName;

  @BeforeClass
  public static void setUpClass() throws Exception {
    hiveConf = new HiveConfForTest(TestHivePartitionCreateTime.class);
    // hive-site.xml defaults to SQLStdHiveAuthorizerFactoryForTest (hive-it-util); use the ql
    // module factory so this test runs from the ql module alone (same as TestHive).
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    // Embedded HMS can mutate the request object and hide discarded return values.
    MetaStoreTestUtils.startMetaStoreWithRetry(hiveConf);
    SessionState.start(hiveConf);
    hive = Hive.get(hiveConf);
  }

  @Before
  public void setUp() throws Exception {
    tableName = "test_partition_create_time_" + System.nanoTime();
    createExternalPartitionedTable(tableName);
  }

  @After
  public void tearDown() throws Exception {
    hive.dropTable(Warehouse.DEFAULT_DATABASE_NAME, tableName, true, true, true);
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
  public void testDynamicPartitionStatsReloadCreateTime() throws Exception {
    verifyStatsReloadCreateTime(true);
  }

  @Test
  public void testStaticPartitionStatsReloadCreateTime() throws Exception {
    verifyStatsReloadCreateTime(false);
  }

  private void verifyStatsReloadCreateTime(boolean dynamic) throws Exception {
    Table table = hive.getTable(tableName);
    Map<String, String> partSpec = partitionSpec("2026-07-25");
    hive.createPartition(table, partSpec);
    Partition stored = hive.getPartition(table, partSpec, false);
    int createTime = stored.getTPartition().getCreateTime();
    assertTrue(createTime > 0);
    FileSystem fs = stored.getDataLocation().getFileSystem(hiveConf);
    try (OutputStream out = fs.create(new Path(stored.getDataLocation(), "000000_0"))) {
      out.write("data\n".getBytes());
    }

    // HIVE-25918 reloads metadata before updating stats. Even a stale object from
    // MoveTask must not overwrite the creation time stored in HMS.
    Partition stale = new Partition(table, stored.getTPartition().deepCopy());
    stale.getTPartition().setCreateTime(0);
    LoadTableDesc load = new LoadTableDesc(table.getDataLocation(), table, false, true, partSpec);
    if (dynamic) {
      load.setDPCtx(new DynamicPartitionCtx(partitionSpec(null), "__HIVE_DEFAULT_PARTITION__", 10));
    }
    BasicStatsWork work = new BasicStatsWork(load);
    work.setNoStatsAggregator(true);
    work.setStatsReliable(true);
    BasicStatsTask stats = new BasicStatsTask(hiveConf, work);
    stats.setDpPartSpecs(Arrays.asList(stale));

    assertEquals(0, stats.process(hive, table));
    Partition updated = hive.getPartition(table, partSpec, false);
    assertEquals(createTime, updated.getTPartition().getCreateTime());
    assertEquals("1", updated.getParameters().get("numFiles"));
    assertEquals(0, stale.getTPartition().getCreateTime());
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
