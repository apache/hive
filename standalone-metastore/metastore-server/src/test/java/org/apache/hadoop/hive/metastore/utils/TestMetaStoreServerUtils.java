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

package org.apache.hadoop.hive.metastore.utils;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.common.StatsSetupConst.COLUMN_STATS_ACCURATE;
import static org.apache.hadoop.hive.common.StatsSetupConst.FAST_STATS;
import static org.apache.hadoop.hive.common.StatsSetupConst.NUM_FILES;
import static org.apache.hadoop.hive.common.StatsSetupConst.NUM_ERASURE_CODED_FILES;
import static org.apache.hadoop.hive.common.StatsSetupConst.STATS_GENERATED;
import static org.apache.hadoop.hive.common.StatsSetupConst.TOTAL_SIZE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category(MetastoreUnitTest.class)
public class TestMetaStoreServerUtils {

  private static final String DB_NAME = "db1";
  private static final String TABLE_NAME = "tbl1";

  private final Map<String, String> paramsWithStats = ImmutableMap.of(
      NUM_FILES, "1",
      TOTAL_SIZE, "2",
      NUM_ERASURE_CODED_FILES, "0"
  );

  private Database db;

  public TestMetaStoreServerUtils() {
    try {
      db = new DatabaseBuilder().setName(DB_NAME).build(null);
    } catch (TException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testTrimMapNullsXform() throws Exception {
    Map<String,String> m = new HashMap<>();
    m.put("akey","aval");
    m.put("blank","");
    m.put("null",null);

    Map<String, String> expected = ImmutableMap.of("akey", "aval",
        "blank", "", "null", "");

    Map<String,String> xformed = MetaStoreServerUtils.trimMapNulls(m,true);
    assertThat(xformed, is(expected));
  }

  @Test
  public void testTrimMapNullsPrune() throws Exception {
    Map<String,String> m = new HashMap<>();
    m.put("akey","aval");
    m.put("blank","");
    m.put("null",null);
    Map<String, String> expected = ImmutableMap.of("akey", "aval", "blank", "");

    Map<String,String> pruned = MetaStoreServerUtils.trimMapNulls(m,false);
    assertThat(pruned, is(expected));
  }

  @Test
  public void testcolumnsIncludedByNameType() {
    FieldSchema col1 = new FieldSchema("col1", "string", "col1 comment");
    FieldSchema col1a = new FieldSchema("col1", "string", "col1 but with a different comment");
    FieldSchema col2 = new FieldSchema("col2", "string", "col2 comment");
    FieldSchema col3 = new FieldSchema("col3", "string", "col3 comment");
    Assert.assertTrue(MetaStoreServerUtils.columnsIncludedByNameType(Arrays.asList(col1), Arrays.asList(col1)));
    Assert.assertTrue(MetaStoreServerUtils.columnsIncludedByNameType(Arrays.asList(col1), Arrays.asList(col1a)));
    Assert.assertTrue(MetaStoreServerUtils.columnsIncludedByNameType(Arrays.asList(col1, col2), Arrays.asList(col1, col2)));
    Assert.assertTrue(MetaStoreServerUtils.columnsIncludedByNameType(Arrays.asList(col1, col2), Arrays.asList(col2, col1)));
    Assert.assertTrue(MetaStoreServerUtils.columnsIncludedByNameType(Arrays.asList(col1, col2), Arrays.asList(col1, col2, col3)));
    Assert.assertTrue(MetaStoreServerUtils.columnsIncludedByNameType(Arrays.asList(col1, col2), Arrays.asList(col3, col2, col1)));
    Assert.assertFalse(MetaStoreServerUtils.columnsIncludedByNameType(Arrays.asList(col1, col2), Arrays.asList(col1)));
  }

  /**
   * Verify that updateTableStatsSlow really updates table statistics.
   * The test does the following:
   * <ol>
   *   <li>Create database</li>
   *   <li>Create unpartitioned table</li>
   *   <li>Create unpartitioned table which has params</li>
   *   <li>Call updateTableStatsSlow with arguments which should cause stats calculation</li>
   *   <li>Verify table statistics using mocked warehouse</li>
   *   <li>Create table which already have stats</li>
   *   <li>Call updateTableStatsSlow forcing stats recompute</li>
   *   <li>Verify table statistics using mocked warehouse</li>
   *   <li>Verifies behavior when STATS_GENERATED is set in environment context</li>
   * </ol>
   */
  @Test
  public void testUpdateTableStatsSlow_statsUpdated() throws TException {
    long fileLength = 5;

    // Create database and table
    Table tbl = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("id", "int")
        .build(null);


    // Set up mock warehouse
    FileStatus fs1 = getFileStatus(1, true, 2, 3, 4, "/tmp/0", false);
    FileStatus fs2 = getFileStatus(fileLength, false, 3, 4, 5, "/tmp/1", true);
    FileStatus fs3 = getFileStatus(fileLength, false, 3, 4, 5, "/tmp/1", false);
    List<FileStatus> fileStatus = Arrays.asList(fs1, fs2, fs3);
    Warehouse wh = mock(Warehouse.class);
    when(wh.getFileStatusesForUnpartitionedTable(db, tbl)).thenReturn(fileStatus);

    Map<String, String> expected = ImmutableMap.of(NUM_FILES, "2",
        TOTAL_SIZE, String.valueOf(2 * fileLength),
        NUM_ERASURE_CODED_FILES, "1"
    );
    MetaStoreServerUtils.updateTableStatsSlow(db, tbl, wh, false, false, null);
    assertThat(tbl.getParameters(), is(expected));

    // Verify that when stats are already present and forceRecompute is specified they are recomputed
    Table tbl1 = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("id", "int")
        .addTableParam(NUM_FILES, "0")
        .addTableParam(TOTAL_SIZE, "0")
        .build(null);
    when(wh.getFileStatusesForUnpartitionedTable(db, tbl1)).thenReturn(fileStatus);
    MetaStoreServerUtils.updateTableStatsSlow(db, tbl1, wh, false, true, null);
    assertThat(tbl1.getParameters(), is(expected));

    // Verify that COLUMN_STATS_ACCURATE is removed from params
    Table tbl2 = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("id", "int")
        .addTableParam(COLUMN_STATS_ACCURATE, "true")
        .build(null);
    when(wh.getFileStatusesForUnpartitionedTable(db, tbl2)).thenReturn(fileStatus);
    MetaStoreServerUtils.updateTableStatsSlow(db, tbl2, wh, false, true, null);
    assertThat(tbl2.getParameters(), is(expected));

    EnvironmentContext context = new EnvironmentContext(ImmutableMap.of(STATS_GENERATED,
        StatsSetupConst.TASK));

    // Verify that if environment context has STATS_GENERATED set to task,
    // COLUMN_STATS_ACCURATE in params is set to correct value
    Table tbl3 = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("id", "int")
        .addTableParam(COLUMN_STATS_ACCURATE, "foo") // The value doesn't matter
        .build(null);
    when(wh.getFileStatusesForUnpartitionedTable(db, tbl3)).thenReturn(fileStatus);
    MetaStoreServerUtils.updateTableStatsSlow(db, tbl3, wh, false, true, context);

    Map<String, String> expected1 = ImmutableMap.of(NUM_FILES, "2",
        TOTAL_SIZE, String.valueOf(2 * fileLength),
        NUM_ERASURE_CODED_FILES, "1",
        COLUMN_STATS_ACCURATE, "{\"BASIC_STATS\":\"true\"}");
    assertThat(tbl3.getParameters(), is(expected1));
  }

  /**
   * Verify that the call to updateTableStatsSlow() removes DO_NOT_UPDATE_STATS from table params.
   */
  @Test
  public void testUpdateTableStatsSlow_removesDoNotUpdateStats() throws TException {
    // Create database and table
    Table tbl = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("id", "int")
        .addTableParam(StatsSetupConst.DO_NOT_UPDATE_STATS, "true")
        .build(null);
    Table tbl1 = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("id", "int")
        .addTableParam(StatsSetupConst.DO_NOT_UPDATE_STATS, "false")
        .build(null);
    Warehouse wh = mock(Warehouse.class);
    MetaStoreServerUtils.updateTableStatsSlow(db, tbl, wh, false, true, null);
    assertThat(tbl.getParameters(), is(Collections.emptyMap()));
    verify(wh, never()).getFileStatusesForUnpartitionedTable(db, tbl);
    MetaStoreServerUtils.updateTableStatsSlow(db, tbl1, wh, true, false, null);
    assertThat(tbl.getParameters(), is(Collections.emptyMap()));
    verify(wh, never()).getFileStatusesForUnpartitionedTable(db, tbl1);
  }

  /**
   * Verify that updateTableStatsSlow() does not calculate table statistics when
   * <ol>
   *   <li>newDir is true</li>
   *   <li>Table is partitioned</li>
   *   <li>Stats are already present and forceRecompute isn't set</li>
   * </ol>
   */
  @Test
  public void testUpdateTableStatsSlow_doesNotUpdateStats() throws TException {
    // Create database and table
    FieldSchema fs = new FieldSchema("date", "string", "date column");
    List<FieldSchema> cols = Collections.singletonList(fs);

    Table tbl = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("id", "int")
        .build(null);
    Warehouse wh = mock(Warehouse.class);
    // newDir(true) => stats not updated
    MetaStoreServerUtils.updateTableStatsSlow(db, tbl, wh, true, false, null);
    verify(wh, never()).getFileStatusesForUnpartitionedTable(db, tbl);

    // partitioned table => stats not updated
    Table tbl1 = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("id", "int")
        .setPartCols(cols)
        .build(null);
    MetaStoreServerUtils.updateTableStatsSlow(db, tbl1, wh, false, false, null);
    verify(wh, never()).getFileStatusesForUnpartitionedTable(db, tbl1);

    // Already contains stats => stats not updated when forceRecompute isn't set
    Table tbl2 = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("id", "int")
        .setTableParams(paramsWithStats)
        .build(null);
    MetaStoreServerUtils.updateTableStatsSlow(db, tbl2, wh, false, false, null);
    verify(wh, never()).getFileStatusesForUnpartitionedTable(db, tbl2);
  }

  @Test
  public void isFastStatsSameWithNullPartitions() {
    Partition partition = new Partition();
    assertFalse(MetaStoreServerUtils.isFastStatsSame(null, null));
    assertFalse(MetaStoreServerUtils.isFastStatsSame(null, partition));
    assertFalse(MetaStoreServerUtils.isFastStatsSame(partition, null));
  }

  @Test
  public void isFastStatsSameWithNoMatchingStats() {
    Partition oldPartition = new Partition();
    Map<String, String> stats = new HashMap<>();
    oldPartition.setParameters(stats);
    assertFalse(MetaStoreServerUtils.isFastStatsSame(oldPartition, null));
    stats.put("someKeyThatIsNotInFastStats","value");
    oldPartition.setParameters(stats);
    assertFalse(MetaStoreServerUtils.isFastStatsSame(oldPartition, null));
  }

  //Test case where one or all of the FAST_STATS parameters are not present in newPart
  @Test
  public void isFastStatsSameMatchingButOnlyOneStat() {
    Partition oldPartition = new Partition();
    Partition newPartition = new Partition();
    Map<String, String> randomParams = new HashMap<String, String>();
    randomParams.put("randomParam1", "randomVal1");
    newPartition.setParameters(randomParams);
    assertFalse(MetaStoreServerUtils.isFastStatsSame(oldPartition, newPartition));
  }

  //Test case where all parameters are present and their values are same
  @Test
  public void isFastStatsSameMatching() {
    Partition oldPartition = new Partition();
    Partition newPartition = new Partition();
    Map<String, String> stats = new HashMap<>();

    Map<String, String> oldParams = new HashMap<>();
    Map<String, String> newParams = new HashMap<>();
    long testVal = 1;
    for (String key : FAST_STATS) {
      oldParams.put(key, String.valueOf(testVal));
      newParams.put(key, String.valueOf(testVal));
    }

    oldPartition.setParameters(oldParams);
    newPartition.setParameters(newParams);
    assertTrue(MetaStoreServerUtils.isFastStatsSame(oldPartition, newPartition));
  }

  //Test case where all parameters are present and their values are different
  @Test
  public void isFastStatsSameDifferent() {
    Partition oldPartition = new Partition();
    Partition newPartition = new Partition();
    Map<String, String> stats = new HashMap<>();

    Map<String, String> oldParams = new HashMap<>();
    Map<String, String> newParams = new HashMap<>();
    long testVal = 1;
    for (String key : FAST_STATS) {
      oldParams.put(key, String.valueOf(testVal));
      newParams.put(key, String.valueOf(++testVal));
    }

    oldPartition.setParameters(oldParams);
    newPartition.setParameters(newParams);
    assertFalse(MetaStoreServerUtils.isFastStatsSame(oldPartition, newPartition));
  }

  @Test
  public void isFastStatsSameNullStatsInNew() {
    Partition oldPartition = new Partition();
    Partition newPartition = new Partition();
    Map<String, String> oldParams = new HashMap<>();
    Map<String, String> newParams = new HashMap<>();
    long testVal = 1;
    for (String key : FAST_STATS) {
      oldParams.put(key, String.valueOf(testVal));
      newParams.put(key, null);
    }
    oldPartition.setParameters(oldParams);
    newPartition.setParameters(newParams);
    assertFalse(MetaStoreServerUtils.isFastStatsSame(oldPartition, newPartition));
  }

  /**
   * Build a FileStatus object.
   */
  private static FileStatus getFileStatus(long fileLength, boolean isdir, int blockReplication,
      int blockSize, int modificationTime, String pathString, boolean isErasureCoded) {
    return new FileStatus(fileLength, isdir, blockReplication, blockSize, modificationTime,
        0L, (FsPermission)null, (String)null, (String)null, null,
        new Path(pathString), false, false, isErasureCoded);
  }

}

