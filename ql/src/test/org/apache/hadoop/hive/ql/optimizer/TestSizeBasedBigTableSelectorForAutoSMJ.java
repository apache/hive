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
package org.apache.hadoop.hive.ql.optimizer;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * The big-table choice for an automatic sort-merge join must take a handler table's size from
 * the handler's statistics, and must never list the table's location for one.
 */
public class TestSizeBasedBigTableSelectorForAutoSMJ {

  private final SizeBasedBigTableSelectorForAutoSMJ selector = new TableSizeBasedBigTableSelectorForAutoSMJ();
  private final HiveConf conf = new HiveConf();

  private Table handlerTable(Map<String, String> basicStats) {
    HiveStorageHandler handler = Mockito.mock(HiveStorageHandler.class);
    Mockito.when(handler.canProvideBasicStatistics()).thenReturn(true);
    Mockito.when(handler.getBasicStatistics(Mockito.any())).thenReturn(basicStats);
    Table table = Mockito.mock(Table.class);
    Mockito.when(table.isNonNative()).thenReturn(true);
    Mockito.when(table.getStorageHandler()).thenReturn(handler);
    return table;
  }

  @Test
  public void handlerTableSizeComesFromItsStatisticsWithoutTouchingTheFilesystem() {
    Table table = handlerTable(Map.of(StatsSetupConst.TOTAL_SIZE, "12345"));

    Assert.assertEquals(12345, selector.getSize(conf, table));
    // the location is only asked for on the listing fallback, which a handler table never takes
    Mockito.verify(table, Mockito.never()).getPath();
  }

  @Test
  public void handlerTableOfUnknownSizeReportsUnknownRatherThanListing() {
    Table table = handlerTable(Map.of());

    Assert.assertEquals(-1, selector.getSize(conf, table));
    Mockito.verify(table, Mockito.never()).getPath();
  }

  @Test
  public void handlerPartitionsAreSizedTogetherAndSummed() {
    // the table's own size stands for every partition rather than for any of them, so the
    // partitions a scan reads are asked for by name and their sizes added
    Table table = handlerTable(Map.of(StatsSetupConst.TOTAL_SIZE, "777"));
    Mockito.when(table.getStorageHandler().getAggrBasicStatsFor(Mockito.eq(table), Mockito.anyList()))
        .thenReturn(Map.of(
            "p=a", Map.of(StatsSetupConst.TOTAL_SIZE, "100"),
            "p=b", Map.of(StatsSetupConst.TOTAL_SIZE, "20")));

    Assert.assertEquals("the partitions read, not the table once per partition",
        120, selector.getSize(conf, table, List.of(partition("p=a"), partition("p=b"))));
  }

  @Test
  public void handlerPartitionOfUnknownSizeFallsBackToTheTable() {
    // sizing one partition and not the other would stand for less than the scan reads, so the
    // table's own size answers instead: more than the scan reads, never less
    Table table = handlerTable(Map.of(StatsSetupConst.TOTAL_SIZE, "777"));
    Mockito.when(table.getStorageHandler().getAggrBasicStatsFor(Mockito.eq(table), Mockito.anyList()))
        .thenReturn(Map.of("p=a", Map.of(StatsSetupConst.TOTAL_SIZE, "100")));

    Assert.assertEquals(777, selector.getSize(conf, table, List.of(partition("p=a"), partition("p=b"))));
  }

  private static Partition partition(String name) {
    Partition partition = Mockito.mock(Partition.class);
    Mockito.when(partition.getName()).thenReturn(name);
    return partition;
  }

  @Test
  public void nativeTableKeepsReadingItsParameter() {
    Table table = Mockito.mock(Table.class);
    Mockito.when(table.isNonNative()).thenReturn(false);
    Mockito.when(table.getProperty("totalSize")).thenReturn("4242");

    Assert.assertEquals(4242, selector.getSize(conf, table));
  }
}
