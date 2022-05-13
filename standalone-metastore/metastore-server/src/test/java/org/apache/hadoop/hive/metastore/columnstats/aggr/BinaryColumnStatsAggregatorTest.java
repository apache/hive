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
package org.apache.hadoop.hive.metastore.columnstats.aggr;

import org.apache.hadoop.hive.metastore.StatisticsTestUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.assertBinaryStats;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

@Category(MetastoreUnitTest.class)
public class BinaryColumnStatsAggregatorTest {

  private static final Table TABLE = new Table("dummy", "db", "hive", 0, 0,
      0, null, null, Collections.emptyMap(), null, null,
      TableType.MANAGED_TABLE.toString());
  private static final FieldSchema COL = new FieldSchema("col", "int", "");

  @Test
  public void testAggregateSingleStat() throws MetaException {
    BinaryColumnStatsAggregator aggregator = new BinaryColumnStatsAggregator();
    List<String> partitionNames = Collections.singletonList("part1");
    
    ColumnStatisticsData data1 = StatisticsTestUtils.createBinaryStats(1L, 8.5, 13L);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = Collections.singletonList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)));

    ColumnStatisticsObj stats = aggregator.aggregate(statsList, partitionNames, true);
    assertBinaryStats(stats, 1L, 8.5, 13L);
  }

  @Test
  public void testAggregateSingleStatWhenNullValues() throws MetaException {
    BinaryColumnStatsAggregator aggregator = new BinaryColumnStatsAggregator();

    List<String> partitionNames = Collections.singletonList("part1");
    ColumnStatisticsData data1 = StatisticsTestUtils.createBinaryStats(
            1L, null, null);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = Collections.singletonList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)));

    ColumnStatisticsObj statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertBinaryStats(statsObj, 1L, null, null);
  }

  @Test
  public void testAggregateMultipleStatsWhenSomeNullValues() throws MetaException {
    BinaryColumnStatsAggregator aggregator = new BinaryColumnStatsAggregator();

    List<String> partitionNames = Arrays.asList("part1", "part2");

    ColumnStatisticsData data1 = StatisticsTestUtils.createBinaryStats(
        1L, 3.0, 4L);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    ColumnStatisticsData data2 = StatisticsTestUtils.createBinaryStats(
        2L, null, null);
    ColumnStatistics stats2 = StatisticsTestUtils.createColStats(data2, TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)),
        new ColStatsObjWithSourceInfo(stats2.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(1)));

    ColumnStatisticsObj statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertBinaryStats(statsObj, 3L, 3.0, 4L);
  }

  @Test
  public void testAggregateMultiStatsWhenAllAvailable() throws MetaException {
    BinaryColumnStatsAggregator aggregator = new BinaryColumnStatsAggregator();

    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    ColumnStatisticsData data1 = StatisticsTestUtils.createBinaryStats(
        1L, 20.0 / 3, 13L);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    ColumnStatisticsData data2 = StatisticsTestUtils.createBinaryStats(
        2L, 14.0, 18L);
    ColumnStatistics stats2 = StatisticsTestUtils.createColStats(data2, TABLE, COL, partitionNames.get(1));

    ColumnStatisticsData data3 = StatisticsTestUtils.createBinaryStats(
        3L, 17.5, 18L);
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(data3, TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)),
        new ColStatsObjWithSourceInfo(stats2.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(1)),
        new ColStatsObjWithSourceInfo(stats3.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(2)));

    ColumnStatisticsObj stats = aggregator.aggregate(statsList, partitionNames, true);
    assertBinaryStats(stats, 6L, 17.5, 18L);
  }

  @Test
  public void testAggregateMultiStatsWhenOnlySomeAvailable() throws MetaException {
    BinaryColumnStatsAggregator aggregator = new BinaryColumnStatsAggregator();
    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    ColumnStatisticsData data1 = StatisticsTestUtils.createBinaryStats(
        1L, 20.0 / 3, 13L);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    ColumnStatisticsData data3 = StatisticsTestUtils.createBinaryStats(
        3L, 17.5, 18L);
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(data3, TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)),
        new ColStatsObjWithSourceInfo(stats3.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(2)));

    ColumnStatisticsObj stats = aggregator.aggregate(statsList, partitionNames, false);
    assertBinaryStats(stats, 4L, 17.5, 18L);
  }
}
