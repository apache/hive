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

import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.assertBooleanStats;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

@Category(MetastoreUnitTest.class)
public class BooleanColumnStatsAggregatorTest {

  private static final Table TABLE = new Table("dummy", "db", "hive", 0, 0,
      0, null, null, Collections.emptyMap(), null, null,
      TableType.MANAGED_TABLE.toString());
  private static final FieldSchema COL = new FieldSchema("col", "int", "");

  @Test
  public void testAggregateSingleStat() throws MetaException {
    BooleanColumnStatsAggregator aggregator = new BooleanColumnStatsAggregator();
    List<String> partitionNames = Collections.singletonList("part1");

    ColumnStatisticsData data1 = StatisticsTestUtils.createBooleanStats(1L, 2L, 13L);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = Collections.singletonList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)));

    ColumnStatisticsObj stats = aggregator.aggregate(statsList, partitionNames, true);
    assertBooleanStats(stats, 1L, 2L, 13L);
  }

  @Test
  public void testAggregateSingleStatWhenNullValues() throws MetaException {
    BooleanColumnStatsAggregator aggregator = new BooleanColumnStatsAggregator();

    List<String> partitionNames = Collections.singletonList("part1");
    ColumnStatisticsData data1 = StatisticsTestUtils.createBooleanStats(1L, null, null);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = Collections.singletonList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)));

    ColumnStatisticsObj statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertBooleanStats(statsObj, 1L, null, null);

    aggregator.useDensityFunctionForNDVEstimation = true;
    statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertBooleanStats(statsObj, 1L, null, null);

    aggregator.useDensityFunctionForNDVEstimation = false;
    aggregator.ndvTuner = 1;
    // ndv tuner does not have any effect because min numDVs and max numDVs coincide (we have a single stats)
    statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertBooleanStats(statsObj, 1L, null, null);
  }

  @Test
  public void testAggregateMultipleStatsWhenSomeNullValues() throws MetaException {
    BooleanColumnStatsAggregator aggregator = new BooleanColumnStatsAggregator();

    List<String> partitionNames = Arrays.asList("part1", "part2");

    ColumnStatisticsData data1 = StatisticsTestUtils.createBooleanStats(
        1L, 2L, 4L);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    ColumnStatisticsData data2 = StatisticsTestUtils.createBooleanStats(
        2L, null, null);
    ColumnStatistics stats2 = StatisticsTestUtils.createColStats(data2, TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)),
        new ColStatsObjWithSourceInfo(stats2.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(1)));

    ColumnStatisticsObj statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertBooleanStats(statsObj, 3L, 2L, 4L);
  }

  @Test
  public void testAggregateMultiStatsWhenAllAvailable() throws MetaException {
    BooleanColumnStatsAggregator aggregator = new BooleanColumnStatsAggregator();

    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    ColumnStatisticsData data1 = StatisticsTestUtils.createBooleanStats(
        1L, 3L, 13L);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    ColumnStatisticsData data2 = StatisticsTestUtils.createBooleanStats(
        2L, 6L, 18L);
    ColumnStatistics stats2 = StatisticsTestUtils.createColStats(data2, TABLE, COL, partitionNames.get(1));

    ColumnStatisticsData data3 = StatisticsTestUtils.createBooleanStats(
        3L, 2L, 18L);
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(data3, TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)),
        new ColStatsObjWithSourceInfo(stats2.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(1)),
        new ColStatsObjWithSourceInfo(stats3.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(2)));

    ColumnStatisticsObj stats = aggregator.aggregate(statsList, partitionNames, true);
    // max among numDVs values is kept
    assertBooleanStats(stats, 6L, 11L, 49L);
  }

  @Test
  public void testAggregateMultiStatsWhenOnlySomeAvailable() throws MetaException {
    BooleanColumnStatsAggregator aggregator = new BooleanColumnStatsAggregator();
    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    ColumnStatisticsData data1 = StatisticsTestUtils.createBooleanStats(
        1L, 3L, 13L);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    ColumnStatisticsData data3 = StatisticsTestUtils.createBooleanStats(
        3L, 2L, 18L);
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(data3, TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)),
        new ColStatsObjWithSourceInfo(stats3.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(2)));

    ColumnStatisticsObj stats = aggregator.aggregate(statsList, partitionNames, false);
    assertBooleanStats(stats, 4L, 5L, 31L);
  }
}
