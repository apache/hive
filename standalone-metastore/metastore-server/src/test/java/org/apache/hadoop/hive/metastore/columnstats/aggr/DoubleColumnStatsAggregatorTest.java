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
import org.apache.hadoop.hive.metastore.columnstats.ColStatsBuilder;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Category(MetastoreUnitTest.class)
public class DoubleColumnStatsAggregatorTest {

  private static final Table TABLE = new Table("dummy", "db", "hive", 0, 0,
      0, null, null, Collections.emptyMap(), null, null,
      TableType.MANAGED_TABLE.toString());
  private static final FieldSchema COL = new FieldSchema("col", "double", "");

  @Test
  public void testAggregateSingleStat() throws MetaException {
    List<String> partitionNames = Collections.singletonList("part1");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(double.class).numNulls(1).numDVs(2)
        .low(1d).high(4d).hll(1, 3).build();
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = StatisticsTestUtils.createColStatsObjWithSourceInfoList(
        TABLE, partitionNames, Collections.singletonList(stats1));

    DoubleColumnStatsAggregator aggregator = new DoubleColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);

    Assert.assertEquals(data1, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateSingleStatWhenNullValues() throws MetaException {
    List<String> partitionNames = Collections.singletonList("part1");
    ColumnStatisticsData data1 = new ColStatsBuilder<>(double.class).numNulls(1).numDVs(2).build();
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = StatisticsTestUtils.createColStatsObjWithSourceInfoList(
        TABLE, partitionNames, Collections.singletonList(stats1));

    DoubleColumnStatsAggregator aggregator = new DoubleColumnStatsAggregator();

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    Assert.assertEquals(data1, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = true;
    computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    Assert.assertEquals(data1, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = false;
    aggregator.ndvTuner = 1;
    // ndv tuner does not have any effect because min numDVs and max numDVs coincide (we have a single stats)
    computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    Assert.assertEquals(data1, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultipleStatsWhenSomeNullValues() throws MetaException {
    List<String> partitionNames = Arrays.asList("part1", "part2");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(double.class).numNulls(1).numDVs(2)
        .low(1d).high(2d).hll(1, 2).build();
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    ColumnStatisticsData data2 = new ColStatsBuilder<>(double.class).numNulls(2).numDVs(3).build();
    ColumnStatistics stats2 = StatisticsTestUtils.createColStats(data2, TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = StatisticsTestUtils.createColStatsObjWithSourceInfoList(
        TABLE, partitionNames, Arrays.asList(stats1, stats2));

    DoubleColumnStatsAggregator aggregator = new DoubleColumnStatsAggregator();

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(double.class).numNulls(3).numDVs(3)
        .low(1d).high(2d).hll(1, 2).build();
    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = true;
    computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    expectedStats = new ColStatsBuilder<>(double.class).numNulls(3).numDVs(4)
        .low(1d).high(2d).hll(1, 2).build();
    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = false;
    aggregator.ndvTuner = 1;
    computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    expectedStats = new ColStatsBuilder<>(double.class).numNulls(3).numDVs(5)
        .low(1d).high(2d).hll(1, 2).build();
    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsWhenAllAvailable() throws MetaException {
    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(double.class).numNulls(1).numDVs(3)
        .low(1d).high(3d).hll(1, 2, 3).build();
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    ColumnStatisticsData data2 = new ColStatsBuilder<>(double.class).numNulls(2).numDVs(3)
        .low(3d).high(5d).hll(3, 4, 5).build();
    ColumnStatistics stats2 = StatisticsTestUtils.createColStats(data2, TABLE, COL, partitionNames.get(1));

    ColumnStatisticsData data3 = new ColStatsBuilder<>(double.class).numNulls(3).numDVs(2)
        .low(6d).high(7d).hll(6, 7).build();
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(data3, TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = StatisticsTestUtils.createColStatsObjWithSourceInfoList(
        TABLE, partitionNames, Arrays.asList(stats1, stats2, stats3));

    DoubleColumnStatsAggregator aggregator = new DoubleColumnStatsAggregator();

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    // the aggregation does not update hll, only numNDVs is, it keeps the first hll
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(double.class).numNulls(6).numDVs(7)
        .low(1d).high(7d).hll(1, 2, 3).build();
    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsWhenUnmergeableBitVectors() throws MetaException {
    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(double.class).numNulls(1).numDVs(3)
        .low(1d).high(3d).fmSketch(1, 2, 3).build();
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    ColumnStatisticsData data2 = new ColStatsBuilder<>(double.class).numNulls(2).numDVs(3)
        .low(3d).high(5d).hll(3, 4, 5).build();
    ColumnStatistics stats2 = StatisticsTestUtils.createColStats(data2, TABLE, COL, partitionNames.get(1));

    ColumnStatisticsData data3 = new ColStatsBuilder<>(double.class).numNulls(3).numDVs(4)
        .low(1d).high(8d).hll(1, 2, 6, 8).build();
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(data3, TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = StatisticsTestUtils.createColStatsObjWithSourceInfoList(
        TABLE, partitionNames, Arrays.asList(stats1, stats2, stats3));

    DoubleColumnStatsAggregator aggregator = new DoubleColumnStatsAggregator();

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    // the aggregation does not update the bitvector, only numDVs is, it keeps the first bitvector;
    // numDVs is set to the maximum among all stats when non-mergeable bitvectors are detected
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(double.class).numNulls(6).numDVs(4)
        .low(1d).high(8d).fmSketch(1, 2, 3).build();
    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = true;
    computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    // the use of the density function leads to a different estimation for numNDV
    expectedStats = new ColStatsBuilder<>(double.class).numNulls(6).numDVs(6)
        .low(1d).high(8d).fmSketch(1, 2, 3).build();
    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());

    // here the ndv lower bound is 4 (the highest individual numDVs), the higher bound is 10 (3 + 3 + 4, that is the
    // sum of all the numDVs for all partitions), ndv tuner influences the choice between the lower bound
    // (ndvTuner = 0) and the higher bound (ndvTuner = 1), and intermediate values for ndvTuner in the range (0, 1)
    aggregator.useDensityFunctionForNDVEstimation = false;

    aggregator.ndvTuner = 0;
    computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    expectedStats = new ColStatsBuilder<>(double.class).numNulls(6).numDVs(4)
        .low(1d).high(8d).fmSketch(1, 2, 3).build();
    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());

    aggregator.ndvTuner = 0.5;
    computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    expectedStats = new ColStatsBuilder<>(double.class).numNulls(6).numDVs(7)
        .low(1d).high(8d).fmSketch(1, 2, 3).build();
    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());

    aggregator.ndvTuner = 0.75;
    computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    expectedStats = new ColStatsBuilder<>(double.class).numNulls(6).numDVs(8)
        .low(1d).high(8d).fmSketch(1, 2, 3).build();
    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());

    aggregator.ndvTuner = 1;
    computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    expectedStats = new ColStatsBuilder<>(double.class).numNulls(6).numDVs(10)
        .low(1d).high(8d).fmSketch(1, 2, 3).build();
    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsWhenOnlySomeAvailable() throws MetaException {
    List<String> partitionNames = Arrays.asList("part1", "part2", "part3", "part4");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(double.class).numNulls(1).numDVs(3)
        .low(1d).high(3d).hll(1, 2, 3).build();
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    ColumnStatisticsData data3 = new ColStatsBuilder<>(double.class).numNulls(3).numDVs(1)
        .low(7d).high(7d).hll(7).build();
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(data3, TABLE, COL, partitionNames.get(2));

    ColumnStatisticsData data4 = new ColStatsBuilder<>(double.class).numNulls(2).numDVs(3)
        .low(3d).high(5d).hll(3, 4, 5).build();
    ColumnStatistics stats4 = StatisticsTestUtils.createColStats(data4, TABLE, COL, partitionNames.get(3));

    List<ColStatsObjWithSourceInfo> statsList = StatisticsTestUtils.createColStatsObjWithSourceInfoList(
        TABLE, partitionNames, Arrays.asList(stats1, null, stats3, stats4), Arrays.asList(0, 2, 3));

    DoubleColumnStatsAggregator aggregator = new DoubleColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitionNames, false);

    // hll in case of missing stats is left as null, only numDVs is updated
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(double.class).numNulls(8).numDVs(4)
        .low(1d).high(9.4).build();
    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsOnlySomeAvailableButUnmergeableBitVector() throws MetaException {
    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(double.class).numNulls(1).numDVs(3)
        .low(1d).high(6d).fmSketch(1, 2, 6).build();
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    ColumnStatisticsData data3 = new ColStatsBuilder<>(double.class).numNulls(3).numDVs(1)
        .low(7d).high(7d).hll(7).build();
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(data3, TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = StatisticsTestUtils.createColStatsObjWithSourceInfoList(
        TABLE, partitionNames, Arrays.asList(stats1, null, stats3), Arrays.asList(0, 2));

    DoubleColumnStatsAggregator aggregator = new DoubleColumnStatsAggregator();

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitionNames, false);
    // hll in case of missing stats is left as null, only numDVs is updated
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(double.class).numNulls(6).numDVs(3)
        .low(1d).high(7.5).build();
    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = true;
    computedStatsObj = aggregator.aggregate(statsList, partitionNames, false);
    // the use of the density function leads to a different estimation for numNDV
    expectedStats = new ColStatsBuilder<>(double.class).numNulls(6).numDVs(4)
        .low(1d).high(7.5).build();
    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());
  }
}
