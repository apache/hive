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

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.columnstats.ColStatsBuilder;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.assertEqualStatistics;
import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.createStatsWithInfo;

@Category(MetastoreUnitTest.class)
public class LongColumnStatsAggregatorTest {

  private static final Table TABLE = new Table("dummy", "db", "hive", 0, 0,
      0, null, null, Collections.emptyMap(), null, null,
      TableType.MANAGED_TABLE.toString());
  private static final FieldSchema COL = new FieldSchema("col", "int", "");

  @Test
  public void testAggregateSingleStat() throws MetaException {
    List<String> partitions = Collections.singletonList("part1");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(long.class).numNulls(1).numDVs(2)
        .low(1L).high(4L).hll(1, 4).kll(1, 4).build();
    List<ColStatsObjWithSourceInfo> statsList =
        Collections.singletonList(createStatsWithInfo(data1, TABLE, COL, partitions.get(0)));

    LongColumnStatsAggregator aggregator = new LongColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, true);

    assertEqualStatistics(data1, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateSingleStatWhenNullValues() throws MetaException {
    List<String> partitions = Collections.singletonList("part1");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(long.class).numNulls(1).numDVs(2).build();
    List<ColStatsObjWithSourceInfo> statsList =
        Collections.singletonList(createStatsWithInfo(data1, TABLE, COL, partitions.get(0)));

    LongColumnStatsAggregator aggregator = new LongColumnStatsAggregator();

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    assertEqualStatistics(data1, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = true;
    computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    assertEqualStatistics(data1, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = false;
    aggregator.ndvTuner = 1;
    // ndv tuner does not have any effect because min numDVs and max numDVs coincide (we have a single stats)
    computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    assertEqualStatistics(data1, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultipleStatsWhenSomeNullValues() throws MetaException {
    List<String> partitions = Arrays.asList("part1", "part2");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(long.class).numNulls(1).numDVs(2)
        .low(1L).high(2L).hll(1, 2).build();
    ColumnStatisticsData data2 = new ColStatsBuilder<>(long.class).numNulls(2).numDVs(3).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data2, TABLE, COL, partitions.get(1)));

    LongColumnStatsAggregator aggregator = new LongColumnStatsAggregator();

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(long.class).numNulls(3).numDVs(3)
        .low(1L).high(2L).hll(1, 2).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = true;
    computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    expectedStats = new ColStatsBuilder<>(long.class).numNulls(3).numDVs(4)
        .low(1L).high(2L).hll(1, 2).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = false;
    aggregator.ndvTuner = 1;
    computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    expectedStats = new ColStatsBuilder<>(long.class).numNulls(3).numDVs(5)
        .low(1L).high(2L).hll(1, 2).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsWhenAllAvailable() throws MetaException {
    List<String> partitions = Arrays.asList("part1", "part2", "part3");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(long.class).numNulls(1).numDVs(3)
        .low(1L).high(3L).hll(1, 2, 3).kll(1, 2, 3).build();
    ColumnStatisticsData data2 = new ColStatsBuilder<>(long.class).numNulls(2).numDVs(3)
        .low(3L).high(5L).hll(3, 4, 5).kll(3, 4, 5).build();
    ColumnStatisticsData data3 = new ColStatsBuilder<>(long.class).numNulls(3).numDVs(2)
        .low(6L).high(7L).hll(6, 7).kll(6, 7).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data2, TABLE, COL, partitions.get(1)),
        createStatsWithInfo(data3, TABLE, COL, partitions.get(2)));

    LongColumnStatsAggregator aggregator = new LongColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, true);

    // the aggregation does not update hll, only numDVs is, it keeps the first hll
    // notice that numDVs is computed by using HLL, it can detect that '3' appears twice
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(long.class).numNulls(6).numDVs(7)
        .low(1L).high(7L).hll(1, 2, 3).kll(1, 2, 3, 3, 4, 5, 6, 7).build();

    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsWhenUnmergeableBitVectors() throws MetaException {
    List<String> partitions = Arrays.asList("part1", "part2", "part3");

    long[] values = {1, 2, 3, 3, 4, 5, 1, 2, 6, 8};
    ColumnStatisticsData data1 = new ColStatsBuilder<>(long.class).numNulls(1).numDVs(3)
        .low(1L).high(3L).fmSketch(1, 2, 3).kll(1, 2, 3).build();
    ColumnStatisticsData data2 = new ColStatsBuilder<>(long.class).numNulls(2).numDVs(3)
        .low(3L).high(5L).hll(3, 4, 5).kll(3, 4, 5).build();
    ColumnStatisticsData data3 = new ColStatsBuilder<>(long.class).numNulls(3).numDVs(4)
        .low(1L).high(8L).hll(1, 2, 6, 8).kll(1, 2, 6, 8).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data2, TABLE, COL, partitions.get(1)),
        createStatsWithInfo(data3, TABLE, COL, partitions.get(2)));

    LongColumnStatsAggregator aggregator = new LongColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    // the aggregation does not update the bitvector, only numDVs is, it keeps the first bitvector;
    // numDVs is set to the maximum among all stats when non-mergeable bitvectors are detected
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(long.class).numNulls(6).numDVs(4)
        .low(1L).high(8L).fmSketch(1, 2, 3).kll(values).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = true;
    computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    // the use of the density function leads to a different estimation for numNDV
    expectedStats = new ColStatsBuilder<>(long.class).numNulls(6).numDVs(6)
        .low(1L).high(8L).fmSketch(1, 2, 3).kll(values).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = false;
    double[] tunerValues = new double[] { 0, 0.5, 0.75, 1 };
    long[] expectedDVs = new long[] { 4, 7, 8, 10 };
    for (int i = 0; i < tunerValues.length; i++) {
      aggregator.ndvTuner = tunerValues[i];
      computedStatsObj = aggregator.aggregate(statsList, partitions, true);
      expectedStats = new ColStatsBuilder<>(long.class).numNulls(6).numDVs(expectedDVs[i])
          .low(1L).high(8L).fmSketch(1, 2, 3).kll(values).build();
      assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());
    }
  }

  @Test
  public void testAggregateMultiStatsWhenOnlySomeAvailable() throws MetaException {
    List<String> partitions = Arrays.asList("part1", "part2", "part3", "part4");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(long.class).numNulls(1).numDVs(3)
        .low(1L).high(3L).hll(1, 2, 3).kll(1, 2, 3).build();
    ColumnStatisticsData data3 = new ColStatsBuilder<>(long.class).numNulls(3).numDVs(1)
        .low(7L).high(7L).hll(7).kll(7).build();
    ColumnStatisticsData data4 = new ColStatsBuilder<>(long.class).numNulls(2).numDVs(3)
        .low(3L).high(5L).hll(3, 4, 5).kll(3, 4, 5).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data3, TABLE, COL, partitions.get(2)),
        createStatsWithInfo(data4, TABLE, COL, partitions.get(3)));

    LongColumnStatsAggregator aggregator = new LongColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, false);

    // hll in case of missing stats is left as null, only numDVs is updated
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(long.class).numNulls(8).numDVs(4)
        .low(1L).high(9L).kll(1, 2, 3, 7, 3, 4, 5).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsOnlySomeAvailableButUnmergeableBitVector() throws MetaException {
    List<String> partitions = Arrays.asList("part1", "part2", "part3");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(long.class).numNulls(1).numDVs(3)
        .low(1L).high(6L).fmSketch(1, 2, 6).kll(1, 2, 6).build();
    ColumnStatisticsData data3 = new ColStatsBuilder<>(long.class).numNulls(3).numDVs(1)
        .low(7L).high(7L).hll(7).kll(7).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data3, TABLE, COL, partitions.get(2)));

    LongColumnStatsAggregator aggregator = new LongColumnStatsAggregator();

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, false);
    // hll in case of missing stats is left as null, only numDVs is updated
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(long.class).numNulls(6).numDVs(3)
        .low(1L).high(7L).kll(1, 2, 6, 7).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = true;
    computedStatsObj = aggregator.aggregate(statsList, partitions, false);
    // the use of the density function leads to a different estimation for numNDV
    expectedStats = new ColStatsBuilder<>(long.class).numNulls(6).numDVs(4)
        .low(1L).high(7L).kll(1, 2, 6, 7).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());
  }
}
