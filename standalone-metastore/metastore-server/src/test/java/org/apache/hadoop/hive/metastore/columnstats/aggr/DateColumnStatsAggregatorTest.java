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

import com.google.common.primitives.Longs;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
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
public class DateColumnStatsAggregatorTest {

  private static final Table TABLE = new Table("dummy", "db", "hive", 0, 0,
      0, null, null, Collections.emptyMap(), null, null,
      TableType.MANAGED_TABLE.toString());
  private static final FieldSchema COL = new FieldSchema("col", "date", "");

  private static final Date DATE_1 = new Date(1);
  private static final Date DATE_2 = new Date(2);
  private static final Date DATE_3 = new Date(3);
  private static final Date DATE_4 = new Date(4);
  private static final Date DATE_5 = new Date(5);
  private static final Date DATE_6 = new Date(6);
  private static final Date DATE_7 = new Date(7);
  private static final Date DATE_8 = new Date(8);
  private static final Date DATE_9 = new Date(9);

  @Test
  public void testAggregateSingleStat() throws MetaException {
    List<String> partitions = Collections.singletonList("part1");

    long[] values = { DATE_1.getDaysSinceEpoch(), DATE_4.getDaysSinceEpoch() };
    ColumnStatisticsData data1 = new ColStatsBuilder<>(Date.class).numNulls(1).numDVs(2).low(DATE_1).high(DATE_4)
        .hll(values).kll(values).build();
    List<ColStatsObjWithSourceInfo> statsList =
        Collections.singletonList(createStatsWithInfo(data1, TABLE, COL, partitions.get(0)));

    DateColumnStatsAggregator aggregator = new DateColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, true);

    assertEqualStatistics(data1, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateSingleStatWhenNullValues() throws MetaException {
    List<String> partitions = Collections.singletonList("part1");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(Date.class).numNulls(1).numDVs(2).build();
    List<ColStatsObjWithSourceInfo> statsList =
        Collections.singletonList(createStatsWithInfo(data1, TABLE, COL, partitions.get(0)));

    DateColumnStatsAggregator aggregator = new DateColumnStatsAggregator();
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

    long[] values1 = { DATE_1.getDaysSinceEpoch(), DATE_2.getDaysSinceEpoch() };
    ColumnStatisticsData data1 = new ColStatsBuilder<>(Date.class).numNulls(1).numDVs(2)
        .low(DATE_1).high(DATE_2).hll(values1).kll(values1).build();
    ColumnStatisticsData data2 = new ColStatsBuilder<>(Date.class).numNulls(2).numDVs(3).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data2, TABLE, COL, partitions.get(1)));

    DateColumnStatsAggregator aggregator = new DateColumnStatsAggregator();

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(Date.class).numNulls(3).numDVs(3)
        .low(DATE_1).high(DATE_2).hll(values1).kll(values1).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = true;
    computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    expectedStats = new ColStatsBuilder<>(Date.class).numNulls(3).numDVs(4)
        .low(DATE_1).high(DATE_2).hll(values1).kll(values1).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = false;
    aggregator.ndvTuner = 1;
    computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    expectedStats = new ColStatsBuilder<>(Date.class).numNulls(3).numDVs(5)
        .low(DATE_1).high(DATE_2).hll(values1).kll(values1).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsWhenAllAvailable() throws MetaException {
    List<String> partitions = Arrays.asList("part1", "part2", "part3");

    long[] values1 = { DATE_1.getDaysSinceEpoch(), DATE_2.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch() };
    ColumnStatisticsData data1 = new ColStatsBuilder<>(Date.class).numNulls(1).numDVs(3)
        .low(DATE_1).high(DATE_3).hll(values1).kll(values1).build();

    long[] values2 = { DATE_3.getDaysSinceEpoch(), DATE_4.getDaysSinceEpoch(), DATE_5.getDaysSinceEpoch() };
    ColumnStatisticsData data2 = new ColStatsBuilder<>(Date.class).numNulls(2).numDVs(3)
        .low(DATE_3).high(DATE_5).hll(values2).kll(values2).build();

    long[] values3 = { DATE_6.getDaysSinceEpoch(), DATE_7.getDaysSinceEpoch() };
    ColumnStatisticsData data3 = new ColStatsBuilder<>(Date.class).numNulls(3).numDVs(2)
        .low(DATE_6).high(DATE_7).hll(values3).kll(values3).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data2, TABLE, COL, partitions.get(1)),
        createStatsWithInfo(data3, TABLE, COL, partitions.get(2)));

    DateColumnStatsAggregator aggregator = new DateColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, true);

    // the aggregation does not update hll, only numDVs is, it keeps the first hll
    // notice that numDVs is computed by using HLL, it can detect that 'DATE_3' appears twice
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(Date.class).numNulls(6).numDVs(7)
        .low(DATE_1).high(DATE_7).hll(values1).kll(Longs.concat(values1, values2, values3)).build();

    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsWhenUnmergeableBitVectors() throws MetaException {
    List<String> partitions = Arrays.asList("part1", "part2", "part3");

    long[] values1 = { DATE_1.getDaysSinceEpoch(), DATE_2.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch() };
    ColumnStatisticsData data1 = new ColStatsBuilder<>(Date.class).numNulls(1).numDVs(3)
        .low(DATE_1).high(DATE_3).fmSketch(values1).kll(values1).build();
    long[] values2 = { DATE_3.getDaysSinceEpoch(), DATE_4.getDaysSinceEpoch(), DATE_5.getDaysSinceEpoch() };
    ColumnStatisticsData data2 = new ColStatsBuilder<>(Date.class).numNulls(2).numDVs(3)
        .low(DATE_3).high(DATE_5).hll(values2).kll(values2).build();
    long[] values3 = { DATE_1.getDaysSinceEpoch(), DATE_2.getDaysSinceEpoch(), DATE_6.getDaysSinceEpoch(),
        DATE_8.getDaysSinceEpoch() };
    ColumnStatisticsData data3 = new ColStatsBuilder<>(Date.class).numNulls(3).numDVs(4)
        .low(DATE_1).high(DATE_8).hll(values3).kll(values3).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data2, TABLE, COL, partitions.get(1)),
        createStatsWithInfo(data3, TABLE, COL, partitions.get(2)));

    DateColumnStatsAggregator aggregator = new DateColumnStatsAggregator();
    long[] values = Longs.concat(values1, values2, values3);

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    // the aggregation does not update the bitvector, only numDVs is, it keeps the first bitvector;
    // numDVs is set to the maximum among all stats when non-mergeable bitvectors are detected
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(Date.class).numNulls(6).numDVs(4)
        .low(DATE_1).high(DATE_8).fmSketch(values1).kll(values).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = true;
    computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    // the use of the density function leads to a different estimation for numNDV
    expectedStats = new ColStatsBuilder<>(Date.class).numNulls(6).numDVs(6)
        .low(DATE_1).high(DATE_8).fmSketch(values1).kll(values).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = false;
    double[] tunerValues = new double[] { 0, 0.5, 0.75, 1 };
    long[] expectedNDVs = new long[] { 4, 7, 8, 10 };
    for (int i = 0; i < tunerValues.length; i++) {
      aggregator.ndvTuner = tunerValues[i];
      computedStatsObj = aggregator.aggregate(statsList, partitions, true);
      expectedStats = new ColStatsBuilder<>(Date.class).numNulls(6).numDVs(expectedNDVs[i])
          .low(DATE_1).high(DATE_8).fmSketch(values1).kll(values).build();
      assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());
    }
  }

  @Test
  public void testAggregateMultiStatsWhenOnlySomeAvailable() throws MetaException {
    List<String> partitions = Arrays.asList("part1", "part2", "part3", "part4");

    long[] values1 = { DATE_1.getDaysSinceEpoch(), DATE_2.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch() };
    ColumnStatisticsData data1 = new ColStatsBuilder<>(Date.class).numNulls(1).numDVs(3)
        .low(DATE_1).high(DATE_3).hll(values1).kll(values1).build();

    long[] values3 = { DATE_7.getDaysSinceEpoch() };
    ColumnStatisticsData data3 = new ColStatsBuilder<>(Date.class).numNulls(3).numDVs(1).low(DATE_7).high(DATE_7)
        .hll(DATE_7.getDaysSinceEpoch()).kll(values3).build();

    long[] values4 = { DATE_3.getDaysSinceEpoch(), DATE_4.getDaysSinceEpoch(), DATE_5.getDaysSinceEpoch() };
    ColumnStatisticsData data4 = new ColStatsBuilder<>(Date.class).numNulls(2).numDVs(3)
        .low(DATE_3).high(DATE_5).hll(values4).kll(values4).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data3, TABLE, COL, partitions.get(2)),
        createStatsWithInfo(data4, TABLE, COL, partitions.get(3)));

    DateColumnStatsAggregator aggregator = new DateColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, false);

    // hll in case of missing stats is left as null, only numDVs is updated
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(Date.class).numNulls(8).numDVs(4)
        .low(DATE_1).high(DATE_9).kll(Longs.concat(values1, values3, values4)).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsOnlySomeAvailableButUnmergeableBitVector() throws MetaException {
    List<String> partitions = Arrays.asList("part1", "part2", "part3");

    long[] values1 = { DATE_1.getDaysSinceEpoch(), DATE_2.getDaysSinceEpoch(), DATE_6.getDaysSinceEpoch() };
    ColumnStatisticsData data1 = new ColStatsBuilder<>(Date.class).numNulls(1).numDVs(3)
        .low(DATE_1).high(DATE_6).fmSketch(values1).kll(values1).build();

    long[] values3 = { DATE_7.getDaysSinceEpoch() };
    ColumnStatisticsData data3 = new ColStatsBuilder<>(Date.class).numNulls(3).numDVs(1)
        .low(DATE_7).high(DATE_7).hll(DATE_7.getDaysSinceEpoch()).kll(values3).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data3, TABLE, COL, partitions.get(2)));

    DateColumnStatsAggregator aggregator = new DateColumnStatsAggregator();

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, false);
    // hll in case of missing stats is left as null, only numDVs is updated
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(Date.class).numNulls(6).numDVs(3)
        .low(DATE_1).high(DATE_7).kll(Longs.concat(values1, values3)).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = true;
    computedStatsObj = aggregator.aggregate(statsList, partitions, false);
    // the use of the density function leads to a different estimation for numNDV
    expectedStats = new ColStatsBuilder<>(Date.class).numNulls(6).numDVs(4)
        .low(DATE_1).high(DATE_7).kll(Longs.concat(values1, values3)).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());
  }
}
