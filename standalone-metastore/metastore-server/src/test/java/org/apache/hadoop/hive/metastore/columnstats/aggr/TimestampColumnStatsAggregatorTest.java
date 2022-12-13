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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Timestamp;
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
public class TimestampColumnStatsAggregatorTest {

  private static final Table TABLE = new Table("dummy", "db", "hive", 0, 0,
      0, null, null, Collections.emptyMap(), null, null,
      TableType.MANAGED_TABLE.toString());
  private static final FieldSchema COL = new FieldSchema("col", "timestamp", "");

  private static final Timestamp TS_1 = new Timestamp(1);
  private static final Timestamp TS_2 = new Timestamp(2);
  private static final Timestamp TS_3 = new Timestamp(3);
  private static final Timestamp TS_4 = new Timestamp(4);
  private static final Timestamp TS_5 = new Timestamp(5);
  private static final Timestamp TS_6 = new Timestamp(6);
  private static final Timestamp TS_7 = new Timestamp(7);
  private static final Timestamp TS_8 = new Timestamp(8);
  private static final Timestamp TS_9 = new Timestamp(9);

  @Test
  public void testAggregateSingleStat() throws MetaException {
    List<String> partitions = Collections.singletonList("part1");

    long[] values = { TS_1.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch() };
    ColumnStatisticsData data1 = new ColStatsBuilder<>(Timestamp.class).numNulls(1).numDVs(2).low(TS_1)
        .high(TS_3).hll(values).kll(values).build();
    List<ColStatsObjWithSourceInfo> statsList =
        Collections.singletonList(createStatsWithInfo(data1, TABLE, COL, partitions.get(0)));

    TimestampColumnStatsAggregator aggregator = new TimestampColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, true);

    assertEqualStatistics(data1, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateSingleStatWhenNullValues() throws MetaException {
    List<String> partitions = Collections.singletonList("part1");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(Timestamp.class).numNulls(1).numDVs(2).build();
    List<ColStatsObjWithSourceInfo> statsList =
        Collections.singletonList(createStatsWithInfo(data1, TABLE, COL, partitions.get(0)));

    TimestampColumnStatsAggregator aggregator = new TimestampColumnStatsAggregator();

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

    long[] values1 = { TS_1.getSecondsSinceEpoch(), TS_2.getSecondsSinceEpoch() };
    ColumnStatisticsData data1 = new ColStatsBuilder<>(Timestamp.class).numNulls(1).numDVs(2)
        .low(TS_1).high(TS_2).hll(values1).kll(values1).build();
    ColumnStatisticsData data2 = new ColStatsBuilder<>(Timestamp.class).numNulls(2).numDVs(3).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data2, TABLE, COL, partitions.get(1)));

    TimestampColumnStatsAggregator aggregator = new TimestampColumnStatsAggregator();

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(Timestamp.class).numNulls(3).numDVs(3)
        .low(TS_1).high(TS_2).hll(values1).kll(values1).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = true;
    computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    expectedStats = new ColStatsBuilder<>(Timestamp.class).numNulls(3).numDVs(4)
        .low(TS_1).high(TS_2).hll(values1).kll(values1).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = false;
    aggregator.ndvTuner = 1;
    computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    expectedStats = new ColStatsBuilder<>(Timestamp.class).numNulls(3).numDVs(5)
        .low(TS_1).high(TS_2).hll(values1).kll(values1).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsWhenAllAvailable() throws MetaException {
    List<String> partitions = Arrays.asList("part1", "part2", "part3");

    long[] values1 = { TS_1.getSecondsSinceEpoch(), TS_2.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch() };
    ColumnStatisticsData data1 = new ColStatsBuilder<>(Timestamp.class).numNulls(1).numDVs(2)
        .low(TS_1).high(TS_3).hll(values1).kll(values1).build();

    long[] values2 = { TS_3.getSecondsSinceEpoch(), TS_4.getSecondsSinceEpoch(), TS_5.getSecondsSinceEpoch() };
    ColumnStatisticsData data2 = new ColStatsBuilder<>(Timestamp.class).numNulls(2).numDVs(3)
        .low(TS_3).high(TS_5).hll(values2).kll(values1).build();

    long[] values3 = { TS_6.getSecondsSinceEpoch(), TS_7.getSecondsSinceEpoch() };
    ColumnStatisticsData data3 = new ColStatsBuilder<>(Timestamp.class).numNulls(3).numDVs(2)
        .low(TS_6).high(TS_7).hll(values3).kll(values3).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data2, TABLE, COL, partitions.get(1)),
        createStatsWithInfo(data3, TABLE, COL, partitions.get(2)));

    TimestampColumnStatsAggregator aggregator = new TimestampColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, true);

    // the aggregation does not update hll, only numDVs is, it keeps the first hll
    // notice that numDVs is computed by using HLL, it can detect that 'TS_3' appears twice
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(Timestamp.class).numNulls(6).numDVs(7)
        .low(TS_1).high(TS_7).hll(values1).kll(Longs.concat(values1, values2, values3)).build();

    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsWhenUnmergeableBitVectors() throws MetaException {
    List<String> partitions = Arrays.asList("part1", "part2", "part3");

    long[] values1 = { TS_1.getSecondsSinceEpoch(), TS_2.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch() };
    ColumnStatisticsData data1 = new ColStatsBuilder<>(Timestamp.class).numNulls(1).numDVs(3)
        .low(TS_1).high(TS_3).fmSketch(values1).kll(values1).build();

    long[] values2 = { TS_3.getSecondsSinceEpoch(), TS_4.getSecondsSinceEpoch(), TS_5.getSecondsSinceEpoch() };
    ColumnStatisticsData data2 = new ColStatsBuilder<>(Timestamp.class).numNulls(2).numDVs(3).low(TS_3).high(TS_5)
        .hll(values2).kll(values2).build();

    long[] values3 = { TS_1.getSecondsSinceEpoch(), TS_2.getSecondsSinceEpoch(), TS_6.getSecondsSinceEpoch(),
        TS_8.getSecondsSinceEpoch() };
    ColumnStatisticsData data3 = new ColStatsBuilder<>(Timestamp.class).numNulls(3).numDVs(4).low(TS_1)
        .high(TS_8).hll(values3).kll(values3).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data2, TABLE, COL, partitions.get(1)),
        createStatsWithInfo(data3, TABLE, COL, partitions.get(2)));

    TimestampColumnStatsAggregator aggregator = new TimestampColumnStatsAggregator();

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    // the aggregation does not update the bitvector, only numDVs is, it keeps the first bitvector;
    // numDVs is set to the maximum among all stats when non-mergeable bitvectors are detected
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(Timestamp.class).numNulls(6).numDVs(4).low(TS_1)
        .high(TS_8).fmSketch(values1).kll(Longs.concat(values1, values2, values3)).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = true;
    computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    // the use of the density function leads to a different estimation for numNDV
    expectedStats = new ColStatsBuilder<>(Timestamp.class).numNulls(6).numDVs(6).low(TS_1).high(TS_8)
        .fmSketch(values1).kll(Longs.concat(values1, values2, values3)).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = false;
    double[] tunerValues = new double[] { 0, 0.5, 0.75, 1 };
    long[] expectedDVs = new long[] { 4, 7, 8, 10 };
    for (int i = 0; i < tunerValues.length; i++) {
      aggregator.ndvTuner = tunerValues[i];
      computedStatsObj = aggregator.aggregate(statsList, partitions, true);
      expectedStats = new ColStatsBuilder<>(Timestamp.class).numNulls(6).numDVs(expectedDVs[i])
          .low(TS_1).high(TS_8).fmSketch(values1).kll(Longs.concat(values1, values2, values3))
          .build();
      assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());
    }
  }

  @Test
  public void testAggregateMultiStatsWhenOnlySomeAvailable() throws MetaException {
    List<String> partitions = Arrays.asList("part1", "part2", "part3", "part4");

    long[] values1 = { TS_1.getSecondsSinceEpoch(), TS_2.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch() };
    ColumnStatisticsData data1 = new ColStatsBuilder<>(Timestamp.class).numNulls(1).numDVs(3)
        .low(TS_1).high(TS_3).hll(values1).kll(values1).build();

    long[] values3 = { TS_7.getSecondsSinceEpoch() };
    ColumnStatisticsData data3 = new ColStatsBuilder<>(Timestamp.class).numNulls(3).numDVs(1)
        .low(TS_7).high(TS_7).hll(values3).kll(values3).build();

    long[] values4 = { TS_3.getSecondsSinceEpoch(), TS_4.getSecondsSinceEpoch(), TS_5.getSecondsSinceEpoch() };
    ColumnStatisticsData data4 = new ColStatsBuilder<>(Timestamp.class).numNulls(2).numDVs(3).low(TS_3).high(TS_5)
        .hll(values4).kll(values4).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data3, TABLE, COL, partitions.get(2)),
        createStatsWithInfo(data4, TABLE, COL, partitions.get(3)));

    TimestampColumnStatsAggregator aggregator = new TimestampColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, false);

    // hll in case of missing stats is left as null, only numDVs is updated
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(Timestamp.class).numNulls(8).numDVs(4).low(TS_1)
        .high(TS_9).kll(Longs.concat(values1, values3, values4)).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsOnlySomeAvailableButUnmergeableBitVector() throws MetaException {
    List<String> partitions = Arrays.asList("part1", "part2", "part3");

    long[] values1 = { TS_1.getSecondsSinceEpoch(), TS_2.getSecondsSinceEpoch(), TS_6.getSecondsSinceEpoch() };
    ColumnStatisticsData data1 = new ColStatsBuilder<>(Timestamp.class).numNulls(1).numDVs(3)
        .low(TS_1).high(TS_6).hll(values1).kll(values1).build();

    long[] values3 = { TS_7.getSecondsSinceEpoch() };
    ColumnStatisticsData data3 = new ColStatsBuilder<>(Timestamp.class).numNulls(3).numDVs(1)
        .low(TS_7).high(TS_7).hll(values3).kll(values3).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data3, TABLE, COL, partitions.get(2)));

    TimestampColumnStatsAggregator aggregator = new TimestampColumnStatsAggregator();

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, false);
    // hll in case of missing stats is left as null, only numDVs is updated
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(Timestamp.class).numNulls(6).numDVs(3).low(TS_1)
        .high(TS_7).kll(Longs.concat(values1, values3)).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());

    aggregator.useDensityFunctionForNDVEstimation = true;
    computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    // the use of the density function leads to a different estimation for numNDV
    expectedStats = new ColStatsBuilder<>(Timestamp.class).numNulls(6).numDVs(4).low(TS_1)
        .high(TS_7).kll(Longs.concat(values1, values3)).build();
    assertEqualStatistics(expectedStats, computedStatsObj.getStatsData());
  }
}
