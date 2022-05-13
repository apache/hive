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

import org.apache.hadoop.hive.common.ndv.fm.FMSketch;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
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

import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.assertStringStats;
import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.createFMSketch;
import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.createHll;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

@Category(MetastoreUnitTest.class)
public class StringColumnStatsAggregatorTest {

  private static final Table TABLE = new Table("dummy", "db", "hive", 0, 0,
      0, null, null, Collections.emptyMap(), null, null,
      TableType.MANAGED_TABLE.toString());
  private static final FieldSchema COL = new FieldSchema("col", "int", "");

  private static final String S_1 = "test";
  private static final String S_2 = "try";
  private static final String S_3 = "longer string";
  private static final String S_4 = "even longer string";
  private static final String S_5 = "some string";
  private static final String S_6 = "some other string";
  private static final String S_7 = "yet another string";

  @Test
  public void testAggregateSingleStat() throws MetaException {
    StringColumnStatsAggregator aggregator = new StringColumnStatsAggregator();
    List<String> partitionNames = Collections.singletonList("part1");

    HyperLogLog hll = createHll(S_1, S_3);
    ColumnStatisticsData data1 = StatisticsTestUtils.createStringStats(1L, 2L, 8.5, 13L, hll);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = Collections.singletonList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)));

    ColumnStatisticsObj stats = aggregator.aggregate(statsList, partitionNames, true);
    assertStringStats(stats, 1L, 2L, 8.5, 13L, hll);
  }

  @Test
  public void testAggregateSingleStatWhenNullValues() throws MetaException {
    StringColumnStatsAggregator aggregator = new StringColumnStatsAggregator();

    List<String> partitionNames = Collections.singletonList("part1");
    ColumnStatisticsData data1 = StatisticsTestUtils.createStringStats(
            1L, 2L, null, null, null);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = Collections.singletonList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)));

    ColumnStatisticsObj statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertStringStats(statsObj, 1L, 2L, null, null, null);

    aggregator.useDensityFunctionForNDVEstimation = true;
    statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertStringStats(statsObj, 1L, 2L, null, null, null);

    aggregator.useDensityFunctionForNDVEstimation = false;
    aggregator.ndvTuner = 1;
    // ndv tuner does not have any effect because min numDVs and max numDVs coincide (we have a single stats)
    statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertStringStats(statsObj, 1L, 2L, null, null, null);
  }

  @Test
  public void testAggregateMultipleStatsWhenSomeNullValues() throws MetaException {
    StringColumnStatsAggregator aggregator = new StringColumnStatsAggregator();

    List<String> partitionNames = Arrays.asList("part1", "part2");

    HyperLogLog hll1 = createHll(S_1, S_2);
    ColumnStatisticsData data1 = StatisticsTestUtils.createStringStats(
        1L, 2L, 3.0, 4L, hll1);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    ColumnStatisticsData data2 = StatisticsTestUtils.createStringStats(
        2L, 3L, null, null, null);
    ColumnStatistics stats2 = StatisticsTestUtils.createColStats(data2, TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)),
        new ColStatsObjWithSourceInfo(stats2.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(1)));

    ColumnStatisticsObj statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertStringStats(statsObj, 3L, 3L, 3.0, 4L, hll1);

    // both useDensityFunctionForNDVEstimation and ndvTuner are ignored by StringColumnStatsAggregator
    aggregator.useDensityFunctionForNDVEstimation = true;
    statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertStringStats(statsObj, 3L, 3L, 3.0, 4L, hll1);

    aggregator.useDensityFunctionForNDVEstimation = false;
    aggregator.ndvTuner = 1;
    statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertStringStats(statsObj, 3L, 3L, 3.0, 4L, hll1);
  }

  @Test
  public void testAggregateMultiStatsWhenAllAvailable() throws MetaException {
    StringColumnStatsAggregator aggregator = new StringColumnStatsAggregator();

    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    HyperLogLog hll1 = createHll(S_1, S_2, S_3);
    ColumnStatisticsData data1 = StatisticsTestUtils.createStringStats(
        1L, 3L, 20.0 / 3, 13L, hll1);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    HyperLogLog hll2 = createHll(S_3, S_4, S_5);
    ColumnStatisticsData data2 = StatisticsTestUtils.createStringStats(
        2L, 3L, 14.0, 18L, hll2);
    ColumnStatistics stats2 = StatisticsTestUtils.createColStats(data2, TABLE, COL, partitionNames.get(1));

    HyperLogLog hll3 = createHll(S_6, S_7);
    ColumnStatisticsData data3 = StatisticsTestUtils.createStringStats(
        3L, 2L, 17.5, 18L, hll3);
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(data3, TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)),
        new ColStatsObjWithSourceInfo(stats2.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(1)),
        new ColStatsObjWithSourceInfo(stats3.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(2)));

    ColumnStatisticsObj stats = aggregator.aggregate(statsList, partitionNames, true);
    // the aggregation does not update hll, only numNDVs is, it keeps the first hll
    assertStringStats(stats, 6L, 7L, 17.5, 18L, hll1);
  }

  @Test
  public void testAggregateMultiStatsWhenUnmergeableBitVectors() throws MetaException {
    StringColumnStatsAggregator aggregator = new StringColumnStatsAggregator();

    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    FMSketch fmSketch = createFMSketch(S_1, S_2, S_3);
    ColumnStatisticsData data1 = StatisticsTestUtils.createStringStats(
        1L, 3L, 20.0 / 3, 13L, fmSketch);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    HyperLogLog hll2 = createHll(S_3, S_4, S_5);
    ColumnStatisticsData data2 = StatisticsTestUtils.createStringStats(
        2L, 3L, 14.0, 18L, hll2);
    ColumnStatistics stats2 = StatisticsTestUtils.createColStats(data2, TABLE, COL, partitionNames.get(1));

    HyperLogLog hll3 = createHll(S_6, S_7);
    ColumnStatisticsData data3 = StatisticsTestUtils.createStringStats(
        3L, 2L, 17.5, 18L, hll3);
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(data3, TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)),
        new ColStatsObjWithSourceInfo(stats2.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(1)),
        new ColStatsObjWithSourceInfo(stats3.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(2)));

    ColumnStatisticsObj stats = aggregator.aggregate(statsList, partitionNames, true);
    // the aggregation does not update the bitvector, only numDVs is, it keeps the first bitvector;
    // numDVs is set to the maximum among all stats when non-mergeable bitvectors are detected
    assertStringStats(stats, 6L, 3L, 17.5, 18L, fmSketch);

    // both useDensityFunctionForNDVEstimation and ndvTuner are ignored by StringColumnStatsAggregator
    aggregator.useDensityFunctionForNDVEstimation = true;
    stats = aggregator.aggregate(statsList, partitionNames, true);
    assertStringStats(stats, 6L, 3L, 17.5, 18L, fmSketch);

    aggregator.useDensityFunctionForNDVEstimation = false;
    aggregator.ndvTuner = 0;
    stats = aggregator.aggregate(statsList, partitionNames, true);
    assertStringStats(stats, 6L, 3L, 17.5, 18L, fmSketch);

    aggregator.ndvTuner = 0.5;
    stats = aggregator.aggregate(statsList, partitionNames, true);
    assertStringStats(stats, 6L, 3L, 17.5, 18L, fmSketch);

    aggregator.ndvTuner = 0.75;
    stats = aggregator.aggregate(statsList, partitionNames, true);
    assertStringStats(stats, 6L, 3L, 17.5, 18L, fmSketch);

    aggregator.ndvTuner = 1;
    stats = aggregator.aggregate(statsList, partitionNames, true);
    assertStringStats(stats, 6L, 3L, 17.5, 18L, fmSketch);
  }

  @Test
  public void testAggregateMultiStatsWhenOnlySomeAvailable() throws MetaException {
    StringColumnStatsAggregator aggregator = new StringColumnStatsAggregator();
    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    HyperLogLog hll1 = createHll(S_1, S_2, S_3);
    ColumnStatisticsData data1 = StatisticsTestUtils.createStringStats(
        1L, 3L, 20.0 / 3, 13L, hll1);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    HyperLogLog hll3 = createHll(S_6, S_7);
    ColumnStatisticsData data3 = StatisticsTestUtils.createStringStats(
        3L, 2L, 17.5, 18L, hll3);
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(data3, TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)),
        new ColStatsObjWithSourceInfo(stats3.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(2)));

    ColumnStatisticsObj stats = aggregator.aggregate(statsList, partitionNames, false);
    // hll in case of missing stats is left as null, only numDVs is updated
    assertStringStats(stats, 6L, 3L, 22.916666666666668, 22L, null);
  }

  @Test
  public void testAggregateMultiStatsOnlySomeAvailableButUnmergeableBitVector() throws MetaException {
    StringColumnStatsAggregator aggregator = new StringColumnStatsAggregator();
    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    FMSketch fmSketch = createFMSketch(S_1, S_2, S_3);
    ColumnStatisticsData data1 = StatisticsTestUtils.createStringStats(
        1L, 3L, 20.0 / 3, 13L, fmSketch);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    HyperLogLog hll3 = createHll(S_6, S_7);
    ColumnStatisticsData data3 = StatisticsTestUtils.createStringStats(
        3L, 2L, 17.5, 18L, hll3);
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(data3, TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)),
        new ColStatsObjWithSourceInfo(stats3.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(2)));

    ColumnStatisticsObj stats = aggregator.aggregate(statsList, partitionNames, false);
    // hll in case of missing stats is left as null, only numDVs is updated
    assertStringStats(stats, 6L, 3L, 22.916666666666668, 22L, null);

    // both useDensityFunctionForNDVEstimation and ndvTuner are ignored by StringColumnStatsAggregator
    aggregator.useDensityFunctionForNDVEstimation = true;
    stats = aggregator.aggregate(statsList, partitionNames, true);
    assertStringStats(stats, 6L, 3L, 22.916666666666668, 22L, null);
  }
}
