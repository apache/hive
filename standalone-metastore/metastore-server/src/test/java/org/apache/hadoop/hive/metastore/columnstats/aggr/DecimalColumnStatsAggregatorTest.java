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
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.utils.DecimalUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.assertDecimalStats;
import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.createFMSketch;
import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.createHll;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

@Category(MetastoreUnitTest.class)
public class DecimalColumnStatsAggregatorTest {

  private static final Table TABLE = new Table("dummy", "db", "hive", 0, 0,
      0, null, null, Collections.emptyMap(), null, null,
      TableType.MANAGED_TABLE.toString());
  private static final FieldSchema COL = new FieldSchema("col", "int", "");

  private static final Decimal ONE = DecimalUtils.createThriftDecimal("1.0");
  private static final Decimal TWO = DecimalUtils.createThriftDecimal("2.0");
  private static final Decimal THREE = DecimalUtils.createThriftDecimal("3.0");
  private static final Decimal FOUR = DecimalUtils.createThriftDecimal("4.0");
  private static final Decimal FIVE = DecimalUtils.createThriftDecimal("5.0");
  private static final Decimal SIX = DecimalUtils.createThriftDecimal("6.0");
  private static final Decimal SEVEN = DecimalUtils.createThriftDecimal("7.0");
  private static final Decimal EIGHT = DecimalUtils.createThriftDecimal("8.0");
  private static final Decimal NINE = DecimalUtils.createThriftDecimal("9.0");

  @Test
  public void testAggregateSingleStat() throws MetaException {
    DecimalColumnStatsAggregator aggregator = new DecimalColumnStatsAggregator();

    List<String> partitionNames = Collections.singletonList("part1");

    HyperLogLog hll = createHll(1, 3);
    ColumnStatisticsData data1 = StatisticsTestUtils.createDecimalStats(1L, 2L, ONE, FOUR, hll);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = Collections.singletonList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)));

    ColumnStatisticsObj stats = aggregator.aggregate(statsList, partitionNames, true);

    assertDecimalStats(stats, 1L, 2L, ONE, FOUR, hll);
  }

  @Test
  public void testAggregateSingleStatWhenNullValues() throws MetaException {
    DecimalColumnStatsAggregator aggregator = new DecimalColumnStatsAggregator();

    List<String> partitionNames = Collections.singletonList("part1");
    ColumnStatisticsData data1 = StatisticsTestUtils.createDecimalStats(
            1L, 2L, null, null, null);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = Collections.singletonList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)));

    ColumnStatisticsObj statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertDecimalStats(statsObj, 1L, 2L, null, null, null);

    aggregator.useDensityFunctionForNDVEstimation = true;
    statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertDecimalStats(statsObj, 1L, 2L, null, null, null);

    aggregator.useDensityFunctionForNDVEstimation = false;
    aggregator.ndvTuner = 1;
    // ndv tuner does not have any effect because min numDVs and max numDVs coincide (we have a single stats)
    statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertDecimalStats(statsObj, 1L, 2L, null, null, null);
  }

  @Test
  public void testAggregateMultipleStatsWhenSomeNullValues() throws MetaException {
    DecimalColumnStatsAggregator aggregator = new DecimalColumnStatsAggregator();

    List<String> partitionNames = Arrays.asList("part1", "part2");

    HyperLogLog hll1 = createHll(1, 2);
    ColumnStatisticsData data1 = StatisticsTestUtils.createDecimalStats(
        1L, 2L, ONE, TWO, hll1);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    ColumnStatisticsData data2 = StatisticsTestUtils.createDecimalStats(
        2L, 3L, null, null, null);
    ColumnStatistics stats2 = StatisticsTestUtils.createColStats(data2, TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)),
        new ColStatsObjWithSourceInfo(stats2.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(1)));

    ColumnStatisticsObj statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertDecimalStats(statsObj, 3L, 3L, ONE, TWO, hll1);

    aggregator.useDensityFunctionForNDVEstimation = true;
    statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertDecimalStats(statsObj, 3L, 4L, ONE, TWO, hll1);

    aggregator.useDensityFunctionForNDVEstimation = false;
    aggregator.ndvTuner = 1;
    statsObj = aggregator.aggregate(statsList, partitionNames, true);
    assertDecimalStats(statsObj, 3L, 5L, ONE, TWO, hll1);
  }

  @Test
  public void testAggregateMultiStatsWhenAllAvailable() throws MetaException {
    DecimalColumnStatsAggregator aggregator = new DecimalColumnStatsAggregator();

    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    HyperLogLog hll1 = createHll(1, 2, 3);
    ColumnStatisticsData data1 = StatisticsTestUtils.createDecimalStats(1L, 3L, ONE, THREE, hll1);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    HyperLogLog hll2 = createHll(3, 4, 5);
    ColumnStatisticsData data2 = StatisticsTestUtils.createDecimalStats(2L, 3L, THREE, FIVE, hll2);
    ColumnStatistics stats2 = StatisticsTestUtils.createColStats(data2, TABLE, COL, partitionNames.get(1));

    HyperLogLog hll3 = createHll(6, 7);
    ColumnStatisticsData data3 = StatisticsTestUtils.createDecimalStats(3L, 2L, SIX, SEVEN, hll3);
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
    assertDecimalStats(stats, 6L, 7L, ONE, SEVEN, hll1);
  }

  @Test
  public void testAggregateMultiStatsWhenUnmergeableBitVectors() throws MetaException {
    DecimalColumnStatsAggregator aggregator = new DecimalColumnStatsAggregator();

    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    FMSketch fmSketch = createFMSketch(1, 2, 3);
    ColumnStatisticsData data1 = StatisticsTestUtils.createDecimalStats(1L, 3L, ONE, THREE, fmSketch);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    HyperLogLog hll2 = createHll(3, 4, 5);
    ColumnStatisticsData data2 = StatisticsTestUtils.createDecimalStats(2L, 3L, THREE, FIVE, hll2);
    ColumnStatistics stats2 = StatisticsTestUtils.createColStats(data2, TABLE, COL, partitionNames.get(1));

    HyperLogLog hll3 = createHll(1, 2, 6, 8);
    ColumnStatisticsData data3 = StatisticsTestUtils.createDecimalStats(3L, 4L, ONE, EIGHT, hll3);
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
    assertDecimalStats(stats, 6L, 4L, ONE, EIGHT, fmSketch);

    aggregator.useDensityFunctionForNDVEstimation = true;
    stats = aggregator.aggregate(statsList, partitionNames, true);
    // the use of the density function leads to a different estimation for numNDV
    assertDecimalStats(stats, 6L, 6L, ONE, EIGHT, fmSketch);

    // here the ndv lower bound is 4 (the highest individual numDVs), the higher bound is 10 (3 + 3 + 4, that is the
    // sum of all the numDVs for all partitions), ndv tuner influences the choice between the lower bound
    // (ndvTuner = 0) and the higher bound (ndvTuner = 1), and intermediate values for ndvTuner in the range (0, 1)
    aggregator.useDensityFunctionForNDVEstimation = false;

    aggregator.ndvTuner = 0;
    stats = aggregator.aggregate(statsList, partitionNames, true);
    assertDecimalStats(stats, 6L, 4L, ONE, EIGHT, fmSketch);

    aggregator.ndvTuner = 0.5;
    stats = aggregator.aggregate(statsList, partitionNames, true);
    assertDecimalStats(stats, 6L, 7L, ONE, EIGHT, fmSketch);

    aggregator.ndvTuner = 0.75;
    stats = aggregator.aggregate(statsList, partitionNames, true);
    assertDecimalStats(stats, 6L, 8L, ONE, EIGHT, fmSketch);

    aggregator.ndvTuner = 1;
    stats = aggregator.aggregate(statsList, partitionNames, true);
    assertDecimalStats(stats, 6L, 10L, ONE, EIGHT, fmSketch);
  }

  @Test
  public void testAggregateMultiStatsWhenOnlySomeAvailable() throws MetaException {
    DecimalColumnStatsAggregator aggregator = new DecimalColumnStatsAggregator();

    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    HyperLogLog hll1 = createHll(1, 2, 3);
    ColumnStatisticsData data1 = StatisticsTestUtils.createDecimalStats(1L, 3L, ONE, THREE, hll1);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    HyperLogLog hll3 = createHll(7);
    ColumnStatisticsData data3 = StatisticsTestUtils.createDecimalStats(3L, 1L, SEVEN, SEVEN, hll3);
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(data3, TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)),
        new ColStatsObjWithSourceInfo(stats3.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(2)));

    ColumnStatisticsObj stats = aggregator.aggregate(statsList, partitionNames, false);

    // hll in case of missing stats is left as null, only numDVs is updated
    assertDecimalStats(stats, 6L, 3L, ONE, NINE, null);
  }
  
  @Test
  public void testAggregateMultiStatsOnlySomeAvailableButUnmergeableBitVector() throws MetaException {
    DecimalColumnStatsAggregator aggregator = new DecimalColumnStatsAggregator();
    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    FMSketch fmSketch = createFMSketch(1, 2, 6);
    ColumnStatisticsData data1 = StatisticsTestUtils.createDecimalStats(
        1L, 3L, ONE, SIX, fmSketch);
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    HyperLogLog hll3 = createHll(7);
    ColumnStatisticsData data3 = StatisticsTestUtils.createDecimalStats(
        3L, 1L, SEVEN, SEVEN, hll3);
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(data3, TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        new ColStatsObjWithSourceInfo(stats1.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(0)),
        new ColStatsObjWithSourceInfo(stats3.getStatsObj().get(0), DEFAULT_CATALOG_NAME, TABLE.getDbName(),
            TABLE.getTableName(), partitionNames.get(2)));

    ColumnStatisticsObj stats = aggregator.aggregate(statsList, partitionNames, false);
    // hll in case of missing stats is left as null, only numDVs is updated
    assertDecimalStats(stats, 6L, 3L, ONE, DecimalUtils.createThriftDecimal("7.5"), null);

    aggregator.useDensityFunctionForNDVEstimation = true;
    stats = aggregator.aggregate(statsList, partitionNames, true);
    // the use of the density function leads to a different estimation for numNDV
    assertDecimalStats(stats, 6L, 4L, ONE, DecimalUtils.createThriftDecimal("7.5"), null);
  }
}
