/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.columnstats.aggr;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Timestamp;
import org.apache.hadoop.hive.metastore.api.utils.DecimalUtils;
import org.apache.hadoop.hive.metastore.columnstats.ColStatsBuilder;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.createStatsWithInfo;

@Category(MetastoreUnitTest.class)
@RunWith(Parameterized.class)
public class ColumnStatsAggregatorTest {

  private static class Args {
    final String testMnemo;
    final List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> stats;
    final List<String> partitions;
    final ColumnStatsAggregator aggregator;
    final ColumnStatisticsData expected;

    Args(String testMnemo, List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> stats, List<String> partitions,
        ColumnStatsAggregator aggregator, ColumnStatisticsData expected) {
      this.testMnemo = testMnemo;
      this.stats = stats;
      this.partitions = partitions;
      this.aggregator = aggregator;
      this.expected = expected;
    }

    @Override
    public String toString() {
      String statString = stats.stream().map(s -> s.getColStatsObj().toString()).collect(Collectors.joining(","));
      return "Args{" + "test=" + testMnemo + ", stats=" + statString + ", partitions=" + partitions +
          ", aggregator=" + aggregator.getClass().getSimpleName() + ", expected=" + expected + '}';
    }
  }

  private static final Table TABLE =
      new Table("dummy", "db", "hive", 0, 0, 0, null, null, Collections.emptyMap(), null, null,
          TableType.MANAGED_TABLE.toString());
  private static final FieldSchema BINARY_COL = new FieldSchema("col", "binary", "");
  private static final FieldSchema BOOLEAN_COL = new FieldSchema("col", "boolean", "");
  private static final FieldSchema LONG_COL = new FieldSchema("col", "long", "");
  private static final FieldSchema DATE_COL = new FieldSchema("col", "date", "");
  private static final FieldSchema DECIMAL_COL = new FieldSchema("col", "decimal", "");
  private static final FieldSchema DOUBLE_COL = new FieldSchema("col", "double", "");
  private static final FieldSchema STRING_COL = new FieldSchema("col", "string", "");
  private static final FieldSchema TIMESTAMP_COL = new FieldSchema("col", "timestamp", "");

  private static final Date DATE_1 = new Date(1);
  private static final Date DATE_2 = new Date(2);
  private static final Date DATE_3 = new Date(3);
  private static final Date DATE_4 = new Date(4);
  private static final Date DATE_5 = new Date(5);
  private static final Date DATE_6 = new Date(6);
  private static final Date DATE_7 = new Date(7);
  private static final Date DATE_8 = new Date(8);
  private static final Date DATE_9 = new Date(9);

  private static final String S_1 = "test";
  private static final String S_2 = "try";
  private static final String S_3 = "longer string";
  private static final String S_4 = "even longer string";
  private static final String S_5 = "some string";
  private static final String S_6 = "some other string";
  private static final String S_7 = "yet another string";

  private static final Timestamp TS_1 = new Timestamp(1);
  private static final Timestamp TS_2 = new Timestamp(2);
  private static final Timestamp TS_3 = new Timestamp(3);
  private static final Timestamp TS_4 = new Timestamp(4);
  private static final Timestamp TS_5 = new Timestamp(5);
  private static final Timestamp TS_6 = new Timestamp(6);
  private static final Timestamp TS_7 = new Timestamp(7);
  private static final Timestamp TS_8 = new Timestamp(8);
  private static final Timestamp TS_9 = new Timestamp(9);

  private static final Decimal ONE = DecimalUtils.createThriftDecimal("1.0");
  private static final Decimal TWO = DecimalUtils.createThriftDecimal("2.0");
  private static final Decimal THREE = DecimalUtils.createThriftDecimal("3.0");
  private static final Decimal FOUR = DecimalUtils.createThriftDecimal("4.0");
  private static final Decimal FIVE = DecimalUtils.createThriftDecimal("5.0");
  private static final Decimal SIX = DecimalUtils.createThriftDecimal("6.0");
  private static final Decimal SEVEN = DecimalUtils.createThriftDecimal("7.0");
  private static final Decimal EIGHT = DecimalUtils.createThriftDecimal("8.0");

  private final Args arg;

  public ColumnStatsAggregatorTest(Args arg) {
    this.arg = arg;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<?> data() {
    final List<Args> parameters = new ArrayList<>();
    final List<String> partitions = Arrays.asList("part1", "part2", "part3", "part4");
    ColumnStatisticsData data;
    ColumnStatisticsData expectedStats;
    List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> statsList;
    String testMnemo;

    BinaryColumnStatsAggregator binaryColumnStatsAggregator;
    BooleanColumnStatsAggregator booleanColumnStatsAggregator;
    DateColumnStatsAggregator dateColumnStatsAggregator;
    DoubleColumnStatsAggregator doubleColumnStatsAggregator;
    DecimalColumnStatsAggregator decimalColumnStatsAggregator;
    LongColumnStatsAggregator longColumnStatsAggregator;
    StringColumnStatsAggregator stringColumnStatsAggregator;
    TimestampColumnStatsAggregator timestampColumnStatsAggregator;

    /* test aggregation with a single statistics */
    testMnemo = "singleStats";
    data = new ColStatsBuilder<>(byte[].class).numNulls(1).avgColLen(8.5).maxColLen(13).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, BINARY_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new BinaryColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(Boolean.class).numNulls(1).numFalses(2).numTrues(13).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, BOOLEAN_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new BooleanColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(Date.class).numNulls(1).numDVs(2).low(DATE_1).high(DATE_4)
        .hll(DATE_1.getDaysSinceEpoch(), DATE_4.getDaysSinceEpoch()).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, DATE_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new DateColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(Decimal.class).numNulls(1).numDVs(2).low(ONE).high(FOUR).hll(1, 4).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, DECIMAL_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new DecimalColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(double.class).numNulls(1).numDVs(2).low(1d).high(4d).hll(1, 3).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, DOUBLE_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new DoubleColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(long.class).numNulls(1).numDVs(2).low(1L).high(4L).hll(1, 3).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, LONG_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new LongColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(String.class).numNulls(1).numDVs(2).avgColLen(8.5).maxColLen(13)
        .hll(S_1, S_3).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, STRING_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new StringColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(Timestamp.class).numNulls(1).numDVs(2).low(TS_1)
        .high(TS_3).hll(TS_1.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch()).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, TIMESTAMP_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new TimestampColumnStatsAggregator(), data));

    /* testAggregateSingleStatWhenNullValues */
    testMnemo = "singleStatWhenNullValues";
    data = new ColStatsBuilder<>(Date.class).numNulls(1).numDVs(2).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, DATE_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new DateColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(Decimal.class).numNulls(1).numDVs(2).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, DECIMAL_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new DecimalColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(double.class).numNulls(1).numDVs(2).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, DOUBLE_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new DoubleColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(long.class).numNulls(1).numDVs(2).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, LONG_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new LongColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(Timestamp.class).numNulls(1).numDVs(2).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, TIMESTAMP_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new TimestampColumnStatsAggregator(), data));

    testMnemo = "singleStatWhenNullValuesUsingDensityFunction";
    data = new ColStatsBuilder<>(Date.class).numNulls(1).numDVs(2).build();
    dateColumnStatsAggregator = new DateColumnStatsAggregator();
    dateColumnStatsAggregator.useDensityFunctionForNDVEstimation = true;
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, DATE_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, dateColumnStatsAggregator, data));

    data = new ColStatsBuilder<>(Decimal.class).numNulls(1).numDVs(2).build();
    decimalColumnStatsAggregator = new DecimalColumnStatsAggregator();
    decimalColumnStatsAggregator.useDensityFunctionForNDVEstimation = true;
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, DECIMAL_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, decimalColumnStatsAggregator, data));

    data = new ColStatsBuilder<>(double.class).numNulls(1).numDVs(2).build();
    doubleColumnStatsAggregator = new DoubleColumnStatsAggregator();
    doubleColumnStatsAggregator.useDensityFunctionForNDVEstimation = true;
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, DOUBLE_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new DoubleColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(long.class).numNulls(1).numDVs(2).build();
    longColumnStatsAggregator = new LongColumnStatsAggregator();
    longColumnStatsAggregator.useDensityFunctionForNDVEstimation = true;
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, LONG_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new LongColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(Timestamp.class).numNulls(1).numDVs(2).build();
    timestampColumnStatsAggregator = new TimestampColumnStatsAggregator();
    timestampColumnStatsAggregator.useDensityFunctionForNDVEstimation = true;
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, TIMESTAMP_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new TimestampColumnStatsAggregator(), data));

    // ndv tuner does not have any effect because min numDVs and max numDVs coincide (we have a single stats)
    testMnemo = "singleStatWhenNullValuesNDVTuner1";
    data = new ColStatsBuilder<>(Date.class).numNulls(1).numDVs(2).build();
    dateColumnStatsAggregator = new DateColumnStatsAggregator();
    dateColumnStatsAggregator.ndvTuner = 1;
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, DATE_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, dateColumnStatsAggregator, data));

    data = new ColStatsBuilder<>(Decimal.class).numNulls(1).numDVs(2).build();
    decimalColumnStatsAggregator = new DecimalColumnStatsAggregator();
    decimalColumnStatsAggregator.ndvTuner = 1;
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, DECIMAL_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, decimalColumnStatsAggregator, data));

    data = new ColStatsBuilder<>(double.class).numNulls(1).numDVs(2).build();
    doubleColumnStatsAggregator = new DoubleColumnStatsAggregator();
    doubleColumnStatsAggregator.ndvTuner = 1;
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, DOUBLE_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new DoubleColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(long.class).numNulls(1).numDVs(2).build();
    longColumnStatsAggregator = new LongColumnStatsAggregator();
    longColumnStatsAggregator.ndvTuner = 1;
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, LONG_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new LongColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(Timestamp.class).numNulls(1).numDVs(2).build();
    timestampColumnStatsAggregator = new TimestampColumnStatsAggregator();
    timestampColumnStatsAggregator.ndvTuner = 1;
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, TIMESTAMP_COL, partitions.get(0)));
    parameters.add(new Args(testMnemo, statsList, partitions, new TimestampColumnStatsAggregator(), data));
    /* testAggregateMultipleStatsWhenSomeNullValues */
/*
    testMnemo = "testAggregateMultipleStatsWhenSomeNullValues";
    long[] values1 = { DATE_1.getDaysSinceEpoch(), DATE_2.getDaysSinceEpoch() };
    ColumnStatisticsData data1 = new ColStatsBuilder<>(Date.class).numNulls(1).numDVs(2)
        .low(DATE_1).high(DATE_2).hll(values1).build();
    ColumnStatisticsData data2 = new ColStatsBuilder<>(Date.class).numNulls(2).numDVs(3).build();
    statsList = Arrays.asList(createStatsWithInfo(data1, TABLE, DATE_COL, partitions.get(0)),
        createStatsWithInfo(data2, TABLE, DATE_COL, partitions.get(1)));
    expectedStats = new ColStatsBuilder<>(Date.class).numNulls(3).numDVs(3).low(DATE_1).high(DATE_2).hll(values1).build();
    parameters.add(new Args(testMnemo, statsList, partitions, dateColumnStatsAggregator, expectedStats));


    testMnemo = "testAggregateMultipleStatsWhenSomeNullValuesUsingDensityFunction";
    values1 = new long[] { DATE_1.getDaysSinceEpoch(), DATE_2.getDaysSinceEpoch() };
    data1 = new ColStatsBuilder<>(Date.class).numNulls(1).numDVs(2)
        .low(DATE_1).high(DATE_2).hll(values1).build();
    data2 = new ColStatsBuilder<>(Date.class).numNulls(2).numDVs(3).build();
    statsList = Arrays.asList(createStatsWithInfo(data1, TABLE, DATE_COL, partitions.get(0)),
        createStatsWithInfo(data2, TABLE, DATE_COL, partitions.get(1)));
    expectedStats = new ColStatsBuilder<>(Date.class).numNulls(3).numDVs(4).low(DATE_1).high(DATE_2).hll(values1).build();
    dateColumnStatsAggregator = new DateColumnStatsAggregator();
    dateColumnStatsAggregator.useDensityFunctionForNDVEstimation = true;
    parameters.add(new Args(testMnemo, statsList, partitions, dateColumnStatsAggregator, expectedStats));

    testMnemo = "testAggregateMultipleStatsWhenSomeNullValuesNDVTuner1";
    values1 = new long[] { DATE_1.getDaysSinceEpoch(), DATE_2.getDaysSinceEpoch() };
    data1 = new ColStatsBuilder<>(Date.class).numNulls(1).numDVs(2)
        .low(DATE_1).high(DATE_2).hll(values1).build();
    data2 = new ColStatsBuilder<>(Date.class).numNulls(2).numDVs(3).build();
    statsList = Arrays.asList(createStatsWithInfo(data1, TABLE, DATE_COL, partitions.get(0)),
        createStatsWithInfo(data2, TABLE, DATE_COL, partitions.get(1)));
    expectedStats = new ColStatsBuilder<>(Date.class).numNulls(3).numDVs(5).low(DATE_1).high(DATE_2).hll(values1).build();
    dateColumnStatsAggregator = new DateColumnStatsAggregator();
    dateColumnStatsAggregator.ndvTuner = 1;
    parameters.add(new Args(testMnemo, statsList, partitions, dateColumnStatsAggregator, expectedStats));
*/
    /* multiple stats, all available */

    return parameters;
  }

  @Test
  public void testAggregate() throws MetaException {
    ColumnStatisticsObj computedStatsObj = arg.aggregator.aggregate(arg.stats, arg.partitions, true);
    Assert.assertEquals(arg.expected, computedStatsObj.getStatsData());
  }
}
