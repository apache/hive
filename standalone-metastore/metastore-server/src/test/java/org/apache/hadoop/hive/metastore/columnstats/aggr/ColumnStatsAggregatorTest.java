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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.columnstats.ColStatsBuilder;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.createStatsWithInfo;

@Category(MetastoreUnitTest.class)
@RunWith(Parameterized.class)
public class ColumnStatsAggregatorTest {

  private static class Args {
    final List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> stats;
    final List<String> partitions;
    final ColumnStatsAggregator aggregator;
    final ColumnStatisticsData expected;

    Args(List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> stats, List<String> partitions,
        ColumnStatsAggregator aggregator, ColumnStatisticsData expected) {
      this.stats = stats;
      this.partitions = partitions;
      this.aggregator = aggregator;
      this.expected = expected;
    }

    @Override
    public String toString() {
      String statString = stats.stream().map(s -> s.getColStatsObj().toString()).collect(Collectors.joining(","));
      return "Args{" + "stats=" + statString + ", partitions=" + partitions + ", aggregator=" + aggregator.getClass()
          .getSimpleName() + ", expected=" + expected + '}';
    }
  }

  private static final Table TABLE =
      new Table("dummy", "db", "hive", 0, 0, 0, null, null, Collections.emptyMap(), null, null,
          TableType.MANAGED_TABLE.toString());
  private static final FieldSchema COL = new FieldSchema("col", "int", "");

  private final Args arg;

  public ColumnStatsAggregatorTest(Args arg) {
    this.arg = arg;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<?> data() {
    final List<Args> parameters = new ArrayList<>();
    final List<String> partitions = Collections.singletonList("part1");
    ColumnStatisticsData data;
    List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> statsList;

    data = new ColStatsBuilder<>(long.class).numNulls(1).numDVs(2).low(1L).high(4L).hll(1, 3).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, COL, partitions.get(0)));
    parameters.add(new Args(statsList, partitions, new LongColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(double.class).numNulls(1).numDVs(2).low(1d).high(4d).hll(1, 3).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, COL, partitions.get(0)));
    parameters.add(new Args(statsList, partitions, new DoubleColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(Boolean.class).numNulls(1).numFalses(2).numTrues(13).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, COL, partitions.get(0)));
    parameters.add(new Args(statsList, partitions, new BooleanColumnStatsAggregator(), data));

    Date date1 = new Date(1);
    Date date4 = new Date(4);
    data = new ColStatsBuilder<>(Date.class).numNulls(1).numDVs(2).low(date1).high(date4)
        .hll(date1.getDaysSinceEpoch(), date4.getDaysSinceEpoch()).build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, COL, partitions.get(0)));
    parameters.add(new Args(statsList, partitions, new DateColumnStatsAggregator(), data));

    data = new ColStatsBuilder<>(String.class).numNulls(1).numDVs(2).avgColLen(8.5).maxColLen(13).hll("try", "test")
        .build();
    statsList = Collections.singletonList(createStatsWithInfo(data, TABLE, COL, partitions.get(0)));
    parameters.add(new Args(statsList, partitions, new StringColumnStatsAggregator(), data));

    // TODO Binary, Decimal, Timestamp
    return parameters;
  }

  @Test
  public void testAggregateSingleStat() throws MetaException {
    ColumnStatisticsObj computedStatsObj = arg.aggregator.aggregate(arg.stats, arg.partitions, true);
    Assert.assertEquals(arg.expected, computedStatsObj.getStatsData());
  }
}
