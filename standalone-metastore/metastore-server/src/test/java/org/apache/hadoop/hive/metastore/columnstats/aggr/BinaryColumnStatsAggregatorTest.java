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
public class BinaryColumnStatsAggregatorTest {

  private static final Table TABLE = new Table("dummy", "db", "hive", 0, 0,
      0, null, null, Collections.emptyMap(), null, null,
      TableType.MANAGED_TABLE.toString());
  private static final FieldSchema COL = new FieldSchema("col", "binary", "");

  @Test
  public void testAggregateSingleStat() throws MetaException {
    List<String> partitionNames = Collections.singletonList("part1");
    
    ColumnStatisticsData data1 = new ColStatsBuilder<>(byte[].class).numNulls(1).avgColLen(8.5).maxColLen(13).buildBinaryStats();
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = StatisticsTestUtils.createColStatsObjWithSourceInfoList(
        TABLE, partitionNames, Collections.singletonList(stats1));

    BinaryColumnStatsAggregator aggregator = new BinaryColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);

    Assert.assertEquals(data1, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsWhenAllAvailable() throws MetaException {
    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(byte[].class).numNulls(1).avgColLen(20.0 / 3).maxColLen(13).buildBinaryStats();
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    ColumnStatisticsData data2 = new ColStatsBuilder<>(byte[].class).numNulls(2).avgColLen(14).maxColLen(18).buildBinaryStats();
    ColumnStatistics stats2 = StatisticsTestUtils.createColStats(data2, TABLE, COL, partitionNames.get(1));

    ColumnStatisticsData data3 = new ColStatsBuilder<>(byte[].class).numNulls(3).avgColLen(17.5).maxColLen(18).buildBinaryStats();
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(data3, TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = StatisticsTestUtils.createColStatsObjWithSourceInfoList(
        TABLE, partitionNames, Arrays.asList(stats1, stats2, stats3));

    BinaryColumnStatsAggregator aggregator = new BinaryColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(byte[].class).numNulls(6).avgColLen(17.5).maxColLen(18).buildBinaryStats();

    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsWhenOnlySomeAvailable() throws MetaException {
    List<String> partitionNames = Arrays.asList("part1", "part2", "part3", "part4");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(byte[].class).numNulls(1).avgColLen(20.0 / 3).maxColLen(13).buildBinaryStats();
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(data1, TABLE, COL, partitionNames.get(0));

    ColumnStatisticsData data3 = new ColStatsBuilder<>(byte[].class).numNulls(3).avgColLen(17.5).maxColLen(18).buildBinaryStats();
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(data3, TABLE, COL, partitionNames.get(2));

    ColumnStatisticsData data4 = new ColStatsBuilder<>(byte[].class).numNulls(2).avgColLen(14).maxColLen(18).buildBinaryStats();
    ColumnStatistics stats4 = StatisticsTestUtils.createColStats(data4, TABLE, COL, partitionNames.get(3));

    List<ColStatsObjWithSourceInfo> statsList = StatisticsTestUtils.createColStatsObjWithSourceInfoList(
        TABLE, partitionNames, Arrays.asList(stats1, null, stats3, stats4), Arrays.asList(0, 2, 3));

    BinaryColumnStatsAggregator aggregator = new BinaryColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitionNames, false);
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(byte[].class).numNulls(6).avgColLen(17.5).maxColLen(18).buildBinaryStats();

    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());
  }
}
