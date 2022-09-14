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
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.createStatsWithInfo;

@Category(MetastoreUnitTest.class)
public class BinaryColumnStatsAggregatorTest {

  private static final Table TABLE = new Table("dummy", "db", "hive", 0, 0,
      0, null, null, Collections.emptyMap(), null, null,
      TableType.MANAGED_TABLE.toString());
  private static final FieldSchema COL = new FieldSchema("col", "binary", "");

  @Test
  public void testAggregateSingleStat() throws MetaException {
    List<String> partitions = Collections.singletonList("part1");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(byte[].class).numNulls(1).avgColLen(8.5).maxColLen(13).build();
    List<ColStatsObjWithSourceInfo> statsList =
        Collections.singletonList(createStatsWithInfo(data1, TABLE, COL, partitions.get(0)));

    BinaryColumnStatsAggregator aggregator = new BinaryColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, true);

    Assert.assertEquals(data1, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsWhenAllAvailable() throws MetaException {
    List<String> partitions = Arrays.asList("part1", "part2", "part3");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(byte[].class).numNulls(1).avgColLen(20.0 / 3).maxColLen(13).build();
    ColumnStatisticsData data2 = new ColStatsBuilder<>(byte[].class).numNulls(2).avgColLen(14).maxColLen(18).build();
    ColumnStatisticsData data3 = new ColStatsBuilder<>(byte[].class).numNulls(3).avgColLen(17.5).maxColLen(18).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data2, TABLE, COL, partitions.get(1)),
        createStatsWithInfo(data3, TABLE, COL, partitions.get(2)));

    BinaryColumnStatsAggregator aggregator = new BinaryColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, true);
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(byte[].class).numNulls(6).avgColLen(17.5).maxColLen(18).build();

    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());
  }

  @Test
  public void testAggregateMultiStatsWhenOnlySomeAvailable() throws MetaException {
    List<String> partitions = Arrays.asList("part1", "part2", "part3", "part4");

    ColumnStatisticsData data1 = new ColStatsBuilder<>(byte[].class).numNulls(1).avgColLen(20.0 / 3).maxColLen(13).build();
    ColumnStatisticsData data3 = new ColStatsBuilder<>(byte[].class).numNulls(3).avgColLen(17.5).maxColLen(18).build();
    ColumnStatisticsData data4 = new ColStatsBuilder<>(byte[].class).numNulls(2).avgColLen(14).maxColLen(18).build();

    List<ColStatsObjWithSourceInfo> statsList = Arrays.asList(
        createStatsWithInfo(data1, TABLE, COL, partitions.get(0)),
        createStatsWithInfo(data3, TABLE, COL, partitions.get(2)),
        createStatsWithInfo(data4, TABLE, COL, partitions.get(3)));

    BinaryColumnStatsAggregator aggregator = new BinaryColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitions, false);
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(byte[].class).numNulls(6).avgColLen(17.5).maxColLen(18).build();

    Assert.assertEquals(expectedStats, computedStatsObj.getStatsData());
  }
}
