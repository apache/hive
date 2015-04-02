/**
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

package org.apache.hadoop.hive.metastore.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.hbase.AggregateStatsCache.AggrColStatsCached;
import org.apache.hadoop.hive.metastore.hbase.utils.BloomFilter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAggregateStatsCache {
  static int MAX_CACHE_NODES = 10;
  static int MAX_PARTITIONS_PER_CACHE_NODE = 10;
  static String TIME_TO_LIVE = "20s";
  static float FALSE_POSITIVE_PROBABILITY = (float) 0.01;
  static float MAX_VARIANCE = (float) 0.1;
  static AggregateStatsCache cache;
  static String dbName = "db";
  static String tablePrefix = "tab";
  static String partitionPrefix = "part";
  static String columnPrefix = "col";
  static int numTables = 2;
  static int numPartitions = 20;
  static int numColumns = 5;
  static List<String> tables = new ArrayList<String>();
  static List<String> tabParts = new ArrayList<String>();
  static List<String> tabCols = new ArrayList<String>();

  @BeforeClass
  public static void beforeTest() {
    // All data intitializations
    initializeTables();
    initializePartitions();
    initializeColumns();
  }

  private static void initializeTables() {
    for (int i = 1; i <= numTables; i++) {
      // tab1, tab2
      tables.add(tablePrefix + i);
    }
  }

  private static void initializePartitions() {
    for (int i = 1; i <= numPartitions; i++) {
      // part1 ... part20
      tabParts.add(partitionPrefix + i);
    }
  }

  private static void initializeColumns() {
    for (int i = 1; i <= numColumns; i++) {
      // part1 ... part20
      tabCols.add(columnPrefix + i);
    }
  }

  @AfterClass
  public static void afterTest() {
  }

  @Before
  public void setUp() {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORE_HBASE_AGGREGATE_STATS_CACHE_SIZE,
        MAX_CACHE_NODES);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORE_HBASE_AGGREGATE_STATS_CACHE_MAX_PARTITIONS,
        MAX_PARTITIONS_PER_CACHE_NODE);
    hiveConf.setFloatVar(
        HiveConf.ConfVars.METASTORE_HBASE_AGGREGATE_STATS_CACHE_FALSE_POSITIVE_PROBABILITY,
        FALSE_POSITIVE_PROBABILITY);
    hiveConf.setFloatVar(HiveConf.ConfVars.METASTORE_HBASE_AGGREGATE_STATS_CACHE_MAX_VARIANCE,
        MAX_VARIANCE);
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_HBASE_CACHE_TIME_TO_LIVE, TIME_TO_LIVE);
    cache = AggregateStatsCache.getInstance(hiveConf);

  }

  @After
  public void tearDown() {
  }

  @Test
  public void testBasicAddAndGet() {
    // Add a dummy aggregate stats object for parts 1-9 of tab1 for col1
    int minPart = 1;
    int maxPart = 9;
    String tblName = tables.get(0);
    String colName = tabCols.get(0);
    ColumnStatisticsObj aggrColStats = getDummyLongColStat(colName);
    // Prepare the bloom filter
    BloomFilter bloomFilter =
        new BloomFilter(MAX_PARTITIONS_PER_CACHE_NODE, FALSE_POSITIVE_PROBABILITY);
    List <String> partNames = new ArrayList<String>();
    for (int i = minPart; i <= maxPart; i++) {
      String partName = tabParts.get(i);
      partNames.add(partName);
      bloomFilter.addToFilter(partName.getBytes());
    }
    // Now add to cache
    cache.add(dbName, tblName, colName, maxPart-minPart+1, aggrColStats, bloomFilter);
    // Now get from cache
    AggrColStatsCached aggrStatsCached = cache.get(dbName, tblName, colName, partNames);
    Assert.assertNotNull(aggrStatsCached);

    ColumnStatisticsObj aggrColStatsCached = aggrStatsCached.getColStats();
    Assert.assertEquals(aggrColStats, aggrColStatsCached);

    // Now get a non-existant entry
    aggrStatsCached = cache.get("dbNotThere", tblName, colName, partNames);
    Assert.assertNull(aggrStatsCached);
  }

  @Test
  public void testAddGetWithVariance() {
    // Add a dummy aggregate stats object for parts 1-9 of tab1 for col1
    int minPart = 1;
    int maxPart = 9;
    String tblName = tables.get(0);
    String colName = tabCols.get(0);
    ColumnStatisticsObj aggrColStats = getDummyLongColStat(colName);
    // Prepare the bloom filter
    BloomFilter bloomFilter =
        new BloomFilter(MAX_PARTITIONS_PER_CACHE_NODE, FALSE_POSITIVE_PROBABILITY);
    // The paritions we'll eventually request from the cache
    List <String> partNames = new ArrayList<String>();
    for (int i = minPart; i <= maxPart; i++) {
      String partName = tabParts.get(i);
      // Only add 50% partitions to partnames so that we can see if the request fails
      if (i < maxPart / 2) {
        partNames.add(partName);
      }
      bloomFilter.addToFilter(partName.getBytes());
    }
    // Now add to cache
    cache.add(dbName, tblName, colName, maxPart-minPart+1, aggrColStats, bloomFilter);
    // Now get from cache
    System.out.println(partNames);
    AggrColStatsCached aggrStatsCached = cache.get(dbName, tblName, colName, partNames);
    Assert.assertNull(aggrStatsCached);
  }

  @Test
  public void testMultiThreaded() {
  }

  private ColumnStatisticsObj getDummyLongColStat(String colName) {
    ColumnStatisticsObj aggrColStats = new ColumnStatisticsObj();
    aggrColStats.setColName(colName);
    aggrColStats.setColType("long");
    LongColumnStatsData longStatsData = new LongColumnStatsData();
    // Set some random values
    int highVal = 100;
    int lowVal = 10;
    int numDVs = 50;
    int numNulls = 5;
    longStatsData.setHighValue(highVal);
    longStatsData.setLowValue(lowVal);
    longStatsData.setNumDVs(numDVs);
    longStatsData.setNumNulls(numNulls);
    ColumnStatisticsData aggrColStatsData = new ColumnStatisticsData();
    aggrColStatsData.setLongStats(longStatsData);
    aggrColStats.setStatsData(aggrColStatsData);
    return aggrColStats;
  }
}
