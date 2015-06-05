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

package org.apache.hadoop.hive.metastore;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.AggregateStatsCache.AggrColStats;
import org.apache.hadoop.hive.metastore.AggregateStatsCache.Key;
import org.apache.hive.common.util.BloomFilter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAggregateStatsCache {
  static String DB_NAME = "db";
  static String TAB_PREFIX = "tab";
  static String PART_PREFIX = "part";
  static String COL_PREFIX = "col";
  static int NUM_TABS = 2;
  static int NUM_PARTS = 20;
  static int NUM_COLS = 5;
  static int MAX_CACHE_NODES = 10;
  static int MAX_PARTITIONS_PER_CACHE_NODE = 10;
  static String TIME_TO_LIVE = "20s";
  static String MAX_WRITER_WAIT = "1s";
  static String MAX_READER_WAIT = "1s";
  static float FALSE_POSITIVE_PROBABILITY = (float) 0.01;
  static float MAX_VARIANCE = (float) 0.5;
  static AggregateStatsCache cache;
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

  // tab1, tab2
  private static void initializeTables() {
    for (int i = 1; i <= NUM_TABS; i++) {
      tables.add(TAB_PREFIX + i);
    }
  }

  // part1 ... part20
  private static void initializePartitions() {
    for (int i = 1; i <= NUM_PARTS; i++) {
      tabParts.add(PART_PREFIX + i);
    }
  }

  // col1 ... col5
  private static void initializeColumns() {
    for (int i = 1; i <= NUM_COLS; i++) {
      tabCols.add(COL_PREFIX + i);
    }
  }

  @AfterClass
  public static void afterTest() {
  }

  @Before
  public void setUp() {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORE_AGGREGATE_STATS_CACHE_SIZE,
        MAX_CACHE_NODES);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORE_AGGREGATE_STATS_CACHE_MAX_PARTITIONS,
        MAX_PARTITIONS_PER_CACHE_NODE);
    hiveConf.setFloatVar(
        HiveConf.ConfVars.METASTORE_AGGREGATE_STATS_CACHE_FPP,
        FALSE_POSITIVE_PROBABILITY);
    hiveConf.setFloatVar(HiveConf.ConfVars.METASTORE_AGGREGATE_STATS_CACHE_MAX_VARIANCE,
        MAX_VARIANCE);
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_AGGREGATE_STATS_CACHE_TTL, TIME_TO_LIVE);
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_AGGREGATE_STATS_CACHE_MAX_WRITER_WAIT, MAX_WRITER_WAIT);
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_AGGREGATE_STATS_CACHE_MAX_READER_WAIT, MAX_READER_WAIT);
    cache = AggregateStatsCache.getInstance(hiveConf);
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testCacheKey() {
    Key k1 = new Key("db", "tbl1", "col");
    Key k2 = new Key("db", "tbl1", "col");
    // k1 equals k2
    Assert.assertEquals(k1, k2);
    Key k3 = new Key("db", "tbl2", "col");
    // k1 not equals k3
    Assert.assertNotEquals(k1, k3);
  }

  @Test
  public void testBasicAddAndGet() throws Exception {
    // Partnames: [tab1part1...tab1part9]
    List<String> partNames = preparePartNames(tables.get(0), 1, 9);
    // Prepare the bloom filter
    BloomFilter bloomFilter = prepareBloomFilter(partNames);
    // Add a dummy aggregate stats object for the above parts (part1...part9) of tab1 for col1
    String tblName = tables.get(0);
    String colName = tabCols.get(0);
    int highVal = 100, lowVal = 10, numDVs = 50, numNulls = 5;
    // We'll treat this as the aggregate col stats for part1...part9 of tab1, col1
    ColumnStatisticsObj aggrColStats =
        getDummyLongColStat(colName, highVal, lowVal, numDVs, numNulls);
    // Now add to cache the dummy colstats for these 10 partitions
    cache.add(DB_NAME, tblName, colName, 10, aggrColStats, bloomFilter);
    // Now get from cache
    AggrColStats aggrStatsCached = cache.get(DB_NAME, tblName, colName, partNames);
    Assert.assertNotNull(aggrStatsCached);

    ColumnStatisticsObj aggrColStatsCached = aggrStatsCached.getColStats();
    Assert.assertEquals(aggrColStats, aggrColStatsCached);

    // Now get a non-existant entry
    aggrStatsCached = cache.get("dbNotThere", tblName, colName, partNames);
    Assert.assertNull(aggrStatsCached);
  }

  @Test
  public void testAddGetWithVariance() throws Exception {
    // Partnames: [tab1part1...tab1part9]
    List<String> partNames = preparePartNames(tables.get(0), 1, 9);
    // Prepare the bloom filter
    BloomFilter bloomFilter = prepareBloomFilter(partNames);
    // Add a dummy aggregate stats object for the above parts (part1...part9) of tab1 for col1
    String tblName = tables.get(0);
    String colName = tabCols.get(0);
    int highVal = 100, lowVal = 10, numDVs = 50, numNulls = 5;
    // We'll treat this as the aggregate col stats for part1...part9 of tab1, col1
    ColumnStatisticsObj aggrColStats =
        getDummyLongColStat(colName, highVal, lowVal, numDVs, numNulls);
    // Now add to cache
    cache.add(DB_NAME, tblName, colName, 10, aggrColStats, bloomFilter);

    // Now prepare partnames with only 5 partitions: [tab1part1...tab1part5]
    partNames = preparePartNames(tables.get(0), 1, 5);
    // This get should fail because its variance ((10-5)/5) is way past MAX_VARIANCE (0.5)
    AggrColStats aggrStatsCached = cache.get(DB_NAME, tblName, colName, partNames);
    Assert.assertNull(aggrStatsCached);

    // Now prepare partnames with 10 partitions: [tab1part11...tab1part20], but with no overlap
    partNames = preparePartNames(tables.get(0), 11, 20);
    // This get should fail because its variance ((10-0)/10) is way past MAX_VARIANCE (0.5)
    aggrStatsCached = cache.get(DB_NAME, tblName, colName, partNames);
    Assert.assertNull(aggrStatsCached);

    // Now prepare partnames with 9 partitions: [tab1part1...tab1part8], which are contained in the
    // object that we added to the cache
    partNames = preparePartNames(tables.get(0), 1, 8);
    // This get should succeed because its variance ((10-9)/9) is within past MAX_VARIANCE (0.5)
    aggrStatsCached = cache.get(DB_NAME, tblName, colName, partNames);
    Assert.assertNotNull(aggrStatsCached);
    ColumnStatisticsObj aggrColStatsCached = aggrStatsCached.getColStats();
    Assert.assertEquals(aggrColStats, aggrColStatsCached);
  }

  @Test
  public void testTimeToLive() throws Exception {
    // Add a dummy node to cache
    // Partnames: [tab1part1...tab1part9]
    List<String> partNames = preparePartNames(tables.get(0), 1, 9);
    // Prepare the bloom filter
    BloomFilter bloomFilter = prepareBloomFilter(partNames);
    // Add a dummy aggregate stats object for the above parts (part1...part9) of tab1 for col1
    String tblName = tables.get(0);
    String colName = tabCols.get(0);
    int highVal = 100, lowVal = 10, numDVs = 50, numNulls = 5;
    // We'll treat this as the aggregate col stats for part1...part9 of tab1, col1
    ColumnStatisticsObj aggrColStats =
        getDummyLongColStat(colName, highVal, lowVal, numDVs, numNulls);
    // Now add to cache
    cache.add(DB_NAME, tblName, colName, 10, aggrColStats, bloomFilter);

    // Sleep for 30 seconds
    Thread.sleep(30000);

    // Get should fail now (since TTL is 20s) and we've snoozed for 30 seconds
    AggrColStats aggrStatsCached = cache.get(DB_NAME, tblName, colName, partNames);
    Assert.assertNull(aggrStatsCached);
  }

  /**
   * Prepares an array of partition names by getting partitions from minPart ... maxPart and
   * prepending with table name
   * Example: [tab1part1, tab1part2 ...]
   *
   * @param tabName
   * @param minPart
   * @param maxPart
   * @return
   * @throws Exception
   */
  private List<String> preparePartNames(String tabName, int minPart, int maxPart) throws Exception {
    if ((minPart < 1) || (maxPart > NUM_PARTS)) {
      throw new Exception("tabParts does not have these partition numbers");
    }
    List<String> partNames = new ArrayList<String>();
    for (int i = minPart; i <= maxPart; i++) {
      String partName = tabParts.get(i-1);
      partNames.add(tabName + partName);
    }
    return partNames;
  }

  /**
   * Prepares a bloom filter from the list of partition names
   * @param partNames
   * @return
   */
  private BloomFilter prepareBloomFilter(List <String> partNames) {
    BloomFilter bloomFilter =
        new BloomFilter(MAX_PARTITIONS_PER_CACHE_NODE, FALSE_POSITIVE_PROBABILITY);
    for (String partName: partNames) {
      bloomFilter.add(partName.getBytes());
    }
    return bloomFilter;
  }

  private ColumnStatisticsObj getDummyLongColStat(String colName, int highVal, int lowVal, int numDVs, int numNulls) {
    ColumnStatisticsObj aggrColStats = new ColumnStatisticsObj();
    aggrColStats.setColName(colName);
    aggrColStats.setColType("long");
    LongColumnStatsData longStatsData = new LongColumnStatsData();
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
