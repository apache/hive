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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.client;

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.metastore.utils.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.HIVE_ENGINE;

/**
 * Tests for updating partition column stats. All the tests will be executed first using embedded metastore and then
 * with remote metastore. For embedded metastore, TRY_DIRECT_SQL is set to false. For remote metastore TRY_DIRECT_SQL
 * is set to true. setPartitionColumnStatistics is modified to use direct sql in case direct sql is enabled. This tests
 * makes sure that both the code path (direct sql enabled and disabled) is tested.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestPartitionStat extends MetaStoreClientTest {
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;
  private static final String DB_NAME = "test_part_stat";
  private static final String TABLE_NAME = "test_part_stat_table";
  private static final String DEFAULT_COL_TYPE = "int";
  private static final String PART_COL_NAME = "year";
  private static final Partition[] PARTITIONS = new Partition[5];

  public TestPartitionStat(String name, AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  @BeforeClass
  public static void startMetaStores() {
    Map<MetastoreConf.ConfVars, String> msConf = new HashMap<>();
    Map<String, String> extraConf = new HashMap<>();
    extraConf.put(MetastoreConf.ConfVars.HIVE_IN_TEST.getVarname(), "true");
    extraConf.put(MetastoreConf.ConfVars.STATS_FETCH_BITVECTOR.getVarname(), "true");
    extraConf.put(MetastoreConf.ConfVars.STATS_FETCH_KLL.getVarname(), "true");
    startMetaStores(msConf, extraConf);
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

    // Clean up the database
    client.dropDatabase(DB_NAME, true, true, true);
    metaStore.cleanWarehouseDirs();
    DatabaseBuilder databaseBuilder = new DatabaseBuilder().setName(DB_NAME);
    databaseBuilder.create(client, metaStore.getConf());

    // Create test tables with 3 partitions
    createTable(TABLE_NAME, getYearPartCol());
    createPartitions();
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (client != null) {
        try {
          client.close();
        } catch (Exception e) {
          // HIVE-19729: Shallow the exceptions based on the discussion in the Jira
        }
      }
    } finally {
      client = null;
    }
  }

  private void createPartitions() throws Exception {
    PARTITIONS[0] = createPartition(Lists.newArrayList("2017"), getYearPartCol());
    PARTITIONS[1] = createPartition(Lists.newArrayList("2018"), getYearPartCol());
    PARTITIONS[2] = createPartition(Lists.newArrayList("2019"), getYearPartCol());
    PARTITIONS[3] = createPartition(Lists.newArrayList("2020"), getYearPartCol());
    PARTITIONS[4] = createPartition(Lists.newArrayList("2021"), getYearPartCol());
  }

  @SuppressWarnings("SameParameterValue")
  private Table createTable(String tableName, List<FieldSchema> partCols) throws Exception {
    String type = "MANAGED_TABLE";
    String location = metaStore.getWarehouseRoot() + "/" + tableName;

    return new TableBuilder()
            .setDbName(DB_NAME)
            .setTableName(tableName)
            .setType(type)
            .addCol("test_id", "int", "test col id")
            .addCol("test_value", "string", "test col value")
            .setPartCols(partCols)
            .setLocation(location)
            .create(client, metaStore.getConf());
  }

  private Partition createPartition(List<String> values,
                                    List<FieldSchema> partCols) throws Exception {
    new PartitionBuilder()
            .setDbName(DB_NAME)
            .setTableName(TABLE_NAME)
            .setValues(values)
            .setCols(partCols)
            .addToTable(client, metaStore.getConf());
    return client.getPartition(DB_NAME, TABLE_NAME, values);
  }

  private static List<FieldSchema> getYearPartCol() {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema(PART_COL_NAME, DEFAULT_COL_TYPE, "year part col"));
    return cols;
  }

  @SuppressWarnings("SameParameterValue")
  private ColumnStatisticsData createStatsData(long numNulls, long numDVs, long low, long high, HyperLogLog hll,
      KllFloatsSketch kll) {
    ColumnStatisticsData data = new ColumnStatisticsData();
    LongColumnStatsDataInspector stats = new LongColumnStatsDataInspector();
    stats.setLowValue(low);
    stats.setHighValue(high);
    stats.setNumNulls(numNulls);
    stats.setNumDVs(numDVs);
    stats.setBitVectors(hll.serialize());
    stats.setHistogram(kll.toByteArray());
    data.setLongStats(stats);
    return data;
  }

  private ColumnStatistics createPartColStats(List<String> partValue, ColumnStatisticsData partitionStats) {
    String pName = FileUtils.makePartName(Collections.singletonList(PART_COL_NAME), partValue);
    ColumnStatistics colStats = new ColumnStatistics();
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(false, DB_NAME, TABLE_NAME);
    statsDesc.setPartName(pName);
    colStats.setStatsDesc(statsDesc);
    colStats.setEngine(HIVE_ENGINE);
    ColumnStatisticsObj statObj = new ColumnStatisticsObj(PART_COL_NAME, "int", partitionStats);
    colStats.addToStatsObj(statObj);
    return colStats;
  }

  private void assertLongStatsEquals(LongColumnStatsData expectedData, LongColumnStatsData actualData) {
    Assert.assertEquals(expectedData.getNumDVs(), actualData.getNumDVs());
    Assert.assertEquals(expectedData.getNumNulls(), actualData.getNumNulls());
    Assert.assertEquals(expectedData.getHighValue(), actualData.getHighValue());
    Assert.assertEquals(expectedData.getLowValue(), actualData.getLowValue());
    Assert.assertArrayEquals(expectedData.getBitVectors(), actualData.getBitVectors());
    Assert.assertArrayEquals(expectedData.getHistogram(), actualData.getHistogram());
  }

  private List<String> updatePartColStat(Map<List<String>, ColumnStatisticsData> partitionStats) throws Exception {
    SetPartitionsStatsRequest rqst = new SetPartitionsStatsRequest();
    rqst.setEngine(HIVE_ENGINE);
    List<String> pNameList = new ArrayList<>();
    for (Map.Entry<List<String>, ColumnStatisticsData> entry : partitionStats.entrySet()) {
      ColumnStatistics colStats = createPartColStats(entry.getKey(), entry.getValue());
      String pName = FileUtils.makePartName(Collections.singletonList(PART_COL_NAME), entry.getKey());
      rqst.addToColStats(colStats);
      pNameList.add(pName);
    }
    client.setPartitionColumnStatistics(rqst);
    return pNameList;
  }

  private void validateStats(Map<List<String>, ColumnStatisticsData> partitionStats,
                             List<String> pNameList) throws Exception {
    Map<String, List<ColumnStatisticsObj>> statistics = client.getPartitionColumnStatistics(DB_NAME, TABLE_NAME,
            pNameList, Collections.singletonList(PART_COL_NAME), HIVE_ENGINE);
    for (Map.Entry<List<String>, ColumnStatisticsData> entry : partitionStats.entrySet()) {
      String pName = FileUtils.makePartName(Collections.singletonList(PART_COL_NAME), entry.getKey());
      ColumnStatisticsObj statisticsObjs = statistics.get(pName).get(0);
      ColumnStatisticsData data = entry.getValue();
      assertLongStatsEquals(statisticsObjs.getStatsData().getLongStats(), data.getLongStats());
    }
  }

  @Test
  public void testUpdateStatSingle() throws Exception {
    Map<List<String>, ColumnStatisticsData> partitionStats = new HashMap<>();
    HyperLogLog hll = HyperLogLog.builder().build();
    hll.addLong(1);
    hll.addLong(2);
    hll.addLong(3);

    KllFloatsSketch kll = new KllFloatsSketch();
    kll.update(1);
    kll.update(2);
    kll.update(3);

    partitionStats.put(PARTITIONS[0].getValues(), createStatsData(100, 50, 1, 100, hll, kll));
    List<String> pNameList = updatePartColStat(partitionStats);
    validateStats(partitionStats, pNameList);
  }

  @Test
  public void testUpdateStatMultiple() throws Exception {
    HyperLogLog hll = HyperLogLog.builder().build();
    hll.addLong(1);
    hll.addLong(2);
    hll.addLong(4);

    KllFloatsSketch kll = new KllFloatsSketch();
    kll.update(1);
    kll.update(2);
    kll.update(5);

    Map<List<String>, ColumnStatisticsData> partitionStats = new HashMap<>();
    partitionStats.put(PARTITIONS[0].getValues(), createStatsData(100, 50, 1, 100, hll, kll));
    partitionStats.put(PARTITIONS[1].getValues(), createStatsData(100, 500, 1, 100, hll, kll));
    partitionStats.put(PARTITIONS[2].getValues(), createStatsData(100, 150, 1, 100, hll, kll));

    hll = HyperLogLog.builder().build();
    hll.addLong(1);
    hll.addLong(3);
    hll.addLong(4);

    kll = new KllFloatsSketch();
    kll.update(1);
    kll.update(3);
    kll.update(5);

    partitionStats.put(PARTITIONS[3].getValues(), createStatsData(100, 50, 2, 100, hll, kll));
    partitionStats.put(PARTITIONS[4].getValues(), createStatsData(100, 50, 1, 1000, hll, kll));
    List<String> pNameList = updatePartColStat(partitionStats);
    validateStats(partitionStats, pNameList);
  }
}
