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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestHBaseStoreBitVector {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseStoreBitVector.class.getName());
  static Map<String, String> emptyParameters = new HashMap<String, String>();
  // Table with NUM_PART_KEYS partitioning keys and NUM_PARTITIONS values per key
  static final int NUM_PART_KEYS = 1;
  static final int NUM_PARTITIONS = 5;
  static final String DB = "db";
  static final String TBL = "tbl";
  static final String COL = "col";
  static final String PART_KEY_PREFIX = "part";
  static final String PART_VAL_PREFIX = "val";
  static final String PART_KV_SEPARATOR = "=";
  static final List<String> PART_KEYS = new ArrayList<String>();
  static final List<String> PART_VALS = new ArrayList<String>();
  // Initialize mock partitions
  static {
    for (int i = 1; i <= NUM_PART_KEYS; i++) {
      PART_KEYS.add(PART_KEY_PREFIX + i);
    }
    for (int i = 1; i <= NUM_PARTITIONS; i++) {
      PART_VALS.add(PART_VAL_PREFIX + i);
    }
  }
  static final long DEFAULT_TIME = System.currentTimeMillis();
  static final String PART_KEY = "part";
  static final String LONG_COL = "longCol";
  static final String LONG_TYPE = "long";
  static final String INT_TYPE = "int";
  static final String INT_VAL = "1234";
  static final String DOUBLE_COL = "doubleCol";
  static final String DOUBLE_TYPE = "double";
  static final String DOUBLE_VAL = "3.1415";
  static final String STRING_COL = "stringCol";
  static final String STRING_TYPE = "string";
  static final String STRING_VAL = "stringval";
  static final String DECIMAL_COL = "decimalCol";
  static final String DECIMAL_TYPE = "decimal(5,3)";
  static final String DECIMAL_VAL = "12.123";
  static List<ColumnStatisticsObj> longColStatsObjs = new ArrayList<ColumnStatisticsObj>(
      NUM_PARTITIONS);
  static List<ColumnStatisticsObj> doubleColStatsObjs = new ArrayList<ColumnStatisticsObj>(
      NUM_PARTITIONS);
  static List<ColumnStatisticsObj> stringColStatsObjs = new ArrayList<ColumnStatisticsObj>(
      NUM_PARTITIONS);
  static List<ColumnStatisticsObj> decimalColStatsObjs = new ArrayList<ColumnStatisticsObj>(
      NUM_PARTITIONS);

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock HTableInterface htable;
  SortedMap<String, Cell> rows = new TreeMap<>();
  HBaseStore store;


  @BeforeClass
  public static void beforeTest() {
    // All data intitializations
    populateMockStats();
  }

  private static void populateMockStats() {
    ColumnStatisticsObj statsObj;
    // Add NUM_PARTITIONS ColumnStatisticsObj of each type
    // For aggregate stats test, we'll treat each ColumnStatisticsObj as stats for 1 partition
    // For the rest, we'll just pick the 1st ColumnStatisticsObj from this list and use it
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      statsObj = mockLongStats(i);
      longColStatsObjs.add(statsObj);
      statsObj = mockDoubleStats(i);
      doubleColStatsObjs.add(statsObj);
      statsObj = mockStringStats(i);
      stringColStatsObjs.add(statsObj);
      statsObj = mockDecimalStats(i);
      decimalColStatsObjs.add(statsObj);
    }
  }

  private static ColumnStatisticsObj mockLongStats(int i) {
    long high = 120938479124L + 100*i;
    long low = -12341243213412124L - 50*i;
    long nulls = 23 + i;
    long dVs = 213L + 10*i;
    String bitVectors = "{0, 1, 2, 3, 4, 5, 6, 7, 8}{1, 2, 3, 4, 5, 6, 7, 8}";
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(LONG_COL);
    colStatsObj.setColType(LONG_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    LongColumnStatsData longData = new LongColumnStatsData();
    longData.setHighValue(high);
    longData.setLowValue(low);
    longData.setNumNulls(nulls);
    longData.setNumDVs(dVs);
    longData.setBitVectors(bitVectors);
    data.setLongStats(longData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private static ColumnStatisticsObj mockDoubleStats(int i) {
    double high = 123423.23423 + 100*i;
    double low = 0.00001234233 - 50*i;
    long nulls = 92 + i;
    long dVs = 1234123421L + 10*i;
    String bitVectors = "{0, 1, 2, 3, 4, 5, 6, 7, 8}{0, 2, 3, 4, 5, 6, 7, 8}";
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(DOUBLE_COL);
    colStatsObj.setColType(DOUBLE_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    DoubleColumnStatsData doubleData = new DoubleColumnStatsData();
    doubleData.setHighValue(high);
    doubleData.setLowValue(low);
    doubleData.setNumNulls(nulls);
    doubleData.setNumDVs(dVs);
    doubleData.setBitVectors(bitVectors);
    data.setDoubleStats(doubleData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private static ColumnStatisticsObj mockStringStats(int i) {
    long maxLen = 1234 + 10*i;
    double avgLen = 32.3 + i;
    long nulls = 987 + 10*i;
    long dVs = 906 + i;
    String bitVectors = "{0, 1, 2, 3, 4, 5, 6, 7, 8}{0, 1, 3, 4, 5, 6, 7, 8}";
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(STRING_COL);
    colStatsObj.setColType(STRING_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    StringColumnStatsData stringData = new StringColumnStatsData();
    stringData.setMaxColLen(maxLen);
    stringData.setAvgColLen(avgLen);
    stringData.setNumNulls(nulls);
    stringData.setNumDVs(dVs);
    stringData.setBitVectors(bitVectors);
    data.setStringStats(stringData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private static ColumnStatisticsObj mockDecimalStats(int i) {
    Decimal high = new Decimal();
    high.setScale((short)3);
    String strHigh = String.valueOf(3876 + 100*i);
    high.setUnscaled(strHigh.getBytes());
    Decimal low = new Decimal();
    low.setScale((short)3);
    String strLow = String.valueOf(38 + i);
    low.setUnscaled(strLow.getBytes());
    long nulls = 13 + i;
    long dVs = 923947293L + 100*i;
    String bitVectors = "{0, 1, 2, 3, 4, 5, 6, 7, 8}{0, 1, 2, 4, 5, 6, 7, 8}";
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(DECIMAL_COL);
    colStatsObj.setColType(DECIMAL_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    DecimalColumnStatsData decimalData = new DecimalColumnStatsData();
    decimalData.setHighValue(high);
    decimalData.setLowValue(low);
    decimalData.setNumNulls(nulls);
    decimalData.setNumDVs(dVs);
    decimalData.setBitVectors(bitVectors);
    data.setDecimalStats(decimalData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  @AfterClass
  public static void afterTest() {
  }


  @Before
  public void init() throws IOException {
    MockitoAnnotations.initMocks(this);
    HiveConf conf = new HiveConf();
    conf.setBoolean(HBaseReadWrite.NO_CACHE_CONF, true);
    store = MockUtils.init(conf, htable, rows);
  }

  @Test
  public void longTableStatistics() throws Exception {
    createMockTable(LONG_COL, LONG_TYPE);
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for table level stats
    ColumnStatisticsDesc desc = getMockTblColStatsDesc();
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = longColStatsObjs.get(0);
    LongColumnStatsData longData = obj.getStatsData().getLongStats();
    // Add to DB
    stats.addToStatsObj(obj);
    store.updateTableColumnStatistics(stats);
    // Get from DB
    ColumnStatistics statsFromDB = store.getTableColumnStatistics(DB, TBL, Arrays.asList(LONG_COL));
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.getStatsDesc().getTableName());
    Assert.assertTrue(statsFromDB.getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.LONG_STATS, dataFromDB.getSetField());
    // Compare LongColumnStatsData
    LongColumnStatsData longDataFromDB = dataFromDB.getLongStats();
    Assert.assertEquals(longData.getHighValue(), longDataFromDB.getHighValue());
    Assert.assertEquals(longData.getLowValue(), longDataFromDB.getLowValue());
    Assert.assertEquals(longData.getNumNulls(), longDataFromDB.getNumNulls());
    Assert.assertEquals(longData.getNumDVs(), longDataFromDB.getNumDVs());
    Assert.assertEquals(longData.getBitVectors(), longDataFromDB.getBitVectors());
  }

  @Test
  public void doubleTableStatistics() throws Exception {
    createMockTable(DOUBLE_COL, DOUBLE_TYPE);
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for table level stats
    ColumnStatisticsDesc desc = getMockTblColStatsDesc();
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = doubleColStatsObjs.get(0);
    DoubleColumnStatsData doubleData = obj.getStatsData().getDoubleStats();
    // Add to DB
    stats.addToStatsObj(obj);
    store.updateTableColumnStatistics(stats);
    // Get from DB
    ColumnStatistics statsFromDB = store.getTableColumnStatistics(DB, TBL, Arrays.asList(DOUBLE_COL));
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.getStatsDesc().getTableName());
    Assert.assertTrue(statsFromDB.getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.DOUBLE_STATS, dataFromDB.getSetField());
    // Compare DoubleColumnStatsData
    DoubleColumnStatsData doubleDataFromDB = dataFromDB.getDoubleStats();
    Assert.assertEquals(doubleData.getHighValue(), doubleDataFromDB.getHighValue(), 0.01);
    Assert.assertEquals(doubleData.getLowValue(), doubleDataFromDB.getLowValue(), 0.01);
    Assert.assertEquals(doubleData.getNumNulls(), doubleDataFromDB.getNumNulls());
    Assert.assertEquals(doubleData.getNumDVs(), doubleDataFromDB.getNumDVs());
    Assert.assertEquals(doubleData.getBitVectors(), doubleDataFromDB.getBitVectors());
  }

  @Test
  public void stringTableStatistics() throws Exception {
    createMockTable(STRING_COL, STRING_TYPE);
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for table level stats
    ColumnStatisticsDesc desc = getMockTblColStatsDesc();
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = stringColStatsObjs.get(0);
    StringColumnStatsData stringData = obj.getStatsData().getStringStats();
    // Add to DB
    stats.addToStatsObj(obj);
    store.updateTableColumnStatistics(stats);
    // Get from DB
    ColumnStatistics statsFromDB = store.getTableColumnStatistics(DB, TBL, Arrays.asList(STRING_COL));
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.getStatsDesc().getTableName());
    Assert.assertTrue(statsFromDB.getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.STRING_STATS, dataFromDB.getSetField());
    // Compare StringColumnStatsData
    StringColumnStatsData stringDataFromDB = dataFromDB.getStringStats();
    Assert.assertEquals(stringData.getMaxColLen(), stringDataFromDB.getMaxColLen());
    Assert.assertEquals(stringData.getAvgColLen(), stringDataFromDB.getAvgColLen(), 0.01);
    Assert.assertEquals(stringData.getNumNulls(), stringDataFromDB.getNumNulls());
    Assert.assertEquals(stringData.getNumDVs(), stringDataFromDB.getNumDVs());
    Assert.assertEquals(stringData.getBitVectors(), stringDataFromDB.getBitVectors());
  }

  @Test
  public void decimalTableStatistics() throws Exception {
    createMockTable(DECIMAL_COL, DECIMAL_TYPE);
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for table level stats
    ColumnStatisticsDesc desc = getMockTblColStatsDesc();
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = decimalColStatsObjs.get(0);
    DecimalColumnStatsData decimalData = obj.getStatsData().getDecimalStats();
    // Add to DB
    stats.addToStatsObj(obj);
    store.updateTableColumnStatistics(stats);
    // Get from DB
    ColumnStatistics statsFromDB = store.getTableColumnStatistics(DB, TBL, Arrays.asList(DECIMAL_COL));
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.getStatsDesc().getTableName());
    Assert.assertTrue(statsFromDB.getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.DECIMAL_STATS, dataFromDB.getSetField());
    // Compare DecimalColumnStatsData
    DecimalColumnStatsData decimalDataFromDB = dataFromDB.getDecimalStats();
    Assert.assertEquals(decimalData.getHighValue(), decimalDataFromDB.getHighValue());
    Assert.assertEquals(decimalData.getLowValue(), decimalDataFromDB.getLowValue());
    Assert.assertEquals(decimalData.getNumNulls(), decimalDataFromDB.getNumNulls());
    Assert.assertEquals(decimalData.getNumDVs(), decimalDataFromDB.getNumDVs());
    Assert.assertEquals(decimalData.getBitVectors(), decimalDataFromDB.getBitVectors());
  }

  @Test
  public void longPartitionStatistics() throws Exception {
    createMockTableAndPartition(INT_TYPE, INT_VAL);
    // Add partition stats for: LONG_COL and partition: {PART_KEY, INT_VAL} to DB
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for partition level stats
    ColumnStatisticsDesc desc = getMockPartColStatsDesc(PART_KEY, INT_VAL);
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = longColStatsObjs.get(0);
    LongColumnStatsData longData = obj.getStatsData().getLongStats();
    // Add to DB
    stats.addToStatsObj(obj);
    List<String> parVals = new ArrayList<String>();
    parVals.add(INT_VAL);
    store.updatePartitionColumnStatistics(stats, parVals);
    // Get from DB
    List<String> partNames = new ArrayList<String>();
    partNames.add(desc.getPartName());
    List<String> colNames = new ArrayList<String>();
    colNames.add(obj.getColName());
    List<ColumnStatistics> statsFromDB = store.getPartitionColumnStatistics(DB, TBL, partNames, colNames);
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(1, statsFromDB.size());
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.get(0).getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.get(0).getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.get(0).getStatsDesc().getTableName());
    Assert.assertFalse(statsFromDB.get(0).getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.get(0).getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.get(0).getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.LONG_STATS, dataFromDB.getSetField());
    // Compare LongColumnStatsData
    LongColumnStatsData longDataFromDB = dataFromDB.getLongStats();
    Assert.assertEquals(longData.getHighValue(), longDataFromDB.getHighValue());
    Assert.assertEquals(longData.getLowValue(), longDataFromDB.getLowValue());
    Assert.assertEquals(longData.getNumNulls(), longDataFromDB.getNumNulls());
    Assert.assertEquals(longData.getNumDVs(), longDataFromDB.getNumDVs());
    Assert.assertEquals(longData.getBitVectors(), longDataFromDB.getBitVectors());
  }

  @Test
  public void doublePartitionStatistics() throws Exception {
    createMockTableAndPartition(DOUBLE_TYPE, DOUBLE_VAL);
    // Add partition stats for: DOUBLE_COL and partition: {PART_KEY, DOUBLE_VAL} to DB
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for partition level stats
    ColumnStatisticsDesc desc = getMockPartColStatsDesc(PART_KEY, DOUBLE_VAL);
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = doubleColStatsObjs.get(0);
    DoubleColumnStatsData doubleData = obj.getStatsData().getDoubleStats();
    // Add to DB
    stats.addToStatsObj(obj);
    List<String> parVals = new ArrayList<String>();
    parVals.add(DOUBLE_VAL);
    store.updatePartitionColumnStatistics(stats, parVals);
    // Get from DB
    List<String> partNames = new ArrayList<String>();
    partNames.add(desc.getPartName());
    List<String> colNames = new ArrayList<String>();
    colNames.add(obj.getColName());
    List<ColumnStatistics> statsFromDB = store.getPartitionColumnStatistics(DB, TBL, partNames, colNames);
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(1, statsFromDB.size());
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.get(0).getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.get(0).getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.get(0).getStatsDesc().getTableName());
    Assert.assertFalse(statsFromDB.get(0).getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.get(0).getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.get(0).getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.DOUBLE_STATS, dataFromDB.getSetField());
    // Compare DoubleColumnStatsData
    DoubleColumnStatsData doubleDataFromDB = dataFromDB.getDoubleStats();
    Assert.assertEquals(doubleData.getHighValue(), doubleDataFromDB.getHighValue(), 0.01);
    Assert.assertEquals(doubleData.getLowValue(), doubleDataFromDB.getLowValue(), 0.01);
    Assert.assertEquals(doubleData.getNumNulls(), doubleDataFromDB.getNumNulls());
    Assert.assertEquals(doubleData.getNumDVs(), doubleDataFromDB.getNumDVs());
    Assert.assertEquals(doubleData.getBitVectors(), doubleDataFromDB.getBitVectors());
  }

  @Test
  public void stringPartitionStatistics() throws Exception {
    createMockTableAndPartition(STRING_TYPE, STRING_VAL);
    // Add partition stats for: STRING_COL and partition: {PART_KEY, STRING_VAL} to DB
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for partition level stats
    ColumnStatisticsDesc desc = getMockPartColStatsDesc(PART_KEY, STRING_VAL);
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = stringColStatsObjs.get(0);
    StringColumnStatsData stringData = obj.getStatsData().getStringStats();
    // Add to DB
    stats.addToStatsObj(obj);
    List<String> parVals = new ArrayList<String>();
    parVals.add(STRING_VAL);
    store.updatePartitionColumnStatistics(stats, parVals);
    // Get from DB
    List<String> partNames = new ArrayList<String>();
    partNames.add(desc.getPartName());
    List<String> colNames = new ArrayList<String>();
    colNames.add(obj.getColName());
    List<ColumnStatistics> statsFromDB = store.getPartitionColumnStatistics(DB, TBL, partNames, colNames);
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(1, statsFromDB.size());
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.get(0).getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.get(0).getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.get(0).getStatsDesc().getTableName());
    Assert.assertFalse(statsFromDB.get(0).getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.get(0).getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.get(0).getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.STRING_STATS, dataFromDB.getSetField());
    // Compare StringColumnStatsData
    StringColumnStatsData stringDataFromDB = dataFromDB.getStringStats();
    Assert.assertEquals(stringData.getMaxColLen(), stringDataFromDB.getMaxColLen());
    Assert.assertEquals(stringData.getAvgColLen(), stringDataFromDB.getAvgColLen(), 0.01);
    Assert.assertEquals(stringData.getNumNulls(), stringDataFromDB.getNumNulls());
    Assert.assertEquals(stringData.getNumDVs(), stringDataFromDB.getNumDVs());
    Assert.assertEquals(stringData.getBitVectors(), stringDataFromDB.getBitVectors());
  }

  @Test
  public void decimalPartitionStatistics() throws Exception {
    createMockTableAndPartition(DECIMAL_TYPE, DECIMAL_VAL);
    // Add partition stats for: DECIMAL_COL and partition: {PART_KEY, DECIMAL_VAL} to DB
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for partition level stats
    ColumnStatisticsDesc desc = getMockPartColStatsDesc(PART_KEY, DECIMAL_VAL);
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = decimalColStatsObjs.get(0);
    DecimalColumnStatsData decimalData = obj.getStatsData().getDecimalStats();
    // Add to DB
    stats.addToStatsObj(obj);
    List<String> parVals = new ArrayList<String>();
    parVals.add(DECIMAL_VAL);
    store.updatePartitionColumnStatistics(stats, parVals);
    // Get from DB
    List<String> partNames = new ArrayList<String>();
    partNames.add(desc.getPartName());
    List<String> colNames = new ArrayList<String>();
    colNames.add(obj.getColName());
    List<ColumnStatistics> statsFromDB = store.getPartitionColumnStatistics(DB, TBL, partNames, colNames);
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(1, statsFromDB.size());
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.get(0).getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.get(0).getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.get(0).getStatsDesc().getTableName());
    Assert.assertFalse(statsFromDB.get(0).getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.get(0).getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.get(0).getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.DECIMAL_STATS, dataFromDB.getSetField());
    // Compare DecimalColumnStatsData
    DecimalColumnStatsData decimalDataFromDB = dataFromDB.getDecimalStats();
    Assert.assertEquals(decimalData.getHighValue(), decimalDataFromDB.getHighValue());
    Assert.assertEquals(decimalData.getLowValue(), decimalDataFromDB.getLowValue());
    Assert.assertEquals(decimalData.getNumNulls(), decimalDataFromDB.getNumNulls());
    Assert.assertEquals(decimalData.getNumDVs(), decimalDataFromDB.getNumDVs());
    Assert.assertEquals(decimalData.getBitVectors(), decimalDataFromDB.getBitVectors());
  }

  private Table createMockTable(String name, String type) throws Exception {
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema(name, type, ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "value");
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 17,
        serde, new ArrayList<String>(), new ArrayList<Order>(), params);
    int currentTime = (int)(System.currentTimeMillis() / 1000);
    Table table = new Table(TBL, DB, "me", currentTime, currentTime, 0, sd, cols,
        emptyParameters, null, null, null);
    store.createTable(table);
    return table;
  }

  private Table createMockTableAndPartition(String partType, String partVal) throws Exception {
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", partType, ""));
    List<String> vals = new ArrayList<String>();
    vals.add(partVal);
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "value");
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 17,
        serde, Arrays.asList("bucketcol"), Arrays.asList(new Order("sortcol", 1)), params);
    int currentTime = (int)(System.currentTimeMillis() / 1000);
    Table table = new Table(TBL, DB, "me", currentTime, currentTime, 0, sd, cols,
        emptyParameters, null, null, null);
    store.createTable(table);
    Partition part = new Partition(vals, DB, TBL, currentTime, currentTime, sd,
        emptyParameters);
    store.addPartition(part);
    return table;
  }
  /**
   * Returns a dummy table level ColumnStatisticsDesc with default values
   */
  private ColumnStatisticsDesc getMockTblColStatsDesc() {
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
    desc.setLastAnalyzed(DEFAULT_TIME);
    desc.setDbName(DB);
    desc.setTableName(TBL);
    desc.setIsTblLevel(true);
    return desc;
  }

  /**
   * Returns a dummy partition level ColumnStatisticsDesc
   */
  private ColumnStatisticsDesc getMockPartColStatsDesc(String partKey, String partVal) {
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
    desc.setLastAnalyzed(DEFAULT_TIME);
    desc.setDbName(DB);
    desc.setTableName(TBL);
    // part1=val1
    desc.setPartName(partKey + PART_KV_SEPARATOR + partVal);
    desc.setIsTblLevel(false);
    return desc;
  }

}
