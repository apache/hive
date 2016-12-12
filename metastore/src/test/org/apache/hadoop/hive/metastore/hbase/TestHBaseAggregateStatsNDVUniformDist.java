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
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.StatObjectConverter;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHBaseAggregateStatsNDVUniformDist {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestHBaseAggregateStatsNDVUniformDist.class.getName());

  @Mock
  HTableInterface htable;
  private HBaseStore store;
  SortedMap<String, Cell> rows = new TreeMap<>();

  // NDV will be 3 for bitVectors[0] and 12 for bitVectors[1] 
  String bitVectors[] = {
      "{0, 4, 5, 7}{0, 1}{0, 1, 2}{0, 1, 4}{0}{0, 2}{0, 3}{0, 2, 3, 4}{0, 1, 4}{0, 1}{0}{0, 1, 3, 8}{0, 2}{0, 2}{0, 9}{0, 1, 4}",
      "{1, 2}{1, 2}{1, 2}{1, 2}{1, 2}{1, 2}{1, 2}{1, 2}{1, 2}{1, 2}{1, 2}{1, 2}{1, 2}{1, 2}{1, 2}{1, 2}" };

  @Before
  public void before() throws IOException {
    MockitoAnnotations.initMocks(this);
    HiveConf conf = new HiveConf();
    conf.setBoolean(HBaseReadWrite.NO_CACHE_CONF, true);
    conf.setBoolean(HiveConf.ConfVars.HIVE_METASTORE_STATS_NDV_DENSITY_FUNCTION.varname, true);
    store = MockUtils.init(conf, htable, rows);
    store.backdoor().getStatsCache().resetCounters();
  }

  private static interface Checker {
    void checkStats(AggrStats aggrStats) throws Exception;
  }

  @Test
  public void allPartitionsHaveBitVectorStatus() throws Exception {
    String dbName = "default";
    String tableName = "snp";
    long now = System.currentTimeMillis();
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col1", "long", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, Collections.<String, String> emptyMap());
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName, dbName, "me", (int) now, (int) now, 0, sd, partCols,
        Collections.<String, String> emptyMap(), null, null, null);
    store.createTable(table);

    List<List<String>> partVals = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      List<String> partVal = Arrays.asList("" + i);
      partVals.add(partVal);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/default/hit/ds=" + partVal);
      Partition part = new Partition(partVal, dbName, tableName, (int) now, (int) now, psd,
          Collections.<String, String> emptyMap());
      store.addPartition(part);
      ColumnStatistics cs = new ColumnStatistics();
      ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
      desc.setLastAnalyzed(now);
      desc.setPartName("ds=" + partVal);
      cs.setStatsDesc(desc);
      ColumnStatisticsObj obj = new ColumnStatisticsObj();
      obj.setColName("col1");
      obj.setColType("long");
      ColumnStatisticsData data = new ColumnStatisticsData();
      LongColumnStatsData dcsd = new LongColumnStatsData();
      dcsd.setHighValue(1000 + i);
      dcsd.setLowValue(-1000 - i);
      dcsd.setNumNulls(i);
      dcsd.setNumDVs(10 * i + 1);
      dcsd.setBitVectors(bitVectors[0]);
      data.setLongStats(dcsd);
      obj.setStatsData(data);
      cs.addToStatsObj(obj);
      store.updatePartitionColumnStatistics(cs, partVal);
    }

    Checker statChecker = new Checker() {
      @Override
      public void checkStats(AggrStats aggrStats) throws Exception {
        Assert.assertEquals(10, aggrStats.getPartsFound());
        Assert.assertEquals(1, aggrStats.getColStatsSize());
        ColumnStatisticsObj cso = aggrStats.getColStats().get(0);
        Assert.assertEquals("col1", cso.getColName());
        Assert.assertEquals("long", cso.getColType());
        LongColumnStatsData lcsd = cso.getStatsData().getLongStats();
        Assert.assertEquals(1009, lcsd.getHighValue(), 0.01);
        Assert.assertEquals(-1009, lcsd.getLowValue(), 0.01);
        Assert.assertEquals(45, lcsd.getNumNulls());
        Assert.assertEquals(3, lcsd.getNumDVs());
      }
    };
    List<String> partNames = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      partNames.add("ds=" + i);
    }
    AggrStats aggrStats = store.get_aggr_stats_for(dbName, tableName, partNames,
        Arrays.asList("col1"));
    statChecker.checkStats(aggrStats);
  }

  @Test
  public void noPartitionsHaveBitVectorStatus() throws Exception {
    String dbName = "default";
    String tableName = "snp";
    long now = System.currentTimeMillis();
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col2", "long", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, Collections.<String, String> emptyMap());
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName, dbName, "me", (int) now, (int) now, 0, sd, partCols,
        Collections.<String, String> emptyMap(), null, null, null);
    store.createTable(table);

    List<List<String>> partVals = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      List<String> partVal = Arrays.asList("" + i);
      partVals.add(partVal);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/default/hit/ds=" + partVal);
      Partition part = new Partition(partVal, dbName, tableName, (int) now, (int) now, psd,
          Collections.<String, String> emptyMap());
      store.addPartition(part);
      ColumnStatistics cs = new ColumnStatistics();
      ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
      desc.setLastAnalyzed(now);
      desc.setPartName("ds=" + partVal);
      cs.setStatsDesc(desc);
      ColumnStatisticsObj obj = new ColumnStatisticsObj();
      obj.setColName("col2");
      obj.setColType("long");
      ColumnStatisticsData data = new ColumnStatisticsData();
      LongColumnStatsData dcsd = new LongColumnStatsData();
      dcsd.setHighValue(1000 + i);
      dcsd.setLowValue(-1000 - i);
      dcsd.setNumNulls(i);
      dcsd.setNumDVs(10 * i + 1);
      data.setLongStats(dcsd);
      obj.setStatsData(data);
      cs.addToStatsObj(obj);
      store.updatePartitionColumnStatistics(cs, partVal);
    }

    Checker statChecker = new Checker() {
      @Override
      public void checkStats(AggrStats aggrStats) throws Exception {
        Assert.assertEquals(10, aggrStats.getPartsFound());
        Assert.assertEquals(1, aggrStats.getColStatsSize());
        ColumnStatisticsObj cso = aggrStats.getColStats().get(0);
        Assert.assertEquals("col2", cso.getColName());
        Assert.assertEquals("long", cso.getColType());
        LongColumnStatsData lcsd = cso.getStatsData().getLongStats();
        Assert.assertEquals(1009, lcsd.getHighValue(), 0.01);
        Assert.assertEquals(-1009, lcsd.getLowValue(), 0.01);
        Assert.assertEquals(45, lcsd.getNumNulls());
        Assert.assertEquals(91, lcsd.getNumDVs());
      }
    };
    List<String> partNames = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      partNames.add("ds=" + i);
    }
    AggrStats aggrStats = store.get_aggr_stats_for(dbName, tableName, partNames,
        Arrays.asList("col2"));
    statChecker.checkStats(aggrStats);
  }

  @Test
  public void TwoEndsOfPartitionsHaveBitVectorStatus() throws Exception {
    String dbName = "default";
    String tableName = "snp";
    long now = System.currentTimeMillis();
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col3", "long", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, Collections.<String, String> emptyMap());
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName, dbName, "me", (int) now, (int) now, 0, sd, partCols,
        Collections.<String, String> emptyMap(), null, null, null);
    store.createTable(table);

    List<List<String>> partVals = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      List<String> partVal = Arrays.asList("" + i);
      partVals.add(partVal);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/default/hit/ds=" + partVal);
      Partition part = new Partition(partVal, dbName, tableName, (int) now, (int) now, psd,
          Collections.<String, String> emptyMap());
      store.addPartition(part);
      if (i < 2 || i > 7) {
        ColumnStatistics cs = new ColumnStatistics();
        ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
        desc.setLastAnalyzed(now);
        desc.setPartName("ds=" + partVal);
        cs.setStatsDesc(desc);
        ColumnStatisticsObj obj = new ColumnStatisticsObj();
        obj.setColName("col3");
        obj.setColType("long");
        ColumnStatisticsData data = new ColumnStatisticsData();
        LongColumnStatsData dcsd = new LongColumnStatsData();
        dcsd.setHighValue(1000 + i);
        dcsd.setLowValue(-1000 - i);
        dcsd.setNumNulls(i);
        dcsd.setNumDVs(10 * i + 1);
        dcsd.setBitVectors(bitVectors[i / 5]);
        data.setLongStats(dcsd);
        obj.setStatsData(data);
        cs.addToStatsObj(obj);
        store.updatePartitionColumnStatistics(cs, partVal);
      }
    }

    Checker statChecker = new Checker() {
      @Override
      public void checkStats(AggrStats aggrStats) throws Exception {
        Assert.assertEquals(4, aggrStats.getPartsFound());
        Assert.assertEquals(1, aggrStats.getColStatsSize());
        ColumnStatisticsObj cso = aggrStats.getColStats().get(0);
        Assert.assertEquals("col3", cso.getColName());
        Assert.assertEquals("long", cso.getColType());
        LongColumnStatsData lcsd = cso.getStatsData().getLongStats();
        Assert.assertEquals(1010, lcsd.getHighValue(), 0.01);
        Assert.assertEquals(-1010, lcsd.getLowValue(), 0.01);
        Assert.assertEquals(45, lcsd.getNumNulls());
        Assert.assertEquals(12, lcsd.getNumDVs());
      }
    };
    List<String> partNames = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      partNames.add("ds=" + i);
    }
    AggrStats aggrStats = store.get_aggr_stats_for(dbName, tableName, partNames,
        Arrays.asList("col3"));
    statChecker.checkStats(aggrStats);
  }

  @Test
  public void MiddleOfPartitionsHaveBitVectorStatus() throws Exception {
    String dbName = "default";
    String tableName = "snp";
    long now = System.currentTimeMillis();
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col4", "long", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, Collections.<String, String> emptyMap());
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName, dbName, "me", (int) now, (int) now, 0, sd, partCols,
        Collections.<String, String> emptyMap(), null, null, null);
    store.createTable(table);

    List<List<String>> partVals = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      List<String> partVal = Arrays.asList("" + i);
      partVals.add(partVal);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/default/hit/ds=" + partVal);
      Partition part = new Partition(partVal, dbName, tableName, (int) now, (int) now, psd,
          Collections.<String, String> emptyMap());
      store.addPartition(part);
      if (i > 2 && i < 7) {
        ColumnStatistics cs = new ColumnStatistics();
        ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
        desc.setLastAnalyzed(now);
        desc.setPartName("ds=" + partVal);
        cs.setStatsDesc(desc);
        ColumnStatisticsObj obj = new ColumnStatisticsObj();
        obj.setColName("col4");
        obj.setColType("long");
        ColumnStatisticsData data = new ColumnStatisticsData();
        LongColumnStatsData dcsd = new LongColumnStatsData();
        dcsd.setHighValue(1000 + i);
        dcsd.setLowValue(-1000 - i);
        dcsd.setNumNulls(i);
        dcsd.setNumDVs(10 * i + 1);
        dcsd.setBitVectors(bitVectors[0]);
        data.setLongStats(dcsd);
        obj.setStatsData(data);
        cs.addToStatsObj(obj);
        store.updatePartitionColumnStatistics(cs, partVal);
      }
    }

    Checker statChecker = new Checker() {
      @Override
      public void checkStats(AggrStats aggrStats) throws Exception {
        Assert.assertEquals(4, aggrStats.getPartsFound());
        Assert.assertEquals(1, aggrStats.getColStatsSize());
        ColumnStatisticsObj cso = aggrStats.getColStats().get(0);
        Assert.assertEquals("col4", cso.getColName());
        Assert.assertEquals("long", cso.getColType());
        LongColumnStatsData lcsd = cso.getStatsData().getLongStats();
        Assert.assertEquals(1006, lcsd.getHighValue(), 0.01);
        Assert.assertEquals(-1006, lcsd.getLowValue(), 0.01);
        Assert.assertEquals(45, lcsd.getNumNulls());
        Assert.assertEquals(3, lcsd.getNumDVs());
      }
    };
    List<String> partNames = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      partNames.add("ds=" + i);
    }
    AggrStats aggrStats = store.get_aggr_stats_for(dbName, tableName, partNames,
        Arrays.asList("col4"));
    statChecker.checkStats(aggrStats);
  }

  @Test
  public void TwoEndsAndMiddleOfPartitionsHaveBitVectorStatusLong() throws Exception {
    String dbName = "default";
    String tableName = "snp";
    long now = System.currentTimeMillis();
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col5_long", "long", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, Collections.<String, String> emptyMap());
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName, dbName, "me", (int) now, (int) now, 0, sd, partCols,
        Collections.<String, String> emptyMap(), null, null, null);
    store.createTable(table);

    List<List<String>> partVals = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      List<String> partVal = Arrays.asList("" + i);
      partVals.add(partVal);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/default/hit/ds=" + partVal);
      Partition part = new Partition(partVal, dbName, tableName, (int) now, (int) now, psd,
          Collections.<String, String> emptyMap());
      store.addPartition(part);
      if (i == 0 || i == 2 || i == 3 || i == 5 || i == 6 || i == 8) {
        ColumnStatistics cs = new ColumnStatistics();
        ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
        desc.setLastAnalyzed(now);
        desc.setPartName("ds=" + partVal);
        cs.setStatsDesc(desc);
        ColumnStatisticsObj obj = new ColumnStatisticsObj();
        obj.setColName("col5_long");
        obj.setColType("long");
        ColumnStatisticsData data = new ColumnStatisticsData();
        LongColumnStatsData dcsd = new LongColumnStatsData();
        dcsd.setHighValue(1000 + i);
        dcsd.setLowValue(-1000 - i);
        dcsd.setNumNulls(i);
        dcsd.setNumDVs(10 * i + 1);
        dcsd.setBitVectors(bitVectors[i / 5]);
        data.setLongStats(dcsd);
        obj.setStatsData(data);
        cs.addToStatsObj(obj);
        store.updatePartitionColumnStatistics(cs, partVal);
      }
    }

    Checker statChecker = new Checker() {
      @Override
      public void checkStats(AggrStats aggrStats) throws Exception {
        Assert.assertEquals(6, aggrStats.getPartsFound());
        Assert.assertEquals(1, aggrStats.getColStatsSize());
        ColumnStatisticsObj cso = aggrStats.getColStats().get(0);
        Assert.assertEquals("col5_long", cso.getColName());
        Assert.assertEquals("long", cso.getColType());
        LongColumnStatsData lcsd = cso.getStatsData().getLongStats();
        Assert.assertEquals(1010, lcsd.getHighValue(), 0.01);
        Assert.assertEquals(-1010, lcsd.getLowValue(), 0.01);
        Assert.assertEquals(40, lcsd.getNumNulls());
        Assert.assertEquals(12, lcsd.getNumDVs());
      }
    };
    List<String> partNames = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      partNames.add("ds=" + i);
    }
    AggrStats aggrStats = store.get_aggr_stats_for(dbName, tableName, partNames,
        Arrays.asList("col5_long"));
    statChecker.checkStats(aggrStats);
  }
  
  @Test
  public void TwoEndsAndMiddleOfPartitionsHaveBitVectorStatusDecimal() throws Exception {
    String dbName = "default";
    String tableName = "snp";
    long now = System.currentTimeMillis();
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col5_decimal", "decimal", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, Collections.<String, String> emptyMap());
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName, dbName, "me", (int) now, (int) now, 0, sd, partCols,
        Collections.<String, String> emptyMap(), null, null, null);
    store.createTable(table);

    List<List<String>> partVals = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      List<String> partVal = Arrays.asList("" + i);
      partVals.add(partVal);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/default/hit/ds=" + partVal);
      Partition part = new Partition(partVal, dbName, tableName, (int) now, (int) now, psd,
          Collections.<String, String> emptyMap());
      store.addPartition(part);
      if (i == 0 || i == 2 || i == 3 || i == 5 || i == 6 || i == 8) {
        ColumnStatistics cs = new ColumnStatistics();
        ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
        desc.setLastAnalyzed(now);
        desc.setPartName("ds=" + partVal);
        cs.setStatsDesc(desc);
        ColumnStatisticsObj obj = new ColumnStatisticsObj();
        obj.setColName("col5_decimal");
        obj.setColType("decimal");
        ColumnStatisticsData data = new ColumnStatisticsData();
        DecimalColumnStatsData dcsd = new DecimalColumnStatsData();
        dcsd.setHighValue(StatObjectConverter.createThriftDecimal("" + (1000 + i)));
        dcsd.setLowValue(StatObjectConverter.createThriftDecimal("" + (-1000 - i)));
        dcsd.setNumNulls(i);
        dcsd.setNumDVs(10 * i + 1);
        dcsd.setBitVectors(bitVectors[i / 5]);
        data.setDecimalStats(dcsd);
        obj.setStatsData(data);
        cs.addToStatsObj(obj);
        store.updatePartitionColumnStatistics(cs, partVal);
      }
    }

    Checker statChecker = new Checker() {
      @Override
      public void checkStats(AggrStats aggrStats) throws Exception {
        Assert.assertEquals(6, aggrStats.getPartsFound());
        Assert.assertEquals(1, aggrStats.getColStatsSize());
        ColumnStatisticsObj cso = aggrStats.getColStats().get(0);
        Assert.assertEquals("col5_decimal", cso.getColName());
        Assert.assertEquals("decimal", cso.getColType());
        DecimalColumnStatsData lcsd = cso.getStatsData().getDecimalStats();
        Assert.assertEquals(1010, HBaseUtils.getDoubleValue(lcsd.getHighValue()), 0.01);
        Assert.assertEquals(-1010, HBaseUtils.getDoubleValue(lcsd.getLowValue()), 0.01);
        Assert.assertEquals(40, lcsd.getNumNulls());
        Assert.assertEquals(12, lcsd.getNumDVs());
      }
    };
    List<String> partNames = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      partNames.add("ds=" + i);
    }
    AggrStats aggrStats = store.get_aggr_stats_for(dbName, tableName, partNames,
        Arrays.asList("col5_decimal"));
    statChecker.checkStats(aggrStats);
  }

  @Test
  public void TwoEndsAndMiddleOfPartitionsHaveBitVectorStatusDouble() throws Exception {
    String dbName = "default";
    String tableName = "snp";
    long now = System.currentTimeMillis();
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col5_double", "double", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, Collections.<String, String> emptyMap());
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName, dbName, "me", (int) now, (int) now, 0, sd, partCols,
        Collections.<String, String> emptyMap(), null, null, null);
    store.createTable(table);

    List<List<String>> partVals = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      List<String> partVal = Arrays.asList("" + i);
      partVals.add(partVal);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/default/hit/ds=" + partVal);
      Partition part = new Partition(partVal, dbName, tableName, (int) now, (int) now, psd,
          Collections.<String, String> emptyMap());
      store.addPartition(part);
      if (i == 0 || i == 2 || i == 3 || i == 5 || i == 6 || i == 8) {
        ColumnStatistics cs = new ColumnStatistics();
        ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
        desc.setLastAnalyzed(now);
        desc.setPartName("ds=" + partVal);
        cs.setStatsDesc(desc);
        ColumnStatisticsObj obj = new ColumnStatisticsObj();
        obj.setColName("col5_double");
        obj.setColType("double");
        ColumnStatisticsData data = new ColumnStatisticsData();
        DoubleColumnStatsData dcsd = new DoubleColumnStatsData();
        dcsd.setHighValue(1000 + i);
        dcsd.setLowValue(-1000 - i);
        dcsd.setNumNulls(i);
        dcsd.setNumDVs(10 * i + 1);
        dcsd.setBitVectors(bitVectors[i / 5]);
        data.setDoubleStats(dcsd);
        obj.setStatsData(data);
        cs.addToStatsObj(obj);
        store.updatePartitionColumnStatistics(cs, partVal);
      }
    }

    Checker statChecker = new Checker() {
      @Override
      public void checkStats(AggrStats aggrStats) throws Exception {
        Assert.assertEquals(6, aggrStats.getPartsFound());
        Assert.assertEquals(1, aggrStats.getColStatsSize());
        ColumnStatisticsObj cso = aggrStats.getColStats().get(0);
        Assert.assertEquals("col5_double", cso.getColName());
        Assert.assertEquals("double", cso.getColType());
        DoubleColumnStatsData lcsd = cso.getStatsData().getDoubleStats();
        Assert.assertEquals(1010, lcsd.getHighValue(), 0.01);
        Assert.assertEquals(-1010, lcsd.getLowValue(), 0.01);
        Assert.assertEquals(40, lcsd.getNumNulls());
        Assert.assertEquals(12, lcsd.getNumDVs());
      }
    };
    List<String> partNames = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      partNames.add("ds=" + i);
    }
    AggrStats aggrStats = store.get_aggr_stats_for(dbName, tableName, partNames,
        Arrays.asList("col5_double"));
    statChecker.checkStats(aggrStats);
  }
}
