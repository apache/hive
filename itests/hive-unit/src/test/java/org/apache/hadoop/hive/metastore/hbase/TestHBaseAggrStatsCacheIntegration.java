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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Integration tests with HBase Mini-cluster for HBaseStore
 */
public class TestHBaseAggrStatsCacheIntegration extends HBaseIntegrationTests {

  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseStoreIntegration.class.getName());

  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void startup() throws Exception {
    HBaseIntegrationTests.startMiniCluster();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    HBaseIntegrationTests.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    setupConnection();
    setupHBaseStore();
    store.backdoor().getStatsCache().resetCounters();
  }

  private static interface Checker {
    void checkStats(AggrStats aggrStats) throws Exception;
  }

  @Test
  public void hit() throws Exception {
    String dbName = "default";
    String tableName = "hit";
    List<String> partVals1 = Arrays.asList("today");
    List<String> partVals2 = Arrays.asList("yesterday");
    long now = System.currentTimeMillis();

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col1", "boolean", "nocomment"));
    cols.add(new FieldSchema("col2", "varchar", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, Collections.<String, String>emptyMap());
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName, dbName, "me", (int)now, (int)now, 0, sd, partCols,
        Collections.<String, String>emptyMap(), null, null, null);
    store.createTable(table);

    for (List<String> partVals : Arrays.asList(partVals1, partVals2)) {
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/default/hit/ds=" + partVals.get(0));
      Partition part = new Partition(partVals, dbName, tableName, (int) now, (int) now, psd,
          Collections.<String, String>emptyMap());
      store.addPartition(part);

      ColumnStatistics cs = new ColumnStatistics();
      ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
      desc.setLastAnalyzed(now);
      desc.setPartName("ds=" + partVals.get(0));
      cs.setStatsDesc(desc);
      ColumnStatisticsObj obj = new ColumnStatisticsObj();
      obj.setColName("col1");
      obj.setColType("boolean");
      ColumnStatisticsData data = new ColumnStatisticsData();
      BooleanColumnStatsData bcsd = new BooleanColumnStatsData();
      bcsd.setNumFalses(10);
      bcsd.setNumTrues(20);
      bcsd.setNumNulls(30);
      data.setBooleanStats(bcsd);
      obj.setStatsData(data);
      cs.addToStatsObj(obj);

      obj = new ColumnStatisticsObj();
      obj.setColName("col2");
      obj.setColType("varchar");
      data = new ColumnStatisticsData();
      StringColumnStatsData scsd = new StringColumnStatsData();
      scsd.setAvgColLen(10.3);
      scsd.setMaxColLen(2000);
      scsd.setNumNulls(3);
      scsd.setNumDVs(12342);
      data.setStringStats(scsd);
      obj.setStatsData(data);
      cs.addToStatsObj(obj);

      store.updatePartitionColumnStatistics(cs, partVals);
    }

    Checker statChecker = new Checker() {
      @Override
      public void checkStats(AggrStats aggrStats) throws Exception {
        Assert.assertEquals(2, aggrStats.getPartsFound());
        Assert.assertEquals(2, aggrStats.getColStatsSize());
        ColumnStatisticsObj cso = aggrStats.getColStats().get(0);
        Assert.assertEquals("col1", cso.getColName());
        Assert.assertEquals("boolean", cso.getColType());
        BooleanColumnStatsData bcsd = cso.getStatsData().getBooleanStats();
        Assert.assertEquals(20, bcsd.getNumFalses());
        Assert.assertEquals(40, bcsd.getNumTrues());
        Assert.assertEquals(60, bcsd.getNumNulls());

        cso = aggrStats.getColStats().get(1);
        Assert.assertEquals("col2", cso.getColName());
        Assert.assertEquals("varchar", cso.getColType());
        StringColumnStatsData scsd = cso.getStatsData().getStringStats();
        Assert.assertEquals(10.3, scsd.getAvgColLen(), 0.1);
        Assert.assertEquals(2000, scsd.getMaxColLen());
        Assert.assertEquals(6, scsd.getNumNulls());
        Assert.assertEquals(12342, scsd.getNumDVs());
      }
    };

    AggrStats aggrStats = store.get_aggr_stats_for(dbName, tableName,
        Arrays.asList("ds=today", "ds=yesterday"), Arrays.asList("col1", "col2"));
    statChecker.checkStats(aggrStats);

    // Check that we had to build it from the stats
    Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
    Assert.assertEquals(2, store.backdoor().getStatsCache().totalGets.getCnt());
    Assert.assertEquals(2, store.backdoor().getStatsCache().misses.getCnt());

    // Call again, this time it should come from memory.  Also, reverse the name order this time
    // to assure that we still hit.
    aggrStats = store.get_aggr_stats_for(dbName, tableName,
        Arrays.asList("ds=yesterday", "ds=today"), Arrays.asList("col1", "col2"));
    statChecker.checkStats(aggrStats);

    Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
    Assert.assertEquals(4, store.backdoor().getStatsCache().totalGets.getCnt());
    Assert.assertEquals(2, store.backdoor().getStatsCache().misses.getCnt());

    store.backdoor().getStatsCache().flushMemory();
    // Call again, this time it should come from hbase
    aggrStats = store.get_aggr_stats_for(dbName, tableName,
        Arrays.asList("ds=today", "ds=yesterday"), Arrays.asList("col1", "col2"));
    statChecker.checkStats(aggrStats);

    Assert.assertEquals(2, store.backdoor().getStatsCache().hbaseHits.getCnt());
    Assert.assertEquals(6, store.backdoor().getStatsCache().totalGets.getCnt());
    Assert.assertEquals(2, store.backdoor().getStatsCache().misses.getCnt());
  }

  @Test
  public void someWithStats() throws Exception {
    String dbName = "default";
    String tableName = "psws";
    List<String> partVals1 = Arrays.asList("today");
    List<String> partVals2 = Arrays.asList("yesterday");
    long now = System.currentTimeMillis();

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col1", "long", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, Collections.<String, String>emptyMap());
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName, dbName, "me", (int)now, (int)now, 0, sd, partCols,
        Collections.<String, String>emptyMap(), null, null, null);
    store.createTable(table);

    boolean first = true;
    for (List<String> partVals : Arrays.asList(partVals1, partVals2)) {
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/default/psws/ds=" + partVals.get(0));
      Partition part = new Partition(partVals, dbName, tableName, (int) now, (int) now, psd,
          Collections.<String, String>emptyMap());
      store.addPartition(part);

      if (first) {
        ColumnStatistics cs = new ColumnStatistics();
        ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
        desc.setLastAnalyzed(now);
        desc.setPartName("ds=" + partVals.get(0));
        cs.setStatsDesc(desc);
        ColumnStatisticsObj obj = new ColumnStatisticsObj();
        obj.setColName("col1");
        obj.setColType("long");
        ColumnStatisticsData data = new ColumnStatisticsData();
        LongColumnStatsData lcsd = new LongColumnStatsData();
        lcsd.setHighValue(192L);
        lcsd.setLowValue(-20L);
        lcsd.setNumNulls(30);
        lcsd.setNumDVs(32);
        data.setLongStats(lcsd);
        obj.setStatsData(data);
        cs.addToStatsObj(obj);

        store.updatePartitionColumnStatistics(cs, partVals);
        first = false;
      }
    }

    Checker statChecker = new Checker() {
      @Override
      public void checkStats(AggrStats aggrStats) throws Exception {
        Assert.assertEquals(1, aggrStats.getPartsFound());
        Assert.assertEquals(1, aggrStats.getColStatsSize());
        ColumnStatisticsObj cso = aggrStats.getColStats().get(0);
        Assert.assertEquals("col1", cso.getColName());
        Assert.assertEquals("long", cso.getColType());
        LongColumnStatsData lcsd = cso.getStatsData().getLongStats();
        Assert.assertEquals(192L, lcsd.getHighValue());
        Assert.assertEquals(-20L, lcsd.getLowValue());
        Assert.assertEquals(30, lcsd.getNumNulls());
        Assert.assertEquals(32, lcsd.getNumDVs());
      }
    };

    AggrStats aggrStats = store.get_aggr_stats_for(dbName, tableName,
        Arrays.asList("ds=today", "ds=yesterday"), Arrays.asList("col1"));
    statChecker.checkStats(aggrStats);

    // Check that we had to build it from the stats
    Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
    Assert.assertEquals(1, store.backdoor().getStatsCache().totalGets.getCnt());
    Assert.assertEquals(1, store.backdoor().getStatsCache().misses.getCnt());

    // Call again, this time it should come from memory.  Also, reverse the name order this time
    // to assure that we still hit.
    aggrStats = store.get_aggr_stats_for(dbName, tableName,
        Arrays.asList("ds=yesterday", "ds=today"), Arrays.asList("col1"));
    statChecker.checkStats(aggrStats);

    Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
    Assert.assertEquals(2, store.backdoor().getStatsCache().totalGets.getCnt());
    Assert.assertEquals(1, store.backdoor().getStatsCache().misses.getCnt());

    store.backdoor().getStatsCache().flushMemory();
    // Call again, this time it should come from hbase
    aggrStats = store.get_aggr_stats_for(dbName, tableName,
        Arrays.asList("ds=today", "ds=yesterday"), Arrays.asList("col1"));
    statChecker.checkStats(aggrStats);

    Assert.assertEquals(1, store.backdoor().getStatsCache().hbaseHits.getCnt());
    Assert.assertEquals(3, store.backdoor().getStatsCache().totalGets.getCnt());
    Assert.assertEquals(1, store.backdoor().getStatsCache().misses.getCnt());
  }

  @Test
  public void invalidation() throws Exception {
    try {
      String dbName = "default";
      String tableName = "invalidation";
      List<String> partVals1 = Arrays.asList("today");
      List<String> partVals2 = Arrays.asList("yesterday");
      List<String> partVals3 = Arrays.asList("tomorrow");
      long now = System.currentTimeMillis();

      List<FieldSchema> cols = new ArrayList<>();
      cols.add(new FieldSchema("col1", "boolean", "nocomment"));
      SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
      StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
          serde, null, null, Collections.<String, String>emptyMap());
      List<FieldSchema> partCols = new ArrayList<>();
      partCols.add(new FieldSchema("ds", "string", ""));
      Table table = new Table(tableName, dbName, "me", (int) now, (int) now, 0, sd, partCols,
          Collections.<String, String>emptyMap(), null, null, null);
      store.createTable(table);

      for (List<String> partVals : Arrays.asList(partVals1, partVals2, partVals3)) {
        StorageDescriptor psd = new StorageDescriptor(sd);
        psd.setLocation("file:/tmp/default/invalidation/ds=" + partVals.get(0));
        Partition part = new Partition(partVals, dbName, tableName, (int) now, (int) now, psd,
            Collections.<String, String>emptyMap());
        store.addPartition(part);

        ColumnStatistics cs = new ColumnStatistics();
        ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
        desc.setLastAnalyzed(now);
        desc.setPartName("ds=" + partVals.get(0));
        cs.setStatsDesc(desc);
        ColumnStatisticsObj obj = new ColumnStatisticsObj();
        obj.setColName("col1");
        obj.setColType("boolean");
        ColumnStatisticsData data = new ColumnStatisticsData();
        BooleanColumnStatsData bcsd = new BooleanColumnStatsData();
        bcsd.setNumFalses(10);
        bcsd.setNumTrues(20);
        bcsd.setNumNulls(30);
        data.setBooleanStats(bcsd);
        obj.setStatsData(data);
        cs.addToStatsObj(obj);

        store.updatePartitionColumnStatistics(cs, partVals);
      }

      Checker statChecker = new Checker() {
        @Override
        public void checkStats(AggrStats aggrStats) throws Exception {
          Assert.assertEquals(2, aggrStats.getPartsFound());
          Assert.assertEquals(1, aggrStats.getColStatsSize());
          ColumnStatisticsObj cso = aggrStats.getColStats().get(0);
          Assert.assertEquals("col1", cso.getColName());
          Assert.assertEquals("boolean", cso.getColType());
          BooleanColumnStatsData bcsd = cso.getStatsData().getBooleanStats();
          Assert.assertEquals(20, bcsd.getNumFalses());
          Assert.assertEquals(40, bcsd.getNumTrues());
          Assert.assertEquals(60, bcsd.getNumNulls());
        }
      };

      AggrStats aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=today", "ds=yesterday"), Arrays.asList("col1"));
      statChecker.checkStats(aggrStats);

      // Check that we had to build it from the stats
      Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
      Assert.assertEquals(1, store.backdoor().getStatsCache().totalGets.getCnt());
      Assert.assertEquals(1, store.backdoor().getStatsCache().misses.getCnt());

      // Call again, this time it should come from memory.  Also, reverse the name order this time
      // to assure that we still hit.
      aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=yesterday", "ds=today"), Arrays.asList("col1"));
      statChecker.checkStats(aggrStats);

      Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
      Assert.assertEquals(2, store.backdoor().getStatsCache().totalGets.getCnt());
      Assert.assertEquals(1, store.backdoor().getStatsCache().misses.getCnt());

      // Now call a different combination to get it in memory too
      aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=tomorrow", "ds=today"), Arrays.asList("col1"));
      statChecker.checkStats(aggrStats);

      Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
      Assert.assertEquals(3, store.backdoor().getStatsCache().totalGets.getCnt());
      Assert.assertEquals(2, store.backdoor().getStatsCache().misses.getCnt());

      aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=tomorrow", "ds=today"), Arrays.asList("col1"));
      statChecker.checkStats(aggrStats);

      Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
      Assert.assertEquals(4, store.backdoor().getStatsCache().totalGets.getCnt());
      Assert.assertEquals(2, store.backdoor().getStatsCache().misses.getCnt());

      // wake the invalidator and check again to make sure it isn't too aggressive about
      // removing our stuff.
      store.backdoor().getStatsCache().wakeInvalidator();

      aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=tomorrow", "ds=today"), Arrays.asList("col1"));
      statChecker.checkStats(aggrStats);

      Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
      Assert.assertEquals(5, store.backdoor().getStatsCache().totalGets.getCnt());
      Assert.assertEquals(2, store.backdoor().getStatsCache().misses.getCnt());

      // Update statistics for 'tomorrow'
      ColumnStatistics cs = new ColumnStatistics();
      ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
      desc.setLastAnalyzed(now);
      desc.setPartName("ds=" + partVals3.get(0));
      cs.setStatsDesc(desc);
      ColumnStatisticsObj obj = new ColumnStatisticsObj();
      obj.setColName("col1");
      obj.setColType("boolean");
      ColumnStatisticsData data = new ColumnStatisticsData();
      BooleanColumnStatsData bcsd = new BooleanColumnStatsData();
      bcsd.setNumFalses(100);
      bcsd.setNumTrues(200);
      bcsd.setNumNulls(300);
      data.setBooleanStats(bcsd);
      obj.setStatsData(data);
      cs.addToStatsObj(obj);

      Checker afterUpdate = new Checker() {
        @Override
        public void checkStats(AggrStats aggrStats) throws Exception {
          Assert.assertEquals(2, aggrStats.getPartsFound());
          Assert.assertEquals(1, aggrStats.getColStatsSize());
          ColumnStatisticsObj cso = aggrStats.getColStats().get(0);
          Assert.assertEquals("col1", cso.getColName());
          Assert.assertEquals("boolean", cso.getColType());
          BooleanColumnStatsData bcsd = cso.getStatsData().getBooleanStats();
          Assert.assertEquals(110, bcsd.getNumFalses());
          Assert.assertEquals(220, bcsd.getNumTrues());
          Assert.assertEquals(330, bcsd.getNumNulls());
        }
      };

      store.updatePartitionColumnStatistics(cs, partVals3);

      store.backdoor().getStatsCache().setRunInvalidatorEvery(100);
      store.backdoor().getStatsCache().wakeInvalidator();

      aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=tomorrow", "ds=today"), Arrays.asList("col1"));
      afterUpdate.checkStats(aggrStats);

      // Check that we missed, which means this aggregate was dropped from the cache.
      Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
      Assert.assertEquals(6, store.backdoor().getStatsCache().totalGets.getCnt());
      Assert.assertEquals(3, store.backdoor().getStatsCache().misses.getCnt());

      // Check that our other aggregate is still in the cache.
      aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=yesterday", "ds=today"), Arrays.asList("col1"));
      statChecker.checkStats(aggrStats);

      Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
      Assert.assertEquals(7, store.backdoor().getStatsCache().totalGets.getCnt());
      Assert.assertEquals(3, store.backdoor().getStatsCache().misses.getCnt());

      // Drop 'yesterday', so our first aggregate should be dumped from memory and hbase
      store.dropPartition(dbName, tableName, partVals2);

      store.backdoor().getStatsCache().wakeInvalidator();

      aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=yesterday", "ds=today"), Arrays.asList("col1"));
      new Checker() {
        @Override
        public void checkStats(AggrStats aggrStats) throws Exception {
          Assert.assertEquals(1, aggrStats.getPartsFound());
          Assert.assertEquals(1, aggrStats.getColStatsSize());
          ColumnStatisticsObj cso = aggrStats.getColStats().get(0);
          Assert.assertEquals("col1", cso.getColName());
          Assert.assertEquals("boolean", cso.getColType());
          BooleanColumnStatsData bcsd = cso.getStatsData().getBooleanStats();
          Assert.assertEquals(10, bcsd.getNumFalses());
          Assert.assertEquals(20, bcsd.getNumTrues());
          Assert.assertEquals(30, bcsd.getNumNulls());
        }
      }.checkStats(aggrStats);

      // Check that we missed, which means this aggregate was dropped from the cache.
      Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
      Assert.assertEquals(8, store.backdoor().getStatsCache().totalGets.getCnt());
      Assert.assertEquals(4, store.backdoor().getStatsCache().misses.getCnt());

      // Check that our other aggregate is still in the cache.
      aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=tomorrow", "ds=today"), Arrays.asList("col1"));
      afterUpdate.checkStats(aggrStats);

      Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
      Assert.assertEquals(9, store.backdoor().getStatsCache().totalGets.getCnt());
      Assert.assertEquals(4, store.backdoor().getStatsCache().misses.getCnt());
    } finally {
      store.backdoor().getStatsCache().setRunInvalidatorEvery(5000);
      store.backdoor().getStatsCache().setMaxTimeInCache(500000);
      store.backdoor().getStatsCache().wakeInvalidator();
    }
  }

  @Test
  public void alterInvalidation() throws Exception {
    try {
      String dbName = "default";
      String tableName = "ai";
      List<String> partVals1 = Arrays.asList("today");
      List<String> partVals2 = Arrays.asList("yesterday");
      List<String> partVals3 = Arrays.asList("tomorrow");
      long now = System.currentTimeMillis();

      List<FieldSchema> cols = new ArrayList<>();
      cols.add(new FieldSchema("col1", "boolean", "nocomment"));
      SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
      StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
          serde, null, null, Collections.<String, String>emptyMap());
      List<FieldSchema> partCols = new ArrayList<>();
      partCols.add(new FieldSchema("ds", "string", ""));
      Table table = new Table(tableName, dbName, "me", (int) now, (int) now, 0, sd, partCols,
          Collections.<String, String>emptyMap(), null, null, null);
      store.createTable(table);

      Partition[] partitions = new Partition[3];
      int partnum = 0;
      for (List<String> partVals : Arrays.asList(partVals1, partVals2, partVals3)) {
        StorageDescriptor psd = new StorageDescriptor(sd);
        psd.setLocation("file:/tmp/default/invalidation/ds=" + partVals.get(0));
        Partition part = new Partition(partVals, dbName, tableName, (int) now, (int) now, psd,
            Collections.<String, String>emptyMap());
        partitions[partnum++] = part;
        store.addPartition(part);

        ColumnStatistics cs = new ColumnStatistics();
        ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
        desc.setLastAnalyzed(now);
        desc.setPartName("ds=" + partVals.get(0));
        cs.setStatsDesc(desc);
        ColumnStatisticsObj obj = new ColumnStatisticsObj();
        obj.setColName("col1");
        obj.setColType("boolean");
        ColumnStatisticsData data = new ColumnStatisticsData();
        BooleanColumnStatsData bcsd = new BooleanColumnStatsData();
        bcsd.setNumFalses(10);
        bcsd.setNumTrues(20);
        bcsd.setNumNulls(30);
        data.setBooleanStats(bcsd);
        obj.setStatsData(data);
        cs.addToStatsObj(obj);

        store.updatePartitionColumnStatistics(cs, partVals);
      }

      AggrStats aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=today", "ds=tomorrow"), Arrays.asList("col1"));
      aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=today", "ds=yesterday"), Arrays.asList("col1"));

      // Check that we had to build it from the stats
      Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
      Assert.assertEquals(2, store.backdoor().getStatsCache().totalGets.getCnt());
      Assert.assertEquals(2, store.backdoor().getStatsCache().misses.getCnt());

      // wake the invalidator and check again to make sure it isn't too aggressive about
      // removing our stuff.
      store.backdoor().getStatsCache().wakeInvalidator();

      Partition newPart = new Partition(partitions[2]);
      newPart.setLastAccessTime((int)System.currentTimeMillis());
      store.alterPartition(dbName, tableName, partVals3, newPart);

      store.backdoor().getStatsCache().setRunInvalidatorEvery(100);
      store.backdoor().getStatsCache().wakeInvalidator();

      aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=tomorrow", "ds=today"), Arrays.asList("col1"));

      // Check that we missed, which means this aggregate was dropped from the cache.
      Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
      Assert.assertEquals(3, store.backdoor().getStatsCache().totalGets.getCnt());
      Assert.assertEquals(3, store.backdoor().getStatsCache().misses.getCnt());

      // Check that our other aggregate is still in the cache.
      aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=yesterday", "ds=today"), Arrays.asList("col1"));

      Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
      Assert.assertEquals(4, store.backdoor().getStatsCache().totalGets.getCnt());
      Assert.assertEquals(3, store.backdoor().getStatsCache().misses.getCnt());
    } finally {
      store.backdoor().getStatsCache().setRunInvalidatorEvery(5000);
      store.backdoor().getStatsCache().setMaxTimeInCache(500000);
      store.backdoor().getStatsCache().wakeInvalidator();
    }
  }

  @Test
  public void altersInvalidation() throws Exception {
    try {
      String dbName = "default";
      String tableName = "asi";
      List<String> partVals1 = Arrays.asList("today");
      List<String> partVals2 = Arrays.asList("yesterday");
      List<String> partVals3 = Arrays.asList("tomorrow");
      long now = System.currentTimeMillis();

      List<FieldSchema> cols = new ArrayList<>();
      cols.add(new FieldSchema("col1", "boolean", "nocomment"));
      SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
      StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
          serde, null, null, Collections.<String, String>emptyMap());
      List<FieldSchema> partCols = new ArrayList<>();
      partCols.add(new FieldSchema("ds", "string", ""));
      Table table = new Table(tableName, dbName, "me", (int) now, (int) now, 0, sd, partCols,
          Collections.<String, String>emptyMap(), null, null, null);
      store.createTable(table);

      Partition[] partitions = new Partition[3];
      int partnum = 0;
      for (List<String> partVals : Arrays.asList(partVals1, partVals2, partVals3)) {
        StorageDescriptor psd = new StorageDescriptor(sd);
        psd.setLocation("file:/tmp/default/invalidation/ds=" + partVals.get(0));
        Partition part = new Partition(partVals, dbName, tableName, (int) now, (int) now, psd,
            Collections.<String, String>emptyMap());
        partitions[partnum++] = part;
        store.addPartition(part);

        ColumnStatistics cs = new ColumnStatistics();
        ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
        desc.setLastAnalyzed(now);
        desc.setPartName("ds=" + partVals.get(0));
        cs.setStatsDesc(desc);
        ColumnStatisticsObj obj = new ColumnStatisticsObj();
        obj.setColName("col1");
        obj.setColType("boolean");
        ColumnStatisticsData data = new ColumnStatisticsData();
        BooleanColumnStatsData bcsd = new BooleanColumnStatsData();
        bcsd.setNumFalses(10);
        bcsd.setNumTrues(20);
        bcsd.setNumNulls(30);
        data.setBooleanStats(bcsd);
        obj.setStatsData(data);
        cs.addToStatsObj(obj);

        store.updatePartitionColumnStatistics(cs, partVals);
      }

      AggrStats aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=today", "ds=tomorrow"), Arrays.asList("col1"));
      aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=today", "ds=yesterday"), Arrays.asList("col1"));

      // Check that we had to build it from the stats
      Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
      Assert.assertEquals(2, store.backdoor().getStatsCache().totalGets.getCnt());
      Assert.assertEquals(2, store.backdoor().getStatsCache().misses.getCnt());

      // wake the invalidator and check again to make sure it isn't too aggressive about
      // removing our stuff.
      store.backdoor().getStatsCache().wakeInvalidator();

      Partition[] newParts = new Partition[2];
      newParts[0] = new Partition(partitions[0]);
      newParts[0].setLastAccessTime((int)System.currentTimeMillis());
      newParts[1] = new Partition(partitions[2]);
      newParts[1].setLastAccessTime((int) System.currentTimeMillis());
      store.alterPartitions(dbName, tableName, Arrays.asList(partVals1, partVals3),
          Arrays.asList(newParts));

      store.backdoor().getStatsCache().setRunInvalidatorEvery(100);
      store.backdoor().getStatsCache().wakeInvalidator();

      aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=tomorrow", "ds=today"), Arrays.asList("col1"));

      // Check that we missed, which means this aggregate was dropped from the cache.
      Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
      Assert.assertEquals(3, store.backdoor().getStatsCache().totalGets.getCnt());
      Assert.assertEquals(3, store.backdoor().getStatsCache().misses.getCnt());

      // Check that our other aggregate got dropped too
      aggrStats = store.get_aggr_stats_for(dbName, tableName,
          Arrays.asList("ds=yesterday", "ds=today"), Arrays.asList("col1"));

      Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
      Assert.assertEquals(4, store.backdoor().getStatsCache().totalGets.getCnt());
      Assert.assertEquals(4, store.backdoor().getStatsCache().misses.getCnt());
    } finally {
      store.backdoor().getStatsCache().setRunInvalidatorEvery(5000);
      store.backdoor().getStatsCache().setMaxTimeInCache(500000);
      store.backdoor().getStatsCache().wakeInvalidator();
    }
  }
}
