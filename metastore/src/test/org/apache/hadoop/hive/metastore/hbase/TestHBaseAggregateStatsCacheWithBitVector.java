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
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
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

public class TestHBaseAggregateStatsCacheWithBitVector {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestHBaseAggregateStatsCacheWithBitVector.class.getName());

  @Mock
  HTableInterface htable;
  private HBaseStore store;
  SortedMap<String, Cell> rows = new TreeMap<>();

  @Before
  public void before() throws IOException {
    MockitoAnnotations.initMocks(this);
    HiveConf conf = new HiveConf();
    conf.setBoolean(HBaseReadWrite.NO_CACHE_CONF, true);
    store = MockUtils.init(conf, htable, rows);
    store.backdoor().getStatsCache().resetCounters();
  }

  private static interface Checker {
    void checkStats(AggrStats aggrStats) throws Exception;
  }

  @Test
  public void allPartitions() throws Exception {
    String dbName = "default";
    String tableName = "snp";
    List<String> partVals1 = Arrays.asList("today");
    List<String> partVals2 = Arrays.asList("yesterday");
    long now = System.currentTimeMillis();

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col1", "boolean", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, Collections.<String, String> emptyMap());
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName, dbName, "me", (int) now, (int) now, 0, sd, partCols,
        Collections.<String, String> emptyMap(), null, null, null);
    store.createTable(table);

    StorageDescriptor psd = new StorageDescriptor(sd);
    psd.setLocation("file:/tmp/default/hit/ds=" + partVals1.get(0));
    Partition part = new Partition(partVals1, dbName, tableName, (int) now, (int) now, psd,
        Collections.<String, String> emptyMap());
    store.addPartition(part);

    psd = new StorageDescriptor(sd);
    psd.setLocation("file:/tmp/default/hit/ds=" + partVals2.get(0));
    part = new Partition(partVals2, dbName, tableName, (int) now, (int) now, psd,
        Collections.<String, String> emptyMap());
    store.addPartition(part);

    ColumnStatistics cs = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
    desc.setLastAnalyzed(now);
    desc.setPartName("ds=" + partVals1.get(0));
    cs.setStatsDesc(desc);
    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName("col1");
    obj.setColType("double");
    ColumnStatisticsData data = new ColumnStatisticsData();
    DoubleColumnStatsData dcsd = new DoubleColumnStatsData();
    dcsd.setHighValue(1000.2342343);
    dcsd.setLowValue(-20.1234213423);
    dcsd.setNumNulls(30);
    dcsd.setNumDVs(12342);
    dcsd.setBitVectors("{0, 4, 5, 7}{0, 1}{0, 1, 2}{0, 1, 4}{0}{0, 2}{0, 3}{0, 2, 3, 4}{0, 1, 4}{0, 1}{0}{0, 1, 3, 8}{0, 2}{0, 2}{0, 9}{0, 1, 4}");
    data.setDoubleStats(dcsd);
    obj.setStatsData(data);
    cs.addToStatsObj(obj);
    store.updatePartitionColumnStatistics(cs, partVals1);

    cs = new ColumnStatistics();
    desc = new ColumnStatisticsDesc(false, dbName, tableName);
    desc.setLastAnalyzed(now);
    desc.setPartName("ds=" + partVals2.get(0));
    cs.setStatsDesc(desc);
    obj = new ColumnStatisticsObj();
    obj.setColName("col1");
    obj.setColType("double");
    data = new ColumnStatisticsData();
    dcsd = new DoubleColumnStatsData();
    dcsd.setHighValue(1000.2342343);
    dcsd.setLowValue(-20.1234213423);
    dcsd.setNumNulls(30);
    dcsd.setNumDVs(12342);
    dcsd.setBitVectors("{0, 1}{0, 1}{1, 2, 4}{0, 1, 2}{0, 1, 2}{0, 2}{0, 1, 3, 4}{0, 1}{0, 1}{3, 4, 6}{2}{0, 1}{0, 3}{0}{0, 1}{0, 1, 4}");
    data.setDoubleStats(dcsd);
    obj.setStatsData(data);
    cs.addToStatsObj(obj);

    store.updatePartitionColumnStatistics(cs, partVals2);

    Checker statChecker = new Checker() {
      @Override
      public void checkStats(AggrStats aggrStats) throws Exception {
        Assert.assertEquals(2, aggrStats.getPartsFound());
        Assert.assertEquals(1, aggrStats.getColStatsSize());
        ColumnStatisticsObj cso = aggrStats.getColStats().get(0);
        Assert.assertEquals("col1", cso.getColName());
        Assert.assertEquals("double", cso.getColType());
        DoubleColumnStatsData dcsd = cso.getStatsData().getDoubleStats();
        Assert.assertEquals(1000.23, dcsd.getHighValue(), 0.01);
        Assert.assertEquals(-20.12, dcsd.getLowValue(), 0.01);
        Assert.assertEquals(60, dcsd.getNumNulls());
        Assert.assertEquals(5, dcsd.getNumDVs());
        // we do not store the bitvector for the aggrStats.
        // we can store that if it is necessary in the future.
      }
    };

    AggrStats aggrStats = store.get_aggr_stats_for(dbName, tableName,
        Arrays.asList("ds=today", "ds=yesterday"), Arrays.asList("col1"));
    statChecker.checkStats(aggrStats);

    // Check that we had to build it from the stats
    Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
    Assert.assertEquals(1, store.backdoor().getStatsCache().totalGets.getCnt());
    Assert.assertEquals(1, store.backdoor().getStatsCache().misses.getCnt());

    // Call again, this time it should come from memory. Also, reverse the name
    // order this time
    // to assure that we still hit.
    aggrStats = store.get_aggr_stats_for(dbName, tableName,
        Arrays.asList("ds=yesterday", "ds=today"), Arrays.asList("col1"));
    statChecker.checkStats(aggrStats);

    Assert.assertEquals(0, store.backdoor().getStatsCache().hbaseHits.getCnt());
    Assert.assertEquals(2, store.backdoor().getStatsCache().totalGets.getCnt());
    Assert.assertEquals(1, store.backdoor().getStatsCache().misses.getCnt());
  }

}
