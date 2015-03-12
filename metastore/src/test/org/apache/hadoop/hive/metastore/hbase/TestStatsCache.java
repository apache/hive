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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestStatsCache {
  private static final Log LOG = LogFactory.getLog(TestStatsCache.class.getName());

  @Mock HTableInterface htable;
  static Put[] puts = new Put[2];
  HBaseReadWrite hrw;

  @Before
  public void before() throws IOException {
    MockitoAnnotations.initMocks(this);

    // For reasons I don't understand we have to do the mockito setup here in before, so we allow
    // each method to place one put in puts[], and then we return that.
    Mockito.when(htable.get(Mockito.any(Get.class))).thenAnswer(new Answer<Result>() {
      @Override
      public Result answer(InvocationOnMock invocation) throws Throwable {
        List<Cell> cells = new ArrayList<Cell>();
        if (puts[0] == null) return new Result();
        for (Cell cell : puts[0].getFamilyCellMap().firstEntry().getValue()) {
          cells.add(cell);
        }
        return Result.create(cells);
      }
    });

    Mockito.when(htable.get(Mockito.anyList())).thenAnswer(new Answer<Result[]>() {
      @Override
      public Result[] answer(InvocationOnMock invocation) throws Throwable {
        Result[] results = new Result[2];
        for (int i = 0; i < 2; i++) {
          List<Cell> cells = new ArrayList<Cell>();
          if (puts[i] == null) {
            results[i] = new Result();
          } else {
            for (Cell cell : puts[i].getFamilyCellMap().firstEntry().getValue()) {
              cells.add(cell);
            }
            results[i] = Result.create(cells);
          }
        }
        return results;
      }
    });

    HBaseConnection hconn = Mockito.mock(HBaseConnection.class);
    Mockito.when(hconn.getHBaseTable(Mockito.anyString())).thenReturn(htable);
    HiveConf conf = new HiveConf();
    conf.setIntVar(HiveConf.ConfVars.METASTORE_HBASE_CACHE_SIZE, 30);
    conf.setVar(HiveConf.ConfVars.METASTORE_HBASE_CONNECTION_CLASS, HBaseReadWrite.TEST_CONN);
    HBaseReadWrite.setTestConnection(hconn);
    hrw = HBaseReadWrite.getInstance(conf);
    StatsCache.getInstance(conf).clear();
    puts[0] = puts[1] = null;
  }

  @Test
  public void tableAllHit() throws IOException {
    String dbName = "default";
    String tableName = "mytable";
    long now = System.currentTimeMillis();

    ColumnStatistics cs = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(true, dbName, tableName);
    desc.setLastAnalyzed(now);
    cs.setStatsDesc(desc);
    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName("col1");
    obj.setColType("boolean");
    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(new BooleanColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);

    obj = new ColumnStatisticsObj();
    obj.setColName("col2");
    obj.setColType("long");
    data = new ColumnStatisticsData();
    data.setLongStats(new LongColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);

    hrw.updateStatistics(dbName, tableName, null, null, cs);

    cs = new ColumnStatistics();
    desc = new ColumnStatisticsDesc(true, dbName, tableName);
    desc.setLastAnalyzed(now);
    cs.setStatsDesc(desc);
    obj = new ColumnStatisticsObj();
    obj.setColName("col3");
    obj.setColType("double");
    data = new ColumnStatisticsData();
    data.setDoubleStats(new DoubleColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);

    hrw.updateStatistics(dbName, tableName, null, null, cs);

    // Now, ask for all 3 of these.  We should hit all on the cache.  We'll know if we don't
    // because we've mocked hbase and it will return null on the get.
    cs = hrw.getTableStatistics(dbName, tableName, Arrays.asList("col1", "col2", "col3"));

    Assert.assertEquals(now, cs.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(dbName, cs.getStatsDesc().getDbName());
    Assert.assertEquals(tableName, cs.getStatsDesc().getTableName());
    Assert.assertTrue(cs.getStatsDesc().isIsTblLevel());

    // There's no need to check every last field in each obj, as the objects aren't de/serialized
    // in the cache.  Just make sure we found the objects we expected.
    Assert.assertEquals(3, cs.getStatsObjSize());
    for (ColumnStatisticsObj csobj : cs.getStatsObj()) {
      if (csobj.getColName().equals("col1")) {
        Assert.assertEquals(ColumnStatisticsData._Fields.BOOLEAN_STATS,
            csobj.getStatsData().getSetField());
      } else if (csobj.getColName().equals("col2")) {
        Assert.assertEquals(ColumnStatisticsData._Fields.LONG_STATS,
            csobj.getStatsData().getSetField());
      } else if (csobj.getColName().equals("col3")) {
        Assert.assertEquals(ColumnStatisticsData._Fields.DOUBLE_STATS,
            csobj.getStatsData().getSetField());
      } else {
        Assert.fail("Unknown column");
      }
    }
  }

  @Test
  public void tableAllMiss() throws IOException {
    String dbName = "default";
    String tableName = "misstable";
    long now = System.currentTimeMillis();

    // Build a column stats object to return from mockito hbase
    ColumnStatistics cs = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(true, dbName, tableName);
    desc.setLastAnalyzed(now);
    cs.setStatsDesc(desc);
    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName("col1");
    obj.setColType("boolean");
    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(new BooleanColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);
    byte[] serialized = HBaseUtils.serializeStatsForOneColumn(cs, obj);

    // The easiest way to get this into hbase format is to shove it into a put and then pull out
    // the result for mockito to return.
    Put put = new Put(HBaseUtils.buildKey(dbName, tableName));
    put.add(HBaseReadWrite.STATS_CF, "col1".getBytes(HBaseUtils.ENCODING), serialized);

    obj = new ColumnStatisticsObj();
    obj.setColName("col2");
    obj.setColType("long");
    data = new ColumnStatisticsData();
    data.setLongStats(new LongColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);
    serialized = HBaseUtils.serializeStatsForOneColumn(cs, obj);
    put.add(HBaseReadWrite.STATS_CF, "col2".getBytes(HBaseUtils.ENCODING), serialized);
    puts[0] = put;

    // Now, ask for all 3 of these.  We should miss all on the cache.
    cs = hrw.getTableStatistics(dbName, tableName, Arrays.asList("col1", "col2", "col3"));

    Assert.assertEquals(now, cs.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(dbName, cs.getStatsDesc().getDbName());
    Assert.assertEquals(tableName, cs.getStatsDesc().getTableName());
    Assert.assertTrue(cs.getStatsDesc().isIsTblLevel());

    // There's no need to check every last field in each obj, as the objects aren't de/serialized
    // in the cache.  Just make sure we found the objects we expected.
    Assert.assertEquals(2, cs.getStatsObjSize());
    for (ColumnStatisticsObj csobj : cs.getStatsObj()) {
      if (csobj.getColName().equals("col1")) {
        Assert.assertEquals(ColumnStatisticsData._Fields.BOOLEAN_STATS,
            csobj.getStatsData().getSetField());
      } else if (csobj.getColName().equals("col2")) {
        Assert.assertEquals(ColumnStatisticsData._Fields.LONG_STATS,
            csobj.getStatsData().getSetField());
      } else {
        Assert.fail("Unknown column");
      }
    }
  }

  @Test
  public void tableSomeHit() throws IOException {
    String dbName = "default";
    String tableName = "sometable";
    long now = System.currentTimeMillis();

    ColumnStatistics cs = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(true, dbName, tableName);
    desc.setLastAnalyzed(now);
    cs.setStatsDesc(desc);
    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName("col1");
    obj.setColType("boolean");
    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(new BooleanColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);

    obj = new ColumnStatisticsObj();
    obj.setColName("col2");
    obj.setColType("long");
    data = new ColumnStatisticsData();
    data.setLongStats(new LongColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);

    hrw.updateStatistics(dbName, tableName, null, null, cs);

    cs = new ColumnStatistics();
    desc = new ColumnStatisticsDesc(true, dbName, tableName);
    desc.setLastAnalyzed(now);
    cs.setStatsDesc(desc);
    obj = new ColumnStatisticsObj();
    obj.setColName("col3");
    obj.setColType("double");
    data = new ColumnStatisticsData();
    data.setDoubleStats(new DoubleColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);

    Put put = new Put(HBaseUtils.buildKey(dbName, tableName));
    byte[] serialized = HBaseUtils.serializeStatsForOneColumn(cs, obj);
    put.add(HBaseReadWrite.STATS_CF, "col3".getBytes(HBaseUtils.ENCODING), serialized);
    puts[0] = put;

    // Now, ask for all 3 of these.  We should hit the first two on the cache and the third from
    // the get
    cs = hrw.getTableStatistics(dbName, tableName, Arrays.asList("col1", "col2", "col3"));

    Assert.assertEquals(now, cs.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(dbName, cs.getStatsDesc().getDbName());
    Assert.assertEquals(tableName, cs.getStatsDesc().getTableName());
    Assert.assertTrue(cs.getStatsDesc().isIsTblLevel());

    // There's no need to check every last field in each obj, as the objects aren't de/serialized
    // in the cache.  Just make sure we found the objects we expected.
    Assert.assertEquals(3, cs.getStatsObjSize());
    for (ColumnStatisticsObj csobj : cs.getStatsObj()) {
      if (csobj.getColName().equals("col1")) {
        Assert.assertEquals(ColumnStatisticsData._Fields.BOOLEAN_STATS,
            csobj.getStatsData().getSetField());
      } else if (csobj.getColName().equals("col2")) {
        Assert.assertEquals(ColumnStatisticsData._Fields.LONG_STATS,
            csobj.getStatsData().getSetField());
      } else if (csobj.getColName().equals("col3")) {
        Assert.assertEquals(ColumnStatisticsData._Fields.DOUBLE_STATS,
            csobj.getStatsData().getSetField());
      } else {
        Assert.fail("Unknown column");
      }
    }
  }

  @Test
  public void tableTimeout() throws Exception {
    String dbName = "default";
    String tableName = "timeouttable";
    long now = System.currentTimeMillis();

    ColumnStatistics cs = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(true, dbName, tableName);
    desc.setLastAnalyzed(now);
    cs.setStatsDesc(desc);
    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName("col1");
    obj.setColType("boolean");
    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(new BooleanColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);

    obj = new ColumnStatisticsObj();
    obj.setColName("col2");
    obj.setColType("long");
    data = new ColumnStatisticsData();
    data.setLongStats(new LongColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);

    hrw.updateStatistics(dbName, tableName, null, null, cs);

    StatsCache.getInstance(null).makeWayOld();

    // Now, ask for all 3 of these.  We should hit all on the cache.  We'll know if we don't
    // because we've mocked hbase and it will return null on the get.
    cs = hrw.getTableStatistics(dbName, tableName, Arrays.asList("col1", "col2", "col3"));

    Assert.assertEquals(0, cs.getStatsObjSize());
  }

  @Test
  public void partAllHit() throws IOException {
    String dbName = "default";
    String tableName = "partallhit";
    List<String> partVals1 = Arrays.asList("today");
    List<String> partVals2 = Arrays.asList("yesterday");
    List<String> partNames = Arrays.asList("ds=today", "ds=yeserday");
    long now = System.currentTimeMillis();

    ColumnStatistics cs = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
    desc.setLastAnalyzed(now);
    desc.setPartName(partNames.get(0));
    cs.setStatsDesc(desc);
    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName("col1");
    obj.setColType("boolean");
    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(new BooleanColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);

    hrw.updateStatistics(dbName, tableName, partNames.get(0), partVals1, cs);

    cs = new ColumnStatistics();
    desc = new ColumnStatisticsDesc(false, dbName, tableName);
    desc.setLastAnalyzed(now);
    desc.setPartName(partNames.get(0));
    cs.setStatsDesc(desc);
    obj = new ColumnStatisticsObj();
    obj.setColName("col2");
    obj.setColType("long");
    data = new ColumnStatisticsData();
    data.setLongStats(new LongColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);

    hrw.updateStatistics(dbName, tableName, partNames.get(0), partVals1, cs);

    cs = new ColumnStatistics();
    desc = new ColumnStatisticsDesc(false, dbName, tableName);
    desc.setLastAnalyzed(now);
    desc.setPartName(partNames.get(1));
    cs.setStatsDesc(desc);
    cs.addToStatsObj(obj);

    obj = new ColumnStatisticsObj();
    obj.setColName("col1");
    obj.setColType("boolean");
    data = new ColumnStatisticsData();
    data.setBooleanStats(new BooleanColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);

    hrw.updateStatistics(dbName, tableName, partNames.get(1), partVals2, cs);

    // Now, ask for all 3 of these.  We should hit all on the cache.  We'll know if we don't
    // because we've mocked hbase and it will return null on the get.
    List<ColumnStatistics> results = hrw.getPartitionStatistics(dbName, tableName,
        partNames, Arrays.asList(partVals1, partVals2), Arrays.asList("col1", "col2"));

    Assert.assertEquals(2, results.size());
    for (int i = 0; i < results.size(); i++) {
      Assert.assertEquals(now, results.get(i).getStatsDesc().getLastAnalyzed());
      Assert.assertEquals(dbName, results.get(i).getStatsDesc().getDbName());
      Assert.assertEquals(tableName, results.get(i).getStatsDesc().getTableName());
      Assert.assertFalse(cs.getStatsDesc().isIsTblLevel());

      Assert.assertEquals(partNames.get(i), results.get(i).getStatsDesc().getPartName());
      Assert.assertEquals(2, results.get(i).getStatsObjSize());
      for (ColumnStatisticsObj csobj : cs.getStatsObj()) {
        if (csobj.getColName().equals("col1")) {
          Assert.assertEquals(ColumnStatisticsData._Fields.BOOLEAN_STATS,
              csobj.getStatsData().getSetField());
        } else if (csobj.getColName().equals("col2")) {
          Assert.assertEquals(ColumnStatisticsData._Fields.LONG_STATS,
              csobj.getStatsData().getSetField());
        } else {
          Assert.fail("Unknown column");
        }
      }
    }
  }

  @Test
  public void partAllMiss() throws IOException {
    String dbName = "default";
    String tableName = "misspart";
    List<List<String>> partVals = Arrays.asList(Arrays.asList("today"), Arrays.asList("yesterday"));
    List<String> partNames = Arrays.asList("ds=today", "ds=yeserday");
    long now = System.currentTimeMillis();

    // Build a column stats object to return from mockito hbase
    ColumnStatistics cs = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
    desc.setLastAnalyzed(now);
    desc.setPartName(partNames.get(0));
    cs.setStatsDesc(desc);
    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName("col1");
    obj.setColType("boolean");
    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(new BooleanColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);
    byte[] serialized = HBaseUtils.serializeStatsForOneColumn(cs, obj);

    // The easiest way to get this into hbase format is to shove it into a put and then pull out
    // the result for mockito to return.
    Put put = new Put(HBaseUtils.buildKey(dbName, tableName, partNames.get(0)));
    put.add(HBaseReadWrite.STATS_CF, "col1".getBytes(HBaseUtils.ENCODING), serialized);
    puts[0] = put;

    cs = new ColumnStatistics();
    desc = new ColumnStatisticsDesc(false, dbName, tableName);
    desc.setLastAnalyzed(now);
    desc.setPartName(partNames.get(1));
    cs.setStatsDesc(desc);
    obj = new ColumnStatisticsObj();
    obj.setColName("col2");
    obj.setColType("long");
    data = new ColumnStatisticsData();
    data.setLongStats(new LongColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);
    serialized = HBaseUtils.serializeStatsForOneColumn(cs, obj);
    put = new Put(HBaseUtils.buildKey(dbName, tableName, partNames.get(1)));
    put.add(HBaseReadWrite.STATS_CF, "col2".getBytes(HBaseUtils.ENCODING), serialized);
    puts[1] = put;

    List<ColumnStatistics> results = hrw.getPartitionStatistics(dbName, tableName, partNames,
        partVals, Arrays.asList("col1", "col2", "col3"));

    Assert.assertEquals(2, results.size());
    for (int i = 0; i < results.size(); i++) {
      Assert.assertEquals(now, results.get(i).getStatsDesc().getLastAnalyzed());
      Assert.assertEquals(dbName, results.get(i).getStatsDesc().getDbName());
      Assert.assertEquals(tableName, results.get(i).getStatsDesc().getTableName());
      Assert.assertEquals(partNames.get(i), results.get(i).getStatsDesc().getPartName());
      Assert.assertFalse(cs.getStatsDesc().isIsTblLevel());

      Assert.assertEquals(1, results.get(i).getStatsObjSize());
      for (ColumnStatisticsObj csobj : cs.getStatsObj()) {
        if (csobj.getColName().equals("col1")) {
          Assert.assertEquals(ColumnStatisticsData._Fields.BOOLEAN_STATS,
              csobj.getStatsData().getSetField());
        } else if (csobj.getColName().equals("col2")) {
          Assert.assertEquals(ColumnStatisticsData._Fields.LONG_STATS,
              csobj.getStatsData().getSetField());
        } else {
          Assert.fail("Unknown column");
        }
      }
    }
  }

  @Test
  public void partSomeHit() throws IOException {
    String dbName = "default";
    String tableName = "partialpart";
    List<List<String>> partVals = Arrays.asList(Arrays.asList("today"), Arrays.asList("yesterday"));
    List<String> partNames = Arrays.asList("ds=today", "ds=yeserday");
    long now = System.currentTimeMillis();

    // Build a column stats object to return from mockito hbase
    ColumnStatistics cs = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
    desc.setLastAnalyzed(now);
    desc.setPartName(partNames.get(0));
    cs.setStatsDesc(desc);
    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName("col1");
    obj.setColType("boolean");
    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(new BooleanColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);
    byte[] serialized = HBaseUtils.serializeStatsForOneColumn(cs, obj);

    // The easiest way to get this into hbase format is to shove it into a put and then pull out
    // the result for mockito to return.
    Put put = new Put(HBaseUtils.buildKey(dbName, tableName, partNames.get(0)));
    put.add(HBaseReadWrite.STATS_CF, "col1".getBytes(HBaseUtils.ENCODING), serialized);
    puts[0] = put;


    // col2 partition 1 goes into the cache
    cs = new ColumnStatistics();
    desc = new ColumnStatisticsDesc(false, dbName, tableName);
    desc.setLastAnalyzed(now);
    desc.setPartName(partNames.get(0));
    cs.setStatsDesc(desc);
    obj = new ColumnStatisticsObj();
    obj.setColName("col2");
    obj.setColType("long");
    data = new ColumnStatisticsData();
    data.setLongStats(new LongColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);

    hrw.updateStatistics(dbName, tableName, partNames.get(0), partVals.get(0), cs);

    cs = new ColumnStatistics();
    desc = new ColumnStatisticsDesc(false, dbName, tableName);
    desc.setLastAnalyzed(now);
    desc.setPartName(partNames.get(1));
    cs.setStatsDesc(desc);
    obj = new ColumnStatisticsObj();
    obj.setColName("col2");
    obj.setColType("long");
    data = new ColumnStatisticsData();
    data.setLongStats(new LongColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);
    serialized = HBaseUtils.serializeStatsForOneColumn(cs, obj);
    put = new Put(HBaseUtils.buildKey(dbName, tableName, partNames.get(1)));
    put.add(HBaseReadWrite.STATS_CF, "col2".getBytes(HBaseUtils.ENCODING), serialized);
    puts[1] = put;

    // Now, ask for all 3 of these.  We should miss all on the cache.
    List<ColumnStatistics> results = hrw.getPartitionStatistics(dbName, tableName, partNames,
        partVals, Arrays.asList("col1", "col2", "col3"));

    Assert.assertEquals(2, results.size());
    Assert.assertEquals(2, results.get(0).getStatsObjSize());
    Assert.assertEquals(1, results.get(1).getStatsObjSize());

    for (int i = 0; i < results.size(); i++) {
      Assert.assertEquals(now, results.get(i).getStatsDesc().getLastAnalyzed());
      Assert.assertEquals(dbName, results.get(i).getStatsDesc().getDbName());
      Assert.assertEquals(tableName, results.get(i).getStatsDesc().getTableName());
      Assert.assertEquals(partNames.get(i), results.get(i).getStatsDesc().getPartName());
      Assert.assertFalse(cs.getStatsDesc().isIsTblLevel());

      for (ColumnStatisticsObj csobj : cs.getStatsObj()) {
        if (csobj.getColName().equals("col1")) {
          Assert.assertEquals(ColumnStatisticsData._Fields.BOOLEAN_STATS,
              csobj.getStatsData().getSetField());
        } else if (csobj.getColName().equals("col2")) {
          Assert.assertEquals(ColumnStatisticsData._Fields.LONG_STATS,
              csobj.getStatsData().getSetField());
        } else {
          Assert.fail("Unknown column");
        }
      }
    }
  }

  @Test
  public void partTimeout() throws Exception {
    String dbName = "default";
    String tableName = "timeoutpart";
    String partName = "ds=today";
    List<String> partVals = Arrays.asList("today");
    long now = System.currentTimeMillis();

    ColumnStatistics cs = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
    desc.setLastAnalyzed(now);
    desc.setPartName(partName);
    cs.setStatsDesc(desc);
    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName("col1");
    obj.setColType("boolean");
    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(new BooleanColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);

    hrw.updateStatistics(dbName, tableName, partName, partVals, cs);

    StatsCache.getInstance(null).makeWayOld();

    // Because our mocked 'get' always returns two results, I have to pass two part names, even
    // though both will return nothing.
    List<ColumnStatistics> results = hrw.getPartitionStatistics(dbName, tableName,
        Arrays.asList(partName, "fred"), Arrays.asList(partVals, Arrays.asList("fred")),
        Arrays.asList("col1", "col2", "col3"));

    Assert.assertEquals(2, results.size());
    Assert.assertEquals(0, results.get(0).getStatsObjSize());
    Assert.assertEquals(0, results.get(1).getStatsObjSize());
  }

  @Test
  public void cleaning() throws Exception {
    String dbName = "default";
    String tableName = "cleaning";
    long now = System.currentTimeMillis();

    ColumnStatistics cs = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(true, dbName, tableName);
    desc.setLastAnalyzed(now);
    cs.setStatsDesc(desc);
    for (int i = 0; i < 15; i++) {
      ColumnStatisticsObj obj = new ColumnStatisticsObj();
      obj.setColName("col" + i);
      obj.setColType("boolean");
      ColumnStatisticsData data = new ColumnStatisticsData();
      data.setBooleanStats(new BooleanColumnStatsData());
      obj.setStatsData(data);
      cs.addToStatsObj(obj);
    }

    hrw.updateStatistics(dbName, tableName, null, null, cs);

    Assert.assertEquals(15, StatsCache.getInstance(null).cacheSize());

    StatsCache.getInstance(null).makeWayOld();

    // Put one more in.  This should throw it over the edge and cause it to clean.
    cs = new ColumnStatistics();
    desc = new ColumnStatisticsDesc(true, dbName, tableName);
    desc.setLastAnalyzed(now);
    cs.setStatsDesc(desc);
    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName("col16");
    obj.setColType("boolean");
    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(new BooleanColumnStatsData());
    obj.setStatsData(data);
    cs.addToStatsObj(obj);

    hrw.updateStatistics(dbName, tableName, null, null, cs);

    while (StatsCache.getInstance(null).cleaning()) {
      Thread.sleep(250);
    }

    Assert.assertEquals(1, StatsCache.getInstance(null).cacheSize());
  }
  // TODO test cleaning

}
