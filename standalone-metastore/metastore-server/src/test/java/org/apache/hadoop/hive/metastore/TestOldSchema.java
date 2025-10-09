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
package org.apache.hadoop.hive.metastore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

@Category(MetastoreUnitTest.class)
public class TestOldSchema {
  private ObjectStore store = null;
  private Configuration conf;

  private static final Logger LOG = LoggerFactory.getLogger(TestOldSchema.class.getName());

  private static final String ENGINE = "hive";

  public static class MockPartitionExpressionProxy implements PartitionExpressionProxy {
    @Override
    public String convertExprToFilter(byte[] expr, String defaultPartitionName, boolean decodeFilterExpToStr) throws MetaException {
      return null;
    }

    @Override
    public boolean filterPartitionsByExpr(List<FieldSchema> partColumns, byte[] expr,
                                          String defaultPartitionName,
                                          List<String> partitionNames) throws MetaException {
      return false;
    }

    @Override
    public FileMetadataExprType getMetadataType(String inputFormat) {
      return null;
    }

    @Override
    public SearchArgument createSarg(byte[] expr) {
      return null;
    }

    @Override
    public FileFormatProxy getFileFormatProxy(FileMetadataExprType type) {
      return null;
    }
  }

  private byte bitVectors[][] = new byte[2][];

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.STATS_FETCH_BITVECTOR, false);
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    store = new ObjectStore();
    store.setConf(conf);
    dropAllStoreObjects(store);
    HMSHandler.createDefaultCatalog(store, new Warehouse(conf));

    HyperLogLog hll = HyperLogLog.builder().build();
    hll.addLong(1);
    bitVectors[1] = hll.serialize();
    hll = HyperLogLog.builder().build();
    hll.addLong(2);
    hll.addLong(3);
    hll.addLong(3);
    hll.addLong(4);
    bitVectors[0] = hll.serialize();
  }

  @After
  public void tearDown() {
  }

  /**
   * Tests partition operations
   */
  @Ignore("HIVE-19509: Disable tests that are failing continuously")
  @Test
  public void testPartitionOps() throws Exception {
    String dbName = "default";
    String tableName = "snp";
    Database db1 = new DatabaseBuilder()
        .setName(dbName)
        .setDescription("description")
        .setLocation("locationurl")
        .build(conf);
    store.createDatabase(db1);
    long now = System.currentTimeMillis();
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col1", "long", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, Collections.emptyMap());
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName, dbName, "me", (int) now, (int) now, 0, sd, partCols,
        Collections.emptyMap(), null, null, null);
    store.createTable(table);
    MTable mTable = store.ensureGetMTable(table.getCatName(), table.getDbName(), table.getTableName());

    Deadline.startTimer("getPartition");
    for (int i = 0; i < 10; i++) {
      List<String> partVal = new ArrayList<>();
      partVal.add(String.valueOf(i));
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/default/hit/ds=" + partVal);
      Partition part = new Partition(partVal, dbName, tableName, (int) now, (int) now, psd,
          Collections.emptyMap());
      part.setCatName(DEFAULT_CATALOG_NAME);
      store.addPartition(part);
      ColumnStatistics cs = new ColumnStatistics();
      ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, dbName, tableName);
      desc.setLastAnalyzed(now);
      desc.setPartName("ds=" + String.valueOf(i));
      cs.setStatsDesc(desc);
      ColumnStatisticsObj obj = new ColumnStatisticsObj();
      obj.setColName("col1");
      obj.setColType("bigint");
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
      cs.setEngine(ENGINE);
      store.updatePartitionColumnStatistics(table, mTable, cs, partVal, null, -1);

    }

    Checker statChecker = new Checker() {
      @Override
      public void checkStats(AggrStats aggrStats) throws Exception {
        Assert.assertEquals(10, aggrStats.getPartsFound());
        Assert.assertEquals(1, aggrStats.getColStatsSize());
        ColumnStatisticsObj cso = aggrStats.getColStats().get(0);
        Assert.assertEquals("col1", cso.getColName());
        Assert.assertEquals("bigint", cso.getColType());
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
    AggrStats aggrStats = store.get_aggr_stats_for(DEFAULT_CATALOG_NAME, dbName, tableName, partNames,
        Arrays.asList("col1"), ENGINE);
    statChecker.checkStats(aggrStats);

  }

  private interface Checker {
    void checkStats(AggrStats aggrStats) throws Exception;
  }

  private static void dropAllStoreObjects(RawStore store) throws MetaException,
      InvalidObjectException, InvalidInputException {
    try {
      Deadline.registerIfNot(100000);
      Deadline.startTimer("getPartition");
      List<String> dbs = store.getAllDatabases(DEFAULT_CATALOG_NAME);
      for (int i = 0; i < dbs.size(); i++) {
        String db = dbs.get(i);
        List<String> tbls = store.getAllTables(DEFAULT_CATALOG_NAME, db);
        for (String tbl : tbls) {
          List<String> partNames = store.listPartitionNames(DEFAULT_CATALOG_NAME, db, tbl, (short) -1);
          store.dropPartitions(DEFAULT_CATALOG_NAME, db, tbl, partNames);
          store.dropTable(DEFAULT_CATALOG_NAME, db, tbl);
        }
        store.dropDatabase(DEFAULT_CATALOG_NAME, db);
      }
    } catch (NoSuchObjectException e) {
    }
  }

}
