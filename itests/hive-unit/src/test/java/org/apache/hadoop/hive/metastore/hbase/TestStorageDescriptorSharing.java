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
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration tests with HBase Mini-cluster for HBaseStore
 */
public class TestStorageDescriptorSharing {

  private static final Log LOG = LogFactory.getLog(TestHBaseStoreIntegration.class.getName());

  private static HBaseTestingUtility utility;
  private static HTableInterface tblTable;
  private static HTableInterface sdTable;
  private static HTableInterface partTable;
  private static HTableInterface dbTable;
  private static HTableInterface roleTable;
  private static HTableInterface globalPrivsTable;
  private static HTableInterface principalRoleMapTable;
  private static Map<String, String> emptyParameters = new HashMap<String, String>();

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock private HConnection hconn;
  private HBaseStore store;
  private HiveConf conf;

  @BeforeClass
  public static void startMiniCluster() throws Exception {
    utility = new HBaseTestingUtility();
    utility.startMiniCluster();
    byte[][] families = new byte[][] {HBaseReadWrite.CATALOG_CF, HBaseReadWrite.STATS_CF};
    tblTable = utility.createTable(HBaseReadWrite.TABLE_TABLE.getBytes(HBaseUtils.ENCODING),
        families);
    sdTable = utility.createTable(HBaseReadWrite.SD_TABLE.getBytes(HBaseUtils.ENCODING),
        HBaseReadWrite.CATALOG_CF);
    partTable = utility.createTable(HBaseReadWrite.PART_TABLE.getBytes(HBaseUtils.ENCODING),
        families);
    dbTable = utility.createTable(HBaseReadWrite.DB_TABLE.getBytes(HBaseUtils.ENCODING),
        HBaseReadWrite.CATALOG_CF);
    roleTable = utility.createTable(HBaseReadWrite.ROLE_TABLE.getBytes(HBaseUtils.ENCODING),
        HBaseReadWrite.CATALOG_CF);
    globalPrivsTable =
        utility.createTable(HBaseReadWrite.GLOBAL_PRIVS_TABLE.getBytes(HBaseUtils.ENCODING),
            HBaseReadWrite.CATALOG_CF);
    principalRoleMapTable =
        utility.createTable(HBaseReadWrite.USER_TO_ROLE_TABLE.getBytes(HBaseUtils.ENCODING),
            HBaseReadWrite.CATALOG_CF);
  }

  @AfterClass
  public static void shutdownMiniCluster() throws Exception {
    utility.shutdownMiniCluster();
  }

  @Before
  public void setupConnection() throws IOException {
    MockitoAnnotations.initMocks(this);
    Mockito.when(hconn.getTable(HBaseReadWrite.SD_TABLE)).thenReturn(sdTable);
    Mockito.when(hconn.getTable(HBaseReadWrite.TABLE_TABLE)).thenReturn(tblTable);
    Mockito.when(hconn.getTable(HBaseReadWrite.PART_TABLE)).thenReturn(partTable);
    Mockito.when(hconn.getTable(HBaseReadWrite.DB_TABLE)).thenReturn(dbTable);
    Mockito.when(hconn.getTable(HBaseReadWrite.ROLE_TABLE)).thenReturn(roleTable);
    Mockito.when(hconn.getTable(HBaseReadWrite.GLOBAL_PRIVS_TABLE)).thenReturn(globalPrivsTable);
    Mockito.when(hconn.getTable(HBaseReadWrite.USER_TO_ROLE_TABLE)).thenReturn(principalRoleMapTable);
    conf = new HiveConf();
    // Turn off caching, as we want to test actual interaction with HBase
    conf.setBoolean(HBaseReadWrite.NO_CACHE_CONF, true);
    HBaseReadWrite hbase = HBaseReadWrite.getInstance(conf);
    hbase.setConnection(hconn);
    store = new HBaseStore();
    store.setConf(conf);
  }

  @Test
  public void createManyPartitions() throws Exception {
    String dbName = "default";
    String tableName = "manyParts";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("pc", "string", ""));
    Table table = new Table(tableName, dbName, "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    store.createTable(table);

    List<String> partVals = Arrays.asList("alan", "bob", "carl", "doug", "ethan");
    for (String val : partVals) {
      List<String> vals = new ArrayList<String>();
      vals.add(val);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/pc=" + val);
      Partition part = new Partition(vals, dbName, tableName, startTime, startTime, psd,
          emptyParameters);
      store.addPartition(part);

      Partition p = store.getPartition(dbName, tableName, vals);
      Assert.assertEquals("file:/tmp/pc=" + val, p.getSd().getLocation());
    }

    Assert.assertEquals(1, HBaseReadWrite.getInstance(conf).countStorageDescriptor());

    sd = new StorageDescriptor(cols, "file:/tmp", "input2", "output", false, 0,
        serde, null, null, emptyParameters);
    table = new Table("differenttable", "default", "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    store.createTable(table);

    Assert.assertEquals(2, HBaseReadWrite.getInstance(conf).countStorageDescriptor());

  }
}
