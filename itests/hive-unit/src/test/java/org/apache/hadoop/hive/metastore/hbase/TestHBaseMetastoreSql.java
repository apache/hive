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
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration tests with HBase Mini-cluster for HBaseStore
 */
public class TestHBaseMetastoreSql {

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
  @Mock private HBaseConnection hconn;
  private HBaseStore store;
  private HiveConf conf;
  private Driver driver;

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
    Mockito.when(hconn.getHBaseTable(HBaseReadWrite.SD_TABLE)).thenReturn(sdTable);
    Mockito.when(hconn.getHBaseTable(HBaseReadWrite.TABLE_TABLE)).thenReturn(tblTable);
    Mockito.when(hconn.getHBaseTable(HBaseReadWrite.PART_TABLE)).thenReturn(partTable);
    Mockito.when(hconn.getHBaseTable(HBaseReadWrite.DB_TABLE)).thenReturn(dbTable);
    Mockito.when(hconn.getHBaseTable(HBaseReadWrite.ROLE_TABLE)).thenReturn(roleTable);
    Mockito.when(hconn.getHBaseTable(HBaseReadWrite.GLOBAL_PRIVS_TABLE)).thenReturn(globalPrivsTable);
    Mockito.when(hconn.getHBaseTable(HBaseReadWrite.USER_TO_ROLE_TABLE)).thenReturn(principalRoleMapTable);
    conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTORE_HBASE_CONNECTION_CLASS, HBaseReadWrite.TEST_CONN);
    conf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    conf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL,
        "org.apache.hadoop.hive.metastore.hbase.HBaseStore");
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_FASTPATH, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    HBaseReadWrite.setTestConnection(hconn);

    SessionState.start(new CliSessionState(conf));
    driver = new Driver(conf);
  }

  @Test
  public void insertIntoTable() throws Exception {
    driver.run("create table iit (c int)");
    CommandProcessorResponse rsp = driver.run("insert into table iit values (3)");
    Assert.assertEquals(0, rsp.getResponseCode());
  }

  @Test
  public void insertIntoPartitionTable() throws Exception {
    driver.run("create table iipt (c int) partitioned by (ds string)");
    CommandProcessorResponse rsp =
        driver.run("insert into table iipt partition(ds) values (1, 'today'), (2, 'yesterday')," +
            "(3, 'tomorrow')");
    Assert.assertEquals(0, rsp.getResponseCode());
  }


}
