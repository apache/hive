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
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Integration tests with HBase Mini-cluster for HBaseStore
 */
public class IMockUtils {

  private static final Log LOG = LogFactory.getLog(IMockUtils.class.getName());

  protected static HBaseTestingUtility utility;
  protected static HTableInterface tblTable;
  protected static HTableInterface sdTable;
  protected static HTableInterface partTable;
  protected static HTableInterface dbTable;
  protected static HTableInterface funcTable;
  protected static HTableInterface roleTable;
  protected static HTableInterface globalPrivsTable;
  protected static HTableInterface principalRoleMapTable;
  protected static Map<String, String> emptyParameters = new HashMap<String, String>();

  @Mock
  private HBaseConnection hconn;
  protected HBaseStore store;
  protected HiveConf conf;
  protected Driver driver;

  protected static void startMiniCluster() throws Exception {
    utility = new HBaseTestingUtility();
    utility.startMiniCluster();
    byte[][] families = new byte[][]{HBaseReadWrite.CATALOG_CF, HBaseReadWrite.STATS_CF};
    tblTable = utility.createTable(HBaseReadWrite.TABLE_TABLE.getBytes(HBaseUtils.ENCODING),
        families);
    sdTable = utility.createTable(HBaseReadWrite.SD_TABLE.getBytes(HBaseUtils.ENCODING),
        HBaseReadWrite.CATALOG_CF);
    partTable = utility.createTable(HBaseReadWrite.PART_TABLE.getBytes(HBaseUtils.ENCODING),
        families);
    dbTable = utility.createTable(HBaseReadWrite.DB_TABLE.getBytes(HBaseUtils.ENCODING),
        HBaseReadWrite.CATALOG_CF);
    funcTable = utility.createTable(HBaseReadWrite.FUNC_TABLE.getBytes(HBaseUtils.ENCODING),
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

  protected static void shutdownMiniCluster() throws Exception {
    utility.shutdownMiniCluster();
  }

  protected void setupConnection() throws IOException {
    MockitoAnnotations.initMocks(this);
    Mockito.when(hconn.getHBaseTable(HBaseReadWrite.SD_TABLE)).thenReturn(sdTable);
    Mockito.when(hconn.getHBaseTable(HBaseReadWrite.TABLE_TABLE)).thenReturn(tblTable);
    Mockito.when(hconn.getHBaseTable(HBaseReadWrite.PART_TABLE)).thenReturn(partTable);
    Mockito.when(hconn.getHBaseTable(HBaseReadWrite.DB_TABLE)).thenReturn(dbTable);
    Mockito.when(hconn.getHBaseTable(HBaseReadWrite.FUNC_TABLE)).thenReturn(funcTable);
    Mockito.when(hconn.getHBaseTable(HBaseReadWrite.ROLE_TABLE)).thenReturn(roleTable);
    Mockito.when(hconn.getHBaseTable(HBaseReadWrite.GLOBAL_PRIVS_TABLE)).thenReturn(
        globalPrivsTable);
    Mockito.when(hconn.getHBaseTable(HBaseReadWrite.USER_TO_ROLE_TABLE)).thenReturn(
        principalRoleMapTable);
    conf = new HiveConf();
  }

  protected void setupDriver() {
    conf.setVar(HiveConf.ConfVars.METASTORE_HBASE_CONNECTION_CLASS, HBaseReadWrite.TEST_CONN);
    conf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    conf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL,
        "org.apache.hadoop.hive.metastore.hbase.HBaseStore");
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_FASTPATH, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    // Setup so we can test SQL standard auth
    conf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_AUTHORIZATION_SQLSTD_HS2_MODE, true);
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        SQLStdHiveAuthorizerFactoryForTest.class.getName());
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER,
        SessionStateConfigUserAuthenticator.class.getName());
    conf.setBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    conf.setVar(HiveConf.ConfVars.USERS_IN_ADMIN_ROLE, System.getProperty("user.name"));
    HBaseReadWrite.setTestConnection(hconn);

    SessionState.start(new CliSessionState(conf));
    driver = new Driver(conf);
  }

  protected void setupHBaseStore() {
    // Turn off caching, as we want to test actual interaction with HBase
    conf.setBoolean(HBaseReadWrite.NO_CACHE_CONF, true);
    conf.setVar(HiveConf.ConfVars.METASTORE_HBASE_CONNECTION_CLASS, HBaseReadWrite.TEST_CONN);
    HBaseReadWrite.setTestConnection(hconn);
    // HBaseReadWrite hbase = HBaseReadWrite.getInstance(conf);
    store = new HBaseStore();
    store.setConf(conf);
  }

}

