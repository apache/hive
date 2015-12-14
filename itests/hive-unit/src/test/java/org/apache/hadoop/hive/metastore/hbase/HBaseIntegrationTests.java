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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Integration tests with HBase Mini-cluster for HBaseStore
 */
public class HBaseIntegrationTests {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseIntegrationTests.class.getName());

  protected static HBaseTestingUtility utility;
  protected static HBaseAdmin admin;
  protected static Map<String, String> emptyParameters = new HashMap<>();
  protected static HiveConf conf;

  protected HBaseStore store;
  protected Driver driver;

  protected static void startMiniCluster() throws Exception {
    String connectionClassName =
        System.getProperty(HiveConf.ConfVars.METASTORE_HBASE_CONNECTION_CLASS.varname);
    boolean testingTephra =
        connectionClassName != null && connectionClassName.equals(TephraHBaseConnection.class.getName());
    if (testingTephra) {
      LOG.info("Testing with Tephra");
    }
    Configuration hbaseConf = HBaseConfiguration.create();
    hbaseConf.setInt("hbase.master.info.port", -1);
    utility = new HBaseTestingUtility(hbaseConf);
    utility.startMiniCluster();
    conf = new HiveConf(utility.getConfiguration(), HBaseIntegrationTests.class);
    admin = utility.getHBaseAdmin();
    HBaseStoreTestUtil.initHBaseMetastore(admin, null);
  }

  protected static void shutdownMiniCluster() throws Exception {
    utility.shutdownMiniCluster();
  }

  protected void setupConnection() throws IOException {

  }

  protected void setupDriver() {
    // This chicanery is necessary to make the driver work.  Hive tests need the pfile file
    // system, while the hbase one uses something else.  So first make sure we've configured our
    // hbase connection, then get a new config file and populate it as desired.
    HBaseReadWrite.setConf(conf);
    conf = new HiveConf();
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
    conf.setVar(HiveConf.ConfVars.HIVEMAPREDMODE,"nonstrict");
    //HBaseReadWrite.setTestConnection(hconn);

    SessionState.start(new CliSessionState(conf));
    driver = new Driver(conf);
  }

  protected void setupHBaseStore() {
    // Turn off caching, as we want to test actual interaction with HBase
    conf.setBoolean(HBaseReadWrite.NO_CACHE_CONF, true);
    store = new HBaseStore();
    store.setConf(conf);
  }

}

