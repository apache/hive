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
package org.apache.hadoop.hive.ql.lockmgr;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClientWithLocalCache;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.Arrays;
import java.util.Collection;

/**
 * Base class for "end-to-end" tests for DbTxnManager and simulate concurrent queries.
 */
@RunWith(Parameterized.class)
public abstract class DbTxnManagerEndToEndTestBase {

  protected static HiveConf conf;
  protected HiveTxnManager txnMgr;
  protected Context ctx;
  protected Driver driver, driver2;
  protected TxnStore txnHandler;

  @Parameterized.Parameter
  public boolean minHistoryLevel;

  @Parameterized.Parameters(name = "minHistoryLevel = {0}")
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][] { { true }, { false } });
  }

  @Before
  public void setUp() throws Exception {
    conf = new HiveConf(Driver.class);
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    conf.setBoolVar(HiveConf.ConfVars.TXN_MERGE_INSERT_X_LOCK, true);
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, false);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.TXN_USE_MIN_HISTORY_LEVEL, minHistoryLevel);
    TestTxnDbUtil.setConfValues(conf);
    TestTxnDbUtil.prepDb(conf);

    // set up metastore client cache
    if (conf.getBoolVar(HiveConf.ConfVars.MSC_CACHE_ENABLED)) {
      HiveMetaStoreClientWithLocalCache.init(conf);
    }
    SessionState.start(conf);
    ctx = new Context(conf);
    driver = new Driver(new QueryState.Builder().withHiveConf(conf).nonIsolated().build());
    driver2 = new Driver(new QueryState.Builder().withHiveConf(conf).build());
    SessionState ss = SessionState.get();
    ss.initTxnMgr(conf);
    txnMgr = ss.getTxnMgr();
    Assert.assertTrue(txnMgr instanceof DbTxnManager);
    txnHandler = TxnUtils.getTxnStore(conf);
  }

  @After
  public void tearDown() throws Exception {
    TestTxnDbUtil.cleanDb(conf);
    driver.close();
    driver2.close();
    if (txnMgr != null) {
      txnMgr.closeTxnManager();
    }
  }
}
