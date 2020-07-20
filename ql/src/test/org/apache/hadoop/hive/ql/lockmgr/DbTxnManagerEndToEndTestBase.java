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
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Base class for "end-to-end" tests for DbTxnManager and simulate concurrent queries.
 */
public abstract class DbTxnManagerEndToEndTestBase {

  protected static HiveConf conf = new HiveConf(Driver.class);
  protected HiveTxnManager txnMgr;
  protected Context ctx;
  protected Driver driver;
  protected IDriver driver2;
  protected TxnStore txnHandler;

  public DbTxnManagerEndToEndTestBase() {
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    TxnDbUtil.setConfValues(conf);
  }
  @BeforeClass
  public static void setUpDB() throws Exception{
    TxnDbUtil.prepDb(conf);
  }

  @Before
  public void setUp() throws Exception {
    SessionState.start(conf);
    ctx = new Context(conf);
    driver = new Driver(new QueryState.Builder().withHiveConf(conf).nonIsolated().build());
    driver2 = DriverFactory.newDriver(conf);
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, false);
    TxnDbUtil.cleanDb(conf);
    SessionState ss = SessionState.get();
    ss.initTxnMgr(conf);
    txnMgr = ss.getTxnMgr();
    Assert.assertTrue(txnMgr instanceof DbTxnManager);
    txnHandler = TxnUtils.getTxnStore(conf);

  }
  @After
  public void tearDown() throws Exception {
    driver.close();
    driver2.close();
    if (txnMgr != null) {
      txnMgr.closeTxnManager();
    }
  }
}
