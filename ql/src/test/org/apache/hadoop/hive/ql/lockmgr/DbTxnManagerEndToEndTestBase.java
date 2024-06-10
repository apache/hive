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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.HiveMetaStoreClientWithLocalCache;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;

/**
 * Base class for "end-to-end" tests for DbTxnManager and simulate concurrent queries.
 */
public abstract class DbTxnManagerEndToEndTestBase {

  private static final String TEST_DATA_DIR = new File(
    System.getProperty("java.io.tmpdir") + File.separator +
      DbTxnManagerEndToEndTestBase.class.getCanonicalName() + "-" + System.currentTimeMillis())
    .getPath().replaceAll("\\\\", "/");

  protected static HiveConf conf = new HiveConf(Driver.class);
  protected HiveTxnManager txnMgr;
  protected Context ctx;
  protected Driver driver, driver2;
  protected TxnStore txnHandler;

  public DbTxnManagerEndToEndTestBase() {
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
      "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.TXN_MERGE_INSERT_X_LOCK, true);

    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.WAREHOUSE, getWarehouseDir());
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON, true);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON, true);
    TestTxnDbUtil.setConfValues(conf);
  }
  
  @BeforeClass
  public static void setUpDB() throws Exception{
    TestTxnDbUtil.prepDb(conf);
  }

  @Before
  public void setUp() throws Exception {
    // set up metastore client cache
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.MSC_CACHE_ENABLED)) {
      HiveMetaStoreClientWithLocalCache.init(conf);
    }
    SessionState.start(conf);
    ctx = new Context(conf);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_LOCKS_PARTITION_THRESHOLD, -1);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, false);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.TXN_WRITE_X_LOCK, false);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.TXN_USE_MIN_HISTORY_LEVEL, true);

    driver = new Driver(new QueryState.Builder().withHiveConf(conf).nonIsolated().build());
    driver2 = new Driver(new QueryState.Builder().withHiveConf(conf).build());
    
    TestTxnDbUtil.cleanDb(conf);
    SessionState ss = SessionState.get();
    ss.initTxnMgr(conf);
    txnMgr = ss.getTxnMgr();
    Assert.assertTrue(txnMgr instanceof DbTxnManager);
    txnHandler = TxnUtils.getTxnStore(conf);

    File f = new File(getWarehouseDir());
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(getWarehouseDir()).mkdirs())) {
      throw new RuntimeException("Could not create " + getWarehouseDir());
    }
  }
  
  @After
  public void tearDown() throws Exception {
    driver.close();
    conf.unset(ValidTxnList.VALID_TXNS_KEY);
    
    driver2.close();
    if (txnMgr != null) {
      txnMgr.closeTxnManager();
    }
    FileUtils.deleteDirectory(new File(TEST_DATA_DIR));
  }

  protected String getWarehouseDir() {
    return TEST_DATA_DIR + "/warehouse";
  }
}
