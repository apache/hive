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
package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.txn.AcidHouseKeeperService;
import org.apache.hadoop.hive.metastore.txn.AcidTxnCleanerService;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.listener.DbNotificationListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;


@RunWith(Parameterized.class)
public class TestTimedOutTxnNotificationLogging extends TestTxnNotificationLogging {

  private HiveConf hiveConf;

  @Parameterized.Parameter
  public int numberOfTxns;

  @Parameterized.Parameter(1)
  public TxnType txnType;

  @Parameterized.Parameter(2)
  public int expectedNotifications;

  @Parameterized.Parameters(name = "{index}: numberOfTxns={0},txnType={1},expectedNotifications={2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] { { 3, TxnType.REPL_CREATED, 3 }, { 3, TxnType.DEFAULT, 3 }, { 3, TxnType.READ_ONLY, 0 } });
  }

  @Before
  public void setUp() throws Exception {
    setConf();
    TestTxnDbUtil.prepDb(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));
    hive = new HiveMetaStoreClient(hiveConf);
    objectStore = new ObjectStore();
    objectStore.setConf(hiveConf);
    acidTxnCleanerService = new AcidTxnCleanerService();
    acidTxnCleanerService.setConf(hiveConf);
    acidHouseKeeperService = new AcidHouseKeeperService();
    acidHouseKeeperService.setConf(hiveConf);
  }

  private void setConf() {
    hiveConf = new HiveConf();
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.WAREHOUSE, "/tmp");
    MetastoreConf.setTimeVar(hiveConf, MetastoreConf.ConfVars.TXN_TIMEOUT, 1, TimeUnit.SECONDS);
    HiveConf.setVar(hiveConf, HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        SQLStdHiveAuthorizerFactory.class.getName());
    MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS,
        DbNotificationListener.class.getName());
    MetastoreConf.setTimeVar(hiveConf, MetastoreConf.ConfVars.EVENT_DB_LISTENER_CLEAN_INTERVAL, 10,
        TimeUnit.MILLISECONDS);
    MetastoreConf.setTimeVar(hiveConf, MetastoreConf.ConfVars.EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL, 0,
        TimeUnit.SECONDS);
  }

  @After
  public void tearDown() throws Exception {
    TestTxnDbUtil.cleanDb(hiveConf);
    if (hive != null) {
      hive.close();
    }
    SessionState.get().close();
    hiveConf = null;
  }

  @Test
  public void testTxnNotificationLogging() throws Exception {
    try {
      List<Long> txnIds = openTxns(numberOfTxns, txnType);
      assertEquals(txnIds.size(), getNumberOfTxnsWithTxnState(txnIds, TxnState.OPEN));
      assertEquals(expectedNotifications, getNumNotifications(txnIds, MessageBuilder.OPEN_TXN_EVENT));
      Thread.sleep(1000);
      acidHouseKeeperService.run(); //this will abort timed-out txns
      if (txnType != TxnType.REPL_CREATED) {
        assertEquals(txnIds.size(), getNumberOfTxnsWithTxnState(txnIds, TxnState.ABORTED));
        assertEquals(expectedNotifications, getNumNotifications(txnIds, MessageBuilder.ABORT_TXN_EVENT));
      }
    } finally {
      runCleanerServices();
    }
  }

}