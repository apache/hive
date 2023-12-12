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

import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.*;
import org.apache.hadoop.hive.metastore.messaging.event.filters.AndFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.CatalogFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.EventBoundaryFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.ReplEventFilter;
import org.apache.hadoop.hive.metastore.txn.AcidHouseKeeperService;
import org.apache.hadoop.hive.metastore.txn.AcidTxnCleanerService;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.events.EventUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.hcatalog.listener.DbNotificationListener;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestTxnNotificationLogging {

  @Parameterized.Parameter
  public int numberOfTxns;
  @Parameterized.Parameter(1)
  public TxnType txnType;
  @Parameterized.Parameter(2)
  public int expectedNotifications;
  private HiveConf hiveConf = new HiveConf();
  private Hive hiveDb;

  @Parameterized.Parameters(name = "{index}: numberOfTxns={0},txnType={1},expectedNotifications={2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] { { 3, TxnType.REPL_CREATED, 3 }, { 3, TxnType.DEFAULT, 3 }, { 3, TxnType.READ_ONLY, 0 }});
  }

  @Before public void setUp() throws Exception {
    TestTxnDbUtil.prepDb(hiveConf);
    //UserGroupInformation.setLoginUser(UserGroupInformation.createProxyUser("hive", UserGroupInformation.getLoginUser()));
    HiveConf.setVar(hiveConf,HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER, SQLStdHiveAuthorizerFactory.class.getName());
    MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS, DbNotificationListener.class.getName());
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.WAREHOUSE, "/tmp");
    MetastoreConf.setTimeVar(hiveConf, MetastoreConf.ConfVars.TXN_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    SessionState.start(hiveConf);
    try {
      hiveDb = Hive.get(hiveConf, false);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("Unable to initialize Hive Metastore using configuration: \n " + hiveConf);
      throw e;
    }
  }

  @After public void tearDown() throws Exception {
    TestTxnDbUtil.cleanDb(hiveConf);
    if (hiveDb != null) {
      hiveDb.close(true);
    }
    Hive.closeCurrent();
    SessionState.get().close();
  }

   @Test
   public void testTxnNotificationLogging() throws Exception {
    try {
      List<Long> txnIds = openTxns(numberOfTxns, txnType);
      assertEquals(txnIds.size(), getNumberOfTxns(txnIds, TxnState.OPEN));
      assertEquals(expectedNotifications,getNumNotifications(txnIds, MessageBuilder.OPEN_TXN_EVENT));
      Thread.sleep(1000);
      runHouseKeeperService();
      if (TxnType.REPL_CREATED != txnType) {
        assertEquals(txnIds.size(), getNumberOfTxns(txnIds, TxnState.ABORTED));
        assertEquals(expectedNotifications,getNumNotifications(txnIds, MessageBuilder.ABORT_TXN_EVENT));
      }
    } finally {
      runTxnHouseKeeperService();
      Hive.closeCurrent();
    }
  }

  private int getNumNotifications(List<Long> txnIds, String eventType) throws IOException {
    EventUtils.NotificationEventIterator eventIterator = getEventsIterator(0, 100, 100);
    int numNotifications = 0;
    MessageDeserializer deserializer = null;
    while (eventIterator.hasNext()) {
      NotificationEvent ev = eventIterator.next();
      if (eventType.equals(ev.getEventType())) {
        deserializer = ReplUtils.getEventDeserializer(ev);
        switch (ev.getEventType()) {
        case MessageBuilder.OPEN_TXN_EVENT:
          OpenTxnMessage openTxnMessage = deserializer.getOpenTxnMessage(ev.getMessage());
          if(txnIds.contains(openTxnMessage.getTxnIds().get(0))){
            numNotifications++;
          }
          break;
        case MessageBuilder.ABORT_TXN_EVENT:
          AbortTxnMessage abortTxnMessage = deserializer.getAbortTxnMessage(ev.getMessage());
          if(txnIds.contains(abortTxnMessage.getTxnId())){
            numNotifications++;
          }
        }
      }
    }
    return numNotifications;
  }

  private List<Long> openTxns(int txnCounter, TxnType txnType) throws TException {
    List<Long> txnIds = new LinkedList<>();
    for (; txnCounter > 0; txnCounter--) {
      if (txnType == TxnType.REPL_CREATED) {
        Long srcTxn = (long) (11 + txnCounter);
        List<Long> srcTxns = Arrays.asList(new Long[] { srcTxn });
        txnIds.addAll(hiveDb.getMSC().replOpenTxn("testPolicy", srcTxns, "hive", txnType));
      } else {
        txnIds.add(hiveDb.getMSC().openTxn("hive", txnType));
      }
    }
    return txnIds;
  }

  private int getNumberOfTxns(List<Long> txnIds, TxnState txnState) throws TException {
    AtomicInteger numTxns = new AtomicInteger();
    hiveDb.getMSC().showTxns().getOpen_txns().forEach(txnInfo -> {
      if (txnInfo.getState() == txnState && txnIds.contains(txnInfo.getId())) {
        numTxns.incrementAndGet();
      }
    });
    return numTxns.get();
  }

  private void runHouseKeeperService() {
    MetastoreTaskThread acidHouseKeeperService = new AcidHouseKeeperService();
    acidHouseKeeperService.setConf(hiveConf);
    acidHouseKeeperService.run(); //this will abort timedout txns
  }

  private void runTxnHouseKeeperService() {
    MetastoreTaskThread acidTxnCleanerService = new AcidTxnCleanerService();
    acidTxnCleanerService.setConf(hiveConf);
    acidTxnCleanerService.run(); //this will remove empty aborted txns
  }

  private EventUtils.NotificationEventIterator getEventsIterator(int eventFrom, int eventTo, int maxEventLimit)
      throws IOException {
    IMetaStoreClient.NotificationFilter evFilter = new AndFilter(new ReplEventFilter(new ReplScope()),
        new CatalogFilter(MetaStoreUtils.getDefaultCatalog(hiveConf)), new EventBoundaryFilter(eventFrom, eventTo));
    return new EventUtils.NotificationEventIterator(new EventUtils.MSClientNotificationFetcher(hiveDb), eventFrom,
        maxEventLimit, evFilter);
  }
}
//2023-10-17T01:22:02,814 DEBUG [Thread-3] util.ShutdownHookManager: ShutdownHookManager completed shutdown.