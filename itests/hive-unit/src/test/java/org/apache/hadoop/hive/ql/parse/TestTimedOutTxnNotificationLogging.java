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
import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.AbortTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.OpenTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.event.filters.AndFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.CatalogFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.EventBoundaryFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.ReplEventFilter;
import org.apache.hadoop.hive.metastore.txn.service.AcidHouseKeeperService;
import org.apache.hadoop.hive.metastore.txn.service.AcidTxnCleanerService;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.listener.DbNotificationListener;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


@RunWith(Parameterized.class)
public class TestTimedOutTxnNotificationLogging {

  private HiveConf hiveConf;

  private ObjectStore objectStore;

  private MetastoreTaskThread acidTxnCleanerService;

  private MetastoreTaskThread acidHouseKeeperService;

  private static IMetaStoreClient hive;

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
    hiveConf = new HiveConfForTest(getClass());
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
      Assert.assertEquals(txnIds.size(), getNumberOfTxnsWithTxnState(txnIds, TxnState.OPEN));
      Assert.assertEquals(expectedNotifications, getNumberOfNotificationsWithEventType(txnIds, MessageBuilder.OPEN_TXN_EVENT));
      Thread.sleep(1000);
      acidHouseKeeperService.run(); //this will abort timed-out txns
      if (txnType != TxnType.REPL_CREATED) {
        Assert.assertEquals(txnIds.size(), getNumberOfTxnsWithTxnState(txnIds, TxnState.ABORTED));
        Assert.assertEquals(expectedNotifications, getNumberOfNotificationsWithEventType(txnIds, MessageBuilder.ABORT_TXN_EVENT));
      }
    } finally {
      runCleanerServices();
    }
  }

  private int getNumberOfNotificationsWithEventType(List<Long> txnIds, String eventType) throws TException {
    int numNotifications = 0;
    IMetaStoreClient.NotificationFilter evFilter = new AndFilter(new ReplEventFilter(new ReplScope()),
        new CatalogFilter(MetaStoreUtils.getDefaultCatalog(hiveConf)), new EventBoundaryFilter(0, 100));
    NotificationEventResponse rsp = hive.getNextNotification(new NotificationEventRequest(), true, evFilter);
    if (rsp.getEvents() == null) {
      return numNotifications;
    }
    Iterator<NotificationEvent> eventIterator = rsp.getEvents().iterator();
    MessageDeserializer deserializer;
    while (eventIterator.hasNext()) {
      NotificationEvent ev = eventIterator.next();
      if (eventType.equals(ev.getEventType())) {
        deserializer = ReplUtils.getEventDeserializer(ev);
        switch (ev.getEventType()) {
        case MessageBuilder.OPEN_TXN_EVENT:
          OpenTxnMessage openTxnMessage = deserializer.getOpenTxnMessage(ev.getMessage());
          if (txnIds.contains(openTxnMessage.getTxnIds().get(0))) {
            numNotifications++;
          }
          break;
        case MessageBuilder.ABORT_TXN_EVENT:
          AbortTxnMessage abortTxnMessage = deserializer.getAbortTxnMessage(ev.getMessage());
          if (txnIds.contains(abortTxnMessage.getTxnId())) {
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
        List<Long> srcTxns = Collections.singletonList(srcTxn);
        txnIds.addAll(hive.replOpenTxn("testPolicy", srcTxns, "hive", txnType));
      } else {
        txnIds.add(hive.openTxn("hive", txnType));
      }
    }
    return txnIds;
  }

  private int getNumberOfTxnsWithTxnState(List<Long> txnIds, TxnState txnState) throws TException {
    AtomicInteger numTxns = new AtomicInteger();
    hive.showTxns().getOpen_txns().forEach(txnInfo -> {
      if (txnInfo.getState() == txnState && txnIds.contains(txnInfo.getId())) {
        numTxns.incrementAndGet();
      }
    });
    return numTxns.get();
  }

  private void runCleanerServices() {
    objectStore.cleanNotificationEvents(0);
    acidTxnCleanerService.run(); //this will remove empty aborted txns
  }
}