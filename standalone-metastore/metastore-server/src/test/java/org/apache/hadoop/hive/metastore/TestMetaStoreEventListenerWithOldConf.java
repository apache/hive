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

package org.apache.hadoop.hive.metastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Mostly same tests as TestMetaStoreEventListener, but using old hive conf values instead of new
 * metastore conf values.
 */
@Category(MetastoreUnitTest.class)
public class TestMetaStoreEventListenerWithOldConf {
  private Configuration conf;

  private static final String metaConfKey = "hive.metastore.partition.name.whitelist.pattern";
  private static final String metaConfVal = "";

  @Before
  public void setUp() throws Exception {
    System.setProperty("hive.metastore.event.listeners",
        DummyListener.class.getName());
    System.setProperty("hive.metastore.pre.event.listeners",
        DummyPreListener.class.getName());

    conf = MetastoreConf.newMetastoreConf();

    MetastoreConf.setVar(conf, ConfVars.PARTITION_NAME_WHITELIST_PATTERN, metaConfVal);
    MetastoreConf.setLongVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES, 3);
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);

    DummyListener.notifyList.clear();
    DummyPreListener.notifyList.clear();
  }

  @Test
  public void testMetaConfNotifyListenersClosingClient() throws Exception {
    HiveMetaStoreClient closingClient = new HiveMetaStoreClient(conf, null);
    closingClient.setMetaConf(metaConfKey, "[test pattern modified]");
    ConfigChangeEvent event = (ConfigChangeEvent) DummyListener.getLastEvent();
    assertEquals(event.getOldValue(), metaConfVal);
    assertEquals(event.getNewValue(), "[test pattern modified]");
    closingClient.close();

    Thread.sleep(2 * 1000);

    event = (ConfigChangeEvent) DummyListener.getLastEvent();
    assertEquals(event.getOldValue(), "[test pattern modified]");
    assertEquals(event.getNewValue(), metaConfVal);
  }

  @Test
  public void testMetaConfNotifyListenersNonClosingClient() throws Exception {
    HiveMetaStoreClient nonClosingClient = new HiveMetaStoreClient(conf, null);
    nonClosingClient.setMetaConf(metaConfKey, "[test pattern modified]");
    ConfigChangeEvent event = (ConfigChangeEvent) DummyListener.getLastEvent();
    assertEquals(event.getOldValue(), metaConfVal);
    assertEquals(event.getNewValue(), "[test pattern modified]");
    // This should also trigger meta listener notification via TServerEventHandler#deleteContext
    nonClosingClient.getTTransport().close();

    Thread.sleep(2 * 1000);

    event = (ConfigChangeEvent) DummyListener.getLastEvent();
    assertEquals(event.getOldValue(), "[test pattern modified]");
    assertEquals(event.getNewValue(), metaConfVal);
  }

  @Test
  public void testMetaConfDuplicateNotification() throws Exception {
    HiveMetaStoreClient closingClient = new HiveMetaStoreClient(conf, null);
    closingClient.setMetaConf(metaConfKey, metaConfVal);
    int beforeCloseNotificationEventCounts = DummyListener.notifyList.size();
    closingClient.close();

    Thread.sleep(2 * 1000);

    int afterCloseNotificationEventCounts = DummyListener.notifyList.size();
    // Setting key to same value, should not trigger configChange event during shutdown
    assertEquals(beforeCloseNotificationEventCounts, afterCloseNotificationEventCounts);
  }

  @Test
  public void testMetaConfSameHandler() throws Exception {
    HiveMetaStoreClient closingClient = new HiveMetaStoreClient(conf, null);
    closingClient.setMetaConf(metaConfKey, "[test pattern modified]");
    ConfigChangeEvent event = (ConfigChangeEvent) DummyListener.getLastEvent();
    int beforeCloseNotificationEventCounts = DummyListener.notifyList.size();
    IHMSHandler beforeHandler = event.getIHMSHandler();
    closingClient.close();

    Thread.sleep(2 * 1000);
    event = (ConfigChangeEvent) DummyListener.getLastEvent();
    int afterCloseNotificationEventCounts = DummyListener.notifyList.size();
    IHMSHandler afterHandler = event.getIHMSHandler();
    // Meta-conf cleanup should trigger an event to listener
    assertNotSame(beforeCloseNotificationEventCounts, afterCloseNotificationEventCounts);
    // Both the handlers should be same
    assertEquals(beforeHandler, afterHandler);
  }
}
