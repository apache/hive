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

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.IndexBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.AddIndexEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterIndexEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropIndexEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.apache.hadoop.hive.metastore.events.PreAddIndexEvent;
import org.apache.hadoop.hive.metastore.events.PreAddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterIndexEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreDropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreDropIndexEvent;
import org.apache.hadoop.hive.metastore.events.PreDropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreDropTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.events.PreLoadPartitionDoneEvent;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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

    int port = MetaStoreTestUtils.findFreePort();
    conf = MetastoreConf.newMetastoreConf();

    MetastoreConf.setVar(conf, ConfVars.PARTITION_NAME_WHITELIST_PATTERN, metaConfVal);
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    MetastoreConf.setLongVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES, 3);
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetaStoreTestUtils.startMetaStore(port, HadoopThriftAuthBridge.getBridge(), conf);

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
    IHMSHandler beforeHandler = event.getHandler();
    closingClient.close();

    Thread.sleep(2 * 1000);
    event = (ConfigChangeEvent) DummyListener.getLastEvent();
    int afterCloseNotificationEventCounts = DummyListener.notifyList.size();
    IHMSHandler afterHandler = event.getHandler();
    // Meta-conf cleanup should trigger an event to listener
    assertNotSame(beforeCloseNotificationEventCounts, afterCloseNotificationEventCounts);
    // Both the handlers should be same
    assertEquals(beforeHandler, afterHandler);
  }
}
