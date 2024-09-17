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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TestCompactionHeartbeatService {

  private static Field HEARTBEAT_SINGLETON;
  private static Field HEARTBEAT_CLIENTPOOL;
  @Mock
  private HiveConf conf;
  @Mock
  private IMetaStoreClient client;
  private MockedStatic<HiveMetaStoreUtils> hiveMetaStoreUtilsMockedStatic;
  private ObjectPool<IMetaStoreClient> clientPool;

  @BeforeClass
  public static void setupClass() throws NoSuchFieldException {
    HEARTBEAT_SINGLETON = CompactionHeartbeatService.class.getDeclaredField("instance");
    HEARTBEAT_SINGLETON.setAccessible(true);

    HEARTBEAT_CLIENTPOOL = CompactionHeartbeatService.class.getDeclaredField("clientPool");
    HEARTBEAT_CLIENTPOOL.setAccessible(true);
  }

  @Before
  public void setup() throws Exception {
    hiveMetaStoreUtilsMockedStatic = mockStatic(HiveMetaStoreUtils.class);
    hiveMetaStoreUtilsMockedStatic.when(() -> HiveMetaStoreUtils.getHiveMetastoreClient(any())).thenReturn(client);

    Mockito.when(conf.get(MetastoreConf.ConfVars.TXN_TIMEOUT.getVarname())).thenReturn("100ms");
    Mockito.when(conf.get(MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS.getVarname())).thenReturn("4");
    HEARTBEAT_SINGLETON.set(null,null);

    IMetaStoreClientFactory metaStoreClientFactory = spy((new IMetaStoreClientFactory(conf)));
    doReturn(client).when(metaStoreClientFactory).create();

    clientPool = spy(new GenericObjectPool<>(metaStoreClientFactory));

    CompactionHeartbeatService compactionHeartbeatService = CompactionHeartbeatService.getInstance(conf);
    HEARTBEAT_CLIENTPOOL.set(compactionHeartbeatService, clientPool);
  }

  @After
  public void tearDown() throws InterruptedException {
    CompactionHeartbeatService.getInstance(conf).shutdown();
    hiveMetaStoreUtilsMockedStatic.close();
  }

  @Test
  public void testHeartbeat() throws Exception {
    CompactionHeartbeatService.getInstance(conf).startHeartbeat(0, 0,"table");
    Thread.sleep(300);
    CompactionHeartbeatService.getInstance(conf).stopHeartbeat(0);
    verify(client, atLeast(1)).heartbeat(0,0);
  }

  @Test(expected = IllegalStateException.class)
  public void testStopHeartbeatForNonExistentTxn() throws InterruptedException {
    CompactionHeartbeatService.getInstance(conf).stopHeartbeat(0);
  }

  @Test
  public void testNoHeartbeatAfterStop() throws Exception {
    AtomicBoolean stopped = new AtomicBoolean(false);
    doAnswer((Answer<Void>) invocationOnMock -> {
      if (stopped.get()) {
        Assert.fail("Heartbeat after stopHeartbeat call");
      }
      return null;
    }).when(client).heartbeat(0,0);
    CompactionHeartbeatService.getInstance(conf).startHeartbeat(0, 0,"table");
    Thread.sleep(200);
    CompactionHeartbeatService.getInstance(conf).stopHeartbeat(0);
    stopped.set(true);
    verify(client, atLeast(1)).heartbeat(0,0);
  }

  @Test(expected = IllegalStateException.class)
  public void testStartHeartbeatTwice() {
    CompactionHeartbeatService.getInstance(conf).startHeartbeat(0, 0,"table");
    CompactionHeartbeatService.getInstance(conf).startHeartbeat(0, 0,"table");
  }

  @Test
  public void testStopHeartbeatAbortedTheThread() throws Exception {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    AtomicBoolean heartbeated = new AtomicBoolean(false);
    doAnswer((Answer<Void>) invocationOnMock -> {
      //make sure we call stopHeartbeat when we are in the middle of the hearbeat call
      countDownLatch.countDown();
      Thread.sleep(500);
      heartbeated.set(true);
      return null;
    }).when(client).heartbeat(0,0);
    CompactionHeartbeatService.getInstance(conf).startHeartbeat(0, 0,"table");
    //We try to stop heartbeating while it's in the middle of a heartbeat
    countDownLatch.await();
    CompactionHeartbeatService.getInstance(conf).stopHeartbeat(0);
    Assert.assertFalse(heartbeated.get());
    // Check if heartbeat was done only once despite the timing is 100ms and the first took 500ms
    verify(client, times(1)).heartbeat(0,0);
  }

  @Test
  public void testBadClientInvalidated() throws Exception {
    CountDownLatch countDownLatch = new CountDownLatch(3);
    doAnswer((Answer<Void>) invocationOnMock -> {
      countDownLatch.countDown();
      if (countDownLatch.getCount() == 0) {
        Thread.sleep(100);
      }
      throw new RuntimeException();
    }).when(client).heartbeat(0,0);
    CompactionHeartbeatService.getInstance(conf).startHeartbeat(0, 0,"table");
    //We stop only after 3 heartbeats
    countDownLatch.await();
    CompactionHeartbeatService.getInstance(conf).stopHeartbeat(0);
    // Check if bad clients were closed and new ones were requested
    verify(client, times(3)).heartbeat(0,0);
    verify(client, times(3)).close();
    HiveMetaStoreUtils.getHiveMetastoreClient(conf);
    verify(clientPool, times(3)).borrowObject();
  }
}
