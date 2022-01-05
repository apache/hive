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

package org.apache.hadoop.hive.common;

import com.google.common.base.Throwables;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.DebugUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.zookeeper.WatchedEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.BindException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class TestDeregisterWatcher {
  private static final Logger LOG = LoggerFactory.getLogger(TestDeregisterWatcher.class);
  private TestingServer server;
  private ZooKeeperHiveHelper zookeeperHiveHelper;

  @Before
  public void setUp() throws Exception {
    System.setProperty(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES, "true");
    while (server == null) {
      try {
        server = new TestingServer();
      } catch (BindException e) {
        LOG.warn("Getting bind exception - retrying to allocate server");
        server = null;
      }
    }

    Configuration conf = MetastoreConf.newMetastoreConf();
    String connectString = server.getConnectString();
    String port = Integer.toString(server.getPort());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS, connectString);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_ZOOKEEPER_CLIENT_PORT, port);
    zookeeperHiveHelper = MetastoreConf.getZKConfig(conf);

    server.start();
  }

  @After
  public void teardown() throws Exception {
    zookeeperHiveHelper.getZookeeperClient().close();
    server.close();
    server = null;
  }

  @Test
  public void testDeregisterAfterSessionExpired() throws Exception {
    TestZKDeRegisterWatcher watcher = new TestZKDeRegisterWatcher(zookeeperHiveHelper);
    zookeeperHiveHelper.addServerInstanceToZooKeeper("test", "testData",
        new DefaultACLProvider(), watcher);

    CuratorFramework curator = zookeeperHiveHelper.getZookeeperClient();
    CountDownLatch reconnected = new CountDownLatch(1);
    curator.getConnectionStateListenable().addListener((client, newState) -> {
      if (newState == ConnectionState.RECONNECTED) {
        reconnected.countDown();
      }
    });

    KillSession.kill(curator.getZookeeperClient().getZooKeeper());
    assertTrue(reconnected.await(10, TimeUnit.SECONDS));

    String znodePath = zookeeperHiveHelper.getZNode().getActualPath();
    curator.delete().forPath(znodePath);

    assertTrue(watcher.waitUntilNodeDeleted(10, TimeUnit.SECONDS));
    assertTrue(zookeeperHiveHelper.isDeregisteredWithZooKeeper());
  }

  private static final class TestZKDeRegisterWatcher extends ZKDeRegisterWatcher {
    private final CountDownLatch latch;

    public TestZKDeRegisterWatcher(ZooKeeperHiveHelper helper) {
      super(helper);
      this.latch = new CountDownLatch(1);
    }

    @Override
    public void process(WatchedEvent event) {
      super.process(event);
      if (event.getType() == Event.EventType.NodeDeleted) {
        latch.countDown();
      }
    }

    public boolean waitUntilNodeDeleted(long duration, TimeUnit unit) {
      try {
        return latch.await(duration, unit);
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
