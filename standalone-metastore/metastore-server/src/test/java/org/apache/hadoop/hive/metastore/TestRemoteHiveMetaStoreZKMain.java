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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ZooKeeperHiveHelper;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Starts the metastore the way {@link HiveMetaStore#main} does: zookeeper discovery with the URIs cleared.
 */
@Category(MetastoreCheckinTest.class)
public class TestRemoteHiveMetaStoreZKMain {
  private static final String CHROOT = "/hive/chroot";
  private static final String NAMESPACE = "TestRemoteHiveMetaStoreZKMain";

  private TestingServer zkServer;
  private Configuration conf;
  private Thread metaStoreThread;

  @Before
  public void setUp() throws Exception {
    zkServer = new TestingServer();
    try (CuratorFramework zk = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(1))) {
      zk.start();
      zk.create().creatingParentsIfNeeded().forPath(CHROOT);
    }
    conf = MetastoreConf.newMetastoreConf();
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetastoreConf.setVar(conf, ConfVars.CONNECT_URL_KEY, "jdbc:derby:memory:" + NAMESPACE + ";create=true");
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, zkServer.getConnectString() + CHROOT);
    MetastoreConf.setVar(conf, ConfVars.THRIFT_ZOOKEEPER_NAMESPACE, NAMESPACE);
    MetastoreConf.setVar(conf, ConfVars.THRIFT_SERVICE_DISCOVERY_MODE, "zookeeper");
    MetastoreConf.setVar(conf, ConfVars.THRIFT_BIND_HOST, "localhost");
  }

  @After
  public void tearDown() throws Exception {
    if (metaStoreThread != null) {
      metaStoreThread.interrupt();
    }
    if (zkServer != null) {
      zkServer.close();
    }
  }

  @Test
  public void testRegistersInQuorumClearedByMain() throws Exception {
    // as main: build the helper, then clear the URIs
    ZooKeeperHiveHelper helper = HiveMetaStore.getRegistrationHelper(conf);
    conf.set(ConfVars.THRIFT_URIS.getHiveName(), "");
    conf.set(ConfVars.THRIFT_URIS.getVarname(), "");

    int port = MetaStoreTestUtils.findFreePort();
    metaStoreThread = new Thread(() -> {
      try {
        HiveMetaStore.startMetaStore(port, HadoopThriftAuthBridge.getBridge(), conf, false, null, helper);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    });
    metaStoreThread.setDaemon(true);
    metaStoreThread.start();

    MetaStoreTestUtils.waitForAssertion("metastore registered",
        () -> Assert.assertEquals(List.of("localhost:" + port), serverUris(helper)), 200, 60000);

    // as the shutdown hook of main
    helper.removeServerInstanceFromZooKeeper();
    Assert.assertEquals(List.of(), serverUris(helper));
  }

  @Test
  public void testEmptyQuorumIsRejected() {
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "");
    Assert.assertThrows(IllegalArgumentException.class, () -> HiveMetaStore.getRegistrationHelper(conf));
  }

  private static List<String> serverUris(ZooKeeperHiveHelper helper) {
    try {
      return helper.getServerUris();
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }
}
