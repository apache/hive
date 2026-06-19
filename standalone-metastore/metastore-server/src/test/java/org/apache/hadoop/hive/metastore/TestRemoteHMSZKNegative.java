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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ZooKeeperHiveHelper;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test MetaStore Client throws exception when no MetaStore spec found in zookeeper.
 *
 */
public class TestRemoteHMSZKNegative {
  private TestingServer zkServer;
  private Configuration conf;
  private CuratorFramework zkClient;
  private String rootNamespace = this.getClass().getSimpleName();

  @Before
  public void setUp() throws Exception {
    zkServer = new TestingServer();
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, zkServer.getConnectString());
    MetastoreConf.setVar(conf, ConfVars.THRIFT_ZOOKEEPER_NAMESPACE, rootNamespace);
    MetastoreConf.setVar(conf, ConfVars.THRIFT_SERVICE_DISCOVERY_MODE, "zookeeper");
    zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
    zkClient.start();
    zkClient.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT)
        .forPath(ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace);
  }

  @Test
  public void createClient() {
    try {
      new HiveMetaStoreClient(conf);
      Assert.fail("an exception is expected");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof MetaException);
      Assert.assertTrue(e.getMessage().contains("No metastore service discovered in ZooKeeper"));
    }
  }

  @After
  public void stop() throws Exception {
    if (zkClient != null) {
      zkClient.close();
    }
    if (zkServer != null) {
      zkServer.stop();
    }
  }
}
