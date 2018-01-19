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

package org.apache.hadoop.hive.ql.lockmgr.zookeeper;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.common.metrics.metrics2.MetricsReporting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManagerCtx;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;
import org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper;
import org.apache.zookeeper.KeeperException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestZookeeperLockManager {

  private HiveConf conf;
  private TestingServer server;
  private CuratorFramework client;
  private HiveLockObject hiveLock;
  private ZooKeeperHiveLock zLock;
  private HiveLockObjectData lockObjData;
  private static final String PARENT = "hive";
  private static final String TABLE = "t1";
  private static final String PARENT_LOCK_PATH = "/hive/t1";
  private static final String TABLE_LOCK_PATH = "/hive/t1/00001";

  @Before
  public void setup() {
    conf = new HiveConf();
    lockObjData = new HiveLockObjectData("1", "10", "SHARED", "show tables", conf);
    hiveLock = new HiveLockObject(TABLE, lockObjData);
    zLock = new ZooKeeperHiveLock(TABLE_LOCK_PATH, hiveLock, HiveLockMode.SHARED);

    while (server == null)
    {
      try {
        server = new TestingServer();
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        client = builder.connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).build();
        client.start();
      } catch (Exception e) {
        System.err.println("Getting bind exception - retrying to allocate server");
        server = null;
      }
    }
  }

  @After
  public void teardown() throws Exception
  {
    client.close();
    server.close();
    server = null;
  }

  @Test
  public void testDeleteNoChildren() throws Exception
  {
    client.create().creatingParentsIfNeeded().forPath(TABLE_LOCK_PATH, lockObjData.toString().getBytes());
    byte[] data = client.getData().forPath(TABLE_LOCK_PATH);
    Assert.assertArrayEquals(lockObjData.toString().getBytes(), data);
    ZooKeeperHiveLockManager.unlockPrimitive(zLock, PARENT, client);
    try {
      data = client.getData().forPath(TABLE_LOCK_PATH);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertEquals( e instanceof KeeperException.NoNodeException, true);
    }
    try {
      data = client.getData().forPath(PARENT_LOCK_PATH);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertEquals( e instanceof KeeperException.NoNodeException, true);
    }
  }

  @Test
  public void testGetQuorumServers() {
    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM, "node1");
    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT, "9999");
    Assert.assertEquals("node1:9999", ZooKeeperHiveHelper.getQuorumServers(conf));

    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM, "node1,node2,node3");
    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT, "9999");
    Assert.assertEquals("node1:9999,node2:9999,node3:9999", ZooKeeperHiveHelper.getQuorumServers(conf));

    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM, "node1:5666,node2,node3");
    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT, "9999");
    Assert.assertEquals("node1:5666,node2:9999,node3:9999", ZooKeeperHiveHelper.getQuorumServers(conf));
  }

  @Test
  public void testMetrics() throws Exception{
    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM, "localhost");
    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT, String.valueOf(server.getPort()));
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_REPORTER, MetricsReporting.JSON_FILE.name() + "," + MetricsReporting.JMX.name());
    MetricsFactory.init(conf);
    CodahaleMetrics metrics = (CodahaleMetrics) MetricsFactory.getInstance();

    HiveLockManagerCtx ctx = new HiveLockManagerCtx(conf);
    ZooKeeperHiveLockManager zMgr= new ZooKeeperHiveLockManager();
    zMgr.setContext(ctx);
    ZooKeeperHiveLock curLock = zMgr.lock(hiveLock, HiveLockMode.SHARED, false);
    String json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.ZOOKEEPER_HIVE_SHAREDLOCKS, 1);

    zMgr.unlock(curLock);
    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.ZOOKEEPER_HIVE_SHAREDLOCKS, 0);
    zMgr.close();
  }

}

