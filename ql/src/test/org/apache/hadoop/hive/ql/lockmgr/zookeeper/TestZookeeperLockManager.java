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

import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.common.metrics.metrics2.MetricsReporting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManagerCtx;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.zookeeper.KeeperException;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

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
    conf.setVar(ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES, "100ms");
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
    Assert.assertEquals("node1:9999", conf.getZKConfig().getQuorumServers());

    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM, "node1,node2,node3");
    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT, "9999");
    Assert.assertEquals("node1:9999,node2:9999,node3:9999",
            conf.getZKConfig().getQuorumServers());

    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM, "node1:5666,node2,node3");
    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT, "9999");
    Assert.assertEquals("node1:5666,node2:9999,node3:9999",
            conf.getZKConfig().getQuorumServers());
  }

  @Ignore("HIVE-24859")
  @Test()
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

    testLockModeTiming(zMgr, HiveLockMode.SHARED, HiveLockMode.SHARED, 1000, 2000);
    testLockModeTiming(zMgr, HiveLockMode.SEMI_SHARED, HiveLockMode.SEMI_SHARED, 2000, 3000);
    testLockModeTiming(zMgr, HiveLockMode.EXCLUSIVE, HiveLockMode.SHARED, 2000, 3000);
    testLockModeTiming(zMgr, HiveLockMode.EXCLUSIVE, HiveLockMode.SEMI_SHARED, 2000, 3000);

    zMgr.close();
  }

  static class LockTesterThread extends Thread {
    private CyclicBarrier barrier;
    private ZooKeeperHiveLockManager zMgr;
    private HiveLockObject hiveLock;
    private HiveLockMode lockMode;
    private Semaphore semaphore;

    public LockTesterThread(CyclicBarrier barrier, Semaphore semaphore, ZooKeeperHiveLockManager zMgr,
        HiveLockObject hiveLock,
        HiveLockMode semiShared) {
      this.barrier = barrier;
      this.semaphore = semaphore;
      this.zMgr = zMgr;
      this.hiveLock = hiveLock;
      this.lockMode = semiShared;

    }

    public void run() {
      try {
        barrier.await();
        ZooKeeperHiveLock l = zMgr.lock(hiveLock, lockMode, false);
        Thread.sleep(1000);
        zMgr.unlock(l);
        semaphore.release();
      } catch (LockException | InterruptedException | BrokenBarrierException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  private void testLockModeTiming(ZooKeeperHiveLockManager zMgr, HiveLockMode lock1mode, HiveLockMode lock2mode,
      int minT, int maxT)
      throws Exception, LockException, InterruptedException, BrokenBarrierException {

    CyclicBarrier barrier = new CyclicBarrier(3);
    Semaphore semaphore = new Semaphore(0);

    new LockTesterThread(barrier, semaphore, zMgr, hiveLock, lock1mode).start();
    new LockTesterThread(barrier, semaphore, zMgr, hiveLock, lock2mode).start();
    barrier.await();
    long t0 = System.currentTimeMillis();
    semaphore.acquire(2);
    long t1 = System.currentTimeMillis();
    long dt = t1 - t0;
    assertTrue(String.format("%d>%d", dt, minT), dt > minT);
    assertTrue(String.format("%d<%d", dt, maxT), dt < maxT);
  }

}

