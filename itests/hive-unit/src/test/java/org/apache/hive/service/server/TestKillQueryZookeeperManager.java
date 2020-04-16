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
package org.apache.hive.service.server;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link KillQueryZookeeperManager}.
 */
public class TestKillQueryZookeeperManager {

  private static final Logger LOG = LoggerFactory.getLogger(TestKillQueryZookeeperManager.class);
  private static final String BARRIER_ROOT_PATH = "/killqueries";
  private static final String QUERYID = "QUERY1";
  private static final String SERVER1 = "localhost:1234";
  private static final String SERVER2 = "localhost:1235";
  private static final String USER = "user";
  private static final int TIMEOUT = 1000;

  TestingServer server;

  @Before
  public void setupZookeeper() throws Exception {
    server = new TestingServer();
  }

  @After
  public void shutdown() {
    if (server != null) {
      CloseableUtils.closeQuietly(server);
    }
  }

  private CuratorFramework getClient() {
    return CuratorFrameworkFactory.builder().connectString(server.getConnectString()).sessionTimeoutMs(TIMEOUT * 100)
        .connectionTimeoutMs(TIMEOUT).retryPolicy(new RetryOneTime(1)).build();
  }

  @Test
  public void testBarrierServerCrash() throws Exception {
    try (CuratorFramework client = getClient()) {
      client.start();
      client.create().creatingParentContainersIfNeeded().forPath(BARRIER_ROOT_PATH);
      final KillQueryZookeeperManager.KillQueryZookeeperBarrier barrier =
          new KillQueryZookeeperManager.KillQueryZookeeperBarrier(client, BARRIER_ROOT_PATH);
      barrier.setBarrier(QUERYID, SERVER1, USER, true);

      final ExecutorService service = Executors.newSingleThreadExecutor();
      Future<Object> future = service.submit(() -> {
        Thread.sleep(TIMEOUT / 2);
        server.stop();
        return null;
      });

      barrier.waitOnBarrier(1, TIMEOUT, TIMEOUT * 2, TimeUnit.MILLISECONDS);
      future.get();
      Assert.fail();
    } catch (KeeperException.ConnectionLossException expected) {
      // expected
    }
  }

  @Test
  public void testNoBarrier() throws Exception {
    try (CuratorFramework client = getClient()) {
      client.start();
      client.create().creatingParentContainersIfNeeded().forPath(BARRIER_ROOT_PATH);
      IllegalStateException result = null;
      final KillQueryZookeeperManager.KillQueryZookeeperBarrier barrier =
          new KillQueryZookeeperManager.KillQueryZookeeperBarrier(client, BARRIER_ROOT_PATH);
      try {
        barrier.confirmProgress(SERVER1);
      } catch (IllegalStateException e) {
        result = e;
      }
      Assert.assertNotNull(result);
      Assert.assertEquals("Barrier is not initialised", result.getMessage());
    }
  }

  @Test
  public void testNo() throws Exception {
    try (CuratorFramework client = getClient()) {
      client.start();
      client.create().creatingParentContainersIfNeeded().forPath(BARRIER_ROOT_PATH);
      final KillQueryZookeeperManager.KillQueryZookeeperBarrier barrier =
          new KillQueryZookeeperManager.KillQueryZookeeperBarrier(client, BARRIER_ROOT_PATH);
      barrier.setBarrier(QUERYID, SERVER1, USER, true);

      ExecutorService service = Executors.newSingleThreadExecutor();
      service.submit(() -> {
        Thread.sleep(TIMEOUT / 2);
        barrier.confirmNo(SERVER2);
        return null;
      });

      Assert.assertFalse(barrier.waitOnBarrier(1, TIMEOUT, TIMEOUT * 2, TimeUnit.MILLISECONDS));
    }
  }

  @Test
  public void testDone() throws Exception {
    try (CuratorFramework client = getClient()) {
      client.start();
      client.create().creatingParentContainersIfNeeded().forPath(BARRIER_ROOT_PATH);
      final KillQueryZookeeperManager.KillQueryZookeeperBarrier barrier =
          new KillQueryZookeeperManager.KillQueryZookeeperBarrier(client, BARRIER_ROOT_PATH);
      barrier.setBarrier(QUERYID, SERVER1, USER, true);

      ExecutorService service = Executors.newSingleThreadExecutor();
      service.submit(() -> {
        Thread.sleep(TIMEOUT / 2);
        try {
          barrier.confirmProgress(SERVER2);
          Thread.sleep(TIMEOUT / 2);
          barrier.confirmDone(SERVER2);
        } catch (Exception e) {
          LOG.error("Confirmation error", e);
        }
        return null;
      });

      Assert.assertTrue(barrier.waitOnBarrier(1, TIMEOUT, TIMEOUT, TimeUnit.MILLISECONDS));
    }
  }

  @Test
  public void testFailed() throws Exception {
    try (CuratorFramework client = getClient()) {
      client.start();
      client.create().creatingParentContainersIfNeeded().forPath(BARRIER_ROOT_PATH);
      final KillQueryZookeeperManager.KillQueryZookeeperBarrier barrier =
          new KillQueryZookeeperManager.KillQueryZookeeperBarrier(client, BARRIER_ROOT_PATH);
      barrier.setBarrier(QUERYID, SERVER1, USER, true);

      ExecutorService service = Executors.newSingleThreadExecutor();
      service.submit(() -> {
        Thread.sleep(TIMEOUT / 2);
        barrier.confirmProgress(SERVER2);
        Thread.sleep(TIMEOUT / 2);
        barrier.confirmFailed(SERVER2);
        return null;
      });

      Assert.assertFalse(barrier.waitOnBarrier(1, TIMEOUT, TIMEOUT * 2, TimeUnit.MILLISECONDS));
    }
  }

  @Test
  public void testConfirmTimeout() throws Exception {
    try (CuratorFramework client = getClient()) {
      client.start();
      client.create().creatingParentContainersIfNeeded().forPath(BARRIER_ROOT_PATH);
      final KillQueryZookeeperManager.KillQueryZookeeperBarrier barrier =
          new KillQueryZookeeperManager.KillQueryZookeeperBarrier(client, BARRIER_ROOT_PATH);
      barrier.setBarrier(QUERYID, SERVER1, USER, true);

      Assert.assertFalse(barrier.waitOnBarrier(1, TIMEOUT, TIMEOUT * 2, TimeUnit.MILLISECONDS));
    }
  }

  @Test
  public void testKillTimeout() throws Exception {
    try (CuratorFramework client = getClient()) {
      client.start();
      client.create().creatingParentContainersIfNeeded().forPath(BARRIER_ROOT_PATH);
      final KillQueryZookeeperManager.KillQueryZookeeperBarrier barrier =
          new KillQueryZookeeperManager.KillQueryZookeeperBarrier(client, BARRIER_ROOT_PATH);
      barrier.setBarrier(QUERYID, SERVER1, USER, true);
      ExecutorService service = Executors.newSingleThreadExecutor();
      service.submit(() -> {
        Thread.sleep(TIMEOUT / 2);
        barrier.confirmProgress(SERVER2);
        // server died
        return null;
      });
      Assert.assertFalse(barrier.waitOnBarrier(1, TIMEOUT, TIMEOUT * 2, TimeUnit.MILLISECONDS));
    }
  }
}
