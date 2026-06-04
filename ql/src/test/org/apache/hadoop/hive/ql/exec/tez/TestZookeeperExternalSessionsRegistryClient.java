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

package org.apache.hadoop.hive.ql.exec.tez;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ZookeeperExternalSessionsRegistryClient}.
 */
public class TestZookeeperExternalSessionsRegistryClient {

  /**
   * Integration-style unit test that ensures {@link ZookeeperExternalSessionsRegistryClient}
   * can discover sessions from ZooKeeper and hand them out via {@link ExternalSessionsRegistry#getSession()}.
   */
  @Test
  public void testGetAndReturnSession() throws Exception {
    CuratorFramework client = null;
    ZookeeperExternalSessionsRegistryClient registry = null;

    try (TestingServer server = new TestingServer()) {
      String connectString = server.getConnectString();

      HiveConf conf = new HiveConf();
      conf.setVar(ConfVars.HIVE_ZOOKEEPER_QUORUM, connectString);
      conf.setVar(ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE, "/tez_ns");
      conf.setIntVar(ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_WAIT_MAX_ATTEMPTS, 1);

      String namespace = HiveConf.getVar(conf, ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE);
      String effectivePath = ZookeeperExternalSessionsRegistryClient.normalizeZkPath(namespace);

      CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
      client = builder.connectString(connectString).retryPolicy(new RetryOneTime(1)).build();
      client.start();

      client.create().creatingParentsIfNeeded().forPath(effectivePath + "/app_1");
      client.create().forPath(effectivePath + "/app_2");

      registry = new ZookeeperExternalSessionsRegistryClient(conf);

      String first = registry.getSession();
      assertTrue("app_1".equals(first) || "app_2".equals(first));

      registry.returnSession(first);
      String second = registry.getSession();
      assertNotNull(second);
      assertTrue("app_1".equals(second) || "app_2".equals(second));
    } finally {
      if (registry != null) {
        registry.close();
      }
      if (client != null) {
        client.close();
      }
    }
  }

  /**
   * Verifies that the same external session ID can be obtained and returned
   * multiple times in a row from {@link ZookeeperExternalSessionsRegistryClient}.
   */
  @Test
  public void testReuseSameSession() throws Exception {
    CuratorFramework client = null;
    ZookeeperExternalSessionsRegistryClient registry = null;

    try (TestingServer server = new TestingServer()) {
      String connectString = server.getConnectString();

      HiveConf conf = new HiveConf();
      conf.setVar(ConfVars.HIVE_ZOOKEEPER_QUORUM, connectString);
      conf.setVar(ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE, "/tez_ns_reuse");
      conf.setIntVar(ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_WAIT_MAX_ATTEMPTS, 1);

      String namespace = HiveConf.getVar(conf, ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE);
      String effectivePath = ZookeeperExternalSessionsRegistryClient.normalizeZkPath(namespace);

      CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
      client = builder.connectString(connectString).retryPolicy(new RetryOneTime(1)).build();
      client.start();

      client.create().creatingParentsIfNeeded().forPath(effectivePath + "/app_reuse");

      registry = new ZookeeperExternalSessionsRegistryClient(conf);

      String first = registry.getSession();
      assertEquals("app_reuse", first);

      registry.returnSession(first);
      String second = registry.getSession();
      assertEquals("app_reuse", second);

      registry.returnSession(second);
      String third = registry.getSession();
      assertEquals("app_reuse", third);
    } finally {
      if (registry != null) {
        registry.close();
      }
      if (client != null) {
        client.close();
      }
    }
  }

  /**
   * Tests that multiple registry clients (simulating multiple HS2 instances)
   * respect the global distributed lock (claims) and do not claim the same session simultaneously.
   */
  @Test
  public void testSessionClaimsFromDifferentRegistryClients() throws Exception {
    CuratorFramework client = null;
    ZookeeperExternalSessionsRegistryClient registry1 = null;
    ZookeeperExternalSessionsRegistryClient registry2 = null;

    try (TestingServer server = new TestingServer()) {
      String connectString = server.getConnectString();

      HiveConf conf = new HiveConf();
      conf.setVar(ConfVars.HIVE_ZOOKEEPER_QUORUM, connectString);
      conf.setVar(ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE, "/tez_ns_concurrent");
      conf.setIntVar(ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_WAIT_MAX_ATTEMPTS, 5);

      String namespace = HiveConf.getVar(conf, ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE);
      String effectivePath = ZookeeperExternalSessionsRegistryClient.normalizeZkPath(namespace);

      CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
      client = builder.connectString(connectString).retryPolicy(new RetryOneTime(1)).build();
      client.start();

      client.create().creatingParentsIfNeeded().forPath(effectivePath + "/app_1");
      client.create().forPath(effectivePath + "/app_2");

      registry1 = new ZookeeperExternalSessionsRegistryClient(conf);
      registry2 = new ZookeeperExternalSessionsRegistryClient(conf);

      String sessionFromRegistry1 = registry1.getSession();
      String sessionFromRegistry2 = registry2.getSession();

      assertNotNull("Registry 1 should have claimed a session", sessionFromRegistry1);
      assertNotNull("Registry 2 should have claimed a session", sessionFromRegistry2);

      assertNotEquals("The two registries should claim different sessions!",
          sessionFromRegistry1, sessionFromRegistry2);

      registry1.returnSession(sessionFromRegistry1);

      String session3FromRegistry2 = registry2.getSession();
      assertEquals("Registry 2 should be able to claim the newly released session",
          sessionFromRegistry1, session3FromRegistry2);

      registry2.returnSession(sessionFromRegistry2);
      registry2.returnSession(session3FromRegistry2);
    } finally {
      if (registry1 != null) {
        registry1.close();
      }
      if (registry2 != null) {
        registry2.close();
      }
      if (client != null) {
        client.close();
      }
    }
  }

  /**
   * Tests that the InterProcessMutex enforces strict Global FIFO ordering.
   * Clients form a queue when no sessions are available, and are served in exact order.
   */
  @Test
  public void testFIFOSessionClaimsFromDifferentRegistries() throws Exception {
    try (TestingServer server = new TestingServer()) {
      String connectString = server.getConnectString();

      HiveConf conf = new HiveConf();
      conf.setVar(ConfVars.HIVE_ZOOKEEPER_QUORUM, connectString);
      conf.setVar(ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE, "/tez_ns_fifo");
      conf.setIntVar(ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_WAIT_MAX_ATTEMPTS, 15);

      String namespace = HiveConf.getVar(conf, ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE);
      String effectivePath = ZookeeperExternalSessionsRegistryClient.normalizeZkPath(namespace);

      CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
      CuratorFramework client = builder.connectString(connectString).retryPolicy(new RetryOneTime(1)).build();
      client.start();

      ExecutorService executor = Executors.newFixedThreadPool(3);
      ZookeeperExternalSessionsRegistryClient registry1 = new ZookeeperExternalSessionsRegistryClient(conf);
      ZookeeperExternalSessionsRegistryClient registry2 = new ZookeeperExternalSessionsRegistryClient(conf);
      ZookeeperExternalSessionsRegistryClient registry3 = new ZookeeperExternalSessionsRegistryClient(conf);
      try {
        Future<String> future1 = executor.submit(registry1::getSession);
        Thread.sleep(500);
        Future<String> future2 = executor.submit(registry2::getSession);
        Thread.sleep(500);
        Future<String> future3 = executor.submit(registry3::getSession);

        client.create().creatingParentsIfNeeded().forPath(effectivePath + "/app_first");
        assertEquals("Registry 1 should get the first AM", "app_first", future1.get(5, TimeUnit.SECONDS));

        client.create().forPath(effectivePath + "/app_second");
        String session2 = future2.get(5, TimeUnit.SECONDS);

        assertEquals("Registry 2 should get the second AM", "app_second", session2);
        registry2.returnSession(session2);

        assertEquals("Registry 3 should get the second AM", "app_second", future3.get(5, TimeUnit.SECONDS));
      } finally {
        registry1.close();
        registry2.close();
        registry3.close();
        client.close();
        executor.shutdownNow();
      }
    }
  }
}

