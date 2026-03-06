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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.Test;

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
    TestingServer server = null;
    CuratorFramework client = null;
    ZookeeperExternalSessionsRegistryClient registry = null;
    try {
      server = new TestingServer();
      String connectString = server.getConnectString();

      HiveConf conf = new HiveConf();
      conf.setVar(ConfVars.HIVE_ZOOKEEPER_QUORUM, connectString);
      conf.setVar(ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE, "/tez_ns");
      conf.setIntVar(ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_WAIT_MAX_ATTEMPTS, 1);

      String namespace = HiveConf.getVar(conf, ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE);
      String effectivePath = namespace + "/tez_am/server";

      CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
      client = builder.connectString(connectString).retryPolicy(new RetryOneTime(1)).build();
      client.start();

      client.create().creatingParentsIfNeeded().forPath(effectivePath + "/app_1");
      client.create().forPath(effectivePath + "/app_2");

      registry = new ZookeeperExternalSessionsRegistryClient(conf);

      String first = registry.getSession();
      assertTrue(first.equals("app_1") || first.equals("app_2"));

      registry.returnSession(first);
      String second = registry.getSession();
      assertNotNull(second);
      assertTrue(second.equals("app_1") || second.equals("app_2"));
    } finally {
      if (registry != null) {
        registry.close();
      }
      if (client != null) {
        client.close();
      }
      if (server != null) {
        server.close();
      }
    }
  }

  /**
   * Verifies that the same external session ID can be obtained and returned
   * multiple times in a row from {@link ZookeeperExternalSessionsRegistryClient}.
   */
  @Test
  public void testReuseSameSession() throws Exception {
    TestingServer server = null;
    CuratorFramework client = null;
    ZookeeperExternalSessionsRegistryClient registry = null;
    try {
      server = new TestingServer();
      String connectString = server.getConnectString();

      HiveConf conf = new HiveConf();
      conf.setVar(ConfVars.HIVE_ZOOKEEPER_QUORUM, connectString);
      conf.setVar(ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE, "/tez_ns_reuse");
      conf.setIntVar(ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_WAIT_MAX_ATTEMPTS, 1);

      String namespace = HiveConf.getVar(conf, ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE);
      String effectivePath = namespace + "/tez_am/server";

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
      if (server != null) {
        server.close();
      }
    }
  }
}

