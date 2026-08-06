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
package org.apache.hadoop.hive.llap;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.llap.registry.impl.LlapZookeeperRegistryImpl;
import org.apache.hadoop.hive.registry.impl.ZkRegistryBase;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ProactiveEviction} focusing on the ZooKeeper-based LLAP registry interaction
 * with Kerberos authentication enabled.
 *
 * The tests use a local TestingServer (embedded ZooKeeper) and mock UGI to simulate a secure
 * environment without requiring a real KDC. The "llap-sasl" namespace is used because
 * HIVE_ZOOKEEPER_USE_KERBEROS is enabled, which is the namespace the registry uses in production
 * when Kerberos is active.
 */
public class TestProactiveEviction {

  private HiveConf hiveConf = new HiveConf();

  private CuratorFramework curatorFramework;
  private TestingServer server;

  private UserGroupInformation ugi;

  MockedStatic<UserGroupInformation> userGroupInformationMockedStatic;

  @Before
  public void setUp() throws Exception {
    ugi = mock(UserGroupInformation.class);
    userGroupInformationMockedStatic = mockStatic(UserGroupInformation.class);
    userGroupInformationMockedStatic.when(UserGroupInformation::isSecurityEnabled).thenReturn(true);
    userGroupInformationMockedStatic.when(UserGroupInformation::getCurrentUser).thenReturn(ugi);
    when(ugi.getShortUserName()).thenReturn("hive");

    server = new TestingServer();

    hiveConf.setVar(HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS, "@testinstance");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_USE_KERBEROS, true);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM, server.getConnectString());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE, "testinstance");
    hiveConf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_NAMESPACE, "testinstance");
    hiveConf.setVar(HiveConf.ConfVars.LLAP_ZK_REGISTRY_USER, "hive");
    hiveConf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT, "1000ms");
    hiveConf.setVar(HiveConf.ConfVars.LLAP_KERBEROS_PRINCIPAL, "hive/host@REALM");
    hiveConf.setVar(HiveConf.ConfVars.LLAP_KERBEROS_KEYTAB_FILE, "/keytab");
  }

  @After
  public void tearDown() {
    if (curatorFramework != null) {
      CloseableUtils.closeQuietly(curatorFramework);
      curatorFramework = null;
    }
    if (server != null) {
      CloseableUtils.closeQuietly(server);
    }
    if (userGroupInformationMockedStatic != null) {
      userGroupInformationMockedStatic.close();
    }
  }

  /**
   * Verifies that ProactiveEviction.evict() handles gracefully the case where Kerberos is enabled
   * but no LLAP daemon instances are registered in ZooKeeper. The eviction should be skipped
   * without throwing an exception; ClusterNotReadyException is caught internally.
   */
  @Test
  public void testEvictWithKerberosWithoutComputeInstances() throws Exception {
    LlapProxy.setDaemon(true);

    ((Map<?, ?>) FieldUtils.readStaticField(LlapRegistryService.class, "yarnRegistries", true)).clear();

    ProactiveEviction.Request.Builder llapEvictRequestBuilder =
        ProactiveEviction.Request.Builder.create();
    llapEvictRequestBuilder.addTable("testDb", "testTable");

    try {
      ProactiveEviction.evict(hiveConf, llapEvictRequestBuilder.build());
    } catch (Exception e) {
      fail("Expected evict() to handle missing instances gracefully, but threw: " + e);
    }
  }

  /**
   * Verifies that ProactiveEviction.evict() can discover and send eviction requests to LLAP
   * daemon instances registered in ZooKeeper, with Kerberos enabled.
   *
   * The test pre-creates ZK znodes simulating LLAP daemons, then calls evict() which internally
   * creates a fresh LlapRegistryService client that discovers them via the PathChildrenCache.
   * The eviction tasks are fire-and-forget (they will fail to connect to the fake endpoints,
   * but that's logged and swallowed by EvictionRequestTask).
   */
  @Test
  public void testEvictWithKerberosAndRegisteredComputes() throws Exception {
    LlapProxy.setDaemon(true);

    String instanceName = "testinstance";

    LlapZookeeperRegistryImpl registry =
        new LlapZookeeperRegistryImpl(instanceName, hiveConf);

    curatorFramework = CuratorFrameworkFactory.builder()
        .connectString(server.getConnectString())
        .sessionTimeoutMs(1000)
        .namespace("llap-sasl")
        .retryPolicy(new RetryOneTime(1000))
        .build();
    curatorFramework.start();

    FieldUtils.writeField(registry, "zooKeeperClient", curatorFramework, true);

    String workersPath = (String) FieldUtils.readField(registry, "workersPath", true);

    PersistentEphemeralNode znode1 = createZnode(workersPath, "instance-1");
    PersistentEphemeralNode znode2 = createZnode(workersPath, "instance-2");

    ((Map<?, ?>) FieldUtils.readStaticField(LlapRegistryService.class, "yarnRegistries", true)).clear();

    // Verify that the registry discovers both registered instances
    Collection<LlapServiceInstance> instances = registry.getInstances("LLAP", 10000).getAll();
    assertEquals(2, instances.size());

    ProactiveEviction.Request.Builder llapEvictRequestBuilder =
        ProactiveEviction.Request.Builder.create();
    llapEvictRequestBuilder.addTable("testDb", "testTable");
    ProactiveEviction.evict(hiveConf, llapEvictRequestBuilder.build());

    CloseableUtils.closeQuietly(znode1);
    CloseableUtils.closeQuietly(znode2);
  }

  private PersistentEphemeralNode createZnode(String workersPath, String id) throws Exception {
    ServiceRecord serviceRecord = new ServiceRecord();
    serviceRecord.addInternalEndpoint(
        RegistryTypeUtils.ipcEndpoint("llap", new InetSocketAddress("localhost", 4000)));
    serviceRecord.addInternalEndpoint(
        RegistryTypeUtils.ipcEndpoint("shuffle", new InetSocketAddress("localhost", 4001)));
    serviceRecord.addInternalEndpoint(
        RegistryTypeUtils.ipcEndpoint("llapmng", new InetSocketAddress("localhost", 4002)));
    serviceRecord.addInternalEndpoint(
        RegistryTypeUtils.ipcEndpoint("llapoutputformat", new InetSocketAddress("localhost", 4003)));
    serviceRecord.addExternalEndpoint(
        RegistryTypeUtils.webEndpoint("services", new URI("http://localhost:4004")));
    serviceRecord.set(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS, "10");
    serviceRecord.set(HiveConf.ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname, "100");
    serviceRecord.set(ZkRegistryBase.UNIQUE_IDENTIFIER, id);

    PersistentEphemeralNode znode = new PersistentEphemeralNode(
        curatorFramework,
        PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL,
        workersPath + "/worker-",
        new RegistryUtils.ServiceRecordMarshal().toBytes(serviceRecord));
    znode.start();
    if (!znode.waitForInitialCreate(10, TimeUnit.SECONDS)) {
      fail("Max znode creation wait time exhausted");
    }
    return znode;
  }

}
