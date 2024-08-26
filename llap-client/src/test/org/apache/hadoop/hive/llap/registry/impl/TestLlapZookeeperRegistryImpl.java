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
package org.apache.hadoop.hive.llap.registry.impl;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.registry.ServiceInstanceSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Integer.parseInt;
import static org.junit.Assert.assertEquals;

public class TestLlapZookeeperRegistryImpl {

  private HiveConf hiveConf = new HiveConf();

  private LlapZookeeperRegistryImpl registry;

  private CuratorFramework curatorFramework;
  private TestingServer server;
  private final static String NAMESPACE_PREFIX = "llap-";

  @Before
  public void setUp() throws Exception {
    hiveConf.set(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM.varname, "localhost");
    registry = new LlapZookeeperRegistryImpl("TestLlapZookeeperRegistryImpl", hiveConf);

    server = new TestingServer();
    server.start();

    curatorFramework = CuratorFrameworkFactory.
            builder().
            connectString(server.getConnectString()).
            sessionTimeoutMs(1000).
            retryPolicy(new RetryOneTime(1000)).
            build();
    curatorFramework.start();

    trySetMock(registry, "zooKeeperClient", curatorFramework);

  }

  @After
  public void tearDown() throws IOException {
    curatorFramework.close();
    server.stop();
  }

  @Test
  public void testRegister() throws Exception {
    // Given
    int expectedExecutorCount = HiveConf.getIntVar(hiveConf, HiveConf.ConfVars.LLAP_DAEMON_NUM_EXECUTORS);
    int expectedQueueSize = HiveConf.getIntVar(hiveConf, HiveConf.ConfVars.LLAP_DAEMON_TASK_SCHEDULER_WAIT_QUEUE_SIZE);

    // When
    registry.register();
    ServiceInstanceSet<LlapServiceInstance> serviceInstanceSet =
        registry.getInstances("LLAP", 1000);

    // Then
    Collection<LlapServiceInstance> llaps = serviceInstanceSet.getAll();
    assertEquals(1, llaps.size());
    LlapServiceInstance serviceInstance = llaps.iterator().next();
    Map<String, String> attributes = serviceInstance.getProperties();

    assertEquals(expectedQueueSize,
        parseInt(attributes.get(LlapRegistryService.LLAP_DAEMON_TASK_SCHEDULER_ENABLED_WAIT_QUEUE_SIZE)));
    assertEquals(expectedExecutorCount,
        parseInt(attributes.get(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS)));
  }

  @Test
  public void testUpdate() throws Exception {
    // Given
    String expectedExecutorCount = "2";
    String expectedQueueSize = "20";
    Map<String, String> capacityValues = new HashMap<>(2);
    capacityValues.put(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS, expectedExecutorCount);
    capacityValues.put(LlapRegistryService.LLAP_DAEMON_TASK_SCHEDULER_ENABLED_WAIT_QUEUE_SIZE, expectedQueueSize);

    // When
    registry.register();
    registry.updateRegistration(capacityValues.entrySet());
    ServiceInstanceSet<LlapServiceInstance> serviceInstanceSet =
            registry.getInstances("LLAP", 1000);

    // Then
    Collection<LlapServiceInstance> llaps = serviceInstanceSet.getAll();
    assertEquals(1, llaps.size());
    LlapServiceInstance serviceInstance = llaps.iterator().next();
    Map<String, String> attributes = serviceInstance.getProperties();

    assertEquals(expectedQueueSize,
            attributes.get(LlapRegistryService.LLAP_DAEMON_TASK_SCHEDULER_ENABLED_WAIT_QUEUE_SIZE));
    assertEquals(expectedExecutorCount,
            attributes.get(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS));
  }

  @Test
  public void testPersistentNodePath() {
    String llapRootNameSpace = "/" + LlapZookeeperRegistryImpl.getRootNamespace(hiveConf,
        HiveConf.getVar(hiveConf, HiveConf.ConfVars.LLAP_ZK_REGISTRY_NAMESPACE), NAMESPACE_PREFIX);
    String persistentNodeName = "/pnode0";

    Assert.assertEquals(llapRootNameSpace + "/user-" + System.getProperty("user.name") + persistentNodeName,
        registry.getPersistentNodePath());
  }

  static <T> void trySetMock(Object o, String field, T value) {
    try {
      Field fieldToChange = Arrays.stream(FieldUtils.getAllFields(o.getClass()))
              .filter(f -> f.getName().equals(field))
              .findFirst()
              .orElseThrow(NoSuchFieldException::new);

      fieldToChange.setAccessible(true);

      fieldToChange.set(o, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Mocking is only supported, if only one field is assignable from the given class.");
    }
  }
}
