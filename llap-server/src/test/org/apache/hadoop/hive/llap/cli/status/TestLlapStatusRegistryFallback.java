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

package org.apache.hadoop.hive.llap.cli.status;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests registry-only status helpers used when LLAP runs without YARN.
 */
public class TestLlapStatusRegistryFallback {

  @Test
  public void testCreateLlapInstanceFromRegistryUsesWorkerIdentityWhenNoContainerId() {
    LlapServiceInstance instance = new TestLlapServiceInstance("worker-1", "llap-host-0",
        "http://llap-host-0:15002", 15004, 15001, 15551, Collections.emptyMap());

    LlapInstance llapInstance = LlapStatusServiceDriver.createLlapInstanceFromRegistry(instance);

    assertEquals("llap-host-0", llapInstance.getHostname());
    assertEquals("worker-1", llapInstance.getContainerId());
    assertEquals("http://llap-host-0:15002", llapInstance.getWebUrl());
    assertEquals("http://llap-host-0:15002/status", llapInstance.getStatusUrl());
    assertEquals(Integer.valueOf(15004), llapInstance.getMgmtPort());
  }

  @Test
  public void testCreateLlapInstanceFromRegistryUsesYarnContainerIdWhenPresent() {
    Map<String, String> props = new HashMap<>();
    props.put(HiveConf.ConfVars.LLAP_DAEMON_CONTAINER_ID.varname, "container_123_456");
    LlapServiceInstance instance = new TestLlapServiceInstance("worker-2", "yarn-host",
        "http://yarn-host:15002", 15004, 15001, 15551, props);

    LlapInstance llapInstance = LlapStatusServiceDriver.createLlapInstanceFromRegistry(instance);

    assertEquals("container_123_456", llapInstance.getContainerId());
  }

  @Test
  public void testUpdateStateFromInstanceCounts() {
    AppStatusBuilder builder = new AppStatusBuilder();
    builder.setDesiredInstances(2);

    LlapStatusServiceDriver.updateStateFromInstanceCounts(builder, 2);
    assertEquals(State.RUNNING_ALL, builder.getState());

    LlapStatusServiceDriver.updateStateFromInstanceCounts(builder, 1);
    assertEquals(State.RUNNING_PARTIAL, builder.getState());

    LlapStatusServiceDriver.updateStateFromInstanceCounts(builder, 0);
    assertEquals(State.LAUNCHING, builder.getState());
  }

  @Test
  public void testUsesRegistryBasedLlapStatusFromExternalSessions() {
    Configuration conf = new Configuration(false);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_TEZ_USE_EXTERNAL_SESSIONS, true);
    assertTrue(LlapStatusServiceDriver.usesRegistryBasedLlapStatus(conf));
  }

  @Test
  public void testUsesRegistryBasedLlapStatusFromTezFrameworkMode() {
    Configuration conf = new Configuration(false);
    conf.set("tez.am.framework.mode", "STANDALONE_ZOOKEEPER");
    assertTrue(LlapStatusServiceDriver.usesRegistryBasedLlapStatus(conf));
  }

  @Test
  public void testUsesYarnBasedLlapStatusByDefault() {
    Configuration conf = new Configuration(false);
    assertFalse(LlapStatusServiceDriver.usesRegistryBasedLlapStatus(conf));
  }

  @Test
  public void testUpdateRunningThresholdAchievedWhenFullyRunning() {
    AppStatusBuilder builder = new AppStatusBuilder();
    builder.setDesiredInstances(2);
    builder.setLiveInstances(2);
    builder.setState(State.RUNNING_ALL);

    LlapStatusServiceDriver.updateRunningThresholdAchieved(builder, 1.0f);
    assertTrue(builder.isRunningThresholdAchieved());
  }

  @Test
  public void testUpdateRunningThresholdAchievedWhenLaunching() {
    AppStatusBuilder builder = new AppStatusBuilder();
    builder.setDesiredInstances(2);
    builder.setLiveInstances(0);
    builder.setState(State.LAUNCHING);

    LlapStatusServiceDriver.updateRunningThresholdAchieved(builder, 1.0f);
    assertFalse(builder.isRunningThresholdAchieved());
  }

  private static final class TestLlapServiceInstance implements LlapServiceInstance {
    private final String workerIdentity;
    private final String host;
    private final String servicesAddress;
    private final int mgmtPort;
    private final int rpcPort;
    private final int shufflePort;
    private final Map<String, String> properties;

    private TestLlapServiceInstance(String workerIdentity, String host, String servicesAddress,
        int mgmtPort, int rpcPort, int shufflePort, Map<String, String> properties) {
      this.workerIdentity = workerIdentity;
      this.host = host;
      this.servicesAddress = servicesAddress;
      this.mgmtPort = mgmtPort;
      this.rpcPort = rpcPort;
      this.shufflePort = shufflePort;
      this.properties = properties;
    }

    @Override
    public String getWorkerIdentity() {
      return workerIdentity;
    }

    @Override
    public String getHost() {
      return host;
    }

    @Override
    public int getRpcPort() {
      return rpcPort;
    }

    @Override
    public Map<String, String> getProperties() {
      return properties;
    }

    @Override
    public int getManagementPort() {
      return mgmtPort;
    }

    @Override
    public int getShufflePort() {
      return shufflePort;
    }

    @Override
    public String getServicesAddress() {
      return servicesAddress;
    }

    @Override
    public int getOutputFormatPort() {
      return 0;
    }

    @Override
    public String getExternalHostname() {
      return host;
    }

    @Override
    public int getExternalClientsRpcPort() {
      return 0;
    }

    @Override
    public Resource getResource() {
      return null;
    }
  }
}
