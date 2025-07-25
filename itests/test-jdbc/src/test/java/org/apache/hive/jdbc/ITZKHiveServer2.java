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
package org.apache.hive.jdbc;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class ITZKHiveServer2 extends ITHiveServer2 {
  private final String hostName = "test-standalone-jdbc-plain";
  private final TestItZookeeper zookeeper;
  public ITZKHiveServer2(File workDir) throws Exception {
    super();
    zookeeper = new TestItZookeeper();
    zookeeper.start();
  }

  @Override
  protected Map<String, String> prepareEnvArgs() {
    Map<String, String> envArgs = new HashMap<>();
    envArgs.put("SERVICE_NAME", "hiveserver2");
    envArgs.put("HIVE_SERVER2_TRANSPORT_MODE", "all");
    List<String> properties = new ArrayList<>();
    properties.add("hive.zookeeper.quorum=zookeeper");
    properties.add("hive.server2.support.dynamic.service.discovery=true");
    properties.add("hive.server2.thrift.port=10010");
    properties.add("hive.server2.thrift.http.port=10011");
    StringBuilder builder = new StringBuilder();
    for (String prop : properties) {
      builder.append("-D").append(prop).append(" ");
    }
    envArgs.put("SERVICE_OPTS", builder.toString());
    return envArgs;
  }

  @Override
  protected void beforeStart(GenericContainer<?> container) {
    container
        .withNetwork(zookeeper.network)
        .withCreateContainerCmdModifier(it -> it.withHostName(hostName))
        .setPortBindings(List.of("10010:10010", "10011:10011"));
  }

  @Override
  protected String getHttpJdbcUrl() {
    return "jdbc:hive2://" + hostName + ":" + container.getMappedPort(10011) + "/;" +
        "transportMode=http;httpPath=cliservice";
  }

  @Override
  protected String getBaseJdbcUrl() {
    return "jdbc:hive2://" + hostName + ":" + container.getMappedPort(10010) + "/";
  }

  @Override
  protected String getZkConnectionUrl() {
    return "jdbc:hive2://" + hostName + ":" + zookeeper.getListeningPort() + "/default;" +
        "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
  }

  @Override
  public void stop() throws Exception {
    try {
      super.stop();
    } finally {
      zookeeper.stop();
    }
  }

  private static class TestItZookeeper extends ITAbstractContainer {
    Network network = Network.newNetwork();
    GenericContainer<?> zookeeper;

    @Override
    public void start() throws Exception {
      zookeeper = new GenericContainer<>(DockerImageName.parse("zookeeper:3.8.4"))
          .withNetwork(network)
          .withNetworkAliases("zookeeper")
          .withExposedPorts(2181)
          .waitingFor(Wait.forLogMessage(".*binding to port.*2181.*\\n", 1));
      zookeeper.start();
    }

    @Override
    public void stop() throws Exception {
      try {
        network.close();
      } finally {
        if (zookeeper != null) {
          zookeeper.stop();
        }
      }
    }

    public int getListeningPort() {
      return zookeeper.getMappedPort(2181);
    }
  }
}
