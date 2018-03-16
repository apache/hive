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

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.registry.impl.ZkRegistryBase;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.server.HS2ActivePassiveHARegistry;
import org.apache.hive.service.server.HS2ActivePassiveHARegistryClient;
import org.apache.hive.service.server.HiveServer2Instance;
import org.apache.hive.service.servlet.HS2Peers;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestActivePassiveHA {
  private MiniHS2 miniHS2_1 = null;
  private MiniHS2 miniHS2_2 = null;
  private static TestingServer zkServer;
  private Connection hs2Conn = null;
  private HiveConf hiveConf1;
  private HiveConf hiveConf2;

  @BeforeClass
  public static void beforeTest() throws Exception {
    MiniHS2.cleanupLocalDir();
    zkServer = new TestingServer();
    Class.forName(MiniHS2.getJdbcDriverName());
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (zkServer != null) {
      zkServer.close();
      zkServer = null;
    }
    MiniHS2.cleanupLocalDir();
  }

  @Before
  public void setUp() throws Exception {
    hiveConf1 = new HiveConf();
    hiveConf1.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    // Set up zookeeper dynamic service discovery configs
    setHAConfigs(hiveConf1);
    miniHS2_1 = new MiniHS2.Builder().withConf(hiveConf1).cleanupLocalDirOnStartup(false).build();
    hiveConf2 = new HiveConf();
    hiveConf2.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    // Set up zookeeper dynamic service discovery configs
    setHAConfigs(hiveConf2);
    miniHS2_2 = new MiniHS2.Builder().withConf(hiveConf2).cleanupLocalDirOnStartup(false).build();
  }

  @After
  public void tearDown() throws Exception {
    if (hs2Conn != null) {
      hs2Conn.close();
    }
    if ((miniHS2_1 != null) && miniHS2_1.isStarted()) {
      miniHS2_1.stop();
    }
    if ((miniHS2_2 != null) && miniHS2_2.isStarted()) {
      miniHS2_2.stop();
    }
  }

  private static void setHAConfigs(Configuration conf) {
    conf.setBoolean(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY.varname, true);
    conf.set(ConfVars.HIVE_ZOOKEEPER_QUORUM.varname, zkServer.getConnectString());
    final String zkRootNamespace = "hs2test";
    conf.set(ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE.varname, zkRootNamespace);
    conf.setBoolean(ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_ENABLE.varname, true);
    conf.setTimeDuration(ConfVars.HIVE_ZOOKEEPER_CONNECTION_TIMEOUT.varname, 2, TimeUnit.SECONDS);
    conf.setTimeDuration(ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME.varname, 100, TimeUnit.MILLISECONDS);
    conf.setInt(ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES.varname, 1);
  }

  @Test(timeout = 60000)
  public void testActivePassive() throws Exception {
    Map<String, String> confOverlay = new HashMap<>();
    hiveConf1.set(ZkRegistryBase.UNIQUE_IDENTIFIER, UUID.randomUUID().toString());
    miniHS2_1.start(confOverlay);
    while(!miniHS2_1.isStarted()) {
      Thread.sleep(100);
    }

    hiveConf2.set(ZkRegistryBase.UNIQUE_IDENTIFIER, UUID.randomUUID().toString());
    miniHS2_2.start(confOverlay);
    while(!miniHS2_2.isStarted()) {
      Thread.sleep(100);
    }

    assertEquals(true, miniHS2_1.isLeader());
    String url = "http://localhost:" + hiveConf1.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
    assertEquals("true", sendGet(url));

    assertEquals(false, miniHS2_2.isLeader());
    url = "http://localhost:" + hiveConf2.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
    assertEquals("false", sendGet(url));

    url = "http://localhost:" + hiveConf1.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/peers";
    String resp = sendGet(url);
    ObjectMapper objectMapper = new ObjectMapper();
    HS2Peers.HS2Instances hs2Peers = objectMapper.readValue(resp, HS2Peers.HS2Instances.class);
    int port1 = Integer.parseInt(hiveConf1.get(ConfVars.HIVE_SERVER2_THRIFT_PORT.varname));
    assertEquals(2, hs2Peers.getHiveServer2Instances().size());
    for (HiveServer2Instance hsi : hs2Peers.getHiveServer2Instances()) {
      if (hsi.getRpcPort() == port1) {
        assertEquals(true, hsi.isLeader());
      } else {
        assertEquals(false, hsi.isLeader());
      }
    }

    Configuration conf = new Configuration();
    setHAConfigs(conf);
    HS2ActivePassiveHARegistry client = HS2ActivePassiveHARegistryClient.getClient(conf);
    List<HiveServer2Instance> hs2Instances = new ArrayList<>(client.getAll());
    assertEquals(2, hs2Instances.size());
    List<HiveServer2Instance> leaders = new ArrayList<>();
    List<HiveServer2Instance> standby = new ArrayList<>();
    for (HiveServer2Instance instance : hs2Instances) {
      if (instance.isLeader()) {
        leaders.add(instance);
      } else {
        standby.add(instance);
      }
    }
    assertEquals(1, leaders.size());
    assertEquals(1, standby.size());

    miniHS2_1.stop();

    while(!miniHS2_2.isStarted()) {
      Thread.sleep(100);
    }
    assertEquals(true, miniHS2_2.isLeader());
    url = "http://localhost:" + hiveConf2.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
    assertEquals("true", sendGet(url));

    while (client.getAll().size() != 1) {
      Thread.sleep(100);
    }

    client = HS2ActivePassiveHARegistryClient.getClient(conf);
    hs2Instances = new ArrayList<>(client.getAll());
    assertEquals(1, hs2Instances.size());
    leaders = new ArrayList<>();
    standby = new ArrayList<>();
    for (HiveServer2Instance instance : hs2Instances) {
      if (instance.isLeader()) {
        leaders.add(instance);
      } else {
        standby.add(instance);
      }
    }
    assertEquals(1, leaders.size());
    assertEquals(0, standby.size());

    url = "http://localhost:" + hiveConf2.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/peers";
    resp = sendGet(url);
    objectMapper = new ObjectMapper();
    hs2Peers = objectMapper.readValue(resp, HS2Peers.HS2Instances.class);
    int port2 = Integer.parseInt(hiveConf2.get(ConfVars.HIVE_SERVER2_THRIFT_PORT.varname));
    assertEquals(1, hs2Peers.getHiveServer2Instances().size());
    for (HiveServer2Instance hsi : hs2Peers.getHiveServer2Instances()) {
      if (hsi.getRpcPort() == port2) {
        assertEquals(true, hsi.isLeader());
      } else {
        assertEquals(false, hsi.isLeader());
      }
    }

    // start 1st server again
    hiveConf1.set(ZkRegistryBase.UNIQUE_IDENTIFIER, UUID.randomUUID().toString());
    miniHS2_1.start(confOverlay);

    while(!miniHS2_1.isStarted()) {
      Thread.sleep(100);
    }
    assertEquals(false, miniHS2_1.isLeader());
    url = "http://localhost:" + hiveConf1.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
    assertEquals("false", sendGet(url));

    while (client.getAll().size() != 2) {
      Thread.sleep(100);
    }

    client = HS2ActivePassiveHARegistryClient.getClient(conf);
    hs2Instances = new ArrayList<>(client.getAll());
    assertEquals(2, hs2Instances.size());
    leaders = new ArrayList<>();
    standby = new ArrayList<>();
    for (HiveServer2Instance instance : hs2Instances) {
      if (instance.isLeader()) {
        leaders.add(instance);
      } else {
        standby.add(instance);
      }
    }
    assertEquals(1, leaders.size());
    assertEquals(1, standby.size());

    url = "http://localhost:" + hiveConf1.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/peers";
    resp = sendGet(url);
    objectMapper = new ObjectMapper();
    hs2Peers = objectMapper.readValue(resp, HS2Peers.HS2Instances.class);
    port2 = Integer.parseInt(hiveConf2.get(ConfVars.HIVE_SERVER2_THRIFT_PORT.varname));
    assertEquals(2, hs2Peers.getHiveServer2Instances().size());
    for (HiveServer2Instance hsi : hs2Peers.getHiveServer2Instances()) {
      if (hsi.getRpcPort() == port2) {
        assertEquals(true, hsi.isLeader());
      } else {
        assertEquals(false, hsi.isLeader());
      }
    }
  }

  private String sendGet(String url) throws Exception {
    URL obj = new URL(url);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("GET");
    BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
    String inputLine;
    StringBuilder response = new StringBuilder();
    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }
    in.close();
    return response.toString();
  }
}