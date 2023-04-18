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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.registry.impl.ZkRegistryBase;
import org.apache.hive.http.security.PamAuthenticator;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.server.HS2ActivePassiveHARegistry;
import org.apache.hive.service.server.HS2ActivePassiveHARegistryClient;
import org.apache.hive.service.server.HiveServer2Instance;
import org.apache.hive.service.server.TestHS2HttpServerPam;
import org.apache.hive.service.servlet.HS2Peers;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.StatusLine;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.util.B64Code;
import org.eclipse.jetty.util.StringUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;



public class TestActivePassiveHA {
  private MiniHS2 miniHS2_1 = null;
  private MiniHS2 miniHS2_2 = null;
  private static TestingServer zkServer;
  private Connection hs2Conn = null;
  private static String ADMIN_USER = "user1"; // user from TestPamAuthenticator
  private static String ADMIN_PASSWORD = "1";
  private static String serviceDiscoveryMode = "zooKeeperHA";
  private static String zkHANamespace = "hs2ActivePassiveHATest";
  private HiveConf hiveConf1;
  private HiveConf hiveConf2;
  private static Path kvDataFilePath;

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
    final String dataFileDir = hiveConf1.get("test.data.files").replace('\\', '/').replace("c:", "");
    kvDataFilePath = new Path(dataFileDir, "kv1.txt");
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
    conf.setBoolean(ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_ENABLE.varname, true);
    conf.set(ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_REGISTRY_NAMESPACE.varname, zkHANamespace);
    conf.setTimeDuration(ConfVars.HIVE_ZOOKEEPER_CONNECTION_TIMEOUT.varname, 2, TimeUnit.SECONDS);
    conf.setTimeDuration(ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME.varname, 100, TimeUnit.MILLISECONDS);
    conf.setInt(ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES.varname, 1);
  }

  @Test(timeout = 60000)
  public void testActivePassiveHA() throws Exception {
    String instanceId1 = UUID.randomUUID().toString();
    miniHS2_1.start(getConfOverlay(instanceId1));
    String instanceId2 = UUID.randomUUID().toString();
    miniHS2_2.start(getConfOverlay(instanceId2));

    assertEquals(true, miniHS2_1.getIsLeaderTestFuture().get());
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
      if (hsi.getRpcPort() == port1 && hsi.getWorkerIdentity().equals(instanceId1)) {
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

    assertEquals(true, miniHS2_2.getIsLeaderTestFuture().get());
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
      if (hsi.getRpcPort() == port2 && hsi.getWorkerIdentity().equals(instanceId2)) {
        assertEquals(true, hsi.isLeader());
      } else {
        assertEquals(false, hsi.isLeader());
      }
    }

    // start 1st server again
    instanceId1 = UUID.randomUUID().toString();
    miniHS2_1.start(getConfOverlay(instanceId1));

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
      if (hsi.getRpcPort() == port2 && hsi.getWorkerIdentity().equals(instanceId2)) {
        assertEquals(true, hsi.isLeader());
      } else {
        assertEquals(false, hsi.isLeader());
      }
    }
  }

  @Test(timeout = 60000)
  public void testConnectionActivePassiveHAServiceDiscovery() throws Exception {
    String instanceId1 = UUID.randomUUID().toString();
    miniHS2_1.start(getConfOverlay(instanceId1));
    String instanceId2 = UUID.randomUUID().toString();
    Map<String, String> confOverlay = getConfOverlay(instanceId2);
    confOverlay.put(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, "http");
    confOverlay.put(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname, "clidriverTest");
    miniHS2_2.start(confOverlay);

    assertEquals(true, miniHS2_1.getIsLeaderTestFuture().get());
    assertEquals(true, miniHS2_1.isLeader());
    String url = "http://localhost:" + hiveConf1.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
    assertEquals("true", sendGet(url));

    assertEquals(false, miniHS2_2.isLeader());
    url = "http://localhost:" + hiveConf2.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
    assertEquals("false", sendGet(url));

    // miniHS2_1 will be leader
    String zkConnectString = zkServer.getConnectString();
    String zkJdbcUrl = miniHS2_1.getJdbcURL();
    // getAllUrls will parse zkJdbcUrl and will plugin the active HS2's host:port
    String parsedUrl = HiveConnection.getAllUrls(zkJdbcUrl).get(0).getJdbcUriString();
    String hs2_1_directUrl = "jdbc:hive2://" + miniHS2_1.getHost() + ":" + miniHS2_1.getBinaryPort() +
      "/default;serviceDiscoveryMode=" + serviceDiscoveryMode + ";zooKeeperNamespace=" + zkHANamespace + ";";
    assertTrue(zkJdbcUrl.contains(zkConnectString));
    assertEquals(hs2_1_directUrl, parsedUrl);
    openConnectionAndRunQuery(zkJdbcUrl);

    // miniHS2_2 will become leader
    miniHS2_1.stop();
    assertEquals(true, miniHS2_2.getIsLeaderTestFuture().get());
    parsedUrl = HiveConnection.getAllUrls(zkJdbcUrl).get(0).getJdbcUriString();
    String hs2_2_directUrl = "jdbc:hive2://" + miniHS2_2.getHost() + ":" + miniHS2_2.getHttpPort() +
      "/default;serviceDiscoveryMode=" + serviceDiscoveryMode + ";zooKeeperNamespace=" + zkHANamespace + ";";
    assertTrue(zkJdbcUrl.contains(zkConnectString));
    assertEquals(hs2_2_directUrl, parsedUrl);
    openConnectionAndRunQuery(zkJdbcUrl);

    // miniHS2_2 will continue to be leader
    instanceId1 = UUID.randomUUID().toString();
    miniHS2_1.start(getConfOverlay(instanceId1));
    parsedUrl = HiveConnection.getAllUrls(zkJdbcUrl).get(0).getJdbcUriString();
    assertTrue(zkJdbcUrl.contains(zkConnectString));
    assertEquals(hs2_2_directUrl, parsedUrl);
    openConnectionAndRunQuery(zkJdbcUrl);

    // miniHS2_1 will become leader
    miniHS2_2.stop();
    assertEquals(true, miniHS2_1.getIsLeaderTestFuture().get());
    parsedUrl = HiveConnection.getAllUrls(zkJdbcUrl).get(0).getJdbcUriString();
    hs2_1_directUrl = "jdbc:hive2://" + miniHS2_1.getHost() + ":" + miniHS2_1.getBinaryPort() +
      "/default;serviceDiscoveryMode=" + serviceDiscoveryMode + ";zooKeeperNamespace=" + zkHANamespace + ";";
    assertTrue(zkJdbcUrl.contains(zkConnectString));
    assertEquals(hs2_1_directUrl, parsedUrl);
    openConnectionAndRunQuery(zkJdbcUrl);
  }

  @Test(timeout = 60000)
  public void testManualFailover() throws Exception {
    hiveConf1.setBoolVar(ConfVars.HIVE_SERVER2_WEBUI_ENABLE_CORS, true);
    hiveConf2.setBoolVar(ConfVars.HIVE_SERVER2_WEBUI_ENABLE_CORS, true);
    setPamConfs(hiveConf1);
    setPamConfs(hiveConf2);
    PamAuthenticator pamAuthenticator1 = new TestHS2HttpServerPam.TestPamAuthenticator(hiveConf1);
    PamAuthenticator pamAuthenticator2 = new TestHS2HttpServerPam.TestPamAuthenticator(hiveConf2);
    try {
      String instanceId1 = UUID.randomUUID().toString();
      miniHS2_1.setPamAuthenticator(pamAuthenticator1);
      miniHS2_1.start(getSecureConfOverlay(instanceId1));
      String instanceId2 = UUID.randomUUID().toString();
      Map<String, String> confOverlay = getSecureConfOverlay(instanceId2);
      confOverlay.put(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, "http");
      confOverlay.put(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname, "clidriverTest");
      miniHS2_2.setPamAuthenticator(pamAuthenticator2);
      miniHS2_2.start(confOverlay);
      String url1 = "http://localhost:" + hiveConf1.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
      String url2 = "http://localhost:" + hiveConf2.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";

      // when we start miniHS2_1 will be leader (sequential start)
      assertEquals(true, miniHS2_1.getIsLeaderTestFuture().get());
      assertEquals(true, miniHS2_1.isLeader());
      assertEquals("true", sendGet(url1, true, true));

      // trigger failover on miniHS2_1
      String resp = sendDelete(url1, true, true);
      assertTrue(resp.contains("Failover successful!"));

      // make sure miniHS2_1 is not leader
      assertEquals(true, miniHS2_1.getNotLeaderTestFuture().get());
      assertEquals(false, miniHS2_1.isLeader());
      assertEquals("false", sendGet(url1, true, true));

      // make sure miniHS2_2 is the new leader
      assertEquals(true, miniHS2_2.getIsLeaderTestFuture().get());
      assertEquals(true, miniHS2_2.isLeader());
      assertEquals("true", sendGet(url2, true, true));

      // send failover request again to miniHS2_1 and get a failure
      resp = sendDelete(url1, true, true);
      assertTrue(resp.contains("Cannot failover an instance that is not a leader"));
      assertEquals(true, miniHS2_1.getNotLeaderTestFuture().get());
      assertEquals(false, miniHS2_1.isLeader());

      // send failover request to miniHS2_2 and make sure miniHS2_1 takes over (returning back to leader, test listeners)
      resp = sendDelete(url2, true, true);
      assertTrue(resp.contains("Failover successful!"));
      assertEquals(true, miniHS2_1.getIsLeaderTestFuture().get());
      assertEquals(true, miniHS2_1.isLeader());
      assertEquals("true", sendGet(url1, true, true));
      assertEquals(true, miniHS2_2.getNotLeaderTestFuture().get());
      assertEquals("false", sendGet(url2, true, true));
      assertEquals(false, miniHS2_2.isLeader());
    } finally {
      resetFailoverConfs();
    }
  }

  @Test(timeout = 60000)
  public void testManualFailoverUnauthorized() throws Exception {
    setPamConfs(hiveConf1);
    PamAuthenticator pamAuthenticator1 = new TestHS2HttpServerPam.TestPamAuthenticator(hiveConf1);
    try {
      String instanceId1 = UUID.randomUUID().toString();
      miniHS2_1.setPamAuthenticator(pamAuthenticator1);
      miniHS2_1.start(getSecureConfOverlay(instanceId1));

      // dummy HS2 instance just to trigger failover
      String instanceId2 = UUID.randomUUID().toString();
      Map<String, String> confOverlay = getSecureConfOverlay(instanceId2);
      confOverlay.put(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, "http");
      confOverlay.put(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname, "clidriverTest");
      miniHS2_2.start(confOverlay);

      String url1 = "http://localhost:" + hiveConf1.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
      // when we start miniHS2_1 will be leader (sequential start)
      assertEquals(true, miniHS2_1.getIsLeaderTestFuture().get());
      assertEquals(true, miniHS2_1.isLeader());
      assertEquals("true", sendGet(url1, true));

      // trigger failover on miniHS2_1 without authorization header
      assertTrue(sendDelete(url1, false).contains("Unauthorized"));
      assertTrue(sendDelete(url1, true).contains("Failover successful!"));
      assertEquals(true, miniHS2_1.getNotLeaderTestFuture().get());
      assertEquals(false, miniHS2_1.isLeader());
      assertEquals(true, miniHS2_2.getIsLeaderTestFuture().get());
      assertEquals(true, miniHS2_2.isLeader());
    } finally {
      // revert configs to not affect other tests
      unsetPamConfs(hiveConf1);
    }
  }

  @Test(timeout = 60000)
  public void testNoConnectionOnPassive() throws Exception {
    hiveConf1.setBoolVar(ConfVars.HIVE_SERVER2_WEBUI_ENABLE_CORS, true);
    hiveConf2.setBoolVar(ConfVars.HIVE_SERVER2_WEBUI_ENABLE_CORS, true);
    setPamConfs(hiveConf1);
    setPamConfs(hiveConf2);
    try {
      PamAuthenticator pamAuthenticator1 = new TestHS2HttpServerPam.TestPamAuthenticator(hiveConf1);
      PamAuthenticator pamAuthenticator2 = new TestHS2HttpServerPam.TestPamAuthenticator(hiveConf2);
      String instanceId1 = UUID.randomUUID().toString();
      miniHS2_1.setPamAuthenticator(pamAuthenticator1);
      miniHS2_1.start(getSecureConfOverlay(instanceId1));
      String instanceId2 = UUID.randomUUID().toString();
      Map<String, String> confOverlay = getSecureConfOverlay(instanceId2);
      miniHS2_2.setPamAuthenticator(pamAuthenticator2);
      miniHS2_2.start(confOverlay);
      String url1 = "http://localhost:" + hiveConf1.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
      assertEquals(true, miniHS2_1.getIsLeaderTestFuture().get());
      assertEquals(true, miniHS2_1.isLeader());

      // Don't get urls from ZK, it will actually be a service discovery URL that we don't want.
      String hs1Url = "jdbc:hive2://" + miniHS2_1.getHost() + ":" + miniHS2_1.getBinaryPort();
      Connection hs2Conn = getConnection(hs1Url, System.getProperty("user.name")); // Should work.
      hs2Conn.close();

      String resp = sendDelete(url1, true);
      assertTrue(resp, resp.contains("Failover successful!"));
      // wait for failover to close sessions
      while (miniHS2_1.getOpenSessionsCount() != 0) {
        Thread.sleep(100);
      }

      assertEquals(true, miniHS2_2.getIsLeaderTestFuture().get());
      assertEquals(true, miniHS2_2.isLeader());

      try {
        hs2Conn = getConnection(hs1Url, System.getProperty("user.name"));
        fail("Should throw");
      } catch (Exception e) {
        if (!e.getMessage().contains("Cannot open sessions on an inactive HS2")) {
          throw e;
        }
      }
    } finally {
      resetFailoverConfs();
    }
  }

  private void resetFailoverConfs() {
    // revert configs to not affect other tests
    unsetPamConfs(hiveConf1);
    unsetPamConfs(hiveConf2);
    hiveConf1.unset(ConfVars.HIVE_SERVER2_WEBUI_ENABLE_CORS.varname);
    hiveConf2.unset(ConfVars.HIVE_SERVER2_WEBUI_ENABLE_CORS.varname);
  }

  @Test(timeout = 60000)
  public void testClientConnectionsOnFailover() throws Exception {
    setPamConfs(hiveConf1);
    setPamConfs(hiveConf2);
    PamAuthenticator pamAuthenticator1 = new TestHS2HttpServerPam.TestPamAuthenticator(hiveConf1);
    PamAuthenticator pamAuthenticator2 = new TestHS2HttpServerPam.TestPamAuthenticator(hiveConf2);
    try {
      String instanceId1 = UUID.randomUUID().toString();
      miniHS2_1.setPamAuthenticator(pamAuthenticator1);
      miniHS2_1.start(getSecureConfOverlay(instanceId1));
      String instanceId2 = UUID.randomUUID().toString();
      Map<String, String> confOverlay = getSecureConfOverlay(instanceId2);
      confOverlay.put(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, "http");
      confOverlay.put(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname, "clidriverTest");
      miniHS2_2.setPamAuthenticator(pamAuthenticator2);
      miniHS2_2.start(confOverlay);
      String url1 = "http://localhost:" + hiveConf1.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
      String url2 = "http://localhost:" + hiveConf2.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
      String zkJdbcUrl = miniHS2_1.getJdbcURL();
      String zkConnectString = zkServer.getConnectString();
      assertTrue(zkJdbcUrl.contains(zkConnectString));

      // when we start miniHS2_1 will be leader (sequential start)
      assertEquals(true, miniHS2_1.getIsLeaderTestFuture().get());
      assertEquals(true, miniHS2_1.isLeader());
      assertEquals("true", sendGet(url1, true));

      // before failover, check if we are getting connection from miniHS2_1
      String hs2_1_directUrl = "jdbc:hive2://" + miniHS2_1.getHost() + ":" + miniHS2_1.getBinaryPort() +
        "/default;serviceDiscoveryMode=" + serviceDiscoveryMode + ";zooKeeperNamespace=" + zkHANamespace + ";";
      String parsedUrl = HiveConnection.getAllUrls(zkJdbcUrl).get(0).getJdbcUriString();
      assertEquals(hs2_1_directUrl, parsedUrl);
      hs2Conn = getConnection(zkJdbcUrl, System.getProperty("user.name"));
      while (miniHS2_1.getOpenSessionsCount() != 1) {
        Thread.sleep(100);
      }

      // trigger failover on miniHS2_1 and make sure the connections are closed
      String resp = sendDelete(url1, true);
      assertTrue(resp.contains("Failover successful!"));
      // wait for failover to close sessions
      while (miniHS2_1.getOpenSessionsCount() != 0) {
        Thread.sleep(100);
      }

      // make sure miniHS2_1 is not leader
      assertEquals(true, miniHS2_1.getNotLeaderTestFuture().get());
      assertEquals(false, miniHS2_1.isLeader());
      assertEquals("false", sendGet(url1, true));

      // make sure miniHS2_2 is the new leader
      assertEquals(true, miniHS2_2.getIsLeaderTestFuture().get());
      assertEquals(true, miniHS2_2.isLeader());
      assertEquals("true", sendGet(url2, true));

      // when we make a new connection we should get it from miniHS2_2 this time
      String hs2_2_directUrl = "jdbc:hive2://" + miniHS2_2.getHost() + ":" + miniHS2_2.getHttpPort() +
        "/default;serviceDiscoveryMode=" + serviceDiscoveryMode + ";zooKeeperNamespace=" + zkHANamespace + ";";
      parsedUrl = HiveConnection.getAllUrls(zkJdbcUrl).get(0).getJdbcUriString();
      assertEquals(hs2_2_directUrl, parsedUrl);
      hs2Conn = getConnection(zkJdbcUrl, System.getProperty("user.name"));
      while (miniHS2_2.getOpenSessionsCount() != 1) {
        Thread.sleep(100);
      }

      // send failover request again to miniHS2_1 and get a failure
      resp = sendDelete(url1, true);
      assertTrue(resp.contains("Cannot failover an instance that is not a leader"));
      assertEquals(true, miniHS2_1.getNotLeaderTestFuture().get());
      assertEquals(false, miniHS2_1.isLeader());

      // send failover request to miniHS2_2 and make sure miniHS2_1 takes over (returning back to leader, test listeners)
      resp = sendDelete(url2, true);
      assertTrue(resp.contains("Failover successful!"));
      assertEquals(true, miniHS2_1.getIsLeaderTestFuture().get());
      assertEquals(true, miniHS2_1.isLeader());
      assertEquals("true", sendGet(url1, true));
      assertEquals(true, miniHS2_2.getNotLeaderTestFuture().get());
      assertEquals("false", sendGet(url2, true));
      assertEquals(false, miniHS2_2.isLeader());
      // make sure miniHS2_2 closes all its connections
      while (miniHS2_2.getOpenSessionsCount() != 0) {
        Thread.sleep(100);
      }
      // new connections goes to miniHS2_1 now
      hs2Conn = getConnection(zkJdbcUrl, System.getProperty("user.name"));
      while (miniHS2_1.getOpenSessionsCount() != 1) {
        Thread.sleep(100);
      }
    } finally {
      // revert configs to not affect other tests
      unsetPamConfs(hiveConf1);
      unsetPamConfs(hiveConf2);
    }
  }

  private Connection getConnection(String jdbcURL, String user) throws SQLException {
    return DriverManager.getConnection(jdbcURL, user, "bar");
  }

  private void openConnectionAndRunQuery(String jdbcUrl) throws Exception {
    hs2Conn = getConnection(jdbcUrl, System.getProperty("user.name"));
    String tableName = "testTab1";
    Statement stmt = hs2Conn.createStatement();
    // create table
    stmt.execute("DROP TABLE IF EXISTS " + tableName);
    stmt.execute("CREATE TABLE " + tableName
      + " (under_col INT COMMENT 'the under column', value STRING) COMMENT ' test table'");
    // load data
    stmt.execute("load data local inpath '" + kvDataFilePath.toString() + "' into table "
      + tableName);
    ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
    assertTrue(res.next());
    assertEquals("val_238", res.getString(2));
    res.close();
    stmt.close();
  }

  private String sendGet(String url) throws Exception {
    return sendGet(url, false);
  }

  private String sendGet(String url, boolean enableAuth) throws Exception {
    return sendAuthMethod(new HttpGet(url), enableAuth, false);
  }

  private String sendGet(String url, boolean enableAuth, boolean enableCORS) throws Exception {
    return sendAuthMethod(new HttpGet(url), enableAuth, enableCORS);
  }

  private String sendDelete(String url, boolean enableAuth) throws Exception {
    return sendAuthMethod(new HttpDelete(url), enableAuth, false);
  }

  private String sendDelete(String url, boolean enableAuth, boolean enableCORS) throws Exception {
    return sendAuthMethod(new HttpDelete(url), enableAuth, enableCORS);
  }

  private String sendAuthMethod(HttpRequestBase method, boolean enableAuth, boolean enableCORS) throws Exception {
    CloseableHttpResponse httpResponse = null;
    String response = null;
    int statusCode = -1;

    try (
        CloseableHttpClient client = HttpClients.createDefault();
    ) {

      // CORS check
      if (enableCORS) {
        String origin = "http://example.com";

        HttpOptions optionsMethod = new HttpOptions(method.getURI().toString());

        optionsMethod.addHeader("Origin", origin);

        if (enableAuth) {
          setupAuthHeaders(optionsMethod);
        }

        httpResponse = client.execute(optionsMethod);

        if (httpResponse != null) {
          StatusLine statusLine = httpResponse.getStatusLine();
          if (statusLine != null) {
            response = httpResponse.getStatusLine().getReasonPhrase();
            statusCode = httpResponse.getStatusLine().getStatusCode();

            if (statusCode == 200) {
              Header originHeader = httpResponse.getFirstHeader("Access-Control-Allow-Origin");
              assertNotNull(originHeader);
              assertEquals(origin, originHeader.getValue());
            } else {
              fail("CORS returned: " + statusCode + " Error: " + response);
            }
          } else {
            fail("Status line is null");
          }
        } else {
          fail("No http Response");
        }
      }

      if (enableAuth) {
        setupAuthHeaders(method);
      }

      httpResponse = client.execute(method);

      if (httpResponse != null) {
        StatusLine statusLine = httpResponse.getStatusLine();
        if (statusLine != null) {
           response = httpResponse.getStatusLine().getReasonPhrase();
           statusCode = httpResponse.getStatusLine().getStatusCode();
          if (statusCode == 200) {
            return EntityUtils.toString(httpResponse.getEntity());
          }
        }
      }

        return response;
    } finally {
      httpResponse.close();
    }
  }

  private void setupAuthHeaders(final HttpRequestBase method) {
    String authB64Code =
        B64Code.encode(ADMIN_USER + ":" + ADMIN_PASSWORD, StringUtil.__ISO_8859_1);
    method.setHeader(HttpHeader.AUTHORIZATION.asString(), "Basic " + authB64Code);
  }

  private Map<String, String> getConfOverlay(final String instanceId) {
    Map<String, String> confOverlay = new HashMap<>();
    confOverlay.put("hive.server2.zookeeper.publish.configs", "true");
    confOverlay.put(ZkRegistryBase.UNIQUE_IDENTIFIER, instanceId);
    return confOverlay;
  }

  private Map<String, String> getSecureConfOverlay(final String instanceId) {
    Map<String, String> confOverlay = new HashMap<>();
    confOverlay.put("hive.server2.zookeeper.publish.configs", "true");
    confOverlay.put(ZkRegistryBase.UNIQUE_IDENTIFIER, instanceId);
    confOverlay.put("hadoop.security.instrumentation.requires.admin", "true");
    confOverlay.put("hadoop.security.authorization", "true");
    confOverlay.put(ConfVars.USERS_IN_ADMIN_ROLE.varname, ADMIN_USER);
    return confOverlay;
  }

  private void setPamConfs(final HiveConf hiveConf) {
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PAM_SERVICES, "sshd");
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_PAM, true);
    hiveConf.setBoolVar(ConfVars.HIVE_IN_TEST, true);
  }

  private void unsetPamConfs(final HiveConf hiveConf) {
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PAM_SERVICES, "");
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_PAM, false);
    hiveConf.setBoolVar(ConfVars.HIVE_IN_TEST, false);
  }

  // This is test for llap command AuthZ added in HIVE-19033 which require ZK access for it to pass
  @Test(timeout = 60000)
  public void testNoAuthZLlapClusterInfo() throws Exception {
    String instanceId1 = UUID.randomUUID().toString();
    miniHS2_1.start(getConfOverlay(instanceId1));
    Connection hs2Conn = getConnection(miniHS2_1.getJdbcURL(), "user1");
    boolean caughtException = false;
    Statement stmt = hs2Conn.createStatement();
    try {
      stmt.execute("set hive.llap.daemon.service.hosts=@localhost");
      stmt.execute("llap cluster -info");
    } catch (SQLException e) {
      caughtException = true;
    } finally {
      stmt.close();
      hs2Conn.close();
    }
    assertEquals(false, caughtException);
  }
}
