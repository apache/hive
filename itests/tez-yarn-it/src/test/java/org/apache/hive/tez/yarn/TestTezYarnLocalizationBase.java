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
package org.apache.hive.tez.yarn;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;

public class TestTezYarnLocalizationBase {

  protected static TezYarnClusterContainer cluster;
  protected static HiveServer2 hs2;
  protected static int hs2Port;
  protected static String hdfsUri;

  @BeforeClass
  public static void startAll() throws Exception {
    cluster = new TezYarnClusterContainer();
    cluster.start();

    hdfsUri = cluster.getHdfsUri();

    cluster.namenodeContainer().execInContainer("hdfs", "dfs", "-mkdir", "-p", "/tmp/hive-29483/warehouse");
    cluster.namenodeContainer().execInContainer("hdfs", "dfs", "-mkdir", "-p", "/tmp/hive-29483/scratch");
    cluster.namenodeContainer().execInContainer("hdfs", "dfs", "-chmod", "-R", "777", "/tmp/hive-29483");

    Path localScratch = Files.createTempDirectory("hive-29483-local-");

    HiveConf conf = new HiveConf();
    URL hiveSite = TestTezYarnLocalizationBase.class.getClassLoader().getResource("hive-site-yarn-it.xml");
    URL yarnSite = TestTezYarnLocalizationBase.class.getClassLoader().getResource("yarn-site.xml");
    if (hiveSite != null) conf.addResource(hiveSite);
    if (yarnSite != null) conf.addResource(yarnSite);

    conf.set("fs.defaultFS", hdfsUri);
    conf.set("hive.metastore.warehouse.dir", hdfsUri + "/tmp/hive-29483/warehouse");
    conf.set(HiveConf.ConfVars.SCRATCH_DIR.varname, hdfsUri + "/tmp/hive-29483/scratch");
    conf.set(HiveConf.ConfVars.LOCAL_SCRATCH_DIR.varname, localScratch.toAbsolutePath().toString());

    conf.set("javax.jdo.option.ConnectionURL",
        "jdbc:derby:" + localScratch.resolve("metastore_db").toAbsolutePath() + ";create=true");

    conf.set("yarn.resourcemanager.address", cluster.getResourceManagerAddress());
    conf.set("yarn.resourcemanager.webapp.address", cluster.getResourceManagerWebAppAddress());

    hs2Port = findFreePort();
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, hs2Port);
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, "localhost");
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT, findFreePort());
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE, "binary");
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION, "NOSASL");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);

    hs2 = new HiveServer2();
    hs2.init(conf);
    hs2.start();

    waitForJdbc(hs2Port);
  }

  @AfterClass
  public static void stopAll() {
    if (hs2 != null) {
      hs2.stop();
      hs2 = null;
    }
    if (cluster != null) {
      cluster.stop();
      cluster = null;
    }
  }

  @Test
  public void testJdbcSessionOpen() throws Exception {
    String url = jdbcUrl(hs2Port);
    try (Connection conn = DriverManager.getConnection(url, "hive", "")) {
      Assert.assertNotNull("JDBC connection must not be null", conn);
    }
  }

  private static void waitForJdbc(int port) throws InterruptedException {
    String url = jdbcUrl(port);
    long deadline = System.currentTimeMillis() + 60_000;
    while (System.currentTimeMillis() < deadline) {
      try (Connection c = DriverManager.getConnection(url, "hive", "")) {
        return;
      } catch (Exception ignored) {
        Thread.sleep(2000);
      }
    }
    throw new IllegalStateException("HiveServer2 JDBC endpoint not reachable on port " + port + " after 60s");
  }

  private static String jdbcUrl(int port) {
    // HS2 is configured with HIVE_SERVER2_AUTHENTICATION=NOSASL, which requires the client to
    // set auth=noSasl explicitly.
    return "jdbc:hive2://localhost:" + port + "/default;auth=noSasl";
  }

  private static int findFreePort() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      s.setReuseAddress(true);
      return s.getLocalPort();
    }
  }
}
