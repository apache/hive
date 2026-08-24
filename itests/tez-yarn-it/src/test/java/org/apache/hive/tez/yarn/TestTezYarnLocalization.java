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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.tez.yarn;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestTezYarnLocalization {

  private static final Logger LOG = LoggerFactory.getLogger(TestTezYarnLocalization.class);

  private static final String HDFS_BASE      = "hdfs://namenode:8020";
  private static final String HDFS_WAREHOUSE = HDFS_BASE + "/tmp/hive-tez-loc/warehouse";
  private static final String HDFS_SCRATCH   = HDFS_BASE + "/tmp/hive-tez-loc/scratch";
  private static final String HDFS_ROOT      = "/tmp/hive-tez-loc";

  private static TezYarnClusterContainer cluster;
  private static HiveServer2 hs2;
  private static int hs2Port;

  @BeforeClass
  public static void startAll() throws Exception {
    cluster = new TezYarnClusterContainer(true);
    cluster.start();

    GenericContainer<?> nn = cluster.namenodeContainer();
    var r = nn.execInContainer("hdfs", "dfs", "-mkdir", "-p", "/tmp");
    Assert.assertEquals("hdfs dfs -mkdir -p /tmp failed:\n" + r.getStderr(), 0, r.getExitCode());
    r = nn.execInContainer("hdfs", "dfs", "-chmod", "-R", "777", "/tmp");
    Assert.assertEquals("hdfs dfs -chmod -R 777 /tmp failed:\n" + r.getStderr(), 0, r.getExitCode());

    nn.execInContainer("hdfs", "dfs", "-mkdir", "-p", HDFS_ROOT + "/warehouse");
    nn.execInContainer("hdfs", "dfs", "-mkdir", "-p", HDFS_ROOT + "/scratch");
    nn.execInContainer("hdfs", "dfs", "-mkdir", "-p", HDFS_ROOT + "/user-install");
    nn.execInContainer("hdfs", "dfs", "-chmod", "-R", "777", HDFS_ROOT);

    String tezLibUris = cluster.uploadTezLibsToHdfs();
    LOG.info("Staged Tez libs to HDFS: {}", tezLibUris);

    Path localScratch = Files.createDirectories(
            Path.of("/tmp", "hive-tez-loc-" + System.currentTimeMillis()));
    HiveConf conf = buildHiveConf(tezLibUris, localScratch);

    hs2 = new HiveServer2();
    hs2.init(conf);
    hs2.start();

    waitForJdbc(hs2Port);
    LOG.info("HiveServer2 is ready on port {}", hs2Port);
  }

  @AfterClass
  public static void stopAll() {
    dumpNodeManagerDiagnostics();
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
  public void testQuerySucceedsWithAppJar() throws Exception {
    String url = jdbcUrl(hs2Port);
    try (Connection conn = DriverManager.getConnection(url, "hive", "")) {
      try (Statement stmt = conn.createStatement()) {

        stmt.execute("CREATE TABLE IF NOT EXISTS tez_loc_test (id INT) STORED AS ORC");
        stmt.execute("CREATE TABLE IF NOT EXISTS tez_source (id INT) STORED AS ORC");

        stmt.execute("INSERT INTO tez_loc_test SELECT count(*) FROM tez_source");

        try (ResultSet rs = stmt.executeQuery("SELECT id FROM tez_loc_test")) {
          Assert.assertTrue("Result set must contain at least one row", rs.next());
          long count = rs.getLong(1);
          Assert.assertEquals(
              "INSERT SELECT count(*) FROM empty tez_source should return 0 (hive-exec.jar was localized)",
              0L, count);
          LOG.info("Tez query succeeded: inserted count(*) = {}", count);
        }
      }
    }

    verifyTezYarnAppExists();
  }

  /** Prints Tez AM and NodeManager logs to the Surefire *-output.txt file at teardown. */
  private static void dumpNodeManagerDiagnostics() {
    if (cluster == null) {
      return;
    }
    System.out.println("########## BEGIN NodeManager diagnostics ##########");
    try {
      dumpNmCommand("launch_container.sh (AM launch command + classpath)",
          "find /tmp -name 'launch_container.sh' 2>/dev/null | head -3 "
          + "| xargs -I{} sh -c 'echo \"--- {} ---\"; cat {}' 2>/dev/null || true");

      dumpNmCommand("container syslog (Tez AM log4j output)",
          "find /var/log/hadoop/userlogs -name 'syslog*' 2>/dev/null | head -10 "
          + "| xargs -I{} sh -c 'echo \"--- {} ---\"; cat {}' 2>/dev/null || true");

      dumpNmCommand("container stdout + stderr + prelaunch.err",
          "find /var/log/hadoop/userlogs \\( -name 'stdout' -o -name 'stderr' -o -name 'prelaunch.err' \\) "
          + "2>/dev/null | head -20 | xargs -I{} sh -c 'echo \"--- {} ---\"; cat {}' 2>/dev/null || true");

      dumpNmCommand("NodeManager daemon log (tail 200)",
          "find /var/log/hadoop -maxdepth 1 -name '*.log' 2>/dev/null | head -3 "
          + "| xargs -I{} sh -c 'echo \"--- {} ---\"; tail -200 {}' 2>/dev/null || true");
    } catch (Exception e) {
      System.out.println("Could not dump NodeManager diagnostics: " + e);
    }
    System.out.println("########## END NodeManager diagnostics ##########");
    System.out.flush();
  }

  private static void dumpNmCommand(String label, String bashCommand) {
    try {
      GenericContainer.ExecResult r =
          cluster.nodeManagerContainer().execInContainer("bash", "-c", bashCommand);
      String out = r.getStdout();
      System.out.println("===== NM: " + label + " =====");
      System.out.println(out.isEmpty() ? "(no output found)" : out);
    } catch (Exception e) {
      System.out.println("===== NM: " + label + " (dump failed: " + e + ") =====");
    }
  }

  private static HiveConf buildHiveConf(String tezLibUris, Path localScratch) throws Exception {
    HiveConf conf = new HiveConf();

    URL hiveSite = TestTezYarnLocalization.class.getClassLoader().getResource("hive-site-yarn-it.xml");
    URL yarnSite = TestTezYarnLocalization.class.getClassLoader().getResource("yarn-site.xml");
    if (hiveSite != null) { conf.addResource(hiveSite); }
    if (yarnSite  != null) { conf.addResource(yarnSite); }

    conf.set("fs.defaultFS", HDFS_BASE);
    conf.setBoolean("dfs.client.use.datanode.hostname", true);
    conf.set("hive.metastore.warehouse.dir", HDFS_WAREHOUSE);
    conf.set(HiveConf.ConfVars.SCRATCH_DIR.varname, HDFS_SCRATCH);
    conf.set(HiveConf.ConfVars.LOCAL_SCRATCH_DIR.varname, localScratch.toAbsolutePath().toString());
    conf.setVar(HiveConf.ConfVars.HIVE_USER_INSTALL_DIR, HDFS_ROOT + "/user-install");

    conf.set("javax.jdo.option.ConnectionURL",
        "jdbc:derby:" + localScratch.resolve("metastore_db").toAbsolutePath() + ";create=true");

    conf.setBoolVar(HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL, false);
    conf.set("hive.stats.autogather", "false");
    conf.set("hive.stats.column.autogather", "false");
    conf.set("yarn.resourcemanager.hostname",       "resourcemanager");
    conf.set("yarn.resourcemanager.address",        "resourcemanager:8032");
    conf.set("yarn.resourcemanager.webapp.address", "resourcemanager:8088");

    conf.set("tez.lib.uris", tezLibUris);
    conf.setBoolean("tez.use.cluster.hadoop-libs", true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS, false);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_SESSIONS_PER_DEFAULT_QUEUE, 0);

    conf.set("tez.am.client.am.port-range",
        TezYarnClusterContainer.AM_CLIENT_PORT + "-" + TezYarnClusterContainer.AM_CLIENT_PORT);

    String containerEnv = "JAVA_HOME=" + TezYarnClusterContainer.CONTAINER_JAVA_21_HOME
        + ",HADOOP_HOME=/opt/hadoop"
        + ",HADOOP_MAPRED_HOME=/opt/hadoop";
    conf.set("tez.am.launch.env", containerEnv);
    conf.set("tez.task.launch.env", containerEnv);

    hs2Port = findFreePort();
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, hs2Port);
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, "localhost");
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT, findFreePort());
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE, "binary");
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION, "NOSASL");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);

    return conf;
  }

  private static void verifyTezYarnAppExists() {
    try {
      GenericContainer<?> rm = cluster.resourceManagerContainer();
      GenericContainer.ExecResult result = rm.execInContainer(
          "yarn", "application", "-list", "-appTypes", "TEZ", "-appStates", "ALL");
      String out = result.getStdout();
      LOG.info("YARN application list (TEZ, ALL states):\n{}", out);

      Pattern appIdPattern = Pattern.compile("(application_\\d+_\\d+)");
      Matcher matcher = appIdPattern.matcher(out);
      boolean found = false;
      while (matcher.find()) {
        LOG.info("Found Tez YARN application: {}", matcher.group(1));
        found = true;
      }

      Assert.assertTrue(
          "At least one Tez YARN application must be visible in the ResourceManager after running a Tez query",
          found);

    } catch (Exception e) {
      LOG.warn("Could not verify Tez YARN application existence via RM exec; "
          + "primary query-result assertion already passed. Cause: {}", e.getMessage());
    }
  }

  private static void waitForJdbc(int port) throws InterruptedException {
    String url = jdbcUrl(port);
    long deadline = System.currentTimeMillis() + 120_000;
    while (System.currentTimeMillis() < deadline) {
      try (Connection c = DriverManager.getConnection(url, "hive", "")) {
        return;
      } catch (Exception ignored) {
        Thread.sleep(2000);
      }
    }
    throw new IllegalStateException(
        "HiveServer2 JDBC endpoint not reachable on port " + port + " after 120s");
  }

  private static String jdbcUrl(int port) {
    return "jdbc:hive2://localhost:" + port + "/default;auth=noSasl";
  }

  private static int findFreePort() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      s.setReuseAddress(true);
      return s.getLocalPort();
    }
  }
}
