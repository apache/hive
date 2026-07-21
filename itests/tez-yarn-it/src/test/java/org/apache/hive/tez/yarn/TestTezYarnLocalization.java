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

/**
 * End-to-end integration test for HIVE-29483: verifies that {@code hive-exec.jar}
 * is correctly localized by Tez on YARN.
 *
 * <p>The test starts a real Hadoop YARN+HDFS cluster in Docker containers
 * (fixed host ports so the same URIs work from both the host JVM and inside
 * YARN containers), stages Tez framework jars to HDFS, brings up an in-process
 * HiveServer2 pointed at that cluster, and then runs an actual Tez DAG via JDBC.
 *
 * <p>If {@code hive-exec.jar} were missing from {@code commonLocalResources}
 * (i.e. if {@code TezSessionState.appJarLr} were removed), the Tez task
 * containers would fail with {@code ClassNotFoundException} for Hive executor
 * classes and this test would fail.
 *
 * <h3>Prerequisites</h3>
 * <ul>
 *   <li>Docker must be running on the host.</li>
 *   <li>Host ports 8020, 9870, 8032, 8088 must be free.</li>
 *   <li>The test JVM must resolve {@code namenode} and {@code resourcemanager}
 *       to {@code 127.0.0.1}.  When run via Maven this is automatic because
 *       Surefire passes {@code -Djdk.net.hosts.file=custom_hosts_file}.</li>
 * </ul>
 *
 * <h3>Running locally</h3>
 * <pre>
 * mvn test -Pitests,tez-yarn -pl itests/tez-yarn-it \
 *     -Dtest=TestTezYarnLocalization#testQuerySucceedsWithAppJar
 * </pre>
 */
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
    nn.execInContainer("hdfs", "dfs", "-mkdir", "-p", HDFS_ROOT + "/warehouse");
    nn.execInContainer("hdfs", "dfs", "-mkdir", "-p", HDFS_ROOT + "/scratch");
    nn.execInContainer("hdfs", "dfs", "-mkdir", "-p", HDFS_ROOT + "/user-install");
    nn.execInContainer("hdfs", "dfs", "-chmod", "-R", "777", HDFS_ROOT);

    String tezLibUris = cluster.uploadTezLibsToHdfs();
    LOG.info("Staged Tez libs to HDFS: {}", tezLibUris);

    Path localScratch = Files.createTempDirectory("hive-tez-loc-");
    HiveConf conf = buildHiveConf(tezLibUris, localScratch);

    hs2 = new HiveServer2();
    hs2.init(conf);
    hs2.start();

    waitForJdbc(hs2Port);
    LOG.info("HiveServer2 is ready on port {}", hs2Port);
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

  /**
   * Positive test case for HIVE-29483.
   *
   * <p>Runs a Tez DAG via JDBC and asserts that:
   * <ol>
   *   <li>The query returns the expected result (primary proof that
   *       {@code hive-exec.jar} was localized and Hive executor classes
   *       were available inside YARN task containers).</li>
   *   <li>A Tez YARN application is visible in the ResourceManager
   *       (secondary confirmation that the DAG actually ran on YARN).</li>
   * </ol>
   */
  @Test
  public void testQuerySucceedsWithAppJar() throws Exception {
    String url = jdbcUrl(hs2Port);
    try (Connection conn = DriverManager.getConnection(url, "hive", "")) {
      try (Statement stmt = conn.createStatement()) {

        stmt.execute("CREATE TABLE IF NOT EXISTS tez_loc_test (id INT) STORED AS ORC");
        stmt.execute("INSERT INTO tez_loc_test VALUES (1), (2), (3)");

        // COUNT(*) forces a Tez reduce task. If hive-exec.jar is not localized the task
        // container fails with ClassNotFoundException before this assertion is reached.
        try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tez_loc_test")) {
          Assert.assertTrue("Result set must contain at least one row", rs.next());
          long count = rs.getLong(1);
          Assert.assertEquals(
              "SELECT count(*) FROM tez_loc_test should return 3 (hive-exec.jar was localized)",
              3L, count);
          LOG.info("Tez query succeeded: count(*) = {}", count);
        }
      }
    }

    verifyTezYarnAppExists();
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static HiveConf buildHiveConf(String tezLibUris, Path localScratch) throws Exception {
    HiveConf conf = new HiveConf();

    URL hiveSite = TestTezYarnLocalization.class.getClassLoader().getResource("hive-site-yarn-it.xml");
    URL yarnSite = TestTezYarnLocalization.class.getClassLoader().getResource("yarn-site.xml");
    if (hiveSite != null) { conf.addResource(hiveSite); }
    if (yarnSite  != null) { conf.addResource(yarnSite); }

    conf.set("fs.defaultFS", HDFS_BASE);
    // Force the HDFS client to use DataNode hostnames (resolvable via custom_hosts_file)
    // rather than docker-internal IPs that are unreachable from the host JVM.
    conf.setBoolean("dfs.client.use.datanode.hostname", true);
    conf.set("hive.metastore.warehouse.dir", HDFS_WAREHOUSE);
    conf.set(HiveConf.ConfVars.SCRATCH_DIR.varname, HDFS_SCRATCH);
    conf.set(HiveConf.ConfVars.LOCAL_SCRATCH_DIR.varname, localScratch.toAbsolutePath().toString());
    // hive-exec.jar is uploaded to this HDFS path by DagUtils when hive.jar.directory is unset.
    conf.setVar(HiveConf.ConfVars.HIVE_USER_INSTALL_DIR, HDFS_ROOT + "/user-install");

    conf.set("javax.jdo.option.ConnectionURL",
        "jdbc:derby:" + localScratch.resolve("metastore_db").toAbsolutePath() + ";create=true");

    conf.set("yarn.resourcemanager.hostname",       "resourcemanager");
    conf.set("yarn.resourcemanager.address",        "resourcemanager:8032");
    conf.set("yarn.resourcemanager.webapp.address", "resourcemanager:8088");

    // tez.lib.uris provides Tez framework jars to YARN containers. tez.use.cluster.hadoop-libs
    // tells Tez to source Hadoop jars from the cluster node (/opt/hadoop) rather than requiring
    // them in tez.lib.uris, which would require staging the full Hadoop distribution to HDFS.
    conf.set("tez.lib.uris", tezLibUris);
    conf.setBoolean("tez.use.cluster.hadoop-libs", true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS, false);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_SESSIONS_PER_DEFAULT_QUEUE, 0);

    // The Hadoop image daemons use Java 8 but hive-exec requires Java 21; point Tez AM/task
    // container environments at the Java 21 runtime added by the custom Dockerfile.
    // HADOOP_HOME must be set explicitly: the apache/hadoop image does not export it into YARN
    // container environments, so tez.use.cluster.hadoop-libs's "${HADOOP_HOME}/bin/hadoop classpath"
    // call would silently produce an empty classpath without it.
    String containerEnv = "JAVA_HOME=" + TezYarnClusterContainer.CONTAINER_JAVA_21_HOME
        + ",HADOOP_HOME=/opt/hadoop"
        + ",HADOOP_MAPRED_HOME=/opt/hadoop";
    conf.set("tez.am.launch.env", containerEnv);
    conf.set("tez.task.launch.env", containerEnv);
    conf.set("yarn.app.mapreduce.am.env", containerEnv);
    conf.set("mapreduce.map.env", containerEnv);
    conf.set("mapreduce.reduce.env", containerEnv);

    hs2Port = findFreePort();
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, hs2Port);
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, "localhost");
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT, findFreePort());
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE, "binary");
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION, "NOSASL");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);

    return conf;
  }

  /**
   * Checks that at least one Tez YARN application is registered with the
   * ResourceManager.  If the check cannot be performed (e.g. due to a
   * container exec error), a warning is logged and the test continues —
   * the primary assertion is the query result check in the calling test.
   */
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
