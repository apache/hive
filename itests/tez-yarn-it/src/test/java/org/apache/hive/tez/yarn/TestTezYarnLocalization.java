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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hive.service.server.HiveServer2;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

public class TestTezYarnLocalization {

  private static final Logger LOG = LoggerFactory.getLogger(TestTezYarnLocalization.class);

  private static TezYarnClusterContainer cluster;
  private static HiveServer2 hs2;
  private static int hs2Port;

  @BeforeClass
  public static void startAll() throws Exception {
    cluster = new TezYarnClusterContainer();
    cluster.start();

    TestTezYarnUtils.setupHdfs(cluster);

    String tezLibUris = cluster.uploadTezLibsToHdfs();
    LOG.info("Staged Tez libs to HDFS: {}", tezLibUris);

    Path localScratch = Files.createTempDirectory("hive-tez-loc-");
    hs2Port = TestTezYarnUtils.findFreePort();
    HiveConf conf = TestTezYarnUtils.buildHiveConf(tezLibUris, localScratch, hs2Port);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SESSION_SILENT, true);
    conf.setVar(HiveConf.ConfVars.PRE_EXEC_HOOKS, "");
    conf.setVar(HiveConf.ConfVars.POST_EXEC_HOOKS, "");

    hs2 = new HiveServer2();
    hs2.init(conf);
    hs2.start();

    TestTezYarnUtils.waitForJdbc(hs2Port);
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
    String url = TestTezYarnUtils.jdbcUrl(hs2Port);
    try (Connection conn = DriverManager.getConnection(url, "hive", "")) {
      try (Statement stmt = conn.createStatement()) {

        stmt.execute("CREATE TABLE IF NOT EXISTS tez_loc_test (id INT) STORED AS ORC");
        stmt.execute("INSERT INTO tez_loc_test VALUES (42)");

        try (ResultSet rs = stmt.executeQuery("SELECT id FROM tez_loc_test")) {
          Assert.assertTrue("Result set must contain at least one row", rs.next());
          int count = rs.getInt(1);
          Assert.assertEquals(
                  "INSERT VALUES should return the inserted row value (hive-exec.jar was localized)",
                  42, count);
          LOG.info("Tez query succeeded: inserted row value = {}", count);
        }
      }
    }

    verifyTezYarnAppExists();
    verifyHiveExecJarOnHdfs();
    verifyHiveExecJarLocalizedInNm();
  }

  private static void verifyHiveExecJarOnHdfs() throws IOException {
    List<String> matchingJars = new ArrayList<>();
    org.apache.hadoop.fs.Path searchRoot =
        new org.apache.hadoop.fs.Path(TestTezYarnUtils.HDFS_ROOT + "/user-install");
    try (FileSystem fs = cluster.newFileSystem()) {
      RemoteIterator<LocatedFileStatus> files = fs.listFiles(searchRoot, true);
      while (files.hasNext()) {
        LocatedFileStatus file = files.next();
        String fileName = file.getPath().getName();
        if (fileName.startsWith("hive-exec-") && fileName.endsWith(".jar")) {
          matchingJars.add(file.getPath().toString());
        }
      }
    }
    LOG.info("HDFS hive-exec.jar search in {}/user-install: {}",
        TestTezYarnUtils.HDFS_ROOT,
        matchingJars.isEmpty() ? "(none found)" : String.join("\n", matchingJars));
    Assert.assertFalse(
            "hive-exec.jar was not staged to HDFS under " + TestTezYarnUtils.HDFS_ROOT + "/user-install — "
                    + "TezSessionState.buildCommonLocalResources() localization step 1 appears to have failed.",
            matchingJars.isEmpty());
  }

  private static void verifyHiveExecJarLocalizedInNm() throws IOException, InterruptedException {
    Container.ExecResult r = cluster.nodeManagerContainer().execInContainer(
            "bash", "-c", "find /tmp -name 'hive-exec-*.jar' 2>/dev/null | head -5");
    LOG.info("NodeManager hive-exec.jar localization check: {}",
            r.getStdout().trim().isEmpty() ? "(none found)" : r.getStdout().trim());
    Assert.assertFalse(
            "hive-exec.jar was not found in the NodeManager container's local filesystem after Tez query — "
                    + "YARN localization step 2 appears to have failed.",
            r.getStdout().trim().isEmpty());
  }

  /**
   * Logs Tez AM and NodeManager diagnostics at teardown.
   */
  private static void dumpNodeManagerDiagnostics() {
    if (cluster == null) {
      return;
    }
    LOG.info("########## BEGIN NodeManager diagnostics ##########");
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
    } catch (Exception e) {
      LOG.warn("Could not dump NodeManager diagnostics", e);
    }
    LOG.info("########## END NodeManager diagnostics ##########");
  }

  private static void dumpNmCommand(String label, String bashCommand) {
    try {
      Container.ExecResult r =
              cluster.nodeManagerContainer().execInContainer("bash", "-c", bashCommand);
      String out = r.getStdout();
      LOG.info("===== NM: {} =====\n{}", label, out.isEmpty() ? "(no output found)" : out);
    } catch (Exception e) {
      LOG.warn("===== NM: {} (dump failed) =====", label, e);
    }
  }

  private static void verifyTezYarnAppExists() throws Exception {
    List<ApplicationReport> applications;
    try (YarnClient yarnClient = cluster.newYarnClient()) {
      applications = yarnClient.getApplications(
          Collections.singleton("TEZ"), EnumSet.allOf(YarnApplicationState.class));
    }
    if (applications.isEmpty()) {
      LOG.info("YARN application list (TEZ, ALL states): (none found)");
    } else {
      LOG.info("YARN application list (TEZ, ALL states):");
      for (ApplicationReport app : applications) {
        LOG.info("Found Tez YARN application: {} ({})",
            app.getApplicationId(), app.getYarnApplicationState());
      }
    }
    Assert.assertFalse(
            "At least one Tez YARN application must be visible in the ResourceManager after running a Tez query",
            applications.isEmpty());
  }
}
