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
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;

import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;

/**
 * Shared HDFS paths, fixture setup, Hive configuration and JDBC helpers for the tez-yarn-it tests.
 */
public final class TestTezYarnUtils {

  static final String HDFS_BASE = "hdfs://namenode:8020";
  static final String HDFS_ROOT = "/tmp/hive-tez-loc";
  static final String HDFS_WAREHOUSE = HDFS_BASE + HDFS_ROOT + "/warehouse";
  static final String HDFS_SCRATCH = HDFS_BASE + HDFS_ROOT + "/scratch";

  private TestTezYarnUtils() {
  }

  /**
   * Creates the HDFS fixture directories from inside the NameNode container, which runs as the Hadoop superuser.
   * A host-side FileSystem client lacks write permission on "/".
   */
  static void setupHdfs(ContainerState nn) throws Exception {
    String[] dirs = {
        "/tmp",
        HDFS_ROOT + "/warehouse",
        HDFS_ROOT + "/scratch",
        HDFS_ROOT + "/user-install"
    };
    for (String dir : dirs) {
      Container.ExecResult r = nn.execInContainer("hdfs", "dfs", "-mkdir", "-p", dir);
      if (r.getExitCode() != 0) {
        throw new IllegalStateException("hdfs dfs -mkdir -p " + dir + " failed:\n" + r.getStderr());
      }
    }
    for (String dir : new String[]{"/tmp", HDFS_ROOT}) {
      Container.ExecResult r = nn.execInContainer("hdfs", "dfs", "-chmod", "-R", "777", dir);
      if (r.getExitCode() != 0) {
        throw new IllegalStateException("hdfs dfs -chmod -R 777 " + dir + " failed:\n" + r.getStderr());
      }
    }
  }

  static HiveConf buildHiveConf(String tezLibUris, Path localScratch, int hs2Port) throws Exception {
    HiveConf conf = new HiveConf();

    URL hiveSite = TestTezYarnUtils.class.getClassLoader().getResource("hive-site-yarn-it.xml");
    URL yarnSite = TestTezYarnUtils.class.getClassLoader().getResource("yarn-site.xml");
    if (hiveSite != null) {
      conf.addResource(hiveSite);
    }
    if (yarnSite != null) {
      conf.addResource(yarnSite);
    }

    // Dynamic properties: values derived at runtime from container ports or temp directories.
    conf.set("fs.defaultFS", HDFS_BASE);
    conf.set("hive.metastore.warehouse.dir", HDFS_WAREHOUSE);
    conf.set(HiveConf.ConfVars.SCRATCH_DIR.varname, HDFS_SCRATCH);
    conf.set(HiveConf.ConfVars.LOCAL_SCRATCH_DIR.varname, localScratch.toAbsolutePath().toString());
    conf.setVar(HiveConf.ConfVars.HIVE_USER_INSTALL_DIR, HDFS_ROOT + "/user-install");
    conf.set("javax.jdo.option.ConnectionURL",
        "jdbc:derby:" + localScratch.resolve("metastore_db").toAbsolutePath() + ";create=true");
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL, false);

    conf.set("tez.lib.uris", tezLibUris);

    conf.set("tez.am.client.am.port-range",
        TezYarnClusterContainer.AM_CLIENT_PORT_START + "-" + TezYarnClusterContainer.AM_CLIENT_PORT_END);
    String containerEnv = "JAVA_HOME=" + TezYarnClusterContainer.CONTAINER_JAVA_HOME
        + ",HADOOP_HOME=/opt/hadoop"
        + ",HADOOP_MAPRED_HOME=/opt/hadoop";
    conf.set("tez.am.launch.env", containerEnv);
    conf.set("tez.task.launch.env", containerEnv);

    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, hs2Port);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT, findFreePort());

    return conf;
  }

  static void waitForJdbc(int port) {
    String url = jdbcUrl(port);
    try {
      Awaitility.await()
              .atMost(Duration.ofSeconds(120))
              .pollInterval(Duration.ofSeconds(2))
              .ignoreExceptions()
              .until(() -> {
                try (Connection c = DriverManager.getConnection(url, "hive", "")) {
                  return true;
                }
              });
    } catch (ConditionTimeoutException e) {
      throw new IllegalStateException(
              "HiveServer2 JDBC endpoint not reachable on port " + port + " after 120s", e);
    }
  }

  static String jdbcUrl(int port) {
    return "jdbc:hive2://localhost:" + port + "/default;auth=noSasl";
  }

  static int findFreePort() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      s.setReuseAddress(true);
      return s.getLocalPort();
    }
  }
}
