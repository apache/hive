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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Starts a full HDFS + YARN + HiveServer2 cluster backed by Docker containers
 * and keeps it running until the process is interrupted. Useful for manual testing with Beeline
 * without restarting the cluster between queries.
 */
public class StartTezYarnCluster {

  private static final Logger LOG = LoggerFactory.getLogger(StartTezYarnCluster.class);

  private static final String HDFS_BASE = "hdfs://namenode:8020";
  private static final String HDFS_ROOT = "/tmp/hive-tez-loc";
  private static final String HDFS_WAREHOUSE = HDFS_BASE + HDFS_ROOT + "/warehouse";
  private static final String HDFS_SCRATCH = HDFS_BASE + HDFS_ROOT + "/scratch";

  @Test
  public void testRunCluster() throws Exception {
    if (!Boolean.parseBoolean(System.getProperty("tez.yarn.cluster.run", "false"))) {
      return;
    }

    TezYarnClusterContainer cluster = new TezYarnClusterContainer(true);
    cluster.start();

    setupHdfs(cluster.namenodeContainer());

    String tezLibUris = cluster.uploadTezLibsToHdfs();
    LOG.info("Staged Tez libs to HDFS: {}", tezLibUris);

    Path localScratch = Files.createTempDirectory("hive-tez-loc-");
    String derbyUrl = "jdbc:derby:"
        + localScratch.resolve("metastore_db").toAbsolutePath() + ";create=true";
    System.setProperty("javax.jdo.option.ConnectionURL", derbyUrl);

    int hs2Port = Integer.parseInt(System.getProperty("tez.yarn.cluster.hs2.port", "10000"));
    HiveConf conf = buildConf(tezLibUris, localScratch, hs2Port);

    HiveServer2 hs2 = new HiveServer2();
    hs2.init(conf);
    hs2.start();

    String jdbcUrl = "jdbc:hive2://localhost:" + hs2Port + "/default;auth=noSasl";
    LOG.info("=====================================================");
    LOG.info("HiveServer2 is ready on port {}", hs2Port);
    LOG.info("JDBC URL  : {}", jdbcUrl);
    LOG.info("Beeline   : beeline -u '{}' -n hive", jdbcUrl);
    LOG.info("Press Ctrl+C to stop the cluster.");
    LOG.info("=====================================================");

    Thread.currentThread().join();
  }

  private static void setupHdfs(GenericContainer<?> nn) throws Exception {
    String[] dirs = {
        "/tmp",
        HDFS_ROOT + "/warehouse",
        HDFS_ROOT + "/scratch",
        HDFS_ROOT + "/user-install"
    };
    for (String dir : dirs) {
      GenericContainer.ExecResult r = nn.execInContainer("hdfs", "dfs", "-mkdir", "-p", dir);
      if (r.getExitCode() != 0) {
        throw new IllegalStateException("hdfs dfs -mkdir -p " + dir + " failed:\n" + r.getStderr());
      }
    }
    for (String dir : new String[]{"/tmp", HDFS_ROOT}) {
      GenericContainer.ExecResult r = nn.execInContainer("hdfs", "dfs", "-chmod", "-R", "777", dir);
      if (r.getExitCode() != 0) {
        throw new IllegalStateException("hdfs dfs -chmod -R 777 " + dir + " failed:\n" + r.getStderr());
      }
    }
  }

  private static HiveConf buildConf(String tezLibUris, Path localScratch, int hs2Port) throws Exception {
    HiveConf conf = new HiveConf();
    URL hiveSite = StartTezYarnCluster.class.getClassLoader().getResource("hive-site-yarn-it.xml");
    URL yarnSite = StartTezYarnCluster.class.getClassLoader().getResource("yarn-site.xml");
    if (hiveSite != null) {
      conf.addResource(hiveSite);
    }
    if (yarnSite != null) {
      conf.addResource(yarnSite);
    }

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
        TezYarnClusterContainer.AM_CLIENT_PORT + "-" + TezYarnClusterContainer.AM_CLIENT_PORT);
    String containerEnv = "JAVA_HOME=" + TezYarnClusterContainer.CONTAINER_JAVA_21_HOME
        + ",HADOOP_HOME=/opt/hadoop"
        + ",HADOOP_MAPRED_HOME=/opt/hadoop";
    conf.set("tez.am.launch.env", containerEnv);
    conf.set("tez.task.launch.env", containerEnv);

    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, hs2Port);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT, hs2Port + 1);

    return conf;
  }
}
