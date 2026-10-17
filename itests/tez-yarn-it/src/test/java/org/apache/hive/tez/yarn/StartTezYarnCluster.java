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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Keep-alive HDFS + YARN + HiveServer2 cluster for manual Beeline testing.
 */
public class StartTezYarnCluster {

  private static final Logger LOG = LoggerFactory.getLogger(StartTezYarnCluster.class);

  @Test
  public void testRunCluster() throws Exception {
    TezYarnClusterContainer cluster = new TezYarnClusterContainer();
    cluster.start();

    TestTezYarnUtils.setupHdfs(cluster.namenodeContainer());

    String tezLibUris = cluster.uploadTezLibsToHdfs();
    LOG.info("Staged Tez libs to HDFS: {}", tezLibUris);

    Path localScratch = Files.createTempDirectory("hive-tez-loc-");

    int hs2Port = Integer.parseInt(System.getProperty("tez.yarn.cluster.hs2.port", "10000"));
    HiveConf conf = TestTezYarnUtils.buildHiveConf(tezLibUris, localScratch, hs2Port);

    HiveServer2 hs2 = new HiveServer2();
    hs2.init(conf);
    hs2.start();

    TestTezYarnUtils.waitForJdbc(hs2Port);

    String jdbcUrl = TestTezYarnUtils.jdbcUrl(hs2Port);
    String logFile = System.getProperty("tez.yarn.it.log.file");
    String bar = "======================================================================";
    System.out.println(bar);
    System.out.println("  Tez-on-YARN cluster is up. Ctrl-C this JVM to tear it down.");
    System.out.println(bar);
    System.out.printf("  %-22s %s%n", "HDFS URI", cluster.getHdfsUri());
    System.out.printf("  %-22s %s%n", "NameNode web UI", cluster.getNameNodeWebUrl());
    System.out.printf("  %-22s %s%n", "ResourceManager RPC", cluster.getResourceManagerAddress());
    System.out.printf("  %-22s %s%n", "ResourceManager UI", cluster.getResourceManagerWebUrl());
    System.out.printf("  %-22s %s%n", "NodeManager UI", cluster.getNodeManagerWebUrl());
    System.out.printf("  %-22s %s%n", "NodeManager AM RPC range", cluster.getNodeManagerAmRpcRange());
    System.out.printf("  %-22s %s%n", "Tez libs on HDFS", tezLibUris);
    System.out.printf("  %-22s %s%n", "HDFS warehouse", TestTezYarnUtils.HDFS_WAREHOUSE);
    System.out.printf("  %-22s %s%n", "HiveServer2 JDBC", jdbcUrl);
    System.out.printf("  %-22s %s%n", "Log file (slf4j)",
        logFile != null ? new File(logFile).getAbsolutePath() : "(configured by log4j2)");
    System.out.println(bar);
    System.out.println("  Beeline:");
    System.out.println("    beeline -u '" + jdbcUrl + "' -n hive");
    System.out.println(bar);
    System.out.flush();

    Thread.currentThread().join();
  }
}
