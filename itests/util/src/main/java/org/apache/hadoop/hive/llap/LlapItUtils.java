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

package org.apache.hadoop.hive.llap;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.configuration.LlapDaemonConfiguration;
import org.apache.hadoop.hive.llap.daemon.MiniLlapCluster;
import org.apache.hadoop.hive.llap.daemon.impl.LlapDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapItUtils {

  private static final Logger LOG = LoggerFactory.getLogger(LlapItUtils.class);

  public static MiniLlapCluster startAndGetMiniLlapCluster(Configuration conf,
                                                           MiniZooKeeperCluster miniZkCluster,
                                                           String confDir) throws
      IOException {
    MiniLlapCluster llapCluster;
    LOG.info("Using conf dir: {}", confDir);
    if (confDir != null && !confDir.isEmpty()) {
      conf.addResource(new URL("file://" + new File(confDir).toURI().getPath()
          + "/tez-site.xml"));
    }

    Configuration daemonConf = new LlapDaemonConfiguration(conf);
    final String clusterName = "llap";
    final long maxMemory = LlapDaemon.getTotalHeapSize();
    // 15% for io cache
    final long memoryForCache = (long) (0.15f * maxMemory);
    // 75% for 4 executors
    final long totalExecutorMemory = (long) (0.75f * maxMemory);
    final int numExecutors = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_DAEMON_NUM_EXECUTORS);
    final boolean asyncIOEnabled = true;
    // enabling this will cause test failures in Mac OS X
    final boolean directMemoryEnabled = false;
    final int numLocalDirs = 1;
    LOG.info("MiniLlap Configs -  maxMemory: " + maxMemory +
        " memoryForCache: " + memoryForCache
        + " totalExecutorMemory: " + totalExecutorMemory + " numExecutors: " + numExecutors
        + " asyncIOEnabled: " + asyncIOEnabled + " directMemoryEnabled: " + directMemoryEnabled
        + " numLocalDirs: " + numLocalDirs);
    llapCluster = MiniLlapCluster.create(clusterName,
        miniZkCluster,
        1,
        numExecutors,
        totalExecutorMemory,
        asyncIOEnabled,
        directMemoryEnabled,
        memoryForCache,
        numLocalDirs);
    llapCluster.init(daemonConf);
    llapCluster.start();

    // Augment conf with the settings from the started llap configuration.
    Configuration llapConf = llapCluster.getClusterSpecificConfiguration();
    Iterator<Map.Entry<String, String>> confIter = llapConf.iterator();
    while (confIter.hasNext()) {
      Map.Entry<String, String> entry = confIter.next();
      conf.set(entry.getKey(), entry.getValue());
    }
    return llapCluster;
  }

}
