/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.druid;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * This class has the hooks to start and stop the external Druid Nodes
 */
public class MiniDruidCluster extends AbstractService {
  private static final Logger log = LoggerFactory.getLogger(MiniDruidCluster.class);

  private static final String
      COMMON_DRUID_JVM_PROPERTIES =
      "-Duser.timezone=UTC -Dfile.encoding=UTF-8 -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager "
          + "-Ddruid.emitter=logging -Ddruid.emitter.logging.logLevel=info";

  private static final List<String>
      HISTORICAL_JVM_CONF =
      Arrays.asList("-server", "-XX:MaxDirectMemorySize=10g", "-Xmx512m", "-Xmx512m", COMMON_DRUID_JVM_PROPERTIES);

  private static final List<String>
      COORDINATOR_JVM_CONF =
      Arrays.asList("-server", "-XX:MaxDirectMemorySize=2g", "-Xmx512m", "-Xms512m", COMMON_DRUID_JVM_PROPERTIES);

  private static final Map<String, String>
      COMMON_DRUID_CONF =
      ImmutableMap.of("druid.metadata.storage.type",
          "derby",
          "druid.storage.type",
          "hdfs",
          "druid.processing.buffer.sizeBytes",
          "10485760",
          "druid.processing.numThreads",
          "2",
          "druid.worker.capacity",
          "4");

  private static final Map<String, String>
      COMMON_DRUID_HISTORICAL =
      ImmutableMap.of("druid.server.maxSize", "130000000000");

  private static final Map<String, String>
      COMMON_COORDINATOR_INDEXER =
      ImmutableMap.<String,String>builder()
          .put("druid.indexer.logs.type", "file")
          .put("druid.coordinator.asOverlord.enabled", "true")
          .put("druid.coordinator.asOverlord.overlordService", "druid/overlord")
          .put("druid.coordinator.period", "PT2S")
          .put("druid.manager.segments.pollDuration", "PT2S")
          .put("druid.indexer.runner.javaOpts", "-Xmx512m")
          .build();
  private static final int MIN_PORT_NUMBER = 60000;
  private static final int MAX_PORT_NUMBER = 65535;

  private final DruidNode historical;

  private final DruidNode broker;

  // Coordinator is running as Overlord as well.
  private final DruidNode coordinator;

  private final List<DruidNode> druidNodes;

  private final File dataDirectory;

  private final File logDirectory;
  private final String derbyURI;
  private final int coordinatorPort;
  private final int historicalPort;
  private final int brokerPort;


  public MiniDruidCluster(String name, String logDir, String tmpDir, Integer zookeeperPort, String classpath) {
    super(name);
    this.dataDirectory = new File(tmpDir, "druid-data");
    this.logDirectory = new File(logDir);

    boolean isKafka = name.contains("kafka");
    coordinatorPort = isKafka ? 8081 : 9081;
    brokerPort = coordinatorPort + 1;
    historicalPort = brokerPort + 1;
    int derbyPort = historicalPort + 1;

    ensureCleanDirectory(dataDirectory);

    derbyURI =
        String.format("jdbc:derby://localhost:%s/%s/druid_derby/metadata.db;create=true",
            derbyPort,
            dataDirectory.getAbsolutePath());
    String
        segmentsCache =
        String.format("[{\"path\":\"%s/druid/segment-cache\",\"maxSize\":130000000000}]",
            dataDirectory.getAbsolutePath());
    String indexingLogDir = new File(logDirectory, "indexer-log").getAbsolutePath();

    ImmutableMap.Builder<String, String> coordinatorMapBuilder = new ImmutableMap.Builder();
    ImmutableMap.Builder<String, String> historicalMapBuilder = new ImmutableMap.Builder();
    ImmutableMap.Builder<String, String> brokerMapBuilder = new ImmutableMap.Builder();

    Map<String, String>
        coordinatorProperties =
        coordinatorMapBuilder.putAll(COMMON_DRUID_CONF)
            .putAll(COMMON_COORDINATOR_INDEXER)
            .put("druid.metadata.storage.connector.connectURI", derbyURI)
            .put("druid.metadata.storage.connector.port", String.valueOf(derbyPort))
            .put("druid.indexer.logs.directory", indexingLogDir)
            .put("druid.zk.service.host", "localhost:" + zookeeperPort)
            .put("druid.coordinator.startDelay", "PT1S")
            .put("druid.indexer.runner", "local")
            .put("druid.storage.storageDirectory", getDeepStorageDir())
            .put("druid.port", String.valueOf(coordinatorPort))
            .build();
    Map<String, String>
        historicalProperties =
        historicalMapBuilder.putAll(COMMON_DRUID_CONF)
            .putAll(COMMON_DRUID_HISTORICAL)
            .put("druid.zk.service.host", "localhost:" + zookeeperPort)
            .put("druid.segmentCache.locations", segmentsCache)
            .put("druid.storage.storageDirectory", getDeepStorageDir())
            .put("druid.port", String.valueOf(historicalPort))
            .build();
    Map<String, String>
        brokerProperties =
        brokerMapBuilder.putAll(COMMON_DRUID_CONF)
            .put("druid.zk.service.host", "localhost:" + zookeeperPort)
            .put("druid.port", String.valueOf(brokerPort))
            .build();
    coordinator =
        new ForkingDruidNode("coordinator", classpath, coordinatorProperties, COORDINATOR_JVM_CONF, logDirectory, null);
    historical =
        new ForkingDruidNode("historical", classpath, historicalProperties, HISTORICAL_JVM_CONF, logDirectory, null);
    broker = new ForkingDruidNode("broker", classpath, brokerProperties, HISTORICAL_JVM_CONF, logDirectory, null);
    druidNodes = Arrays.asList(coordinator, historical, broker);

  }

  private int findPort(int start, int end) {
    int port = start;
    while (!available(port)) {
      port++;
      if (port == end) {
        throw new RuntimeException("can not find free port for range " + start + ":" + end);
      }
    }
    return port;
  }

  /**
   * Checks to see if a specific port is available.
   *
   * @param port the port to check for availability
   */
  public static boolean available(int port) {
    if (port < MIN_PORT_NUMBER || port > MAX_PORT_NUMBER) {
      throw new IllegalArgumentException("Invalid start port: " + port);
    }

    ServerSocket ss = null;
    DatagramSocket ds = null;
    try {
      ss = new ServerSocket(port);
      ss.setReuseAddress(true);
      ds = new DatagramSocket(port);
      ds.setReuseAddress(true);
      return true;
    } catch (IOException e) {
    } finally {
      if (ds != null) {
        ds.close();
      }

      if (ss != null) {
        try {
          ss.close();
        } catch (IOException e) {
          /* should not be thrown */
        }
      }
    }

    return false;
  }

  public static void ensureCleanDirectory(File dir) {
    try {
      if (dir.exists()) {
        // need to clean data directory to ensure that there is no interference from old runs
        // Cleaning is happening here to allow debugging in case of tests fail
        // we don;t have to clean logs since it is an append mode
        log.info("Cleaning the druid directory [{}]", dir.getAbsolutePath());
        FileUtils.deleteDirectory(dir);
      } else {
        log.info("Creating the druid directory [{}]", dir.getAbsolutePath());
        dir.mkdirs();
      }
    } catch (IOException e) {
      log.error("Failed to clean druid directory");
      Throwables.propagate(e);
    }
  }

  @Override protected void serviceStart() throws Exception {
    druidNodes.stream().forEach(node -> {
      try {
        node.start();
      } catch (IOException e) {
        log.error("Failed to start node " + node.getNodeType() + " Consequently will destroy the cluster");
        druidNodes.stream().filter(node1 -> node1.isAlive()).forEach(nodeToStop -> {
          try {
            log.info("Stopping Node " + nodeToStop.getNodeType());
            nodeToStop.close();
          } catch (IOException e1) {
            log.error("Error while stopping " + nodeToStop.getNodeType(), e1);
          }
        });
        Throwables.propagate(e);
      }
    });
  }

  @Override protected void serviceStop() throws Exception {
    druidNodes.stream().forEach(node -> {
      try {
        node.close();
      } catch (IOException e) {
        // nothing that we can really do about it
        log.error(String.format("Failed to stop druid node [%s]", node.getNodeType()), e);
      }
    });
  }

  public String getMetadataURI() {
    return derbyURI;
  }

  public String getDeepStorageDir() {
    return dataDirectory.getAbsolutePath() + File.separator + "deep-storage";
  }

  public String getCoordinatorURI() {
    return String.format(Locale.ROOT,"localhost:%s", coordinatorPort);
  }

  public String getBrokerURI() {
    return String.format(Locale.ROOT,"localhost:%s", brokerPort);
  }

  public String getOverlordURI() {
    // Overlord and coordinator both run in same JVM.
    return getCoordinatorURI();
  }
}
