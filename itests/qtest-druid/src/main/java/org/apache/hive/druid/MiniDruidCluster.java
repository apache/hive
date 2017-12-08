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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * This class has the hooks to start and stop the external Druid Nodes
 */
public class MiniDruidCluster extends AbstractService {
  private static final Logger log = LoggerFactory.getLogger(MiniDruidCluster.class);

  private static final String COMMON_DRUID_JVM_PROPPERTIES = "-Duser.timezone=UTC -Dfile.encoding=UTF-8 -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager -Ddruid.emitter=logging -Ddruid.emitter.logging.logLevel=info";

  private static final List<String> HISTORICAL_JVM_CONF = Arrays
          .asList("-server", "-XX:MaxDirectMemorySize=10g", "-Xmx512m", "-Xmx512m",
                  COMMON_DRUID_JVM_PROPPERTIES
          );

  private static final List<String> COORDINATOR_JVM_CONF = Arrays
          .asList("-server", "-XX:MaxDirectMemorySize=2g", "-Xmx512m", "-Xms512m",
                  COMMON_DRUID_JVM_PROPPERTIES
          );

  private static final Map<String, String> COMMON_DRUID_CONF = ImmutableMap.of(
          "druid.metadata.storage.type", "derby"
  );

  private static final Map<String, String> COMMON_DRUID_HISTORICAL = ImmutableMap.of(
          "druid.processing.buffer.sizeBytes", "213870912",
          "druid.processing.numThreads", "2",
          "druid.server.maxSize", "130000000000"
  );

  private static final Map<String, String> COMMON_COORDINATOR_INDEXER = ImmutableMap
          .of(
                  "druid.indexer.logs.type", "file",
                  "druid.coordinator.asOverlord.enabled", "true",
                  "druid.coordinator.asOverlord.overlordService", "druid/overlord",
                  "druid.coordinator.period", "PT10S",
                  "druid.manager.segments.pollDuration", "PT10S"
          );

  private final DruidNode historical;

  private final DruidNode broker;

  // Coordinator is running as Overlord as well.
  private final DruidNode coordinator;

  private final List<DruidNode> druidNodes;

  private final File dataDirectory;

  private final File logDirectory;

  public MiniDruidCluster(String name) {
    this(name, "/tmp/miniDruid/log", "/tmp/miniDruid/data", 2181, null);
  }


  public MiniDruidCluster(String name, String logDir, String dataDir, Integer zookeeperPort, String classpath) {
    super(name);
    this.dataDirectory = new File(dataDir, "druid-data");
    this.logDirectory = new File(logDir);
    try {

      if (dataDirectory.exists()) {
        // need to clean data directory to ensure that there is no interference from old runs
        // Cleaning is happening here to allow debugging in case of tests fail
        // we don;t have to clean logs since it is an append mode
        log.info("Cleaning the druid-data directory [{}]", dataDirectory.getAbsolutePath());
        FileUtils.deleteDirectory(dataDirectory);
      } else {
        log.info("Creating the druid-data directory [{}]", dataDirectory.getAbsolutePath());
        dataDirectory.mkdirs();
      }
    } catch (IOException e) {
      log.error("Failed to clean data directory");
      Throwables.propagate(e);
    }
    String derbyURI = String
            .format("jdbc:derby://localhost:1527/%s/druid_derby/metadata.db;create=true",
                    dataDirectory.getAbsolutePath()
            );
    String segmentsCache = String
            .format("[{\"path\":\"%s/druid/segment-cache\",\"maxSize\":130000000000}]",
                    dataDirectory.getAbsolutePath()
            );
    String indexingLogDir = new File(logDirectory, "indexer-log").getAbsolutePath();

    ImmutableMap.Builder<String, String> coordinatorMapBuilder = new ImmutableMap.Builder();
    ImmutableMap.Builder<String, String> historicalMapBuilder = new ImmutableMap.Builder();

    Map<String, String> coordinatorProperties = coordinatorMapBuilder.putAll(COMMON_DRUID_CONF)
            .putAll(COMMON_COORDINATOR_INDEXER)
            .put("druid.metadata.storage.connector.connectURI", derbyURI)
            .put("druid.indexer.logs.directory", indexingLogDir)
            .put("druid.zk.service.host", "localhost:" + zookeeperPort)
            .put("druid.coordinator.startDelay", "PT1S")
            .build();
    Map<String, String> historicalProperties = historicalMapBuilder.putAll(COMMON_DRUID_CONF)
            .putAll(COMMON_DRUID_HISTORICAL)
            .put("druid.zk.service.host", "localhost:" + zookeeperPort)
            .put("druid.segmentCache.locations", segmentsCache)
            .put("druid.storage.storageDirectory", getDeepStorageDir())
            .put("druid.storage.type", "hdfs")
            .build();
    coordinator = new ForkingDruidNode("coordinator", classpath, coordinatorProperties,
            COORDINATOR_JVM_CONF,
            logDirectory, null
    );
    historical = new ForkingDruidNode("historical", classpath, historicalProperties, HISTORICAL_JVM_CONF,
            logDirectory, null
    );
    broker = new ForkingDruidNode("broker", classpath, historicalProperties, HISTORICAL_JVM_CONF,
            logDirectory, null
    );
    druidNodes = Arrays.asList(coordinator, historical, broker);

  }

  @Override
  protected void serviceStart() throws Exception {
    druidNodes.stream().forEach(node -> {
      try {
        node.start();
      } catch (IOException e) {
        log.error("Failed to start node " + node.getNodeType()
                + " Consequently will destroy the cluster");
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

  @Override
  protected void serviceStop() throws Exception {
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
    return String.format("jdbc:derby://localhost:1527/%s/druid_derby/metadata.db",
            dataDirectory.getAbsolutePath()
    );
  }

  public String getDeepStorageDir() {
    return dataDirectory.getAbsolutePath() + File.separator + "deep-storage";
  }
}
