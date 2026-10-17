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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.SafeMode;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

/**
 * Starts an HDFS + YARN cluster from docker-compose.yml via Testcontainers ComposeContainer.
 * Fixed published ports plus custom_hosts_file let the host JVM reach namenode/resourcemanager.
 * HDFS and YARN are driven through the Hadoop client APIs rather than in-container CLIs.
 */
public class TezYarnClusterContainer {

  private static final Logger LOG = LoggerFactory.getLogger(TezYarnClusterContainer.class);

  public static final String CONTAINER_JAVA_HOME = "/opt/jdk21";

  private final String hadoop_image;

  private static final int NN_RPC_PORT = 8020;
  private static final int NN_HTTP_PORT = 9870;
  private static final int RM_RPC_PORT = 8032;
  private static final int RM_HTTP_PORT = 8088;
  private static final int NM_HTTP_PORT = 8042;
  // Tez AM client RPC port range, published by the NM container so the host JVM can reach the AM.
  public static final int AM_CLIENT_PORT_START = 41000;
  public static final int AM_CLIENT_PORT_END = 41020;

  // Compose V2 names service containers "<service>-1"; used by getContainerByServiceName().
  private static final int SERVICE_INSTANCE = 1;

  private static final Duration CLUSTER_READY_TIMEOUT = Duration.ofMinutes(2);
  private static final Duration CLUSTER_READY_POLL_INTERVAL = Duration.ofSeconds(3);
  private static final long RM_CONNECT_MAX_WAIT_MS = 2000;
  private static final long RM_CONNECT_RETRY_INTERVAL_MS = 500;

  private final ComposeContainer compose;

  public TezYarnClusterContainer() {
    hadoop_image = buildHadoopImage();
    String basedir = System.getProperty("basedir", ".");
    File composeFile = Paths.get(basedir, "src/test/docker/hadoop-yarn/docker-compose.yml").toFile();
    LOG.info("Starting Tez-on-YARN cluster from {} (image {})", composeFile, hadoop_image);
    // withLocalCompose(false): no local docker-compose binary required on CI.
    compose = new ComposeContainer(composeFile)
        .withLocalCompose(false);
  }

  public void start() throws Exception {
    try {
      compose.start();
      // Avoid flakiness: wait until HDFS has left safemode before running HDFS operations.
      waitForSafemodeExit();
      waitForNodeManagerRegistration();
      verifyJava21InNodeManager();
    } catch (Exception e) {
      try {
        stop();
      } catch (Exception stopException) {
        LOG.info("Failed to stop TezYarnClusterContainer after startup failure", stopException);
      }
      throw e;
    }
  }

  public void stop() {
    compose.stop();
  }

  private void verifyJava21InNodeManager() throws Exception {
    Container.ExecResult r = nodeManagerContainer().execInContainer(
            CONTAINER_JAVA_HOME + "/bin/java", "-version");
    if (r.getExitCode() == 0) {
      LOG.info("Java 21 is functional in NodeManager ({}): {}",
              CONTAINER_JAVA_HOME, r.getStderr().trim());
    } else {
      throw new IllegalStateException(
              "Java check failed in NodeManager (exit " + r.getExitCode()
                      + "). stderr: " + r.getStderr());
    }
  }

  public String getHdfsUri() {
    return "hdfs://namenode:" + NN_RPC_PORT;
  }

  public String getResourceManagerAddress() {
    return "resourcemanager:" + RM_RPC_PORT;
  }

  public String getResourceManagerWebAppAddress() {
    return "resourcemanager:" + RM_HTTP_PORT;
  }

  // Web UIs are opened from the host, so they use the published localhost ports;
  // RPC and AM endpoints use the container hostnames that resolve via custom_hosts_file.
  public String getNameNodeWebUrl() {
    return "http://localhost:" + NN_HTTP_PORT;
  }

  public String getResourceManagerWebUrl() {
    return "http://localhost:" + RM_HTTP_PORT;
  }

  public String getNodeManagerWebUrl() {
    return "http://localhost:" + NM_HTTP_PORT;
  }

  public String getNodeManagerAmRpcRange() {
    return "nodemanager:" + AM_CLIENT_PORT_START + "-" + AM_CLIENT_PORT_END;
  }

  public String uploadJarToHdfs(Path localJarPath) throws IOException {
    String fileName = localJarPath.getFileName().toString();
    String hdfsDir = "/tmp/hive-tez-yarn-jars";
    String hdfsPath = hdfsDir + "/" + fileName;

    copyToHdfs(localJarPath, hdfsDir, hdfsPath);

    return hdfsPath;
  }

  public String uploadTezLibsToHdfs() throws IOException {
    String tezDistPath = System.getProperty("tez.dist.path");
    if (tezDistPath == null || tezDistPath.isEmpty()) {
      throw new IllegalStateException(
          "System property 'tez.dist.path' is not set. "
          + "It must point to a Tez distribution tarball selected by the Maven test run "
          + "(default staged tarball, local tarball, or downloaded tarball). "
          + "If running tests in isolation, invoke Maven with -Ptez-yarn so surefire sets it.");
    }

    Path tarball = Paths.get(tezDistPath);
    if (!Files.isRegularFile(tarball)) {
      throw new IllegalStateException(
          "Tez distribution tarball not found at: " + tarball.toAbsolutePath()
          + ". Build the staged tarball with "
          + "'mvn test-compile -Pitests,tez-yarn -pl itests/tez-yarn-it' "
          + "or pass a valid local/downloaded archive via tez.dist.source options.");
    }

    String fileName = tarball.getFileName().toString();
    String hdfsDir = "/tmp/hive-tez-yarn";
    String hdfsPath = hdfsDir + "/" + fileName;

    LOG.info("Uploading Tez distribution tarball ({}) to HDFS path {}",
        tarball.toAbsolutePath(), hdfsPath);

    copyToHdfs(tarball, hdfsDir, hdfsPath);

    // "#tez" is the YARN container link name for the localized archive.
    return "hdfs://namenode:" + NN_RPC_PORT + hdfsPath + "#tez";
  }

  private void copyToHdfs(Path localPath, String hdfsDir, String hdfsPath) throws IOException {
    // java.nio.file.Path is used for local files here, so the HDFS paths are qualified explicitly.
    org.apache.hadoop.fs.Path src = new org.apache.hadoop.fs.Path(localPath.toUri());
    org.apache.hadoop.fs.Path dest = new org.apache.hadoop.fs.Path(hdfsPath);

    try (FileSystem fs = newFileSystem()) {
      fs.mkdirs(new org.apache.hadoop.fs.Path(hdfsDir));
      // delSrc=false, overwrite=true mirrors the semantics of "hdfs dfs -put -f".
      fs.copyFromLocalFile(false, true, src, dest);
    }
  }

  ContainerState namenodeContainer() {
    return serviceContainer("namenode");
  }

  ContainerState resourceManagerContainer() {
    return serviceContainer("resourcemanager");
  }

  ContainerState nodeManagerContainer() {
    return serviceContainer("nodemanager");
  }

  private ContainerState serviceContainer(String serviceName) {
    ContainerState cs = compose.getContainerByServiceName(serviceName + "-" + SERVICE_INSTANCE)
        .orElseThrow(() -> new IllegalStateException(
            "Compose service container not found: " + serviceName));

    if (!cs.isRunning()){
      throw new IllegalStateException(
          "Compose service container not running: " + serviceName);
    }
    return cs;
  }

  private void waitForSafemodeExit() {
    try {
      Awaitility.await()
              .pollDelay(Duration.ZERO)
              .pollInterval(CLUSTER_READY_POLL_INTERVAL)
              .atMost(CLUSTER_READY_TIMEOUT)
              // The NameNode healthcheck only covers the web port, so RPC may still be refused here.
              .ignoreExceptions()
              .until(() -> {
                try (FileSystem fs = newFileSystem()) {
                  return !((SafeMode) fs).setSafeMode(SafeModeAction.GET);
                }
              });
    } catch (ConditionTimeoutException e) {
      throw new IllegalStateException(
              "HDFS did not leave safemode within " + CLUSTER_READY_TIMEOUT.toMinutes() + " minutes", e);
    }
  }

  private void waitForNodeManagerRegistration() {
    try (YarnClient yarnClient = YarnClient.createYarnClient()) {
      yarnClient.init(buildHadoopConfiguration());
      yarnClient.start();

      Awaitility.await()
              .pollDelay(Duration.ZERO)
              .pollInterval(CLUSTER_READY_POLL_INTERVAL)
              .atMost(CLUSTER_READY_TIMEOUT)
              .ignoreExceptions()
              .until(() -> !yarnClient.getNodeReports(NodeState.RUNNING).isEmpty());
    } catch (ConditionTimeoutException e) {
      throw new IllegalStateException("NodeManager did not register with ResourceManager within "
              + CLUSTER_READY_TIMEOUT.toMinutes() + " minutes", e);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to query the ResourceManager for registered NodeManagers", e);
    }
  }

  /**
   * Configuration for host JVM access to the containerised HDFS and YARN services.
   */
  private Configuration buildHadoopConfiguration() {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, getHdfsUri());
    // Reach DataNodes by their container hostname (custom_hosts_file), not their container IP.
    conf.setBoolean("dfs.client.use.datanode.hostname", true);
    conf.setInt("dfs.replication", 1);
    conf.set(YarnConfiguration.RM_ADDRESS, getResourceManagerAddress());
    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, RM_CONNECT_MAX_WAIT_MS);
    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, RM_CONNECT_RETRY_INTERVAL_MS);
    return conf;
  }

  /**
   * Uncached FileSystem so callers can close it without affecting the HiveServer2 instance.
   */
  private FileSystem newFileSystem() throws IOException {
    return FileSystem.newInstance(URI.create(getHdfsUri()), buildHadoopConfiguration());
  }

  private static String buildHadoopImage() {
    String basedir = System.getProperty("basedir", ".");
    Path dockerfile = Paths.get(basedir, "src/test/docker/hadoop-yarn/Dockerfile");
    String imageName = "hive-it-hadoop-" + java.util.UUID.randomUUID();
    return new ImageFromDockerfile(imageName, false)
        .withDockerfile(dockerfile)
        .get();
  }
}
