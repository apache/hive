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

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

/** Starts an HDFS + YARN cluster from docker-compose.yml via Testcontainers ComposeContainer.
 *  Fixed published ports plus custom_hosts_file let the host JVM reach namenode/resourcemanager. */
public class TezYarnClusterContainer {

  private static final Logger LOG = LoggerFactory.getLogger(TezYarnClusterContainer.class);

  /** Path to the Java 21 runtime inside containers for Tez AM/task launch environments. */
  public static final String CONTAINER_JAVA_21_HOME = "/opt/jdk21";

  private static final String HADOOP_IMAGE = buildHadoopImage();

  private static final int NN_RPC_PORT = 8020;
  private static final int RM_RPC_PORT = 8032;
  private static final int RM_HTTP_PORT = 8088;
  // Tez AM client RPC port, published by the NM container so the host JVM can reach the AM.
  public static final int AM_CLIENT_PORT = 41000;

  // Compose V2 names service containers "<service>-1"; used by getContainerByServiceName().
  private static final int SERVICE_INSTANCE = 1;

  private final ComposeContainer compose;

  public TezYarnClusterContainer() {
    String basedir = System.getProperty("basedir", ".");
    File composeFile = Paths.get(basedir, "src/test/docker/hadoop-yarn/docker-compose.yml").toFile();
    LOG.info("Starting Tez-on-YARN cluster from {} (image {})", composeFile, HADOOP_IMAGE);
    // withLocalCompose(false): no local docker-compose binary required on CI.
    compose = new ComposeContainer(composeFile)
        .withLocalCompose(false);
  }

  public void start() {
    try {
      compose.start();
      // Avoid flakiness: wait until HDFS has left safemode before running HDFS operations.
      requireSuccess(namenodeContainer().execInContainer("hdfs", "dfsadmin", "-safemode", "wait"),
              "hdfs dfsadmin -safemode wait");
      waitForNodeManagerRegistration();
      verifyJava21InNodeManager();
    } catch (Exception e) {
      try {
        stop();
      } catch (Exception ignored) {
      }
      throw new IllegalStateException("Failed to start TezYarnClusterContainer", e);
    }
  }

  public void stop() {
    compose.stop();
  }

  private void verifyJava21InNodeManager() {
    try {
      Container.ExecResult r = nodeManagerContainer().execInContainer(
          CONTAINER_JAVA_21_HOME + "/bin/java", "-version");
      if (r.getExitCode() == 0) {
        LOG.info("Java 21 is functional in NodeManager ({}): {}",
            CONTAINER_JAVA_21_HOME, r.getStderr().trim());
      } else {
        LOG.warn("Java 21 check FAILED in NodeManager (exit {}). "
            + "Tez AM/task containers will fail at launch time. "
            + "stderr: {}", r.getExitCode(), r.getStderr());
      }
    } catch (Exception e) {
      LOG.warn("Could not verify Java 21 in NodeManager container", e);
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

  public String uploadJarToHdfs(Path localJarPath) throws IOException, InterruptedException {
    String fileName = localJarPath.getFileName().toString();
    String containerTmp = "/tmp/" + fileName;
    String hdfsDir = "/tmp/hive-tez-yarn-jars";
    String hdfsPath = hdfsDir + "/" + fileName;

    ContainerState namenode = namenodeContainer();
    namenode.copyFileToContainer(MountableFile.forHostPath(localJarPath, 0644), containerTmp);

    Container.ExecResult mkdir = namenode.execInContainer("hdfs", "dfs", "-mkdir", "-p", hdfsDir);
    requireSuccess(mkdir, "hdfs dfs -mkdir -p " + hdfsDir);

    Container.ExecResult put = namenode.execInContainer("hdfs", "dfs", "-put", "-f", containerTmp, hdfsPath);
    requireSuccess(put, "hdfs dfs -put -f " + containerTmp + " " + hdfsPath);

    return hdfsPath;
  }

  public String uploadTezLibsToHdfs() throws IOException, InterruptedException {
    String tezDistPath = System.getProperty("tez.dist.path");
    if (tezDistPath == null || tezDistPath.isEmpty()) {
      throw new IllegalStateException(
          "System property 'tez.dist.path' is not set. "
          + "It must point to the tez-libs.tar.gz assembled from Tez Maven artifacts. "
          + "This is set automatically by the Maven surefire configuration; "
          + "if running tests in isolation, ensure the module was built with "
          + "'mvn test-compile -Pitests,tez-yarn' first.");
    }

    Path tarball = Paths.get(tezDistPath);
    if (!Files.isRegularFile(tarball)) {
      throw new IllegalStateException(
          "Tez distribution tarball not found at: " + tarball.toAbsolutePath()
          + ". Run 'mvn test-compile -Pitests,tez-yarn -pl itests/tez-yarn-it' to build it.");
    }

    String fileName = tarball.getFileName().toString();
    String containerTmp = "/tmp/" + fileName;
    String hdfsDir = "/tmp/hive-tez-yarn";
    String hdfsPath = hdfsDir + "/" + fileName;

    LOG.info("Uploading Tez distribution tarball ({}) to HDFS path {}",
        tarball.toAbsolutePath(), hdfsPath);

    ContainerState namenode = namenodeContainer();
    Container.ExecResult mkdir = namenode.execInContainer("hdfs", "dfs", "-mkdir", "-p", hdfsDir);
    requireSuccess(mkdir, "hdfs dfs -mkdir -p " + hdfsDir);

    namenode.copyFileToContainer(MountableFile.forHostPath(tarball, 0644), containerTmp);

    Container.ExecResult put = namenode.execInContainer(
        "hdfs", "dfs", "-put", "-f", containerTmp, hdfsPath);
    requireSuccess(put, "hdfs dfs -put -f " + containerTmp + " " + hdfsPath);

    // "#tez" is the YARN container link name for the localized archive.
    return "hdfs://namenode:" + NN_RPC_PORT + hdfsPath + "#tez";
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
    return compose.getContainerByServiceName(serviceName + "-" + SERVICE_INSTANCE)
        .orElseThrow(() -> new IllegalStateException(
            "Compose service container not running: " + serviceName));
  }

  private void waitForNodeManagerRegistration() {
    final String[] lastOut = {""};

    try {
      Awaitility.await()
              .pollDelay(Duration.ZERO)
              .pollInterval(Duration.ofSeconds(3))
              .atMost(Duration.ofMinutes(2))
              .ignoreExceptions()
              .until(() -> {
                Container.ExecResult result = resourceManagerContainer().execInContainer("yarn", "node", "-list");
                String out = result.getStdout();
                lastOut[0] = out;
                return out.contains("Total Nodes:") && !out.contains("Total Nodes:0");
              });
    } catch (ConditionTimeoutException e) {
      throw new IllegalStateException(
              "NodeManager did not register with ResourceManager within 2 minutes. Last output:\n" + lastOut[0], e);
    }
  }

  private static void requireSuccess(Container.ExecResult result, String cmd) {
    if (result.getExitCode() != 0) {
      throw new IllegalStateException("Command failed (" + cmd + ")\nstdout:\n"
          + result.getStdout() + "\nstderr:\n" + result.getStderr());
    }
  }

  private static String buildHadoopImage() {
    String basedir = System.getProperty("basedir", ".");
    Path dockerfile = Paths.get(basedir, "src/test/docker/hadoop-yarn/Dockerfile");
    return new ImageFromDockerfile("hive-it-hadoop-jdk21", false)
        .withDockerfile(dockerfile)
        .get();
  }
}
