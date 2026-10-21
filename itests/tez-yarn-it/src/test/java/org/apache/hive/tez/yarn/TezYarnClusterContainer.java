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
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

public class TezYarnClusterContainer {

  private static final Logger LOG = LoggerFactory.getLogger(TezYarnClusterContainer.class);

  /** Path to the Java 21 runtime inside containers for Tez AM/task launch environments. */
  public static final String CONTAINER_JAVA_21_HOME = "/opt/jdk21";

  private static final String HADOOP_IMAGE = buildHadoopImage();
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);
  private static final Map<String, String> COMMON_ENV = loadCommonEnv();

  private static final int NN_RPC_PORT  = 8020;
  private static final int NN_HTTP_PORT = 9870;
  private static final int RM_RPC_PORT  = 8032;
  private static final int RM_HTTP_PORT = 8088;
  private static final int DN_HTTP_PORT = 9864;
  private static final int DN_XFER_PORT = 9866;
  // Tez AM client RPC port, published by the NM container so the host JVM can reach the AM.
  public static final int AM_CLIENT_PORT = 41000;

  private final Network network;
  private final GenericContainer<?> namenode;
  private final GenericContainer<?> datanode;
  private final GenericContainer<?> resourcemanager;
  private final GenericContainer<?> nodemanager;
  private final boolean fixedPorts;

  public TezYarnClusterContainer() {
    this(false);
  }

  public TezYarnClusterContainer(boolean fixedPorts) {
    this.fixedPorts = fixedPorts;
    network = Network.newNetwork();
    namenode = buildNameNode();
    datanode = buildDataNode();
    resourcemanager = buildResourceManager();
    nodemanager = buildNodeManager();
  }

  private GenericContainer<?> buildNameNode() {
    GenericContainer<?> c;
    if (fixedPorts) {
      c = new FixedHostPortGenericContainer<>(HADOOP_IMAGE)
          .withFixedExposedPort(NN_RPC_PORT, NN_RPC_PORT)
          .withFixedExposedPort(NN_HTTP_PORT, NN_HTTP_PORT);
    } else {
      c = new GenericContainer<>(HADOOP_IMAGE)
          .withExposedPorts(NN_HTTP_PORT, NN_RPC_PORT);
    }
    c.withNetwork(network)
     .withNetworkAliases("namenode")
     .withCommand("hdfs", "namenode")
     .withEnv(COMMON_ENV)
     .withEnv("ENSURE_NAMENODE_DIR", "/tmp/hadoop-hadoop/dfs/name")
     .waitingFor(Wait.forHttp("/").forPort(NN_HTTP_PORT).withStartupTimeout(STARTUP_TIMEOUT));
    return c;
  }

  private GenericContainer<?> buildDataNode() {
    GenericContainer<?> c;
    if (fixedPorts) {
      c = new FixedHostPortGenericContainer<>(HADOOP_IMAGE)
          .withFixedExposedPort(DN_XFER_PORT, DN_XFER_PORT)
          .withFixedExposedPort(DN_HTTP_PORT, DN_HTTP_PORT);
    } else {
      c = new GenericContainer<>(HADOOP_IMAGE);
    }
    c.withNetwork(network)
     .withNetworkAliases("datanode")
     .withCommand("hdfs", "datanode")
     .withEnv(COMMON_ENV);
    return c;
  }

  private GenericContainer<?> buildResourceManager() {
    GenericContainer<?> c;
    if (fixedPorts) {
      c = new FixedHostPortGenericContainer<>(HADOOP_IMAGE)
          .withFixedExposedPort(RM_RPC_PORT, RM_RPC_PORT)
          .withFixedExposedPort(RM_HTTP_PORT, RM_HTTP_PORT);
    } else {
      c = new GenericContainer<>(HADOOP_IMAGE)
          .withExposedPorts(RM_HTTP_PORT, RM_RPC_PORT);
    }
    c.withNetwork(network)
     .withNetworkAliases("resourcemanager")
     .withCommand("yarn", "resourcemanager")
     .withEnv(COMMON_ENV)
     .waitingFor(Wait.forHttp("/ws/v1/cluster/info").forPort(RM_HTTP_PORT).withStartupTimeout(STARTUP_TIMEOUT));
    return c;
  }

  private GenericContainer<?> buildNodeManager() {
    GenericContainer<?> c;
    if (fixedPorts) {
      // Fixed hostname "nodemanager" and published AM_CLIENT_PORT so host JVM can reach the Tez AM.
      c = new FixedHostPortGenericContainer<>(HADOOP_IMAGE)
          .withFixedExposedPort(AM_CLIENT_PORT, AM_CLIENT_PORT)
          .withCreateContainerCmdModifier(cmd -> cmd.withHostName("nodemanager"));
    } else {
      c = new GenericContainer<>(HADOOP_IMAGE);
    }
    c.withNetwork(network)
     .withNetworkAliases("nodemanager")
     .withCommand("yarn", "nodemanager")
     .withEnv(COMMON_ENV);
    return c;
  }

  public void start() {
    try {
      namenode.start();
      datanode.start();
      resourcemanager.start();
      nodemanager.start();
      // Avoid flakiness: wait until HDFS has left safemode before running HDFS operations.
      requireSuccess(namenode.execInContainer("hdfs", "dfsadmin", "-safemode", "wait"),
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

  private void verifyJava21InNodeManager() {
    try {
      GenericContainer.ExecResult r = nodemanager.execInContainer(
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

  public void stop() {
    nodemanager.stop();
    resourcemanager.stop();
    datanode.stop();
    namenode.stop();
    network.close();
  }

  public String getHdfsUri() {
    if (fixedPorts) {
      return "hdfs://namenode:" + NN_RPC_PORT;
    }
    return "hdfs://" + namenode.getHost() + ":" + namenode.getMappedPort(NN_RPC_PORT);
  }

  public String getResourceManagerAddress() {
    if (fixedPorts) {
      return "resourcemanager:" + RM_RPC_PORT;
    }
    return resourcemanager.getHost() + ":" + resourcemanager.getMappedPort(RM_RPC_PORT);
  }

  public String getResourceManagerWebAppAddress() {
    if (fixedPorts) {
      return "resourcemanager:" + RM_HTTP_PORT;
    }
    return resourcemanager.getHost() + ":" + resourcemanager.getMappedPort(RM_HTTP_PORT);
  }

  public String uploadJarToHdfs(Path localJarPath) throws IOException, InterruptedException {
    String fileName = localJarPath.getFileName().toString();
    String containerTmp = "/tmp/" + fileName;
    String hdfsDir = "/tmp/hive-tez-yarn-jars";
    String hdfsPath = hdfsDir + "/" + fileName;

    namenode.copyFileToContainer(MountableFile.forHostPath(localJarPath, 0644), containerTmp);

    GenericContainer.ExecResult mkdir = namenode.execInContainer("hdfs", "dfs", "-mkdir", "-p", hdfsDir);
    requireSuccess(mkdir, "hdfs dfs -mkdir -p " + hdfsDir);

    GenericContainer.ExecResult put = namenode.execInContainer("hdfs", "dfs", "-put", "-f", containerTmp, hdfsPath);
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

    GenericContainer.ExecResult mkdir = namenode.execInContainer("hdfs", "dfs", "-mkdir", "-p", hdfsDir);
    requireSuccess(mkdir, "hdfs dfs -mkdir -p " + hdfsDir);

    namenode.copyFileToContainer(MountableFile.forHostPath(tarball, 0644), containerTmp);

    GenericContainer.ExecResult put = namenode.execInContainer(
        "hdfs", "dfs", "-put", "-f", containerTmp, hdfsPath);
    requireSuccess(put, "hdfs dfs -put -f " + containerTmp + " " + hdfsPath);

    // The "#tez" fragment sets the YARN container link name for this LocalResource.
    // YARN extracts the archive to $PWD/tez/ inside each container.
    return "hdfs://namenode:" + NN_RPC_PORT + hdfsPath + "#tez";
  }

  GenericContainer<?> namenodeContainer() {
    return namenode;
  }

  GenericContainer<?> resourceManagerContainer() {
    return resourcemanager;
  }

  GenericContainer<?> nodeManagerContainer() {
    return nodemanager;
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
                GenericContainer.ExecResult result = resourcemanager.execInContainer("yarn", "node", "-list");
                String out = result.getStdout();
                lastOut[0] = out;
                return out.contains("Total Nodes:") && !out.contains("Total Nodes:0");
              });
    } catch (ConditionTimeoutException e) {
      throw new IllegalStateException(
              "NodeManager did not register with ResourceManager within 2 minutes. Last output:\n" + lastOut[0], e);
    }
  }

  private static void requireSuccess(GenericContainer.ExecResult result, String cmd) {
    if (result.getExitCode() != 0) {
      throw new IllegalStateException("Command failed (" + cmd + ")\nstdout:\n"
          + result.getStdout() + "\nstderr:\n" + result.getStderr());
    }
  }

  private static Map<String, String> loadCommonEnv() {
    Map<String, String> env = new LinkedHashMap<>();
    String basedir = System.getProperty("basedir", ".");
    Path configPath = Paths.get(basedir, "src/test/docker/hadoop-yarn/config");
    try (Stream<String> lines = Files.lines(configPath, StandardCharsets.UTF_8)) {
      lines.map(String::trim)
          .filter(l -> !l.isEmpty())
          .filter(l -> !l.startsWith("#"))
          .forEach(l -> {
            int idx = l.indexOf('=');
            if (idx < 0) {
              throw new IllegalArgumentException("Invalid config line (missing '='): " + l);
            }
            String key = l.substring(0, idx).trim();
            String value = l.substring(idx + 1);
            env.put(key, value);
          });
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load Hadoop docker config from " + configPath, e);
    }
    return env;
  }

  private static String buildHadoopImage() {
    String basedir = System.getProperty("basedir", ".");
    Path dockerfile = Paths.get(basedir, "src/test/docker/hadoop-yarn/Dockerfile");
    return new ImageFromDockerfile("hive-it-hadoop-jdk21", false)
        .withDockerfile(dockerfile)
        .get();
  }
}
