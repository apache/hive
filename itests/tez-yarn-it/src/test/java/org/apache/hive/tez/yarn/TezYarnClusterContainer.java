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
package org.apache.hive.tez.yarn;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Manages a pseudo-distributed HDFS+YARN cluster (NameNode, DataNode,
 * ResourceManager, NodeManager) using a custom image built from
 * apache/hadoop:3.4.2 (see {@code src/test/docker/hadoop-yarn/Dockerfile}).
 * The image adds a Java 21 runtime at {@link #CONTAINER_JAVA_21_HOME} so that
 * Tez AM/task containers (which load hive-exec compiled for Java 21) can run,
 * while the HDFS/YARN daemons keep the image default Java 8. Intended for
 * integration tests that need real YARN container scheduling and HDFS-backed
 * resource localization.
 *
 * <p>All four containers share a single Docker network and are configured via
 * the same environment-variable convention used by the official image (the
 * env var key encodes the target XML file and property name, e.g.
 * {@code CORE-SITE.XML_fs.defaultFS=hdfs://namenode}).
 *
 * <p>Two modes are supported:
 * <ul>
 *   <li><b>Dynamic-port mode</b> (default, {@code new TezYarnClusterContainer()}):
 *       Testcontainers assigns random host ports. Use for tests where only the
 *       host JVM accesses HDFS/YARN (e.g. JDBC connectivity smoke tests).</li>
 *   <li><b>Fixed-port mode</b> ({@code new TezYarnClusterContainer(true)}):
 *       Host ports match container ports (NN: 8020/9870, RM: 8032/8088).
 *       The docker-network hostnames {@code namenode} and {@code resourcemanager}
 *       must resolve to {@code 127.0.0.1} on the host, e.g. via
 *       {@code -Djdk.net.hosts.file=custom_hosts_file}. Use for tests where
 *       YARN containers also need to reach HDFS by the same URI as the host JVM
 *       (real Tez-on-YARN localization tests).</li>
 * </ul>
 */
public class TezYarnClusterContainer {

  /**
   * Path (inside the containers) to the Java 21 runtime added by the custom Dockerfile.
   * The HDFS/YARN daemons keep the image default Java 8, but the Tez AM/task containers
   * (which load hive-exec compiled for Java 21) must be pointed here via {@code JAVA_HOME}.
   */
  public static final String CONTAINER_JAVA_21_HOME = "/opt/jdk21";

  // Custom image: apache/hadoop:3.4.2 + Corretto 21 at /opt/jdk21 (see src/test/docker/hadoop-yarn/Dockerfile).
  private static final String HADOOP_IMAGE = buildHadoopImage();
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);
  private static final Map<String, String> COMMON_ENV = loadCommonEnv();

  // Fixed host ports used when fixedPorts=true. Must match container-internal ports.
  private static final int NN_RPC_PORT  = 8020;
  private static final int NN_HTTP_PORT = 9870;
  private static final int RM_RPC_PORT  = 8032;
  private static final int RM_HTTP_PORT = 8088;
  // DataNode ports (Hadoop 3.x defaults). Host JVM must reach 9866 for HDFS writes.
  private static final int DN_HTTP_PORT = 9864;
  private static final int DN_XFER_PORT = 9866;

  private final Network network;
  private final GenericContainer<?> namenode;
  private final GenericContainer<?> datanode;
  private final GenericContainer<?> resourcemanager;
  private final GenericContainer<?> nodemanager;
  private final boolean fixedPorts;

  /** Dynamic-port mode; backward-compatible with existing tests. */
  public TezYarnClusterContainer() {
    this(false);
  }

  /**
   * @param fixedPorts when {@code true}, maps host ports 8020/9870/8032/8088
   *                   to the matching container ports so that the same hostnames
   *                   and ports work from both the host JVM and inside containers.
   */
  public TezYarnClusterContainer(boolean fixedPorts) {
    this.fixedPorts = fixedPorts;
    network = Network.newNetwork();

    if (fixedPorts) {
      namenode = new FixedHostPortGenericContainer<>(HADOOP_IMAGE)
          .withFixedExposedPort(NN_RPC_PORT, NN_RPC_PORT)
          .withFixedExposedPort(NN_HTTP_PORT, NN_HTTP_PORT)
          .withNetwork(network)
          .withNetworkAliases("namenode")
          .withCommand("hdfs", "namenode")
          .withEnv(COMMON_ENV)
          .withEnv("ENSURE_NAMENODE_DIR", "/tmp/hadoop-hadoop/dfs/name")
          .waitingFor(Wait.forHttp("/").forPort(NN_HTTP_PORT).withStartupTimeout(STARTUP_TIMEOUT));

      resourcemanager = new FixedHostPortGenericContainer<>(HADOOP_IMAGE)
          .withFixedExposedPort(RM_RPC_PORT, RM_RPC_PORT)
          .withFixedExposedPort(RM_HTTP_PORT, RM_HTTP_PORT)
          .withNetwork(network)
          .withNetworkAliases("resourcemanager")
          .withCommand("yarn", "resourcemanager")
          .withEnv(COMMON_ENV)
          .waitingFor(Wait.forHttp("/ws/v1/cluster/info").forPort(RM_HTTP_PORT).withStartupTimeout(STARTUP_TIMEOUT));

      datanode = new FixedHostPortGenericContainer<>(HADOOP_IMAGE)
          .withFixedExposedPort(DN_XFER_PORT, DN_XFER_PORT)
          .withFixedExposedPort(DN_HTTP_PORT, DN_HTTP_PORT)
          .withNetwork(network)
          .withNetworkAliases("datanode")
          .withCommand("hdfs", "datanode")
          .withEnv(COMMON_ENV);
    } else {
      namenode = new GenericContainer<>(HADOOP_IMAGE)
          .withNetwork(network)
          .withNetworkAliases("namenode")
          .withCommand("hdfs", "namenode")
          .withEnv(COMMON_ENV)
          .withEnv("ENSURE_NAMENODE_DIR", "/tmp/hadoop-hadoop/dfs/name")
          .withExposedPorts(NN_HTTP_PORT, NN_RPC_PORT)
          .waitingFor(Wait.forHttp("/").forPort(NN_HTTP_PORT).withStartupTimeout(STARTUP_TIMEOUT));

      resourcemanager = new GenericContainer<>(HADOOP_IMAGE)
          .withNetwork(network)
          .withNetworkAliases("resourcemanager")
          .withCommand("yarn", "resourcemanager")
          .withEnv(COMMON_ENV)
          .withExposedPorts(RM_HTTP_PORT, RM_RPC_PORT)
          .waitingFor(Wait.forHttp("/ws/v1/cluster/info").forPort(RM_HTTP_PORT).withStartupTimeout(STARTUP_TIMEOUT));

      datanode = new GenericContainer<>(HADOOP_IMAGE)
          .withNetwork(network)
          .withNetworkAliases("datanode")
          .withCommand("hdfs", "datanode")
          .withEnv(COMMON_ENV);
    }

    nodemanager = new GenericContainer<>(HADOOP_IMAGE)
        .withNetwork(network)
        .withNetworkAliases("nodemanager")
        .withCommand("yarn", "nodemanager")
        .withEnv(COMMON_ENV);
  }

  /**
   * Starts NN → DN → RM → NM in order and then polls until at least one
   * NodeManager has registered with the ResourceManager.
   */
  public void start() {
    namenode.start();
    datanode.start();
    resourcemanager.start();
    nodemanager.start();
    waitForNodeManagerRegistration();
  }

  public void stop() {
    nodemanager.stop();
    resourcemanager.stop();
    datanode.stop();
    namenode.stop();
    network.close();
  }

  /**
   * Returns the HDFS URI for use in {@code HiveConf} / {@code Configuration}.
   *
   * <p>In fixed-port mode, returns {@code hdfs://namenode:8020} — a URI that
   * works from both the host JVM (via the custom hosts file) and from inside
   * YARN containers (via the docker network alias). In dynamic-port mode,
   * returns {@code hdfs://localhost:<mappedPort>}.
   */
  public String getHdfsUri() {
    if (fixedPorts) {
      return "hdfs://namenode:" + NN_RPC_PORT;
    }
    return "hdfs://" + namenode.getHost() + ":" + namenode.getMappedPort(NN_RPC_PORT);
  }

  /**
   * Returns {@code host:port} of the ResourceManager RPC address, suitable for
   * {@code yarn.resourcemanager.address}.
   */
  public String getResourceManagerAddress() {
    if (fixedPorts) {
      return "resourcemanager:" + RM_RPC_PORT;
    }
    return resourcemanager.getHost() + ":" + resourcemanager.getMappedPort(RM_RPC_PORT);
  }

  /**
   * Returns {@code host:port} of the ResourceManager web-application address,
   * suitable for {@code yarn.resourcemanager.webapp.address}.
   */
  public String getResourceManagerWebAppAddress() {
    if (fixedPorts) {
      return "resourcemanager:" + RM_HTTP_PORT;
    }
    return resourcemanager.getHost() + ":" + resourcemanager.getMappedPort(RM_HTTP_PORT);
  }

  /**
   * Copies a local jar into the HDFS directory {@code /tmp/hive-29483-jars} and returns
   * the resulting HDFS path. Used to seed localized resources in integration tests.
   */
  public String uploadJarToHdfs(Path localJarPath) throws IOException, InterruptedException {
    String fileName = localJarPath.getFileName().toString();
    String containerTmp = "/tmp/" + fileName;
    String hdfsDir = "/tmp/hive-29483-jars";
    String hdfsPath = hdfsDir + "/" + fileName;

    // Ensure the file is readable by the container's default user (hadoop).
    namenode.copyFileToContainer(MountableFile.forHostPath(localJarPath, 0644), containerTmp);

    GenericContainer.ExecResult mkdir = namenode.execInContainer("hdfs", "dfs", "-mkdir", "-p", hdfsDir);
    requireSuccess(mkdir, "hdfs dfs -mkdir -p " + hdfsDir);

    GenericContainer.ExecResult put = namenode.execInContainer("hdfs", "dfs", "-put", "-f", containerTmp, hdfsPath);
    requireSuccess(put, "hdfs dfs -put -f " + containerTmp + " " + hdfsPath);

    return hdfsPath;
  }

  /**
   * Discovers Tez jars on the test JVM classpath, uploads them to
   * {@code /tmp/hive-29483/tez-libs/} on HDFS, and returns a comma-separated
   * list of fully-qualified HDFS URIs suitable for the {@code tez.lib.uris}
   * configuration property.
   *
   * <p>Only available in fixed-port mode; the returned URIs use the stable
   * hostname {@code namenode} which must be resolvable from both the host JVM
   * and YARN containers.
   *
   * @throws IllegalStateException if no Tez jars are found on the classpath
   */
  public String uploadTezLibsToHdfs() throws IOException, InterruptedException {
    String hdfsDir = "/tmp/hive-29483/tez-libs";
    GenericContainer.ExecResult mkdir = namenode.execInContainer("hdfs", "dfs", "-mkdir", "-p", hdfsDir);
    requireSuccess(mkdir, "hdfs dfs -mkdir -p " + hdfsDir);

    Set<Path> tezJars = findTezJarsFromClasspath();
    List<String> hdfsUris = new ArrayList<>();
    for (Path jarPath : tezJars) {
      String jarName = jarPath.getFileName().toString();
      String containerTmp = "/tmp/" + jarName;
      String hdfsPath = hdfsDir + "/" + jarName;

      namenode.copyFileToContainer(MountableFile.forHostPath(jarPath, 0644), containerTmp);
      GenericContainer.ExecResult put = namenode.execInContainer(
          "hdfs", "dfs", "-put", "-f", containerTmp, hdfsPath);
      requireSuccess(put, "hdfs dfs -put -f " + containerTmp + " " + hdfsPath);

      hdfsUris.add("hdfs://namenode:" + NN_RPC_PORT + hdfsPath);
    }

    if (hdfsUris.isEmpty()) {
      throw new IllegalStateException(
          "No Tez jars (filenames containing 'tez') were found on the test classpath. "
          + "Ensure tez-api, tez-dag, tez-runtime-library, etc. are transitive dependencies.");
    }
    return String.join(",", hdfsUris);
  }

  /**
   * Discovers Tez jars from the running JVM's classpath using two complementary strategies:
   * <ol>
   *   <li>Reflection on known Tez probe classes — works regardless of how Surefire
   *       passes the classpath (direct {@code -cp} or manifest-only argfile JAR).</li>
   *   <li>Scanning {@link ManagementFactory#getRuntimeMXBean() RuntimeMXBean#getClassPath()}
   *       for entries whose filename contains {@code "tez"} — a safety net when the
   *       reflection approach misses a jar (e.g. classes not loaded yet).</li>
   * </ol>
   * Only regular {@code .jar} files whose filename contains {@code "tez"} and does
   * not end with {@code "-tests.jar"} are returned.
   */
  private static Set<Path> findTezJarsFromClasspath() {
    Set<Path> jars = new LinkedHashSet<>();

    // Strategy 1: reflect on known Tez probe classes to get their source jar.
    // Use Class.forName to avoid compile-time imports of Tez internal modules.
    String[] probeClassNames = {
        "org.apache.tez.dag.api.TezConfiguration",         // tez-api
        "org.apache.tez.common.TezConverterUtils",         // tez-common
        "org.apache.tez.dag.app.DAGAppMaster",             // tez-dag (the Tez AM)
        "org.apache.tez.mapreduce.hadoop.MRHelpers",       // tez-mapreduce
        "org.apache.tez.runtime.LogicalIOProcessorRuntimeTask", // tez-runtime-internals
        "org.apache.tez.runtime.library.api.KeyValueReader"    // tez-runtime-library
    };
    for (String className : probeClassNames) {
      try {
        Class<?> cls = Class.forName(className);
        CodeSource cs = cls.getProtectionDomain().getCodeSource();
        if (cs != null && cs.getLocation() != null) {
          String path = cs.getLocation().getPath();
          if (path.endsWith(".jar")) {
            Path p = Paths.get(path);
            String name = p.getFileName().toString();
            if (name.contains("tez") && !name.endsWith("-tests.jar") && Files.isRegularFile(p)) {
              jars.add(p);
            }
          }
        }
      } catch (ClassNotFoundException | SecurityException ignored) {
        // Class not on classpath; skip.
      }
    }

    // Strategy 2: scan the JVM classpath string for jars with "tez" in the name.
    String cp = ManagementFactory.getRuntimeMXBean().getClassPath();
    for (String entry : cp.split(File.pathSeparator)) {
      if (!entry.endsWith(".jar")) {
        continue;
      }
      Path p = Paths.get(entry);
      String name = p.getFileName().toString();
      if (name.contains("tez") && !name.endsWith("-tests.jar") && Files.isRegularFile(p)) {
        jars.add(p);
      }
    }

    return jars;
  }

  // Package-private: only TezYarnClusterContainerTest should need direct exec access.
  GenericContainer<?> namenodeContainer() {
    return namenode;
  }

  GenericContainer<?> resourceManagerContainer() {
    return resourcemanager;
  }

  private void waitForNodeManagerRegistration() {
    long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
    while (System.currentTimeMillis() < deadline) {
      try {
        GenericContainer.ExecResult result = resourcemanager.execInContainer("yarn", "node", "-list");
        String out = result.getStdout();
        if (out.contains("Total Nodes:") && !out.contains("Total Nodes:0")) {
          return;
        }
        Thread.sleep(3000);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while waiting for NodeManager registration");
      } catch (Exception ignored) {
        try { Thread.sleep(3000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
      }
    }
    throw new IllegalStateException("NodeManager did not register with ResourceManager within 2 minutes");
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

  /**
   * Builds (or reuses a cached build of) the custom Hadoop image that layers a Java 21
   * runtime on top of apache/hadoop:3.4.2. Testcontainers caches the build by content
   * hash, so repeated runs reuse the previously built image.
   *
   * @return the resulting local image tag
   */
  private static String buildHadoopImage() {
    String basedir = System.getProperty("basedir", ".");
    Path dockerfile = Paths.get(basedir, "src/test/docker/hadoop-yarn/Dockerfile");
    return new ImageFromDockerfile("hive-it-hadoop-jdk21", false)
        .withDockerfile(dockerfile)
        .get();
  }
}
