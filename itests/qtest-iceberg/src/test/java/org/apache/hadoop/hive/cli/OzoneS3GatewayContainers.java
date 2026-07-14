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

package org.apache.hadoop.hive.cli;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;

/**
 * Minimal Apache Ozone cluster with S3 Gateway for q-tests that need an S3-compatible endpoint.
 *
 * <p>Layout matches {@code packaging/src/docker/storage/ozone/docker-compose.yml}: scm, om, datanode,
 * and s3g on a shared Docker network. S3G is reachable as {@link #S3_DOCKER_ALIAS}:{@link #S3G_PORT}
 * from other containers (for example Gravitino) and via the published host port from the test JVM.
 */
public final class OzoneS3GatewayContainers {

  private static final Logger LOG = LoggerFactory.getLogger(OzoneS3GatewayContainers.class);
  private static final DockerImageName OZONE_IMAGE = DockerImageName.parse("apache/ozone:2.1.0");

  private static final int SCM_PORT = 9876;
  private static final int OM_PORT = 9874;
  /** Ozone 2.1.0 HDDS datanode client RPC port ({@code hdds.datanode.client.port}). */
  private static final int DATANODE_CLIENT_PORT = 19864;

  private static final String S3_BUCKET_READY_MARKER = "OZONE_S3_BUCKET_READY";

  public static final int S3G_PORT = 9878;
  public static final String S3_DOCKER_ALIAS = "s3.ozone";
  public static final String S3_DOCKER_ENDPOINT = String.format("http://%s:%d", S3_DOCKER_ALIAS, S3G_PORT);

  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(5);

  private GenericContainer<?> scm;
  private GenericContainer<?> om;
  private GenericContainer<?> datanode;
  private GenericContainer<?> s3g;

  /**
   * Starts scm → om → datanode → s3g on {@code network} and creates {@code bucketName} under the
   * default S3 volume ({@code /s3v}).
   */
  @SuppressWarnings("resource")
  public void start(Network network, String bucketName) {
    Map<String, String> commonEnv = commonEnv();
    scm = startScm(network, commonEnv);
    om = startOm(network, commonEnv);
    datanode = startDatanode(network, commonEnv);
    s3g = startS3g(network, commonEnv, bucketName);
    LOG.info("Ozone S3G ready at {} (host {}:{})", S3_DOCKER_ENDPOINT, s3g.getHost(), s3g.getMappedPort(S3G_PORT));
  }

  public void stop() {
    stopQuietly(s3g);
    stopQuietly(datanode);
    stopQuietly(om);
    stopQuietly(scm);
    s3g = null;
    datanode = null;
    om = null;
    scm = null;
  }

  public String getHost() {
    return s3g.getHost();
  }

  public int getMappedPort() {
    return s3g.getMappedPort(S3G_PORT);
  }

  private static Map<String, String> commonEnv() {
    Map<String, String> env = new HashMap<>();
    env.put("OZONE-SITE.XML_hdds.datanode.dir", "/data/hdds");
    env.put("OZONE-SITE.XML_ozone.metadata.dirs", "/data/metadata");
    env.put("OZONE-SITE.XML_ozone.om.address", "om");
    env.put("OZONE-SITE.XML_ozone.om.http-address", "om:" + OM_PORT);
    env.put("OZONE-SITE.XML_ozone.replication", "1");
    env.put("OZONE-SITE.XML_ozone.scm.block.client.address", "scm");
    env.put("OZONE-SITE.XML_ozone.scm.client.address", "scm");
    env.put("OZONE-SITE.XML_ozone.scm.datanode.id.dir", "/data/metadata");
    env.put("OZONE-SITE.XML_ozone.scm.names", "scm");
    env.put("OZONE-SITE.XML_hdds.scm.safemode.min.datanode", "1");
    env.put("OZONE-SITE.XML_hdds.scm.safemode.healthy.pipeline.pct", "0");
    env.put("OZONE-SITE.XML_hdds.scm.safemode.enabled", "false");
    // Path-style only: host JVM and Gravitino use http://host:9878/bucket/... . Setting
    // ozone.s3g.domain.name would require virtual-host Host headers (e.g. bucket.s3.ozone).
    // Keep CI/docker-desktop friendly (see itests/test-docker/helm/ozone/values.yaml).
    env.put("OZONE-SITE.XML_hdds.datanode.volume.min.free.space", "256MB");
    env.put("OZONE-SITE.XML_ozone.scm.container.size", "128MB");
    env.put("OZONE-SITE.XML_ozone.scm.block.size", "32MB");
    env.put("no_proxy", "om,scm,s3g,localhost,127.0.0.1");
    return env;
  }

  @SuppressWarnings("resource")
  private static GenericContainer<?> startScm(Network network, Map<String, String> commonEnv) {
    GenericContainer<?> container = new GenericContainer<>(OZONE_IMAGE)
        .withNetwork(network)
        .withNetworkAliases("scm")
        .withExposedPorts(SCM_PORT)
        .withEnv(commonEnv)
        .withEnv("ENSURE_SCM_INITIALIZED", "/data/metadata/scm/current/VERSION")
        .withCommand("ozone", "scm")
        .waitingFor(Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT))
        .withLogConsumer(outputFrame -> LOG.debug("[ozone-scm] {}", outputFrame.getUtf8String().trim()));
    container.start();
    return container;
  }

  @SuppressWarnings("resource")
  private static GenericContainer<?> startOm(Network network, Map<String, String> commonEnv) {
    GenericContainer<?> container = new GenericContainer<>(OZONE_IMAGE)
        .withNetwork(network)
        .withNetworkAliases("om")
        .withExposedPorts(OM_PORT)
        .withEnv(commonEnv)
        .withEnv("CORE-SITE.XML_hadoop.proxyuser.hadoop.hosts", "*")
        .withEnv("CORE-SITE.XML_hadoop.proxyuser.hadoop.groups", "*")
        .withEnv("ENSURE_OM_INITIALIZED", "/data/metadata/om/current/VERSION")
        .withEnv("WAITFOR", "scm:" + SCM_PORT)
        .withCommand("ozone", "om")
        .waitingFor(Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT))
        .withLogConsumer(outputFrame -> LOG.debug("[ozone-om] {}", outputFrame.getUtf8String().trim()));
    container.start();
    return container;
  }

  @SuppressWarnings("resource")
  private static GenericContainer<?> startDatanode(Network network, Map<String, String> commonEnv) {
    GenericContainer<?> container = new GenericContainer<>(OZONE_IMAGE)
        .withNetwork(network)
        .withExposedPorts(DATANODE_CLIENT_PORT)
        .withEnv(commonEnv)
        .withCommand("ozone", "datanode")
        .waitingFor(Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT))
        .withLogConsumer(outputFrame -> LOG.debug("[ozone-dn] {}", outputFrame.getUtf8String().trim()));
    container.start();
    return container;
  }

  @SuppressWarnings("resource")
  private static GenericContainer<?> startS3g(Network network, Map<String, String> commonEnv, String bucketName) {
    String bootstrap = ""
        + "set -e\n"
        + "ozone s3g &\n"
        + "s3g_pid=$!\n"
        + "until ozone sh volume list >/dev/null 2>&1; do echo 'waiting for OM...' && sleep 1; done\n"
        + "ozone sh volume create /s3v || true\n"
        + "ozone sh bucket delete /s3v/" + bucketName + " || true\n"
        + "ozone sh bucket create /s3v/" + bucketName + "\n"
        + "echo " + S3_BUCKET_READY_MARKER + "\n"
        + "wait \"$s3g_pid\"\n";
    GenericContainer<?> container = new GenericContainer<>(OZONE_IMAGE)
        .withNetwork(network)
        .withNetworkAliases(S3_DOCKER_ALIAS)
        .withExposedPorts(S3G_PORT)
        .withEnv(commonEnv)
        .withEnv("WAITFOR", "om:" + OM_PORT)
        .withCommand("sh", "-c", bootstrap)
        .waitingFor(new WaitAllStrategy()
            .withStrategy(Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT))
            .withStrategy(Wait.forLogMessage(".*" + S3_BUCKET_READY_MARKER + ".*\\n", 1)
                .withStartupTimeout(STARTUP_TIMEOUT)))
        .withLogConsumer(outputFrame -> LOG.info("[ozone-s3g] {}", outputFrame.getUtf8String().trim()));
    container.start();
    return container;
  }

  private static void stopQuietly(GenericContainer<?> container) {
    if (container != null) {
      container.stop();
    }
  }
}
