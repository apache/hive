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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * {@code fake-gcs-server} emulator for q-tests that need {@code gs://} with vended OAuth tokens.
 *
 * <p>Gravitino (Docker) and Hive (host JVM) both reach fake-gcs via the published host port using
 * {@link #getHostGatewayEndpoint()} ({@code host.testcontainers.internal}) for resumable upload
 * Location headers and for GCS clients in both Gravitino (Docker) and Hive/LLAP (host JVM). Host
 * tests must map {@link #HOST_GATEWAY} to {@code 127.0.0.1} via {@code -Djdk.net.hosts.file}.
 */
public final class GcsFakeServerContainers {

  private static final Logger LOG = LoggerFactory.getLogger(GcsFakeServerContainers.class);
  private static final DockerImageName FAKE_GCS_IMAGE =
      DockerImageName.parse("fsouza/fake-gcs-server:1.47.4");

  public static final String PROJECT_ID = "test-project";
  public static final int PORT = 4443;
  public static final String DOCKER_ALIAS = "gcs.fake";
  public static final String DOCKER_ENDPOINT =
      String.format("http://%s:%d", DOCKER_ALIAS, PORT);
  /** Host gateway hostname reachable from containers via {@code host-gateway} extra_hosts. */
  public static final String HOST_GATEWAY = "host.testcontainers.internal";

  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);

  private GenericContainer<?> fakeGcs;

  /** Starts fake-gcs-server on {@code network} and creates {@code bucketName} if absent. */
  @SuppressWarnings("resource")
  public void start(Network network, String bucketName) throws Exception {
    fakeGcs = new GenericContainer<>(FAKE_GCS_IMAGE)
        .withNetwork(network)
        .withNetworkAliases(DOCKER_ALIAS)
        .withExposedPorts(PORT)
        .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
            "/bin/fake-gcs-server", "-scheme", "http", "-port", String.valueOf(PORT)))
        .waitingFor(Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT))
        .withLogConsumer(outputFrame -> LOG.debug("[fake-gcs] {}", outputFrame.getUtf8String().trim()));
    fakeGcs.start();
    createBucketIfMissing(bucketName);
    configureExternalUrl(getHostGatewayEndpoint());
    LOG.info("fake-gcs-server ready at {} (host {}:{}, externalUrl {})",
        DOCKER_ENDPOINT, fakeGcs.getHost(), fakeGcs.getMappedPort(PORT), getHostGatewayEndpoint());
  }

  public void stop() {
    if (fakeGcs != null) {
      fakeGcs.stop();
      fakeGcs = null;
    }
  }

  public String getHost() {
    return fakeGcs.getHost();
  }

  public int getMappedPort() {
    return fakeGcs.getMappedPort(PORT);
  }

  /** Host-reachable GCS API root ({@code http://host:port}). */
  public String getHostEndpoint() {
    return String.format("http://%s:%d", getHost(), getMappedPort());
  }

  /**
   * GCS API root reachable from Docker containers (via {@code host-gateway}) and from the host JVM
   * for resumable upload Location headers.
   */
  public String getHostGatewayEndpoint() {
    return String.format("http://%s:%d", HOST_GATEWAY, getMappedPort());
  }

  /**
   * fake-gcs-server must know the externally reachable URL for resumable uploads; see
   * <a href="https://github.com/fsouza/fake-gcs-server/blob/master/examples/java/README.md">
   * fake-gcs-server Java example</a>.
   */
  public void configureExternalUrl(String externalUrl) throws IOException, InterruptedException {
    URI uri = URI.create(getHostEndpoint() + "/_internal/config");
    String body = String.format("{\"externalUrl\":\"%s\"}", externalUrl);
    HttpRequest request = HttpRequest.newBuilder(uri)
        .header("Content-Type", "application/json")
        .PUT(HttpRequest.BodyPublishers.ofString(body))
        .build();
    HttpResponse<Void> response =
        HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding());
    if (response.statusCode() != 200) {
      throw new IOException(String.format(
          "Failed to configure fake-gcs-server externalUrl=%s at %s: HTTP %d",
          externalUrl, uri, response.statusCode()));
    }
  }

  private void createBucketIfMissing(String bucketName) throws IOException, InterruptedException {
    URI uri = URI.create(String.format(
        "%s/storage/v1/b?project=%s", getHostEndpoint(), PROJECT_ID));
    String body = String.format("{\"name\":\"%s\"}", bucketName);
    HttpRequest request = HttpRequest.newBuilder(uri)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build();
    HttpResponse<Void> response =
        HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding());
    int status = response.statusCode();
    if (status == 200 || status == 409) {
      return;
    }
    throw new IOException(String.format(
        "Failed to create fake-gcs bucket '%s' at %s: HTTP %d", bucketName, uri, status));
  }
}
