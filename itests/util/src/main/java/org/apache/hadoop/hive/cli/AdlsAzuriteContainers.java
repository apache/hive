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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Locale;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Azurite blob emulator for q-tests that need Azure WASB/WASBS with vended account keys.
 *
 * <p>Azurite does not implement the ADLS Gen2 DFS endpoint ({@code abfss://}), so integration tests
 * use {@code wasbs://} against the blob endpoint. Gravitino and Hive session wiring must use a
 * host-reachable blob endpoint override, analogous to {@link OzoneS3GatewayContainers} for S3.
 */
public final class AdlsAzuriteContainers {

  private static final Logger LOG = LoggerFactory.getLogger(AdlsAzuriteContainers.class);
  private static final DockerImageName AZURITE_IMAGE =
      DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:3.35.0");
  private static final DateTimeFormatter RFC_1123 =
      DateTimeFormatter.RFC_1123_DATE_TIME.withLocale(Locale.US).withZone(ZoneOffset.UTC);
  private static final String BLOB_API_VERSION = "2021-04-10";

  /** Well-known Azurite account and key (safe for local emulation only). */
  public static final String ACCOUNT_NAME = "devstoreaccount1";
  public static final String ACCOUNT_KEY =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

  public static final int BLOB_PORT = 10000;
  /** Gravitino Iceberg REST HTTP port when the server shares Azurite's network namespace. */
  public static final int GRAVITINO_HTTP_PORT = 9001;
  public static final String AZURITE_DOCKER_ALIAS = "azurite";
  public static final String AZURITE_BLOB_DOCKER_ENDPOINT =
      String.format("http://%s:%d", AZURITE_DOCKER_ALIAS, BLOB_PORT);

  /**
   * WASB authority host for {@code devstoreaccount1}; must match
   * {@link #STORAGE_EMULATOR_ACCOUNT_NAME} for Hadoop emulator mode.
   */
  public static final String STORAGE_EMULATOR_ACCOUNT_NAME =
      ACCOUNT_NAME + ".blob.core.windows.net";

  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);

  private static final class AzuriteContainer extends GenericContainer<AzuriteContainer> {
    AzuriteContainer() {
      super(AZURITE_IMAGE);
      // WASB emulator client code uses 127.0.0.1:10000 on the host JVM.
      addFixedExposedPort(BLOB_PORT, BLOB_PORT);
    }
  }

  private GenericContainer<?> azurite;

  /** Starts Azurite on {@code network} and creates {@code containerName} if absent. */
  @SuppressWarnings("resource")
  public void start(Network network, String containerName) throws Exception {
    azurite = new AzuriteContainer()
        .withNetwork(network)
        .withNetworkAliases(AZURITE_DOCKER_ALIAS)
        .withExposedPorts(BLOB_PORT, GRAVITINO_HTTP_PORT)
        .withCommand("azurite", "--blobHost", "0.0.0.0", "--blobPort", String.valueOf(BLOB_PORT))
        .waitingFor(Wait.forListeningPorts(BLOB_PORT).withStartupTimeout(STARTUP_TIMEOUT))
        .withLogConsumer(outputFrame -> LOG.debug("[azurite] {}", outputFrame.getUtf8String().trim()));
    azurite.start();
    createBlobContainerIfMissing(containerName);
    LOG.info("Azurite blob ready at {} (host {}:{})",
        AZURITE_BLOB_DOCKER_ENDPOINT, azurite.getHost(), azurite.getMappedPort(BLOB_PORT));
  }

  public void stop() {
    if (azurite != null) {
      azurite.stop();
      azurite = null;
    }
  }

  public String getHost() {
    return azurite.getHost();
  }

  public int getMappedPort() {
    return azurite.getMappedPort(BLOB_PORT);
  }

  /** Host-reachable blob endpoint for WASB/WASBS ({@code http://host:port}). */
  public String getHostBlobEndpoint() {
    return String.format("http://%s:%d", getHost(), getMappedPort());
  }

  /** Docker container id; used to share Azurite's network namespace with Gravitino. */
  public String getContainerId() {
    return azurite.getContainerId();
  }

  public int getMappedGravitinoPort() {
    return azurite.getMappedPort(GRAVITINO_HTTP_PORT);
  }

  /**
   * Creates the blob container via the Azure Blob REST API and Shared Key auth.
   *
   * <p>Uses JDK {@link HttpClient} instead of the Azure SDK so hive-it-util does not pull
   * {@code azure-core-http-netty} and its platform-specific {@code netty-tcnative} artifacts,
   * which are missing from the Jenkins Artifactory mirror.
   */
  private void createBlobContainerIfMissing(String containerName) throws IOException, InterruptedException {
    String xMsDate = RFC_1123.format(ZonedDateTime.now(ZoneOffset.UTC));
    URI uri = URI.create(String.format("%s/%s/%s?restype=container",
        getHostBlobEndpoint(), ACCOUNT_NAME, containerName));

    // Match Azure SDK SharedKey signing (see StorageSharedKeyCredential.buildStringToSign):
    // - Content-Length "0" is treated as empty in the string-to-sign
    // - Path-style Azurite URLs canonicalize as /{account}{path} where path already contains /{account}/...
    String canonicalizedHeaders = "x-ms-date:" + xMsDate + "\n" + "x-ms-version:" + BLOB_API_VERSION;
    String canonicalizedResource = "/" + ACCOUNT_NAME + uri.getPath() + "\nrestype:container";
    String stringToSign = String.join("\n",
        "PUT",
        "", "", "", "", "", "",
        "", "", "", "", "",
        canonicalizedHeaders,
        canonicalizedResource);
    String authorization = "SharedKey " + ACCOUNT_NAME + ":" + signStringToSign(stringToSign);

    HttpRequest request = HttpRequest.newBuilder(uri)
        .PUT(HttpRequest.BodyPublishers.noBody())
        .header("x-ms-date", xMsDate)
        .header("x-ms-version", BLOB_API_VERSION)
        .header("Authorization", authorization)
        .build();

    HttpResponse<Void> response =
        HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding());
    int status = response.statusCode();
    if (status == 201 || status == 409) {
      return;
    }
    throw new IOException(String.format(
        "Failed to create Azurite container '%s' at %s: HTTP %d",
        containerName, uri, status));
  }

  private static String signStringToSign(String stringToSign) throws IOException {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(Base64.getDecoder().decode(ACCOUNT_KEY), "HmacSHA256"));
      return Base64.getEncoder().encodeToString(mac.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new IOException("Failed to sign Azurite REST request", e);
    }
  }
}
