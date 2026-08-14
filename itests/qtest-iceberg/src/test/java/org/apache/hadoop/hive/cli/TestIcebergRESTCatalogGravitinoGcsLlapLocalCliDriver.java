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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.control.CliAdapter;
import org.apache.hadoop.hive.cli.control.CliConfigs;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.hive.rest.catalog.RestAccessDelegationMode;
import org.apache.iceberg.hive.rest.catalog.RestCatalogAccessDelegation;
import org.apache.iceberg.hive.rest.catalog.client.HiveRESTCatalogClient;
import org.apache.iceberg.rest.extension.OAuth2AuthorizationServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;

/**
 * LLAP {@link CliAdapter} qtests for Hive against the Gravitino Iceberg REST server with OAuth2 and
 * GCS vended OAuth tokens ({@code gcs-token}) on {@code fake-gcs-server}.
 *
 * <p>Reuses {@code iceberg_rest_catalog_gravitino.q} and the same masked golden file as the S3
 * Gravitino test; table {@code LOCATION} and {@code metadata_location} URIs are masked in the q-file.
 */
@RunWith(Parameterized.class)
public class TestIcebergRESTCatalogGravitinoGcsLlapLocalCliDriver {

  private static final CliAdapter CLI_ADAPTER =
      new CliConfigs.TestIcebergRESTCatalogGravitinoGcsLlapLocalCliDriver().getCliAdapter();

  private static final Logger LOG =
      LoggerFactory.getLogger(TestIcebergRESTCatalogGravitinoGcsLlapLocalCliDriver.class);

  private static final String CATALOG_NAME = "ice01";
  private static final long GRAVITINO_STARTUP_TIMEOUT_MINUTES = 5L;
  private static final int GRAVITINO_HTTP_PORT = 9001;

  private static final String GRAVITINO_GCS_CONF_TEMPLATE = "gravitino-gcs-vended-oauth-template.conf";
  private static final String GCS_SA_TEMPLATE = "gcs-test-service-account-template.json";
  private static final String GRAVITINO_ROOT_DIR = "/root/gravitino-iceberg-rest-server";
  private static final String GRAVITINO_STARTUP_SCRIPT = GRAVITINO_ROOT_DIR + "/bin/start-iceberg-rest-server.sh";
  private static final String GRAVITINO_H2_LIB = GRAVITINO_ROOT_DIR + "/libs/h2-driver.jar";
  private static final String GRAVITINO_GCP_BUNDLE_LIB =
      GRAVITINO_ROOT_DIR + "/libs/gravitino-iceberg-gcp-bundle.jar";
  private static final String GRAVITINO_ICEBERG_GCP_LIB = GRAVITINO_ROOT_DIR + "/libs/iceberg-gcp.jar";
  private static final String GRAVITINO_ICEBERG_GCP_BUNDLE_LIB =
      GRAVITINO_ROOT_DIR + "/libs/iceberg-gcp-bundle.jar";
  private static final String GRAVITINO_GCP_LIB = GRAVITINO_ROOT_DIR + "/libs/gravitino-gcp.jar";
  private static final String GRAVITINO_CONF_FILE = GRAVITINO_ROOT_DIR + "/conf/gravitino-iceberg-rest-server.conf";
  private static final String GRAVITINO_SA_FILE = "/tmp/gcs-test-sa.json";
  private static final String GRAVITINO_STS_CA_FILE = "/tmp/sts-mock.crt";
  private static final DockerImageName GRAVITINO_IMAGE =
      DockerImageName.parse("apache/gravitino-iceberg-rest:1.0.0");

  private static final String GCS_BUCKET = "iceberg-vend";

  private static final String OAUTH2_SERVER_ICEBERG_CLIENT_ID = "iceberg-client";
  private static final String OAUTH2_SERVER_ICEBERG_CLIENT_SECRET = "iceberg-client-secret";

  private final String name;
  private final File qfile;

  private GenericContainer<?> gravitinoContainer;
  private GcsFakeServerContainers fakeGcs;
  private GcsOAuthTokenMock gcsOAuthTokenMock;
  private GcsStsProxyContainer gcsStsProxy;
  private Path warehouseDir;
  private OAuth2AuthorizationServer oAuth2AuthorizationServer;

  @Parameters(name = "{0}")
  public static List<Object[]> getParameters() throws Exception {
    return CLI_ADAPTER.getParameters();
  }

  @ClassRule
  public static final TestRule CLI_CLASS_RULE = CLI_ADAPTER.buildClassRule();

  @Rule
  public final TestRule cliTestRule = CLI_ADAPTER.buildTestRule();

  public TestIcebergRESTCatalogGravitinoGcsLlapLocalCliDriver(String name, File qfile) {
    this.name = name;
    this.qfile = qfile;
  }

  @Before
  public void setup() throws Exception {
    Network dockerNetwork = Network.newNetwork();

    gcsOAuthTokenMock = new GcsOAuthTokenMock();
    gcsOAuthTokenMock.start();

    startOAuth2AuthorizationServer(dockerNetwork);
    createWarehouseDir();
    fakeGcs = new GcsFakeServerContainers();
    fakeGcs.start(dockerNetwork, GCS_BUCKET);
    gcsStsProxy = new GcsStsProxyContainer();
    gcsStsProxy.start(dockerNetwork, gcsOAuthTokenMock.getStsPort());
    prepareGravitinoConfig();
    startGravitinoContainer(dockerNetwork);

    String host = gravitinoContainer.getHost();
    Integer port = gravitinoContainer.getMappedPort(GRAVITINO_HTTP_PORT);
    String restCatalogPrefix = String.format("%s%s.", IcebergCatalogProperties.CATALOG_CONFIG_PREFIX, CATALOG_NAME);

    @SuppressWarnings("HttpUrlsUsage")
    String restCatalogUri = String.format("http://%s:%d/iceberg", host, port);

    Configuration conf = SessionState.get().getConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_IMPL, HiveRESTCatalogClient.class.getName());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, CATALOG_NAME);
    conf.set(restCatalogPrefix + "uri", restCatalogUri);
    conf.set(restCatalogPrefix + "type", CatalogUtil.ICEBERG_CATALOG_TYPE_REST);

    conf.set(restCatalogPrefix + "rest.auth.type", "oauth2");
    conf.set(restCatalogPrefix + "oauth2-server-uri", oAuth2AuthorizationServer.getTokenEndpoint());
    conf.set(restCatalogPrefix + "credential", oAuth2AuthorizationServer.getClientCredential());
    conf.set(
        restCatalogPrefix + RestCatalogAccessDelegation.ACCESS_DELEGATION_PROPERTY,
        RestAccessDelegationMode.VENDED_CREDENTIALS.modeName());
    conf.set(restCatalogPrefix + CatalogProperties.FILE_IO_IMPL, GCSFileIO.class.getName());

    applyHostGcsClientSettings(conf, restCatalogPrefix);
    applyHostGcsFilesystemSettings(conf);
  }

  @After
  public void teardown() throws Exception {
    if (gravitinoContainer != null) {
      gravitinoContainer.stop();
    }
    if (fakeGcs != null) {
      fakeGcs.stop();
    }
    if (gcsOAuthTokenMock != null) {
      gcsOAuthTokenMock.close();
    }
    if (gcsStsProxy != null) {
      gcsStsProxy.stop();
    }
    if (oAuth2AuthorizationServer != null) {
      oAuth2AuthorizationServer.stop();
    }
    if (warehouseDir != null) {
      FileUtils.deleteDirectory(warehouseDir.toFile());
    }
  }

  /**
   * Seeds host-reachable Iceberg GCS client settings on the HS2 session so
   * {@link org.apache.iceberg.mr.hive.IcebergVendedCredentialUtil} can override vended
   * {@code gcs.service.host} for the test JVM (similar to S3 endpoint override).
   */
  private void applyHostGcsClientSettings(Configuration conf, String restCatalogPrefix) {
    // Same host-gateway URL as fake-gcs externalUrl (resumable upload Location headers); see custom_hosts_file.
    conf.set(restCatalogPrefix + GCPProperties.GCS_SERVICE_HOST, fakeGcs.getHostGatewayEndpoint());
    conf.set(restCatalogPrefix + GCPProperties.GCS_PROJECT_ID, GcsFakeServerContainers.PROJECT_ID);
  }

  /**
   * Wires Hadoop to use the GCS connector for {@code gs://} on the host-visible fake-gcs-server
   * endpoint (see {@link TestIcebergRESTCatalogGravitinoS3LlapLocalCliDriver#applyHostS3FilesystemSettings}).
   */
  private void applyHostGcsFilesystemSettings(Configuration conf) {
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    conf.set("fs.gs.project.id", GcsFakeServerContainers.PROJECT_ID);
    conf.set("fs.gs.storage.root.url", fakeGcs.getHostGatewayEndpoint());
    // fake-gcs-server accepts unauthenticated requests; skip GCE metadata credential lookup.
    conf.setBoolean("google.cloud.auth.service.account.enable", false);
    conf.setBoolean("google.cloud.auth.null.enable", true);
    conf.setBoolean("fs.gs.auth.service.account.enable", false);
    conf.setBoolean("fs.gs.auth.null.enable", true);
  }

  @SuppressWarnings("resource")
  private void startGravitinoContainer(Network dockerNetwork) {
    gravitinoContainer = new GenericContainer<>(GRAVITINO_IMAGE)
        .withExposedPorts(GRAVITINO_HTTP_PORT)
        .withExtraHost("host.testcontainers.internal", "host-gateway")
        .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("bash", "-c",
            "keytool -importcert -noprompt -alias sts-mock -file " + GRAVITINO_STS_CA_FILE
                + " -keystore \"$JAVA_HOME/lib/security/cacerts\" -storepass changeit"
                + " && mkdir -p /tmp/gravitino-bootstrap && exec " + GRAVITINO_STARTUP_SCRIPT))
        .withCopyFileToContainer(
            MountableFile.forHostPath(Paths.get(warehouseDir.toString(), GRAVITINO_GCS_CONF_TEMPLATE)),
            GRAVITINO_CONF_FILE)
        .withCopyFileToContainer(
            MountableFile.forHostPath(Paths.get(warehouseDir.toString(), GCS_SA_TEMPLATE)),
            GRAVITINO_SA_FILE)
        .withCopyFileToContainer(
            MountableFile.forHostPath(Paths.get(warehouseDir.toString(), "sts-mock.crt")),
            GRAVITINO_STS_CA_FILE)
        .withCopyFileToContainer(
            MountableFile.forHostPath(
                Paths.get("target", "test-dependencies", "h2-driver.jar").toAbsolutePath()),
            GRAVITINO_H2_LIB)
        .withCopyFileToContainer(
            MountableFile.forHostPath(
                Paths.get("target", "test-dependencies", "gravitino-iceberg-gcp-bundle.jar")
                    .toAbsolutePath()),
            GRAVITINO_GCP_BUNDLE_LIB)
        .withCopyFileToContainer(
            MountableFile.forHostPath(
                Paths.get("target", "test-dependencies", "iceberg-gcp.jar").toAbsolutePath()),
            GRAVITINO_ICEBERG_GCP_LIB)
        .withCopyFileToContainer(
            MountableFile.forHostPath(
                Paths.get("target", "test-dependencies", "iceberg-gcp-bundle.jar").toAbsolutePath()),
            GRAVITINO_ICEBERG_GCP_BUNDLE_LIB)
        .withCopyFileToContainer(
            MountableFile.forHostPath(
                Paths.get("target", "test-dependencies", "gravitino-gcp.jar").toAbsolutePath()),
            GRAVITINO_GCP_LIB)
        .withNetwork(dockerNetwork)
        .waitingFor(
            new WaitAllStrategy()
                .withStrategy(Wait.forLogMessage(".*GravitinoIcebergRESTServer is running.*\\n", 1)
                    .withStartupTimeout(Duration.ofMinutes(GRAVITINO_STARTUP_TIMEOUT_MINUTES)))
                .withStrategy(Wait.forListeningPort()
                    .withStartupTimeout(Duration.ofMinutes(GRAVITINO_STARTUP_TIMEOUT_MINUTES))))
        .withLogConsumer(new Slf4jLogConsumer(LOG));

    gravitinoContainer.start();
  }

  private void startOAuth2AuthorizationServer(Network dockerNetwork) {
    oAuth2AuthorizationServer = new OAuth2AuthorizationServer(dockerNetwork, false);
    oAuth2AuthorizationServer.start();
  }

  private void createWarehouseDir() {
    try {
      warehouseDir = Paths.get("/tmp", "iceberg-gcs-test-" + System.currentTimeMillis()).toAbsolutePath();
      Files.createDirectories(warehouseDir);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create temp directory for Gravitino config staging", e);
    }
  }

  private void prepareGravitinoConfig() throws Exception {
    prepareGravitinoServerConfig();
    prepareServiceAccountJson();
    Files.copy(
        gcsOAuthTokenMock.getCertificatePath(),
        warehouseDir.resolve("sts-mock.crt"),
        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
  }

  private void prepareGravitinoServerConfig() throws IOException {
    String content = readClasspathResource(GRAVITINO_GCS_CONF_TEMPLATE);
    String updatedContent = content
        .replace("GCS_BUCKET", GCS_BUCKET)
        .replace("GCS_DOCKER_ENDPOINT", fakeGcs.getHostGatewayEndpoint())
        .replace("OAUTH2_SERVER_URI", oAuth2AuthorizationServer.getIssuer())
        .replace("OAUTH2_JWKS_URI", getJwksUri())
        .replace("OAUTH2_CLIENT_ID", OAUTH2_SERVER_ICEBERG_CLIENT_ID)
        .replace("OAUTH2_CLIENT_SECRET", OAUTH2_SERVER_ICEBERG_CLIENT_SECRET)
        .replace("HTTP_PORT", String.valueOf(GRAVITINO_HTTP_PORT));

    Files.writeString(warehouseDir.resolve(GRAVITINO_GCS_CONF_TEMPLATE), updatedContent);
  }

  private void prepareServiceAccountJson() throws IOException {
    String content = readClasspathResource(GCS_SA_TEMPLATE);
    String updatedContent = content.replace("TOKEN_URI", gcsOAuthTokenMock.getTokenUri());
    Files.writeString(warehouseDir.resolve(GCS_SA_TEMPLATE), updatedContent);
  }

  private static String readClasspathResource(String resource) throws IOException {
    try (InputStream in = TestIcebergRESTCatalogGravitinoGcsLlapLocalCliDriver.class.getClassLoader()
        .getResourceAsStream(resource)) {
      if (in == null) {
        throw new IOException("Resource not found: " + resource);
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private String getJwksUri() {
    String reachableHost = oAuth2AuthorizationServer.getKeycloackContainerDockerInternalHostName();
    int internalPort = 8080;
    return oAuth2AuthorizationServer.getIssuer()
        .replace("localhost", reachableHost)
        .replace("127.0.0.1", reachableHost)
        .replaceFirst(":[0-9]+", ":" + internalPort);
  }

  @Test
  public void testCliDriver() throws Exception {
    CLI_ADAPTER.runTest(name, qfile);
  }
}
