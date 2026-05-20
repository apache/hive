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

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.control.CliAdapter;
import org.apache.hadoop.hive.cli.control.CliConfigs;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.hive.client.HiveRESTCatalogClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.extension.OAuth2AuthorizationServer;
import org.apache.iceberg.types.Types;
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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * LLAP {@link CliAdapter} qtests for Hive against the Gravitino Iceberg REST server image
 * ({@link #GRAVITINO_IMAGE}), with OAuth2 on the catalog HTTP API and an Iceberg warehouse on
 * MinIO using Gravitino {@code s3-secret-key} credential vending (see
 * {@link #GRAVITINO_S3_CONF_TEMPLATE}).
 *
 * <p>Table metadata and data live under {@code s3://} in {@link #S3_BUCKET}. The host temp directory
 * {@link #warehouseDir} is used to render the Gravitino server configuration and as the source path for
 * {@code .withCopyFileToContainer} (config + H2 driver JAR).</p>
 */
@RunWith(Parameterized.class)
public class TestIcebergRESTCatalogGravitinoLlapLocalCliDriver {

  private static final CliAdapter CLI_ADAPTER =
      new CliConfigs.TestIcebergRESTCatalogGravitinoLlapLocalCliDriver().getCliAdapter();

  private static final Logger LOG = LoggerFactory.getLogger(TestIcebergRESTCatalogGravitinoLlapLocalCliDriver.class);

  private static final String CATALOG_NAME = "ice01";
  private static final long GRAVITINO_STARTUP_TIMEOUT_MINUTES = 5L;
  private static final int GRAVITINO_HTTP_PORT = 9001;
  /**
   * Classpath resource (under {@code itests/qtest-iceberg/src/test/resources/}) for the Gravitino Iceberg REST
   * server: JDBC catalog backend, MinIO {@code s3://} warehouse, {@code s3-secret-key} credential vending, OAuth2,
   * and {@code http://minio:9000} for container-to-container access to MinIO.
   */
  private static final String GRAVITINO_S3_CONF_TEMPLATE = "gravitino-s3-vended-oauth-template.conf";
  private static final String GRAVITINO_ROOT_DIR = "/root/gravitino-iceberg-rest-server";
  private static final String GRAVITINO_STARTUP_SCRIPT = GRAVITINO_ROOT_DIR + "/bin/start-iceberg-rest-server.sh";
  private static final String GRAVITINO_H2_LIB = GRAVITINO_ROOT_DIR + "/libs/h2-driver.jar";
  private static final String GRAVITINO_CONF_FILE = GRAVITINO_ROOT_DIR + "/conf/gravitino-iceberg-rest-server.conf";
  private static final DockerImageName GRAVITINO_IMAGE = DockerImageName.parse("apache/gravitino-iceberg-rest:1.0.0");

  private static final String S3_BUCKET = "iceberg-vend";
  private static final String MINIO_ACCESS_KEY = "minioadmin";
  private static final String MINIO_SECRET_KEY = "minioadmin";
  private static final int MINIO_API_PORT = 9000;
  private static final DockerImageName MINIO_IMAGE = DockerImageName.parse("minio/minio:RELEASE.2024-09-22T00-33-43Z");

  private static final String OAUTH2_SERVER_ICEBERG_CLIENT_ID = "iceberg-client";
  private static final String OAUTH2_SERVER_ICEBERG_CLIENT_SECRET = "iceberg-client-secret";

  private final String name;
  private final File qfile;

  private GenericContainer<?> gravitinoContainer;
  private GenericContainer<?> minioContainer;
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

  public TestIcebergRESTCatalogGravitinoLlapLocalCliDriver(String name, File qfile) {
    this.name = name;
    this.qfile = qfile;
  }

  @Before
  public void setup() throws Exception {
    Network dockerNetwork = Network.newNetwork();

    startOAuth2AuthorizationServer(dockerNetwork);
    createWarehouseDir();
    startMinio(dockerNetwork);
    ensureMinioBucket();
    prepareGravitinoConfig();
    startGravitinoContainer(dockerNetwork);

    String host = gravitinoContainer.getHost();
    Integer port = gravitinoContainer.getMappedPort(GRAVITINO_HTTP_PORT);
    String restCatalogPrefix = String.format("%s%s.", IcebergCatalogProperties.CATALOG_CONFIG_PREFIX, CATALOG_NAME);

    // Suppress IntelliJ warning about using HTTP since this is a local test container connection
    @SuppressWarnings("HttpUrlsUsage")
    String restCatalogUri = String.format("http://%s:%d/iceberg", host, port);

    Configuration conf = SessionState.get().getConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_IMPL, HiveRESTCatalogClient.class.getName());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, CATALOG_NAME);
    conf.set(restCatalogPrefix + "uri", restCatalogUri);
    conf.set(restCatalogPrefix + "type", CatalogUtil.ICEBERG_CATALOG_TYPE_REST);

    // OAUTH2 configs for the Iceberg REST client (catalog HTTP API)
    conf.set(restCatalogPrefix + "rest.auth.type", "oauth2");
    conf.set(restCatalogPrefix + "oauth2-server-uri", oAuth2AuthorizationServer.getTokenEndpoint());
    conf.set(restCatalogPrefix + "credential", oAuth2AuthorizationServer.getClientCredential());
    conf.set(restCatalogPrefix + "header.X-Iceberg-Access-Delegation", "vended-credentials");

    // Hadoop S3A + Iceberg S3FileIO on the host JVM (Hive CLI / Tez / LLAP), see class Javadoc
    applyHostS3aForMinio(conf);
    applyIcebergS3ClientEndpointOverride(conf, restCatalogPrefix);

    // Smoke-test Iceberg RESTCatalog + vended S3 (S3FileIO) against Gravitino/MinIO before LLAP/qfile so failures
    // in OAuth, delegation headers, or object storage surface without the full CLI harness.
    verifyIcebergRestUsesVendedS3FromGravitino();
  }

  @After
  public void teardown() throws IOException {
    if (gravitinoContainer != null) {
      gravitinoContainer.stop();
    }

    if (minioContainer != null) {
      minioContainer.stop();
    }

    if (oAuth2AuthorizationServer != null) {
      oAuth2AuthorizationServer.stop();
    }

    if (warehouseDir != null) {
      FileUtils.deleteDirectory(warehouseDir.toFile());
    }
  }

  /**
   * Configure Iceberg {@code S3FileIO} for the host JVM (Tez/LLAP): published MinIO endpoint,
   * path-style, region, and root credentials matching {@link #MINIO_ACCESS_KEY} /
   * {@link #MINIO_SECRET_KEY} (same values Gravitino uses for {@code s3-secret-key} vending).
   * <p>
   * Gravitino keeps {@code http://minio:9000} for container-side I/O; catalog metadata may still
   * reference that host. Separately, vended S3 properties are not always merged into every task
   * {@link org.apache.hadoop.conf.Configuration}, which can surface as MinIO {@code 403 Forbidden}
   * on {@code headObject} from the SDK.
   */
  private void applyIcebergS3ClientEndpointOverride(Configuration conf, String restCatalogPrefix) {
    String host = minioContainer.getHost();
    int port = minioContainer.getMappedPort(MINIO_API_PORT);
    @SuppressWarnings("HttpUrlsUsage")
    String icebergS3Endpoint = String.format("http://%s:%d", host, port);
    conf.set(restCatalogPrefix + "s3.endpoint", icebergS3Endpoint);
    conf.set(restCatalogPrefix + "s3.path-style-access", "true");
    // Align with gravitino-s3-vended-oauth-template.conf (gravitino.iceberg-rest.s3-region).
    conf.set(restCatalogPrefix + "client.region", "us-east-1");
    conf.set(restCatalogPrefix + "s3.access-key-id", MINIO_ACCESS_KEY);
    conf.set(restCatalogPrefix + "s3.secret-access-key", MINIO_SECRET_KEY);
  }

  /**
   * Hadoop S3A settings for the test bucket on the published MinIO endpoint. Uses per-bucket keys because Hive
   * strips global {@code fs.s3a.access.key} / {@code fs.s3a.secret.key} from configs sent to Tez (see
   * {@code hive.conf.hidden.list}).
   */
  private void applyHostS3aForMinio(Configuration conf) {
    String minioHost = minioContainer.getHost();
    int minioPort = minioContainer.getMappedPort(MINIO_API_PORT);
    @SuppressWarnings("HttpUrlsUsage")
    String endpoint = String.format("http://%s:%d", minioHost, minioPort);
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A");
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    // Per-bucket keys: Hive strips fs.s3a.access.key / fs.s3a.secret.key from Tez configs (hive.conf.hidden.list).
    String bucketPrefix = "fs.s3a.bucket." + S3_BUCKET + ".";
    conf.set(bucketPrefix + "endpoint", endpoint);
    conf.setBoolean(bucketPrefix + "path.style.access", true);
    conf.set(bucketPrefix + "access.key", MINIO_ACCESS_KEY);
    conf.set(bucketPrefix + "secret.key", MINIO_SECRET_KEY);
    conf.setBoolean(bucketPrefix + "connection.ssl.enabled", false);
  }

  /**
   * Starts a Gravitino container with the Iceberg REST server configured for this test.
   *
   * <p>This method configures the container to:
   * <ul>
   *   <li>Expose {@link #GRAVITINO_HTTP_PORT} on the container and map it to a host port.</li>
   *   <li>Adjust the entrypoint so a bootstrap directory exists before {@link #GRAVITINO_STARTUP_SCRIPT} runs
   *       (the Iceberg <em>warehouse</em> itself is {@code s3://} on MinIO, not this path).</li>
   *   <li>Copy the rendered Gravitino configuration from {@link #warehouseDir} into the image at
   *       {@link #GRAVITINO_CONF_FILE}.</li>
   *   <li>Copy the H2 driver JAR (JDBC catalog backend metadata) into {@link #GRAVITINO_H2_LIB}.</li>
   *   <li>Attach the container to {@code dockerNetwork} so it reaches the OAuth2 server and the {@code minio}
   *       alias.</li>
   *   <li>Wait for the Gravitino Iceberg REST server to finish starting (log line + listening port).</li>
   *   <li>Stream container logs into {@link #LOG}.</li>
   * </ul>
   *
   * <p>Note: the {@code @SuppressWarnings("resource")} annotation is applied because IntelliJ and some compilers
   * flag {@link GenericContainer} as a resource that should be used with try-with-resources. Here the container
   * lifecycle is explicit: it is started in this method and stopped in {@link #teardown()} via
   * {@code gravitinoContainer.stop()}.</p>
   */
  @SuppressWarnings("resource")
  private void startGravitinoContainer(Network dockerNetwork) {
    gravitinoContainer = new GenericContainer<>(GRAVITINO_IMAGE)
        .withExposedPorts(GRAVITINO_HTTP_PORT)
        // Bootstrap dir for the server script; warehouse is s3:// on MinIO (see template)
        .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("bash", "-c",
            "mkdir -p /tmp/gravitino-bootstrap && exec " + GRAVITINO_STARTUP_SCRIPT))
        // Mount Gravitino configuration file (rendered under warehouseDir on the host)
        .withCopyFileToContainer(
            MountableFile.forHostPath(Paths.get(warehouseDir.toString(), GRAVITINO_S3_CONF_TEMPLATE)),
            GRAVITINO_CONF_FILE)
        // Mount the H2 driver JAR into the server's lib directory
        .withCopyFileToContainer(
            MountableFile.forHostPath(
                Paths.get("target", "test-dependencies", "h2-driver.jar").toAbsolutePath()),
            GRAVITINO_H2_LIB)
        // Same Docker network as OAuth2 and MinIO (Gravitino uses http://minio:9000 in config)
        .withNetwork(dockerNetwork)
        // Wait for the server to be fully started
        .waitingFor(
            new WaitAllStrategy()
                .withStrategy(Wait.forLogMessage(".*GravitinoIcebergRESTServer is running.*\\n", 1)
                    .withStartupTimeout(Duration.ofMinutes(GRAVITINO_STARTUP_TIMEOUT_MINUTES)))
                .withStrategy(Wait.forListeningPort()
                    .withStartupTimeout(Duration.ofMinutes(GRAVITINO_STARTUP_TIMEOUT_MINUTES))))
        .withLogConsumer(new Slf4jLogConsumer(LOG));

    gravitinoContainer.start();
  }

  /**
   * MinIO for the Iceberg warehouse. {@code .withNetworkAliases("minio")} matches
   * {@code gravitino.iceberg-rest.s3-endpoint = http://minio:9000} inside the Gravitino container.
   */
  @SuppressWarnings("resource")
  private void startMinio(Network dockerNetwork) {
    minioContainer = new GenericContainer<>(MINIO_IMAGE)
        .withNetwork(dockerNetwork)
        .withNetworkAliases("minio")
        .withExposedPorts(MINIO_API_PORT)
        .withEnv("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
        .withEnv("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
        .withCommand("server", "/data")
        .waitingFor(Wait.forListeningPort());

    minioContainer.start();
  }

  /** Creates {@link #S3_BUCKET} if missing so Gravitino and Hive can use {@code s3://} paths. */
  private void ensureMinioBucket() throws Exception {
    MinioClient client = MinioClient.builder()
        .endpoint(minioContainer.getHost(), minioContainer.getMappedPort(MINIO_API_PORT), false)
        .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
        .build();
    if (!client.bucketExists(BucketExistsArgs.builder().bucket(S3_BUCKET).build())) {
      client.makeBucket(MakeBucketArgs.builder().bucket(S3_BUCKET).build());
    }
  }

  /** Keycloak-backed OAuth2 used by Gravitino REST authentication and by the Hive REST client. */
  private void startOAuth2AuthorizationServer(Network dockerNetwork) {
    oAuth2AuthorizationServer = new OAuth2AuthorizationServer(dockerNetwork, false);
    oAuth2AuthorizationServer.start();
  }

  /**
   * Host directory used to write the rendered Gravitino config (see {@link #prepareGravitinoConfig}) and as the
   * source path for {@code .withCopyFileToContainer} in {@link #startGravitinoContainer}. This is not the Iceberg
   * warehouse root; the warehouse is {@code s3://}{@link #S3_BUCKET}{@code /...} on MinIO.
   */
  private void createWarehouseDir() {
    try {
      warehouseDir = Paths.get("/tmp", "iceberg-test-" + System.currentTimeMillis()).toAbsolutePath();
      Files.createDirectories(warehouseDir);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create temp directory for Gravitino config staging", e);
    }
  }

  /**
   * Reads {@link #GRAVITINO_S3_CONF_TEMPLATE} from the classpath, substitutes bucket / MinIO / OAuth placeholders,
   * and writes the result under {@link #warehouseDir} for copying into the Gravitino container.
   */
  private void prepareGravitinoConfig() throws IOException {
    String content;
    try (InputStream in = TestIcebergRESTCatalogGravitinoLlapLocalCliDriver.class.getClassLoader()
        .getResourceAsStream(GRAVITINO_S3_CONF_TEMPLATE)) {
      if (in == null) {
        throw new IOException("Resource not found: " + GRAVITINO_S3_CONF_TEMPLATE);
      }
      content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }

    String updatedContent = content
        .replace("S3_BUCKET", S3_BUCKET)
        .replace("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
        .replace("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
        .replace("OAUTH2_SERVER_URI", oAuth2AuthorizationServer.getIssuer())
        .replace("OAUTH2_JWKS_URI", getJwksUri())
        .replace("OAUTH2_CLIENT_ID", OAUTH2_SERVER_ICEBERG_CLIENT_ID)
        .replace("OAUTH2_CLIENT_SECRET", OAUTH2_SERVER_ICEBERG_CLIENT_SECRET)
        .replace("HTTP_PORT", String.valueOf(GRAVITINO_HTTP_PORT));

    Path configFile = warehouseDir.resolve(GRAVITINO_S3_CONF_TEMPLATE);
    Files.writeString(configFile, updatedContent);
  }

  /**
   * JWKS URL reachable from <em>inside</em> the Gravitino container: host/port in the issuer are rewritten to the
   * Keycloak container hostname and its internal HTTP port.
   */
  private String getJwksUri() {
    String reachableHost = oAuth2AuthorizationServer.getKeycloackContainerDockerInternalHostName();
    int internalPort = 8080; // Keycloak container's internal port
    return oAuth2AuthorizationServer.getIssuer()
        .replace("localhost", reachableHost)
        .replace("127.0.0.1", reachableHost)
        // Replace issuer's mapped host port with Keycloak's internal port on the Docker network
        .replaceFirst(":[0-9]+", ":" + internalPort);
  }

  /**
   * Proves the Iceberg REST client receives vended S3 credentials from Gravitino: create/load an S3-located table and
   * run a scan (same catalog properties as {@link HiveRESTCatalogClient#reconnect()}). Invoked from {@link #setup()}
   * after Hive session configuration is complete.
   */
  private void verifyIcebergRestUsesVendedS3FromGravitino() throws Exception {
    Configuration conf = SessionState.get().getConf();
    String catalogName = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT);
    Map<String, String> props = IcebergCatalogProperties.getCatalogProperties(conf);

    try (RESTCatalog rest = new RESTCatalog()) {
      rest.setConf(conf);
      rest.initialize(catalogName, props);
      Namespace ns = Namespace.of("vend_cred_chk");

      if (!rest.namespaceExists(ns)) {
        rest.createNamespace(ns, new java.util.HashMap<>());
      }

      TableIdentifier tid = TableIdentifier.of(ns, "tiny");
      if (rest.tableExists(tid)) {
        rest.dropTable(tid, false);
      }

      String location = String.format("s3://%s/warehouse/vend_cred_chk/tiny", S3_BUCKET);
      rest.buildTable(
              tid,
              new Schema(Types.NestedField.required(1, "x", Types.IntegerType.get())))
          .withPartitionSpec(PartitionSpec.unpartitioned())
          .withLocation(location)
          .create();

      org.apache.iceberg.Table table = rest.loadTable(tid);
      assertThat(table.io().getClass().getSimpleName()).isEqualTo("S3FileIO");
      assertThat(table.newScan().planFiles()).isEmpty();
      rest.dropTable(tid, false);
    }
  }

  @Test
  public void testCliDriver() throws Exception {
    CLI_ADAPTER.runTest(name, qfile);
  }
}
