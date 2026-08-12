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
import org.apache.iceberg.hadoop.HadoopFileIO;
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
 * Azure vended credentials ({@code azure-account-key}) on Azurite WASB blob storage.
 *
 * <p>Reuses {@code iceberg_rest_catalog_gravitino.q} and the same masked golden file as the S3
 * Gravitino test; table {@code LOCATION} and {@code metadata_location} URIs are masked in the q-file.
 */
@RunWith(Parameterized.class)
public class TestIcebergRESTCatalogGravitinoAdlsLlapLocalCliDriver {

  private static final CliAdapter CLI_ADAPTER =
      new CliConfigs.TestIcebergRESTCatalogGravitinoAdlsLlapLocalCliDriver().getCliAdapter();

  private static final Logger LOG =
      LoggerFactory.getLogger(TestIcebergRESTCatalogGravitinoAdlsLlapLocalCliDriver.class);

  private static final String CATALOG_NAME = "ice01";
  private static final long GRAVITINO_STARTUP_TIMEOUT_MINUTES = 5L;
  private static final int GRAVITINO_HTTP_PORT = 9001;

  private static final String GRAVITINO_ADLS_CONF_TEMPLATE = "gravitino-adls-vended-oauth-template.conf";
  private static final String GRAVITINO_ADLS_CORE_SITE_TEMPLATE = "gravitino-adls-core-site-template.xml";
  private static final String GRAVITINO_ROOT_DIR = "/root/gravitino-iceberg-rest-server";
  private static final String GRAVITINO_STARTUP_SCRIPT = GRAVITINO_ROOT_DIR + "/bin/start-iceberg-rest-server.sh";
  private static final String GRAVITINO_H2_LIB = GRAVITINO_ROOT_DIR + "/libs/h2-driver.jar";
  private static final String GRAVITINO_AZURE_BUNDLE_LIB =
      GRAVITINO_ROOT_DIR + "/libs/gravitino-iceberg-azure-bundle.jar";
  private static final String GRAVITINO_ICEBERG_AZURE_LIB =
      GRAVITINO_ROOT_DIR + "/libs/iceberg-azure-bundle.jar";
  private static final String GRAVITINO_HADOOP_AZURE_LIB =
      GRAVITINO_ROOT_DIR + "/libs/gravitino-hadoop-azure.jar";
  private static final String GRAVITINO_AZURE_STORAGE_LIB = GRAVITINO_ROOT_DIR + "/libs/azure-storage.jar";
  private static final String GRAVITINO_AZURE_KEYVAULT_CORE_LIB =
      GRAVITINO_ROOT_DIR + "/libs/azure-keyvault-core.jar";
  private static final String GRAVITINO_MORTBAY_JETTY_UTIL_LIB =
      GRAVITINO_ROOT_DIR + "/libs/mortbay-jetty-util.jar";
  private static final String GRAVITINO_CONF_FILE = GRAVITINO_ROOT_DIR + "/conf/gravitino-iceberg-rest-server.conf";
  private static final String GRAVITINO_CORE_SITE_FILE = GRAVITINO_ROOT_DIR + "/conf/core-site.xml";
  private static final DockerImageName GRAVITINO_IMAGE =
      DockerImageName.parse("apache/gravitino-iceberg-rest:1.0.0");

  private static final String CONTAINER_NAME = "iceberg-vend";

  private static final String OAUTH2_SERVER_ICEBERG_CLIENT_ID = "iceberg-client";
  private static final String OAUTH2_SERVER_ICEBERG_CLIENT_SECRET = "iceberg-client-secret";

  /** Hadoop WASB emulator account name; must match the WASB URI authority host. */
  private static final String AZURE_STORAGE_EMULATOR_ACCOUNT_NAME = "fs.azure.storage.emulator.account.name";

  private final String name;
  private final File qfile;

  private GenericContainer<?> gravitinoContainer;
  private AdlsAzuriteContainers azurite;
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

  public TestIcebergRESTCatalogGravitinoAdlsLlapLocalCliDriver(String name, File qfile) {
    this.name = name;
    this.qfile = qfile;
  }

  @Before
  public void setup() throws Exception {
    Network dockerNetwork = Network.newNetwork();

    startOAuth2AuthorizationServer(dockerNetwork);
    createWarehouseDir();
    azurite = new AdlsAzuriteContainers();
    azurite.start(dockerNetwork, CONTAINER_NAME);
    prepareGravitinoConfig();
    startGravitinoContainer();

    String host = azurite.getHost();
    Integer port = azurite.getMappedGravitinoPort();
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
    // ResolvingFileIO maps wasb:// to ADLSFileIO; Azurite needs Hadoop WASB via HadoopFileIO.
    conf.set(restCatalogPrefix + CatalogProperties.FILE_IO_IMPL, HadoopFileIO.class.getName());

    applyHostWasbFilesystemSettings(conf);
  }

  @After
  public void teardown() throws Exception {
    if (gravitinoContainer != null) {
      gravitinoContainer.stop();
    }
    if (azurite != null) {
      azurite.stop();
    }
    if (oAuth2AuthorizationServer != null) {
      oAuth2AuthorizationServer.stop();
    }
    if (warehouseDir != null) {
      FileUtils.deleteDirectory(warehouseDir.toFile());
    }
  }

  /**
   * Wires Hadoop WASB emulator mode for {@code wasb://} on the host JVM.
   *
   * <p>Azurite is published on {@code localhost:10000}; Hadoop's development-storage client uses that
   * endpoint when {@code fs.azure.storage.emulator.account.name} matches the WASB URI authority host.
   * Access keys are vended per query for REST delegation coverage; emulator I/O uses the well-known
   * Azurite account key locally via {@link HadoopFileIO} (not {@code ADLSFileIO}).
   */
  private void applyHostWasbFilesystemSettings(Configuration conf) {
    conf.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
    conf.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure");
    conf.set("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb");
    conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
    conf.set(AZURE_STORAGE_EMULATOR_ACCOUNT_NAME, AdlsAzuriteContainers.STORAGE_EMULATOR_ACCOUNT_NAME);
  }

  @SuppressWarnings("resource")
  private void startGravitinoContainer() {
    gravitinoContainer = new GenericContainer<>(GRAVITINO_IMAGE)
        // Share Azurite's network namespace so Hadoop WASB emulator (127.0.0.1:10000) reaches Azurite.
        .withNetworkMode("container:" + azurite.getContainerId())
        .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("bash", "-c",
            "mkdir -p /tmp/gravitino-bootstrap && exec " + GRAVITINO_STARTUP_SCRIPT))
        .withCopyFileToContainer(
            MountableFile.forHostPath(Paths.get(warehouseDir.toString(), GRAVITINO_ADLS_CONF_TEMPLATE)),
            GRAVITINO_CONF_FILE)
        .withCopyFileToContainer(
            MountableFile.forHostPath(Paths.get(warehouseDir.toString(), GRAVITINO_ADLS_CORE_SITE_TEMPLATE)),
            GRAVITINO_CORE_SITE_FILE)
        .withCopyFileToContainer(
            MountableFile.forHostPath(
                Paths.get("target", "test-dependencies", "h2-driver.jar").toAbsolutePath()),
            GRAVITINO_H2_LIB)
        .withCopyFileToContainer(
            MountableFile.forHostPath(
                Paths.get("target", "test-dependencies", "gravitino-iceberg-azure-bundle.jar")
                    .toAbsolutePath()),
            GRAVITINO_AZURE_BUNDLE_LIB)
        .withCopyFileToContainer(
            MountableFile.forHostPath(
                Paths.get("target", "test-dependencies", "iceberg-azure-bundle.jar").toAbsolutePath()),
            GRAVITINO_ICEBERG_AZURE_LIB)
        .withCopyFileToContainer(
            MountableFile.forHostPath(
                Paths.get("target", "test-dependencies", "gravitino-hadoop-azure.jar").toAbsolutePath()),
            GRAVITINO_HADOOP_AZURE_LIB)
        .withCopyFileToContainer(
            MountableFile.forHostPath(
                Paths.get("target", "test-dependencies", "azure-storage.jar").toAbsolutePath()),
            GRAVITINO_AZURE_STORAGE_LIB)
        .withCopyFileToContainer(
            MountableFile.forHostPath(
                Paths.get("target", "test-dependencies", "azure-keyvault-core.jar").toAbsolutePath()),
            GRAVITINO_AZURE_KEYVAULT_CORE_LIB)
        .withCopyFileToContainer(
            MountableFile.forHostPath(
                Paths.get("target", "test-dependencies", "mortbay-jetty-util.jar").toAbsolutePath()),
            GRAVITINO_MORTBAY_JETTY_UTIL_LIB)
        .waitingFor(
            new WaitAllStrategy()
                .withStrategy(Wait.forLogMessage(".*GravitinoIcebergRESTServer is running.*\\n", 1)
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
      warehouseDir = Paths.get("/tmp", "iceberg-adls-test-" + System.currentTimeMillis()).toAbsolutePath();
      Files.createDirectories(warehouseDir);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create temp directory for Gravitino config staging", e);
    }
  }

  private void prepareGravitinoConfig() throws IOException {
    String content;
    try (InputStream in = TestIcebergRESTCatalogGravitinoAdlsLlapLocalCliDriver.class.getClassLoader()
        .getResourceAsStream(GRAVITINO_ADLS_CONF_TEMPLATE)) {
      if (in == null) {
        throw new IOException("Resource not found: " + GRAVITINO_ADLS_CONF_TEMPLATE);
      }
      content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }

    String updatedContent = content
        .replace("CONTAINER", CONTAINER_NAME)
        .replace("ACCOUNT", AdlsAzuriteContainers.ACCOUNT_NAME)
        .replace("ACCOUNT_KEY", AdlsAzuriteContainers.ACCOUNT_KEY)
        .replace("AZURITE_DOCKER_ENDPOINT", AdlsAzuriteContainers.AZURITE_BLOB_DOCKER_ENDPOINT)
        .replace("OAUTH2_SERVER_URI", oAuth2AuthorizationServer.getIssuer())
        .replace("OAUTH2_JWKS_URI", getJwksUri())
        .replace("OAUTH2_CLIENT_ID", OAUTH2_SERVER_ICEBERG_CLIENT_ID)
        .replace("OAUTH2_CLIENT_SECRET", OAUTH2_SERVER_ICEBERG_CLIENT_SECRET)
        .replace("HTTP_PORT", String.valueOf(GRAVITINO_HTTP_PORT));

    Path configFile = warehouseDir.resolve(GRAVITINO_ADLS_CONF_TEMPLATE);
    Files.writeString(configFile, updatedContent);

    prepareGravitinoCoreSiteConfig();
  }

  private void prepareGravitinoCoreSiteConfig() throws IOException {
    String content;
    try (InputStream in = TestIcebergRESTCatalogGravitinoAdlsLlapLocalCliDriver.class.getClassLoader()
        .getResourceAsStream(GRAVITINO_ADLS_CORE_SITE_TEMPLATE)) {
      if (in == null) {
        throw new IOException("Resource not found: " + GRAVITINO_ADLS_CORE_SITE_TEMPLATE);
      }
      content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }

    String updatedContent = content
        .replace("ACCOUNT", AdlsAzuriteContainers.ACCOUNT_NAME)
        .replace("ACCOUNT_KEY", AdlsAzuriteContainers.ACCOUNT_KEY)
        .replace("AZURITE_DOCKER_ENDPOINT", AdlsAzuriteContainers.AZURITE_BLOB_DOCKER_ENDPOINT);

    Path coreSiteFile = warehouseDir.resolve(GRAVITINO_ADLS_CORE_SITE_TEMPLATE);
    Files.writeString(coreSiteFile, updatedContent);
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
