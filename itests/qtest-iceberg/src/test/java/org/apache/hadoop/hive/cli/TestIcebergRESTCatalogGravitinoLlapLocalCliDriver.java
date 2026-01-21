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

import com.github.dockerjava.api.command.CopyArchiveFromContainerCmd;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.control.CliAdapter;
import org.apache.hadoop.hive.cli.control.CliConfigs;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.hive.client.HiveRESTCatalogClient;
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
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class TestIcebergRESTCatalogGravitinoLlapLocalCliDriver {

  private static final CliAdapter CLI_ADAPTER =
      new CliConfigs.TestIcebergRESTCatalogGravitinoLlapLocalCliDriver().getCliAdapter();
  
  private static final Logger LOG = LoggerFactory.getLogger(TestIcebergRESTCatalogGravitinoLlapLocalCliDriver.class);
  
  private static final String CATALOG_NAME = "ice01";
  private static final long GRAVITINO_STARTUP_TIMEOUT_MINUTES = 5L;
  private static final int GRAVITINO_HTTP_PORT = 9001;
  private static final String GRAVITINO_CONF_FILE_TEMPLATE = "gravitino-h2-test-template.conf";
  private static final String GRAVITINO_ROOT_DIR = "/root/gravitino-iceberg-rest-server";
  private static final String GRAVITINO_STARTUP_SCRIPT = GRAVITINO_ROOT_DIR + "/bin/start-iceberg-rest-server.sh";
  private static final String GRAVITINO_H2_LIB = GRAVITINO_ROOT_DIR + "/libs/h2-driver.jar";
  private static final String GRAVITINO_CONF_FILE = GRAVITINO_ROOT_DIR + "/conf/gravitino-iceberg-rest-server.conf";
  private static final DockerImageName GRAVITINO_IMAGE =
      DockerImageName.parse("apache/gravitino-iceberg-rest:1.0.0");

  private static final String OAUTH2_SERVER_ICEBERG_CLIENT_ID = "iceberg-client";
  private static final String OAUTH2_SERVER_ICEBERG_CLIENT_SECRET = "iceberg-client-secret";

  private final String name;
  private final File qfile;

  private GenericContainer<?> gravitinoContainer;
  private Path warehouseDir;
  private final ScheduledExecutorService fileSyncExecutor = Executors.newSingleThreadScheduledExecutor();
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
  public void setup() throws IOException {
    Network dockerNetwork = Network.newNetwork();
    
    startOAuth2AuthorizationServer(dockerNetwork);
    createWarehouseDir();
    prepareGravitinoConfig();
    startGravitinoContainer(dockerNetwork);
    fileSyncExecutor.scheduleAtFixedRate(this::syncWarehouseDir, 0, 5, TimeUnit.SECONDS);

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

    // OAUTH2 Configs
    conf.set(restCatalogPrefix + "rest.auth.type", "oauth2");
    conf.set(restCatalogPrefix + "oauth2-server-uri", oAuth2AuthorizationServer.getTokenEndpoint());
    conf.set(restCatalogPrefix + "credential", oAuth2AuthorizationServer.getClientCredential());
  }

  @After
  public void teardown() throws IOException {
    if (gravitinoContainer != null) {
      gravitinoContainer.stop();
    }
    
    if (oAuth2AuthorizationServer != null) {
      oAuth2AuthorizationServer.stop();
    }

    fileSyncExecutor.shutdownNow();
    FileUtils.deleteDirectory(warehouseDir.toFile());
  }

  /**
   * Starts a Gravitino container with the Iceberg REST server configured for testing.
   *
   * <p>This method configures the container to:
   * <ul>
   *   <li>Expose container REST port GRAVITINO_HTTP_PORT and map it to a host port.</li>
   *   <li>Modify the container entrypoint to create the warehouse directory before startup.</li>
   *   <li>Copy a dynamically prepared Gravitino configuration file into the container.</li>
   *   <li>Copy the H2 driver JAR into the server's lib directory.</li>
   *   <li>Wait for the Gravitino Iceberg REST server to finish starting (based on logs and port checks).</li>
   *   <li>Stream container logs into the test logger for easier debugging.</li>
   * </ul>
   *
   * <p>Note: The {@code @SuppressWarnings("resource")} annotation is applied because
   * IntelliJ and some compilers flag {@link org.testcontainers.containers.GenericContainer}
   * as a resource that should be managed with try-with-resources. In this test setup,
   * the container lifecycle is managed explicitly: it is started here and stopped in
   * {@code @After} (via {@code gravitinoContainer.stop()}). Using try-with-resources
   * would not work in this context, since the container must remain running across
   * multiple test methods rather than being confined to a single block scope.</p>
   */
  @SuppressWarnings("resource")
  private void startGravitinoContainer(Network dockerNetwork) {
    gravitinoContainer = new GenericContainer<>(GRAVITINO_IMAGE)
        .withExposedPorts(GRAVITINO_HTTP_PORT)
        // Update entrypoint to create the warehouse directory before starting the server
        .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("bash", "-c",
            String.format("mkdir -p %s && exec %s", warehouseDir.toString(), GRAVITINO_STARTUP_SCRIPT)))
        // Mount gravitino configuration file
        .withCopyFileToContainer(
            MountableFile.forHostPath(Paths.get(warehouseDir.toString(), GRAVITINO_CONF_FILE_TEMPLATE)),
            GRAVITINO_CONF_FILE
        )
        // Mount the H2 driver JAR into the server's lib directory
        .withCopyFileToContainer(
            MountableFile.forHostPath(
                Paths.get("target", "test-dependencies", "h2-driver.jar").toAbsolutePath()
            ),
            GRAVITINO_H2_LIB
        )
        // Use the same Docker network as the OAuth2 server so they can communicate
        .withNetwork(dockerNetwork)
        // Wait for the server to be fully started
        .waitingFor(
            new WaitAllStrategy()
                .withStrategy(Wait.forLogMessage(".*GravitinoIcebergRESTServer is running.*\\n", 1)
                    .withStartupTimeout(Duration.ofMinutes(GRAVITINO_STARTUP_TIMEOUT_MINUTES)))
                .withStrategy(Wait.forListeningPort()
                    .withStartupTimeout(Duration.ofMinutes(GRAVITINO_STARTUP_TIMEOUT_MINUTES)))
        )
        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory
            .getLogger(TestIcebergRESTCatalogGravitinoLlapLocalCliDriver.class)));

    gravitinoContainer.start();
  }

  /**
   * Starts a background daemon that continuously synchronizes the Iceberg warehouse
   * directory from the running Gravitino container to the host file system.
   *
   * <p>In CI environments, Testcontainers' {@code .withFileSystemBind()} cannot reliably
   * bind the same host path to the same path inside the container, especially when
   * using remote Docker hosts or Docker-in-Docker setups. This causes the container's
   * writes (e.g., Iceberg metadata files like {@code .metadata.json}) to be invisible
   * on the host.</p>
   *
   * <p>This method works around that limitation by repeatedly copying new files from
   * the container's warehouse directory to the corresponding host directory. Existing
   * files on the host are preserved, and only files that do not yet exist are copied.
   * The sync runs every 1 second while the container is running.</p>
   *
   * <p>Each archive copy from the container is extracted using a {@link TarArchiveInputStream},
   * and directories are created as needed. Files that already exist on the host are skipped
   * to avoid overwriting container data.</p>
   */
  private void syncWarehouseDir() {
    if (gravitinoContainer.isRunning()) {
      try (CopyArchiveFromContainerCmd copyArchiveFromContainerCmd = 
               gravitinoContainer
                   .getDockerClient()
                   .copyArchiveFromContainerCmd(gravitinoContainer.getContainerId(), warehouseDir.toString()); 
           InputStream tarStream = copyArchiveFromContainerCmd.exec();
           TarArchiveInputStream tis = new TarArchiveInputStream(tarStream)) {

        TarArchiveEntry entry;
        while ((entry = tis.getNextEntry()) != null) {
          // Skip directories because we only want to copy metadata files from the container.
          if (entry.isDirectory()) {
            continue;
          }

          /*
           * Tar entry names include a container-specific top-level folder, e.g.:
           *   iceberg-test-1759245909247/iceberg_warehouse/ice_rest/.../metadata.json
           *
           * Strip the first part so the relative path inside the warehouse is preserved
           * when mapping to the host warehouseDir.
           */
          
          String[] parts = entry.getName().split("/", 2);
          if (parts.length < 2) {
            continue; // defensive guard
          }

          Path relativePath = Paths.get(parts[1]);
          Path outputPath = warehouseDir.resolve(relativePath);

          // Skip if already present on host to avoid overwriting
          if (Files.exists(outputPath)) {
            continue;
          }

          Files.createDirectories(outputPath.getParent());
          Files.copy(tis, outputPath);
        }

      } catch (Exception e) {
        LOG.error("Warehouse folder sync failed: {}", e.getMessage());
      }
    }
  }
  
  private void startOAuth2AuthorizationServer(Network dockerNetwork) {
    oAuth2AuthorizationServer = new OAuth2AuthorizationServer(dockerNetwork, false);
    oAuth2AuthorizationServer.start();
  }

  private void createWarehouseDir() {
    try {
      warehouseDir = Paths.get("/tmp", "iceberg-test-" + System.currentTimeMillis()).toAbsolutePath();
      Files.createDirectories(warehouseDir);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create the Iceberg warehouse directory", e);
    }
  }

  private void prepareGravitinoConfig() throws IOException {
    String content;
    try (InputStream in = TestIcebergRESTCatalogGravitinoLlapLocalCliDriver.class.getClassLoader()
        .getResourceAsStream(GRAVITINO_CONF_FILE_TEMPLATE)) {
      if (in == null) {
        throw new IOException("Resource not found: " + GRAVITINO_CONF_FILE_TEMPLATE);
      }
      content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }

    String updatedContent = content
        .replace("/WAREHOUSE_DIR", warehouseDir.toString())
        .replace("OAUTH2_SERVER_URI", oAuth2AuthorizationServer.getIssuer())
        .replace("OAUTH2_JWKS_URI", getJwksUri())
        .replace("OAUTH2_CLIENT_ID", OAUTH2_SERVER_ICEBERG_CLIENT_ID)
        .replace("OAUTH2_CLIENT_SECRET", OAUTH2_SERVER_ICEBERG_CLIENT_SECRET)
        .replace("HTTP_PORT", String.valueOf(GRAVITINO_HTTP_PORT));

    Path configFile = warehouseDir.resolve(GRAVITINO_CONF_FILE_TEMPLATE);
    Files.writeString(configFile, updatedContent);
  }

  private String getJwksUri() {
    String reachableHost = oAuth2AuthorizationServer.getKeycloackContainerDockerInternalHostName();
    int internalPort = 8080; // Keycloak container's internal port
    return oAuth2AuthorizationServer.getIssuer()
        .replace("localhost", reachableHost)
        .replace("127.0.0.1", reachableHost)
        // replace issuer's mapped port with keyclock container's internal port
        .replaceFirst(":[0-9]+", ":" + internalPort);
  }

  @Test
  public void testCliDriver() throws Exception {
    CLI_ADAPTER.runTest(name, qfile);
  }
}
