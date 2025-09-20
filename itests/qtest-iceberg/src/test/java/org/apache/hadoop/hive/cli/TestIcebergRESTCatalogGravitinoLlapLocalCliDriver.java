/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.control.CliAdapter;
import org.apache.hadoop.hive.cli.control.CliConfigs;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hive.CatalogUtils;
import org.apache.iceberg.hive.client.HiveRESTCatalogClient;
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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(Parameterized.class)
public class TestIcebergRESTCatalogGravitinoLlapLocalCliDriver {

  private static final Logger LOG = LoggerFactory.getLogger(TestIcebergRESTCatalogGravitinoLlapLocalCliDriver.class);
  
  private static final String CATALOG_NAME = "ice01";
  private static final String GRAVITINO_CONF_FILE_TEMPLATE = "gravitino-h2-test-template.conf";
  private static final String GRAVITINO_ROOT_DIR = "/root/gravitino-iceberg-rest-server";
  private static final long GRAVITINO_STARTUP_TIMEOUT_MINUTES = 5L;

  private static final String GRAVITINO_STARTUP_SCRIPT = GRAVITINO_ROOT_DIR + "/bin/start-iceberg-rest-server.sh";
  private static final String GRAVITINO_H2_LIB = GRAVITINO_ROOT_DIR + "/libs/h2-driver.jar";
  private static final String GRAVITINO_CONF_FILE = GRAVITINO_ROOT_DIR + "/conf/gravitino-iceberg-rest-server.conf";

  private static final CliAdapter adapter = new CliConfigs.TestIcebergRESTCatalogGravitinoLlapLocalCliDriver().getCliAdapter();
  private static final DockerImageName GRAVITINO_IMAGE =
      DockerImageName.parse("apache/gravitino-iceberg-rest:1.0.0-rc3");

  private final String name;
  private final File qfile;

  private GenericContainer<?> gravitinoContainer;
  private Path warehouseDir;
  private final ExecutorService fileSyncExecutor = Executors.newSingleThreadExecutor();

  @Parameters(name = "{0}")
  public static List<Object[]> getParameters() throws Exception {
    return adapter.getParameters();
  }

  @ClassRule
  public static final TestRule cliClassRule = adapter.buildClassRule();

  @Rule
  public final TestRule cliTestRule = adapter.buildTestRule();

  public TestIcebergRESTCatalogGravitinoLlapLocalCliDriver(String name, File qfile) {
    this.name = name;
    this.qfile = qfile;
  }

  @Before
  public void setup() throws IOException {
    createWarehouseDir();
    prepareGravitinoConfig();
    startGravitinoContainer();
    startWarehouseDirSync();

    String host = gravitinoContainer.getHost();
    Integer port = gravitinoContainer.getMappedPort(9001);
    String restCatalogPrefix = String.format("%s%s.", CatalogUtils.CATALOG_CONFIG_PREFIX, CATALOG_NAME);
    String restCatalogUri = String.format("http://%s:%d/iceberg", host, port);

    Configuration conf = SessionState.get().getConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_IMPL, HiveRESTCatalogClient.class.getName());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, CATALOG_NAME);
    conf.set(restCatalogPrefix + "uri", restCatalogUri);
    conf.set(restCatalogPrefix + "type", CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
  }

  @After
  public void teardown() {
    if (gravitinoContainer != null) {
      gravitinoContainer.stop();
    }

    fileSyncExecutor.shutdownNow();

    if (warehouseDir != null && Files.exists(warehouseDir)) {
      try (var paths = Files.walk(warehouseDir)) {
        paths.sorted(Comparator.reverseOrder())
            .forEach(path -> {
              try { Files.deleteIfExists(path); }
              catch (IOException ignored) {}
            });
      } catch (IOException ignored) {}
    }
  }

  private void startGravitinoContainer() {
    gravitinoContainer = new GenericContainer<>(GRAVITINO_IMAGE)
        .withExposedPorts(9001)
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
        // Wait for the server to be fully started
        .waitingFor(
            new WaitAllStrategy()
                .withStrategy(Wait.forLogMessage(".*GravitinoIcebergRESTServer is running.*\\n", 1)
                    .withStartupTimeout(Duration.ofMinutes(GRAVITINO_STARTUP_TIMEOUT_MINUTES)))
                .withStrategy(Wait.forListeningPort()
                    .withStartupTimeout(Duration.ofMinutes(GRAVITINO_STARTUP_TIMEOUT_MINUTES)))
        )
        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(TestIcebergRESTCatalogGravitinoLlapLocalCliDriver.class)));

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
  private void startWarehouseDirSync() {
    fileSyncExecutor.submit(() -> {
      try {
        while (gravitinoContainer.isRunning()) {
          try (CopyArchiveFromContainerCmd copyArchiveFromContainerCmd = 
                   gravitinoContainer
                       .getDockerClient()
                       .copyArchiveFromContainerCmd(gravitinoContainer.getContainerId(), warehouseDir.toString()); 
               InputStream tarStream = copyArchiveFromContainerCmd.exec();
               TarArchiveInputStream tis = new TarArchiveInputStream(tarStream)) {

            TarArchiveEntry entry;
            while ((entry = tis.getNextTarEntry()) != null) {
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

          Thread.sleep(1000); // sync every 1 second
        }
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    });
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

    String updatedContent = content.replace("/WAREHOUSE_DIR", warehouseDir.toString());
    Path configFile = warehouseDir.resolve(GRAVITINO_CONF_FILE_TEMPLATE);
    Files.writeString(configFile, updatedContent);
  }

  @Test
  public void testCliDriver() throws Exception {
    try {
      adapter.runTest(name, qfile);
    }
    catch (Throwable e) {
      System.err.println("Error running " + qfile);
      throw e;
    }
  }
}