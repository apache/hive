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

package org.apache.hadoop.hive.metastore.minihms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrashPolicy;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import java.io.IOException;
import java.util.Map;

/**
 * The tests should use this abstract class to access the MetaStore services.
 * This abstract class ensures, that the same tests could be run against the different MetaStore
 * configurations.
 */
public abstract class AbstractMetaStoreService {
  private Configuration configuration;
  private Warehouse warehouse;
  private FileSystem warehouseRootFs;
  private Path trashDir;

  public AbstractMetaStoreService(Configuration configuration) {
    this.configuration = new Configuration(configuration);
  }

  /**
   * Returns the actual configuration of the MetaStore.
   * @return The actual configuration
   */
  protected Configuration getConfiguration() {
    return configuration;
  }

  /**
   * Starts the MetaStoreService. Be aware, as the current MetaStore does not implement clean
   * shutdown, starting MetaStoreService is possible only once per test.
   *
   * @throws Exception if any Exception occurs
   */
  public void start() throws Exception {
    warehouse = new Warehouse(configuration);
    warehouseRootFs = warehouse.getFs(warehouse.getWhRoot());
    TrashPolicy trashPolicy = TrashPolicy.getInstance(configuration, warehouseRootFs);
    trashDir = trashPolicy.getCurrentTrashDir();
  }

  /**
   * Starts the service with adding extra configuration to the default ones. Be aware, as the
   * current MetaStore does not implement clean shutdown, starting MetaStoreService is possible only
   * once per test.
   *
   * @param metastoreOverlay The extra metastore parameters which should be set before starting the
   *          service
   * @param configurationOverlay The extra other parameters which should be set before starting the
   *          service
   * @throws Exception if any Exception occurs
   */
  public void start(Map<MetastoreConf.ConfVars, String> metastoreOverlay,
      Map<String, String> configurationOverlay) throws Exception {
    // Set metastoreOverlay parameters
    for (Map.Entry<MetastoreConf.ConfVars, String> entry : metastoreOverlay.entrySet()) {
      MetastoreConf.setVar(configuration, entry.getKey(), entry.getValue());
    }
    // Set other configurationOverlay parameters
    for (Map.Entry<String, String> entry : configurationOverlay.entrySet()) {
      configuration.set(entry.getKey(), entry.getValue());
    }
    // Start the service
    start();
  }

  /**
   * Returns the MetaStoreClient for this MetaStoreService.
   *
   * @return The client connected to this service
   * @throws MetaException if any Exception occurs during client configuration
   */
  public HiveMetaStoreClient getClient() throws MetaException {
    return new HiveMetaStoreClient(configuration);
  }

  /**
   * Returns the MetaStore Warehouse root directory name.
   *
   * @return The warehouse root directory
   * @throws MetaException IO failure
   */
  public Path getWarehouseRoot() throws MetaException {
    return warehouse.getWhRoot();
  }

  /**
   * Returns the External MetaStore Warehouse root directory name.
   *
   * @return The external warehouse root directory
   * @throws MetaException IO failure
   */
  public Path getExternalWarehouseRoot() throws MetaException {
    return warehouse.getWhRootExternal();
  }

  /**
   * Check if a path exists.
   *
   * @param path The path to check
   * @return true if the path exists
   * @throws IOException IO failure
   */
  public boolean isPathExists(Path path) throws IOException {
    return warehouseRootFs.exists(path);
  }

  /**
   * Check if a path exists in the thrash directory.
   *
   * @param path The path to check
   * @return True if the path exists
   * @throws IOException IO failure
   */
  public boolean isPathExistsInTrash(Path path) throws IOException {
    Path pathInTrash = new Path(trashDir.toUri().getScheme(), trashDir.toUri().getAuthority(),
        trashDir.toUri().getPath() + path.toUri().getPath());
    return isPathExists(pathInTrash);
  }

  /**
   * Creates a file on the given path.
   *
   * @param path Destination path
   * @param content The content of the file
   * @throws IOException IO failure
   */
  public void createFile(Path path, String content) throws IOException {
    FSDataOutputStream outputStream = warehouseRootFs.create(path);
    outputStream.write(content.getBytes());
    outputStream.close();
  }

  /**
   * Cleans the warehouse and the thrash dirs in preparation for the tests.
   *
   * @throws MetaException IO failure
   */
  public void cleanWarehouseDirs() throws MetaException {
    warehouse.deleteDir(getWarehouseRoot(), true, true, false);
    warehouse.deleteDir(getExternalWarehouseRoot(), true, true, false);
    warehouse.deleteDir(trashDir, true, true, false);
  }

  /**
   * Stops the MetaStoreService. When MetaStore will implement clean shutdown, this method will
   * call shutdown on MetaStore. Currently this does nothing :(
   */
  public void stop() {
  }

  public Configuration getConf() {
    return configuration;
  }
}
