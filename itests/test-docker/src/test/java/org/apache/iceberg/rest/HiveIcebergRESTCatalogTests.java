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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.exceptions.NotFoundException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HiveIcebergRESTCatalogTests extends CatalogTests<RESTCatalog> {
  private static HiveIcebergRESTCatalogClient client;

  @BeforeAll
  static void beforeClass() throws Exception {
    client = new HiveIcebergRESTCatalogClient();

    assertThat(client.getRestCatalog().listNamespaces())
        .withFailMessage("Namespaces list should not contain: %s", RCKUtils.TEST_NAMESPACES)
        .doesNotContainAnyElementsOf(RCKUtils.TEST_NAMESPACES);
  }

  @BeforeEach
  void before() throws Exception {
    client.cleanupWarehouse();
  }

  @AfterAll
  static void afterClass() throws Exception {
    client.close();
  }

  @Override
  protected RESTCatalog catalog() {
    return client.getRestCatalog();
  }

  @Override
  protected RESTCatalog initCatalog(String catalogName, Map<String, String> additionalProperties) {
    try {
      return new HiveIcebergRESTCatalogClient(additionalProperties).getRestCatalog();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsNamesWithSlashes() {
    return false;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  /**
   * Checks that loading a table fails clearly when its metadata file is missing.
   *
   * <p>The base Iceberg test moves the metadata file with {@code Files.move()} which only works
   * on a local disk. Docker tests store data on S3, so this override renames the metadata file
   * with Hadoop {@link FileSystem} instead, then puts it back in {@code finally}.
   */
  @Test
  @Override
  public void testLoadTableWithMissingMetadataFile(@TempDir java.nio.file.Path tempDir)
      throws IOException {
    RESTCatalog catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TBL.namespace());
    }

    catalog.buildTable(TBL, SCHEMA).create();
    assertThat(catalog.tableExists(TBL)).as("Table should exist").isTrue();

    Table table = catalog.loadTable(TBL);
    String metadataFileLocation =
        ((HasTableOperations) table).operations().current().metadataFileLocation();
    Path metadataPath = new Path(metadataFileLocation);
    Path backupPath = metadataPath.suffix(".bak");
    FileSystem fs = metadataPath.getFileSystem(client.getHadoopConf());

    try {
      assertThat(fs.rename(metadataPath, backupPath)).isTrue();
      assertThatThrownBy(() -> catalog.loadTable(TBL))
          .isInstanceOf(NotFoundException.class)
          .hasMessageContaining("Failed to open input stream for file: " + metadataFileLocation);
    } finally {
      fs.rename(backupPath, metadataPath);
    }
  }
}
