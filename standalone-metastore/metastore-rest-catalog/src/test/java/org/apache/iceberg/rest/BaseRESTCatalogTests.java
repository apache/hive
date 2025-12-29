/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.function.Executable;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class BaseRESTCatalogTests extends CatalogTests<RESTCatalog> {
  private RESTCatalog catalog;

  protected abstract Map<String, String> getDefaultClientConfiguration() throws Exception;

  protected abstract Optional<Map<String, String>> getPermissionTestClientConfiguration() throws Exception;

  @BeforeAll
  void setupAll() throws Exception {
    catalog = RCKUtils.initCatalogClient(getDefaultClientConfiguration());
    Assertions.assertEquals(Collections.singletonList(Namespace.of("default")), catalog.listNamespaces());
  }

  @BeforeEach
  void setup() {
    RCKUtils.purgeCatalogTestEntries(catalog);
  }

  @AfterAll
  void teardownAll() throws Exception {
    catalog.close();
  }

  @Override
  protected RESTCatalog catalog() {
    return catalog;
  }

  @Override
  protected RESTCatalog initCatalog(String catalogName, Map<String, String> additionalProperties) {
    try {
      Map<String, String> properties = new HashMap<>(getDefaultClientConfiguration());
      properties.putAll(additionalProperties);
      return RCKUtils.initCatalogClient(properties);
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

  private void testUnauthorizedAccess(Executable executable) {
    Assertions.assertThrows(ForbiddenException.class, executable);
  }

  @Test
  void testPermissionsWithDeniedUser() throws Exception {
    var properties = getPermissionTestClientConfiguration();
    if (properties.isEmpty()) {
      return;
    }
    var db = Namespace.of("permission_test_db");
    var table = TableIdentifier.of(db, "test_table");
    var view = TableIdentifier.of(db, "test_view");
    try (var client = RCKUtils.initCatalogClient(getDefaultClientConfiguration())) {
      client.createNamespace(db);
      client.createTable(table, new Schema());
      client.buildView(view).withQuery("hive", "SELECT count(*) FROM default.permission_test")
          .withSchema(new Schema()).withDefaultNamespace(db).create();
    } catch (IOException e) {
      throw new AssertionError("Catalog operation failed", e);
    }
    try (var client = RCKUtils.initCatalogClient(properties.get())) {
      // Should this fail?
      Assertions.assertTrue(client.listNamespaces().contains(db));
      testUnauthorizedAccess(() -> client.namespaceExists(db));
      testUnauthorizedAccess(() -> client.loadNamespaceMetadata(db));
      testUnauthorizedAccess(() -> client.createNamespace(Namespace.of("new-db")));
      testUnauthorizedAccess(() -> client.dropNamespace(db));
      testUnauthorizedAccess(() -> client.setProperties(db, Collections.singletonMap("key", "value")));
      testUnauthorizedAccess(() -> client.removeProperties(db, Collections.singleton("key")));

      // Should this fail?
      Assertions.assertEquals(Collections.singletonList(table), client.listTables(db));
      testUnauthorizedAccess(() -> client.tableExists(table));
      testUnauthorizedAccess(() -> client.loadTable(table));
      testUnauthorizedAccess(() -> client.createTable(TableIdentifier.of(db, "new-table"), new Schema()));
      testUnauthorizedAccess(() -> client.renameTable(table, TableIdentifier.of(db, "new-table")));
      testUnauthorizedAccess(() -> client.dropTable(table));

      // Should this fail?
      Assertions.assertEquals(Collections.singletonList(view), client.listViews(db));
      testUnauthorizedAccess(() -> client.viewExists(view));
      testUnauthorizedAccess(() -> client.loadView(view));
      testUnauthorizedAccess(() -> client.buildView(TableIdentifier.of(db, "new-view"))
          .withQuery("hive", "SELECT count(*) FROM default.permission_test").withSchema(new Schema())
          .withDefaultNamespace(db).create());
      testUnauthorizedAccess(() -> client.renameView(view, TableIdentifier.of(db, "new-view")));
      testUnauthorizedAccess(() -> client.dropView(view));

      testUnauthorizedAccess(() -> client.newCreateTableTransaction(TableIdentifier.of(db, "test"),
          new Schema()));
      testUnauthorizedAccess(() -> client.newReplaceTableTransaction(TableIdentifier.of(db, "test"),
          new Schema(), true));
      var dummyMetadata = TableMetadata.newTableMetadata(new Schema(), PartitionSpec.unpartitioned(),
          "dummy-location", Collections.emptyMap());
      testUnauthorizedAccess(() -> client.commitTransaction(TableCommit.create(table, dummyMetadata, dummyMetadata)));
    } catch (IOException e) {
      throw new AssertionError("Catalog operation failed", e);
    }
  }
}
