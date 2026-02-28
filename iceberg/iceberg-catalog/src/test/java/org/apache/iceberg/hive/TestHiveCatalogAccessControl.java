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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TestHiveCatalogAccessControl {
  private static final Schema DUMMY_SCHEMA = new Schema();

  @RegisterExtension
  private static final HiveMetastoreExtension HIVE_METASTORE_EXTENSION = HiveMetastoreExtension.builder()
      .withConfig(Map.of(
          ConfVars.HIVE_AUTHORIZATION_MANAGER.getVarname(), MockHiveAuthorizerFactory.class.getName(),
          ConfVars.PRE_EVENT_LISTENERS.getVarname(), HiveMetaStoreAuthorizer.class.getName()
      )).build();

  @AfterEach
  void afterEach() throws Exception {
    HIVE_METASTORE_EXTENSION.metastore().reset();
  }

  @Test
  void testNamespace() throws Exception {
    var namespace = Namespace.of("permission_test_db");
    asAuthorized(catalog -> catalog.createNamespace(namespace, Collections.emptyMap()));
    asUnauthorized(catalog -> {
      // Should HMS omit namespaces?
      Assertions.assertThat(catalog.listNamespaces()).isEqualTo(List.of(Namespace.of("default"), namespace));
      Assertions.assertThatThrownBy(() -> catalog.namespaceExists(namespace)).isInstanceOf(ForbiddenException.class);
      Assertions.assertThatThrownBy(() -> catalog.loadNamespaceMetadata(namespace))
          .isInstanceOf(ForbiddenException.class);
      var newNamespace = Namespace.of("new_db");
      Assertions.assertThatThrownBy(() -> catalog.createNamespace(newNamespace)).isInstanceOf(ForbiddenException.class);
      Assertions.assertThatThrownBy(() -> catalog.dropNamespace(namespace)).isInstanceOf(ForbiddenException.class);
      var properties = Collections.singletonMap("key", "value");
      Assertions.assertThatThrownBy(() -> catalog.setProperties(namespace, properties))
          .isInstanceOf(ForbiddenException.class);
      var propertyKeys = properties.keySet();
      Assertions.assertThatThrownBy(() -> catalog.removeProperties(namespace, propertyKeys))
          .isInstanceOf(ForbiddenException.class);
    });
  }

  @Test
  void testTable() throws Exception {
    Namespace namespace = Namespace.of("permission_test_db");
    TableIdentifier table = TableIdentifier.of(namespace, "permission_test_table");
    asAuthorized(catalog -> {
      catalog.createNamespace(namespace, Collections.emptyMap());
      catalog.createTable(table, new Schema());
    });
    asUnauthorized(catalog -> {
      // Should HMS omit namespaces?
      Assertions.assertThat(catalog.listTables(namespace)).isEqualTo(Collections.singletonList(table));
      Assertions.assertThatThrownBy(() -> catalog.tableExists(table)).isInstanceOf(ForbiddenException.class);
      Assertions.assertThatThrownBy(() -> catalog.loadTable(table)).isInstanceOf(ForbiddenException.class);
      var newTable = TableIdentifier.of(namespace, "new_table");
      Assertions.assertThatThrownBy(() -> catalog.createTable(newTable, DUMMY_SCHEMA))
          .isInstanceOf(ForbiddenException.class);
      Assertions.assertThatThrownBy(() -> catalog.renameTable(table, newTable)).isInstanceOf(ForbiddenException.class);
      Assertions.assertThatThrownBy(() -> catalog.dropTable(table)).isInstanceOf(ForbiddenException.class);
    });
  }

  @Test
  void testView() throws Exception {
    Namespace namespace = Namespace.of("permission_test_db");
    TableIdentifier view = TableIdentifier.of(namespace, "permission_test_view");
    asAuthorized(catalog -> {
      catalog.createNamespace(namespace, Collections.emptyMap());
      catalog.buildView(view).withQuery("hive", "SELECT 1 AS id").withSchema(new Schema())
          .withDefaultNamespace(namespace).create();
    });
    asUnauthorized(catalog -> {
      // Should HMS omit namespaces?
      Assertions.assertThat(catalog.listViews(namespace)).isEqualTo(Collections.singletonList(view));
      Assertions.assertThatThrownBy(() -> catalog.viewExists(view)).isInstanceOf(ForbiddenException.class);
      Assertions.assertThatThrownBy(() -> catalog.loadView(view)).isInstanceOf(ForbiddenException.class);
      var newView = TableIdentifier.of(namespace, "new_view");
      var builder = catalog.buildView(newView).withQuery("hive", "SELECT 1 AS id").withSchema(DUMMY_SCHEMA)
          .withDefaultNamespace(namespace);
      Assertions.assertThatThrownBy(builder::create).isInstanceOf(ForbiddenException.class);
      Assertions.assertThatThrownBy(() -> catalog.renameView(view, newView)).isInstanceOf(ForbiddenException.class);
      Assertions.assertThatThrownBy(() -> catalog.dropView(view)).isInstanceOf(ForbiddenException.class);
    });
  }

  @Test
  void testTransaction() throws Exception {
    var namespace = Namespace.of("permission_test_db");
    asAuthorized(catalog -> catalog.createNamespace(namespace, Collections.emptyMap()));
    asUnauthorized(catalog -> {
      var newTable = TableIdentifier.of(namespace, "new_table");
      Assertions.assertThatThrownBy(() -> catalog.newCreateTableTransaction(newTable, DUMMY_SCHEMA))
          .isInstanceOf(ForbiddenException.class);
      Assertions.assertThatThrownBy(() -> catalog.newReplaceTableTransaction(newTable, DUMMY_SCHEMA, true))
          .isInstanceOf(ForbiddenException.class);
    });
  }

  private static void asAuthorized(Consumer<HiveCatalog> consumer) throws Exception {
    withUser("authorized_user", consumer);
  }

  private static void asUnauthorized(Consumer<HiveCatalog> consumer) throws Exception {
    withUser(MockHiveAuthorizer.PERMISSION_TEST_USER, consumer);
  }

  private static void withUser(String username, Consumer<HiveCatalog> consumer) throws Exception {
    var ugi = UserGroupInformation.createRemoteUser(username);
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      try (HiveCatalog catalog = createCatalog()) {
        consumer.accept(catalog);
        return null;
      }
    });
  }

  private static HiveCatalog createCatalog() {
    return (HiveCatalog)
        CatalogUtil.loadCatalog(
            HiveCatalog.class.getName(),
            UUID.randomUUID().toString(),
            Map.of(
                CatalogProperties.CLIENT_POOL_CACHE_KEYS, "ugi"
            ),
            HIVE_METASTORE_EXTENSION.hiveConf());
  }
}
