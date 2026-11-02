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

package org.apache.iceberg.rest;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.ServletSecurity.AuthType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveHadoopUtil;
import org.apache.iceberg.hive.MetadataLocator;
import org.apache.iceberg.rest.extension.HiveRESTCatalogServerExtension;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Owner and deferred-authorization state ({@code Authorized}) of tables and views written through the
 * REST catalog: what HMS records, what the Iceberg metadata file holds, and what the REST catalog reports.
 * <p>HMS does not validate view SQL, so the views below reference tables that do not exist
 * ({@code db2.*}).</p>
 */
@Category(MetastoreCheckinTest.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("REST catalog: owner and deferred-authorization state of views")
class TestRESTViewDeferredAuthorization {
  private static final String AUTHORIZED = "Authorized";
  private static final String USER = "user1";
  // A proxy user, which HiveMetaStoreAuthorizer treats as a superuser, like the HiveServer2 service user
  private static final String SUPERUSER = "hive";
  private static final String DB = "default";
  private static final String[] RESERVED_PROPERTIES = {
      AUTHORIZED, HiveCatalog.VIEW_AUTHORIZED_PROPERTY, HiveCatalog.HMS_TABLE_OWNER};

  @RegisterExtension
  private static final HiveRESTCatalogServerExtension REST_CATALOG_EXTENSION =
      HiveRESTCatalogServerExtension.builder(AuthType.SIMPLE)
          .configure("hadoop.proxyuser." + SUPERUSER + ".hosts", "*")
          .configure("hadoop.proxyuser." + SUPERUSER + ".groups", "*")
          .build();

  private RESTCatalog catalog;
  private RESTCatalog superuserCatalog;
  // Reads the Iceberg metadata files directly, without going through the REST server
  private HiveCatalog serverCatalog;
  private IMetaStoreClient hmsClient;

  @BeforeAll
  void setup() throws Exception {
    catalog = restCatalog("user", USER);
    superuserCatalog = restCatalog("superuser", SUPERUSER);
    serverCatalog = REST_CATALOG_EXTENSION.newServerCatalog();
    hmsClient = REST_CATALOG_EXTENSION.newMetastoreClient();
  }

  private static RESTCatalog restCatalog(String name, String user) {
    RESTCatalog restCatalog = new RESTCatalog();
    restCatalog.setConf(new Configuration());
    restCatalog.initialize(name, Map.of(
        "uri", REST_CATALOG_EXTENSION.getRestEndpoint(),
        "header.x-actor-username", user));
    return restCatalog;
  }

  @AfterAll
  void teardown() throws Exception {
    hmsClient.close();
    serverCatalog.close();
    superuserCatalog.close();
    catalog.close();
  }

  private static void createView(RESTCatalog restCatalog, TableIdentifier id, String sql, Map<String, String> props) {
    restCatalog.buildView(id)
        .withQuery("hive", sql)
        .withSchema(new Schema())
        .withDefaultNamespace(Namespace.of(DB))
        .withProperties(props)
        .createOrReplace();
  }

  /** HMS-managed properties are not stored in the Iceberg metadata file. */
  private void assertMetadataFileHasNoReservedProperties(TableIdentifier id) {
    assertThat(serverCatalog.loadView(id).properties()).doesNotContainKeys(RESERVED_PROPERTIES);
  }

  @DisplayName("Create view: HMS records the caller as owner and Authorized=false")
  @Test
  void createViewIsMarkedForDeferredAuthorization() throws Exception {
    TableIdentifier id = TableIdentifier.of(DB, "deferred_create");
    try {
      // HMS-managed properties in the request are not used
      createView(catalog, id, "SELECT * FROM db2.t1", Map.of(
          HiveCatalog.HMS_TABLE_OWNER, SUPERUSER,
          HiveCatalog.VIEW_AUTHORIZED_PROPERTY, "true",
          AUTHORIZED, "true"));

      Table hmsView = hmsClient.getTable(DB, "deferred_create");
      assertThat(hmsView.getOwner()).isEqualTo(USER);
      assertThat(hmsView.getParameters()).containsEntry(AUTHORIZED, "false");
      assertThat(catalog.loadView(id).properties())
          .containsEntry(HiveCatalog.VIEW_AUTHORIZED_PROPERTY, "false")
          .containsEntry(HiveCatalog.HMS_TABLE_OWNER, USER);
      assertMetadataFileHasNoReservedProperties(id);
    } finally {
      catalog.dropView(id);
    }
  }

  @DisplayName("Create table: HMS records the caller as owner")
  @Test
  void createTableIgnoresClientSuppliedOwner() throws Exception {
    TableIdentifier id = TableIdentifier.of(DB, "owned_table");
    try {
      catalog.buildTable(id, new Schema())
          .withProperty(HiveCatalog.HMS_TABLE_OWNER, SUPERUSER)
          .create();
      assertThat(hmsClient.getTable(DB, "owned_table").getOwner()).isEqualTo(USER);
    } finally {
      catalog.dropTable(id);
    }
  }

  @DisplayName("Table property update keeps the recorded owner")
  @Test
  void updateTablePropertiesCannotChangeOwner() throws Exception {
    TableIdentifier id = TableIdentifier.of(DB, "owned_table_update");
    try {
      catalog.buildTable(id, new Schema()).create();
      catalog.loadTable(id).updateProperties().set(HiveCatalog.HMS_TABLE_OWNER, SUPERUSER).commit();

      Table hmsTable = hmsClient.getTable(DB, "owned_table_update");
      assertThat(hmsTable.getOwner()).isEqualTo(USER);
      assertThat(hmsTable.getParameters()).doesNotContainKey(HiveCatalog.HMS_TABLE_OWNER);
    } finally {
      catalog.dropTable(id);
    }
  }

  @DisplayName("Replace view: HMS keeps Authorized=false")
  @Test
  void replaceViewIsMarkedForDeferredAuthorization() throws Exception {
    TableIdentifier id = TableIdentifier.of(DB, "deferred_replace");
    try {
      createView(catalog, id, "SELECT 1", Map.of());
      // Marker properties in the request are not used
      createView(catalog, id, "SELECT * FROM db2.t2", Map.of(
          AUTHORIZED, "true",
          HiveCatalog.VIEW_AUTHORIZED_PROPERTY, "true"));

      Table hmsView = hmsClient.getTable(DB, "deferred_replace");
      assertThat(hmsView.getParameters())
          .containsEntry(AUTHORIZED, "false")
          .doesNotContainKey(HiveCatalog.VIEW_AUTHORIZED_PROPERTY);
      assertThat(catalog.loadView(id).properties())
          .containsEntry(HiveCatalog.VIEW_AUTHORIZED_PROPERTY, "false");
      assertMetadataFileHasNoReservedProperties(id);
    } finally {
      catalog.dropView(id);
    }
  }

  @DisplayName("View property update keeps the recorded owner and Authorized state")
  @Test
  void updateViewPropertiesCannotChangeOwnerOrMarker() throws Exception {
    TableIdentifier id = TableIdentifier.of(DB, "deferred_update");
    try {
      createView(catalog, id, "SELECT * FROM db2.t1", Map.of());
      catalog.loadView(id).updateProperties()
          .set(HiveCatalog.HMS_TABLE_OWNER, SUPERUSER)
          .set(HiveCatalog.VIEW_AUTHORIZED_PROPERTY, "true")
          .set(AUTHORIZED, "true")
          .set("comment", "kept")
          .commit();

      Table hmsView = hmsClient.getTable(DB, "deferred_update");
      assertThat(hmsView.getOwner()).isEqualTo(USER);
      assertThat(hmsView.getParameters()).containsEntry(AUTHORIZED, "false");
      assertThat(catalog.loadView(id).properties())
          .containsEntry(HiveCatalog.VIEW_AUTHORIZED_PROPERTY, "false")
          .containsEntry(HiveCatalog.HMS_TABLE_OWNER, USER)
          // Other properties of the same update are kept
          .containsEntry("comment", "kept");
      assertMetadataFileHasNoReservedProperties(id);
    } finally {
      catalog.dropView(id);
    }
  }

  /**
   * A registered metadata file may hold HMS-managed properties: HMS state and the REST response do not
   * take them from the file.
   */
  @DisplayName("Register view: owner and Authorized state come from HMS, not the metadata file")
  @Test
  void registerViewUsesHmsOwnerAndAuthorizedState() throws Exception {
    TableIdentifier source = TableIdentifier.of(DB, "source_view");
    TableIdentifier registered = TableIdentifier.of(DB, "registered_view");
    // Written by the server-side catalog, so the properties are stored in the metadata file
    serverCatalog.buildView(source)
        .withQuery("hive", "SELECT * FROM db2.t1")
        .withSchema(new Schema())
        .withDefaultNamespace(Namespace.of(DB))
        .withProperty(HiveCatalog.HMS_TABLE_OWNER, SUPERUSER)
        .withProperty(HiveCatalog.VIEW_AUTHORIZED_PROPERTY, "true")
        .withProperty(AUTHORIZED, "true")
        .create();
    try {
      String metadataLocation = hmsClient.getTable(DB, "source_view").getParameters()
          .get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
      catalog.registerView(registered, metadataLocation);

      Table hmsView = hmsClient.getTable(DB, "registered_view");
      assertThat(hmsView.getOwner()).isEqualTo(USER);
      assertThat(hmsView.getParameters())
          .containsEntry(AUTHORIZED, "false")
          .doesNotContainKey(HiveCatalog.VIEW_AUTHORIZED_PROPERTY)
          .doesNotContainKey(HiveCatalog.HMS_TABLE_OWNER);
      assertThat(catalog.loadView(registered).properties())
          .containsEntry(HiveCatalog.VIEW_AUTHORIZED_PROPERTY, "false")
          .containsEntry(HiveCatalog.HMS_TABLE_OWNER, USER);
    } finally {
      // Both views share the metadata files: drop the registered one from HMS only, keeping the files for the source
      hmsClient.dropTable(DB, "registered_view", false, true);
      serverCatalog.dropView(source);
    }
  }

  /** A view written by a superuser has no Authorized=false marker in HMS. */
  @DisplayName("Superuser view is reported as authorized")
  @Test
  void superuserViewIsReportedAuthorized() throws Exception {
    TableIdentifier id = TableIdentifier.of(DB, "superuser_view");
    try {
      createView(superuserCatalog, id, "SELECT 1", Map.of());

      Table hmsView = hmsClient.getTable(DB, "superuser_view");
      assertThat(hmsView.getOwner()).isEqualTo(SUPERUSER);
      assertThat(hmsView.getParameters()).doesNotContainKey(AUTHORIZED);
      assertThat(superuserCatalog.loadView(id).properties())
          .containsEntry(HiveCatalog.VIEW_AUTHORIZED_PROPERTY, "true")
          .containsEntry(HiveCatalog.HMS_TABLE_OWNER, SUPERUSER);
      // The reported state does not depend on the reader
      assertThat(catalog.loadView(id).properties()).containsEntry(HiveCatalog.VIEW_AUTHORIZED_PROPERTY, "true");
    } finally {
      superuserCatalog.dropView(id);
    }
  }

  /** Authorized=false set by a non-superuser write is kept on a later superuser update. */
  @DisplayName("Superuser update keeps Authorized=false")
  @Test
  void deferredMarkerIsStickyAcrossSuperuserUpdate() throws Exception {
    TableIdentifier id = TableIdentifier.of(DB, "deferred_sticky");
    try {
      createView(catalog, id, "SELECT * FROM db2.t1", Map.of());
      createView(superuserCatalog, id, "SELECT 1", Map.of());

      Table hmsView = hmsClient.getTable(DB, "deferred_sticky");
      assertThat(hmsView.getOwner()).isEqualTo(USER);
      assertThat(hmsView.getParameters()).containsEntry(AUTHORIZED, "false");
      assertThat(superuserCatalog.loadView(id).properties())
          .containsEntry(HiveCatalog.VIEW_AUTHORIZED_PROPERTY, "false");
    } finally {
      catalog.dropView(id);
    }
  }

  private static Table hmsView(String owner, String metadataLocation, String authorized) {
    Table hmsView = new Table();
    hmsView.setOwner(owner);
    Map<String, String> parameters = new HashMap<>();
    parameters.put(BaseMetastoreTableOperations.METADATA_LOCATION_PROP, metadataLocation);
    if (authorized != null) {
      parameters.put(AUTHORIZED, authorized);
    }
    hmsView.setParameters(parameters);
    return hmsView;
  }

  @DisplayName("View state: authorized only when HMS matches the returned metadata version")
  @Test
  void viewStateReportsAuthorizedOnlyForTheReturnedVersion() {
    assertThat(HMSCatalogAdapter.viewState(hmsView(SUPERUSER, "v1.json", null), "v1.json"))
        .containsEntry(HiveCatalog.VIEW_AUTHORIZED_PROPERTY, "true")
        .containsEntry(HiveCatalog.HMS_TABLE_OWNER, SUPERUSER);
    assertThat(HMSCatalogAdapter.viewState(hmsView(USER, "v1.json", "false"), "v1.json"))
        .containsEntry(HiveCatalog.VIEW_AUTHORIZED_PROPERTY, "false")
        .containsEntry(HiveCatalog.HMS_TABLE_OWNER, USER);
    // HMS records another metadata version than the one returned
    assertThat(HMSCatalogAdapter.viewState(hmsView(SUPERUSER, "v2.json", null), "v1.json"))
        .containsEntry(HiveCatalog.VIEW_AUTHORIZED_PROPERTY, "false");
    assertThat(HMSCatalogAdapter.viewState(hmsView(SUPERUSER, "v1.json", null), null))
        .containsEntry(HiveCatalog.VIEW_AUTHORIZED_PROPERTY, "false");
    // No HMS object
    assertThat(HMSCatalogAdapter.viewState(null, "v1.json"))
        .containsEntry(HiveCatalog.VIEW_AUTHORIZED_PROPERTY, "false")
        .doesNotContainKey(HiveCatalog.HMS_TABLE_OWNER);
  }

  @DisplayName("Client-supplied owner setting: off by default, caller conf unchanged")
  @Test
  void clientSuppliedOwnerIsOffByDefaultAndCallerConfUnchanged() throws Exception {
    Configuration conf = new Configuration(REST_CATALOG_EXTENSION.getConf());
    assertThat(HMSCatalogFactory.isClientSuppliedOwnerEnabled(conf)).isFalse();
    try (HiveCatalog hiveCatalog = HMSCatalogFactory.createHiveCatalog(conf)) {
      assertThat(hiveCatalog.getConf().getBoolean(HiveHadoopUtil.CLIENT_SUPPLIED_OWNER_ENABLED, true)).isFalse();
      assertThat(conf.get(HiveHadoopUtil.CLIENT_SUPPLIED_OWNER_ENABLED)).isNull();
    }

    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_CLIENT_SUPPLIED_OWNER_ENABLED, true);
    assertThat(HMSCatalogFactory.isClientSuppliedOwnerEnabled(conf)).isTrue();
    try (HiveCatalog hiveCatalog = HMSCatalogFactory.createHiveCatalog(conf)) {
      assertThat(hiveCatalog.getConf().getBoolean(HiveHadoopUtil.CLIENT_SUPPLIED_OWNER_ENABLED, false)).isTrue();
    }
  }

  @DisplayName("HMS lookups report a missing database as not found")
  @Test
  void lookupsInMissingDatabaseReturnNull() {
    MetadataLocator locator = new MetadataLocator(serverCatalog);
    TableIdentifier id = TableIdentifier.of("no_such_db", "v1");

    assertThat(locator.getOwnerAndParameters(id)).isNull();
    assertThat(locator.getLocation(id)).isNull();
  }
}
