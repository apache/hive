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

package org.apache.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.ServletSecurity;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.extension.HiveRESTCatalogServerExtension;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deferred-authorization state ({@code Authorized}) and owner of Iceberg views, as recorded by HMS
 * and as seen by HiveServer2 reading views through {@code HiveRESTCatalogClient}.
 * <p>HMS does not validate view SQL, so the view references a table that does not exist
 * ({@code db2.t1}).</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("HiveRESTCatalogClient: owner and deferred-authorization state of views")
public class TestHiveRESTCatalogClientViewAuthorization {
  private static final String AUTHORIZED = "Authorized";
  private static final String CATALOG_NAME = "ice01";
  private static final String DB_NAME = "db1";
  private static final String VIEW_NAME = "v1";
  private static final String VIEW_SQL = "SELECT * FROM db2.t1";
  private static final String HIVE_ICEBERG_STORAGE_HANDLER = "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler";
  private static final String REST_CATALOG_PREFIX =
      String.format("%s%s.", IcebergCatalogProperties.CATALOG_CONFIG_PREFIX, CATALOG_NAME);

  private static final String HS2_VIEW_NAME = "hs2_view";
  private static final String HS2_USER = "hive";
  private static final String USER = "user1";

  // As in a typical deployment, the HiveServer2 service user is a proxy user, which
  // HiveMetaStoreAuthorizer treats as a superuser; the end user creating views is not.
  @RegisterExtension
  private static final HiveRESTCatalogServerExtension REST_CATALOG_EXTENSION =
      HiveRESTCatalogServerExtension.builder(ServletSecurity.AuthType.SIMPLE)
          .addMetaStoreSchemaClassName(ITestsSchemaInfo.class)
          .configure("hadoop.proxyuser." + HS2_USER + ".hosts", "*")
          .configure("hadoop.proxyuser." + HS2_USER + ".groups", "*")
          .build();

  private RESTCatalog userCatalog;
  private IMetaStoreClient hmsClient;
  private Hive hive;

  @BeforeEach
  void setup() throws Exception {
    userCatalog = new RESTCatalog();
    userCatalog.setConf(new Configuration());
    userCatalog.initialize("user", Map.of(
        "uri", REST_CATALOG_EXTENSION.getRestEndpoint(),
        "header.x-actor-username", USER));
    userCatalog.createNamespace(Namespace.of(DB_NAME));
    userCatalog.buildView(TableIdentifier.of(DB_NAME, VIEW_NAME))
        .withQuery("hive", VIEW_SQL)
        .withSchema(new Schema(Types.NestedField.optional(1, "salary", Types.IntegerType.get())))
        .withDefaultNamespace(Namespace.of(DB_NAME))
        // Not used as owner: the REST catalog records the caller
        .withProperty(HiveCatalog.HMS_TABLE_OWNER, HS2_USER)
        .create();

    hmsClient = REST_CATALOG_EXTENSION.newMetastoreClient();

    // HiveServer2 configured to use the HMS REST catalog through HiveRESTCatalogClient
    Configuration conf = new Configuration(REST_CATALOG_EXTENSION.getConf());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_IMPL,
        "org.apache.iceberg.hive.rest.catalog.client.HiveRESTCatalogClient");
    conf.set(MetastoreConf.ConfVars.CATALOG_DEFAULT.getVarname(), CATALOG_NAME);
    conf.set(REST_CATALOG_PREFIX + "uri", REST_CATALOG_EXTENSION.getRestEndpoint());
    conf.set(REST_CATALOG_PREFIX + "type", CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    conf.set(REST_CATALOG_PREFIX + "header.x-actor-username", HS2_USER);
    hive = Hive.get(new HiveConf(conf, HiveConf.class));
  }

  @AfterEach
  void teardown() throws Exception {
    Hive.closeCurrent();
    userCatalog.dropView(TableIdentifier.of(DB_NAME, VIEW_NAME));
    userCatalog.dropNamespace(Namespace.of(DB_NAME));
    userCatalog.close();
    hmsClient.close();
  }

  @DisplayName("HMS records Authorized=false and the caller as owner for a REST client view")
  @Test
  void metastoreRecordsViewAsDeferredAuthorization() throws Exception {
    org.apache.hadoop.hive.metastore.api.Table hmsView = hmsClient.getTable(DB_NAME, VIEW_NAME);
    assertEquals("false", hmsView.getParameters().get(AUTHORIZED));
    assertEquals(USER, hmsView.getOwner());
  }

  @DisplayName("HiveRESTCatalogClient returns the Authorized parameter recorded in HMS")
  @Test
  void restCatalogClientKeepsDeferredAuthorizationMarker() throws Exception {
    Table view = hive.getTable(DB_NAME, VIEW_NAME);

    assertTrue(view.isView());
    assertEquals(VIEW_SQL, view.getViewExpandedText());
    assertEquals("false", view.getParameters().get(AUTHORIZED),
        "view loaded through HiveRESTCatalogClient must carry the Authorized parameter recorded in HMS");
    assertEquals(USER, view.getOwner());
  }

  /**
   * A view created by HiveServer2 (a superuser for HMS) has no Authorized=false parameter, and has
   * none either when read back through HiveRESTCatalogClient.
   */
  @DisplayName("View created by HiveServer2 stays authorized")
  @Test
  void viewCreatedByHiveServer2StaysAuthorized() throws Exception {
    org.apache.hadoop.hive.metastore.api.Table view = new org.apache.hadoop.hive.metastore.api.Table();
    view.setCatName(CATALOG_NAME);
    view.setDbName(DB_NAME);
    view.setTableName(HS2_VIEW_NAME);
    view.setTableType(TableType.VIRTUAL_VIEW.toString());
    view.setViewOriginalText("select 1 as x");
    view.setViewExpandedText("select 1 as x");
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(Collections.singletonList(new FieldSchema("x", "int", "")));
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setParameters(new HashMap<>());
    view.setSd(sd);
    view.setParameters(new HashMap<>());
    view.getParameters().put(hive_metastoreConstants.META_TABLE_STORAGE, HIVE_ICEBERG_STORAGE_HANDLER);
    view.getParameters().put("view-format", "iceberg");
    try {
      hive.getMSC().createTable(view);

      assertNull(hmsClient.getTable(DB_NAME, HS2_VIEW_NAME).getParameters().get(AUTHORIZED),
          "HMS must not mark a view written by the HiveServer2 service user as deferred");
      Table loaded = hive.getTable(DB_NAME, HS2_VIEW_NAME);
      assertNull(loaded.getParameters().get(AUTHORIZED),
          "HiveServer2 must see its own view as authorized when reading it back");
    } finally {
      userCatalog.dropView(TableIdentifier.of(DB_NAME, HS2_VIEW_NAME));
    }
  }
}
