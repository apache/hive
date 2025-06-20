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
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.types.Types;
import static org.apache.iceberg.types.Types.NestedField.required;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestHMSCatalog extends HMSTestBase {
  public TestHMSCatalog() {
    super();
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
      super.setUp();
  }
  
  @After
  @Override
  public void tearDown() throws Exception {
      super.tearDown();
  }

  @Test
  public void testCreateNamespaceHttp() throws Exception {
    String ns = "nstesthttp";
    // list namespaces
    URL url = new URL("http://hive@localhost:" + catalogPort + "/"+catalogPath+"/v1/namespaces");
    String jwt = generateJWT();
    // check namespaces list (ie 0)
    Object response = clientCall(jwt, url, "GET", null);
    Assert.assertTrue(response instanceof Map);
    Map<String, Object> nsrep = (Map<String, Object>) response;
    List<List<String>> nslist = (List<List<String>>) nsrep.get("namespaces");
    Assert.assertEquals(2, nslist.size());
    Assert.assertTrue((nslist.contains(Collections.singletonList("default"))));
    Assert.assertTrue((nslist.contains(Collections.singletonList("hivedb"))));
    // succeed
    response = clientCall(jwt, url, "POST", false, "{ \"namespace\" : [ \""+ns+"\" ], "+
            "\"properties\":{ \"owner\": \"apache\", \"group\" : \"iceberg\" }"
            +"}");
    Assert.assertNotNull(response);
    HiveMetaStoreClient client = createClient(conf);
    Database database1 = client.getDatabase(ns);
    Assert.assertEquals("apache", database1.getParameters().get("owner"));
    Assert.assertEquals("iceberg", database1.getParameters().get("group"));

    Assert.assertSame(HMSCachingCatalog.class, catalog.getClass());
    List<TableIdentifier> tis = catalog.listTables(Namespace.of(ns));
    Assert.assertTrue(tis.isEmpty());

    // list tables in hivedb
    url = new URL("http://hive@localhost:" + catalogPort + "/" + catalogPath+"/v1/namespaces/" + ns + "/tables");
    // succeed
    response = clientCall(jwt, url, "GET", null);
    Assert.assertNotNull(response);

    // quick check on metrics
    Map<String, Long> counters = reportMetricCounters("list_namespaces", "list_tables");
    counters.forEach((key, value) -> Assert.assertTrue(key, value > 0));
  }
  
  private Schema getTestSchema() {
    return new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get()));
  }
  
  @Test
  public void testCreateTableTxnBuilder() throws Exception {
    URI iceUri = URI.create("http://hive@localhost:" + catalogPort + "/"+catalogPath+"/v1/");
    String jwt = generateJWT();
    Schema schema = getTestSchema();
    final String tblName = "tbl_" + Integer.toHexString(RND.nextInt(65536));
    final TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, tblName);
    String location = temp.newFolder(tableIdent.toString()).toString();

    try {
      Transaction txn = catalog.buildTable(tableIdent, schema)
          .withLocation(location)
          .createTransaction();
      txn.commitTransaction();
      Table table = catalog.loadTable(tableIdent);

      Assert.assertEquals(location, table.location());
      Assert.assertEquals(2, table.schema().columns().size());
      Assert.assertTrue(table.spec().isUnpartitioned());
      List<TableIdentifier> tis = catalog.listTables(Namespace.of(DB_NAME));
      Assert.assertFalse(tis.isEmpty());

      // list namespaces
      URL url = iceUri.resolve("namespaces").toURL();
      // succeed
      Object response = clientCall(jwt, url, "GET", null);
      Assert.assertNotNull(response);
      Assert.assertEquals(200, (int) eval(response, "json -> json.status"));
      List<List<String>> nslist = (List<List<String>>) eval(response, "json -> json.namespaces");
      Assert.assertEquals(2, nslist.size());
      Assert.assertTrue((nslist.contains(Collections.singletonList("default"))));
      Assert.assertTrue((nslist.contains(Collections.singletonList("hivedb"))));

      // list tables in hivedb
      url = iceUri.resolve("namespaces/" + DB_NAME + "/tables").toURL();
      // succeed
      response = clientCall(jwt, url, "GET", null);
      Assert.assertNotNull(response);
      Assert.assertEquals(200, (int) eval(response, "json -> json.status"));
      Assert.assertEquals(1, (int) eval(response, "json -> size(json.identifiers)"));
      Assert.assertEquals(tblName, eval(response, "json -> json.identifiers[0].name"));

      // load table
      url = iceUri.resolve("namespaces/" + DB_NAME + "/tables/" + tblName).toURL();
      // succeed
      response = clientCall(jwt, url, "GET", null);
      Assert.assertNotNull(response);
      Assert.assertEquals(200, (int) eval(response, "json -> json.status"));
      Assert.assertEquals(location, eval(response, "json -> json.metadata.location"));

      // quick check on metrics
      Map<String, Long> counters = reportMetricCounters("list_namespaces", "list_tables", "load_table");
      counters.forEach((key, value) -> Assert.assertTrue(key, value > 0));
      table = catalog.loadTable(tableIdent);
      Assert.assertNotNull(table);
    } catch (IOException xany) {
        Assert.fail(xany.getMessage());
    } finally {
      catalog.dropTable(tableIdent, false);
    }
  }

  @Test
  public void testTableAPI() throws Exception {
    Assert.assertSame(HMSCachingCatalog.class, catalog.getClass());
    URI iceUri = URI.create("http://hive@localhost:" + catalogPort + "/"+catalogPath+"/v1/");
    String jwt = generateJWT();
    Schema schema = getTestSchema();
    final String tblName = "tbl_" + Integer.toHexString(RND.nextInt(65536));
    final TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, tblName);
    String location = temp.newFolder(tableIdent.toString()).toString();
      // create table
    CreateTableRequest create = CreateTableRequest.builder().
             withName(tblName).
             withLocation(location).
             withSchema(schema).build();
      URL url = iceUri.resolve("namespaces/" + DB_NAME + "/tables").toURL();
      Object response = clientCall(jwt, url, "POST", create);
      Assert.assertNotNull(response);
      Assert.assertEquals(200, (int) eval(response, "json -> json.status"));
      Assert.assertEquals(location, eval(response, "json -> json.metadata.location"));
      Table table = catalog.loadTable(tableIdent);
      Assert.assertEquals(location, table.location());
      
      // rename table
      final String rtblName = "TBL_" + Integer.toHexString(RND.nextInt(65536));
      final TableIdentifier rtableIdent = TableIdentifier.of(DB_NAME, rtblName);
      RenameTableRequest rename = RenameTableRequest.builder().
              withSource(tableIdent).
              withDestination(rtableIdent).
              build();
      url = iceUri.resolve("tables/rename").toURL();
      response = clientCall(jwt, url, "POST", rename);
      Assert.assertNotNull(response);
      Assert.assertEquals(200, (int) eval(response, "json -> json.status"));
      table = catalog.loadTable(rtableIdent);
      Assert.assertEquals(location, table.location());
      
     // delete table
      url = iceUri.resolve("namespaces/" + DB_NAME + "/tables/" + rtblName).toURL();
      response = clientCall(jwt, url, "DELETE", null);
      Assert.assertNotNull(response);
      Assert.assertEquals(200, (int) eval(response, "json -> json.status"));
      Assert.assertThrows(NoSuchTableException.class, () -> catalog.loadTable(rtableIdent));
  }

  public static final String CATALOG_CONFIG_PREFIX = "iceberg.catalog.";
  public static final String CATALOG_NAME = "iceberg.catalog";

  private static Map<String, String> getCatalogPropertiesFromConf(Configuration conf, String catalogName) {
    Map<String, String> catalogProperties = Maps.newHashMap();
    String keyPrefix = CATALOG_CONFIG_PREFIX + catalogName;
    conf.forEach(config -> {
      if (config.getKey().startsWith(keyPrefix)) {
        catalogProperties.put(
                config.getKey().substring(keyPrefix.length() + 1),
                config.getValue());
      }
    });
    return catalogProperties;
  }

  @Test
  public void testBuildTable() throws Exception {
    String cname = catalog.name();
    URI iceUri = URI.create("http://localhost:" + catalogPort + "/"+catalogPath);
    String jwt = generateJWT();
    Schema schema = getTestSchema();
    final String tblName = "tbl_" + Integer.toHexString(RND.nextInt(65536));
    final TableIdentifier TBL = TableIdentifier.of(DB_NAME, tblName);
    String location = temp.newFolder(TBL.toString()).toString();

    Configuration configuration = new Configuration();
    configuration.set("iceberg.catalog", cname);
    configuration.set("iceberg.catalog."+cname+".type", "rest");
    configuration.set("iceberg.catalog."+cname+".uri", iceUri.toString());
    configuration.set("iceberg.catalog."+cname+".token", jwt);

    String catalogName = configuration.get(CATALOG_NAME);
    Assert.assertEquals(cname, catalogName);
    Map<String, String> properties = getCatalogPropertiesFromConf(configuration, catalogName);
    Assert.assertFalse(properties.isEmpty());
    RESTCatalog restCatalog = (RESTCatalog) CatalogUtil.buildIcebergCatalog(catalogName, properties, configuration);
    restCatalog.initialize(catalogName, properties);

    restCatalog.buildTable(TBL, schema)
            .withLocation(location)
            .createTransaction()
            .commitTransaction();
    Table table = catalog.loadTable(TBL);
    Assert.assertEquals(location, table.location());
    Table restTable = restCatalog.loadTable(TBL);
    Assert.assertEquals(location, restTable.location());
  }
}
