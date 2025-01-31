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

import com.google.gson.Gson;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import static org.apache.iceberg.types.Types.NestedField.required;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
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
    List<String> nslist = (List<String>) nsrep.get("namespaces");
    Assert.assertEquals(2, nslist.size());
    Assert.assertTrue((nslist.contains(Arrays.asList("default"))));
    Assert.assertTrue((nslist.contains(Arrays.asList("hivedb"))));
    // succeed
    response = clientCall(jwt, url, "POST", false, "{ \"namespace\" : [ \""+ns+"\" ], "+
            "\"properties\":{ \"owner\": \"apache\", \"group\" : \"iceberg\" }"
            +"}");
    Assert.assertNotNull(response);
    HiveMetaStoreClient client = createClient(conf, port);
    Database database1 = client.getDatabase(ns);
    Assert.assertEquals("apache", database1.getParameters().get("owner"));
    Assert.assertEquals("iceberg", database1.getParameters().get("group"));

    List<TableIdentifier> tis = catalog.listTables(Namespace.of(ns));
    Assert.assertTrue(tis.isEmpty());

    // list tables in hivedb
    url = new URL("http://hive@localhost:" + catalogPort + "/" + catalogPath+"/v1/namespaces/" + ns + "/tables");
    // succeed
    response = clientCall(jwt, url, "GET", null);
    Assert.assertNotNull(response);

    // quick check on metrics
    Map<String, Long> counters = reportMetricCounters("list_namespaces", "list_tables");
    counters.entrySet().forEach(m->{
      Assert.assertTrue(m.getKey(), m.getValue() > 0);
    });
  }
  
  private Schema getTestSchema() {
    return new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get()));
  }

  
  @Test
  public void testCreateTableTxnBuilder() throws Exception {
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
      URL url = new URL("http://hive@localhost:" + catalogPort + "/"+catalogPath+"/v1/namespaces");
      String jwt = generateJWT();
      // succeed
      Object response = clientCall(jwt, url, "GET", null);
      Assert.assertNotNull(response);

      // list tables in hivedb
      url = new URL("http://hive@localhost:" + catalogPort + "/" + catalogPath+"/v1/namespaces/" + DB_NAME + "/tables");
      // succeed
      response = clientCall(jwt, url, "GET", null);
      Assert.assertNotNull(response);

      // load table
      url = new URL("http://hive@localhost:" + catalogPort + "/" + catalogPath+"/v1/namespaces/" + DB_NAME + "/tables/" + tblName);
      // succeed
      response = clientCall(jwt, url, "GET", null);
      Assert.assertNotNull(response);
      String str = new Gson().toJson(response);

      // quick check on metrics
      Map<String, Long> counters = reportMetricCounters("list_namespaces", "list_tables", "load_table");
      counters.forEach((key, value) -> Assert.assertTrue(key, value > 0));
      table = catalog.loadTable(tableIdent);
      Assert.assertNotNull(table);
    } catch (Exception xany) {
        String str = xany.getMessage();
    } finally {
        //metastoreClient.dropTable(DB_NAME, tblName);
      catalog.dropTable(tableIdent, false);
    }
  }

}
