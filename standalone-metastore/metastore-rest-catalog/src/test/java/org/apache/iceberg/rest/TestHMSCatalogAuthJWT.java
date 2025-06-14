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

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.types.Types;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static org.apache.iceberg.types.Types.NestedField.required;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class TestHMSCatalogAuthJWT extends HMSTestBase {
  private static final File JWT_AUTHKEY_FILE =
      new File(BASE_DIR,"src/test/resources/auth/jwt/jwt-authorized-key.json");
  private static final File JWT_NOAUTHKEY_FILE =
      new File(BASE_DIR,"src/test/resources/auth/jwt/jwt-unauthorized-key.json");
  private static final File JWT_JWKS_FILE =
      new File(BASE_DIR,"src/test/resources/auth/jwt/jwt-verification-jwks.json");
  private static final int MOCK_JWKS_SERVER_PORT = 8089;

  @ClassRule
  public static final WireMockRule MOCK_JWKS_SERVER = new WireMockRule(MOCK_JWKS_SERVER_PORT);

  private String generateJWT()  throws Exception {
    return generateJWT(JWT_AUTHKEY_FILE.toPath());
  }

  private String generateJWT(Path path)  throws Exception {
    return generateJWT(USER_1, path, TimeUnit.MINUTES.toMillis(5));
  }

  private static String generateJWT(String user, Path keyFile, long lifeTimeMillis) throws Exception {
    RSAKey rsaKeyPair = RSAKey.parse(new String(java.nio.file.Files.readAllBytes(keyFile), StandardCharsets.UTF_8));
    // Create RSA-signer with the private key
    JWSSigner signer = new RSASSASigner(rsaKeyPair);
    JWSHeader header = new JWSHeader
        .Builder(JWSAlgorithm.RS256)
        .keyID(rsaKeyPair.getKeyID())
        .build();
    Date now = new Date();
    Date expirationTime = new Date(now.getTime() + lifeTimeMillis);
    JWTClaimsSet claimsSet = new JWTClaimsSet.Builder()
        .jwtID(UUID.randomUUID().toString())
        .issueTime(now)
        .issuer("auth-server")
        .subject(user)
        .expirationTime(expirationTime)
        .claim("custom-claim-or-payload", "custom-claim-or-payload")
        .build();
    SignedJWT signedJWT = new SignedJWT(header, claimsSet);
    // Compute the RSA signature
    signedJWT.sign(signer);
    return signedJWT.serialize();
  }

  private static Object clientCall(String jwt, URL url, String method, Object arg) throws IOException {
    return clientCall(jwt, url, method, true, arg);
  }

  private static Object clientCall(String jwt, URL url, String method, boolean json, Object arg) throws IOException {
    return clientCall(url, method, json, Collections.singletonMap("Authorization", "Bearer " + jwt), arg);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_SERVLET_AUTH, "jwt");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL,
        "http://localhost:" + MOCK_JWKS_SERVER_PORT + "/jwks");
    MOCK_JWKS_SERVER.stubFor(get("/jwks")
        .willReturn(ok()
            .withBody(Files.readAllBytes(JWT_JWKS_FILE.toPath()))));
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

}